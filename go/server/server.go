// Package server is the MultiPath server: a reverse proxy that accepts links on one HTTP port and
// splices every stream of every session to one configured TCP target (PROTOCOL.md). There is no
// destination in the protocol, so the server is never an open relay; a session exists only while it
// has a live link, so every resource it holds is bounded by the number of live connections.
//
// The same port answers two paths: the link path, where WebSocket upgrades become links, and the
// lines path, which serves the line list from a file (read on every request, so the list can change
// without a restart). TLS belongs to whatever sits in front.
//
// Every event is one log line naming the session by the first 8 hex digits of its id: session start
// (client address, peer implementation), link up and down with the reason, REJECT with the reason,
// stream failures, and session end with the reason.
//
//   - Config: paths, the lines file, the target, PROXY protocol, the limits, the log function.
//
//   - New(cfg) validates the configuration (the lines file must parse) and returns a Server.
//
//   - (*Server).Handler() serves the link and lines paths. Run it under an http.Server whose
//     ReadHeaderTimeout is protocol.HandshakeTimeout, so a connection that never finishes its
//     upgrade is closed.
//
//   - (*Server).Status() returns the live counters: sessions, links, streams, target connections,
//     goroutines. StatusHandler() serves them as JSON for a local status port.
//
//   - (*Server).Shutdown() refuses new sessions and sends GOODBYE on every session, so clients
//     reconnect to the next server process at once instead of waiting for their links to die.
//
//     srv, err := server.New(server.Config{LinkPath: "/multipath/v3/link", LinesPath: "/multipath/v3/lines",
//     LinesFile: "lines.json", Target: "127.0.0.1:80", Limits: protocol.DefaultLimits(), Logf: log.Printf})
//     hs := &http.Server{Addr: ":8080", Handler: srv.Handler(), ReadHeaderTimeout: protocol.HandshakeTimeout}
//     log.Fatal(hs.ListenAndServe())
package server

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micro-teams/multipath/go/link"
	"github.com/micro-teams/multipath/go/mux"
	"github.com/micro-teams/multipath/go/protocol"
	"github.com/micro-teams/multipath/go/session"
)

// Config configures a Server.
type Config struct {
	LinkPath      string // where WebSocket upgrades become links
	LinesPath     string // where the line list is served
	LinesFile     string // the line list document, read on every request
	Target        string // host:port every stream is spliced to
	ProxyProtocol bool   // send a PROXY protocol v2 header with the client address to the target
	Limits        protocol.Limits
	Logf          func(format string, args ...any)
	Clock         *session.Clock // nil means session.DefaultClock()
}

// Server accepts links and splices streams to the target.
type Server struct {
	cfg Config

	mu       sync.Mutex
	sessions map[[16]byte]*serverSession
	closing  bool

	streams atomic.Int64
	targets atomic.Int64
}

type serverSession struct {
	id      string // short id for logs
	key     [16]byte
	client  string // the client address reported to the target
	local   net.Addr
	started time.Time
	s       *session.Session
	m       *mux.Mux
}

// New validates cfg and returns a Server.
func New(cfg Config) (*Server, error) {
	if err := cfg.Limits.Validate(); err != nil {
		return nil, fmt.Errorf("limits: %w", err)
	}
	if !strings.HasPrefix(cfg.LinkPath, "/") || !strings.HasPrefix(cfg.LinesPath, "/") || cfg.LinkPath == cfg.LinesPath {
		return nil, fmt.Errorf("link path %q and lines path %q must be distinct absolute paths", cfg.LinkPath, cfg.LinesPath)
	}
	if _, _, err := net.SplitHostPort(cfg.Target); err != nil {
		return nil, fmt.Errorf("target %q: %w", cfg.Target, err)
	}
	if _, err := readLines(cfg.LinesFile); err != nil {
		return nil, err
	}
	if cfg.Logf == nil {
		return nil, fmt.Errorf("a log function is required")
	}
	return &Server{cfg: cfg, sessions: make(map[[16]byte]*serverSession)}, nil
}

func readLines(file string) ([]byte, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("lines file: %w", err)
	}
	if _, err := protocol.ParseLines(data); err != nil {
		return nil, fmt.Errorf("lines file %s: %w", file, err)
	}
	return data, nil
}

// Handler serves the link and lines paths.
func (s *Server) Handler() http.Handler {
	h := http.NewServeMux()
	h.HandleFunc(s.cfg.LinkPath, s.serveLink)
	h.HandleFunc(s.cfg.LinesPath, s.serveLines)
	return h
}

func (s *Server) serveLines(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	data, err := readLines(s.cfg.LinesFile)
	if err != nil {
		s.cfg.Logf("lines: %v", err)
		http.Error(w, "line list unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data) // a client that went away has nothing to be told
}

// clientAddress is the address reported to the target: see PROTOCOL.md section 10 for when the
// headers can be trusted.
func clientAddress(r *http.Request) string {
	if ip := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); ip != "" {
		return ip
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) serveLink(w http.ResponseWriter, r *http.Request) {
	from := clientAddress(r)
	c, err := link.Accept(w, r)
	if err != nil {
		s.cfg.Logf("link from %s: %v", from, err)
		return
	}
	lim := s.cfg.Limits
	deadline := time.Now().Add(protocol.HandshakeTimeout)
	_ = c.SetReadDeadline(deadline) // a socket that refuses a deadline fails the read below
	msg, err := c.ReadMessage(lim.MaxPayload + protocol.HeaderLen)
	if err != nil {
		s.cfg.Logf("link from %s: no hello: %v", from, err)
		_ = c.Close() // nothing was established; the reason is logged above
		return
	}
	typ, p, err := protocol.ParseFrame(msg, lim.MaxPayload)
	if err == nil && typ != protocol.Hello {
		err = fmt.Errorf("first frame has type 0x%02x", typ)
	}
	var h protocol.HelloFrame
	if err == nil {
		h, err = protocol.DecodeHello(p)
	}
	switch {
	case err != nil:
		s.reject(c, from, protocol.RejectMalformed, "malformed hello: "+err.Error())
		return
	case h.Version != protocol.Version:
		s.reject(c, from, protocol.RejectVersion, fmt.Sprintf("protocol version %d is not supported; this server speaks %d", h.Version, protocol.Version))
		return
	case int(h.Link) >= lim.MaxLinks:
		s.reject(c, from, protocol.RejectLimit, fmt.Sprintf("link index %d outside 0..%d", h.Link, lim.MaxLinks-1))
		return
	}
	_ = c.SetReadDeadline(time.Time{}) // the session's liveness rules take over; a failure shows there

	s.mu.Lock()
	ss := s.sessions[h.Session]
	created := ss == nil
	switch {
	case created && h.Mode == protocol.ModeJoin:
		s.mu.Unlock()
		s.reject(c, from, protocol.RejectUnknownSession, "unknown session")
		return
	case created && s.closing:
		s.mu.Unlock()
		s.reject(c, from, protocol.RejectLimit, "server shutting down")
		return
	case created:
		ss = s.newSession(h, from, r)
		s.sessions[h.Session] = ss
	}
	s.mu.Unlock()
	if created {
		s.cfg.Logf("session %s start: client %s, peer %s, protocol %d", ss.id, from, h.Impl, h.Version)
	}

	// WELCOME goes out before Attach starts the link's writer, so it is the first frame.
	_ = c.SetWriteDeadline(deadline) // a socket that refuses a deadline fails the write below
	err = c.WriteMessage(protocol.EncodeWelcome(protocol.WelcomeFrame{Version: protocol.Version, Created: created, Limits: lim, Impl: protocol.Implementation}))
	if err == nil {
		err = ss.s.Attach(int(h.Link), c)
	}
	if err != nil {
		s.cfg.Logf("session %s link %d from %s: %v", ss.id, h.Link, r.RemoteAddr, err)
		_ = c.Close() // the reason is logged above
		if created {
			ss.s.End("its first link failed: " + err.Error())
		}
		return
	}
	s.cfg.Logf("session %s link %d up from %s", ss.id, h.Link, r.RemoteAddr)
}

// reject sends REJECT and closes the link once the client has had a moment to read it: a close with
// unread input resets the connection, and the reset can overtake the REJECT.
func (s *Server) reject(c *link.Conn, from string, code byte, why string) {
	s.cfg.Logf("link from %s rejected: %s", from, why)
	_ = c.SetDeadline(time.Now().Add(time.Second)) // bounds the courtesy below; failure only shortens it
	if c.WriteMessage(protocol.EncodeReject(code, why)) == nil {
		for {
			if _, err := c.ReadMessage(protocol.DefaultLimits().MaxPayload + protocol.HeaderLen); err != nil {
				break // the client closed, or the second passed
			}
		}
	}
	_ = c.Close() // the rejection is logged above
}

func (s *Server) newSession(h protocol.HelloFrame, from string, r *http.Request) *serverSession {
	ss := &serverSession{id: hex.EncodeToString(h.Session[:4]), key: h.Session, client: from, started: time.Now()}
	ss.local, _ = r.Context().Value(http.LocalAddrContextKey).(net.Addr)
	var m *mux.Mux
	ss.s = session.New(session.Options{
		Limits:  s.cfg.Limits,
		Clock:   s.cfg.Clock,
		Deliver: func(p []byte) error { return m.Feed(p) },
		OnLink: func(i int, up bool, why string) {
			if !up {
				s.cfg.Logf("session %s link %d down: %s", ss.id, i, why)
			}
		},
		OnEnd: func(why string) {
			m.Close(why)
			s.mu.Lock()
			if s.sessions[ss.key] == ss {
				delete(s.sessions, ss.key)
			}
			s.mu.Unlock()
			st := ss.s.Stats()
			s.cfg.Logf("session %s end after %v (%d bytes in, %d out): %s", ss.id, time.Since(ss.started).Round(time.Millisecond), st.DeliverNext, st.SendNext, why)
		},
	})
	m = mux.New(ss.s, false, s.cfg.Limits, func(st *mux.Stream) { go s.splice(ss, st) })
	ss.m = m
	return ss
}

// splice connects one stream to the target.
func (s *Server) splice(ss *serverSession, st *mux.Stream) {
	s.streams.Add(1)
	defer s.streams.Add(-1)
	c, err := net.DialTimeout("tcp", s.cfg.Target, protocol.HandshakeTimeout)
	if err != nil {
		s.cfg.Logf("session %s stream %d: target: %v", ss.id, st.ID(), err)
		_ = st.Reset("target unreachable") // the session may be gone; the failure is logged above
		return
	}
	s.targets.Add(1)
	defer s.targets.Add(-1)
	if s.cfg.ProxyProtocol {
		if _, err := c.Write(proxyHeader(ss.client, ss.local)); err != nil {
			s.cfg.Logf("session %s stream %d: proxy header: %v", ss.id, st.ID(), err)
			_ = st.Reset("target unreachable") // the failure is logged above
			_ = c.Close()                      // the failure is logged above
			return
		}
	}
	if err := mux.Splice(st, c); err != nil {
		s.cfg.Logf("session %s stream %d: %v", ss.id, st.ID(), err)
	}
}

// Status is a snapshot of the server's counters.
type Status struct {
	Sessions   int `json:"sessions"`
	Links      int `json:"links"`
	Streams    int `json:"streams"`
	Targets    int `json:"targets"`
	Goroutines int `json:"goroutines"`
}

// Status returns the live counters.
func (s *Server) Status() Status {
	s.mu.Lock()
	sessions := make([]*serverSession, 0, len(s.sessions))
	for _, ss := range s.sessions {
		sessions = append(sessions, ss)
	}
	s.mu.Unlock()
	st := Status{Sessions: len(sessions), Streams: int(s.streams.Load()), Targets: int(s.targets.Load()), Goroutines: runtime.NumGoroutine()}
	for _, ss := range sessions {
		st.Links += len(ss.s.Stats().Links)
	}
	return st
}

// StatusHandler serves Status as JSON.
func (s *Server) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(s.Status()); err != nil {
			s.cfg.Logf("status: %v", err)
		}
	})
}

// Shutdown refuses new sessions and says goodbye to every session.
func (s *Server) Shutdown() {
	s.mu.Lock()
	s.closing = true
	sessions := make([]*serverSession, 0, len(s.sessions))
	for _, ss := range s.sessions {
		sessions = append(sessions, ss)
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, ss := range sessions {
		wg.Add(1)
		go func() { defer wg.Done(); ss.s.Goodbye("server shutting down") }()
	}
	wg.Wait()
}
