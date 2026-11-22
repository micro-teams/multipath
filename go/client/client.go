// Package client is the MultiPath client: a local TCP port whose every connection becomes one stream
// of a session carried over all of the entry's lines at once (PROTOCOL.md). Programs on the machine
// connect to the port as if it were the server's target; they never learn that lines exist.
//
// The client runs one session at a time. It fetches the line list from its entry, dials every line
// (directly or through the configured HTTP proxy), and keeps every line dialled for the life of the
// session: a line that fails is retried with backoff, never given up. When no line answers, every
// line is dialled again at once into the same session, so its streams survive a network handover.
// When the session ends (the server lost it, the server said goodbye, or no line answered for the
// server's dead time) its streams fail at once and the client starts a new session. The list is also refreshed periodically and replaces the old one; lines that
// left it are closed, new ones are dialled.
//
// The last list the entry served is kept (in memory, and in the Config.LinesCache file across
// restarts). A new session starts from the kept list at once while the entry is asked again, so an
// unreachable entry delays nothing when the lines themselves work; the entry is asked again with
// backoff until it answers, and its answer replaces the kept list in that session. Only the first
// session of a client that has never had a list waits for the entry.
//
// This file: configuration, the session loop and the local port. lines.go fetches, keeps and stores
// the line list; session.go runs one session and its lines; status.go reports state.
//
//   - Config: the local listen address, the entry, the paths, proxy and TLS settings, the line list
//     cache file, timings, the log function. DefaultLinkPath and DefaultLinesPath are the server's
//     default paths.
//
//   - New(cfg) validates cfg, loads the line list cache (an unusable one is logged and ignored), and
//     binds the local port (so Addr is known before Run).
//
//   - (*Client).Addr() is the bound local address.
//
//   - (*Client).Run(ctx) serves the local port and runs sessions until ctx is done; it then sends
//     GOODBYE and returns nil, or returns the error that stopped the local port.
//
//   - (*Client).Status() and StatusHandler() report the session, lines and streams (status.go).
//
//     c, err := client.New(client.Config{Listen: "127.0.0.1:7000", Entry: "https://example.app",
//     LinkPath: client.DefaultLinkPath, LinesPath: client.DefaultLinesPath,
//     Proxy: http.ProxyFromEnvironment, LinesCache: "/var/lib/app/lines.json",
//     Refresh: 10 * time.Minute, Wait: 10 * time.Second, Logf: log.Printf})
//     err = c.Run(ctx)
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micro-teams/multipath/go/mux"
	"github.com/micro-teams/multipath/go/protocol"
	"github.com/micro-teams/multipath/go/session"
)

// The server's default paths (PROTOCOL.md section 2).
const (
	DefaultLinkPath  = "/multipath/v3/link"
	DefaultLinesPath = "/multipath/v3/lines"
)

// Config configures a Client.
type Config struct {
	Listen     string                                // local address programs connect to
	Entry      string                                // origin the line list is fetched from
	LinkPath   string                                // appended to every line's URL
	LinesPath  string                                // appended to the entry
	Proxy      func(*http.Request) (*url.URL, error) // nil means connect directly
	TLSConfig  *tls.Config                           // nil means the system roots
	LinesCache string                                // file keeping the last line list; "" keeps it in memory only
	Refresh    time.Duration                         // how often the line list is re-fetched
	Wait       time.Duration                         // how long a local connection waits for a live session
	Logf       func(format string, args ...any)
	Clock      *session.Clock // nil means session.DefaultClock()
}

// Client is a running client.
type Client struct {
	cfg  Config
	ln   net.Listener
	http *http.Client

	mu       sync.Mutex
	current  *clientSession
	changed  chan struct{}   // closed and replaced whenever current changes
	sessions int             // sessions started
	lastEnd  string          // why the previous session ended
	known    []protocol.Line // the last list the entry served (or the cache file held); nil before any
	knownAt  time.Time       // when the entry served it
	conns    atomic.Int64    // local connections being served
}

// New validates cfg and binds the local port.
func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.Entry)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || (u.Path != "" && u.Path != "/") {
		return nil, fmt.Errorf("entry %q must be an http(s) origin such as https://example.app", cfg.Entry)
	}
	cfg.Entry = strings.TrimSuffix(cfg.Entry, "/")
	if !strings.HasPrefix(cfg.LinkPath, "/") || !strings.HasPrefix(cfg.LinesPath, "/") {
		return nil, fmt.Errorf("link path %q and lines path %q must be absolute", cfg.LinkPath, cfg.LinesPath)
	}
	if cfg.Refresh <= 0 || cfg.Wait <= 0 || cfg.Logf == nil {
		return nil, errors.New("refresh, wait and a log function are required")
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	hc := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: cfg.Proxy, TLSClientConfig: cfg.TLSConfig}}
	c := &Client{cfg: cfg, ln: ln, http: hc, changed: make(chan struct{})}
	if cfg.LinesCache != "" {
		if err := c.loadCache(); err != nil {
			cfg.Logf("line list cache %s unusable, the first session waits for the entry: %v", cfg.LinesCache, err)
		} else if c.known != nil {
			cfg.Logf("line list cache %s: %d lines, fetched at %s", cfg.LinesCache, len(c.known), c.knownAt.Format(time.RFC3339))
		}
	}
	return c, nil
}

// Addr returns the local address programs connect to.
func (c *Client) Addr() net.Addr { return c.ln.Addr() }

// Run serves the local port and runs sessions until ctx is done.
func (c *Client) Run(ctx context.Context) error {
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- c.acceptLoop() }()
	go c.refreshLoop(ctx)
	sessionsDone := make(chan struct{})
	go func() { defer close(sessionsDone); c.sessionLoop(ctx) }()
	select {
	case <-ctx.Done():
		_ = c.ln.Close() // stopping on purpose; the accept loop's error is expected
		<-sessionsDone
		return nil
	case err := <-acceptErr:
		return fmt.Errorf("local port: %w", err)
	}
}

// sessionLoop runs one session after another.
func (c *Client) sessionLoop(ctx context.Context) {
	retry := 250 * time.Millisecond
	for ctx.Err() == nil {
		lines, at := c.knownLines()
		kept := lines != nil
		if !kept {
			var err error
			if lines, err = c.updateLines(ctx); err != nil {
				c.cfg.Logf("line list: %v; retrying in %v", err, retry)
				sleep(ctx, retry)
				retry = min(2*retry, 30*time.Second)
				continue
			}
			retry = 250 * time.Millisecond
		}
		cs := newClientSession(c, lines)
		c.setCurrent(cs)
		if kept {
			c.cfg.Logf("session %s uses the line list fetched at %s while the entry is asked again", cs.id, at.Format(time.RFC3339))
			go c.renewLines(cs)
		}
		select {
		case <-ctx.Done():
			cs.s.Goodbye("client shutting down")
			return
		case <-cs.s.Done():
		}
		why := cs.s.Reason()
		c.mu.Lock()
		c.lastEnd = why
		c.mu.Unlock()
		// After the server went away every client of it reconnects; a random delay spreads them out.
		if cs.serverGone.Load() || strings.HasPrefix(why, session.PeerGoodbye) {
			d := time.Duration(rand.Int64N(int64(2 * time.Second)))
			c.cfg.Logf("starting a new session in %v", d.Round(time.Millisecond))
			sleep(ctx, d)
		}
	}
}

// renewLines asks the entry for the list until it answers or the session ends, and applies the
// answer to the session, which until then runs on the kept list.
func (c *Client) renewLines(cs *clientSession) {
	retry := time.Second
	for {
		lines, err := c.updateLines(cs.ctx)
		if err == nil {
			cs.applyLines(lines)
			return
		}
		if cs.ctx.Err() != nil {
			return
		}
		c.cfg.Logf("line list: %v; session %s stays on the kept list, asking again in %v", err, cs.id, retry)
		sleep(cs.ctx, retry)
		retry = min(2*retry, 30*time.Second)
	}
}

func (c *Client) setCurrent(cs *clientSession) {
	c.mu.Lock()
	c.current = cs
	c.sessions++
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (c *Client) acceptLoop() error {
	for {
		conn, err := c.ln.Accept()
		if err != nil {
			return err
		}
		go c.serveLocal(conn)
	}
}

// serveLocal carries one local connection as one stream of a live session.
func (c *Client) serveLocal(conn net.Conn) {
	c.conns.Add(1)
	defer c.conns.Add(-1)
	m, err := c.liveMux()
	var st *mux.Stream
	if err == nil {
		st, err = m.Open()
	}
	if err != nil {
		c.cfg.Logf("local connection from %s refused: %v", conn.RemoteAddr(), err)
		_ = conn.Close() // the refusal is logged above
		return
	}
	if err := mux.Splice(st, conn); err != nil {
		c.cfg.Logf("local connection from %s, stream %d: %v", conn.RemoteAddr(), st.ID(), err)
	}
}

// liveMux waits up to Config.Wait for a session with a live link and returns its mux.
func (c *Client) liveMux() (*mux.Mux, error) {
	deadline := time.NewTimer(c.cfg.Wait)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		cs, changed := c.current, c.changed
		c.mu.Unlock()
		var live, done <-chan struct{}
		if cs != nil {
			live, done = cs.live, cs.s.Done()
		}
		select {
		case <-live:
			return cs.m, nil
		case <-done:
			// That session ended before it was live; wait for the next one.
			select {
			case <-changed:
			case <-deadline.C:
				return nil, fmt.Errorf("no live session within %v", c.cfg.Wait)
			}
		case <-changed:
		case <-deadline.C:
			return nil, fmt.Errorf("no live session within %v", c.cfg.Wait)
		}
	}
}

// refreshLoop re-fetches the line list and applies it to the running session.
func (c *Client) refreshLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.Refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		lines, err := c.updateLines(ctx)
		if err != nil {
			c.cfg.Logf("line list refresh: %v; keeping the current lines", err)
			continue
		}
		c.mu.Lock()
		cs := c.current
		c.mu.Unlock()
		if cs != nil {
			cs.applyLines(lines)
		}
	}
}
