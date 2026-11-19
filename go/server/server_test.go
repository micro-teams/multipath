// Tests for the server's normal path: the lines endpoint, a stream carried to the target and back,
// the PROXY protocol header, and every resource released when the client's link goes away. The
// helpers here (a server on a test port, a raw handshake, a minimal client session) are shared with
// the handshake and restart tests.
package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/micro-teams/multipath/go/link"
	"github.com/micro-teams/multipath/go/mux"
	"github.com/micro-teams/multipath/go/protocol"
	"github.com/micro-teams/multipath/go/session"
)

// testLog logs through t until the test ends; sessions may still log while they wind down.
func testLog(t *testing.T) func(string, ...any) {
	var mu sync.Mutex
	done := false
	t.Cleanup(func() { mu.Lock(); done = true; mu.Unlock() })
	return func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		if !done {
			t.Logf(format, args...)
		}
	}
}

const testLines = `{"lines":[{"id":"a","url":"http://127.0.0.1"}]}`

// newServer runs a server for target on a test port and returns it with its ws:// link URL.
func newServer(t *testing.T, target string, edit func(*Config)) (*Server, *httptest.Server, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "lines.json")
	if err := os.WriteFile(file, []byte(testLines), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := Config{LinkPath: "/link", LinesPath: "/lines", LinesFile: file, Target: target, Limits: protocol.DefaultLimits(), Logf: testLog(t)}
	if edit != nil {
		edit(&cfg)
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	return srv, hs, "ws" + strings.TrimPrefix(hs.URL, "http") + "/link"
}

func newKey() [16]byte {
	var k [16]byte
	_, _ = rand.Read(k[:]) // crypto/rand does not fail on supported platforms
	return k
}

// handshake dials url, sends h, and returns the link with the server's answer.
func handshake(t *testing.T, url string, h protocol.HelloFrame) (*link.Conn, byte, []byte) {
	t.Helper()
	c, err := link.Dial(context.Background(), url, link.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if h.Version == 0 {
		h.Version = protocol.Version
	}
	if err := c.WriteMessage(protocol.EncodeHello(h)); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second)) // a missing answer fails the read
	msg, err := c.ReadMessage(1 << 16)
	if err != nil {
		t.Fatalf("no answer to hello: %v", err)
	}
	_ = c.SetReadDeadline(time.Time{}) // the session's liveness rules take over
	typ, p, err := protocol.ParseFrame(msg, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	return c, typ, p
}

// clientMux opens a session with one link to url and returns its mux and session.
func clientMux(t *testing.T, url string) (*mux.Mux, *session.Session) {
	t.Helper()
	c, typ, p := handshake(t, url, protocol.HelloFrame{Session: newKey(), Mode: protocol.ModeNew})
	if typ != protocol.Welcome {
		t.Fatalf("answer 0x%02x: %s", typ, p)
	}
	w, err := protocol.DecodeWelcome(p)
	if err != nil || !w.Created || w.Impl != protocol.Implementation {
		t.Fatalf("welcome %+v, %v", w, err)
	}
	var m *mux.Mux
	s := session.New(session.Options{Limits: w.Limits, Client: true, Deliver: func(p []byte) error { return m.Feed(p) }})
	m = mux.New(s, true, w.Limits, nil)
	if err := s.Attach(0, c); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.End("test over") })
	return m, s
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestLinesEndpoint(t *testing.T) {
	srv, hs, _ := newServer(t, "127.0.0.1:1", nil)
	resp, err := http.Get(hs.URL + "/lines")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != testLines || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("%s %q", resp.Status, body)
	}
	if err := os.WriteFile(srv.cfg.LinesFile, []byte(`{"lines":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	resp, err = http.Get(hs.URL + "/lines")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("an invalid lines file was served: %s", resp.Status)
	}
}

func TestNewRefusesBadConfig(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no lines file": {LinkPath: "/l", LinesPath: "/x", Target: "a:1", Limits: protocol.DefaultLimits(), Logf: t.Logf},
		"same paths":    {LinkPath: "/l", LinesPath: "/l", Target: "a:1", Limits: protocol.DefaultLimits(), Logf: t.Logf},
		"bad target":    {LinkPath: "/l", LinesPath: "/x", Target: "a", Limits: protocol.DefaultLimits(), Logf: t.Logf},
		"bad limits":    {LinkPath: "/l", LinesPath: "/x", Target: "a:1", Logf: t.Logf},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// An HTTP exchange travels a stream to the target and back, and everything is released when the
// client's only link goes away.
func TestStreamReachesTargetAndIsReleased(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(append([]byte("echo: "), body...)) // the client checks the answer
	}))
	defer target.Close()
	srv, _, url := newServer(t, strings.TrimPrefix(target.URL, "http://"), nil)
	m, s := clientMux(t, url)
	for i := range 20 {
		st, err := m.Open()
		if err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequest("POST", "http://target/", strings.NewReader("request "+string(rune('a'+i))))
		if err := req.Write(st); err != nil {
			t.Fatal(err)
		}
		_ = st.Close() // the response below shows whether the request arrived
		resp, err := http.ReadResponse(bufio.NewReader(st), req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "echo: request "+string(rune('a'+i)) {
			t.Fatalf("answer %q", body)
		}
	}
	waitFor(t, "streams to be released", func() bool { st := srv.Status(); return st.Streams == 0 && st.Targets == 0 })
	if st := srv.Status(); st.Sessions != 1 || st.Links != 1 {
		t.Fatalf("status %+v", st)
	}
	s.End("test over") // closes the client's only link, so the server's session must end
	waitFor(t, "the session to end", func() bool { return srv.Status().Sessions == 0 })
}

func TestProxyProtocolCarriesTheClientAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan []byte, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		b := make([]byte, 28)
		_, _ = io.ReadFull(c, b) // a short read shows as a mismatch below
		got <- b
	}()
	_, _, url := newServer(t, ln.Addr().String(), func(c *Config) { c.ProxyProtocol = true })
	m, _ := clientMux(t, url)
	st, err := m.Open()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = st.Write([]byte("x")) // the target reads the header that precedes this byte
	b := <-got
	want := append(append([]byte(nil), proxySignature...), 0x21, 0x11, 0, 12, 127, 0, 0, 1, 127, 0, 0, 1, 0, 0)
	if !bytes.Equal(b[:26], want) {
		t.Fatalf("header % x", b)
	}
}

func TestClientAddress(t *testing.T) {
	for want, h := range map[string]http.Header{
		"203.0.113.7":  {"Cf-Connecting-Ip": {"203.0.113.7"}, "X-Forwarded-For": {"198.51.100.1"}},
		"198.51.100.1": {"X-Forwarded-For": {"198.51.100.1, 10.0.0.1"}},
		"192.0.2.9":    {},
	} {
		r := &http.Request{Header: h, RemoteAddr: "192.0.2.9:4567"}
		if got := clientAddress(r); got != want {
			t.Errorf("clientAddress(%v) = %s, want %s", h, got, want)
		}
	}
	v6 := proxyHeader("2001:db8::1", &net.TCPAddr{IP: net.ParseIP("2001:db8::2"), Port: 443})
	if len(v6) != 16+36 || v6[13] != 0x21 {
		t.Errorf("IPv6 header % x", v6)
	}
	if local := proxyHeader("not an address", nil); len(local) != 16 || local[12] != 0x20 {
		t.Errorf("LOCAL header % x", local)
	}
}
