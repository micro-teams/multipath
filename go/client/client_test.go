// Tests for the client against an in-process server: HTTP requests through the local port reach the
// target, the status reports the session and its lines (each answering, with its round trip), a refreshed line list replaces the old one,
// an https entry refuses plain lines, a local connection without a live session is refused after
// the configured wait, the fetched list is stored in the cache file, and a stored list starts a
// session while the entry is unreachable, but only for the entry that served it, and a line whose
// connection setup alone takes longer than the handshake timeout still links, since the HELLO timer
// starts once the connection is up. Behaviour under faults is the conformance suite's job.
package client

import (
	"context"
	"fmt"
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

	"github.com/micro-teams/multipath/go/protocol"
	"github.com/micro-teams/multipath/go/server"
)

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

type fixture struct {
	linesFile  string
	linesCache string // the client's line list cache file
	entry      string // the server's http:// origin
	client     *Client
}

// start runs a target, a server and a client whose entry lists the server n times as lines.
func start(t *testing.T, n int) *fixture {
	t.Helper()
	return startWith(t, n, func(h http.Handler) http.Handler { return h })
}

// startWith is start with the server's handler wrapped.
func startWith(t *testing.T, n int, wrap func(http.Handler) http.Handler) *fixture {
	t.Helper()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "target saw %s", r.URL.Path)
	}))
	t.Cleanup(target.Close)
	f := &fixture{linesFile: filepath.Join(t.TempDir(), "lines.json"), linesCache: filepath.Join(t.TempDir(), "cache.json")}
	hs := httptest.NewUnstartedServer(nil)
	f.entry = "http://" + hs.Listener.Addr().String()
	f.writeLines(t, n)
	srv, err := server.New(server.Config{LinkPath: DefaultLinkPath, LinesPath: DefaultLinesPath, LinesFile: f.linesFile,
		Target: strings.TrimPrefix(target.URL, "http://"), Limits: protocol.DefaultLimits(), Logf: testLog(t)})
	if err != nil {
		t.Fatal(err)
	}
	hs.Config.Handler = wrap(srv.Handler())
	hs.Start()
	t.Cleanup(hs.Close)
	f.client = run(t, Config{Listen: "127.0.0.1:0", Entry: f.entry, LinkPath: DefaultLinkPath, LinesPath: DefaultLinesPath,
		LinesCache: f.linesCache, Refresh: 100 * time.Millisecond, Wait: 3 * time.Second, Logf: testLog(t)})
	return f
}

// run starts a client until the test ends.
func run(t *testing.T, cfg Config) *Client {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
	return c
}

// deadEntry is an origin nothing listens on.
func deadEntry(t *testing.T) string {
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = dead.Close() // nothing listens there now, which is the point
	return "http://" + dead.Addr().String()
}

// writeLines lists the server n times, under distinct ids.
func (f *fixture) writeLines(t *testing.T, n int) {
	var lines []string
	for i := range n {
		lines = append(lines, fmt.Sprintf(`{"id":"line%d","url":%q}`, i, f.entry))
	}
	if err := os.WriteFile(f.linesFile, []byte(`{"lines":[`+strings.Join(lines, ",")+`]}`), 0o644); err != nil {
		t.Fatal(err)
	}
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

func get(t *testing.T, c *Client, path string) string {
	t.Helper()
	// A fresh connection per request: each one is a stream, which is what the test is about.
	hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
	resp, err := hc.Get("http://" + c.Addr().String() + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func TestRequestsReachTheTarget(t *testing.T) {
	f := start(t, 3)
	for i := range 30 {
		if got := get(t, f.client, fmt.Sprintf("/r%d", i)); got != fmt.Sprintf("target saw /r%d", i) {
			t.Fatalf("answer %q", got)
		}
	}
	waitFor(t, "all lines up, answering and measured", func() bool {
		st := f.client.Status()
		up := 0
		for _, l := range st.Lines {
			if l.State == "up" && !l.Soft && l.RTTMs > 0 {
				up++
			}
		}
		return up == 3 && st.Session.State == "live" && st.Session.Server == protocol.Implementation
	})
	waitFor(t, "streams released", func() bool { return len(f.client.Status().Streams) == 0 })
}

func TestRefreshedLineListReplacesTheOld(t *testing.T) {
	f := start(t, 3)
	waitFor(t, "three lines", func() bool { return len(f.client.Status().Lines) == 3 })
	f.writeLines(t, 1)
	waitFor(t, "one line", func() bool {
		st := f.client.Status()
		return len(st.Lines) == 1 && st.Lines[0].ID == "line0"
	})
	if got := get(t, f.client, "/after"); got != "target saw /after" {
		t.Fatalf("answer %q", got)
	}
	if n := f.client.Status().SessionsStarted; n != 1 {
		t.Fatalf("%d sessions started; the refresh must not restart the session", n)
	}
}

func TestHTTPSEntryRefusesPlainLines(t *testing.T) {
	entry := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"lines":[{"id":"a","url":"https://a.example.app"},{"id":"b","url":"http://b.example.app"}]}`) // the client parses it below
	}))
	defer entry.Close()
	c, err := New(Config{Listen: "127.0.0.1:0", Entry: entry.URL, LinkPath: DefaultLinkPath, LinesPath: DefaultLinesPath,
		TLSConfig: entry.Client().Transport.(*http.Transport).TLSClientConfig, Refresh: time.Hour, Wait: time.Second, Logf: testLog(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.ln.Close()
	if _, err := c.fetchLines(context.Background()); err == nil || !strings.Contains(err.Error(), "not https") {
		t.Fatalf("want a refusal, got %v", err)
	}
}

func TestLocalConnectionWithoutASessionIsRefused(t *testing.T) {
	c, err := New(Config{Listen: "127.0.0.1:0", Entry: deadEntry(t), LinkPath: DefaultLinkPath, LinesPath: DefaultLinesPath,
		Refresh: time.Hour, Wait: 300 * time.Millisecond, Logf: testLog(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }() // ends with the test
	conn, err := net.Dial("tcp", c.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second)) // the refusal must come well before
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("want the connection closed, got %v", err)
	}
}

func TestFetchedLineListIsCached(t *testing.T) {
	f := start(t, 2)
	waitFor(t, "the cache file", func() bool { _, err := os.Stat(f.linesCache); return err == nil })
	data, err := os.ReadFile(f.linesCache)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(`{"entry":%q,"lines":[{"id":"line0","url":%q},{"id":"line1","url":%q}]}`, f.entry, f.entry, f.entry)
	if string(data) != want {
		t.Fatalf("cache file %s, want %s", data, want)
	}
}

func TestCachedLineListStartsASessionWithoutTheEntry(t *testing.T) {
	f := start(t, 1)
	entry := deadEntry(t)
	cache := filepath.Join(t.TempDir(), "cache.json")
	doc := fmt.Sprintf(`{"entry":%q,"lines":[{"id":"cached","url":%q}]}`, entry, f.entry)
	if err := os.WriteFile(cache, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	c := run(t, Config{Listen: "127.0.0.1:0", Entry: entry, LinkPath: DefaultLinkPath, LinesPath: DefaultLinesPath,
		LinesCache: cache, Refresh: time.Hour, Wait: 3 * time.Second, Logf: testLog(t)})
	if got := get(t, c, "/cached"); got != "target saw /cached" {
		t.Fatalf("answer %q", got)
	}
	if st := c.Status(); len(st.Lines) != 1 || st.Lines[0].ID != "cached" {
		t.Fatalf("lines %+v", st.Lines)
	}
}

func TestCachedLineListOfAnotherEntryIsIgnored(t *testing.T) {
	cache := filepath.Join(t.TempDir(), "cache.json")
	doc := `{"entry":"https://other.example","lines":[{"id":"a","url":"https://a.example"}]}`
	if err := os.WriteFile(cache, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{Listen: "127.0.0.1:0", Entry: deadEntry(t), LinkPath: DefaultLinkPath, LinesPath: DefaultLinesPath,
		LinesCache: cache, Refresh: time.Hour, Wait: time.Second, Logf: testLog(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.ln.Close()
	if lines, _ := c.knownLines(); lines != nil {
		t.Fatalf("kept %v from another entry's cache", lines)
	}
	if err := c.loadCache(); err == nil || !strings.Contains(err.Error(), "other.example") {
		t.Fatalf("want a refusal naming the other entry, got %v", err)
	}
}

// A line whose WebSocket upgrade takes longer than protocol.HandshakeTimeout, as through a distant
// CDN, links all the same: the setup has protocol.ConnectTimeout, and the HELLO its own timeout.
func TestSlowConnectionSetupStillLinks(t *testing.T) {
	setup := protocol.HandshakeTimeout + time.Second
	f := startWith(t, 1, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == DefaultLinkPath {
				time.Sleep(setup)
			}
			h.ServeHTTP(w, r)
		})
	})
	for deadline := time.Now().Add(setup + 5*time.Second); ; time.Sleep(50 * time.Millisecond) {
		st := f.client.Status()
		if len(st.Lines) == 1 && st.Lines[0].State == "up" {
			if st.Lines[0].LastReason != "" {
				t.Fatalf("the line failed first: %q", st.Lines[0].LastReason)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the line never came up: %+v", st.Lines)
		}
	}
}
