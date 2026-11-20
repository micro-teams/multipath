// Tests for the client against an in-process server: HTTP requests through the local port reach the
// target, the status reports the session and its lines, a refreshed line list replaces the old one,
// an https entry refuses plain lines, and a local connection without a live session is refused after
// the configured wait. Behaviour under faults is the conformance suite's job.
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
	linesFile string
	entry     string // the server's http:// origin
	client    *Client
}

// start runs a target, a server and a client whose entry lists the server n times as lines.
func start(t *testing.T, n int) *fixture {
	t.Helper()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "target saw %s", r.URL.Path)
	}))
	t.Cleanup(target.Close)
	f := &fixture{linesFile: filepath.Join(t.TempDir(), "lines.json")}
	hs := httptest.NewUnstartedServer(nil)
	f.entry = "http://" + hs.Listener.Addr().String()
	f.writeLines(t, n)
	srv, err := server.New(server.Config{LinkPath: DefaultLinkPath, LinesPath: DefaultLinesPath, LinesFile: f.linesFile,
		Target: strings.TrimPrefix(target.URL, "http://"), Limits: protocol.DefaultLimits(), Logf: testLog(t)})
	if err != nil {
		t.Fatal(err)
	}
	hs.Config.Handler = srv.Handler()
	hs.Start()
	t.Cleanup(hs.Close)
	f.client, err = New(Config{Listen: "127.0.0.1:0", Entry: f.entry, LinkPath: DefaultLinkPath, LinesPath: DefaultLinesPath,
		Refresh: 100 * time.Millisecond, Wait: 3 * time.Second, Logf: testLog(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
	return f
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
	waitFor(t, "all lines up", func() bool {
		st := f.client.Status()
		up := 0
		for _, l := range st.Lines {
			if l.State == "up" {
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
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entry := "http://" + dead.Addr().String()
	_ = dead.Close() // nothing listens there now, so no session can start
	c, err := New(Config{Listen: "127.0.0.1:0", Entry: entry, LinkPath: DefaultLinkPath, LinesPath: DefaultLinesPath,
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
