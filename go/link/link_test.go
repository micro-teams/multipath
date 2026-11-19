// Tests for links: a message round trip through Accept and Dial (directly and through an HTTP CONNECT
// proxy), the message size limit, and a Close that does not wait for a stuck write.
package link

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// echoServer accepts links and echoes every message back.
func echoServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r)
		if err != nil {
			t.Logf("accept: %v", err)
			return
		}
		defer c.Close()
		for {
			m, err := c.ReadMessage(1 << 16)
			if err != nil {
				return
			}
			if err := c.WriteMessage(m); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func wsURL(srv *httptest.Server) string { return "ws" + strings.TrimPrefix(srv.URL, "http") + "/link" }

func roundTrip(t *testing.T, c *Conn) {
	t.Helper()
	// Lengths on both sides of the 7-bit and 16-bit length encodings.
	for _, n := range []int{1, 125, 126, 200, 65535} {
		want := bytes.Repeat([]byte{byte(n)}, n)
		if err := c.WriteMessage(want); err != nil {
			t.Fatalf("write %d: %v", n, err)
		}
		got, err := c.ReadMessage(1 << 16)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("echo of %d bytes: got %d bytes, %v", n, len(got), err)
		}
	}
}

func TestDialAndAccept(t *testing.T) {
	srv := echoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, wsURL(srv), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	roundTrip(t, c)
}

// connectProxy is a minimal HTTP CONNECT proxy that counts the tunnels it opened.
func connectProxy(t *testing.T, tunnels *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT only", http.StatusMethodNotAllowed)
			return
		}
		up, err := net.Dial("tcp", r.Host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		tunnels.Add(1)
		w.WriteHeader(http.StatusOK)
		down, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		go func() { _, _ = io.Copy(up, brw); _ = up.Close() }() // the test only checks bytes arrive
		_, _ = io.Copy(down, up)                                // the test only checks bytes arrive
		_ = down.Close()                                        // the tunnel is over either way
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestDialThroughProxy(t *testing.T) {
	srv := echoServer(t)
	var tunnels atomic.Int32
	proxy := connectProxy(t, &tunnels)
	proxyURL, _ := url.Parse(proxy.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, wsURL(srv), Options{Proxy: http.ProxyURL(proxyURL)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	roundTrip(t, c)
	if tunnels.Load() != 1 {
		t.Fatalf("proxy opened %d tunnels, want 1", tunnels.Load())
	}
}

func TestUnsupportedProxyIsAnError(t *testing.T) {
	srv := echoServer(t)
	socks, _ := url.Parse("socks5://127.0.0.1:1")
	_, err := Dial(context.Background(), wsURL(srv), Options{Proxy: http.ProxyURL(socks)})
	if err == nil || !strings.Contains(err.Error(), "only http://") {
		t.Fatalf("want an unsupported-proxy error, got %v", err)
	}
}

func TestReadMessageRefusesOversize(t *testing.T) {
	srv := echoServer(t)
	c, err := Dial(context.Background(), wsURL(srv), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.WriteMessage(make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadMessage(999); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
}

// A write blocked on a peer that never reads must not hold up Close: that is what lets a dead link
// be dropped while its writer is stuck.
func TestCloseDoesNotWaitForAStuckWrite(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	c := &Conn{raw: a}
	go func() { _ = c.WriteMessage(make([]byte, 1<<16)) }() // blocks: nobody reads b
	time.Sleep(50 * time.Millisecond)
	done := make(chan struct{})
	go func() { _ = c.Close(); close(done) }() // only the return matters here
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close waited for a stuck write")
	}
}
