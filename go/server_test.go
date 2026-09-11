package multipath

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// tcpEcho starts an echo server and returns its address.
func tcpEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c); c.Close() }()
		}
	}()
	return ln.Addr().String()
}

// httpHello starts an HTTP server that greets by path, and returns its address.
func httpHello(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello %s", r.URL.Path)
	})
	go http.Serve(ln, mux)
	return ln.Addr().String()
}

// dialClientOverN brings up a client whose N lines all point at origin over plain tcp — enough to
// exercise the redundant/mux/header/splice path end to end.
func dialClientOverN(t *testing.T, origin string, n int) *Client {
	t.Helper()
	lines := make([]Line, n)
	for i := range lines {
		lines[i] = Line{ID: fmt.Sprintf("l%d", i), URL: "http://" + origin, Transport: string(TransportTCP)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, ClientOptions{Lines: lines})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// startOrigin runs a multipath origin routing normal streams to app and tunnels to a fixed echo.
func startOrigin(t *testing.T, appAddr, echoAddr string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	router := Router(
		DialLocal(appAddr),
		func(target string, ticket []byte) (net.Conn, error) { return net.Dial("tcp", echoAddr) },
	)
	go Serve(ln, ServerOptions{}, router)
	return ln.Addr().String()
}

func TestSubstrateTunnel(t *testing.T) {
	echo := tcpEcho(t)
	origin := startOrigin(t, "", echo)
	c := dialClientOverN(t, origin, 3)

	st, err := c.OpenTunnel("anything:0", []byte("ticket"))
	if err != nil {
		t.Fatalf("OpenTunnel: %v", err)
	}
	msg := []byte("redundant tunnel bytes, deduped by offset")
	if _, err := st.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(st, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(msg) {
		t.Fatalf("tunnel echo mismatch: %q", got)
	}
}

func TestSubstrateHTTPRoundTrip(t *testing.T) {
	app := httpHello(t)
	origin := startOrigin(t, app, "")
	c := dialClientOverN(t, origin, 2)

	req, _ := http.NewRequest(http.MethodGet, "http://origin/greetings", nil)
	resp, err := c.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "hello /greetings" {
		t.Fatalf("unexpected response %d %q", resp.StatusCode, body)
	}
}
