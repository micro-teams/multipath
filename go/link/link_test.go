package multipath

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// echoAccept accepts one link off ln (doing the ws handshake for ws lines) and echoes bytes back,
// so a DialLink round trip proves the chosen encapsulation carries an opaque byte stream intact.
func echoAccept(t *testing.T, ln net.Listener, ws bool) {
	t.Helper()
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		conn := raw
		if ws {
			conn, err = acceptWebSocket(raw, defaultLinkPath)
			if err != nil {
				return
			}
		}
		_, _ = io.Copy(conn, conn)
		_ = conn.Close()
	}()
}

func lineFor(ln net.Listener, transport Transport) Line {
	return Line{ID: "t", URL: "http://" + ln.Addr().String(), Transport: string(transport)}
}

func roundTrip(t *testing.T, transport Transport, ws bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	echoAccept(t, ln, ws)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := DialLink(ctx, lineFor(ln, transport), LinkOptions{DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("DialLink(%s): %v", transport, err)
	}
	defer conn.Close()

	// A payload longer than a 125-byte single-byte length, to exercise the 16-bit length path.
	want := []byte(fmt.Sprintf("multipath link over %s — %s", transport, make([]byte, 200)))
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("echo mismatch over %s", transport)
	}
}

func TestDialLinkTCP(t *testing.T) { roundTrip(t, TransportTCP, false) }
func TestDialLinkWS(t *testing.T)  { roundTrip(t, TransportWS, true) }

func TestResolveTransportInference(t *testing.T) {
	cases := map[string]Transport{
		"https://x.example":     TransportTLS,
		"http://x.example:8080": TransportTCP,
	}
	for url, want := range cases {
		got, err := resolveTransport(Line{ID: "t", URL: url})
		if err != nil {
			t.Fatalf("%s: %v", url, err)
		}
		if got != want {
			t.Fatalf("%s: inferred %s, want %s", url, got, want)
		}
	}
}
