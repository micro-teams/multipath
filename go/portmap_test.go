package multipath

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// startEchoTarget starts a plain TCP echo server and returns its address.
func startEchoTarget(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { io.Copy(c, c); c.Close() }(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// TestPortMapForwardsToTarget wires a client Forward (local listener -> mux streams) to a server
// Serve(ForwardTo(echo target)) over an in-memory session pair, then drives many concurrent local
// connections and checks each round-trips through the forward.
func TestPortMapForwardsToTarget(t *testing.T) {
	target := startEchoTarget(t)

	ta, tb := newPipePair()
	cliSess := NewClientSession(ta)
	srvSess := NewServerSession(tb)
	defer cliSess.Close()
	defer srvSess.Close()

	// Server: every accepted stream is spliced to a fresh dial of the echo target.
	go Serve(srvSess, ForwardTo(func() (net.Conn, error) {
		return net.Dial("tcp", target)
	}))

	// Client: a local listener whose connections are forwarded over the session.
	local, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("local listen: %v", err)
	}
	defer local.Close()
	go Forward(local, cliSess)

	const n = 25
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.Dial("tcp", local.Addr().String())
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			msg := append([]byte(fmt.Sprintf("conn-%d:", i)), randBytes(8*1024)...)
			if _, err := c.Write(msg); err != nil {
				errs <- err
				return
			}
			got := make([]byte, len(msg))
			if _, err := io.ReadFull(c, got); err != nil {
				errs <- fmt.Errorf("conn %d read: %v", i, err)
				return
			}
			if !bytes.Equal(got, msg) {
				errs <- fmt.Errorf("conn %d echo mismatch", i)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("port-map forward timed out")
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestPortMapCustomHandler shows the non-forward case (what ccproxy's MITM handler will be): the
// server handler consumes the stream directly instead of dialing a target.
func TestPortMapCustomHandler(t *testing.T) {
	ta, tb := newPipePair()
	cliSess := NewClientSession(ta)
	srvSess := NewServerSession(tb)
	defer cliSess.Close()
	defer srvSess.Close()

	// Handler: read all, reply with the byte count as text, close.
	go Serve(srvSess, func(st *MuxStream) {
		data, _ := io.ReadAll(st)
		fmt.Fprintf(st, "got %d bytes", len(data))
		st.Close()
	})

	local, _ := net.Listen("tcp", "127.0.0.1:0")
	defer local.Close()
	go Forward(local, cliSess)

	c, err := net.Dial("tcp", local.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	payload := randBytes(5000)
	c.Write(payload)
	c.(*net.TCPConn).CloseWrite() // signal EOF so the handler's ReadAll returns
	got, _ := io.ReadAll(c)
	want := fmt.Sprintf("got %d bytes", len(payload))
	if string(got) != want {
		t.Fatalf("custom handler: got %q want %q", got, want)
	}
}
