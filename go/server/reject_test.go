package multipath

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func dialTCP(addr string) func(context.Context, int) (io.ReadWriteCloser, error) {
	return func(ctx context.Context, i int) (io.ReadWriteCloser, error) {
		return net.Dial("tcp", addr)
	}
}

// TestRejectOutOfRangeLinkReportedNotRetried: a link whose HELLO index the origin refuses is reported
// down with the origin's reason (not a bare "connection closed") and is not retried, while an
// in-range link on the same stream stays up.
func TestRejectOutOfRangeLinkReportedNotRetried(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	srv := Listen(ln, fastOpts(1, nil)) // origin accepts index 0 only
	go func() { _, _ = srv.Accept() }()

	var mu sync.Mutex
	var events []LinkState
	copt := fastOpts(2, dialTCP(ln.Addr().String())) // client offers index 0 and 1; 1 is out of range
	copt.OnLinkState = func(e LinkState) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	cli, err := DialRedundant(context.Background(), copt)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			if e.Index == 1 && !e.Up && strings.Contains(e.Reason, "out of range") {
				return true
			}
		}
		return false
	}, "link 1 reported down with the origin's out-of-range reason")

	// Link 0 stays up; link 1 stays rejected and is not retried (no reconnect churn).
	waitFor(t, func() bool {
		for _, s := range cli.Stats() {
			if s.Index == 0 && s.State == "up" {
				return true
			}
		}
		return false
	}, "link 0 up")
	time.Sleep(200 * time.Millisecond)
	if r := cli.Stats()[1].Reconnects; r != 0 {
		t.Fatalf("rejected link 1 was retried %d times; it must not be", r)
	}
}

// TestRejectAllLinksFailsFast reproduces the expensive bug: the origin refuses every link, Dial still
// returns success (it only wrote HELLO), and without the fix every request hangs forever with no log.
// With it, the stream closes with the origin's reason so the next read/write fails fast.
func TestRejectAllLinksFailsFast(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_ = Listen(ln, RedundantOptions{N: 0}) // origin accepts no index at all → rejects every link

	cli, err := DialRedundant(context.Background(), fastOpts(1, dialTCP(ln.Addr().String())))
	if err != nil {
		t.Fatalf("dial should still succeed (HELLO written): %v", err)
	}
	defer cli.Close()

	done := make(chan error, 1)
	go func() {
		_, e := cli.Read(make([]byte, 16))
		done <- e
	}()
	select {
	case e := <-done:
		if e == nil || !strings.Contains(e.Error(), "rejected") {
			t.Fatalf("want a reject error, got %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read hung instead of failing fast when every link was rejected")
	}
}
