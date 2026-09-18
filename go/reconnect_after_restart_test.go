package multipath

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// TestHello2ReconnectOfUnknownConnIDIsRejected: an origin that has never seen a connID must refuse a
// HELLO2 that claims to be a reconnect of it (reconnect=true) instead of silently accepting it as a
// new stream. This is the origin-restart case: the origin's connID table is gone, but a surviving
// client still believes its stream is the same one it always had.
func TestHello2ReconnectOfUnknownConnIDIsRejected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	srv := Listen(ln, fastOpts(1, nil))
	accepted := make(chan struct{}, 1)
	go func() {
		if _, err := srv.Accept(); err == nil {
			accepted <- struct{}{}
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var connID [16]byte
	_, _ = rand.Read(connID[:])
	if _, err := conn.Write(encodeHello2(connID, 0, true)); err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := &frameReader{conn: conn}
	f, err := r.next()
	if err != nil {
		t.Fatalf("reading the origin's response: %v", err)
	}
	if f.typ != frameReject {
		t.Fatalf("want REJECT for a reconnect of an unknown connID, got frame type %d", f.typ)
	}
	if !strings.Contains(f.reason, "unknown connID") {
		t.Fatalf("want a reason naming the unknown connID, got %q", f.reason)
	}

	select {
	case <-accepted:
		t.Fatal("origin must not have surfaced a new logical stream for a rejected reconnect")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestHello2FreshConnIDStillAccepted: the companion case — a HELLO2 that honestly claims
// reconnect=false for a connID the origin has never seen is accepted exactly like a legacy HELLO
// always was. The new behavior in onLink must not affect a stream's first-ever attach.
func TestHello2FreshConnIDStillAccepted(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	srv := Listen(ln, fastOpts(1, nil))
	accepted := make(chan struct{}, 1)
	go func() {
		if _, err := srv.Accept(); err == nil {
			accepted <- struct{}{}
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var connID [16]byte
	_, _ = rand.Read(connID[:])
	if _, err := conn.Write(encodeHello2(connID, 0, false)); err != nil {
		t.Fatal(err)
	}

	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("a fresh connID's HELLO2 must still be accepted as a new logical stream")
	}
}

// TestClientSelfHealsWhenOriginForgetsConnID is the end-to-end shape of the production incident: a
// client's stream is fully established against an origin, that origin process is replaced by a
// fresh one holding no memory of any connID (simulated here by starting a brand-new Acceptor after
// closing the first), and the client's reconnect must surface as an error the caller can act on —
// not a silent, permanently wedged stream that only a whole-process restart (`link retest`) fixes.
func TestClientSelfHealsWhenOriginForgetsConnID(t *testing.T) {
	addr := "127.0.0.1:0"
	ln1, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	realAddr := ln1.Addr().String()
	srv1 := Listen(ln1, fastOpts(1, nil))
	go func() { _, _ = srv1.Accept() }()

	copt := fastOpts(1, dialTCP(realAddr))
	cli, err := DialRedundant(context.Background(), copt)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	waitFor(t, func() bool {
		for _, s := range cli.Stats() {
			if s.Index == 0 && s.State == "up" {
				return true
			}
		}
		return false
	}, "the first link established against the original origin")
	// "up" fires the instant the TCP dial + HELLO write succeed — before the origin's very first
	// reply has necessarily been read. established only flips on that reply (see its doc: it must
	// not race ahead of the origin actually recording the connID), so wait for it explicitly rather
	// than assuming "up" already implies it.
	waitFor(t, func() bool {
		return cli.established.Load()
	}, "the origin's first reply confirming the connID (a PING/PONG round trip)")

	// "Restart" the origin: close the old listener and its stream table, and stand up a brand-new
	// Acceptor that has never heard of this client's connID. Reusing the port (not a fresh Listen on
	// ":0") is what makes this the origin-restart shape rather than the network-partition shape
	// TestRejectOutOfRangeLinkReportedNotRetried already covers. Closing the Listener alone does not
	// touch sockets it already accepted (that is a real OS process exit's job, which this test
	// cannot do), so the established link is killed by hand right after.
	_ = ln1.Close()
	cli.mu.Lock()
	if l := cli.links[0]; l != nil {
		_ = l.conn.Close()
	}
	cli.mu.Unlock()
	ln2, err := net.Listen("tcp", realAddr)
	if err != nil {
		t.Skipf("could not rebind %s immediately after close: %v", realAddr, err)
	}
	defer ln2.Close()
	srv2 := Listen(ln2, fastOpts(1, nil))
	go func() { _, _ = srv2.Accept() }()

	done := make(chan error, 1)
	go func() {
		_, e := cli.Read(make([]byte, 16))
		done <- e
	}()
	select {
	case e := <-done:
		if e == nil || !strings.Contains(e.Error(), "rejected") {
			t.Fatalf("want the stream to fail fast with a rejected-by-origin error, got %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream hung instead of failing fast once the origin no longer recognized its connID")
	}
}

// TestClientSelfHealsWhenOriginForgetsConnIDWithALineDown is the SAME incident as the test above,
// with the one detail production had and that one lacks: a second line that is down and stays down.
//
// T-092 shipped with the test above passing and the product still wedging, and the whole difference
// is N. At N=1 a single REJECT is trivially "every link rejected"; at N=2 with a dead line, the
// origin's verdict on the stream can only ever reach ONE slot, because the dead line never gets far
// enough to be told anything. Recovery that waits for the dead line's opinion waits forever: no
// error reaches the caller, nothing is logged, and the machine sits offline until somebody runs
// `microteams link retest` by hand. That is exactly what ops saw on the clean re-test, on a
// connector that already carried the T-092 fix.
func TestClientSelfHealsWhenOriginForgetsConnIDWithALineDown(t *testing.T) {
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	realAddr := ln1.Addr().String()
	srv1 := Listen(ln1, fastOpts(2, nil))
	go func() { _, _ = srv1.Accept() }()

	// Line 1 is the one that is down: its dial never succeeds, so it never writes a HELLO, is never
	// rejected, and never has an opinion to contribute.
	copt := fastOpts(2, func(ctx context.Context, i int) (io.ReadWriteCloser, error) {
		if i == 1 {
			return nil, errors.New("line down")
		}
		return net.Dial("tcp", realAddr)
	})
	cli, err := DialRedundant(context.Background(), copt)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	waitFor(t, func() bool {
		for _, s := range cli.Stats() {
			if s.Index == 0 && s.State == "up" {
				return true
			}
		}
		return false
	}, "line 0 established against the original origin")
	waitFor(t, func() bool {
		return cli.established.Load()
	}, "the origin's first reply confirming the connID")

	// Restart the origin, as in the test above: a new Acceptor on the same port with no memory of
	// any connID, and the established link killed by hand because closing a Listener does not touch
	// sockets it already accepted.
	_ = ln1.Close()
	cli.mu.Lock()
	if l := cli.links[0]; l != nil {
		_ = l.conn.Close()
	}
	cli.mu.Unlock()
	ln2, err := net.Listen("tcp", realAddr)
	if err != nil {
		t.Skipf("could not rebind %s immediately after close: %v", realAddr, err)
	}
	defer ln2.Close()
	srv2 := Listen(ln2, fastOpts(2, nil))
	go func() { _, _ = srv2.Accept() }()

	done := make(chan error, 1)
	go func() {
		_, e := cli.Read(make([]byte, 16))
		done <- e
	}()
	select {
	case e := <-done:
		if e == nil || !strings.Contains(e.Error(), "rejected") {
			t.Fatalf("want the stream to fail fast with a rejected-by-origin error, got %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream hung: a down line's missing verdict must not hold the whole stream open")
	}
}
