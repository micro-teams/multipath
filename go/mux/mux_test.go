// Tests for the mux over an in-memory transport that feeds each side's frames to the other in order,
// one goroutine per direction: stream round trips, concurrency, independence, reset, credit on both
// the sending and the receiving side, the stream limit, id rules, and splicing to a TCP target.
package mux

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/micro-teams/multipath/go/protocol"
)

// pipe is one direction of the in-memory transport; sends after close fail like a dead session.
type pipe struct {
	mu     sync.Mutex
	ch     chan []byte
	closed bool
}

func newPipe() *pipe { return &pipe{ch: make(chan []byte, 1<<14)} }

func (p *pipe) Send(f []byte, _ bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return io.ErrClosedPipe
	}
	p.ch <- append([]byte(nil), f...)
	return nil
}

func (p *pipe) close() {
	p.mu.Lock()
	p.closed = true
	close(p.ch)
	p.mu.Unlock()
}

func pump(t *testing.T, p *pipe, to *Mux, errs chan<- error) {
	for f := range p.ch {
		if err := to.Feed(f); err != nil {
			t.Errorf("feed: %v", err)
			errs <- err
			return
		}
	}
}

func testLimits() protocol.Limits {
	l := protocol.DefaultLimits()
	l.StreamWindow = 64 << 10
	l.MaxStreams = 64
	return l
}

// pair returns a client mux and a server mux whose accepted streams arrive on the channel.
func pair(t *testing.T, lim protocol.Limits) (*Mux, chan *Stream, chan error) {
	c2s, s2c := newPipe(), newPipe()
	accepted := make(chan *Stream, 128)
	errs := make(chan error, 2)
	cli := New(c2s, true, lim, nil)
	srv := New(s2c, false, lim, func(st *Stream) { accepted <- st })
	go pump(t, c2s, srv, errs)
	go pump(t, s2c, cli, errs)
	t.Cleanup(func() { c2s.close(); s2c.close(); cli.Close("test over"); srv.Close("test over") })
	return cli, accepted, errs
}

func echo(st *Stream) { _, _ = io.Copy(st, st); _ = st.Close() } // the test checks what the client reads

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b) // crypto/rand does not fail on supported platforms
	return b
}

func TestSingleStreamRoundTrip(t *testing.T) {
	cli, accepted, _ := pair(t, testLimits())
	go func() { echo(<-accepted) }()
	st, err := cli.Open()
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("hello multiplexed world")
	if _, err := st.Write(msg); err != nil {
		t.Fatal(err)
	}
	_ = st.Close() // the echo below shows whether the FIN arrived
	if got, _ := io.ReadAll(st); !bytes.Equal(got, msg) {
		t.Fatalf("echo %q", got)
	}
}

func TestManyConcurrentStreams(t *testing.T) {
	cli, accepted, _ := pair(t, testLimits())
	go func() {
		for st := range accepted {
			go echo(st)
		}
	}()
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := cli.Open()
			if err != nil {
				errs <- err
				return
			}
			payload := append([]byte(fmt.Sprintf("stream-%d:", i)), randBytes(200<<10)...)
			go func() { _, _ = st.Write(payload); _ = st.Close() }() // a failed write shows as a mismatch
			if got, _ := io.ReadAll(st); !bytes.Equal(got, payload) {
				errs <- fmt.Errorf("stream %d: %d of %d bytes", i, len(got), len(payload))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestStreamsAreIndependent(t *testing.T) {
	cli, accepted, _ := pair(t, testLimits())
	a, _ := cli.Open()
	b, _ := cli.Open()
	sa, sb := <-accepted, <-accepted
	// a's peer never reads; b must still flow.
	go func() { _, _ = a.Write(randBytes(1 << 20)) }() // blocks on credit, which is the point
	buf := make([]byte, 4)
	for range 100 {
		if _, err := b.Write([]byte("on-b")); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(sb, buf); err != nil || string(buf) != "on-b" {
			t.Fatalf("read b: %q %v", buf, err)
		}
	}
	_ = sa // a's peer never reads, which is the point
}

func TestResetReachesThePeerWithItsReason(t *testing.T) {
	cli, accepted, _ := pair(t, testLimits())
	st, _ := cli.Open()
	peer := <-accepted
	_ = st.Reset("going away") // the peer's read below checks it arrived
	if _, err := peer.Read(make([]byte, 8)); err == nil || !strings.Contains(err.Error(), "going away") {
		t.Fatalf("peer read after reset: %v", err)
	}
}

func TestWriterWaitsForCredit(t *testing.T) {
	cli, accepted, _ := pair(t, testLimits())
	st, _ := cli.Open()
	peer := <-accepted
	wrote := make(chan int, 1)
	go func() { n, _ := st.Write(randBytes(1 << 20)); wrote <- n }() // n is checked below
	time.Sleep(100 * time.Millisecond)
	select {
	case n := <-wrote:
		t.Fatalf("write of %d bytes completed while the peer read nothing", n)
	default:
	}
	go func() { _, _ = io.Copy(io.Discard, peer) }() // drains so the writer can finish
	select {
	case <-wrote:
	case <-time.After(5 * time.Second):
		t.Fatal("write never completed after the peer drained")
	}
}

// A sender that ignores credit is caught by the receiver.
func TestReceiverEnforcesCredit(t *testing.T) {
	srv := New(newPipe(), false, testLimits(), func(*Stream) {})
	if err := srv.Feed(protocol.EncodeMux(protocol.MuxSyn, 1, nil)); err != nil {
		t.Fatal(err)
	}
	chunk := protocol.EncodeMux(protocol.MuxData, 1, make([]byte, protocol.MaxMuxData))
	var err error
	for i := 0; err == nil && i < 10; i++ {
		err = srv.Feed(chunk)
	}
	if err == nil || !strings.Contains(err.Error(), "credit") {
		t.Fatalf("want a credit violation, got %v", err)
	}
}

func TestStreamLimitResetsTheExtraStream(t *testing.T) {
	lim := testLimits()
	lim.MaxStreams = 2
	out := newPipe()
	srv := New(out, false, lim, func(*Stream) {})
	for id := uint32(1); id <= 5; id += 2 {
		if err := srv.Feed(protocol.EncodeMux(protocol.MuxSyn, id, nil)); err != nil {
			t.Fatal(err)
		}
	}
	f, _, _ := protocol.ParseMux(<-out.ch)
	if f.Type != protocol.MuxRst || f.Stream != 5 || string(f.Body) != "too many streams" {
		t.Fatalf("got %+v", f)
	}
}

func TestStreamIDRules(t *testing.T) {
	for name, feed := range map[string][]uint32{"even id": {2}, "id not increasing": {3, 1}, "id repeated": {1, 1}} {
		srv := New(newPipe(), false, testLimits(), func(*Stream) {})
		var err error
		for _, id := range feed {
			if err == nil {
				err = srv.Feed(protocol.EncodeMux(protocol.MuxSyn, id, nil))
			}
		}
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	cli := New(newPipe(), true, testLimits(), nil)
	if err := cli.Feed(protocol.EncodeMux(protocol.MuxSyn, 1, nil)); err == nil {
		t.Error("client accepted a syn")
	}
}

func TestFeedAcceptsAnySplit(t *testing.T) {
	got := make(chan *Stream, 1)
	srv := New(newPipe(), false, testLimits(), func(st *Stream) { got <- st })
	all := append(protocol.EncodeMux(protocol.MuxSyn, 1, nil), protocol.EncodeMux(protocol.MuxData, 1, []byte("abc"))...)
	all = append(all, protocol.EncodeMux(0x7f, 1, []byte("unknown type"))...)
	for _, b := range all {
		if err := srv.Feed([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	st := <-got
	buf := make([]byte, 3)
	if _, err := io.ReadFull(st, buf); err != nil || string(buf) != "abc" {
		t.Fatalf("read %q %v", buf, err)
	}
}

func TestCloseFailsEveryStream(t *testing.T) {
	cli, _, _ := pair(t, testLimits())
	st, _ := cli.Open()
	cli.Close("all links are dead")
	if _, err := st.Read(make([]byte, 1)); err == nil || !strings.Contains(err.Error(), "all links are dead") {
		t.Fatalf("read after close: %v", err)
	}
	if _, err := cli.Open(); err == nil {
		t.Fatal("open after close succeeded")
	}
}

// Splice carries a request and its answer between a stream and a TCP target, with half-close.
func TestSpliceToTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		req, _ := io.ReadAll(c)                        // the reply below shows what arrived
		_, _ = c.Write(append([]byte("got "), req...)) // the client checks the answer
		_ = c.Close()                                  // the client checks the answer
	}()
	cli, accepted, _ := pair(t, testLimits())
	spliced := make(chan error, 1)
	go func() {
		st := <-accepted
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			spliced <- err
			return
		}
		spliced <- Splice(st, c)
	}()
	st, _ := cli.Open()
	_, _ = st.Write([]byte("request")) // the answer below checks it arrived
	_ = st.Close()                     // the answer below checks it arrived
	got, _ := io.ReadAll(st)
	if string(got) != "got request" {
		t.Fatalf("got %q", got)
	}
	if err := <-spliced; err != nil {
		t.Fatalf("splice: %v", err)
	}
	if n := len(cli.Streams()); n != 0 {
		t.Fatalf("%d streams left open", n)
	}
}
