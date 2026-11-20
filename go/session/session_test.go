// Tests for the redundant session over an in-memory fault-injecting link fabric. A fabric link is a
// pair of message pipes with a policy that can, while set, swallow every message (black-hole), stall
// writes like a peer that stopped reading, corrupt one DATA frame, or drop only the ACKs flowing to
// the client. A harness redials a link whenever the client side reports it down, so replay on
// reconnect is exercised for real. Timings are about a quarter of production, with room for the
// race detector on a small machine.
package session

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/micro-teams/multipath/go/protocol"
)

type policy struct {
	blackhole atomic.Bool
	stall     atomic.Bool // writes block until their deadline
	corrupt   atomic.Bool // corrupt the next DATA frame, once
	dropAcks  atomic.Bool // swallow ACKs written by the server side
}

type memConn struct {
	mu     sync.Mutex
	cond   *sync.Cond
	msgs   [][]byte
	closed bool
	wdl    time.Time
	peer   *memConn
	pol    *policy
	server bool
}

func memPair(pol *policy) (client, server *memConn) {
	client, server = &memConn{pol: pol}, &memConn{pol: pol, server: true}
	client.cond, server.cond = sync.NewCond(&client.mu), sync.NewCond(&server.mu)
	client.peer, server.peer = server, client
	return
}

func (c *memConn) WriteMessage(p []byte) error {
	for c.pol.stall.Load() {
		c.mu.Lock()
		closed, dl := c.closed, c.wdl
		c.mu.Unlock()
		if closed {
			return io.ErrClosedPipe
		}
		if !dl.IsZero() && time.Now().After(dl) {
			return os.ErrDeadlineExceeded
		}
		time.Sleep(time.Millisecond)
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	if c.pol.blackhole.Load() || (c.server && p[0] == protocol.Ack && c.pol.dropAcks.Load()) {
		return nil
	}
	out := append([]byte(nil), p...)
	if out[0] == protocol.Data && c.pol.corrupt.CompareAndSwap(true, false) {
		out[len(out)-1] ^= 0xff
	}
	c.peer.mu.Lock()
	if !c.peer.closed {
		c.peer.msgs = append(c.peer.msgs, out)
		c.peer.cond.Broadcast()
	}
	c.peer.mu.Unlock()
	return nil
}

func (c *memConn) ReadMessage(max int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.msgs) == 0 && !c.closed {
		c.cond.Wait()
	}
	if c.closed {
		return nil, io.EOF
	}
	m := c.msgs[0]
	c.msgs = c.msgs[1:]
	if len(m) > max {
		return nil, errors.New("message too large")
	}
	return m, nil
}

func (c *memConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.wdl = t
	c.mu.Unlock()
	return nil
}

func (c *memConn) Close() error {
	for _, e := range []*memConn{c, c.peer} {
		e.mu.Lock()
		e.closed = true
		e.cond.Broadcast()
		e.mu.Unlock()
	}
	return nil
}

// sink collects delivered bytes.
type sink struct {
	mu   sync.Mutex
	cond *sync.Cond
	buf  []byte
}

func newSink() *sink { k := &sink{}; k.cond = sync.NewCond(&k.mu); return k }

func (k *sink) deliver(p []byte) error {
	k.mu.Lock()
	k.buf = append(k.buf, p...)
	k.cond.Broadcast()
	k.mu.Unlock()
	return nil
}

// await waits until n bytes arrived or the timeout passes, and returns what arrived.
func (k *sink) await(n int, timeout time.Duration) []byte {
	deadline := time.Now().Add(timeout)
	go func() { time.Sleep(timeout); k.mu.Lock(); k.cond.Broadcast(); k.mu.Unlock() }()
	k.mu.Lock()
	defer k.mu.Unlock()
	for len(k.buf) < n && time.Now().Before(deadline) {
		k.cond.Wait()
	}
	return append([]byte(nil), k.buf...)
}

func testLimits() protocol.Limits {
	l := protocol.DefaultLimits()
	l.Window = 1 << 20
	l.Ping = 25 * time.Millisecond
	l.ClientDead = 150 * time.Millisecond
	l.ServerDead = 400 * time.Millisecond
	l.Progress = 250 * time.Millisecond
	return l
}

type harness struct {
	t        *testing.T
	clock    *Clock
	pol      []*policy
	cli, srv *Session
	cin, sin *sink
	ended    chan string
	redial   atomic.Bool
}

// newHarness connects a client and a server session over n fabric links.
func newHarness(t *testing.T, n int, lim protocol.Limits) *harness {
	h := &harness{t: t, clock: NewClock(5 * time.Millisecond), cin: newSink(), sin: newSink(), ended: make(chan string, 2)}
	h.redial.Store(true)
	t.Cleanup(h.clock.Stop)
	for range n {
		h.pol = append(h.pol, &policy{})
	}
	h.cli = New(Options{Limits: lim, Client: true, Clock: h.clock, Deliver: h.cin.deliver,
		OnLink: func(i int, up bool, _ string) {
			if !up && h.redial.Load() {
				go func() { time.Sleep(5 * time.Millisecond); h.connect(i) }()
			}
		},
		OnEnd: func(why string) { h.ended <- "client: " + why }})
	h.srv = New(Options{Limits: lim, Clock: h.clock, Deliver: h.sin.deliver,
		OnEnd: func(why string) { h.ended <- "server: " + why }})
	for i := range n {
		h.connect(i)
	}
	t.Cleanup(func() { h.redial.Store(false); h.cli.End("test over"); h.srv.End("test over") })
	return h
}

// connect attaches a fresh pipe as link i on both sides.
func (h *harness) connect(i int) {
	a, b := memPair(h.pol[i])
	if h.srv.Attach(i, b) != nil || h.cli.Attach(i, a) != nil {
		a.Close()
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b) // crypto/rand does not fail on supported platforms
	return b
}

// send writes p in MaxData pieces, as the mux does.
func send(s *Session, p []byte) error {
	for len(p) > 0 {
		n := min(len(p), protocol.MaxData)
		if err := s.Send(p[:n], false); err != nil {
			return err
		}
		p = p[n:]
	}
	return nil
}

func expect(t *testing.T, k *sink, want []byte) {
	t.Helper()
	if got := k.await(len(want), 30*time.Second); !bytes.Equal(got, want) {
		t.Fatalf("received %d of %d bytes, equal=%v", len(got), len(want), bytes.Equal(got, want[:min(len(got), len(want))]))
	}
}

func TestReorderDedup(t *testing.T) {
	k := newSink()
	s := &Session{opt: Options{Deliver: k.deliver}, lim: testLimits(), reorder: map[uint64][]byte{}}
	s.writable = sync.NewCond(&s.mu)
	for _, f := range []struct {
		off  uint64
		data string
	}{{6, "WORLD"}, {0, "HEL"}, {0, "HEL"}, {3, "LO "}, {2, "LLO WO"}} {
		if err := s.onData(f.off, []byte(f.data)); err != nil {
			t.Fatal(err)
		}
	}
	if got := string(k.buf); got != "HELLO WORLD" || len(s.reorder) != 0 || s.reorderBytes != 0 {
		t.Fatalf("delivered %q, reorder %v (%d bytes)", got, s.reorder, s.reorderBytes)
	}
}

func TestDataBeyondWindowIsAViolation(t *testing.T) {
	s := &Session{opt: Options{Deliver: newSink().deliver}, lim: testLimits(), reorder: map[uint64][]byte{}}
	if err := s.onData(uint64(testLimits().Window+protocol.ControlSlack), []byte("x")); err == nil || err.session {
		t.Fatalf("want a link violation, got %v", err)
	}
}

func TestCleanTransfer(t *testing.T) {
	h := newHarness(t, 3, testLimits())
	msg := randBytes(4 << 20)
	go func() { _ = send(h.cli, msg) }() // the receiving side checks what arrived
	expect(t, h.sin, msg)
}

func TestBothDirections(t *testing.T) {
	h := newHarness(t, 3, testLimits())
	a, b := randBytes(1<<20), randBytes(1<<20)
	go func() { _ = send(h.cli, a) }() // the receiving side checks what arrived
	go func() { _ = send(h.srv, b) }() // the receiving side checks what arrived
	expect(t, h.sin, a)
	expect(t, h.cin, b)
}

func TestBlackholedLinksAreReaped(t *testing.T) {
	h := newHarness(t, 3, testLimits())
	h.pol[0].blackhole.Store(true)
	h.pol[2].blackhole.Store(true)
	msg := randBytes(1 << 20)
	go func() { _ = send(h.cli, msg) }() // the receiving side checks what arrived
	expect(t, h.sin, msg)
}

func TestLinksRecoverAndReplay(t *testing.T) {
	h := newHarness(t, 3, testLimits())
	h.pol[0].blackhole.Store(true)
	h.pol[1].blackhole.Store(true)
	first := randBytes(256 << 10)
	go func() {
		_ = send(h.cli, first) // the receiving side checks what arrived
		time.Sleep(20 * time.Millisecond)
		h.pol[0].blackhole.Store(false)
		h.pol[1].blackhole.Store(false)
		_ = send(h.cli, first) // the receiving side checks what arrived
	}()
	expect(t, h.sin, append(append([]byte(nil), first...), first...))
}

func TestCorruptFrameDropsOnlyItsLink(t *testing.T) {
	h := newHarness(t, 3, testLimits())
	h.pol[1].corrupt.Store(true)
	msg := randBytes(1 << 20)
	go func() { _ = send(h.cli, msg) }() // the receiving side checks what arrived
	expect(t, h.sin, msg)
}

// A link whose peer stopped reading must not slow the others: its writer is stuck, its queue fills,
// and it is closed, while the transfer completes over the healthy links.
func TestStalledLinkDoesNotBlockOthers(t *testing.T) {
	h := newHarness(t, 2, testLimits())
	h.redial.Store(false)
	h.pol[0].stall.Store(true)
	msg := randBytes(4 << 20)
	start := time.Now()
	go func() { _ = send(h.cli, msg) }() // the receiving side checks what arrived
	expect(t, h.sin, msg)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("transfer took %v with one stalled link", d)
	}
}

// Many reconnects during a heavy transfer: the replay of each reattached link must precede any newer
// frame, and a frame that arrives ahead of a gap must wait, not vanish.
func TestManyReconnectsUnderLoad(t *testing.T) {
	h := newHarness(t, 2, testLimits())
	msg := randBytes(8 << 20)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(3 * time.Millisecond):
			}
			// Only a link whose sibling is up is dropped: losing both would end the session.
			h.cli.mu.Lock()
			l, other := h.cli.links[i%2], h.cli.links[(i+1)%2]
			h.cli.mu.Unlock()
			if l != nil && other != nil {
				h.cli.drop(l, "test drop")
			}
		}
	}()
	go func() { _ = send(h.cli, msg) }() // the receiving side checks what arrived
	if got := h.sin.await(len(msg), 30*time.Second); !bytes.Equal(got, msg) {
		t.Fatalf("received %d of %d bytes; client ended %q, server ended %q", len(got), len(msg), h.cli.Reason(), h.srv.Reason())
	}
}

// The first frames on a link are the ACK and then the replay from the acknowledged offset, even
// with a sender writing concurrently.
func TestAttachQueuesReplayFirst(t *testing.T) {
	clock := NewClock(time.Hour)
	defer clock.Stop()
	s := New(Options{Limits: testLimits(), Client: true, Clock: clock, Deliver: newSink().deliver})
	defer s.End("test over")
	if err := send(s, randBytes(100<<10)); err != nil {
		t.Fatal(err)
	}
	a, b := memPair(&policy{})
	go func() { _ = send(s, randBytes(100<<10)) }() // the receiving side checks what arrived
	if err := s.Attach(0, a); err != nil {
		t.Fatal(err)
	}
	m, _ := b.ReadMessage(1 << 16)
	if m[0] != protocol.Ack {
		t.Fatalf("first frame 0x%02x, want ACK", m[0])
	}
	next := uint64(0)
	for next < 200<<10 {
		m, err := b.ReadMessage(1 << 16)
		if err != nil {
			t.Fatal(err)
		}
		_, p, _ := protocol.ParseFrame(m, 1<<16)
		off, data, err := protocol.DecodeData(p)
		if err != nil || off != next {
			t.Fatalf("frame at %d, want %d (%v)", off, next, err)
		}
		next += uint64(len(data))
	}
}

func TestSendWaitsForWindowAndEndReleasesIt(t *testing.T) {
	s := New(Options{Limits: testLimits(), Deliver: newSink().deliver, Clock: NewClock(time.Hour)})
	if err := send(s, randBytes(testLimits().Window)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Send([]byte("x"), false) }()
	select {
	case err := <-done:
		t.Fatalf("send past the window returned %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := s.Send([]byte("control"), true); err != nil {
		t.Fatalf("control send waited or failed: %v", err)
	}
	s.End("test over")
	if err := <-done; !errors.Is(err, ErrEnded) {
		t.Fatalf("blocked send returned %v, want ErrEnded", err)
	}
}

// When every link dies at once, both sides end the session within their dead times.
func TestAllLinksDeadEndsBothSides(t *testing.T) {
	h := newHarness(t, 2, testLimits())
	h.redial.Store(false)
	for _, p := range h.pol {
		p.blackhole.Store(true)
	}
	for range 2 {
		select {
		case why := <-h.ended:
			t.Log(why)
		case <-time.After(2 * time.Second):
			t.Fatal("a side did not end the session")
		}
	}
	if err := h.cli.Send([]byte("x"), false); !errors.Is(err, ErrEnded) {
		t.Fatalf("send after the end returned %v", err)
	}
}

func TestGoodbyeEndsThePeer(t *testing.T) {
	h := newHarness(t, 2, testLimits())
	h.redial.Store(false)
	h.cli.Goodbye("client shutting down")
	for range 2 {
		select {
		case why := <-h.ended:
			t.Log(why)
		case <-time.After(time.Second):
			t.Fatal("goodbye did not end both sides")
		}
	}
}

// ACKs that never come back on a link, while pings are answered, trip the progress rule.
func TestProgressRuleDropsALinkWithoutAcks(t *testing.T) {
	h := newHarness(t, 1, testLimits())
	h.redial.Store(false)
	h.pol[0].dropAcks.Store(true)
	start := time.Now()
	if err := send(h.cli, []byte("needs an ack")); err != nil {
		t.Fatal(err)
	}
	select {
	case why := <-h.ended:
		if d := time.Since(start); d > 1500*time.Millisecond {
			t.Fatalf("ended after %v: %s", d, why)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("link without acks survived")
	}
}

func TestStatsCountsLinksAndBytes(t *testing.T) {
	h := newHarness(t, 3, testLimits())
	msg := randBytes(64 << 10)
	go func() { _ = send(h.cli, msg) }() // the receiving side checks what arrived
	expect(t, h.sin, msg)
	st := h.cli.Stats()
	if len(st.Links) != 3 || st.SendNext != uint64(len(msg)) {
		t.Fatalf("stats %+v", st)
	}
	// Every delivered byte left on some link; a link may skip what a faster one already delivered.
	var out int64
	for _, l := range st.Links {
		out += l.BytesOut
		if time.Since(l.LastByte) > time.Second {
			t.Fatalf("link stats %+v", l)
		}
	}
	if out < int64(len(msg)) {
		t.Fatalf("links wrote %d bytes for %d delivered", out, len(msg))
	}
}
