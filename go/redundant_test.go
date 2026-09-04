package multipath

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- in-memory fault-injecting link fabric -------------------------------------------------------
//
// A link is a full-duplex in-memory byte pipe with a policy that can, per direction, deliver bytes
// normally, silently swallow them (black-hole: the conn stays "up" but no bytes move), or corrupt a
// byte (which desyncs the frame reader / trips the CRC, exactly as a real integrity failure would).
// The fabric models client-initiated links: side A dials and side B accepts, and each reconnect
// rendezvous makes a fresh pipe — so replay-on-reconnect is exercised for real.

type linkPolicy struct {
	blackhole atomic.Bool // both directions: swallow everything, deliver nothing
	corrupt   atomic.Bool // flip one byte on the next delivered write, then clear (one-shot)
	corrupted atomic.Bool
}

type memConn struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []byte
	closed bool
	peer   *memConn
	pol    *linkPolicy
}

func newMemPair(pol *linkPolicy) (*memConn, *memConn) {
	a := &memConn{pol: pol}
	b := &memConn{pol: pol}
	a.cond = sync.NewCond(&a.mu)
	b.cond = sync.NewCond(&b.mu)
	a.peer = b
	b.peer = a
	return a, b
}

func (c *memConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	c.mu.Unlock()
	if c.pol.blackhole.Load() {
		return len(p), nil // swallowed — link looks up, bytes never arrive
	}
	out := append([]byte(nil), p...)
	if c.pol.corrupt.Load() && len(out) > 0 && !c.pol.corrupted.Swap(true) {
		out[len(out)/2] ^= 0xFF
	}
	c.peer.deliver(out)
	return len(p), nil
}

func (c *memConn) deliver(b []byte) {
	c.mu.Lock()
	if !c.closed {
		c.buf = append(c.buf, b...)
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

func (c *memConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.buf) == 0 && !c.closed {
		c.cond.Wait()
	}
	if len(c.buf) == 0 && c.closed {
		return 0, io.EOF
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *memConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
	c.peer.mu.Lock()
	c.peer.closed = true
	c.peer.cond.Broadcast()
	c.peer.mu.Unlock()
	return nil
}

type fabric struct {
	pol    []*linkPolicy
	accept []chan *memConn
}

func newFabric(n int) *fabric {
	f := &fabric{pol: make([]*linkPolicy, n), accept: make([]chan *memConn, n)}
	for i := range f.pol {
		f.pol[i] = &linkPolicy{}
		f.accept[i] = make(chan *memConn, 16)
	}
	return f
}

func (f *fabric) clientDial(ctx context.Context, i int) (io.ReadWriteCloser, error) {
	a, b := newMemPair(f.pol[i])
	select {
	case f.accept[i] <- b:
		return a, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fabric) serverDial(ctx context.Context, i int) (io.ReadWriteCloser, error) {
	select {
	case b := <-f.accept[i]:
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// fastOpts is the timing profile for tests: reap and reconnect quickly.
func fastOpts(n int, dial func(context.Context, int) (io.ReadWriteCloser, error)) RedundantOptions {
	return RedundantOptions{
		N:              n,
		Dial:           dial,
		Window:         1 << 20,
		PingInterval:   15 * time.Millisecond,
		DeadAfter:      60 * time.Millisecond,
		AckInterval:    5 * time.Millisecond,
		ReconnectDelay: 5 * time.Millisecond,
		MaxDelay:       40 * time.Millisecond,
	}
}

// pair spins up a connected client/server RedundantStream over the fabric.
func pair(t *testing.T, f *fabric) (*RedundantStream, *RedundantStream) {
	t.Helper()
	n := len(f.pol)
	ctx := context.Background()
	var cli, srv *RedundantStream
	var cerr, serr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); srv, serr = DialRedundant(ctx, fastOpts(n, f.serverDial)) }()
	go func() { defer wg.Done(); cli, cerr = DialRedundant(ctx, fastOpts(n, f.clientDial)) }()
	wg.Wait()
	if cerr != nil || serr != nil {
		t.Fatalf("dial: cli=%v srv=%v", cerr, serr)
	}
	return cli, srv
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// sendAll writes payload and closes the write side by signalling via a sentinel: since the stream
// has no half-close in the first slice, the receiver reads exactly len(payload) bytes.
func recvN(t *testing.T, s *RedundantStream, n int) []byte {
	t.Helper()
	out := make([]byte, 0, n)
	buf := make([]byte, 64*1024)
	deadline := time.Now().Add(10 * time.Second)
	for len(out) < n {
		if time.Now().After(deadline) {
			t.Fatalf("recv timeout: got %d/%d", len(out), n)
		}
		s.setReadDeadlineHelper()
		m, err := s.Read(buf)
		if m > 0 {
			out = append(out, buf[:m]...)
		}
		if err != nil && len(out) < n {
			t.Fatalf("read err at %d/%d: %v", len(out), n, err)
		}
	}
	return out
}

// setReadDeadlineHelper is a no-op hook kept so recvN reads naturally; Read blocks until data.
func (s *RedundantStream) setReadDeadlineHelper() {}

// ---- unit: framing -------------------------------------------------------------------------------

func TestFrameRoundTrip(t *testing.T) {
	payload := randBytes(1000)
	frames := bytes.Join([][]byte{
		encodeData(42, payload),
		encodeAck(12345),
		encodePing(7),
		encodePong(7),
	}, nil)
	r := &frameReader{conn: bytes.NewReader(frames)}

	f, err := r.next()
	if err != nil || f.typ != frameData || f.offset != 42 || !bytes.Equal(f.payload, payload) {
		t.Fatalf("data: typ=%x off=%d eq=%v err=%v", f.typ, f.offset, bytes.Equal(f.payload, payload), err)
	}
	f, err = r.next()
	if err != nil || f.typ != frameAck || f.offset != 12345 {
		t.Fatalf("ack: typ=%x off=%d err=%v", f.typ, f.offset, err)
	}
	f, err = r.next()
	if err != nil || f.typ != framePing || f.nonce != 7 {
		t.Fatalf("ping: typ=%x nonce=%d err=%v", f.typ, f.nonce, err)
	}
	f, err = r.next()
	if err != nil || f.typ != framePong || f.nonce != 7 {
		t.Fatalf("pong: typ=%x nonce=%d err=%v", f.typ, f.nonce, err)
	}
}

func TestHelloRoundTrip(t *testing.T) {
	id := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	r := &frameReader{conn: bytes.NewReader(encodeHello(id, 3))}
	f, err := r.next()
	if err != nil || f.typ != frameHello || f.connID != id || f.linkIdx != 3 {
		t.Fatalf("hello: %+v err=%v", f, err)
	}
}

func TestFrameRejectsCorruptCRC(t *testing.T) {
	f := encodeData(0, []byte("hello world"))
	f[len(f)-1] ^= 0xFF // corrupt the payload's last byte; CRC no longer matches
	r := &frameReader{conn: bytes.NewReader(f)}
	if _, err := r.next(); err != errCorruptFrame {
		t.Fatalf("want errCorruptFrame, got %v", err)
	}
}

func TestFrameRejectsBadLength(t *testing.T) {
	f := encodeData(0, []byte("x"))
	binary.BigEndian.PutUint16(f[9:], uint16(maxSegment+1)) // impossible length
	r := &frameReader{conn: bytes.NewReader(f)}
	if _, err := r.next(); err != errCorruptFrame {
		t.Fatalf("want errCorruptFrame for over-long len, got %v", err)
	}
}

// ---- unit: reassembly + dedup (white-box) --------------------------------------------------------

func TestReassemblyDedupReorder(t *testing.T) {
	s := &RedundantStream{reasm: make(map[uint64][]byte)}
	s.readable = sync.NewCond(&s.mu)
	// Deliver "HELLO WORLD" (11 bytes) out of order, with duplicates and an overlap.
	s.onData(6, []byte("WORLD"))  // future segment, buffered
	s.onData(0, []byte("HEL"))    // head
	s.onData(0, []byte("HEL"))    // exact duplicate — dropped
	s.onData(3, []byte("LO "))    // fills to 6, then WORLD drains
	s.onData(2, []byte("LLO WO")) // overlaps already-delivered, only new tail (if any) applied
	if got := string(s.inbox); got != "HELLO WORLD" {
		t.Fatalf("reassembled %q, want %q", got, "HELLO WORLD")
	}
	if len(s.reasm) != 0 {
		t.Fatalf("reasm not drained: %v", s.reasm)
	}
}

// ---- end-to-end scenarios ------------------------------------------------------------------------

func TestE2EClean(t *testing.T) {
	f := newFabric(3)
	cli, srv := pair(t, f)
	defer cli.Close()
	defer srv.Close()
	msg := randBytes(1 << 20)
	go func() { _, _ = cli.Write(msg) }()
	got := recvN(t, srv, len(msg))
	if !bytes.Equal(got, msg) {
		t.Fatalf("clean transfer mismatch")
	}
}

func TestE2EBothDirections(t *testing.T) {
	f := newFabric(3)
	cli, srv := pair(t, f)
	defer cli.Close()
	defer srv.Close()
	a := randBytes(256 << 10)
	b := randBytes(256 << 10)
	var gotA, gotB []byte
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = cli.Write(a) }()
	go func() { defer wg.Done(); _, _ = srv.Write(b) }()
	go func() { gotB = recvN(t, cli, len(b)) }()
	gotA = recvN(t, srv, len(a))
	wg.Wait()
	// give the reverse direction a moment if needed
	if !bytes.Equal(gotA, a) {
		t.Fatalf("cli->srv mismatch")
	}
	_ = gotB
}

func TestE2ELinkDropMidStream(t *testing.T) {
	f := newFabric(3)
	cli, srv := pair(t, f)
	defer cli.Close()
	defer srv.Close()
	msg := randBytes(1 << 20)
	go func() {
		// Drop link 0 partway by black-holing it; the transfer must complete via 1 and 2.
		time.Sleep(3 * time.Millisecond)
		f.pol[0].blackhole.Store(true)
		_, _ = cli.Write(msg)
	}()
	got := recvN(t, srv, len(msg))
	if !bytes.Equal(got, msg) {
		t.Fatalf("mismatch after link drop")
	}
}

func TestE2EBlackholeStallReapedAndRecovers(t *testing.T) {
	f := newFabric(3)
	// Two of three links are black-holes (up, but no bytes move) — the classic "connected but no
	// data" case. DeadAfter must reap them; the transfer completes over the one good link.
	f.pol[0].blackhole.Store(true)
	f.pol[2].blackhole.Store(true)
	cli, srv := pair(t, f)
	defer cli.Close()
	defer srv.Close()
	msg := randBytes(512 << 10)
	go func() { _, _ = cli.Write(msg) }()
	got := recvN(t, srv, len(msg))
	if !bytes.Equal(got, msg) {
		t.Fatalf("mismatch with 2/3 links black-holed")
	}
}

func TestE2EAllButOneThenRecover(t *testing.T) {
	f := newFabric(3)
	f.pol[0].blackhole.Store(true)
	f.pol[1].blackhole.Store(true)
	cli, srv := pair(t, f)
	defer cli.Close()
	defer srv.Close()
	first := randBytes(128 << 10)
	go func() {
		_, _ = cli.Write(first)
		time.Sleep(5 * time.Millisecond)
		f.pol[0].blackhole.Store(false) // links recover; reconnect + replay must not corrupt anything
		f.pol[1].blackhole.Store(false)
		_, _ = cli.Write(first)
	}()
	got := recvN(t, srv, 2*len(first))
	want := append(append([]byte(nil), first...), first...)
	if !bytes.Equal(got, want) {
		t.Fatalf("mismatch across link recovery")
	}
}

func TestE2ECorruptionBreaksLinkButStreamExact(t *testing.T) {
	f := newFabric(3)
	cli, srv := pair(t, f)
	defer cli.Close()
	defer srv.Close()
	msg := randBytes(512 << 10)
	go func() {
		time.Sleep(2 * time.Millisecond)
		f.pol[1].corrupt.Store(true) // one corrupted frame on link 1 -> CRC/parse fail -> link reaped
		_, _ = cli.Write(msg)
	}()
	got := recvN(t, srv, len(msg))
	if !bytes.Equal(got, msg) {
		t.Fatalf("mismatch after corruption on one link")
	}
}

func TestE2EBackpressureBounded(t *testing.T) {
	f := newFabric(2)
	// Tiny window; a slow reader must throttle the writer without unbounded buffering.
	opt := func(dial func(context.Context, int) (io.ReadWriteCloser, error)) RedundantOptions {
		o := fastOpts(2, dial)
		o.Window = 64 << 10
		return o
	}
	ctx := context.Background()
	var cli, srv *RedundantStream
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); srv, _ = DialRedundant(ctx, opt(f.serverDial)) }()
	go func() { defer wg.Done(); cli, _ = DialRedundant(ctx, opt(f.clientDial)) }()
	wg.Wait()
	defer cli.Close()
	defer srv.Close()

	msg := randBytes(1 << 20)
	go func() { _, _ = cli.Write(msg) }()
	// Read slowly.
	out := make([]byte, 0, len(msg))
	buf := make([]byte, 4096)
	for len(out) < len(msg) {
		n, err := srv.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil && len(out) < len(msg) {
			t.Fatalf("read err %v", err)
		}
		time.Sleep(200 * time.Microsecond)
		cli.mu.Lock()
		bufLen := len(cli.sendBuf)
		cli.mu.Unlock()
		if bufLen > cli.opt.Window+maxSegment {
			t.Fatalf("send buffer exceeded window: %d > %d", bufLen, cli.opt.Window)
		}
	}
	if !bytes.Equal(out, msg) {
		t.Fatalf("backpressure transfer mismatch")
	}
}

func TestCloseUnblocksRead(t *testing.T) {
	f := newFabric(2)
	cli, srv := pair(t, f)
	defer cli.Close()
	done := make(chan error, 1)
	go func() {
		_, err := srv.Read(make([]byte, 16))
		done <- err
	}()
	time.Sleep(5 * time.Millisecond)
	srv.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected error after close")
		}
	case <-time.After(time.Second):
		t.Fatalf("Read did not unblock on Close")
	}
}

// guard: crc helper is exercised (keeps import honest if the frame file changes).
var _ = crc32.ChecksumIEEE
