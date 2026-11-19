package multipath

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// pipeTransport is an in-memory io.ReadWriteCloser pair for testing the mux without a network.
type pipeTransport struct {
	rd *io.PipeReader
	wr *io.PipeWriter
}

func newPipePair() (a, b *pipeTransport) {
	ar, bw := io.Pipe()
	br, aw := io.Pipe()
	return &pipeTransport{rd: ar, wr: aw}, &pipeTransport{rd: br, wr: bw}
}

func (t *pipeTransport) Read(p []byte) (int, error)  { return t.rd.Read(p) }
func (t *pipeTransport) Write(p []byte) (int, error) { return t.wr.Write(p) }
func (t *pipeTransport) Close() error                { t.rd.Close(); return t.wr.Close() }

func muxPair(t *testing.T) (*Session, *Session) {
	t.Helper()
	a, b := newPipePair()
	return NewClientSession(a), NewServerSession(b)
}

func TestMuxSingleStreamRoundTrip(t *testing.T) {
	cli, srv := muxPair(t)
	defer cli.Close()
	defer srv.Close()

	go func() {
		st, err := srv.AcceptStream()
		if err != nil {
			return
		}
		io.Copy(st, st) // echo
		st.Close()
	}()

	st, err := cli.OpenStream()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	msg := []byte("hello multiplexed world")
	if _, err := st.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	st.Close() // half-close so the echo copy ends
	got, _ := io.ReadAll(st)
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo mismatch: %q", got)
	}
}

func TestMuxManyConcurrentStreams(t *testing.T) {
	cli, srv := muxPair(t)
	defer cli.Close()
	defer srv.Close()

	// Server echoes every accepted stream.
	go func() {
		for {
			st, err := srv.AcceptStream()
			if err != nil {
				return
			}
			go func(st *MuxStream) {
				io.Copy(st, st)
				st.Close()
			}(st)
		}
	}()

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := cli.OpenStream()
			if err != nil {
				errs <- err
				return
			}
			// A distinct, larger-than-one-chunk payload per stream, so interleaving and flow
			// control are actually exercised.
			payload := []byte(fmt.Sprintf("stream-%d:", i))
			payload = append(payload, randBytes(40*1024)...)
			go func() { st.Write(payload); st.Close() }()
			got, _ := io.ReadAll(st)
			if !bytes.Equal(got, payload) {
				errs <- fmt.Errorf("stream %d mismatch (%d bytes)", i, len(got))
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestMuxStreamsAreIndependent(t *testing.T) {
	cli, srv := muxPair(t)
	defer cli.Close()
	defer srv.Close()

	accepted := make(chan *MuxStream, 2)
	go func() {
		for {
			st, err := srv.AcceptStream()
			if err != nil {
				return
			}
			accepted <- st
		}
	}()

	a, _ := cli.OpenStream()
	b, _ := cli.OpenStream()
	sa := <-accepted
	sb := <-accepted

	// Write on b, read on b — a stays idle and must not interfere.
	if _, err := b.Write([]byte("on-b")); err != nil {
		t.Fatalf("write b: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(sb, buf); err != nil || string(buf) != "on-b" {
		t.Fatalf("read sb: %q %v", buf, err)
	}
	// Now a.
	if _, err := a.Write([]byte("on-a")); err != nil {
		t.Fatalf("write a: %v", err)
	}
	if _, err := io.ReadFull(sa, buf); err != nil || string(buf) != "on-a" {
		t.Fatalf("read sa: %q %v", buf, err)
	}
	_ = a
	_ = b
}

func TestMuxReset(t *testing.T) {
	cli, srv := muxPair(t)
	defer cli.Close()
	defer srv.Close()

	accepted := make(chan *MuxStream, 1)
	go func() {
		st, err := srv.AcceptStream()
		if err == nil {
			accepted <- st
		}
	}()
	st, _ := cli.OpenStream()
	peer := <-accepted
	st.Reset()
	// The peer's next read must fail (reset), not hang.
	done := make(chan error, 1)
	go func() { _, err := peer.Read(make([]byte, 8)); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected error after reset")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("read did not unblock after reset")
	}
}

func TestMuxFlowControlBounded(t *testing.T) {
	cli, srv := muxPair(t)
	defer cli.Close()
	defer srv.Close()

	accepted := make(chan *MuxStream, 1)
	go func() {
		st, err := srv.AcceptStream()
		if err == nil {
			accepted <- st
		}
	}()
	st, _ := cli.OpenStream()
	peer := <-accepted

	// The writer must not be able to push more than one window ahead of a non-reading peer.
	// Write a lot in the background; with the peer not reading, Write should block after ~1 window.
	wrote := make(chan int, 1)
	go func() {
		big := randBytes(4 << 20)
		n, _ := st.Write(big)
		wrote <- n
	}()
	time.Sleep(200 * time.Millisecond)
	select {
	case n := <-wrote:
		t.Fatalf("write completed (%d) despite peer not reading — no backpressure", n)
	default:
		// good: still blocked
	}
	// Drain the peer; the write should now complete.
	go io.Copy(io.Discard, peer)
	select {
	case <-wrote:
	case <-time.After(5 * time.Second):
		t.Fatalf("write never completed after peer drained")
	}
}

// TestMuxOverRedundantTCPUnderFaults is the integration proof: run a mux Session over a real
// RedundantStream (many TCP links through the fault middlebox), open many concurrent streams, and
// require every stream's echo to be byte-exact despite black-hole / one-way / disconnect cuts.
func TestMuxOverRedundantTCPUnderFaults(t *testing.T) {
	const nLinks = 3
	srvLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer srvLn.Close()
	acc := Listen(srvLn, RedundantOptions{
		N: nLinks, Window: 2 << 20,
		PingInterval: 20 * time.Millisecond, DeadAfter: 80 * time.Millisecond, AckInterval: 8 * time.Millisecond,
	})
	boxes := make([]*middlebox, nLinks)
	for i := 0; i < nLinks; i++ {
		boxes[i] = newMiddlebox(t, srvLn.Addr().String(), int64(5000+i))
		defer boxes[i].close()
	}

	cliRS, err := DialRedundant(context.Background(), RedundantOptions{
		N: nLinks, Window: 2 << 20,
		PingInterval: 20 * time.Millisecond, DeadAfter: 80 * time.Millisecond, AckInterval: 8 * time.Millisecond,
		ReconnectDelay: 5 * time.Millisecond, MaxDelay: 40 * time.Millisecond,
		Dial: func(ctx context.Context, i int) (io.ReadWriteCloser, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", boxes[i].addr())
		},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cliRS.Close()
	srvRS, err := acc.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer srvRS.Close()

	cliSess := NewClientSession(cliRS)
	srvSess := NewServerSession(srvRS)
	defer cliSess.Close()
	defer srvSess.Close()

	// Server: echo every stream.
	go func() {
		for {
			st, err := srvSess.AcceptStream()
			if err != nil {
				return
			}
			go func(st *MuxStream) { io.Copy(st, st); st.Close() }(st)
		}
	}()

	const nStreams = 20
	var wg sync.WaitGroup
	errs := make(chan error, nStreams)
	for i := 0; i < nStreams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := cliSess.OpenStream()
			if err != nil {
				errs <- err
				return
			}
			payload := append([]byte(fmt.Sprintf("s%02d:", i)), randBytes(64*1024)...)
			go func() { st.Write(payload); st.Close() }()
			got, _ := io.ReadAll(st)
			if !bytes.Equal(got, payload) {
				errs <- fmt.Errorf("stream %d mismatch: got %d/%d bytes", i, len(got), len(payload))
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("mux-over-redundant under faults timed out")
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

var _ = rand.Read
