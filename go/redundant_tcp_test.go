package multipath

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A fault-injecting TCP middlebox sits between the Go client and the (here, Go) server, one instance
// per accepted link. It reproduces exactly the cut modes asked for: a black-hole (the TCP connection
// stays open but bytes in one or both directions are silently discarded — "sent but not received"),
// a one-directional cut, and a hard disconnect. Each accepted link runs its own random schedule and
// always returns to healthy (or closes, prompting the client to reconnect a fresh link), so the
// redundant stream is continuously disrupted yet must still deliver every byte, in order, as long as
// at least one link is healthy at any moment.

type linkGov struct {
	dropCS atomic.Bool // drop client->server
	dropSC atomic.Bool // drop server->client
	closed atomic.Bool
}

type middlebox struct {
	ln     net.Listener
	target string
	rng    *rand.Rand
	rmu    sync.Mutex
}

func newMiddlebox(t *testing.T, target string, seed int64) *middlebox {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("middlebox listen: %v", err)
	}
	m := &middlebox{ln: ln, target: target, rng: rand.New(rand.NewSource(seed))}
	go m.loop()
	return m
}

func (m *middlebox) addr() string { return m.ln.Addr().String() }
func (m *middlebox) close()       { _ = m.ln.Close() }

func (m *middlebox) randDur(lo, hi int) time.Duration {
	m.rmu.Lock()
	d := lo + m.rng.Intn(hi-lo+1)
	m.rmu.Unlock()
	return time.Duration(d) * time.Millisecond
}
func (m *middlebox) randIntn(n int) int {
	m.rmu.Lock()
	v := m.rng.Intn(n)
	m.rmu.Unlock()
	return v
}

func (m *middlebox) loop() {
	for {
		client, err := m.ln.Accept()
		if err != nil {
			return
		}
		go m.serveLink(client)
	}
}

func (m *middlebox) serveLink(client net.Conn) {
	server, err := net.Dial("tcp", m.target)
	if err != nil {
		_ = client.Close()
		return
	}
	g := &linkGov{}
	done := make(chan struct{})
	var once sync.Once
	stop := func() {
		once.Do(func() {
			g.closed.Store(true)
			_ = client.Close()
			_ = server.Close()
			close(done)
		})
	}
	go m.govern(g, stop, done)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); m.pump(client, server, &g.dropCS, g); stop() }()
	go func() { defer wg.Done(); m.pump(server, client, &g.dropSC, g); stop() }()
	wg.Wait()
}

// pump copies src->dst, but while drop is set it reads and DISCARDS — the connection stays alive so
// the sender sees no error, but the bytes never arrive. That is the black-hole.
func (m *middlebox) pump(src, dst net.Conn, drop *atomic.Bool, g *linkGov) {
	buf := make([]byte, 32*1024)
	for {
		if g.closed.Load() {
			return
		}
		_ = src.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		n, err := src.Read(buf)
		if n > 0 && !drop.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
	}
}

// govern runs a random fault schedule for one link, always ending each fault back at healthy; it may
// also hard-close the link, which prompts the client to reconnect a fresh one.
func (m *middlebox) govern(g *linkGov, stop func(), done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(m.randDur(3, 15)):
		}
		switch m.randIntn(5) {
		case 0, 1: // black-hole both directions briefly
			g.dropCS.Store(true)
			g.dropSC.Store(true)
			m.sleepOrDone(done, m.randDur(3, 18))
			g.dropCS.Store(false)
			g.dropSC.Store(false)
		case 2: // one-directional cut (client->server)
			g.dropCS.Store(true)
			m.sleepOrDone(done, m.randDur(3, 18))
			g.dropCS.Store(false)
		case 3: // one-directional cut (server->client)
			g.dropSC.Store(true)
			m.sleepOrDone(done, m.randDur(3, 18))
			g.dropSC.Store(false)
		case 4: // hard disconnect
			stop()
			return
		}
	}
}

func (m *middlebox) sleepOrDone(done <-chan struct{}, d time.Duration) {
	select {
	case <-done:
	case <-time.After(d):
	}
}

// TestE2ETCPWithFaultMiddlebox is the real-socket proof: a Go client and a Go server (the same
// server the JVM port will replace) exchange a payload over N TCP links, each passing through a
// middlebox that randomly black-holes, one-way-cuts, and disconnects it. The delivered bytes must
// equal the sent bytes exactly.
func TestE2ETCPWithFaultMiddlebox(t *testing.T) {
	const n = 4
	// Real server.
	srvLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("server listen: %v", err)
	}
	defer srvLn.Close()
	acc := Listen(srvLn, RedundantOptions{
		N: n, Window: 2 << 20,
		PingInterval: 20 * time.Millisecond, DeadAfter: 80 * time.Millisecond,
		AckInterval: 8 * time.Millisecond,
	})

	// One middlebox per link, each fronting the real server.
	boxes := make([]*middlebox, n)
	for i := 0; i < n; i++ {
		boxes[i] = newMiddlebox(t, srvLn.Addr().String(), int64(1000+i))
		defer boxes[i].close()
	}

	// Client dials link i through middlebox i.
	cli, err := DialRedundant(context.Background(), RedundantOptions{
		N: n, Window: 2 << 20,
		PingInterval: 20 * time.Millisecond, DeadAfter: 80 * time.Millisecond,
		AckInterval:    8 * time.Millisecond,
		ReconnectDelay: 5 * time.Millisecond, MaxDelay: 40 * time.Millisecond,
		Dial: func(ctx context.Context, i int) (io.ReadWriteCloser, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", boxes[i].addr())
		},
	})
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer cli.Close()

	srv, err := acc.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer srv.Close()

	msg := randBytes(512 << 10)
	go func() {
		_, _ = cli.Write(msg)
	}()

	got := make([]byte, 0, len(msg))
	buf := make([]byte, 64*1024)
	deadline := time.Now().Add(30 * time.Second)
	for len(got) < len(msg) {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: got %d/%d bytes under fault injection", len(got), len(msg))
		}
		m, rerr := srv.Read(buf)
		if m > 0 {
			got = append(got, buf[:m]...)
		}
		if rerr != nil && len(got) < len(msg) {
			t.Fatalf("read err at %d/%d: %v", len(got), len(msg), rerr)
		}
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("payload mismatch under fault injection")
	}
}
