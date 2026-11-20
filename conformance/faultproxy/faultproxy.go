// Package faultproxy is a TCP proxy that stands for one network line and misbehaves on demand, so
// the conformance scenarios can put the real client and server through what real lines do. Every
// accepted connection is forwarded to one target; its behaviour in each direction follows the
// line's current Fault, which a scenario can change for existing connections, new ones, or both.
//
// The faults are the ones a line actually shows:
//
//   - Pass, with optional one-way Delay, random extra Jitter (bytes keep their order), and a
//     bandwidth cap BPS: a slow or distant line.
//   - Loss: TCP never loses bytes without breaking the connection, so loss shows above TCP as
//     retransmission pauses; with probability LossRate a chunk waits LossPause first.
//   - Stall: the proxy stops reading in both directions, so the sender's TCP buffer fills and its
//     writes block, as when a peer or a middlebox stops reading.
//   - Blackhole: bytes are read and discarded in both directions; the connection stays open, even
//     when one side closes it, so the other side learns of it only by silence.
//   - OneWay: like Blackhole for one direction only (Dir "up", client to server, or "down").
//   - Refuse: new connections are closed at once. Drop (the method) closes existing connections.
//
// API:
//
//   - Listen(target) starts a line on a free loopback port; (*Line).Addr() is its address.
//
//   - (*Line).Set(f, scope) applies a Fault to existing connections (ScopeExisting), new ones
//     (ScopeNew), or both (ScopeAll).
//
//   - (*Line).Drop() hard-disconnects every existing connection; it returns how many.
//
//   - (*Line).Conns() is the number of open connections; (*Line).Close() stops the line.
//
//     ln, _ := faultproxy.Listen("127.0.0.1:8080")
//     defer ln.Close()
//     ln.Set(faultproxy.Fault{Mode: faultproxy.Pass, Delay: 100 * time.Millisecond}, faultproxy.ScopeAll)
//     ln.Set(faultproxy.Fault{Mode: faultproxy.Stall}, faultproxy.ScopeExisting)
package faultproxy

import (
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Mode is what a line does with the bytes of a connection.
type Mode int

// The modes; see the package comment.
const (
	Pass Mode = iota
	Loss
	Stall
	Blackhole
	OneWay
	Refuse
)

// Fault is a line's behaviour.
type Fault struct {
	Mode      Mode
	Delay     time.Duration // one-way delay of every chunk (Pass and Loss)
	Jitter    time.Duration // random extra delay up to this much (Pass and Loss)
	BPS       int           // bandwidth cap per direction in bytes per second, 0 for none
	LossRate  float64       // Loss: probability that a chunk pauses
	LossPause time.Duration // Loss: how long it pauses
	Dir       string        // OneWay: "up" (client to server) or "down" is discarded
}

// Scope says which connections a Set applies to.
type Scope int

// The scopes.
const (
	ScopeAll Scope = iota
	ScopeExisting
	ScopeNew
)

// Line is one proxied line.
type Line struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	def   Fault
	conns map[*conn]struct{}
}

type conn struct {
	a, b   net.Conn // a is the client side, b the target side
	fault  atomic.Pointer[Fault]
	closed atomic.Bool
}

// Listen starts a line forwarding to target.
func Listen(target string) (*Line, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	l := &Line{ln: ln, target: target, conns: map[*conn]struct{}{}}
	go l.serve()
	return l, nil
}

// Addr returns the line's listening address.
func (l *Line) Addr() string { return l.ln.Addr().String() }

// Close stops the line and closes its connections.
func (l *Line) Close() {
	_ = l.ln.Close() // a second close reports an error that means nothing here
	l.Drop()
}

// Set applies f to the connections scope names.
func (l *Line) Set(f Fault, scope Scope) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if scope != ScopeExisting {
		l.def = f
	}
	if scope != ScopeNew {
		for c := range l.conns {
			fc := f
			c.fault.Store(&fc)
		}
	}
}

// Drop closes every existing connection and returns how many there were.
func (l *Line) Drop() int {
	l.mu.Lock()
	victims := make([]*conn, 0, len(l.conns))
	for c := range l.conns {
		victims = append(victims, c)
	}
	l.mu.Unlock()
	for _, c := range victims {
		c.close()
	}
	return len(victims)
}

// Conns returns the number of open connections.
func (l *Line) Conns() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.conns)
}

func (c *conn) close() {
	if c.closed.CompareAndSwap(false, true) {
		_ = c.a.Close() // the proxy is cutting the connection on purpose
		_ = c.b.Close() // the proxy is cutting the connection on purpose
	}
}

func (l *Line) serve() {
	for {
		a, err := l.ln.Accept()
		if err != nil {
			return
		}
		l.mu.Lock()
		f := l.def
		l.mu.Unlock()
		if f.Mode == Refuse {
			_ = a.Close() // refusing is the fault being simulated
			continue
		}
		b, err := net.Dial("tcp", l.target)
		if err != nil {
			_ = a.Close() // the target is down; the client sees what a dead line looks like
			continue
		}
		c := &conn{a: a, b: b}
		c.fault.Store(&f)
		l.mu.Lock()
		l.conns[c] = struct{}{}
		l.mu.Unlock()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.pump(a, b, "up") }()
		go func() { defer wg.Done(); c.pump(b, a, "down") }()
		go func() {
			wg.Wait()
			l.mu.Lock()
			delete(l.conns, c)
			l.mu.Unlock()
		}()
	}
}

type chunk struct {
	due  time.Time
	data []byte
}

// pump copies src to dst under the connection's fault. Reading and writing are split so that delay
// does not slow down reading, while a stalled or slow writer does, through a small queue.
func (c *conn) pump(src, dst net.Conn, dir string) {
	defer c.close()
	q := make(chan chunk, 64)
	go func() {
		defer c.close()
		var last time.Time
		for ch := range q {
			if d := time.Until(ch.due); d > 0 {
				time.Sleep(d)
			}
			for c.fault.Load().Mode == Stall && !c.closed.Load() {
				time.Sleep(10 * time.Millisecond)
			}
			if f := c.fault.Load(); f.BPS > 0 {
				// Pace to the cap from the end of the previous write.
				next := last.Add(time.Duration(int64(len(ch.data)) * int64(time.Second) / int64(f.BPS)))
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				}
				last = time.Now()
			}
			if _, err := dst.Write(ch.data); err != nil {
				return
			}
		}
	}()
	defer close(q)
	buf := make([]byte, 16*1024)
	var due time.Time
	for {
		for c.fault.Load().Mode == Stall && !c.closed.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		n, err := src.Read(buf)
		if n > 0 {
			f := c.fault.Load()
			if f.Mode == Blackhole || (f.Mode == OneWay && f.Dir == dir) {
				continue
			}
			wait := f.Delay
			if f.Jitter > 0 {
				wait += time.Duration(rand.Int64N(int64(f.Jitter)))
			}
			if f.Mode == Loss && rand.Float64() < f.LossRate {
				wait += f.LossPause
			}
			// Bytes keep their order: a chunk is never due before the one ahead of it.
			due = maxTime(due, time.Now().Add(wait))
			q <- chunk{due: due, data: append([]byte(nil), buf[:n]...)}
		}
		if err != nil {
			// A black hole swallows the end of a connection too: the other side must find out by
			// silence, as it would on a real network.
			for f := c.fault.Load(); (f.Mode == Blackhole || (f.Mode == OneWay && f.Dir == dir)) && !c.closed.Load(); f = c.fault.Load() {
				time.Sleep(20 * time.Millisecond)
			}
			return // the connection ended on this side; the deferred close ends the other
		}
	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
