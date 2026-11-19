// Attack scenarios against the real server (PROTOCOL.md sections 8 and 10): hand-made links that
// never read, send hostile lengths, connect and vanish by the thousand, or fill every link slot of a
// session during a large download. Each asserts the resource principle: once the attack stops, the
// server is back to its baseline, and while it runs, goroutines and memory stay within a bound set
// by the number of connections.
package conformance

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/micro-teams/multipath/go/link"
	"github.com/micro-teams/multipath/go/protocol"
)

func (s *stack) linkURL(line int) string {
	return "ws://" + s.lines[line].Addr() + "/multipath/v3/link"
}

// attacker is a hand-driven client link: it performs the handshake and then writes whatever frames
// the scenario wants.
type attacker struct {
	c   *link.Conn
	off uint64 // next session offset
}

func attack(t *testing.T, url string, key [16]byte, index uint8, mode byte) (*attacker, protocol.RejectFrame, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := link.Dial(ctx, url, link.Options{})
	if err != nil {
		return nil, protocol.RejectFrame{}, err
	}
	if err := c.WriteMessage(protocol.EncodeHello(protocol.HelloFrame{Version: protocol.Version, Session: key, Link: index, Mode: mode})); err != nil {
		return nil, protocol.RejectFrame{}, err
	}
	msg, err := c.ReadMessage(1 << 17)
	if err != nil {
		return nil, protocol.RejectFrame{}, err
	}
	if msg[0] == protocol.Reject {
		r, err := protocol.DecodeReject(msg[protocol.HeaderLen:])
		_ = c.Close() // the rejection is what the caller checks
		return nil, r, err
	}
	return &attacker{c: c}, protocol.RejectFrame{}, nil
}

func newKey() [16]byte {
	var k [16]byte
	_, _ = rand.Read(k[:]) // crypto/rand does not fail on supported platforms
	return k
}

// send writes mux frames as one DATA frame.
func (a *attacker) send(muxFrames ...[]byte) error {
	var p []byte
	for _, f := range muxFrames {
		p = append(p, f...)
	}
	err := a.c.WriteMessage(protocol.EncodeData(a.off, p))
	a.off += uint64(len(p))
	return err
}

func request(stream uint32, path string) [][]byte {
	return [][]byte{
		protocol.EncodeMux(protocol.MuxSyn, stream, nil),
		protocol.EncodeMux(protocol.MuxData, stream, []byte("GET "+path+" HTTP/1.1\r\nHost: x\r\n\r\n")),
	}
}

// closedWithin reports whether the server closed a's link within d.
func (a *attacker) closedWithin(d time.Duration) bool {
	_ = a.c.SetReadDeadline(time.Now().Add(d)) // a deadline error is a timeout, i.e. not closed
	for {
		if _, err := a.c.ReadMessage(1 << 17); err != nil {
			ne, ok := err.(net.Error)
			return !ok || !ne.Timeout()
		}
	}
}

// Sessions that request large downloads and never read their links: the server stops sending at the
// stream credit, the progress rule drops the links, and everything is released.
func TestAttackSessionsThatNeverRead(t *testing.T) {
	s := newServerOnly(t, 1)
	base := s.mustServer().Goroutines
	baseRSS := s.server.rssKiB()
	const n = 50
	var attackers []*attacker
	for range n {
		a, _, err := attack(t, s.linkURL(0), newKey(), 0, protocol.ModeNew)
		if err != nil {
			t.Fatal(err)
		}
		var frames [][]byte
		for id := uint32(1); id <= 15; id += 2 {
			frames = append(frames, request(id, "/big?n=104857600")...)
		}
		if err := a.send(frames...); err != nil {
			t.Fatal(err)
		}
		attackers = append(attackers, a)
	}
	// Keep the links audibly alive while never reading them.
	stop := make(chan struct{})
	defer close(stop)
	for _, a := range attackers {
		go func() {
			for i := uint64(0); ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(400 * time.Millisecond):
				}
				if a.c.WriteMessage(protocol.EncodePing(i)) != nil {
					return
				}
			}
		}()
	}
	peakRSS, peak := 0, s.mustServer()
	took := s.waitFor("every attacking link to be dropped", 20*time.Second, func() bool {
		peakRSS = max(peakRSS, s.server.rssKiB())
		st := s.mustServer()
		if st.Goroutines > peak.Goroutines {
			peak = st
		}
		return st.Sessions == 0
	})
	st := s.waitIdle(base, 4)
	measure(t, "%d sessions x 8 streams of 100 MiB never read: dropped after %v; peak %d goroutines (%+v), RSS %d KiB -> peak %d KiB; idle %+v",
		n, took.Round(time.Millisecond), peak.Goroutines, peak, baseRSS, peakRSS, st)
	// Per connection: a reader, a writer, and per stream a splice of two goroutines.
	if bound := base + n*(2+8*2) + 20; peak.Goroutines > bound {
		t.Fatalf("peak %d goroutines over the bound %d", peak.Goroutines, bound)
	}
	// Per session: the send buffer, its copy in the link queue, the stream inboxes, and slack.
	lim := protocol.DefaultLimits()
	if bound := baseRSS + n*(3*lim.Window+1<<20)/1024; peakRSS > bound {
		t.Fatalf("peak RSS %d KiB over the bound %d KiB", peakRSS, bound)
	}
}

// Hostile lengths and offsets close the offending link at once and leave the server serving.
func TestAttackOversizedLengths(t *testing.T) {
	s := newServerOnly(t, 1)
	base := s.mustServer().Goroutines
	lim := protocol.DefaultLimits()
	cases := map[string]func(a *attacker) error{
		"websocket message above the frame limit": func(a *attacker) error {
			return a.c.WriteMessage(make([]byte, lim.MaxPayload+protocol.HeaderLen+1))
		},
		"frame length above its message": func(a *attacker) error {
			return a.c.WriteMessage([]byte{protocol.Data, 0xff, 0xff, 1, 2, 3})
		},
		"data beyond the window": func(a *attacker) error {
			return a.c.WriteMessage(protocol.EncodeData(uint64(lim.Window)*4, []byte("far ahead")))
		},
		"ack of bytes never sent": func(a *attacker) error {
			return a.c.WriteMessage(protocol.EncodeAck(1 << 40))
		},
		"stream data beyond its credit": func(a *attacker) error {
			if err := a.send(protocol.EncodeMux(protocol.MuxSyn, 1, nil)); err != nil {
				return err
			}
			chunk := protocol.EncodeMux(protocol.MuxData, 1, make([]byte, protocol.MaxMuxData))
			for range lim.StreamWindow/protocol.MaxMuxData + 1 {
				if err := a.send(chunk); err != nil {
					return err
				}
			}
			return nil
		},
	}
	for name, hostile := range cases {
		a, _, err := attack(t, s.linkURL(0), newKey(), 0, protocol.ModeNew)
		if err != nil {
			t.Fatal(err)
		}
		if err := hostile(a); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !a.closedWithin(2 * time.Second) {
			t.Errorf("%s: the link stayed open", name)
		}
	}
	// A WebSocket frame header claiming 2^62 bytes, sent by hand right after the upgrade.
	raw, err := net.Dial("tcp", s.serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	upgrade := "GET /multipath/v3/link HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	hostile := []byte{0x82, 0xff, 0x40, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4}
	if _, err := raw.Write(append([]byte(upgrade), hostile...)); err != nil {
		t.Fatal(err)
	}
	_ = raw.SetReadDeadline(time.Now().Add(2 * time.Second)) // a timeout means the server kept it open
	if _, err := io.ReadAll(raw); err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Errorf("websocket length 2^62: the connection stayed open")
		}
	}
	st := s.waitIdle(base, 4)
	if st.Sessions != 0 {
		t.Fatalf("sessions left: %+v", st)
	}
	measure(t, "%d hostile inputs, each closed its link; idle %+v", len(cases)+1, st)
}

// Thousands of links that connect and vanish, links that never upgrade and upgrades that never send
// HELLO: all are released within the handshake timeout.
func TestAttackRapidConnects(t *testing.T) {
	s := newServerOnly(t, 1)
	base := s.mustServer().Goroutines
	start := time.Now()
	var wg sync.WaitGroup
	for w := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 40 {
				a, _, err := attack(t, s.linkURL(0), newKey(), uint8(i%8), protocol.ModeNew)
				if err != nil {
					t.Errorf("worker %d: %v", w, err)
					return
				}
				_ = a.c.Close() // vanishing is the attack
			}
		}()
	}
	var idle []net.Conn
	for range 300 {
		c, err := net.Dial("tcp", s.serverAddr)
		if err != nil {
			t.Fatal(err)
		}
		idle = append(idle, c)
	}
	for range 100 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c, err := link.Dial(ctx, "ws://"+s.serverAddr+"/multipath/v3/link", link.Options{})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	}
	wg.Wait()
	burst := time.Since(start)
	peak := s.mustServer()
	st := s.waitIdle(base, 4)
	settled := time.Since(start)
	for _, c := range idle {
		_ = c.Close() // the server closed them already; this only frees the test's side
	}
	measure(t, "2000 connect-and-vanish sessions, 300 silent sockets, 100 silent upgrades in %v; peak %+v; idle after %v: %+v",
		burst.Round(time.Millisecond), peak, settled.Round(time.Millisecond), st)
	if st.Sessions != 0 || st.Links != 0 {
		t.Fatalf("left behind: %+v", st)
	}
	if settled > 12*time.Second {
		t.Fatalf("took %v to settle", settled)
	}
}

// One session with every link slot taken, plus one more that is refused, carries a large download;
// memory stays within the protocol's per-connection bound.
func TestAttackMaxLinksWithALargeDownload(t *testing.T) {
	lim := protocol.DefaultLimits()
	s := newStack(t, lim.MaxLinks)
	_, r, err := attack(t, s.linkURL(0), newKey(), uint8(lim.MaxLinks), protocol.ModeNew)
	if err != nil || r.Code != protocol.RejectLimit {
		t.Fatalf("a link beyond the limit was not refused: %+v %v", r, err)
	}
	base, baseRSS := s.mustServer().Goroutines, s.server.rssKiB()
	var peakRSS int
	done := make(chan error, 1)
	go func() { done <- s.big(256<<20, 2*time.Minute) }()
	var peak int
	for running := true; running; {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			running = false
		case <-time.After(50 * time.Millisecond):
			peakRSS = max(peakRSS, s.server.rssKiB())
			peak = max(peak, s.mustServer().Goroutines)
		}
	}
	// Session window, reorder buffer, and every link's queue, with room for the Go runtime.
	boundKiB := baseRSS + (3*lim.Window+lim.MaxLinks*(lim.Window+128<<10))/1024 + 64<<10
	measure(t, "256 MiB over %d links: server RSS %d KiB -> peak %d KiB (bound %d KiB); goroutines %d -> peak %d",
		lim.MaxLinks, baseRSS, peakRSS, boundKiB, base, peak)
	if peakRSS > boundKiB {
		t.Fatalf("peak RSS %d KiB over the bound %d KiB", peakRSS, boundKiB)
	}
}
