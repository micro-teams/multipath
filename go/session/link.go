// One link of a session (PROTOCOL.md sections 5.3 and 5.4): a reader goroutine, a writer goroutine
// draining a bounded send queue, and the liveness rules the clock applies to it. Each link writes on
// its own, so a link whose peer stopped reading fills only its own queue and is closed when the queue
// is full or a write is stuck past the dead time; it never blocks the session or the other links.
// Acknowledged DATA is dropped from a queue rather than sent, so a slow link only ever carries what
// the peer still lacks.
//
// Liveness on the client is judged for the session as a whole. Every Limits.Ping the client sends one
// PING round on all links at once. A link answers a round with any inbound byte after its PING (the
// PONG itself may wait behind data the server queued earlier). A link that has not answered the
// round sent Limits.ClientDead ago is soft down: it gets no new DATA but stays attached, and when it
// answers again it first receives the DATA it missed. When no link answers, the session is silent
// and its owner is told once (Options.OnSilent) so that it redials every line into this session. A
// link is closed only after max(protocol.LinkDeadMin, protocol.LinkDeadRTTs × its smoothed round
// trip) without an inbound byte. The server only answers, and closes a link after Limits.ServerDead
// of silence.
//
// Nothing here is exported. The session creates links in Attach; the clock calls (*Session).tick.

package session

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micro-teams/multipath/go/protocol"
)

// queue is a link's bounded FIFO of whole frames.
type queue struct {
	mu     sync.Mutex
	frames [][]byte
	bytes  int
	limit  int
	acked  *atomic.Uint64 // the session's acknowledged offset
	ready  chan struct{}  // holds a token while frames may be waiting
}

// dataEnd returns the stream offset after a DATA frame, or 0 for any other frame.
func dataEnd(f []byte) uint64 {
	if f[0] != protocol.Data {
		return 0
	}
	return binary.BigEndian.Uint64(f[protocol.HeaderLen:]) + uint64(len(f)-protocol.HeaderLen-protocol.DataOverhead)
}

func (q *queue) push(f []byte) bool {
	q.mu.Lock()
	if q.bytes+len(f) > q.limit {
		// DATA that a faster link has already delivered need not be sent; dropping it first means
		// only bytes the peer still lacks count against the limit, so a slow link keeps up.
		acked := q.acked.Load()
		kept := q.frames[:0]
		for _, g := range q.frames {
			if end := dataEnd(g); end > 0 && end <= acked {
				q.bytes -= len(g)
				continue
			}
			kept = append(kept, g)
		}
		clear(q.frames[len(kept):])
		q.frames = kept
	}
	if q.bytes+len(f) > q.limit {
		q.mu.Unlock()
		return false
	}
	q.frames = append(q.frames, f)
	q.bytes += len(f)
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
	return true
}

// pop waits for the next frame; it returns false once done is closed.
func (q *queue) pop(done <-chan struct{}) ([]byte, bool) {
	for {
		q.mu.Lock()
		if len(q.frames) > 0 {
			f := q.frames[0]
			q.frames[0] = nil
			q.frames = q.frames[1:]
			q.bytes -= len(f)
			q.mu.Unlock()
			return f, true
		}
		q.mu.Unlock()
		select {
		case <-q.ready:
		case <-done:
			return nil, false
		}
	}
}

func (q *queue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
}

func (q *queue) empty() bool { return q.size() == 0 }

type link struct {
	s      *Session
	index  int
	conn   Conn
	q      queue
	done   chan struct{}
	closed atomic.Bool

	maxPayload int
	progress   time.Duration

	lastIn   atomic.Int64 // unix nanoseconds of the last message received
	srtt     atomic.Int64 // client: smoothed PONG round trip in nanoseconds, 0 before the first
	bytesIn  atomic.Int64
	bytesOut atomic.Int64

	// Guarded by the session's mu.
	ackSeen    uint64    // highest ACK received on this link
	sentEnd    uint64    // stream offset after the last DATA queued on this link
	progressAt time.Time // when ackSeen last advanced, or data became outstanding
	soft       bool      // client: missed the due round; gets no new DATA until it answers
}

func newLink(s *Session, index int, c Conn, lim protocol.Limits, now time.Time) *link {
	l := &link{s: s, index: index, conn: c, done: make(chan struct{}), maxPayload: lim.MaxPayload,
		progress: lim.Progress, progressAt: now}
	l.q = queue{limit: lim.Window + 2*protocol.ControlSlack, acked: &s.acked, ready: make(chan struct{}, 1)}
	l.lastIn.Store(now.UnixNano())
	return l
}

// dead is the silence, and the longest single write, after which the link is closed.
func (l *link) dead() time.Duration {
	if !l.s.opt.Client {
		return l.s.lim.ServerDead
	}
	return max(l.s.opt.linkDeadMin, protocol.LinkDeadRTTs*time.Duration(l.srtt.Load()))
}

// onPong folds the round trip of a PING sent at sent (its nonce) into the smoothed round trip, with
// the weight of RFC 6298.
func (l *link) onPong(sent int64) {
	sample := time.Now().UnixNano() - sent
	if sample < 0 || sample > int64(time.Minute) {
		return // not a nonce this client sent; nothing to learn from it
	}
	old := l.srtt.Load()
	if old == 0 {
		l.srtt.Store(sample)
		return
	}
	l.srtt.Store(old + (sample-old)/8)
}

// close closes the link once; it reports whether this call closed it.
func (l *link) close() bool {
	if !l.closed.CompareAndSwap(false, true) {
		return false
	}
	close(l.done)
	_ = l.conn.Close() // the reason the link is closed is reported by the caller; a close error adds nothing
	return true
}

func (l *link) writeLoop() {
	for {
		f, ok := l.q.pop(l.done)
		if !ok {
			return
		}
		// A DATA frame the peer has acknowledged meanwhile (through a faster link) is not worth
		// sending; skipping it lets a slow link catch up instead of overflowing.
		if end := dataEnd(f); end > 0 && end <= l.s.acked.Load() {
			continue
		}
		if err := l.conn.SetWriteDeadline(time.Now().Add(l.dead())); err != nil {
			l.s.drop(l, "set write deadline: "+err.Error())
			return
		}
		if err := l.conn.WriteMessage(f); err != nil {
			l.s.drop(l, "write: "+err.Error())
			return
		}
		l.bytesOut.Add(int64(len(f)))
	}
}

func (l *link) readLoop() {
	for {
		msg, err := l.conn.ReadMessage(l.maxPayload + protocol.HeaderLen)
		if err != nil {
			l.s.drop(l, "read: "+err.Error())
			return
		}
		l.lastIn.Store(time.Now().UnixNano())
		l.bytesIn.Add(int64(len(msg)))
		if why := l.handle(msg); why != "" {
			l.s.drop(l, why)
			return
		}
	}
}

// handle processes one message and returns a reason to drop the link, or "".
func (l *link) handle(msg []byte) string {
	typ, p, err := protocol.ParseFrame(msg, l.maxPayload)
	if err != nil {
		return "protocol violation: " + err.Error()
	}
	switch typ {
	case protocol.Data:
		off, data, err := protocol.DecodeData(p)
		if err != nil {
			return "protocol violation: " + err.Error()
		}
		if derr := l.s.onData(off, data); derr != nil {
			if derr.session {
				l.s.Goodbye("protocol violation: " + derr.why) // closes this link too; its next read fails
				return ""
			}
			return derr.why
		}
	case protocol.Ack:
		off, err := protocol.DecodeU64(p)
		if err != nil {
			return "protocol violation: " + err.Error()
		}
		l.s.onAck(l, off)
	case protocol.Ping:
		nonce, err := protocol.DecodeU64(p)
		if err != nil {
			return "protocol violation: " + err.Error()
		}
		if !l.q.push(protocol.EncodePong(nonce)) {
			return "send queue full"
		}
	case protocol.Pong:
		nonce, err := protocol.DecodeU64(p)
		if err != nil {
			return "protocol violation: " + err.Error()
		}
		l.onPong(int64(nonce))
	case protocol.Goodbye:
		why, err := protocol.DecodeReason(p)
		if err != nil {
			return "protocol violation: " + err.Error()
		}
		l.s.End(PeerGoodbye + why) // closes this link too; its next read fails
		return ""
	case protocol.Hello, protocol.Welcome, protocol.Reject:
		return fmt.Sprintf("protocol violation: handshake frame 0x%02x on an established link", typ)
	}
	// Unknown frame types are skipped: later protocol versions may add them.
	return ""
}

// tick applies the liveness rules to every link, sends a due PING round, and flushes a pending ACK.
func (s *Session) tick(now time.Time) {
	s.flushAck()
	type verdict struct {
		l   *link
		why string
	}
	var drops []verdict
	type turn struct {
		index int
		soft  bool
	}
	var turned []turn // links whose soft state changed
	s.mu.Lock()
	client := s.opt.Client
	due := s.dueRoundLocked(now)
	round := client && !now.Before(s.nextRound)
	if round {
		s.nextRound = now.Add(s.lim.Ping)
		s.rounds = append(s.rounds, now)
	}
	attached, answering := 0, 0
	for _, l := range s.links {
		if l == nil {
			continue
		}
		attached++
		last := time.Unix(0, l.lastIn.Load())
		if last.After(s.heardAt) {
			s.heardAt = last
		}
		if dead := l.dead(); now.Sub(last) > dead {
			drops = append(drops, verdict{l, fmt.Sprintf("no inbound byte for %v", dead)})
			continue
		}
		if l.ackSeen < l.sentEnd && now.Sub(l.progressAt) > l.progress {
			drops = append(drops, verdict{l, fmt.Sprintf("acknowledgement stalled for %v with %d bytes outstanding", l.progress, l.sentEnd-l.ackSeen)})
			continue
		}
		if !client {
			continue
		}
		if soft := last.Before(due); soft != l.soft {
			l.soft = soft
			turned = append(turned, turn{l.index, soft})
			if !soft && !s.catchUpLocked(l, now) {
				drops = append(drops, verdict{l, "send queue full"})
				continue
			}
		}
		if !l.soft {
			answering++
		}
		if round && !l.q.push(protocol.EncodePing(uint64(now.UnixNano()))) {
			drops = append(drops, verdict{l, "send queue full"})
		}
	}
	silent := client && attached > 0 && answering == 0
	tellSilent := silent && !s.silent
	s.silent = silent
	gone := client && s.live && !s.closed && now.Sub(s.heardAt) > s.lim.ServerDead
	serverDead := s.lim.ServerDead
	s.mu.Unlock()
	for _, t := range turned {
		if s.opt.OnSoft != nil {
			s.opt.OnSoft(t.index, t.soft)
		}
	}
	for _, d := range drops {
		s.drop(d.l, d.why)
	}
	if tellSilent && s.opt.OnSilent != nil {
		s.opt.OnSilent()
	}
	if gone {
		// The server ends a session whose links were all silent for its dead time; holding it longer
		// would only keep streams waiting for nothing.
		s.End(fmt.Sprintf("no line has answered for %v, so the server has ended the session", serverDead))
	}
}

// dueRoundLocked returns when the newest PING round that is at least Limits.ClientDead old was sent,
// or the zero time, and forgets the older rounds. Caller holds mu.
func (s *Session) dueRoundLocked(now time.Time) time.Time {
	var due time.Time
	n := 0
	for i, r := range s.rounds {
		if now.Sub(r) < s.lim.ClientDead {
			break
		}
		due, n = r, i
	}
	if !due.IsZero() {
		s.rounds = s.rounds[n:]
	}
	return due
}
