// One link of a session (PROTOCOL.md sections 5.3 and 5.4): a reader goroutine, a writer goroutine
// draining a bounded send queue, and the liveness rules the clock applies to it. Each link writes on
// its own, so a link whose peer stopped reading fills only its own queue and is closed when the queue
// is full or a write is stuck past the dead time; it never blocks the session or the other links.
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
	ready  chan struct{} // holds a token while frames may be waiting
}

func (q *queue) push(f []byte) bool {
	q.mu.Lock()
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
	dead       time.Duration // silence, and the longest single write, before the link is dead
	progress   time.Duration
	ping       time.Duration // 0 on the server, which only answers

	lastIn   atomic.Int64 // unix nanoseconds of the last message received
	bytesIn  atomic.Int64
	bytesOut atomic.Int64

	// Guarded by the session's mu.
	ackSeen    uint64    // highest ACK received on this link
	sentEnd    uint64    // stream offset after the last DATA queued on this link
	progressAt time.Time // when ackSeen last advanced, or data became outstanding
	nextPing   time.Time
}

func newLink(s *Session, index int, c Conn, lim protocol.Limits, now time.Time) *link {
	l := &link{s: s, index: index, conn: c, done: make(chan struct{}), maxPayload: lim.MaxPayload,
		dead: lim.ServerDead, progress: lim.Progress, progressAt: now, nextPing: now}
	if s.opt.Client {
		l.dead, l.ping = lim.ClientDead, lim.Ping
	}
	l.q = queue{limit: lim.Window + 2*protocol.ControlSlack, ready: make(chan struct{}, 1)}
	l.lastIn.Store(now.UnixNano())
	return l
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
		if f[0] == protocol.Data {
			end := binary.BigEndian.Uint64(f[protocol.HeaderLen:]) + uint64(len(f)-protocol.HeaderLen-protocol.DataOverhead)
			if end <= l.s.acked.Load() {
				continue
			}
		}
		if err := l.conn.SetWriteDeadline(time.Now().Add(l.dead)); err != nil {
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
		if _, err := protocol.DecodeU64(p); err != nil {
			return "protocol violation: " + err.Error()
		}
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

// tick applies the liveness rules to every link, sends due PINGs, and flushes a pending ACK.
func (s *Session) tick(now time.Time) {
	s.flushAck()
	type verdict struct {
		l   *link
		why string
	}
	var drops []verdict
	s.mu.Lock()
	for _, l := range s.links {
		if l == nil {
			continue
		}
		switch {
		case now.Sub(time.Unix(0, l.lastIn.Load())) > l.dead:
			drops = append(drops, verdict{l, fmt.Sprintf("no inbound byte for %v", l.dead)})
		case l.ackSeen < l.sentEnd && now.Sub(l.progressAt) > l.progress:
			drops = append(drops, verdict{l, fmt.Sprintf("acknowledgement stalled for %v with %d bytes outstanding", l.progress, l.sentEnd-l.ackSeen)})
		case l.ping > 0 && !now.Before(l.nextPing):
			l.nextPing = now.Add(l.ping)
			if !l.q.push(protocol.EncodePing(uint64(now.UnixNano()))) {
				drops = append(drops, verdict{l, "send queue full"})
			}
		}
	}
	s.mu.Unlock()
	for _, d := range drops {
		s.drop(d.l, d.why)
	}
}
