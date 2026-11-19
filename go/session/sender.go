// The send side of a session (PROTOCOL.md sections 5.1 to 5.3): a send buffer of unacknowledged
// bytes bounded by the window, fanned out to every live link, released as ACKs arrive.
//
//   - (*Session).Send(p, control) appends p (1 to protocol.MaxData bytes) to the stream as one DATA
//     frame queued on every live link. A data send waits while the window is full; a control send
//     (mux SYN, FIN, RST, WINDOW) never waits, so a reader can always answer, but may run past the
//     window by at most protocol.ControlSlack bytes, beyond which the peer is not acknowledging and
//     the session ends. Send returns ErrEnded once the session has ended.
//
//	if err := s.Send(protocol.EncodeMux(protocol.MuxData, 1, body), false); err != nil { ... }

package session

import (
	"fmt"
	"time"

	"github.com/micro-teams/multipath/go/protocol"
)

// Send appends p to the stream.
func (s *Session) Send(p []byte, control bool) error {
	if len(p) == 0 || len(p) > protocol.MaxData {
		return fmt.Errorf("multipath: send of %d bytes, want 1 to %d", len(p), protocol.MaxData)
	}
	s.mu.Lock()
	for !s.closed && !control && len(s.sendBuf)+len(p) > s.lim.Window {
		s.writable.Wait()
	}
	if s.closed {
		s.mu.Unlock()
		return ErrEnded
	}
	if len(s.sendBuf)+len(p) > s.lim.Window+protocol.ControlSlack {
		s.mu.Unlock()
		s.End(fmt.Sprintf("the peer has not acknowledged %d bytes", len(s.sendBuf)))
		return ErrEnded
	}
	off := s.sendNext
	s.sendBuf = append(s.sendBuf, p...)
	s.sendNext += uint64(len(p))
	full := s.fanoutLocked(protocol.EncodeData(off, p), s.sendNext)
	s.mu.Unlock()
	for _, l := range full {
		s.drop(l, "send queue full")
	}
	return nil
}

// fanoutLocked queues f on every live link and returns the links whose queue had no room. end is
// the stream offset after f when f is DATA, else 0.
func (s *Session) fanoutLocked(f []byte, end uint64) []*link {
	var full []*link
	now := time.Now()
	for _, l := range s.links {
		if l == nil {
			continue
		}
		if !l.q.push(f) {
			full = append(full, l)
			continue
		}
		if end > 0 {
			if l.ackSeen >= l.sentEnd {
				l.progressAt = now // the progress clock starts when data becomes outstanding
			}
			l.sentEnd = end
		}
	}
	return full
}

// onAck handles an ACK received on link l.
func (s *Session) onAck(l *link, off uint64) {
	s.mu.Lock()
	if off > s.sendNext {
		s.mu.Unlock()
		s.drop(l, fmt.Sprintf("protocol violation: ack %d beyond the %d bytes sent", off, s.sendNext))
		return
	}
	if off > l.ackSeen {
		l.ackSeen = off
		l.progressAt = time.Now()
	}
	if off > s.sendBase {
		s.sendBuf = s.sendBuf[off-s.sendBase:]
		s.sendBase = off
		s.acked.Store(off)
		s.writable.Broadcast()
	}
	s.mu.Unlock()
}
