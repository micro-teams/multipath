// The receive side of a session (PROTOCOL.md sections 5.1 and 5.2): DATA from every link is reduced
// to one in-order stream. Duplicates are dropped by offset, frames ahead of a gap wait in a bounded
// reorder buffer instead of being thrown away (a reconnecting link may legitimately deliver a newer
// frame before the replay of an older one), and the delivered offset is acknowledged on every link
// whenever it has changed: at once when a quarter of the window is unacknowledged, otherwise on the
// next clock tick.
//
// Nothing here is exported; link readers call onData, the clock calls flushAck.

package session

import (
	"fmt"

	"github.com/micro-teams/multipath/go/protocol"
)

// dataError says what an onData failure ends: one link (a frame outside the reorder bounds) or the
// whole session (the deliver callback refused the bytes).
type dataError struct {
	why     string
	session bool
}

func (e *dataError) Error() string { return e.why }

// onData takes one DATA frame's bytes, which the caller does not keep.
func (s *Session) onData(off uint64, data []byte) *dataError {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	s.mu.Lock()
	window := s.lim.Window
	s.mu.Unlock()
	next := s.deliverNext.Load()
	end := off + uint64(len(data))
	if end <= next {
		return nil
	}
	if end > next+uint64(window+protocol.ControlSlack) {
		return &dataError{why: fmt.Sprintf("protocol violation: data up to %d is beyond the window (delivered %d)", end, next)}
	}
	if off > next {
		prev := s.reorder[off]
		if len(prev) >= len(data) {
			return nil
		}
		if s.reorderBytes+len(data)-len(prev) > 2*window {
			return &dataError{why: fmt.Sprintf("protocol violation: reorder buffer over %d bytes", 2*window)}
		}
		s.reorder[off] = data
		s.reorderBytes += len(data) - len(prev)
		return nil
	}
	if err := s.deliverLocked(data[next-off:], end); err != nil {
		return err
	}
	if err := s.drainLocked(); err != nil {
		return err
	}
	s.mu.Lock()
	urgent := s.deliverNext.Load()-s.lastAck >= uint64(window/4)
	s.mu.Unlock()
	if urgent {
		s.flushAck()
	}
	return nil
}

// deliverLocked hands bytes ending at end to the owner. Caller holds rmu.
func (s *Session) deliverLocked(p []byte, end uint64) *dataError {
	if err := s.opt.Deliver(p); err != nil {
		return &dataError{why: err.Error(), session: true}
	}
	s.deliverNext.Store(end)
	return nil
}

// drainLocked delivers buffered frames that have become contiguous. Caller holds rmu.
func (s *Session) drainLocked() *dataError {
	for {
		next := s.deliverNext.Load()
		advanced := false
		for start, buf := range s.reorder {
			end := start + uint64(len(buf))
			if start > next {
				continue
			}
			delete(s.reorder, start)
			s.reorderBytes -= len(buf)
			if end > next {
				if err := s.deliverLocked(buf[next-start:], end); err != nil {
					return err
				}
				advanced = true
				break
			}
		}
		if !advanced {
			return nil
		}
	}
}

// flushAck acknowledges the delivered offset on every live link if it changed since the last ACK.
func (s *Session) flushAck() {
	s.mu.Lock()
	next := s.deliverNext.Load()
	if next == s.lastAck || s.closed {
		s.mu.Unlock()
		return
	}
	s.lastAck = next
	full := s.fanoutLocked(protocol.EncodeAck(next), 0)
	s.mu.Unlock()
	for _, l := range full {
		s.drop(l, "send queue full")
	}
}
