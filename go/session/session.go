// Package session is the redundant layer of MultiPath (PROTOCOL.md section 5): several links joined
// into one reliable, ordered, duplex byte stream. Every byte is sent on every live link; the receiver
// keeps the first copy of each byte, in order, and drops the rest. As long as one link works the
// session works; when the last link dies the session ends for good, on both sides, and everything it
// held is released at once.
//
// The package does not dial, accept or handshake: its owner (the client or the server) brings each
// link up to the point where frames flow and then attaches it. Both ends of a session run the same
// code; Options.Client selects the client's liveness role (it pings, and uses the shorter dead time).
//
// The work is split across files: this one holds the session's life (New, Attach, End, Goodbye,
// Stats); sender.go the send window and replay; receiver.go reordering, delivery and ACKs; link.go a
// link's queue, reader, writer and liveness; clock.go the one shared timer.
//
//   - Conn is what Attach takes: a link whose handshake is done, one message per frame; *link.Conn
//     implements it. ErrEnded is what Send and Attach return once the session has ended.
//
//   - Options: the limits, the role, and three callbacks. Deliver receives the in-order bytes, one
//     call at a time; it must not block, and an error from it ends the session (the mux reports
//     protocol violations this way). OnLink reports each link coming up or going down with a reason;
//     OnEnd reports the end of the session once, with its reason. Callbacks run without locks held.
//
//   - New(opt) creates a session with no links. Bytes sent before the first Attach wait in the send
//     buffer and are replayed onto the first link.
//
//   - (*Session).SetLimits(l) replaces the limits before the first Attach; the client uses it once it
//     has the server's WELCOME.
//
//   - (*Session).Attach(index, conn) adds a link whose handshake is done, replacing any link with the
//     same index. The link first receives the current ACK and the replay of every unacknowledged
//     byte, and only then any newer frame.
//
//   - (*Session).Send(frame, control) sends one piece of at most protocol.MaxData bytes as one DATA
//     frame (sender.go).
//
//   - (*Session).Detach(index, reason) closes one link, as if it had died.
//
//   - (*Session).End(reason) ends the session; (*Session).Goodbye(reason) first sends GOODBYE on
//     every link, for up to half a second, so the peer ends it too.
//
//   - (*Session).Done() is closed when the session has ended; Reason() then says why. A session
//     ended by the peer's GOODBYE has a reason starting with PeerGoodbye.
//
//   - (*Session).Stats() is a snapshot of offsets and per-link counters (Stats, with one LinkStat per
//     live link), for status pages.
//
//     var m *mux.Mux
//     s := session.New(session.Options{Limits: protocol.DefaultLimits(), Client: true,
//     Deliver: func(p []byte) error { return m.Feed(p) },
//     OnLink:  func(i int, up bool, why string) { log.Print(i, up, why) },
//     OnEnd:   func(why string) { log.Print("session ended: ", why) }})
//     m = mux.New(s, true, ...)
//     err := s.Attach(0, conn)
package session

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micro-teams/multipath/go/protocol"
)

// ErrEnded is returned by Send and Attach once the session has ended.
var ErrEnded = errors.New("multipath: session ended")

// PeerGoodbye starts the reason of a session ended by the peer's GOODBYE.
const PeerGoodbye = "the peer said goodbye: "

// Conn is a link whose handshake is done: one message is one frame. *link.Conn implements it.
type Conn interface {
	ReadMessage(max int) ([]byte, error)
	WriteMessage(p []byte) error
	SetWriteDeadline(t time.Time) error
	Close() error
}

// Options configures a session.
type Options struct {
	Limits  protocol.Limits
	Client  bool                                 // the client pings and uses Limits.ClientDead
	Deliver func(p []byte) error                 // in-order bytes; must not block
	OnLink  func(index int, up bool, why string) // optional
	OnEnd   func(why string)                     // optional
	Clock   *Clock                               // nil means DefaultClock()
}

// Session is one redundant byte stream over up to Limits.MaxLinks links.
type Session struct {
	opt  Options
	done chan struct{}

	mu       sync.Mutex
	writable *sync.Cond
	lim      protocol.Limits
	links    []*link // by index; nil while that index has no live link
	live     bool    // a link has been attached; from then on, losing the last one ends the session
	closed   bool
	reason   string
	sendBase uint64 // the peer has acknowledged every byte below
	sendNext uint64 // offset of the next byte sent
	sendBuf  []byte // bytes [sendBase, sendNext)
	lastAck  uint64 // the delivered offset last acknowledged to the peer

	acked atomic.Uint64 // mirror of sendBase for link writers, which skip acknowledged DATA

	rmu          sync.Mutex // receive side; taken before mu when both are needed
	deliverNext  atomic.Uint64
	reorder      map[uint64][]byte
	reorderBytes int
}

// New creates a session with no links.
func New(opt Options) *Session {
	if opt.Clock == nil {
		opt.Clock = DefaultClock()
	}
	s := &Session{
		opt:     opt,
		done:    make(chan struct{}),
		lim:     opt.Limits,
		links:   make([]*link, opt.Limits.MaxLinks),
		reorder: make(map[uint64][]byte),
	}
	s.writable = sync.NewCond(&s.mu)
	opt.Clock.add(s)
	return s
}

// SetLimits replaces the limits; it fails once a link has been attached.
func (s *Session) SetLimits(l protocol.Limits) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.live {
		return errors.New("multipath: limits are fixed once a link is attached")
	}
	s.lim = l
	s.links = make([]*link, l.MaxLinks)
	s.writable.Broadcast()
	return nil
}

// Attach adds a link at index, replacing the link that had it.
func (s *Session) Attach(index int, c Conn) error {
	now := time.Now()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrEnded
	}
	if index < 0 || index >= len(s.links) {
		s.mu.Unlock()
		return fmt.Errorf("multipath: link index %d outside 0..%d", index, len(s.links)-1)
	}
	l := newLink(s, index, c, s.lim, now)
	// The ACK and the replay go into the queue before the link is published, under the same lock
	// that every later send takes, so no newer frame can reach this link ahead of the replay.
	fits := l.q.push(protocol.EncodeAck(s.deliverNext.Load()))
	for off := 0; fits && off < len(s.sendBuf); off += protocol.MaxData {
		end := min(off+protocol.MaxData, len(s.sendBuf))
		fits = l.q.push(protocol.EncodeData(s.sendBase+uint64(off), s.sendBuf[off:end]))
	}
	if !fits {
		// The queue holds a whole window plus slack, so this means the limits are inconsistent.
		s.mu.Unlock()
		l.close()
		return fmt.Errorf("multipath: replay of %d bytes does not fit link %d's send queue", len(s.sendBuf), index)
	}
	l.ackSeen, l.sentEnd = s.sendBase, s.sendNext
	old := s.links[index]
	s.links[index] = l
	s.live = true
	s.mu.Unlock()

	go l.writeLoop()
	go l.readLoop()
	if old != nil {
		s.drop(old, "replaced by a new link")
	}
	if s.opt.OnLink != nil {
		s.opt.OnLink(index, true, "")
	}
	return nil
}

// Detach closes the link at index, if there is one, for the given reason.
func (s *Session) Detach(index int, why string) {
	s.mu.Lock()
	var l *link
	if index >= 0 && index < len(s.links) {
		l = s.links[index]
	}
	s.mu.Unlock()
	if l != nil {
		s.drop(l, why)
	}
}

// drop closes one link and ends the session if it was the last.
func (s *Session) drop(l *link, why string) {
	if !l.close() {
		return
	}
	s.mu.Lock()
	removed := s.links[l.index] == l
	if removed {
		s.links[l.index] = nil
	}
	last := removed && !s.closed && s.liveLinksLocked() == 0
	ended := s.closed
	s.mu.Unlock()
	if !ended && s.opt.OnLink != nil {
		s.opt.OnLink(l.index, false, why)
	}
	if last {
		s.End("all links are dead, the last one: " + why)
	}
}

func (s *Session) liveLinksLocked() int {
	n := 0
	for _, l := range s.links {
		if l != nil {
			n++
		}
	}
	return n
}

// End ends the session at once.
func (s *Session) End(why string) { s.end(why, false) }

// Goodbye sends GOODBYE on every link, waits up to half a second for it to leave, and ends the
// session.
func (s *Session) Goodbye(why string) { s.end(why, true) }

func (s *Session) end(why string, goodbye bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.reason = why
	var links []*link
	for i, l := range s.links {
		if l != nil {
			links = append(links, l)
			s.links[i] = nil
			if goodbye {
				l.q.push(protocol.EncodeGoodbye(why))
			}
		}
	}
	s.sendBuf = nil
	s.writable.Broadcast()
	s.mu.Unlock()
	s.opt.Clock.remove(s)

	if goodbye {
		deadline := time.Now().Add(500 * time.Millisecond)
		for _, l := range links {
			for !l.q.empty() && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
	for _, l := range links {
		l.close()
	}
	close(s.done)
	if s.opt.OnEnd != nil {
		s.opt.OnEnd(why)
	}
}

// Done is closed once the session has ended.
func (s *Session) Done() <-chan struct{} { return s.done }

// Reason says why the session ended, or "" while it runs.
func (s *Session) Reason() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reason
}

// LinkStat is one link in a Stats snapshot.
type LinkStat struct {
	Index    int
	LastByte time.Time // when the last message arrived
	BytesIn  int64
	BytesOut int64
	Queued   int // bytes waiting in the link's send queue
}

// Stats is a snapshot of a session.
type Stats struct {
	SendNext    uint64 // bytes sent
	SendBase    uint64 // bytes acknowledged by the peer
	DeliverNext uint64 // bytes received in order
	Reorder     int    // bytes held out of order
	Links       []LinkStat
}

// Stats returns a snapshot of the session.
func (s *Session) Stats() Stats {
	s.mu.Lock()
	st := Stats{SendNext: s.sendNext, SendBase: s.sendBase, DeliverNext: s.deliverNext.Load()}
	for _, l := range s.links {
		if l != nil {
			st.Links = append(st.Links, LinkStat{Index: l.index, LastByte: time.Unix(0, l.lastIn.Load()),
				BytesIn: l.bytesIn.Load(), BytesOut: l.bytesOut.Load(), Queued: l.q.size()})
		}
	}
	s.mu.Unlock()
	s.rmu.Lock()
	st.Reorder = s.reorderBytes
	s.rmu.Unlock()
	return st
}
