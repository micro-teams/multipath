// The one timer of a process (PROTOCOL.md section 8): every session's pings, liveness checks and
// delayed ACKs run from a single ticking goroutine, so a session costs no goroutine of its own and an
// idle process wakes up at one fixed rate whatever the number of sessions.
//
//   - NewClock(interval) starts a clock that calls every registered session's tick at that interval.
//     The interval bounds how late a dead link is noticed and how long an ACK waits; 50 ms against
//     the 1 s liveness rules of the protocol.
//   - DefaultClock() returns the process-wide 50 ms clock, started on first use; sessions use it
//     unless their Options name another (tests use a faster one).
//   - (*Clock).Stop stops a clock started with NewClock.
//
//	c := session.NewClock(5 * time.Millisecond)
//	defer c.Stop()
//	s := session.New(session.Options{Clock: c, ...})

package session

import (
	"sync"
	"time"
)

// Clock ticks every registered session.
type Clock struct {
	interval time.Duration
	stop     chan struct{}

	mu       sync.Mutex
	sessions map[*Session]struct{}
}

// NewClock starts a clock.
func NewClock(interval time.Duration) *Clock {
	c := &Clock{interval: interval, stop: make(chan struct{}), sessions: make(map[*Session]struct{})}
	go c.run()
	return c
}

var (
	defaultOnce  sync.Once
	defaultClock *Clock
)

// DefaultClock returns the shared 50 ms clock of the process.
func DefaultClock() *Clock {
	defaultOnce.Do(func() { defaultClock = NewClock(50 * time.Millisecond) })
	return defaultClock
}

// Stop stops the clock; registered sessions are no longer ticked.
func (c *Clock) Stop() { close(c.stop) }

func (c *Clock) add(s *Session) {
	c.mu.Lock()
	c.sessions[s] = struct{}{}
	c.mu.Unlock()
}

func (c *Clock) remove(s *Session) {
	c.mu.Lock()
	delete(c.sessions, s)
	c.mu.Unlock()
}

func (c *Clock) run() {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	var batch []*Session
	for {
		select {
		case <-c.stop:
			return
		case now := <-t.C:
			c.mu.Lock()
			batch = batch[:0]
			for s := range c.sessions {
				batch = append(batch, s)
			}
			c.mu.Unlock()
			for _, s := range batch {
				s.tick(now)
			}
		}
	}
}
