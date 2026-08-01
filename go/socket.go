// Choosing a line for a stream, and choosing again when it breaks.
//
// The mirror of ts/src/socket.ts, and the same ceiling: a stream cannot be raced, because two
// connections are two conversations, each with its own state. So the most that is possible is pick
// the best line, and when it breaks, pick again.
//
// The part that is easy to miss is that HTTP health says almost nothing about whether a line can
// carry a stream. A cheap reverse proxy will serve requests perfectly and refuse the Upgrade; a
// middlebox will allow the handshake and then sever anything long-lived. So a line's ability to
// hold a stream is remembered separately from its latency, and a line that fails at this is skipped
// for streams while remaining perfectly good for requests.
//
// Generic over the connection type, so the caller brings whatever WebSocket library it already has
// and this package keeps its zero dependencies. What is being decided here is which line to dial,
// which is not a question about any particular library.

package multipath

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"
)

// StreamOptions configures a stream. Zero values take the defaults.
type StreamOptions[Conn any] struct {
	// Lines in preference order, read afresh before every attempt so a re-ranking takes effect.
	Lines func() []Line
	// Path is joined to each line's origin.
	Path string
	// Dial opens the connection. Bring your own library.
	Dial func(ctx context.Context, url string) (Conn, error)
	// Serve runs the conversation and returns when it ends. Its error, if any, is why.
	Serve func(ctx context.Context, conn Conn) error
	// StableAfter is how long a connection must last before it counts as working.
	//
	// Without it, a line that accepts the handshake and drops it immediately looks like a success
	// every time and the client reconnects in a tight loop forever.
	StableAfter time.Duration
	// RetryDelay is the first reconnect delay; it doubles per consecutive failure up to MaxDelay.
	RetryDelay time.Duration
	MaxDelay   time.Duration
	// Penalty is how long a line is skipped for streams after failing to hold one.
	Penalty time.Duration
	// OnOpen and OnClose are for diagnostics.
	OnOpen  func(Line)
	OnClose func(Line, error)
	Now     func() time.Time
}

const (
	defaultStableAfter = 5 * time.Second
	defaultRetryDelay  = 500 * time.Millisecond
	defaultMaxDelay    = 30 * time.Second
	// Long enough that a proxy which cannot do WebSockets stops being tried every few seconds,
	// short enough that a line merely having a bad minute comes back into rotation.
	defaultPenalty = 60 * time.Second
)

// Stream keeps a connection up, over whichever line can hold one.
type Stream[Conn any] struct {
	opts StreamOptions[Conn]

	mu        sync.RWMutex
	current   *Line
	penalties map[string]time.Time
}

// NewStream prepares a stream. Nothing is dialled until Run is called.
func NewStream[Conn any](opts StreamOptions[Conn]) *Stream[Conn] {
	if opts.StableAfter == 0 {
		opts.StableAfter = defaultStableAfter
	}
	if opts.RetryDelay == 0 {
		opts.RetryDelay = defaultRetryDelay
	}
	if opts.MaxDelay == 0 {
		opts.MaxDelay = defaultMaxDelay
	}
	if opts.Penalty == 0 {
		opts.Penalty = defaultPenalty
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Stream[Conn]{opts: opts, penalties: map[string]time.Time{}}
}

// Current reports which line is carrying the stream, or nil between attempts.
func (s *Stream[Conn]) Current() *Line {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// Penalties reports lines recently unable to hold a stream, and until when.
func (s *Stream[Conn]) Penalties() map[string]time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]time.Time, len(s.penalties))
	for id, until := range s.penalties {
		out[id] = until
	}
	return out
}

// Run keeps the stream connected until the context is cancelled.
//
// Reconnection is not optional politeness: every line here is expected to be less reliable than one
// well-chosen line, and the whole bet is that several beat one. That only pays if breaking is
// routine and recovering is automatic.
func (s *Stream[Conn]) Run(ctx context.Context) error {
	failures := 0

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		line, ok := s.pick()
		if !ok {
			// Nothing to dial yet — a registry that has not loaded. Keep waiting rather than giving
			// up permanently, since it can arrive at any moment.
			failures++
			if err := s.wait(ctx, failures); err != nil {
				return err
			}
			continue
		}

		held, err := s.serveOnce(ctx, line)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if held < s.opts.StableAfter {
			// Never became stable: evidence about this line's ability to carry a stream, which is a
			// different question from whether it answers requests quickly.
			s.penalise(line)
			failures++
		} else {
			// It worked for a while. An ordinary disconnection, so reconnect promptly.
			failures = 0
		}
		if s.opts.OnClose != nil {
			s.opts.OnClose(line, err)
		}
		if err := s.wait(ctx, failures); err != nil {
			return err
		}
	}
}

func (s *Stream[Conn]) serveOnce(ctx context.Context, line Line) (time.Duration, error) {
	started := s.opts.Now()

	conn, err := s.opts.Dial(ctx, StreamURL(line, s.opts.Path))
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	s.current = &line
	s.mu.Unlock()
	if s.opts.OnOpen != nil {
		s.opts.OnOpen(line)
	}

	err = s.opts.Serve(ctx, conn)

	s.mu.Lock()
	s.current = nil
	s.mu.Unlock()

	return s.opts.Now().Sub(started), err
}

// pick returns the best line not currently serving a penalty.
//
// If every line is penalised the least-recently-penalised is used anyway: a client with no
// connection is worse than one on a flaky connection, and the penalties may all be stale.
func (s *Stream[Conn]) pick() (Line, bool) {
	candidates := s.opts.Lines()
	if len(candidates) == 0 {
		return Line{}, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	at := s.opts.Now()
	for _, candidate := range candidates {
		if until, penalised := s.penalties[candidate.ID]; !penalised || !until.After(at) {
			return candidate, true
		}
	}

	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if s.penalties[candidate.ID].Before(s.penalties[best.ID]) {
			best = candidate
		}
	}
	return best, true
}

func (s *Stream[Conn]) penalise(line Line) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.penalties[line.ID] = s.opts.Now().Add(s.opts.Penalty)
}

// wait sleeps before the next attempt, or returns as soon as the caller gives up.
//
// The first retry waits the base delay rather than double it: most disconnections are one-offs, and
// making the common case wait twice as long is a poor trade for arithmetic tidiness.
func (s *Stream[Conn]) wait(ctx context.Context, failures int) error {
	if failures == 0 {
		failures = 1
	}
	exponent := math.Min(float64(failures-1), 8)
	delay := time.Duration(float64(s.opts.RetryDelay) * math.Pow(2, exponent))
	if delay > s.opts.MaxDelay {
		delay = s.opts.MaxDelay
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// StreamURL upgrades a line's scheme: ws:// for http://, wss:// for https://. A same-origin line
// yields the bare path, leaving the scheme to whatever the caller is already using.
func StreamURL(line Line, path string) string {
	if line.URL == "" {
		return path
	}
	return strings.Replace(line.URL, "http", "ws", 1) + path
}
