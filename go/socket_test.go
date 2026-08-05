// The same cases as ts/test/socket.test.ts. The connector and the browser have to agree about when
// a line is unfit to carry a stream, or "why does the terminal keep dropping" becomes a question
// with two answers.

package multipath

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var streamLines = []Line{
	{ID: "cf", URL: "https://cf.example"},
	{ID: "ipv6", URL: "https://ipv6.example"},
	{ID: "frp", URL: "https://frp.example"},
}

// scripted records which lines were dialled and decides how long each conversation lasts.
type scripted struct {
	mu      sync.Mutex
	dialled []string
	// holdFor is how long Serve pretends the connection lasted, per line.
	holdFor map[string]time.Duration
	// holds marks lines whose conversation stays open until the caller gives up — the difference
	// between "this line works" and "this line accepts and immediately drops", which is the whole
	// distinction being tested.
	holds map[string]bool
	// dialErr marks lines that refuse the handshake outright.
	dialErr map[string]bool
	clock   *fakeClock
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (s *scripted) record(url string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dialled = append(s.dialled, url)
}

func (s *scripted) urls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.dialled))
	copy(out, s.dialled)
	return out
}

func lineOf(url string) string {
	for _, line := range streamLines {
		if StreamURL(line, "/mt/ws") == url {
			return line.ID
		}
	}
	return ""
}

// build wires a stream whose connections last exactly as long as the script says, without waiting.
func build(t *testing.T, script *scripted, opts StreamOptions[string]) (*Stream[string], func()) {
	t.Helper()
	if script.clock == nil {
		script.clock = &fakeClock{now: time.Unix(0, 0)}
	}

	opts.Lines = func() []Line { return streamLines }
	opts.Path = "/mt/ws"
	opts.Now = script.clock.Now
	opts.Dial = func(_ context.Context, url string) (string, error) {
		script.record(url)
		if script.dialErr[lineOf(url)] {
			return "", errors.New("refused")
		}
		return url, nil
	}
	opts.Serve = func(ctx context.Context, conn string) error {
		// Advancing the clock rather than sleeping: how long a connection "lasted" is the thing
		// under test, and a test that waited for it would take minutes.
		script.clock.advance(script.holdFor[lineOf(conn)])
		if script.holds[lineOf(conn)] {
			<-ctx.Done()
			return ctx.Err()
		}
		return errors.New("closed")
	}
	if opts.RetryDelay == 0 {
		opts.RetryDelay = time.Millisecond
	}
	if opts.MaxDelay == 0 {
		opts.MaxDelay = 2 * time.Millisecond
	}

	stream := NewStream(opts)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = stream.Run(ctx) }()
	return stream, cancel
}

// settle lets the stream's goroutine make progress without depending on wall-clock timing.
func settle() { time.Sleep(30 * time.Millisecond) }

func TestStreamConnectsToTheBestLine(t *testing.T) {
	script := &scripted{holds: map[string]bool{"cf": true}}
	_, cancel := build(t, script, StreamOptions[string]{})
	defer cancel()
	settle()

	if got := script.urls()[0]; got != "wss://cf.example/mt/ws" {
		t.Errorf("got %q", got)
	}
}

// A stream is stateful, so it cannot be raced: two connections are two conversations. One at a time
// is the ceiling, not a shortcut.
func TestStreamOpensOneConnectionAtATime(t *testing.T) {
	script := &scripted{holds: map[string]bool{"cf": true}}
	_, cancel := build(t, script, StreamOptions[string]{})
	defer cancel()
	settle()

	if got := len(script.urls()); got != 1 {
		t.Errorf("expected one connection, got %d: %v", got, script.urls())
	}
}

// HTTP health says almost nothing about whether a line can carry a stream: a cheap proxy serves
// requests perfectly and refuses the Upgrade.
func TestStreamAvoidsALineThatCannotHoldAStream(t *testing.T) {
	script := &scripted{
		// cf drops instantly; ipv6 holds.
		holdFor: map[string]time.Duration{"cf": 0},
		holds:   map[string]bool{"ipv6": true},
	}
	stream, cancel := build(t, script, StreamOptions[string]{StableAfter: time.Second})
	defer cancel()
	settle()

	if _, penalised := stream.Penalties()["cf"]; !penalised {
		t.Error("expected cf to be penalised for streams")
	}
	urls := script.urls()
	if len(urls) < 2 || lineOf(urls[len(urls)-1]) != "ipv6" {
		t.Errorf("expected a move to ipv6, got %v", urls)
	}
}

// It worked for a while; that is not evidence against the line.
func TestStreamDoesNotPenaliseAnOrdinaryDisconnection(t *testing.T) {
	script := &scripted{holdFor: map[string]time.Duration{"cf": time.Minute}}
	stream, cancel := build(t, script, StreamOptions[string]{StableAfter: time.Second})
	defer cancel()
	settle()

	if _, penalised := stream.Penalties()["cf"]; penalised {
		t.Error("a connection that lasted must not count against the line")
	}
}

func TestStreamTreatsARefusedHandshakeAsAFailure(t *testing.T) {
	script := &scripted{
		dialErr: map[string]bool{"cf": true},
		holds:   map[string]bool{"ipv6": true},
	}
	stream, cancel := build(t, script, StreamOptions[string]{StableAfter: time.Second})
	defer cancel()
	settle()

	if _, penalised := stream.Penalties()["cf"]; !penalised {
		t.Error("expected a refused handshake to penalise the line")
	}
}

// A client with no connection is worse than one on a flaky connection, and the penalties may all be
// stale anyway.
func TestStreamStillConnectsWhenEveryLineIsPenalised(t *testing.T) {
	script := &scripted{holdFor: map[string]time.Duration{}} // nothing holds
	_, cancel := build(t, script, StreamOptions[string]{StableAfter: time.Hour})
	defer cancel()
	settle()

	if len(script.urls()) < 4 {
		t.Errorf("expected it to keep trying, got %v", script.urls())
	}
}

func TestStreamReportsTheCurrentLine(t *testing.T) {
	opened := make(chan string, 1)
	script := &scripted{holds: map[string]bool{"cf": true}}
	_, cancel := build(t, script, StreamOptions[string]{
		OnOpen: func(line Line) {
			select {
			case opened <- line.ID:
			default:
			}
		},
	})
	defer cancel()

	select {
	case got := <-opened:
		if got != "cf" {
			t.Errorf("got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("never opened")
	}
}

func TestStreamStopsWhenTheCallerGivesUp(t *testing.T) {
	script := &scripted{holdFor: map[string]time.Duration{}}
	stream := NewStream(StreamOptions[string]{
		Lines: func() []Line { return streamLines },
		Path:  "/mt/ws",
		Dial: func(_ context.Context, url string) (string, error) {
			script.record(url)
			return url, nil
		},
		Serve:      func(context.Context, string) error { return errors.New("closed") },
		RetryDelay: time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- stream.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected the cancellation to surface, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestStreamWaitsWhenTheRegistryIsEmpty(t *testing.T) {
	var dialled int
	stream := NewStream(StreamOptions[string]{
		Lines: func() []Line { return nil },
		Path:  "/mt/ws",
		Dial: func(context.Context, string) (string, error) {
			dialled++
			return "", nil
		},
		Serve:      func(context.Context, string) error { return nil },
		RetryDelay: time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = stream.Run(ctx)

	// A registry can arrive at any moment; dialling nothing is right, giving up is not.
	if dialled != 0 {
		t.Errorf("expected no dials with no lines, got %d", dialled)
	}
}

func TestStreamURL(t *testing.T) {
	if got := StreamURL(Line{URL: "https://a.example"}, "/mt/ws"); got != "wss://a.example/mt/ws" {
		t.Errorf("got %q", got)
	}
	if got := StreamURL(Line{URL: "http://a.example"}, "/mt/ws"); got != "ws://a.example/mt/ws" {
		t.Errorf("got %q", got)
	}
	// A same-origin line leaves the scheme to whatever the caller is already using.
	if got := StreamURL(Line{URL: ""}, "/mt/ws"); got != "/mt/ws" {
		t.Errorf("got %q", got)
	}
}

// The policy on its own, for a consumer that already owns its reconnect loop — a transport that has
// to speak a handshake and decide what a drop means has a loop belonging to that protocol, and
// reimplementing the line policy beside it would duplicate exactly the judgement calls this file
// exists to hold.

func selectorLines(ids ...string) func() []Line {
	lines := make([]Line, 0, len(ids))
	for _, id := range ids {
		lines = append(lines, Line{ID: id, URL: "https://" + id + ".example"})
	}
	return func() []Line { return lines }
}

func TestSelectorPrefersTheFirstUnpenalisedLine(t *testing.T) {
	selector := NewStreamSelector(selectorLines("a", "b"), 5*time.Second, time.Minute, nil)

	line, ok := selector.Next()
	if !ok || line.ID != "a" {
		t.Fatalf("expected the first line, got %+v (%v)", line, ok)
	}

	// Dropped before it was stable: this line cannot hold a stream right now.
	selector.Closed(line, time.Second)

	if next, _ := selector.Next(); next.ID != "b" {
		t.Errorf("a line that could not hold a stream was chosen again: %+v", next)
	}
}

// The distinction the whole policy turns on: a connection that lasted and then dropped is an
// ordinary disconnection. Held against the line, every line would eventually be penalised for the
// network being a network.
func TestSelectorDoesNotPenaliseAConnectionThatLasted(t *testing.T) {
	selector := NewStreamSelector(selectorLines("a", "b"), 5*time.Second, time.Minute, nil)

	line, _ := selector.Next()
	selector.Closed(line, time.Hour)

	if next, _ := selector.Next(); next.ID != "a" {
		t.Errorf("an ordinary disconnection cost the line its place: %+v", next)
	}
	if len(selector.Penalties()) != 0 {
		t.Errorf("nothing should have been penalised: %v", selector.Penalties())
	}
}

func TestSelectorStillReturnsALineWhenEveryLineIsPenalised(t *testing.T) {
	selector := NewStreamSelector(selectorLines("a", "b"), 5*time.Second, time.Minute, nil)

	first, _ := selector.Next()
	selector.Closed(first, 0)
	second, _ := selector.Next()
	selector.Closed(second, 0)

	// A client with no connection is worse than one on a flaky connection, and the penalties may
	// all be stale by now anyway.
	if _, ok := selector.Next(); !ok {
		t.Error("refused to choose any line, leaving the consumer with no connection at all")
	}
}

func TestSelectorReportsWhatIsCarryingTheStream(t *testing.T) {
	selector := NewStreamSelector(selectorLines("a", "b"), 5*time.Second, time.Minute, nil)

	if selector.Current() != nil {
		t.Error("nothing is connected yet")
	}
	line, _ := selector.Next()
	selector.Opened(line)
	if current := selector.Current(); current == nil || current.ID != "a" {
		t.Errorf("expected to be told which line is carrying it, got %+v", current)
	}
	selector.Closed(line, time.Hour)
	if selector.Current() != nil {
		t.Error("still reporting a line after the connection ended")
	}
}

func TestSelectorWaitsWhenThereAreNoLinesYet(t *testing.T) {
	selector := NewStreamSelector(func() []Line { return nil }, 0, 0, nil)

	if _, ok := selector.Next(); ok {
		t.Error("a registry that has not arrived is not a line to dial")
	}
}
