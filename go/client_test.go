// The same cases as ts/test/strategy.test.ts, against real HTTP servers rather than fakes — a Go
// test can afford httptest, and a hedge is about timing, which a fake transport models badly.

package multipath

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// line spins up a server that behaves as told, and returns a registry line pointing at it.
func line(t *testing.T, id string, handler http.HandlerFunc) Line {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return Line{ID: id, URL: server.URL}
}

func answers(id string, after time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(after):
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(id))
	}
}

// never accepts the connection and answers nothing — a black-holed route, as seen from a client.
func never() http.HandlerFunc {
	return func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }
}

func read(t *testing.T, response *http.Response) string {
	t.Helper()
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return string(body)
}

func TestGetUsesOneLineWhenItAnswers(t *testing.T) {
	var second atomic.Int32
	fast := line(t, "fast", answers("fast", 0))
	slow := line(t, "slow", func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		answers("slow", 0)(w, r)
	})

	client := New(Options{
		Registry:   Registry{Lines: []Line{fast, slow}},
		HedgeAfter: 200 * time.Millisecond,
	})
	response, err := client.Get(context.Background(), "/x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := read(t, response); got != "fast" {
		t.Errorf("got %q", got)
	}
	// The whole reason for hedging rather than fanning out: on a healthy line the extra request is
	// never sent at all.
	if second.Load() != 0 {
		t.Errorf("the second line should not have been asked, got %d requests", second.Load())
	}
}

func TestGetHedgesWhenTheBestLineIsSlow(t *testing.T) {
	slow := line(t, "slow", answers("slow", 2*time.Second))
	fast := line(t, "fast", answers("fast", 0))

	client := New(Options{
		Registry:   Registry{Lines: []Line{slow, fast}},
		HedgeAfter: 50 * time.Millisecond,
	})
	started := time.Now()
	response, err := client.Get(context.Background(), "/x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := read(t, response); got != "fast" {
		t.Errorf("got %q", got)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("waited %v — the hedge did not fire", elapsed)
	}
}

func TestGetSurvivesABlackHole(t *testing.T) {
	stalled := line(t, "stalled", never())
	fast := line(t, "fast", answers("fast", 0))

	client := New(Options{
		Registry:   Registry{Lines: []Line{stalled, fast}},
		HedgeAfter: 50 * time.Millisecond,
	})
	response, err := client.Get(context.Background(), "/x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := read(t, response); got != "fast" {
		t.Errorf("got %q", got)
	}
}

func TestGetDoesNotWaitOutTheHedgeAfterAnImmediateFailure(t *testing.T) {
	// Nothing listening: the connection is refused at once.
	broken := Line{ID: "broken", URL: "http://127.0.0.1:1"}
	fast := line(t, "fast", answers("fast", 0))

	client := New(Options{
		Registry:   Registry{Lines: []Line{broken, fast}},
		HedgeAfter: 2 * time.Second,
	})
	started := time.Now()
	response, err := client.Get(context.Background(), "/x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = read(t, response)
	// Waiting for company that will never come is pure delay.
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("waited %v for a line that failed immediately", elapsed)
	}
}

// An error status is an answer, not a routing failure. Treating a 404 as "try the next line" would
// ask every line for something that does not exist, on every such request, and still end in a 404.
func TestGetAcceptsAnErrorStatusAsTheAnswer(t *testing.T) {
	var secondAsked atomic.Int32
	missing := line(t, "missing", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	other := line(t, "other", func(w http.ResponseWriter, r *http.Request) {
		secondAsked.Add(1)
		answers("other", 0)(w, r)
	})

	client := New(Options{
		Registry:   Registry{Lines: []Line{missing, other}},
		HedgeAfter: 200 * time.Millisecond,
	})
	response, err := client.Get(context.Background(), "/x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusNotFound {
		t.Errorf("expected the 404 to be the answer, got %d", response.StatusCode)
	}
	if secondAsked.Load() != 0 {
		t.Error("a 404 must not be routed around")
	}
}

func TestGetFailsOnlyWhenEveryLineHasFailed(t *testing.T) {
	client := New(Options{
		Registry: Registry{Lines: []Line{
			{ID: "a", URL: "http://127.0.0.1:1"},
			{ID: "b", URL: "http://127.0.0.1:2"},
		}},
		HedgeAfter: 10 * time.Millisecond,
	})
	if _, err := client.Get(context.Background(), "/x"); err == nil {
		t.Error("expected an error when nothing works")
	}
}

func TestGetRefusesAnEmptyRegistry(t *testing.T) {
	client := New(Options{})
	if _, err := client.Get(context.Background(), "/x"); err != ErrNoLine {
		t.Errorf("expected ErrNoLine, got %v", err)
	}
}

func TestWriteUsesOneLineAndIsNeverRaced(t *testing.T) {
	var firstSeen, secondSeen atomic.Int32
	first := line(t, "first", func(w http.ResponseWriter, _ *http.Request) {
		firstSeen.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	second := line(t, "second", func(w http.ResponseWriter, _ *http.Request) {
		secondSeen.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	client := New(Options{Registry: Registry{Lines: []Line{first, second}}})
	response, err := client.Write(context.Background(), http.MethodPost, "/w", []byte("{}"), "k1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = response.Body.Close()

	// Two writes are two writes, however good the de-duplication is.
	if firstSeen.Load() != 1 || secondSeen.Load() != 0 {
		t.Errorf("first=%d second=%d", firstSeen.Load(), secondSeen.Load())
	}
}

func TestWriteCarriesTheKeyAndFailsOverUnderIt(t *testing.T) {
	keys := make(chan string, 2)
	broken := Line{ID: "broken", URL: "http://127.0.0.1:1"}
	working := line(t, "working", func(w http.ResponseWriter, r *http.Request) {
		keys <- r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
	})

	client := New(Options{Registry: Registry{Lines: []Line{broken, working}}})
	response, err := client.Write(context.Background(), http.MethodPost, "/w", []byte("{}"), "same-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = response.Body.Close()

	// The key is what makes failing over safe: the server can recognise the second arrival as one
	// attempt rather than two writes.
	if got := <-keys; got != "same-key" {
		t.Errorf("expected the key to survive the failover, got %q", got)
	}
}

// A response ends the matter, whatever it says. Only a transport failure leaves it unknown whether
// the write happened; a 500 means the request arrived and the server decided.
func TestWriteDoesNotFailOverOnAServerError(t *testing.T) {
	var secondSeen atomic.Int32
	failing := line(t, "failing", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	second := line(t, "second", func(w http.ResponseWriter, _ *http.Request) {
		secondSeen.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	client := New(Options{Registry: Registry{Lines: []Line{failing, second}}})
	response, err := client.Write(context.Background(), http.MethodPost, "/w", nil, "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected the 500 to be returned, got %d", response.StatusCode)
	}
	if secondSeen.Load() != 0 {
		t.Error("a 500 must not be failed over")
	}
}

func TestWriteStopsAfterTheAttemptCap(t *testing.T) {
	var thirdSeen atomic.Int32
	third := line(t, "third", func(w http.ResponseWriter, _ *http.Request) {
		thirdSeen.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	client := New(Options{
		Registry: Registry{Lines: []Line{
			{ID: "a", URL: "http://127.0.0.1:1"},
			{ID: "b", URL: "http://127.0.0.1:2"},
			third,
		}},
		MaxWriteAttempts: 2,
	})
	if _, err := client.Write(context.Background(), http.MethodPost, "/w", nil, "k"); err == nil {
		t.Error("expected an error once the cap was reached")
	}
	// A client that keeps failing over forever hammers a struggling origin from every direction.
	if thirdSeen.Load() != 0 {
		t.Error("the third line should never have been asked")
	}
}

func TestWriteHonoursCancellation(t *testing.T) {
	var secondSeen atomic.Int32
	stalled := line(t, "stalled", never())
	second := line(t, "second", func(w http.ResponseWriter, _ *http.Request) {
		secondSeen.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	client := New(Options{Registry: Registry{Lines: []Line{stalled, second}}})
	if _, err := client.Write(ctx, http.MethodPost, "/w", nil, "k"); err == nil {
		t.Error("expected the cancellation to surface")
	}
	// Failing over after a cancellation would be sending a write the caller asked us not to send.
	if secondSeen.Load() != 0 {
		t.Error("no line should be tried after the caller cancelled")
	}
}

func TestProbeRanksTheReachableLineFirst(t *testing.T) {
	dead := Line{ID: "dead", URL: "http://127.0.0.1:1"}
	alive := line(t, "alive", answers("ok", 0))

	client := New(Options{Registry: Registry{Lines: []Line{dead, alive}}})
	for i := 0; i < 3; i++ {
		client.Probe(context.Background(), "/mt/probe")
	}

	if got := ids(client.Ranked()); !equal(got, []string{"alive", "dead"}) {
		t.Errorf("got %v", got)
	}
	if state := client.Health().Get("dead").State; state != StateDown {
		t.Errorf("expected the dead line down, got %q", state)
	}
}

func TestProbeCountsANonSuccessAsAFailure(t *testing.T) {
	broken := line(t, "broken", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	client := New(Options{Registry: Registry{Lines: []Line{broken}}})
	client.Probe(context.Background(), "/mt/probe")

	// Reachable is not the same as working.
	if got := client.Health().Get("broken").ConsecutiveFailures; got != 1 {
		t.Errorf("expected one failure, got %d", got)
	}
}

func TestSetRegistryForgetsDepartedLines(t *testing.T) {
	alive := line(t, "alive", answers("ok", 0))
	client := New(Options{Registry: Registry{Lines: []Line{alive, {ID: "gone", URL: "http://127.0.0.1:1"}}}})
	client.Probe(context.Background(), "/mt/probe")

	client.SetRegistry(Registry{Lines: []Line{alive}})
	if client.Health().Get("gone").Measured {
		t.Error("expected the departed line to be forgotten")
	}
}

// The winner is usually the only line asked — that is what makes hedging affordable — so the
// clean-up has to be sized by what was actually sent. Counting lines instead of requests leaves a
// goroutine blocked forever on answers that were never going to arrive: harmless in a page that
// gets reloaded, fatal in a connector that runs for weeks.
func TestGetLeavesNothingBehindWhenTheFirstLineWins(t *testing.T) {
	fast := line(t, "fast", answers("fast", 0))
	client := New(Options{
		Registry: Registry{Lines: []Line{
			fast,
			{ID: "b", URL: "http://127.0.0.1:1"},
			{ID: "c", URL: "http://127.0.0.1:1"},
		}},
		// Long enough that the hedge never fires: only the first line is ever asked.
		HedgeAfter: time.Hour,
	})

	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		response, err := client.Get(context.Background(), "/x")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_ = read(t, response)
	}

	// Idle transport goroutines come and go, so allow a little slack — a leak of one per read shows
	// up as twenty, not as three.
	settle(t, before+5, 2*time.Second)
}

// settle waits for the goroutine count to come back under limit, failing if it never does.
func settle(t *testing.T, limit int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		count := runtime.NumGoroutine()
		if count <= limit {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("still %d goroutines after %v, expected at most %d", count, within, limit)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
