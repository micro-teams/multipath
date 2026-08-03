// The measurement loop, held to the same cases as ts/test/prober.test.ts.
//
// Time is injected rather than waited out: a test that sleeps for a backoff window is a test that
// either takes minutes or is flaky, and usually both.

package multipath

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// clock is a hand-wound time source, so backoff can be observed without waiting for it.
type clock struct{ at atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.at.Store(time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}
func (c *clock) now() time.Time          { return time.Unix(0, c.at.Load()) }
func (c *clock) advance(d time.Duration) { c.at.Add(int64(d)) }

func TestProbeAllRanksTheReachableLineFirst(t *testing.T) {
	dead := Line{ID: "dead", URL: "http://127.0.0.1:1"}
	alive := line(t, "alive", answers("ok", 0))

	client := New(Options{Registry: Registry{Lines: []Line{dead, alive}}})
	client.Prober(ProberOptions{ProbePath: "/probe"}).ProbeAll(context.Background())

	if got := client.Ranked()[0].ID; got != "alive" {
		t.Errorf("ranked %q first", got)
	}
}

// A probe that answers 500 answers promptly, and a loop that took prompt for healthy would leave a
// broken line at the top of the ranking indefinitely.
func TestProbeTreatsANonSuccessAsAFailure(t *testing.T) {
	broken := line(t, "broken", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	client := New(Options{Registry: Registry{Lines: []Line{broken}}})
	prober := client.Prober(ProberOptions{ProbePath: "/probe"})
	for i := 0; i < 3; i++ {
		prober.ProbeAll(context.Background())
	}

	health := client.Health().Get("broken")
	if health.State != StateDown {
		t.Errorf("state is %q after three 500s", health.State)
	}
	if health.ConsecutiveFailures != 3 {
		t.Errorf("counted %d failures, expected 3", health.ConsecutiveFailures)
	}
}

// The point of backoff: a dead route costs a request every few minutes rather than every few
// seconds, so a tick that comes round on schedule must not probe it again yet.
func TestARunOfFailuresBacksTheLineOff(t *testing.T) {
	var asked atomic.Int32
	flaky := line(t, "flaky", func(w http.ResponseWriter, _ *http.Request) {
		asked.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})

	time_ := newClock()
	client := New(Options{Registry: Registry{Lines: []Line{flaky}}, Now: time_.now})
	prober := client.Prober(ProberOptions{
		ProbePath: "/probe",
		Interval:  10 * time.Second,
		Now:       time_.now,
	})

	prober.tick(context.Background())
	if asked.Load() != 1 {
		t.Fatalf("expected the first tick to probe once, got %d", asked.Load())
	}

	// One failure already doubles the wait, so a tick at the plain interval is too early.
	time_.advance(10 * time.Second)
	prober.tick(context.Background())
	if asked.Load() != 1 {
		t.Errorf("probed again after %v, which is inside the backoff", 10*time.Second)
	}

	time_.advance(30 * time.Second)
	prober.tick(context.Background())
	if asked.Load() != 2 {
		t.Errorf("never came back to the line: %d probes", asked.Load())
	}
}

func TestBackoffIsCapped(t *testing.T) {
	client := New(Options{Registry: Registry{Lines: []Line{{ID: "x", URL: "http://127.0.0.1:1"}}}})
	prober := client.Prober(ProberOptions{Interval: time.Second, MaxBackoff: 30 * time.Second})

	if got := prober.backoff(0); got != time.Second {
		t.Errorf("a healthy line waits %v, expected the plain interval", got)
	}
	if got := prober.backoff(100); got != 30*time.Second {
		t.Errorf("backoff grew to %v, past the cap", got)
	}
}

// Throughput costs real bytes, so it waits for quiet — and a probe is not traffic. If probing
// counted as activity the application would never look quiet and this would never run at all.
func TestThroughputWaitsForTheApplicationToBeQuiet(t *testing.T) {
	var downloads atomic.Int32
	payload := make([]byte, 64*1024)
	clockUnderTest := newClock()
	server := line(t, "only", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bandwidth" {
			downloads.Add(1)
			// The download takes time on the injected clock too, or the rate would be a division
			// by zero and nothing would be recorded.
			clockUnderTest.advance(100 * time.Millisecond)
			_, _ = w.Write(payload)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	client := New(Options{Registry: Registry{Lines: []Line{server}}, Now: clockUnderTest.now})
	prober := client.Prober(ProberOptions{
		ProbePath:           "/probe",
		BandwidthPath:       "/bandwidth",
		IdleBeforeBandwidth: time.Minute,
		Now:                 clockUnderTest.now,
	})

	// A real request means busy.
	response, err := client.Get(context.Background(), "/x")
	if err != nil {
		t.Fatal(err)
	}
	_ = read(t, response)

	prober.tick(context.Background())
	if downloads.Load() != 0 {
		t.Errorf("measured throughput while the application was busy")
	}

	clockUnderTest.advance(2 * time.Minute)
	prober.tick(context.Background())
	if downloads.Load() != 1 {
		t.Errorf("did not measure throughput after two minutes of quiet: %d", downloads.Load())
	}
	if client.Health().Get("only").ThroughputBps <= 0 {
		t.Error("nothing was recorded for throughput")
	}

	// One measurement per line per interval: the number changes slowly and the bytes are not free.
	prober.tick(context.Background())
	if downloads.Load() != 1 {
		t.Errorf("measured again immediately: %d", downloads.Load())
	}
}

func TestRunStopsWithItsContext(t *testing.T) {
	server := line(t, "only", answers("ok", 0))
	client := New(Options{Registry: Registry{Lines: []Line{server}}})
	prober := client.Prober(ProberOptions{ProbePath: "/probe", Interval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { prober.Run(ctx); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return when its context was cancelled")
	}
}
