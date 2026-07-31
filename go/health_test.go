// Held to the same cases as ts/test/health.test.ts, deliberately. The connector and the browser
// have to mean the same thing by "a line"; a behaviour that only one of them checks is one they
// will eventually disagree about.

package multipath

import (
	"errors"
	"testing"
	"time"
)

var testLines = []Line{
	{ID: "cf", Weight: 100},
	{ID: "ipv6", Weight: 50},
	{ID: "frp", Weight: 10},
}

func ids(lines []Line) []string {
	out := make([]string, len(lines))
	for i, line := range lines {
		out[i] = line.ID
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestUnmeasuredLineIsUp(t *testing.T) {
	// A client that has only just started still has to send requests somewhere.
	if got := NewHealthTable(HealthOptions{}).Get("cf").State; got != StateUp {
		t.Errorf("expected an unmeasured line to be up, got %q", got)
	}
}

func TestFallsBackToWeightWhenNothingIsKnown(t *testing.T) {
	got := ids(NewHealthTable(HealthOptions{}).Rank(testLines))
	if !equal(got, []string{"cf", "ipv6", "frp"}) {
		t.Errorf("got %v", got)
	}
}

func TestMeasurementOutranksWeight(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	now := time.Now()
	health.RecordSuccess("cf", 300*time.Millisecond, now)
	health.RecordSuccess("ipv6", 20*time.Millisecond, now)
	health.RecordSuccess("frp", 90*time.Millisecond, now)

	// frp has the lowest weight and the second-best latency; measurement wins.
	if got := ids(health.Rank(testLines)); !equal(got, []string{"ipv6", "frp", "cf"}) {
		t.Errorf("got %v", got)
	}
}

func TestMeasuredBeatsUnknown(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	health.RecordSuccess("frp", 500*time.Millisecond, time.Now())
	// Even at 500ms, frp is known to work and the others are only assumed to.
	if got := ids(health.Rank(testLines))[0]; got != "frp" {
		t.Errorf("expected frp first, got %q", got)
	}
}

func TestDownLineIsRankedLastButOffered(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	health.RecordSuccess("cf", 10*time.Millisecond, time.Now())
	for i := 0; i < 3; i++ {
		health.RecordFailure("ipv6", errors.New("nope"), time.Now())
	}

	ranked := ids(health.Rank(testLines))
	if ranked[len(ranked)-1] != "ipv6" {
		t.Errorf("expected ipv6 last, got %v", ranked)
	}
	// If everything is down the caller still has to send the request somewhere.
	if len(ranked) != 3 {
		t.Errorf("a down line must still be offered, got %v", ranked)
	}
}

func TestThroughputBreaksALatencyTie(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	now := time.Now()
	health.RecordSuccess("cf", 50*time.Millisecond, now)
	health.RecordSuccess("ipv6", 50*time.Millisecond, now)
	health.RecordThroughput("cf", 1000)
	health.RecordThroughput("ipv6", 9000)

	// The point of measuring throughput at all: equal latency, very unequal usefulness.
	if got := ids(health.Rank(testLines[:2])); !equal(got, []string{"ipv6", "cf"}) {
		t.Errorf("got %v", got)
	}
}

func TestOneFailureIsNoise(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	health.RecordSuccess("cf", 10*time.Millisecond, time.Now())
	health.RecordFailure("cf", errors.New("blip"), time.Now())

	// Demoting here would take a line out of service for a dropped packet.
	if got := health.Get("cf").State; got != StateUp {
		t.Errorf("expected still up after one failure, got %q", got)
	}
}

func TestARunOfFailuresIsAFact(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	for i := 0; i < 3; i++ {
		health.RecordFailure("cf", errors.New("nope"), time.Now())
	}
	if got := health.Get("cf").State; got != StateDown {
		t.Errorf("expected down, got %q", got)
	}
}

func TestRecoversOnTheNextSuccess(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	for i := 0; i < 3; i++ {
		health.RecordFailure("cf", errors.New("nope"), time.Now())
	}
	health.RecordSuccess("cf", 12*time.Millisecond, time.Now())

	entry := health.Get("cf")
	if entry.State != StateUp || entry.ConsecutiveFailures != 0 || entry.LastError != "" {
		t.Errorf("expected a clean recovery, got %+v", entry)
	}
}

func TestSeedsOnTheFirstSample(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	health.RecordSuccess("cf", 100*time.Millisecond, time.Now())
	// Averaging up from zero would make a new line look impossibly fast and win a ranking it has
	// not earned.
	if got := health.Get("cf").Latency; got != 100*time.Millisecond {
		t.Errorf("expected the first sample to seed the average, got %v", got)
	}
}

func TestMovesTowardANewSampleWithoutJumping(t *testing.T) {
	health := NewHealthTable(HealthOptions{Smoothing: 0.5})
	health.RecordSuccess("cf", 100*time.Millisecond, time.Now())
	health.RecordSuccess("cf", 200*time.Millisecond, time.Now())
	if got := health.Get("cf").Latency; got != 150*time.Millisecond {
		t.Errorf("expected 150ms, got %v", got)
	}
}

func TestDegradedLabelling(t *testing.T) {
	health := NewHealthTable(HealthOptions{DegradedFactor: 4})
	now := time.Now()
	health.RecordSuccess("cf", 20*time.Millisecond, now)
	health.RecordSuccess("ipv6", 500*time.Millisecond, now)
	health.ReconcileDegraded()

	if health.Get("cf").State != StateUp || health.Get("ipv6").State != StateDegraded {
		t.Errorf("cf=%q ipv6=%q", health.Get("cf").State, health.Get("ipv6").State)
	}
}

func TestDegradedNeverPromotesADownLine(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	health.RecordSuccess("cf", 20*time.Millisecond, time.Now())
	for i := 0; i < 3; i++ {
		health.RecordFailure("ipv6", errors.New("nope"), time.Now())
	}
	health.ReconcileDegraded()

	if got := health.Get("ipv6").State; got != StateDown {
		t.Errorf("expected still down, got %q", got)
	}
}

func TestRetainForgetsDepartedLines(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	health.RecordSuccess("cf", 10*time.Millisecond, time.Now())
	health.RecordSuccess("gone", 10*time.Millisecond, time.Now())
	health.Retain([]string{"cf"})

	// A re-added id must not inherit the reputation of whatever used to be called that.
	if health.Get("gone").Measured {
		t.Error("expected the departed line to be forgotten")
	}
	if len(health.All()) != 1 {
		t.Errorf("expected one entry, got %d", len(health.All()))
	}
}

// The prober writes from its own goroutine while requests read. The race detector runs in CI.
func TestConcurrentUse(t *testing.T) {
	health := NewHealthTable(HealthOptions{})
	done := make(chan struct{})

	go func() {
		for i := 0; i < 500; i++ {
			health.RecordSuccess("cf", time.Duration(i)*time.Millisecond, time.Now())
		}
		close(done)
	}()
	for i := 0; i < 500; i++ {
		health.Rank(testLines)
	}
	<-done
}
