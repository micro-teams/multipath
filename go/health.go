// Health tracking for the Go client.
//
// A deliberate mirror of ts/src/health.ts, down to the defaults and the judgement calls, because
// the connector and the browser have to mean the same thing by "a line" — a CLI that ranked lines
// differently from the app would make "which line is slow" a question with two answers.
//
// The judgements worth restating here, since they are the ones a reader will want to argue with:
// an unmeasured line is up rather than down, because a client that has only just started has to
// send requests somewhere; one failure is noise and a run of them is a fact, because demoting on
// the first would take a line out of service for a dropped packet; and a down line is ranked last
// but never removed, because if everything is down, trying the least-bad option beats refusing to
// try.

package multipath

import (
	"sort"
	"sync"
	"time"
)

// LineState is how usable a line looks right now.
type LineState string

const (
	// StateUp means the line is answering.
	StateUp LineState = "up"
	// StateDegraded means it answers, but slowly or with recent errors: usable, ranked last.
	StateDegraded LineState = "degraded"
	// StateDown means it is not answering, and is skipped until a probe says otherwise.
	StateDown LineState = "down"
)

// LineHealth is everything measured about one line.
type LineHealth struct {
	LineID string
	State  LineState
	// Latency is the smoothed round-trip time; zero with Measured false means nothing is known.
	Latency  time.Duration
	Measured bool
	// ThroughputBps is the smoothed delivery rate, zero when never measured.
	ThroughputBps       float64
	ConsecutiveFailures int
	LastError           string
	LastProbedAt        time.Time
}

// HealthOptions tunes the table. Zero values take the defaults.
type HealthOptions struct {
	// Smoothing is the weight of each new sample, between 0 and 1.
	//
	// 0.3 keeps roughly the last handful of probes in view: quick enough to notice a line going
	// bad within seconds, slow enough that one unlucky sample does not reorder the ranking for
	// long. A ranking that flaps is worse than a slightly stale one, because every flap moves
	// traffic.
	Smoothing float64
	// FailuresBeforeDown is how many consecutive failures stop being noise and become a fact.
	FailuresBeforeDown int
	// DegradedFactor marks a line answering this many times slower than the best as degraded.
	DegradedFactor float64
}

const (
	defaultSmoothing          = 0.3
	defaultFailuresBeforeDown = 3
	defaultDegradedFactor     = 4.0
)

// HealthTable holds what is known about each line. Safe for concurrent use: the prober writes to it
// from its own goroutine while requests read it.
type HealthTable struct {
	mu      sync.RWMutex
	entries map[string]LineHealth
	opts    HealthOptions
}

// NewHealthTable builds a table, filling in any option left at its zero value.
func NewHealthTable(opts HealthOptions) *HealthTable {
	if opts.Smoothing == 0 {
		opts.Smoothing = defaultSmoothing
	}
	if opts.FailuresBeforeDown == 0 {
		opts.FailuresBeforeDown = defaultFailuresBeforeDown
	}
	if opts.DegradedFactor == 0 {
		opts.DegradedFactor = defaultDegradedFactor
	}
	return &HealthTable{entries: map[string]LineHealth{}, opts: opts}
}

// Get returns what is known about a line. A line nobody has measured is up, not down: optimism is
// the safe default, since a client with no measurements still has to send requests somewhere.
func (h *HealthTable) Get(lineID string) LineHealth {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if entry, ok := h.entries[lineID]; ok {
		return entry
	}
	return LineHealth{LineID: lineID, State: StateUp}
}

// All returns every line the table has heard of.
func (h *HealthTable) All() []LineHealth {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]LineHealth, 0, len(h.entries))
	for _, entry := range h.entries {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LineID < out[j].LineID })
	return out
}

// RecordSuccess folds a round-trip time into the average and clears any failure run.
func (h *HealthTable) RecordSuccess(lineID string, latency time.Duration, at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := h.getLocked(lineID)
	entry.Latency = h.blendDuration(entry.Latency, entry.Measured, latency)
	entry.Measured = true
	entry.State = StateUp
	entry.ConsecutiveFailures = 0
	entry.LastError = ""
	entry.LastProbedAt = at
	h.entries[lineID] = entry
}

// RecordFailure counts a failure, demoting the line only once the run is long enough to be a fact.
func (h *HealthTable) RecordFailure(lineID string, err error, at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := h.getLocked(lineID)
	entry.ConsecutiveFailures++
	if entry.ConsecutiveFailures >= h.opts.FailuresBeforeDown {
		entry.State = StateDown
	}
	if err != nil {
		entry.LastError = err.Error()
	}
	entry.LastProbedAt = at
	h.entries[lineID] = entry
}

// RecordThroughput folds a delivery rate into the average.
func (h *HealthTable) RecordThroughput(lineID string, bytesPerSecond float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := h.getLocked(lineID)
	if entry.ThroughputBps == 0 {
		entry.ThroughputBps = bytesPerSecond
	} else {
		entry.ThroughputBps = entry.ThroughputBps*(1-h.opts.Smoothing) + bytesPerSecond*h.opts.Smoothing
	}
	h.entries[lineID] = entry
}

// Retain forgets lines that have left the registry, so a re-added id cannot inherit the reputation
// of whatever used to be called that.
func (h *HealthTable) Retain(lineIDs []string) {
	keep := make(map[string]bool, len(lineIDs))
	for _, id := range lineIDs {
		keep[id] = true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for id := range h.entries {
		if !keep[id] {
			delete(h.entries, id)
		}
	}
}

// Rank orders lines best-first.
//
// Down lines go last rather than being dropped: if every line is down the caller still has to send
// the request somewhere. Among usable lines, measured latency decides, throughput breaks ties
// because two lines that answer equally fast are distinguished by how much they can carry, and
// configured weight decides only when nothing has been measured at all — a hand-set number goes
// stale, a measurement does not.
func (h *HealthTable) Rank(lines []Line) []Line {
	ranked := make([]Line, len(lines))
	copy(ranked, lines)

	sort.SliceStable(ranked, func(i, j int) bool {
		a, b := h.Get(ranked[i].ID), h.Get(ranked[j].ID)

		if stateRank(a.State) != stateRank(b.State) {
			return stateRank(a.State) < stateRank(b.State)
		}
		if a.Measured && b.Measured && a.Latency != b.Latency {
			return a.Latency < b.Latency
		}
		// A line that has answered outranks one that never has: measured beats unknown.
		if a.Measured != b.Measured {
			return a.Measured
		}
		if a.ThroughputBps != b.ThroughputBps {
			return a.ThroughputBps > b.ThroughputBps
		}
		return ranked[i].Weight > ranked[j].Weight
	})
	return ranked
}

// ReconcileDegraded labels lines answering far slower than the best. Purely a label, for diagnosis.
func (h *HealthTable) ReconcileDegraded() {
	h.mu.Lock()
	defer h.mu.Unlock()

	best := time.Duration(0)
	for _, entry := range h.entries {
		if entry.State == StateDown || !entry.Measured {
			continue
		}
		if best == 0 || entry.Latency < best {
			best = entry.Latency
		}
	}
	if best == 0 {
		return
	}

	for id, entry := range h.entries {
		if entry.State == StateDown || !entry.Measured {
			continue
		}
		if float64(entry.Latency) > float64(best)*h.opts.DegradedFactor {
			entry.State = StateDegraded
		} else {
			entry.State = StateUp
		}
		h.entries[id] = entry
	}
}

func (h *HealthTable) getLocked(lineID string) LineHealth {
	if entry, ok := h.entries[lineID]; ok {
		return entry
	}
	return LineHealth{LineID: lineID, State: StateUp}
}

// blendDuration seeds on the first sample rather than averaging up from zero, which would make a
// new line look impossibly fast and win a ranking it has not earned.
func (h *HealthTable) blendDuration(previous time.Duration, measured bool, sample time.Duration) time.Duration {
	if !measured {
		return sample
	}
	return time.Duration(float64(previous)*(1-h.opts.Smoothing) + float64(sample)*h.opts.Smoothing)
}

func stateRank(state LineState) int {
	switch state {
	case StateUp:
		return 0
	case StateDegraded:
		return 1
	default:
		return 2
	}
}
