// The measurement loop for the Go client: cheap and constant for latency, rare and careful for
// throughput.
//
// The mirror of ts/src/prober.ts, including the defaults, because the connector and the browser have
// to mean the same thing by "this line is slow". Two rules shape it, and both are borrowed from
// there because both are about measurement rather than about a language:
//
// A probe must not distort what it measures. Latency probes are tiny and spaced out; the throughput
// probe moves real bytes, so it only runs after the application has been quiet — measuring bandwidth
// during a burst of traffic reports a number the measurement itself caused.
//
// A failing line must not be probed as often as a healthy one. A line that is down backs off
// exponentially, so a dead route costs a request every few minutes rather than every few seconds.
//
// Client.Probe measures once, which is all a short-lived command needs. This is for the other kind
// of consumer: a connector that stays up for weeks, where "which line is best" is a question whose
// answer changes while the process is running.

package multipath

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// ProberOptions tunes the loop. Zero values take the defaults.
type ProberOptions struct {
	// ProbePath is the unauthenticated liveness endpoint.
	ProbePath string
	// Interval is the gap between probes of a healthy line.
	Interval time.Duration
	// Timeout is when a probe that has not answered counts as a failure.
	Timeout time.Duration
	// MaxBackoff caps the wait for a line that keeps failing.
	MaxBackoff time.Duration

	// BandwidthPath is a download used to measure throughput. Empty disables throughput probing
	// entirely, which is the right default: it costs real bytes, and a consumer that has not
	// deployed such an endpoint should not be guessing at one.
	BandwidthPath string
	// BandwidthInterval is the minimum gap between throughput measurements of the same line.
	BandwidthInterval time.Duration
	// IdleBeforeBandwidth is how long the application must have been quiet before one may run.
	IdleBeforeBandwidth time.Duration

	// Now is injected in tests.
	Now func() time.Time
}

const (
	defaultProbePath           = "/mt/probe"
	defaultProbeInterval       = 15 * time.Second
	defaultProbeTimeout        = 5 * time.Second
	defaultMaxBackoff          = 5 * time.Minute
	defaultBandwidthInterval   = 10 * time.Minute
	defaultIdleBeforeBandwidth = 30 * time.Second
)

// Prober keeps a client's health table current.
//
// Deliberately not started by New: probing costs real requests, and a library that begins making
// them the moment it is constructed is one that surprises people. The caller says when.
type Prober struct {
	client *Client
	opts   ProberOptions

	mu sync.Mutex
	// Next allowed probe time per line — how backoff is expressed.
	nextProbeAt     map[string]time.Time
	nextBandwidthAt map[string]time.Time
	lastTrafficAt   time.Time
}

// Prober builds the measurement loop for this client, and wires it up to notice real traffic so the
// throughput probe can tell quiet from busy.
func (c *Client) Prober(opts ProberOptions) *Prober {
	if opts.ProbePath == "" {
		opts.ProbePath = defaultProbePath
	}
	if opts.Interval == 0 {
		opts.Interval = defaultProbeInterval
	}
	if opts.Timeout == 0 {
		opts.Timeout = defaultProbeTimeout
	}
	if opts.MaxBackoff == 0 {
		opts.MaxBackoff = defaultMaxBackoff
	}
	if opts.BandwidthInterval == 0 {
		opts.BandwidthInterval = defaultBandwidthInterval
	}
	if opts.IdleBeforeBandwidth == 0 {
		opts.IdleBeforeBandwidth = defaultIdleBeforeBandwidth
	}
	if opts.Now == nil {
		opts.Now = c.opts.Now
	}

	prober := &Prober{
		client:          c,
		opts:            opts,
		nextProbeAt:     map[string]time.Time{},
		nextBandwidthAt: map[string]time.Time{},
	}
	c.setTrafficObserver(prober.NoteTraffic)
	return prober
}

// NoteTraffic records that the application just made a real request. Called by the client itself;
// exported because a consumer whose traffic does not go through this client (a WebSocket carrying
// the bulk of it, say) knows about activity this package cannot see.
func (p *Prober) NoteTraffic() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastTrafficAt = p.opts.Now()
}

// Run measures until ctx is cancelled, then returns.
//
// Blocking rather than start/stop: a goroutine the caller owns is easier to reason about than one a
// library hid, and it makes the lifetime of the loop exactly the lifetime of a context that
// something else already manages.
func (p *Prober) Run(ctx context.Context) {
	ticker := time.NewTicker(p.opts.Interval)
	defer ticker.Stop()
	for {
		p.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ProbeAll measures every line now, ignoring backoff. The diagnostic lever, and every test's.
func (p *Prober) ProbeAll(ctx context.Context) {
	p.probeLines(ctx, p.client.Lines())
}

func (p *Prober) tick(ctx context.Context) {
	now := p.opts.Now()

	p.mu.Lock()
	due := make([]Line, 0)
	for _, line := range p.client.Lines() {
		if next, ok := p.nextProbeAt[line.ID]; !ok || !next.After(now) {
			due = append(due, line)
		}
	}
	p.mu.Unlock()

	p.probeLines(ctx, due)

	if p.opts.BandwidthPath != "" {
		p.maybeMeasureThroughput(ctx)
	}
}

func (p *Prober) probeLines(ctx context.Context, lines []Line) {
	var wg sync.WaitGroup
	for _, line := range lines {
		wg.Add(1)
		go func(line Line) {
			defer wg.Done()
			p.probe(ctx, line)
		}(line)
	}
	wg.Wait()
	p.client.health.ReconcileDegraded()
}

func (p *Prober) probe(ctx context.Context, line Line) {
	attemptCtx, cancel := context.WithTimeout(ctx, p.opts.Timeout)
	defer cancel()

	response, latency, err := p.client.sendRaw(
		attemptCtx, line, http.MethodGet, p.opts.ProbePath, nil, nil, "")
	if err == nil {
		// Drained and closed, or the connection cannot be reused and every probe pays for a fresh
		// handshake — which would have the probe measuring its own overhead.
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			err = fmt.Errorf("probe answered %d", response.StatusCode)
		}
	}
	if err == nil {
		p.client.health.RecordSuccess(line.ID, latency, p.opts.Now())
	} else {
		p.client.health.RecordFailure(line.ID, err, p.opts.Now())
	}

	now := p.opts.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil {
		p.nextProbeAt[line.ID] = now.Add(p.opts.Interval)
		return
	}
	p.nextProbeAt[line.ID] = now.Add(p.backoff(p.client.health.Get(line.ID).ConsecutiveFailures))
}

// maybeMeasureThroughput measures one line, if the application has been quiet long enough.
//
// One line per round, never all of them: several at once would compete for the same pipe and each
// report a fraction of the truth.
func (p *Prober) maybeMeasureThroughput(ctx context.Context) {
	now := p.opts.Now()

	p.mu.Lock()
	if now.Sub(p.lastTrafficAt) < p.opts.IdleBeforeBandwidth {
		p.mu.Unlock()
		return
	}
	var candidate *Line
	for _, line := range p.client.Lines() {
		if p.client.health.Get(line.ID).State == StateDown {
			continue
		}
		if next, ok := p.nextBandwidthAt[line.ID]; ok && next.After(now) {
			continue
		}
		chosen := line
		candidate = &chosen
		break
	}
	if candidate == nil {
		p.mu.Unlock()
		return
	}
	// Claim the slot before measuring, so a failure cannot make it retry on the very next tick.
	p.nextBandwidthAt[candidate.ID] = now.Add(p.opts.BandwidthInterval)
	p.mu.Unlock()

	started := p.opts.Now()
	response, _, err := p.client.sendRaw(
		ctx, *candidate, http.MethodGet, p.opts.BandwidthPath, nil, nil, "")
	if err != nil {
		return
	}
	bytes, err := io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if err != nil {
		return
	}
	seconds := p.opts.Now().Sub(started).Seconds()
	if seconds > 0 && bytes > 0 {
		p.client.health.RecordThroughput(candidate.ID, float64(bytes)/seconds)
	}
}

// backoff grows exponentially and is capped: a dead line costs one request every few minutes rather
// than one every few seconds.
func (p *Prober) backoff(consecutiveFailures int) time.Duration {
	grown := p.opts.Interval
	for i := 0; i < consecutiveFailures && i < 10; i++ {
		grown *= 2
		if grown >= p.opts.MaxBackoff {
			return p.opts.MaxBackoff
		}
	}
	return grown
}
