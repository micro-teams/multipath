// The Go client: the connector's equivalent of the browser's line manager.
//
// Same strategy as ts/src/strategy.ts, and the same reason for it. A read may be asked of several
// lines because two copies of an answer are one answer, so it is hedged: best line first, and only
// if it has not answered shortly are the others asked. A write may not, because two writes are two
// writes, so it goes to one line and only a transport failure moves it to the next — carrying the
// same idempotency key, so the server recognises the second arrival as one attempt.
//
// In both, an error status is an answer rather than something to route around. Only silence leaves
// it unknown whether anything happened, and only silence justifies another line.

package multipath

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Options configures a Client. Zero values take the defaults.
type Options struct {
	Registry Registry
	// HTTP is the transport actually used. Injected so tests need no network, and so a connector
	// can supply its own timeouts and proxy settings.
	HTTP *http.Client
	// HedgeAfter is how long the best line gets alone before the rest are asked, for reads.
	HedgeAfter time.Duration
	// WriteTimeout bounds one write attempt before the next line is tried.
	WriteTimeout time.Duration
	// MaxWriteAttempts caps how many lines a write may be sent to. A client that fails over
	// forever hammers a struggling origin from every direction at once.
	MaxWriteAttempts int
	Health           HealthOptions
	// IdempotencyHeader must match the server filter's.
	IdempotencyHeader string
	// BaseURL is what a same-origin line (url "") means to this client.
	//
	// The browser has an origin already; a connector does not, so the registry entry that reads
	// "same origin as the caller" has to be told what that is. Left empty, RoundTrip infers it from
	// the request it is given, which is what makes the transport drop-in.
	BaseURL string
	// Now is injected in tests.
	Now func() time.Time
}

const (
	defaultHedgeAfter        = 150 * time.Millisecond
	defaultWriteTimeout      = 10 * time.Second
	defaultMaxWriteAttempts  = 3
	defaultIdempotencyHeader = "Idempotency-Key"
)

// Client sends requests over whichever line is currently best.
type Client struct {
	mu       sync.RWMutex
	registry Registry
	health   *HealthTable
	opts     Options
	// onTraffic is how the prober learns the application is busy, so it can keep an expensive
	// throughput measurement out of the way of real requests. Nil until a prober is built.
	onTraffic func()
}

// New builds a client. Nothing is measured until Probe is called: a library that starts making
// network requests the moment it is constructed is one that surprises people.
func New(opts Options) *Client {
	if opts.HTTP == nil {
		opts.HTTP = http.DefaultClient
	}
	if opts.HedgeAfter == 0 {
		opts.HedgeAfter = defaultHedgeAfter
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = defaultWriteTimeout
	}
	if opts.MaxWriteAttempts == 0 {
		opts.MaxWriteAttempts = defaultMaxWriteAttempts
	}
	if opts.IdempotencyHeader == "" {
		opts.IdempotencyHeader = defaultIdempotencyHeader
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Client{registry: opts.Registry, health: NewHealthTable(opts.Health), opts: opts}
}

// setTrafficObserver registers the prober's activity hook.
func (c *Client) setTrafficObserver(observe func()) {
	c.mu.Lock()
	c.onTraffic = observe
	c.mu.Unlock()
}

func (c *Client) notifyTraffic() {
	c.mu.RLock()
	observe := c.onTraffic
	c.mu.RUnlock()
	if observe != nil {
		observe()
	}
}

// SetRegistry swaps the lines at runtime, forgetting health for lines that have gone.
func (c *Client) SetRegistry(registry Registry) {
	c.mu.Lock()
	c.registry = registry
	c.mu.Unlock()

	ids := make([]string, 0, len(registry.Lines))
	for _, line := range registry.Lines {
		ids = append(ids, line.ID)
	}
	c.health.Retain(ids)
}

// Lines returns the registry's lines in their configured order.
func (c *Client) Lines() []Line {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Line, len(c.registry.Lines))
	copy(out, c.registry.Lines)
	return out
}

// Ranked returns the lines best-first.
func (c *Client) Ranked() []Line {
	return c.health.Rank(c.Lines())
}

// Health exposes what has been measured, for a connector's own diagnostics.
func (c *Client) Health() *HealthTable { return c.health }

// ErrNoLine is returned when the registry has nothing to send over.
var ErrNoLine = errors.New("multipath: no line available to serve the request")

// Get performs a hedged read.
//
// The best line is asked first; if it has not answered within HedgeAfter the rest are asked too and
// the first response wins, the others being cancelled. On a healthy line that budget is never spent
// and no second request is ever sent — which is what makes hedging affordable, where always fanning
// out would multiply every request by the number of lines to buy an improvement that only exists on
// the slow tail.
func (c *Client) Get(ctx context.Context, path string) (*http.Response, error) {
	return c.hedgedRead(ctx, http.MethodGet, path, nil, "")
}

func (c *Client) hedgedRead(
	ctx context.Context,
	method, path string,
	header http.Header,
	base string,
) (*http.Response, error) {
	lines := c.Ranked()
	if len(lines) == 0 {
		return nil, ErrNoLine
	}
	c.notifyTraffic()

	// One cancel per attempt, not one for the race.
	//
	// A shared context looks tidier and quietly breaks the winner: net/http returns a response as
	// soon as the headers arrive, and the body is still streaming when the caller gets it — so
	// cancelling the context that carried the winning request kills the body the caller is about to
	// read. Small responses often survive it, because the transport has already buffered them,
	// which is what made this look intermittent rather than broken. Losing the race is what makes a
	// request disposable; winning it is not.
	//
	// The TypeScript side learned this and wrote it down; this side had the same bug anyway.
	cancels := make([]context.CancelFunc, len(lines))
	cancelLosers := func(winner int) {
		for i, cancel := range cancels {
			if i != winner && cancel != nil {
				cancel()
			}
		}
	}
	cancelAll := func() { cancelLosers(-1) }

	// Buffered for every line: a loser that finishes after the winner must be able to report and
	// exit rather than blocking forever on a channel nobody is reading.
	results := make(chan outcome, len(lines))

	launch := func(index int, line Line) {
		attemptCtx, cancel := context.WithCancel(ctx)
		cancels[index] = cancel
		go func() {
			response, err := c.send(attemptCtx, line, method, path, nil, header, base)
			results <- outcome{response, err, index}
		}()
	}

	launch(0, lines[0])
	launched := 1
	hedge := time.NewTimer(c.opts.HedgeAfter)
	defer hedge.Stop()

	var lastErr error
	failures := 0
	for {
		select {
		case <-hedge.C:
			for ; launched < len(lines); launched++ {
				launch(launched, lines[launched])
			}
		case result := <-results:
			if result.err == nil {
				// launched-1, not len(lines)-1: the whole point of hedging is that on a healthy
				// line the others are never asked, so counting lines rather than requests leaves
				// this goroutine waiting for answers nobody is going to send.
				go drainRemaining(results, launched-1)
				cancelLosers(result.index)
				// The winner outlives this function: its body is still streaming, and the caller
				// closes it. Tie its cancellation to the context the caller gave us instead, so
				// nothing leaks when they are done or give up.
				context.AfterFunc(ctx, cancels[result.index])
				return result.response, nil
			}
			lastErr = result.err
			failures++
			if failures == len(lines) {
				cancelAll()
				return nil, lastErr
			}
			// A line that fails immediately should not leave the request waiting out the hedge
			// delay for company it will never get.
			if launched < len(lines) {
				launch(launched, lines[launched])
				launched++
			}
		case <-ctx.Done():
			cancelAll()
			return nil, ctx.Err()
		}
	}
}

// Write sends a non-idempotent request over one line, moving to the next only on a transport
// failure — never in parallel, and always under the same idempotency key so the server can
// recognise a second arrival as one attempt rather than two writes.
func (c *Client) Write(
	ctx context.Context,
	method, path string,
	body []byte,
	idempotencyKey string,
) (*http.Response, error) {
	header := http.Header{}
	if idempotencyKey != "" {
		header.Set(c.opts.IdempotencyHeader, idempotencyKey)
	}
	return c.writeWithFailover(ctx, method, path, body, header, "")
}

func (c *Client) writeWithFailover(
	ctx context.Context,
	method, path string,
	body []byte,
	header http.Header,
	base string,
) (*http.Response, error) {
	lines := c.Ranked()
	if len(lines) == 0 {
		return nil, ErrNoLine
	}
	c.notifyTraffic()
	if len(lines) > c.opts.MaxWriteAttempts {
		lines = lines[:c.opts.MaxWriteAttempts]
	}

	var lastErr error
	for _, line := range lines {
		attemptCtx, cancel := context.WithTimeout(ctx, c.opts.WriteTimeout)
		response, err := c.send(attemptCtx, line, method, path, body, header, base)
		if err == nil {
			// The response body outlives the attempt context, so the cancel must not fire yet;
			// closing the body is the caller's job, as with any http.Response.
			context.AfterFunc(ctx, cancel)
			return response, nil
		}
		cancel()
		lastErr = err
		// The caller gave up. Trying another line would be sending a write they asked us not to.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// Probe measures every line once and updates the ranking.
func (c *Client) Probe(ctx context.Context, probePath string) {
	lines := c.Lines()
	var wg sync.WaitGroup
	for _, line := range lines {
		wg.Add(1)
		go func(line Line) {
			defer wg.Done()
			response, latency, err := c.sendRaw(ctx, line, http.MethodGet, probePath, nil, nil, "")
			if err != nil {
				c.health.RecordFailure(line.ID, err, c.opts.Now())
				return
			}
			// Drained and closed, or the connection cannot be reused and every probe costs a new
			// handshake — which would make the probe measure its own overhead.
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				c.health.RecordFailure(
					line.ID,
					fmt.Errorf("probe answered %d", response.StatusCode),
					c.opts.Now(),
				)
				return
			}
			c.health.RecordSuccess(line.ID, latency, c.opts.Now())
		}(line)
	}
	wg.Wait()
	c.health.ReconcileDegraded()
}

// sendRaw issues one request over one line and reports how long it took, recording nothing.
//
// A probe wants this: it decides for itself what the answer means, since a 500 is a perfectly
// prompt reply and a probe that recorded it as a success would leave a broken line ranked first.
func (c *Client) sendRaw(
	ctx context.Context,
	line Line,
	method, path string,
	body []byte,
	header http.Header,
	base string,
) (*http.Response, time.Duration, error) {
	url, err := c.resolve(line, path, base)
	if err != nil {
		return nil, 0, err
	}

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, 0, err
	}
	for key, values := range header {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}

	started := c.opts.Now()
	response, err := c.opts.HTTP.Do(request)
	if err != nil {
		return nil, 0, err
	}
	return response, c.opts.Now().Sub(started), nil
}

// send is sendRaw plus what it says about the line. Every real request is a free measurement, which
// is why the ranking stays current between probes.
func (c *Client) send(
	ctx context.Context,
	line Line,
	method, path string,
	body []byte,
	header http.Header,
	base string,
) (*http.Response, error) {
	response, latency, err := c.sendRaw(ctx, line, method, path, body, header, base)
	if err != nil {
		c.health.RecordFailure(line.ID, err, c.opts.Now())
		return nil, err
	}
	c.health.RecordSuccess(line.ID, latency, c.opts.Now())
	return response, nil
}

// resolve turns a path into the URL for one line, filling in what "same origin" means here.
//
// base is the origin the caller was already talking to, when there is one — RoundTrip has it from
// the request, Get and Write fall back to Options.BaseURL. Without either, a same-origin line
// yields a bare path, which net/http cannot send. Saying so plainly beats "unsupported protocol
// scheme """ from three frames down.
func (c *Client) resolve(line Line, path, base string) (string, error) {
	url, err := line.Resolve(path)
	if err != nil {
		return "", err
	}
	if line.URL != "" {
		return url, nil
	}
	if base == "" {
		base = c.opts.BaseURL
	}
	if base == "" {
		return "", fmt.Errorf(
			"multipath: line %q is same-origin but no base URL is configured (set Options.BaseURL)",
			line.ID)
	}
	return strings.TrimRight(base, "/") + url, nil
}

// outcome is one line's answer to a hedged read, and which attempt it was — the index is what lets
// the winner keep its own context while every other attempt is cancelled.
type outcome struct {
	response *http.Response
	err      error
	index    int
}

// drainRemaining closes the bodies of losers that arrive after the winner, so a cancelled response
// cannot leak a connection.
func drainRemaining(results <-chan outcome, count int) {
	for i := 0; i < count; i++ {
		result, ok := <-results
		if !ok {
			return
		}
		if result.response != nil {
			_, _ = io.Copy(io.Discard, result.response.Body)
			_ = result.response.Body.Close()
		}
	}
}
