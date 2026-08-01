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
	lines := c.Ranked()
	if len(lines) == 0 {
		return nil, ErrNoLine
	}

	ctx, cancel := context.WithCancel(ctx)

	// Buffered for every line: a loser that finishes after the winner must be able to report and
	// exit rather than blocking forever on a channel nobody is reading.
	results := make(chan outcome, len(lines))

	launch := func(line Line) {
		go func() {
			response, err := c.send(ctx, line, http.MethodGet, path, nil, nil)
			results <- outcome{response, err}
		}()
	}

	launch(lines[0])
	launched := 1
	hedge := time.NewTimer(c.opts.HedgeAfter)
	defer hedge.Stop()

	var lastErr error
	failures := 0
	for {
		select {
		case <-hedge.C:
			for ; launched < len(lines); launched++ {
				launch(lines[launched])
			}
		case result := <-results:
			if result.err == nil {
				// Cancelling here also cancels the losers, whose bodies are closed by send.
				// The winner's body is already ours: it was read from a request that completed.
				go drainRemaining(results, len(lines)-1)
				cancel()
				return result.response, nil
			}
			lastErr = result.err
			failures++
			if failures == len(lines) {
				cancel()
				return nil, lastErr
			}
			// A line that fails immediately should not leave the request waiting out the hedge
			// delay for company it will never get.
			if launched < len(lines) {
				launch(lines[launched])
				launched++
			}
		case <-ctx.Done():
			cancel()
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
	lines := c.Ranked()
	if len(lines) == 0 {
		return nil, ErrNoLine
	}
	if len(lines) > c.opts.MaxWriteAttempts {
		lines = lines[:c.opts.MaxWriteAttempts]
	}

	headers := http.Header{}
	if idempotencyKey != "" {
		headers.Set(c.opts.IdempotencyHeader, idempotencyKey)
	}

	var lastErr error
	for _, line := range lines {
		attemptCtx, cancel := context.WithTimeout(ctx, c.opts.WriteTimeout)
		response, err := c.send(attemptCtx, line, method, path, body, headers)
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
			started := c.opts.Now()
			response, err := c.send(ctx, line, http.MethodGet, probePath, nil, nil)
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
			c.health.RecordSuccess(line.ID, c.opts.Now().Sub(started), c.opts.Now())
		}(line)
	}
	wg.Wait()
	c.health.ReconcileDegraded()
}

func (c *Client) send(
	ctx context.Context,
	line Line,
	method, path string,
	body []byte,
	headers http.Header,
) (*http.Response, error) {
	url, err := line.Resolve(path)
	if err != nil {
		return nil, err
	}

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, err
	}
	for key, values := range headers {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}

	started := c.opts.Now()
	response, err := c.opts.HTTP.Do(request)
	if err != nil {
		c.health.RecordFailure(line.ID, err, c.opts.Now())
		return nil, err
	}
	c.health.RecordSuccess(line.ID, c.opts.Now().Sub(started), c.opts.Now())
	return response, nil
}

// outcome is one line's answer to a hedged read.
type outcome struct {
	response *http.Response
	err      error
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
