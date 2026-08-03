// The client as an http.RoundTripper.
//
// Get and Write are the honest shape of what MultiPath does, but they are not the shape a consumer
// already has. A connector's HTTP is usually a stack of round trippers — one adds credentials,
// another retries, another logs — assembled once and handed to whatever makes requests. Asking it
// to rewrite every call site instead is asking it to teach every call site that more than one
// network path exists, which is the thing this library exists to avoid.
//
// It matters beyond convenience for MicroTeams: the connector's HTTP lives in micro-connector, a
// library shared by products that have never heard of MultiPath and must not have to. As a round
// tripper, line selection is something a product injects at its own composition root, underneath
// its own authentication, and nothing else in that library changes.
//
// Layering: authentication belongs *outside* this, closer to the caller. It decides whether to
// attach a credential by looking at the host it was configured for, and this transport is what
// rewrites that host to whichever line is currently best. Underneath, the decision has already been
// made and travels with the request.

package multipath

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
)

// RoundTripper returns the client as an http.RoundTripper: reads are hedged across lines, writes go
// to one line and fail over under a single idempotency key.
//
// The transport it returns is safe for concurrent use and holds no per-request state.
func (c *Client) RoundTripper() http.RoundTripper { return &roundTripper{client: c} }

type roundTripper struct{ client *Client }

// nonIdempotent are the methods that get a key and are never raced. PUT is absent because a
// well-formed PUT already means the same thing twice, and GET/HEAD/OPTIONS because they change
// nothing.
var nonIdempotent = map[string]bool{
	http.MethodPost:   true,
	http.MethodPatch:  true,
	http.MethodDelete: true,
}

func (t *roundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	// The origin the caller was already addressing is what a same-origin registry entry means here,
	// so a single-line registry needs no configuration at all and sends exactly what it sends today.
	base := ""
	if request.URL != nil && request.URL.Host != "" {
		base = request.URL.Scheme + "://" + request.URL.Host
	}
	// Path and query together: a line is an origin, and everything after it belongs to the request.
	path := request.URL.RequestURI()

	// A RoundTripper is required not to modify the request it is given, and the request's own header
	// map may be read concurrently by net/http.
	header := request.Header.Clone()
	if header == nil {
		header = http.Header{}
	}

	if !nonIdempotent[strings.ToUpper(request.Method)] {
		if request.Body != nil {
			defer func() { _ = request.Body.Close() }()
		}
		return t.client.hedgedRead(request.Context(), request.Method, path, header, base)
	}

	body, err := drainBody(request)
	if err != nil {
		return nil, err
	}
	// Minted once per logical request, before any line is chosen — which is precisely what makes
	// failover safe: every attempt at this write carries the same key, so a second arrival is one
	// attempt seen twice rather than two writes. A caller that set the header itself knows something
	// we cannot (that two calls are the same logical write), and is never overruled.
	if header.Get(t.client.opts.IdempotencyHeader) == "" {
		header.Set(t.client.opts.IdempotencyHeader, newIdempotencyKey())
	}
	return t.client.writeWithFailover(request.Context(), request.Method, path, body, header, base)
}

// drainBody reads a request body into memory so it can be sent again over another line.
//
// A body is a stream that can be read once, and failover needs the same bytes twice. GetBody is the
// standard library's own answer to this — it populates it for exactly this reason on redirects —
// so prefer it and fall back to reading.
func drainBody(request *http.Request) ([]byte, error) {
	if request.Body == nil || request.Body == http.NoBody {
		return nil, nil
	}
	if request.GetBody != nil {
		reader, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		defer func() { _ = reader.Close() }()
		return io.ReadAll(reader)
	}
	defer func() { _ = request.Body.Close() }()
	var buffer bytes.Buffer
	if _, err := io.Copy(&buffer, request.Body); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// newIdempotencyKey returns a random key. Uniqueness is all that is asked of it — the server only
// ever compares keys for equality — so 128 random bits, and no dependency for a UUID.
func newIdempotencyKey() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		// crypto/rand does not fail on any platform this runs on. If it somehow did, the write goes
		// out unkeyed — no worse than a single-line client — rather than not going out at all.
		return ""
	}
	return hex.EncodeToString(raw[:])
}
