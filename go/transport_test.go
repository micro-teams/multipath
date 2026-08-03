// The round tripper is tested through a real http.Client, because that is the only way it will ever
// be used: whether it composes with the standard library's own machinery is most of the question.

package multipath

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransportHedgesReadsAndAsksOnlyOneLineWhenItAnswers(t *testing.T) {
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
	via := &http.Client{Transport: client.RoundTripper()}

	response, err := via.Get(fast.URL + "/x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := read(t, response); got != "fast" {
		t.Errorf("got %q", got)
	}
	if second.Load() != 0 {
		t.Errorf("the second line was asked %d times", second.Load())
	}
}

// The headers a caller attached — an Authorization bearer, above all — have to survive the trip,
// on every line. A transport that quietly dropped them would look like an auth bug.
func TestTransportCarriesTheCallersHeaders(t *testing.T) {
	seen := make(chan string, 1)
	only := line(t, "only", func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	})

	client := New(Options{Registry: Registry{Lines: []Line{only}}})
	request, err := http.NewRequest(http.MethodGet, only.URL+"/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer token")

	response, err := (&http.Client{Transport: client.RoundTripper()}).Do(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = read(t, response)
	if got := <-seen; got != "Bearer token" {
		t.Errorf("Authorization arrived as %q", got)
	}
}

// A write is never raced, gets a key it did not have to ask for, and carries the same key to the
// next line when the first one goes silent. Two arrivals, one attempt.
func TestTransportKeysAWriteAndReplaysTheSameKeyOnFailover(t *testing.T) {
	keys := make(chan string, 2)
	bodies := make(chan string, 2)
	record := func(w http.ResponseWriter, r *http.Request) {
		buffer := new(bytes.Buffer)
		_, _ = buffer.ReadFrom(r.Body)
		keys <- r.Header.Get("Idempotency-Key")
		bodies <- buffer.String()
		w.WriteHeader(http.StatusOK)
	}
	broken := Line{ID: "broken", URL: "http://127.0.0.1:1"}
	good := line(t, "good", record)

	client := New(Options{Registry: Registry{Lines: []Line{broken, good}}})
	via := &http.Client{Transport: client.RoundTripper()}

	response, err := via.Post(good.URL+"/x", "application/json", strings.NewReader(`{"a":1}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = read(t, response)

	key := <-keys
	if key == "" {
		t.Fatal("the write went out with no idempotency key")
	}
	// The body has to survive being sent to a line that failed: a failover that arrives empty is
	// worse than one that does not arrive.
	if got := <-bodies; got != `{"a":1}` {
		t.Errorf("body arrived as %q", got)
	}
}

func TestTransportNeverOverridesAKeyTheCallerSet(t *testing.T) {
	keys := make(chan string, 1)
	only := line(t, "only", func(w http.ResponseWriter, r *http.Request) {
		keys <- r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
	})

	client := New(Options{Registry: Registry{Lines: []Line{only}}})
	request, err := http.NewRequest(http.MethodPost, only.URL+"/x", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "mine")

	response, err := (&http.Client{Transport: client.RoundTripper()}).Do(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = read(t, response)
	if got := <-keys; got != "mine" {
		t.Errorf("the caller's key became %q", got)
	}
}

// The query string is part of the request, not part of the line. Losing it is the sort of fault
// that looks like a backend bug: the right endpoint, answering about the wrong thing.
func TestTransportKeepsTheQueryString(t *testing.T) {
	seen := make(chan string, 1)
	only := line(t, "only", func(w http.ResponseWriter, r *http.Request) {
		seen <- r.URL.RequestURI()
		w.WriteHeader(http.StatusOK)
	})

	client := New(Options{Registry: Registry{Lines: []Line{only}}})
	response, err := (&http.Client{Transport: client.RoundTripper()}).
		Get(only.URL + "/chat?page_start=3&page_size=20")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = read(t, response)
	if got := <-seen; got != "/chat?page_start=3&page_size=20" {
		t.Errorf("the server was asked for %q", got)
	}
}

// A same-origin line is the single-line deployment, and through the transport it needs no
// configuration: the origin the caller was already addressing is what "same" means.
func TestTransportTakesTheSameOriginLineFromTheRequest(t *testing.T) {
	reached := make(chan string, 1)
	server := line(t, "real", func(w http.ResponseWriter, r *http.Request) {
		reached <- r.URL.Path
		w.WriteHeader(http.StatusOK)
	})

	client := New(Options{Registry: Registry{Lines: []Line{{ID: "same", URL: ""}}}})
	response, err := (&http.Client{Transport: client.RoundTripper()}).Get(server.URL + "/x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = read(t, response)
	if got := <-reached; got != "/x" {
		t.Errorf("reached %q", got)
	}
}

// Get and Write have no request to infer an origin from, so a same-origin line has to be told what
// it means — and saying so is better than emitting a bare path net/http cannot send.
func TestSameOriginLineNeedsABaseURLOutsideTheTransport(t *testing.T) {
	client := New(Options{Registry: Registry{Lines: []Line{{ID: "same", URL: ""}}}})
	if _, err := client.Get(context.Background(), "/x"); err == nil {
		t.Fatal("expected an error naming the missing base URL")
	} else if !strings.Contains(err.Error(), "BaseURL") {
		t.Errorf("the error does not say what to set: %v", err)
	}

	server := line(t, "real", answers("ok", 0))
	configured := New(Options{
		Registry: Registry{Lines: []Line{{ID: "same", URL: ""}}},
		BaseURL:  server.URL,
	})
	response, err := configured.Get(context.Background(), "/x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := read(t, response); got != "ok" {
		t.Errorf("got %q", got)
	}
}
