// Registry parsing for the Go client.
//
// This is a deliberate duplicate of ts/src/registry.ts, and the duplication is the point: the
// registry JSON is the one vocabulary the three packages share, so each has to be able to read it
// on its own terms. What must not diverge is the *meaning* — an id that is unique, a url that is a
// bare origin or the empty string, a document that is rejected at the boundary rather than
// producing a puzzling URL somewhere much later.
//
// Every rule below therefore mirrors a rule in the TypeScript parser, and the tests are written
// against the same cases so that a change to one that is not made to the other shows up as a
// failure rather than as a subtle disagreement in production.

package multipath

import (
	"encoding/json"
	"fmt"
	"regexp"
)

// Line is one network path to the origin.
type Line struct {
	// Stable, human-readable identifier — "cf", "ipv6-1", "frp-2".
	ID string `json:"id"`
	// Absolute origin for this line, or "" meaning "same origin as the caller's default".
	URL string `json:"url"`
	// Free-form transport label, for diagnosis only.
	Transport string `json:"transport,omitempty"`
	// Static preference, higher is better. Only breaks ties between indistinguishable latencies;
	// measurement outranks it, because a hand-set weight goes stale and an EWMA does not.
	Weight int `json:"weight,omitempty"`
	// True when the browser or client does not see this line under our own domain — a free proxy
	// that cannot be CNAME'd, and therefore a fallback with reduced capability.
	ForeignOrigin bool `json:"foreignOrigin,omitempty"`
}

// Registry is the document served by GET /mt/lines.
type Registry struct {
	Lines []Line `json:"lines"`
}

// An absolute origin and nothing more: no path, no trailing slash. A trailing slash silently
// produces "//mt/probe" once a path is appended, which is the kind of fault that survives review
// and fails in production.
var originPattern = regexp.MustCompile(`^https?://[^/]+$`)

// ParseRegistry validates an untrusted registry document and returns it.
//
// It arrives over the network, so every assumption the client later makes is checked here — the
// only place the mistake is still cheap to read.
func ParseRegistry(data []byte) (Registry, error) {
	var registry Registry
	if err := json.Unmarshal(data, &registry); err != nil {
		return Registry{}, fmt.Errorf("invalid line registry: %w", err)
	}
	if len(registry.Lines) == 0 {
		return Registry{}, fmt.Errorf("invalid line registry: no lines")
	}

	seen := make(map[string]bool, len(registry.Lines))
	for i, line := range registry.Lines {
		if line.ID == "" {
			return Registry{}, fmt.Errorf("invalid line registry: line %d has no id", i)
		}
		// A duplicate id makes the developer panel and every metric lie about which line served
		// what, which is worse than a hard failure because it is believed.
		if seen[line.ID] {
			return Registry{}, fmt.Errorf("invalid line registry: duplicate line id %q", line.ID)
		}
		seen[line.ID] = true

		if line.URL != "" && !originPattern.MatchString(line.URL) {
			return Registry{}, fmt.Errorf(
				"invalid line registry: line %q: url must be an absolute origin with no path or "+
					"trailing slash (or \"\" for same-origin), got %q", line.ID, line.URL)
		}
	}

	return registry, nil
}

// Resolve returns the URL for path over this line.
//
// A same-origin line returns the path unchanged, so a single-line deployment issues exactly the
// requests it issued before MultiPath existed.
func (l Line) Resolve(path string) (string, error) {
	if len(path) == 0 || path[0] != '/' {
		return "", fmt.Errorf("path must start with %q, got %q", "/", path)
	}
	if l.URL == "" {
		return path, nil
	}
	return l.URL + path, nil
}
