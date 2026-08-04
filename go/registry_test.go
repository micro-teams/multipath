// The Go parser is held to the same cases as the TypeScript one on purpose. The registry is the
// only vocabulary the three packages share, so "both accept it" and "both reject it" is the whole
// compatibility guarantee — and a shared meaning that nothing checks is a shared meaning that
// drifts.

package multipath

import "testing"

func TestParseRegistryAcceptsAWellFormedDocument(t *testing.T) {
	registry, err := ParseRegistry([]byte(`{"lines":[
		{"id":"cf","url":"https://cf.mt.example.app","transport":"cloudflare","weight":100},
		{"id":"ipv6-1","url":"https://ipv6-1.mt.example.app","transport":"ipv6","weight":80}
	]}`))
	if err != nil {
		t.Fatalf("expected the document to parse, got %v", err)
	}
	if len(registry.Lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(registry.Lines))
	}
	// Order is preference order, so it has to survive parsing.
	if registry.Lines[0].ID != "cf" || registry.Lines[1].ID != "ipv6-1" {
		t.Errorf("line order was not preserved: %+v", registry.Lines)
	}
	if registry.Lines[0].Weight != 100 {
		t.Errorf("expected weight 100, got %d", registry.Lines[0].Weight)
	}
}

func TestParseRegistryAcceptsTheEmptyURLAsSameOrigin(t *testing.T) {
	registry, err := ParseRegistry([]byte(`{"lines":[{"id":"self","url":""}]}`))
	if err != nil {
		t.Fatalf("same-origin must be expressible, got %v", err)
	}
	if registry.Lines[0].URL != "" {
		t.Errorf("expected an empty url, got %q", registry.Lines[0].URL)
	}
}

// Each of these would otherwise surface much later as a puzzling URL or a panel that lies about
// which line served what.
func TestParseRegistryRejects(t *testing.T) {
	for name, document := range map[string]string{
		"not json":             `nonsense`,
		"lines missing":        `{}`,
		"lines empty":          `{"lines":[]}`,
		"id missing":           `{"lines":[{"url":""}]}`,
		"duplicate id":         `{"lines":[{"id":"cf","url":""},{"id":"cf","url":""}]}`,
		"url relative":         `{"lines":[{"id":"cf","url":"/mt"}]}`,
		"url has a path":       `{"lines":[{"id":"cf","url":"https://cf.example.app/mt"}]}`,
		"url trailing slash":   `{"lines":[{"id":"cf","url":"https://cf.example.app/"}]}`,
		"url is not http(s)":   `{"lines":[{"id":"cf","url":"ws://cf.example.app"}]}`,
		"url with credentials": `{"lines":[{"id":"cf","url":"https://a@b.example.app/x"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseRegistry([]byte(document)); err == nil {
				t.Errorf("expected %s to be rejected, but it parsed", name)
			}
		})
	}
}

func TestResolve(t *testing.T) {
	remote := Line{ID: "cf", URL: "https://cf.mt.example.app"}
	sameOrigin := Line{ID: "self", URL: ""}

	if got, err := remote.Resolve("/mt/probe"); err != nil || got != "https://cf.mt.example.app/mt/probe" {
		t.Errorf("got %q, %v", got, err)
	}

	// The MP-1 no-op guarantee, in Go: one same-origin line changes nothing.
	if got, err := sameOrigin.Resolve("/mt/probe"); err != nil || got != "/mt/probe" {
		t.Errorf("got %q, %v", got, err)
	}

	// Better a loud error than a mangled URL that half works.
	if _, err := remote.Resolve("mt/probe"); err == nil {
		t.Error("expected an unrooted path to be rejected")
	}
	if _, err := remote.Resolve(""); err == nil {
		t.Error("expected an empty path to be rejected")
	}
}

// The same document the TypeScript parser had to be taught to accept: a server serialising an
// unset optional field as JSON null. encoding/json already treats null as "leave the zero value",
// so this is parity insurance rather than a fix — and parity is the whole contract between these
// two parsers, so it is worth a test that fails if someone ever hand-rolls this decoding.
func TestParseRegistryTreatsNullOptionalFieldsAsAbsent(t *testing.T) {
	registry, err := ParseRegistry([]byte(`{"lines":[
		{"id":"origin","url":"","transport":null,"weight":null,"foreignOrigin":null},
		{"id":"direct","url":"https://direct.mt.example.app","transport":"direct","weight":90,"foreignOrigin":null}
	]}`))
	if err != nil {
		t.Fatalf("a null optional field was rejected: %v", err)
	}
	if len(registry.Lines) != 2 {
		t.Fatalf("expected 2 lines, got %+v", registry.Lines)
	}
	if registry.Lines[0].Transport != "" || registry.Lines[0].Weight != 0 {
		t.Errorf("null did not read as absent: %+v", registry.Lines[0])
	}
	if registry.Lines[1].Weight != 90 {
		t.Errorf("a real value was lost: %+v", registry.Lines[1])
	}
}
