// Tests for the line list parser: a well-formed document parses in order, every malformed document
// named in PROTOCOL.md section 2 is refused, extra members are ignored, and LinkURL maps schemes.
package protocol

import "testing"

func TestParseLinesAcceptsAWellFormedDocument(t *testing.T) {
	list, err := ParseLines([]byte(`{"lines":[
		{"id":"cf","url":"https://cf.example.app"},
		{"id":"direct","url":"https://direct.example.app:8443"}
	]}`))
	if err != nil {
		t.Fatalf("expected the document to parse, got %v", err)
	}
	if len(list.Lines) != 2 || list.Lines[0].ID != "cf" || list.Lines[1].ID != "direct" {
		t.Fatalf("lines not preserved in order: %+v", list.Lines)
	}
}

// A server may carry members for other consumers; they must not break a client.
func TestParseLinesIgnoresUnknownMembers(t *testing.T) {
	list, err := ParseLines([]byte(`{"lines":[{"id":"cf","url":"https://cf.example.app","transport":null,"weight":90}],"extra":1}`))
	if err != nil || list.Lines[0].URL != "https://cf.example.app" {
		t.Fatalf("unknown members broke parsing: %+v %v", list, err)
	}
}

func TestParseLinesRejects(t *testing.T) {
	for name, document := range map[string]string{
		"not json":             `nonsense`,
		"lines missing":        `{}`,
		"lines empty":          `{"lines":[]}`,
		"id missing":           `{"lines":[{"url":"https://a.example.app"}]}`,
		"duplicate id":         `{"lines":[{"id":"cf","url":"https://a.example.app"},{"id":"cf","url":"https://b.example.app"}]}`,
		"url empty":            `{"lines":[{"id":"cf","url":""}]}`,
		"url relative":         `{"lines":[{"id":"cf","url":"/mt"}]}`,
		"url has a path":       `{"lines":[{"id":"cf","url":"https://cf.example.app/mt"}]}`,
		"url trailing slash":   `{"lines":[{"id":"cf","url":"https://cf.example.app/"}]}`,
		"url is not http(s)":   `{"lines":[{"id":"cf","url":"ws://cf.example.app"}]}`,
		"url with credentials": `{"lines":[{"id":"cf","url":"https://a@b.example.app"}]}`,
		"url with a query":     `{"lines":[{"id":"cf","url":"https://b.example.app?x"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseLines([]byte(document)); err == nil {
				t.Errorf("expected %s to be rejected, but it parsed", name)
			}
		})
	}
}

func TestLinkURL(t *testing.T) {
	for url, want := range map[string]string{
		"https://cf.example.app":    "wss://cf.example.app/multipath/v3/link",
		"http://127.0.0.1:8080":     "ws://127.0.0.1:8080/multipath/v3/link",
		"https://cf.example.app:99": "wss://cf.example.app:99/multipath/v3/link",
	} {
		if got := LinkURL(Line{ID: "x", URL: url}, "/multipath/v3/link"); got != want {
			t.Errorf("LinkURL(%s) = %s, want %s", url, got, want)
		}
	}
}
