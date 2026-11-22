// Tests for the line list parser: a well-formed document parses in order, every malformed document
// named in PROTOCOL.md section 2 is refused, extra members are ignored, an address is kept, and
// LinkURL maps schemes.
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

func TestParseLinesKeepsAnAddress(t *testing.T) {
	list, err := ParseLines([]byte(`{"lines":[
		{"id":"v4","url":"https://relay.example.app:8443","address":"203.0.113.7:8443"},
		{"id":"v6","url":"https://v6.example.app","address":"[2001:db8::7]:443"},
		{"id":"v6b","url":"https://v6.example.app","address":"[2001:db8:0:1:2:3:4:5]:443"}
	]}`))
	if err != nil {
		t.Fatalf("expected the document to parse, got %v", err)
	}
	if list.Lines[0].Address != "203.0.113.7:8443" || list.Lines[1].Address != "[2001:db8::7]:443" {
		t.Fatalf("addresses not kept: %+v", list.Lines)
	}
}

func TestParseLinesRejects(t *testing.T) {
	for name, document := range map[string]string{
		"not json":               `nonsense`,
		"lines missing":          `{}`,
		"lines empty":            `{"lines":[]}`,
		"id missing":             `{"lines":[{"url":"https://a.example.app"}]}`,
		"duplicate id":           `{"lines":[{"id":"cf","url":"https://a.example.app"},{"id":"cf","url":"https://b.example.app"}]}`,
		"url empty":              `{"lines":[{"id":"cf","url":""}]}`,
		"url relative":           `{"lines":[{"id":"cf","url":"/mt"}]}`,
		"url has a path":         `{"lines":[{"id":"cf","url":"https://cf.example.app/mt"}]}`,
		"url trailing slash":     `{"lines":[{"id":"cf","url":"https://cf.example.app/"}]}`,
		"url is not http(s)":     `{"lines":[{"id":"cf","url":"ws://cf.example.app"}]}`,
		"url with credentials":   `{"lines":[{"id":"cf","url":"https://a@b.example.app"}]}`,
		"url with a query":       `{"lines":[{"id":"cf","url":"https://b.example.app?x"}]}`,
		"address is a name":      `{"lines":[{"id":"cf","url":"https://a.example.app","address":"a.example.app:443"}]}`,
		"address has no port":    `{"lines":[{"id":"cf","url":"https://a.example.app","address":"203.0.113.7"}]}`,
		"address port zero":      `{"lines":[{"id":"cf","url":"https://a.example.app","address":"203.0.113.7:0"}]}`,
		"address v6 unbracketed": `{"lines":[{"id":"cf","url":"https://a.example.app","address":"2001:db8::7:443"}]}`,
		"address has a zone":     `{"lines":[{"id":"cf","url":"https://a.example.app","address":"[fe80::1%eth0]:443"}]}`,
		"address v6 uppercase":   `{"lines":[{"id":"cf","url":"https://a.example.app","address":"[2001:DB8::7]:443"}]}`,
		"address port padded":    `{"lines":[{"id":"cf","url":"https://a.example.app","address":"203.0.113.7:0443"}]}`,
		"address not canonical":  `{"lines":[{"id":"cf","url":"https://a.example.app","address":"203.0.113.07:443"}]}`,
		"address is a number":    `{"lines":[{"id":"cf","url":"https://a.example.app","address":443}]}`,
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
