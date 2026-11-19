// The line list document (PROTOCOL.md section 2): what a server serves at its lines path and what a
// client dials. It arrives over the network, so every assumption a client later makes about it is
// checked here, at the boundary, rather than surfacing as a puzzling URL much later.
//
//   - Line is one entry: a unique ID and the URL of an origin (http or https, no path).
//
//   - LineList is the whole document: {"lines": [...]}. Members other than id and url are ignored.
//
//   - ParseLines(data) validates a document and returns it; a document with no lines, a duplicate or
//     empty id, or a URL that is not a bare http(s) origin is an error.
//
//   - LinkURL(line, linkPath) returns the WebSocket URL of a line's link endpoint.
//
//     list, err := protocol.ParseLines([]byte(`{"lines":[{"id":"cf","url":"https://cf.example.app"}]}`))
//     u := protocol.LinkURL(list.Lines[0], "/multipath/v3/link") // "wss://cf.example.app/multipath/v3/link"
package protocol

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Line is one network path to the server.
type Line struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// LineList is the document served at the lines path.
type LineList struct {
	Lines []Line `json:"lines"`
}

// A bare origin: no path, no trailing slash, no credentials. A trailing slash would produce "//"
// once the link path is appended.
var originPattern = regexp.MustCompile(`^https?://[^/@?#]+$`)

// ParseLines validates an untrusted line list document.
func ParseLines(data []byte) (LineList, error) {
	var list LineList
	if err := json.Unmarshal(data, &list); err != nil {
		return LineList{}, fmt.Errorf("invalid line list: %w", err)
	}
	if len(list.Lines) == 0 || len(list.Lines) > 255 {
		return LineList{}, fmt.Errorf("invalid line list: %d lines, want 1 to 255", len(list.Lines))
	}
	seen := make(map[string]bool, len(list.Lines))
	for i, line := range list.Lines {
		if line.ID == "" {
			return LineList{}, fmt.Errorf("invalid line list: line %d has no id", i)
		}
		// A duplicate id makes every log and status view lie about which line is which.
		if seen[line.ID] {
			return LineList{}, fmt.Errorf("invalid line list: duplicate line id %q", line.ID)
		}
		seen[line.ID] = true
		if !originPattern.MatchString(line.URL) {
			return LineList{}, fmt.Errorf("invalid line list: line %q: url must be an http(s) origin with no path, got %q", line.ID, line.URL)
		}
	}
	return list, nil
}

// LinkURL returns the ws:// or wss:// URL of a line's link endpoint.
func LinkURL(line Line, linkPath string) string {
	return "ws" + strings.TrimPrefix(line.URL, "http") + linkPath
}
