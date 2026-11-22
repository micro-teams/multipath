// The line list document (PROTOCOL.md section 2): what a server serves at its lines path and what a
// client dials. It arrives over the network, so every assumption a client later makes about it is
// checked here, at the boundary, rather than surfacing as a puzzling URL much later.
//
//   - Line is one entry: a unique ID, the URL of an origin (http or https, no path), and optionally
//     the Address to connect to instead of the URL's host: an IP literal and a port. A link to a line
//     with an address goes to that address, sends no TLS server name (RFC 6066 has none for an IP)
//     and verifies the certificate against the URL's host, so a network that filters by server name
//     sees none. A client that cannot verify a certificate against another name than the one it
//     connects to (a browser) ignores the address and dials the URL.
//
//   - LineList is the whole document: {"lines": [...]}. Members other than id, url and address are
//     ignored.
//
//   - ParseLines(data) validates a document and returns it; a document with no lines, a duplicate or
//     empty id, a URL that is not a bare http(s) origin, or an address that is not an IP literal
//     with a port is an error.
//
//   - LinkURL(line, linkPath) returns the WebSocket URL of a line's link endpoint.
//
//     list, err := protocol.ParseLines([]byte(`{"lines":[{"id":"cf","url":"https://cf.example.app"}]}`))
//     u := protocol.LinkURL(list.Lines[0], "/multipath/v3/link") // "wss://cf.example.app/multipath/v3/link"
//     // {"id":"relay","url":"https://relay.example.app:8443","address":"203.0.113.7:8443"}: a link
//     // connects to 203.0.113.7:8443 and verifies the certificate against relay.example.app.
package protocol

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"regexp"
	"strings"
)

// Line is one network path to the server.
type Line struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	Address string `json:"address,omitempty"` // IP:port to connect to instead of the URL's host
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
		if line.Address != "" && !isAddress(line.Address) {
			return LineList{}, fmt.Errorf("invalid line list: line %q: address must be an IP literal and a port, got %q", line.ID, line.Address)
		}
	}
	return list, nil
}

// isAddress reports whether s is an IP literal and a port, as "203.0.113.7:8443" or
// "[2001:db8::7]:8443". A host name is refused: it would be resolved and its name sent as the
// server name, which is what the address exists to avoid.
func isAddress(s string) bool {
	ap, err := netip.ParseAddrPort(s)
	return err == nil && ap.Port() != 0 && ap.Addr().Zone() == "" && ap.String() == s
}

// LinkURL returns the ws:// or wss:// URL of a line's link endpoint.
func LinkURL(line Line, linkPath string) string {
	return "ws" + strings.TrimPrefix(line.URL, "http") + linkPath
}
