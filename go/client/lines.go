// Fetching the line list from the entry (PROTOCOL.md section 2). The list comes only from the
// configured entry, travels through the same proxy as the links, and an https entry accepts only
// https lines, so that a tampered list cannot downgrade the links to plain text.
//
// Nothing here is exported; the session loop and the refresh loop call fetchLines.

package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/micro-teams/multipath/go/protocol"
)

// fetchLines fetches and validates the entry's line list.
func (c *Client) fetchLines(ctx context.Context) ([]protocol.Line, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.Entry+c.cfg.LinesPath, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", req.URL, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	list, err := protocol.ParseLines(data)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(c.cfg.Entry, "https:") {
		for _, l := range list.Lines {
			if !strings.HasPrefix(l.URL, "https:") {
				return nil, fmt.Errorf("line %q is not https while the entry is: refusing the whole list", l.ID)
			}
		}
	}
	return list.Lines, nil
}
