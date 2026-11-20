// The line list (PROTOCOL.md section 2): fetched from the entry, kept, and stored. The list comes only
// from the configured entry, travels through the same proxy as the links, and an https entry accepts
// only https lines, so that a tampered list cannot downgrade the links to plain text.
//
// The last list the entry served is kept in memory and, when Config.LinesCache names a file, on disk
// as {"entry": "<origin>", "lines": [...]}, so that a session can start while the entry is unreachable,
// across restarts too. A stored list is used only by a client of the same entry, and passes the same
// checks as a fetched one.
//
// Nothing here is exported; the session loop and the refresh loop call updateLines and knownLines,
// and New calls loadCache.

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/micro-teams/multipath/go/protocol"
)

// storedLines is the cache file's document: the line list document with the entry that served it.
type storedLines struct {
	Entry string          `json:"entry"`
	Lines []protocol.Line `json:"lines"`
}

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
	return c.checkLines(data)
}

// checkLines parses a line list document and applies the entry's policy to it.
func (c *Client) checkLines(data []byte) ([]protocol.Line, error) {
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

// updateLines fetches the entry's list and keeps it, in memory and in the cache file.
func (c *Client) updateLines(ctx context.Context) ([]protocol.Line, error) {
	lines, err := c.fetchLines(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.known, c.knownAt = lines, time.Now()
	c.mu.Unlock()
	if c.cfg.LinesCache != "" {
		if err := c.saveCache(lines); err != nil {
			c.cfg.Logf("line list cache %s: %v", c.cfg.LinesCache, err)
		}
	}
	return lines, nil
}

// knownLines is the last list the entry served, and when; nil before any.
func (c *Client) knownLines() ([]protocol.Line, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.known, c.knownAt
}

// saveCache replaces the cache file in one rename, so a reader never sees half a list.
func (c *Client) saveCache(lines []protocol.Line) error {
	data, err := json.Marshal(storedLines{Entry: c.cfg.Entry, Lines: lines})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(c.cfg.LinesCache), "."+filepath.Base(c.cfg.LinesCache)+"-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()           // the write error is what matters
		_ = os.Remove(tmp.Name()) // likewise
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name()) // the close error is what matters
		return err
	}
	return os.Rename(tmp.Name(), c.cfg.LinesCache)
}

// loadCache reads the cache file into the known list. A missing file is not an error: nothing was
// stored yet.
func (c *Client) loadCache() error {
	info, err := os.Stat(c.cfg.LinesCache)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	data, err := os.ReadFile(c.cfg.LinesCache)
	if err != nil {
		return err
	}
	var stored storedLines
	if err := json.Unmarshal(data, &stored); err != nil {
		return fmt.Errorf("invalid line list cache: %w", err)
	}
	if stored.Entry != c.cfg.Entry {
		return fmt.Errorf("it holds the list of entry %q, not %q", stored.Entry, c.cfg.Entry)
	}
	lines, err := c.checkLines(data)
	if err != nil {
		return err
	}
	c.known, c.knownAt = lines, info.ModTime()
	return nil
}
