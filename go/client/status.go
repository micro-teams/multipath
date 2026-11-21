// The client's status: what a person or a supervising program needs to tell a healthy client from a
// wedged one, without reading logs. MicroTeams shows it in `microteams status`.
//
//   - Status is the snapshot: the current session (SessionStatus: id, state, age, offsets,
//     unacknowledged bytes), every line of it (LineStatus: state, whether it is soft down, its
//     smoothed round trip, time since its last byte, bytes in and out, reconnects, last reason;
//     the answering line with the smallest round trip is the quickest way to the server), every open
//     stream (StreamStatus: bytes in and out, buffered
//     bytes, send credit), the number of sessions
//     started and why the previous one ended, and the number of local connections being served.
//   - (*Client).Status() returns it; (*Client).StatusHandler() serves it as JSON, for a status port
//     bound to a local address.
//
//	http.Handle("/status", c.StatusHandler())
//	// {"session":{"id":"1a2b3c4d","state":"live",...},"lines":[{"id":"cf","state":"up",...}],...}

package client

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"
)

// Status is a snapshot of the client.
type Status struct {
	Session          *SessionStatus `json:"session"` // nil before the first session
	Lines            []LineStatus   `json:"lines"`
	Streams          []StreamStatus `json:"streams"`
	SessionsStarted  int            `json:"sessionsStarted"`
	LastSessionEnd   string         `json:"lastSessionEnd,omitempty"`
	LocalConnections int64          `json:"localConnections"`
}

// SessionStatus describes the current session.
type SessionStatus struct {
	ID       string `json:"id"`
	State    string `json:"state"` // "connecting", "live" or "ended"
	AgeMs    int64  `json:"ageMs"`
	Server   string `json:"server,omitempty"`
	Sent     uint64 `json:"sent"`
	Unacked  uint64 `json:"unacked"`
	Received uint64 `json:"received"`
	Reorder  int    `json:"reorder"`
}

// LineStatus describes one line of the current session.
type LineStatus struct {
	Index         int    `json:"index"`
	ID            string `json:"id"`
	URL           string `json:"url"`
	State         string `json:"state"`           // "connecting", "up" or "down"
	Soft          bool   `json:"soft,omitempty"`  // up, but silent since the due ping round: no new data on it
	RTTMs         int64  `json:"rttMs,omitempty"` // smoothed ping round trip; absent before the first answer
	LastByteAgoMs int64  `json:"lastByteAgoMs,omitempty"`
	BytesIn       int64  `json:"bytesIn"`
	BytesOut      int64  `json:"bytesOut"`
	Reconnects    int    `json:"reconnects"`
	LastReason    string `json:"lastReason,omitempty"`
}

// StreamStatus describes one open stream.
type StreamStatus struct {
	ID         uint32 `json:"id"`
	BytesIn    int64  `json:"bytesIn"`
	BytesOut   int64  `json:"bytesOut"`
	Buffered   int    `json:"buffered"`
	SendCredit int    `json:"sendCredit"`
}

// Status returns a snapshot of the client.
func (c *Client) Status() Status {
	c.mu.Lock()
	cs := c.current
	st := Status{SessionsStarted: c.sessions, LastSessionEnd: c.lastEnd, LocalConnections: c.conns.Load()}
	c.mu.Unlock()
	if cs == nil {
		return st
	}
	ss := cs.s.Stats()
	cs.mu.Lock()
	state := "connecting"
	if cs.welcomed {
		state = "live"
	}
	select {
	case <-cs.s.Done():
		state = "ended"
	default:
	}
	st.Session = &SessionStatus{ID: cs.id, State: state, AgeMs: time.Since(cs.started).Milliseconds(), Server: cs.server,
		Sent: ss.SendNext, Unacked: ss.SendNext - ss.SendBase, Received: ss.DeliverNext, Reorder: ss.Reorder}
	slots := make([]*slot, 0, len(cs.slots))
	for _, sl := range cs.slots {
		slots = append(slots, sl)
	}
	m := cs.m
	cs.mu.Unlock()

	sort.Slice(slots, func(i, j int) bool { return slots[i].index < slots[j].index })
	for _, sl := range slots {
		sl.mu.Lock()
		ls := LineStatus{Index: sl.index, ID: sl.line.ID, URL: sl.line.URL, State: sl.state, Reconnects: sl.reconnects, LastReason: sl.reason}
		sl.mu.Unlock()
		for _, l := range ss.Links {
			if l.Index == sl.index {
				ls.LastByteAgoMs = time.Since(l.LastByte).Milliseconds()
				ls.BytesIn, ls.BytesOut = l.BytesIn, l.BytesOut
				ls.Soft, ls.RTTMs = l.Soft, l.RTT.Milliseconds()
			}
		}
		st.Lines = append(st.Lines, ls)
	}
	if m != nil {
		for _, s := range m.Streams() {
			st.Streams = append(st.Streams, StreamStatus{ID: s.ID, BytesIn: s.BytesIn, BytesOut: s.BytesOut, Buffered: s.Buffered, SendCredit: s.SendCredit})
		}
		sort.Slice(st.Streams, func(i, j int) bool { return st.Streams[i].ID < st.Streams[j].ID })
	}
	return st
}

// StatusHandler serves Status as JSON.
func (c *Client) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(c.Status()); err != nil {
			c.cfg.Logf("status: %v", err)
		}
	})
}
