// The client end of the substrate: one redundant mux to the origin, carried over every line at once,
// with each exchange a mux stream on top of it.
//
// There is no line-picking and no per-request strategy here, because there is nothing to pick: a
// request is a stream over the one redundant transport, and the redundant transport already writes
// every byte to every live line and delivers each byte once from whichever line arrived first. A
// dead line is simply never the fastest; there is no timeout to wait out and no retry to issue.
//
// Every stream names a service the origin has registered — there is no "normal vs tunnel" and no
// client-chosen address. A caller opens a service by name (or does one HTTP round trip over it); the
// redundancy is underneath, in the bytes, and the reachable set is exactly what the origin registered.

package multipath

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
)

// ErrNoLine is returned when there is nothing to open a link over.
var ErrNoLine = errors.New("multipath: no line available")

// ClientOptions configures a Client.
type ClientOptions struct {
	// Lines are the network paths to the origin; one link is opened over each. Order is irrelevant
	// — every line carries every byte.
	Lines []Line
	// Link tunes how each line is reached (TLS config, ws path). See LinkOptions.
	Link LinkOptions
	// Redundant tunes the redundant stream (window, ping/reconnect timings). N and Dial are filled
	// in from Lines; anything set here for those is ignored.
	Redundant RedundantOptions
}

// Client is a live redundant mux to one origin.
type Client struct {
	sess *Session
	rs   *RedundantStream
}

// Dial brings up the redundant transport over every line and returns once at least one link is up.
func Dial(ctx context.Context, opt ClientOptions) (*Client, error) {
	if len(opt.Lines) == 0 {
		return nil, ErrNoLine
	}
	lines := opt.Lines
	link := opt.Link
	ropt := opt.Redundant
	ropt.N = len(lines)
	ropt.Dial = func(ctx context.Context, i int) (io.ReadWriteCloser, error) {
		return DialLink(ctx, lines[i], link)
	}
	rs, err := DialRedundant(ctx, ropt)
	if err != nil {
		return nil, err
	}
	return &Client{sess: NewClientSession(rs), rs: rs}, nil
}

// Open opens a stream to the named service, carrying ticket for the origin's handler to authorise it.
// The returned stream is a plain byte pipe; what rides it is between the caller and that service. If
// the origin has not registered the name it resets the stream with a reason, which surfaces as an
// error on the first Read.
func (c *Client) Open(service string, ticket []byte) (*MuxStream, error) {
	st, err := c.sess.OpenStream()
	if err != nil {
		return nil, err
	}
	if err := WriteHeader(st, Header{Service: service, Ticket: ticket}); err != nil {
		_ = st.Reset()
		return nil, err
	}
	return st, nil
}

// RoundTrip carries one HTTP exchange over a stream to the named service: the request is written to a
// fresh stream and the response read back off it. Line redundancy happens underneath without the
// caller knowing more than one path exists. Closing the response body releases the stream.
func (c *Client) RoundTrip(service string, ticket []byte, req *http.Request) (*http.Response, error) {
	st, err := c.Open(service, ticket)
	if err != nil {
		return nil, err
	}
	if err := req.Write(st); err != nil {
		_ = st.Reset()
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(st), req)
	if err != nil {
		_ = st.Reset()
		return nil, err
	}
	resp.Body = &streamBody{ReadCloser: resp.Body, st: st}
	return resp, nil
}

// Stats returns a snapshot of every underlying line's health (up/connecting/down, last byte seen,
// reconnect count, last drop reason). A status view reads this; to react to changes as they happen,
// set ClientOptions.Redundant.OnLinkState instead. The redundant transport hides line failure from
// the data path on purpose — this is how a caller sees the failures it is surviving.
func (c *Client) Stats() []LinkStat { return c.rs.Stats() }

// Close tears down the redundant transport and every stream on it.
func (c *Client) Close() error { return c.sess.Close() }

// streamBody ties the lifetime of a response's stream to its body: when the caller is done reading,
// the stream is reset, freeing it on both ends.
type streamBody struct {
	io.ReadCloser
	st *MuxStream
}

func (b *streamBody) Close() error {
	err := b.ReadCloser.Close()
	_ = b.st.Reset()
	return err
}
