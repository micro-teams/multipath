// The origin end of the substrate: accept redundant streams over every line, demultiplex each into
// logical streams, read the one-line header off each, and splice it to where it belongs.
//
// The whole server is "demux, then splice". A normal stream is spliced to the origin's own service
// on loopback — the application stays an ordinary server that never learns a line existed. A tunnel
// stream is authorised by the consumer's policy (ticket + destination) and, if allowed, spliced to
// its target. There is no request middleware, no idempotency, no coalescing: the redundant layer
// already delivered each byte exactly once, so the origin sees each request exactly once.

package multipath

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
)

// ServerOptions configures the origin's link acceptor.
type ServerOptions struct {
	// MaxLinks bounds how many links one client may attach to a stream. It must be at least as
	// large as any client's line count. Default 8.
	MaxLinks int
	// Window is the redundant stream's send window. Default is the redundant default (4MiB).
	Window int
	// TLSConfig terminates raw-TLS (direct) links. Nil means only plaintext/CDN-fronted links are
	// accepted — enough for a testbed, not for a public origin.
	TLSConfig *tls.Config
	// LinkPath is the WebSocket upgrade path CDN-fronted links arrive on. Default "/mt/link".
	LinkPath string
}

// StreamRouter decides what an accepted stream does, given its header. It owns closing the stream.
type StreamRouter func(h Header, st *MuxStream)

// Serve accepts redundant streams off raw and serves each client's streams through router until raw
// is closed. raw yields ordinary transport conns (a net.Listener on a TCP or TLS port); this
// function does the per-line decapsulation, the connID grouping, and the mux.
func Serve(raw net.Listener, opt ServerOptions, router StreamRouter) error {
	if opt.MaxLinks == 0 {
		opt.MaxLinks = 8
	}
	if opt.LinkPath == "" {
		opt.LinkPath = defaultLinkPath
	}
	acc := Listen(&linkListener{raw: raw, opt: opt}, RedundantOptions{N: opt.MaxLinks, Window: opt.Window})
	defer acc.Close()
	for {
		rs, err := acc.Accept()
		if err != nil {
			return err
		}
		go serveClient(rs, router)
	}
}

func serveClient(rs *RedundantStream, router StreamRouter) {
	sess := NewServerSession(rs)
	defer sess.Close()
	for {
		st, err := sess.AcceptStream()
		if err != nil {
			return
		}
		go func() {
			h, err := ReadHeader(st)
			if err != nil {
				_ = st.Reset()
				return
			}
			router(h, st)
		}()
	}
}

// Router builds the standard StreamRouter: normal streams go to the origin's own service via local,
// tunnel streams are authorised and dialled by egress. Either may be nil to refuse that kind.
func Router(local func() (net.Conn, error), egress func(target string, ticket []byte) (net.Conn, error)) StreamRouter {
	return func(h Header, st *MuxStream) {
		var up net.Conn
		var err error
		switch h.Kind {
		case KindNormal:
			if local == nil {
				_ = st.Reset()
				return
			}
			up, err = local()
		case KindTunnel:
			if egress == nil {
				_ = st.Reset()
				return
			}
			up, err = egress(h.Target, h.Ticket)
		default:
			_ = st.Reset()
			return
		}
		if err != nil {
			_ = st.Reset()
			return
		}
		spliceStreamConn(st, up)
	}
}

// DialLocal returns a local function for Router that dials the origin's own service at addr each
// time — the loopback reverse-proxy for normal application traffic.
func DialLocal(addr string) func() (net.Conn, error) {
	return func() (net.Conn, error) { return net.Dial("tcp", addr) }
}

// linkListener decapsulates each accepted conn — raw TLS, a WebSocket, or plaintext — so the
// redundant Acceptor above it always sees the same thing: a byte stream whose first frame is HELLO.
type linkListener struct {
	raw net.Listener
	opt ServerOptions
}

func (l *linkListener) Accept() (net.Conn, error) {
	for {
		raw, err := l.raw.Accept()
		if err != nil {
			return nil, err
		}
		conn, err := l.decap(raw)
		if err != nil {
			_ = raw.Close()
			continue // a bad link is not a reason to stop serving the good ones
		}
		return conn, nil
	}
}

func (l *linkListener) Close() error   { return l.raw.Close() }
func (l *linkListener) Addr() net.Addr { return l.raw.Addr() }

// decap sniffs the first byte to tell TLS (0x16, a ClientHello) from an HTTP upgrade ('G', a
// WebSocket) from a raw frame stream, and unwraps accordingly. A TLS link is unwrapped and then
// sniffed once more, since a CDN that re-originates TLS to us still arrives as a WebSocket inside it.
func (l *linkListener) decap(raw net.Conn) (net.Conn, error) {
	br := bufio.NewReader(raw)
	first, err := br.Peek(1)
	if err != nil {
		return nil, err
	}
	switch {
	case first[0] == 0x16:
		if l.opt.TLSConfig == nil {
			return nil, fmt.Errorf("multipath: TLS link but no server TLS config")
		}
		conn := tls.Server(&bufConn{r: br, Conn: raw}, l.opt.TLSConfig)
		if err := conn.Handshake(); err != nil {
			return nil, err
		}
		return l.sniffWSOrRaw(conn)
	case first[0] == 'G':
		return acceptWebSocket(&bufConn{r: br, Conn: raw}, l.opt.LinkPath)
	default:
		return &bufConn{r: br, Conn: raw}, nil
	}
}

// sniffWSOrRaw looks at a post-TLS conn: an HTTP upgrade is a WebSocket, anything else is a raw
// frame stream.
func (l *linkListener) sniffWSOrRaw(conn net.Conn) (net.Conn, error) {
	br := bufio.NewReader(conn)
	first, err := br.Peek(1)
	if err != nil {
		return nil, err
	}
	wrapped := &bufConn{r: br, Conn: conn}
	if first[0] == 'G' {
		return acceptWebSocket(wrapped, l.opt.LinkPath)
	}
	return wrapped, nil
}

// bufConn is a net.Conn whose reads come from a buffered reader holding any already-peeked bytes,
// so a sniff never loses the byte it looked at.
type bufConn struct {
	r io.Reader
	net.Conn
}

func (c *bufConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// spliceStreamConn relays a mux stream and a net.Conn in both directions, preserving half-close:
// when one direction reaches EOF it half-closes the write side of the other (FIN on the stream,
// CloseWrite on the TCP conn) rather than aborting, so a request-then-EOF still gets its reply. Once
// both directions are done, both ends are torn down; a stream or conn closed elsewhere (session
// teardown) unblocks the copies, so nothing leaks.
func spliceStreamConn(st *MuxStream, c net.Conn) {
	var wg sync.WaitGroup
	var toStream, toConn error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, toStream = io.Copy(st, c)
		_ = st.Close() // FIN: no more from this side
	}()
	go func() {
		defer wg.Done()
		_, toConn = io.Copy(c, st)
		halfCloseWrite(c)
	}()
	wg.Wait()
	// Reset only aborts an abnormal end; a clean both-way EOF is torn down by the FINs above (the
	// mux frees the stream on both-FIN), so the peer keeps whatever it had not yet drained.
	if toStream != nil || toConn != nil {
		_ = st.Reset()
	}
	_ = c.Close()
}

func halfCloseWrite(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
		return
	}
	_ = c.Close()
}
