// The origin end of the substrate: accept redundant streams over every line, demultiplex each into
// logical streams, read the one-line header off each, and hand it to the named service it asked for.
//
// The whole server is "demux, then dispatch". Every stream names a service the origin registered;
// the origin looks the name up and hands the stream to that service's handler. A name it did not
// register is refused with a reason. There is no "main service" and no client-chosen address, so a
// client can only ever reach a service the origin put in its registry — the open-relay/SSRF surface
// is gone by construction. There is no request middleware, no idempotency, no coalescing either: the
// redundant layer already delivered each byte exactly once, so a handler sees each request once.
//
// A service handler owns the stream it is given. It may serve it in process (read and write the
// stream directly, no listening port anywhere) or, with the DialService convenience, splice it to a
// real backend address. In-process is preferred: a loopback port is attack surface a named,
// in-process service does not have.

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

// ServiceHandler serves one accepted stream for a registered service. It owns the stream: it must
// read/write and eventually close or reset it. ticket is the opaque capability the client sent; the
// handler decides what it means (this library does not interpret it).
type ServiceHandler func(st *MuxStream, ticket []byte)

// Services is the origin's registry: the set of named services a client may open. A name absent from
// the map is refused. Build it before Serve; it is read-only once serving.
type Services map[string]ServiceHandler

// Serve accepts redundant streams off raw and dispatches each client's streams to services until raw
// is closed. raw yields ordinary transport conns (a net.Listener on a TCP or TLS port); this function
// does the per-line decapsulation, the connID grouping, and the mux.
func Serve(raw net.Listener, opt ServerOptions, services Services) error {
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
		go serveClient(rs, services)
	}
}

func serveClient(rs *RedundantStream, services Services) {
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
			handler, ok := services[h.Service]
			if !ok {
				// Say why: an unknown service is refused with a reason the client surfaces on read,
				// not a bare reset indistinguishable from a network drop.
				_ = st.ResetWithReason(fmt.Sprintf("unknown service: %q", h.Service))
				return
			}
			handler(st, h.Ticket)
		}()
	}
}

// DialService returns a ServiceHandler that dials addr and splices the stream to it — the convenience
// for a service that really is a backend TCP address. Prefer an in-process handler where you can:
// this exists for backends you do not control.
func DialService(addr string) ServiceHandler {
	return func(st *MuxStream, _ []byte) {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			_ = st.ResetWithReason(fmt.Sprintf("dial %s: %v", addr, err))
			return
		}
		spliceStreamConn(st, c)
	}
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
