// Generic port mapping over a mux Session: the product-agnostic "forward a local port to a remote
// service, over the redundant+multiplexed transport" convenience. It is pure glue over the mux —
// every local connection becomes one mux stream — so it introduces no new wire format and inherits
// the mux's cross-language guarantees.
//
// Two halves, ssh -L style:
//   - client: Forward(listener, session) — accept local connections, open one mux stream per
//     connection, and splice the two together.
//   - server: Serve(session, handler) — accept mux streams and hand each to a handler. The generic
//     handler is ForwardTo(dial), which dials a fixed target and splices. A consumer that needs to
//     do something other than forward-to-a-target (e.g. ccproxy's MITM) supplies its own handler.
//
// The mapping's target is fixed per session/handler (like ssh -L localport:host:port): there is no
// per-stream address negotiated on the wire, which keeps this a plain mux consumer. A consumer that
// needs dynamic targets can carry its own address header inside the stream.

package multipath

import (
	"io"
	"net"
	"sync"
)

// StreamHandler processes one accepted server-side mux stream and owns closing it.
type StreamHandler func(*MuxStream)

// Serve accepts streams off a server Session and dispatches each to h in its own goroutine. It
// returns when the session ends.
func Serve(sess *Session, h StreamHandler) error {
	for {
		st, err := sess.AcceptStream()
		if err != nil {
			return err
		}
		go h(st)
	}
}

// ForwardTo returns a handler that dials a target (via dial) and splices it to the stream. dial is a
// thunk so each stream gets a fresh connection.
func ForwardTo(dial func() (net.Conn, error)) StreamHandler {
	return func(st *MuxStream) {
		up, err := dial()
		if err != nil {
			st.Reset()
			return
		}
		spliceStreamConn(st, up)
	}
}

// Forward accepts local connections on ln and forwards each over a new mux stream on sess. It
// returns when ln stops accepting.
func Forward(ln net.Listener, sess *Session) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go func(c net.Conn) {
			st, err := sess.OpenStream()
			if err != nil {
				_ = c.Close()
				return
			}
			spliceStreamConn(st, c)
		}(c)
	}
}

// spliceStreamConn relays a mux stream and a net.Conn in both directions, preserving half-close:
// when one direction reaches EOF it half-closes the write side of the other (FIN on the stream,
// CloseWrite on the TCP conn) rather than aborting, so request/response protocols that write-then-EOF
// still get their reply. Once both directions are done, both ends are fully torn down. A stream/conn
// closed elsewhere (session teardown) unblocks the copies, so nothing leaks.
func spliceStreamConn(st *MuxStream, c net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(st, c) // client -> server
		st.Close()     // FIN: no more from this side
	}()
	go func() {
		defer wg.Done()
		io.Copy(c, st) // server -> client
		halfCloseWrite(c)
	}()
	wg.Wait()
	st.Reset()
	_ = c.Close()
}

func halfCloseWrite(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
		return
	}
	_ = c.Close()
}
