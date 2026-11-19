// Splicing a stream to a TCP connection, used by the server (stream to target) and by the client
// (local connection to stream). Half-close is preserved: when one direction reaches EOF the other
// side's write half is closed (FIN on the stream, CloseWrite on the socket), so a request followed by
// EOF still gets its answer. An error in either direction aborts both, so a failed session or a
// reset connection never leaves the other copy waiting on a peer that is gone.
//
//   - Splice(st, c) relays until both directions are done, then closes c; it returns the first
//     error, or nil when both directions ended cleanly. It runs one direction in a new goroutine and
//     the other in the caller's.
//
//	go func() {
//	    if err := mux.Splice(st, conn); err != nil { log.Print(err) }
//	}()

package mux

import (
	"errors"
	"io"
	"net"
	"sync"
)

// Splice relays st and c in both directions.
func Splice(st *Stream, c net.Conn) error {
	var once sync.Once
	var first error
	abort := func(err error) {
		once.Do(func() {
			first = err
			// Unblock both copies: the stream's reader and writer, and the socket's.
			_ = st.Reset(err.Error()) // the stream may already be gone; the error is reported below
			_ = c.Close()             // the error that caused the abort is reported below
		})
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := io.Copy(c, st); err != nil {
			abort(err)
			return
		}
		if tc, ok := c.(*net.TCPConn); ok {
			if err := tc.CloseWrite(); err != nil {
				abort(err)
			}
		}
	}()
	if _, err := io.Copy(st, c); err != nil {
		abort(err)
	} else if err := st.Close(); err != nil {
		abort(err)
	}
	<-done
	if first == nil {
		if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
	}
	return first
}
