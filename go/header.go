// L5 — the one thing said at the start of a mux stream: what it is and, for a tunnel, where it goes.
//
// A mux stream is an opaque duplex once it is open, but the server has to know what to do with a
// freshly accepted one before any payload flows. That decision needs at most three things: the kind
// of stream, a target address when it is a tunnel, and an opaque ticket the consumer (not this
// library) uses to authorise egress. So the header is those three, sent once, ahead of the bytes —
// the moral equivalent of a SOCKS request line, with no round trip: the client writes it and starts
// streaming, the server reads it and starts splicing.
//
// The library defines the format and carries the ticket as opaque bytes; it never reads the ticket
// and never decides whether an address is allowed. Kind, target policy, and ticket meaning are the
// server handler's concern — for a normal stream the target is the local application, for a tunnel
// it is whatever the consumer's policy permits.

package multipath

import (
	"encoding/binary"
	"fmt"
	"io"
)

// StreamKind is what an accepted mux stream carries.
type StreamKind uint8

const (
	// KindNormal is application traffic (HTTP, WebSocket) bound for the origin's own service. The
	// address is empty; the server splices it to the local application.
	KindNormal StreamKind = 0
	// KindTunnel is an opaque tunnel to Target. The server authorises it (ticket + destination
	// policy) and, if allowed, splices it to Target.
	KindTunnel StreamKind = 1
)

// headerVersion is bumped only on an incompatible change to the layout below; a reader rejects
// anything it does not recognise rather than guessing.
const headerVersion = 1

// maxHeaderField bounds the two length-prefixed fields so a corrupt or hostile length cannot make
// the reader allocate without limit before any policy has run.
const maxHeaderField = 4096

// Header is what a client says at the start of a mux stream.
type Header struct {
	Kind StreamKind
	// Target is "host:port" for a tunnel, empty for a normal stream.
	Target string
	// Ticket is an opaque egress capability the server's handler interprets; the library does not.
	Ticket []byte
}

// WriteHeader writes h ahead of the stream payload. Layout (big-endian):
//
//	version:u8 | kind:u8 | targetLen:u16 | target[] | ticketLen:u16 | ticket[]
func WriteHeader(w io.Writer, h Header) error {
	if len(h.Target) > maxHeaderField || len(h.Ticket) > maxHeaderField {
		return fmt.Errorf("multipath: stream header field exceeds %d bytes", maxHeaderField)
	}
	buf := make([]byte, 0, 6+len(h.Target)+len(h.Ticket))
	buf = append(buf, headerVersion, byte(h.Kind))
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(h.Target)))
	buf = append(buf, h.Target...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(h.Ticket)))
	buf = append(buf, h.Ticket...)
	_, err := w.Write(buf)
	return err
}

// ReadHeader reads a Header written by WriteHeader. It reads exactly the header bytes and no more,
// so the caller may splice the stream immediately afterwards.
func ReadHeader(r io.Reader) (Header, error) {
	var fixed [2]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return Header{}, err
	}
	if fixed[0] != headerVersion {
		return Header{}, fmt.Errorf("multipath: unsupported stream header version %d", fixed[0])
	}
	h := Header{Kind: StreamKind(fixed[1])}
	target, err := readField(r)
	if err != nil {
		return Header{}, err
	}
	h.Target = string(target)
	if h.Ticket, err = readField(r); err != nil {
		return Header{}, err
	}
	return h, nil
}

// readField reads one u16-length-prefixed field, rejecting a length past the bound before it reads
// the body.
func readField(r io.Reader) ([]byte, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint16(lenBuf[:])
	if int(n) > maxHeaderField {
		return nil, fmt.Errorf("multipath: stream header field length %d exceeds %d", n, maxHeaderField)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}
