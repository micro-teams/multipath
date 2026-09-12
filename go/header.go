// L5 — the one thing said at the start of a mux stream: which named service it wants.
//
// A mux stream is an opaque duplex once it is open, but the server has to know what to do with a
// freshly accepted one before any payload flows. In this substrate every stream is a request for a
// named service the origin has registered — there is no "main service" and no client-chosen address.
// The header is therefore just the service name and an opaque ticket the consumer (not this library)
// uses to authorise the request, sent once ahead of the bytes: the client writes it and starts
// streaming, the server looks the name up in its registry and hands the stream to that service's
// handler. A name the origin has not registered is refused — so a client can never reach an address
// of its own choosing, which removes the open-relay/SSRF surface by construction.
//
// The library defines the format and carries the ticket as opaque bytes; it never reads the ticket
// and never decides whether a service is allowed. The service name's meaning and the ticket's
// meaning are the origin's registry and handler concern.

package multipath

import (
	"encoding/binary"
	"fmt"
	"io"
)

// headerVersion is bumped only on an incompatible change to the layout below; a reader rejects
// anything it does not recognise rather than guessing.
const headerVersion = 2

// maxHeaderField bounds the two length-prefixed fields so a corrupt or hostile length cannot make
// the reader allocate without limit before any policy has run.
const maxHeaderField = 4096

// Header is what a client says at the start of a mux stream: the name of the registered service it
// wants, plus an opaque ticket its handler interprets.
type Header struct {
	// Service is the registered service name the stream is for. The origin refuses an unknown name.
	Service string
	// Ticket is an opaque capability the service's handler interprets; the library does not.
	Ticket []byte
}

// WriteHeader writes h ahead of the stream payload. Layout (big-endian):
//
//	version:u8 | serviceLen:u16 | service[] | ticketLen:u16 | ticket[]
func WriteHeader(w io.Writer, h Header) error {
	if len(h.Service) > maxHeaderField || len(h.Ticket) > maxHeaderField {
		return fmt.Errorf("multipath: stream header field exceeds %d bytes", maxHeaderField)
	}
	buf := make([]byte, 0, 5+len(h.Service)+len(h.Ticket))
	buf = append(buf, headerVersion)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(h.Service)))
	buf = append(buf, h.Service...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(h.Ticket)))
	buf = append(buf, h.Ticket...)
	_, err := w.Write(buf)
	return err
}

// ReadHeader reads a Header written by WriteHeader. It reads exactly the header bytes and no more,
// so the caller may hand the stream to the service handler immediately afterwards.
func ReadHeader(r io.Reader) (Header, error) {
	var ver [1]byte
	if _, err := io.ReadFull(r, ver[:]); err != nil {
		return Header{}, err
	}
	if ver[0] != headerVersion {
		return Header{}, fmt.Errorf("multipath: unsupported stream header version %d", ver[0])
	}
	service, err := readField(r)
	if err != nil {
		return Header{}, err
	}
	h := Header{Service: string(service)}
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
