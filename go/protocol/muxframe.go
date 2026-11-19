// Mux frame codecs (PROTOCOL.md section 6). Mux frames travel inside the session byte stream, which
// arrives in pieces of any size, so the decoder works on a buffer and reports how much of it one
// frame used, or that the frame is not complete yet.
//
//   - MuxSyn ... MuxWindow are the mux frame types; MaxMuxData bounds one DATA body.
//
//   - EncodeMux(typ, stream, body) returns one whole mux frame. A sender writes it to the session in
//     one piece, so it travels as one DATA frame.
//
//   - ParseMux(buf) (MuxFrame, n, err) decodes the first frame of buf: n == 0 means "need more
//     bytes"; otherwise the frame used buf[:n] and its Body aliases buf. A frame shorter than its
//     stream id is ErrCorrupt. Unknown types are returned like any other, for the caller to skip.
//
//     f := protocol.EncodeMux(protocol.MuxData, 1, []byte("hi"))
//     m, n, err := protocol.ParseMux(f) // m.Type == MuxData, m.Stream == 1, n == len(f)
package protocol

import (
	"encoding/binary"
	"fmt"
)

// Mux frame types.
const (
	MuxSyn    byte = 0x01
	MuxData   byte = 0x02
	MuxFin    byte = 0x03
	MuxRst    byte = 0x04
	MuxWindow byte = 0x05
)

// MaxMuxData is the largest DATA body; a mux frame then still fits one session DATA frame.
const MaxMuxData = 16384

// muxHeader is type u8, length u16 and stream u32.
const muxHeader = 7

// MuxFrame is one decoded mux frame.
type MuxFrame struct {
	Type   byte
	Stream uint32
	Body   []byte
}

// EncodeMux returns one mux frame.
func EncodeMux(typ byte, stream uint32, body []byte) []byte {
	b := make([]byte, muxHeader, muxHeader+len(body))
	b[0] = typ
	binary.BigEndian.PutUint16(b[1:], uint16(4+len(body)))
	binary.BigEndian.PutUint32(b[3:], stream)
	return append(b, body...)
}

// ParseMux decodes the first frame of buf, or returns n == 0 if buf does not hold a whole frame.
func ParseMux(buf []byte) (MuxFrame, int, error) {
	if len(buf) < HeaderLen {
		return MuxFrame{}, 0, nil
	}
	length := int(binary.BigEndian.Uint16(buf[1:]))
	if length < 4 {
		return MuxFrame{}, 0, fmt.Errorf("%w: mux frame length %d", ErrCorrupt, length)
	}
	if len(buf) < HeaderLen+length {
		return MuxFrame{}, 0, nil
	}
	f := MuxFrame{Type: buf[0], Stream: binary.BigEndian.Uint32(buf[3:]), Body: buf[muxHeader : HeaderLen+length]}
	return f, HeaderLen + length, nil
}
