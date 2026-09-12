// Wire framing for the redundant stream: encode/decode the frame types and a streaming reader that
// reassembles frames off a byte-oriented link. Kept separate from the stream logic so the wire
// format is easy to read in one place and to mirror in the JVM implementation.
//
// Frame layout (all integers big-endian):
//
//	HELLO 0x05 | connID[16] | linkIndex:u16
//	DATA  0x01 | offset:u64  | len:u16 | crc32:u32 | payload[len]
//	ACK   0x02 | cumulative:u64
//	PING  0x03 | nonce:u64
//	PONG  0x04 | nonce:u64
//	REJECT 0x06 | reasonLen:u16 | reason[reasonLen]
//
// REJECT is the one frame that flows server→client: the origin refuses a link (e.g. a HELLO link
// index out of range) and says why, in UTF-8, before closing. Without it a refused link is
// indistinguishable from a flaky network — the client would back off, reconnect, be closed again,
// forever, while Dial reported success (it only wrote HELLO) and every request hung. With it the
// client surfaces the reason and stops retrying a link that will never be accepted.
//
// HELLO is the first frame a client sends on every link (initial dial and every reconnect). It lets
// a server that accepts many independent TCP connections group them: all links carrying the same
// connID are the same logical stream, and a reconnecting link re-attaches to it.

package multipath

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// frame is one decoded frame. Only the fields relevant to typ are set.
type frame struct {
	typ     byte
	offset  uint64   // DATA offset, or ACK cumulative
	payload []byte   // DATA
	nonce   uint64   // PING / PONG
	connID  [16]byte // HELLO
	linkIdx uint16   // HELLO
	reason  string   // REJECT
}

// encodeReject frames a server→client refusal carrying a UTF-8 reason. The reason is bounded so a
// hostile length cannot force a large allocation on the client.
func encodeReject(reason string) []byte {
	r := []byte(reason)
	if len(r) > maxSegment {
		r = r[:maxSegment]
	}
	b := make([]byte, 1+2+len(r))
	b[0] = frameReject
	binary.BigEndian.PutUint16(b[1:], uint16(len(r)))
	copy(b[3:], r)
	return b
}

func encodeHello(connID [16]byte, linkIdx uint16) []byte {
	b := make([]byte, 1+16+2)
	b[0] = frameHello
	copy(b[1:], connID[:])
	binary.BigEndian.PutUint16(b[17:], linkIdx)
	return b
}

func encodeData(offset uint64, payload []byte) []byte {
	b := make([]byte, dataHdrLen+len(payload))
	b[0] = frameData
	binary.BigEndian.PutUint64(b[1:], offset)
	binary.BigEndian.PutUint16(b[9:], uint16(len(payload)))
	binary.BigEndian.PutUint32(b[11:], crc32.ChecksumIEEE(payload))
	copy(b[dataHdrLen:], payload)
	return b
}

func encodeAck(cumulative uint64) []byte {
	b := make([]byte, 1+8)
	b[0] = frameAck
	binary.BigEndian.PutUint64(b[1:], cumulative)
	return b
}

func encodePing(nonce uint64) []byte { return encodeNonce(framePing, nonce) }
func encodePong(nonce uint64) []byte { return encodeNonce(framePong, nonce) }

func encodeNonce(typ byte, nonce uint64) []byte {
	b := make([]byte, 1+8)
	b[0] = typ
	binary.BigEndian.PutUint64(b[1:], nonce)
	return b
}

// frameReader reads whole frames off a link. It uses io.ReadFull, so a link that delivers bytes in
// arbitrary chunk boundaries still yields correct frames.
type frameReader struct {
	conn io.Reader
	hdr  [dataHdrLen]byte
}

// next returns the next frame. A corrupt DATA frame (CRC or length mismatch) or an unknown tag is
// reported as errCorruptFrame so the caller drops the link; the same bytes arrive intact on another
// link or a replay.
func (r *frameReader) next() (frame, error) {
	var t [1]byte
	if _, err := io.ReadFull(r.conn, t[:]); err != nil {
		return frame{}, err
	}
	switch t[0] {
	case frameHello:
		var b [18]byte
		if _, err := io.ReadFull(r.conn, b[:]); err != nil {
			return frame{}, err
		}
		f := frame{typ: frameHello, linkIdx: binary.BigEndian.Uint16(b[16:])}
		copy(f.connID[:], b[:16])
		return f, nil
	case frameData:
		if _, err := io.ReadFull(r.conn, r.hdr[1:dataHdrLen]); err != nil {
			return frame{}, err
		}
		offset := binary.BigEndian.Uint64(r.hdr[1:])
		n := binary.BigEndian.Uint16(r.hdr[9:])
		want := binary.BigEndian.Uint32(r.hdr[11:])
		if n > maxSegment {
			return frame{}, errCorruptFrame
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r.conn, buf); err != nil {
			return frame{}, err
		}
		if crc32.ChecksumIEEE(buf) != want {
			return frame{}, errCorruptFrame
		}
		return frame{typ: frameData, offset: offset, payload: buf}, nil
	case frameAck:
		var b [8]byte
		if _, err := io.ReadFull(r.conn, b[:]); err != nil {
			return frame{}, err
		}
		return frame{typ: frameAck, offset: binary.BigEndian.Uint64(b[:])}, nil
	case framePing, framePong:
		var b [8]byte
		if _, err := io.ReadFull(r.conn, b[:]); err != nil {
			return frame{}, err
		}
		return frame{typ: t[0], nonce: binary.BigEndian.Uint64(b[:])}, nil
	case frameReject:
		var l [2]byte
		if _, err := io.ReadFull(r.conn, l[:]); err != nil {
			return frame{}, err
		}
		n := binary.BigEndian.Uint16(l[:])
		if n > maxSegment {
			return frame{}, errCorruptFrame
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r.conn, buf); err != nil {
			return frame{}, err
		}
		return frame{typ: frameReject, reason: string(buf)}, nil
	default:
		return frame{}, errCorruptFrame
	}
}
