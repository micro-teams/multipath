// Wire framing for the redundant stream: encode/decode the four frame types and a streaming reader
// that reassembles frames off a byte-oriented link. Kept separate from the stream logic so the wire
// format is easy to read in one place and to mirror in the JVM implementation.

package multipath

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

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

func encodePing(nonce uint64) []byte {
	b := make([]byte, 1+8)
	b[0] = framePing
	binary.BigEndian.PutUint64(b[1:], nonce)
	return b
}

func encodePong(nonce uint64) []byte {
	b := make([]byte, 1+8)
	b[0] = framePong
	binary.BigEndian.PutUint64(b[1:], nonce)
	return b
}

// frameReader reads whole frames off a link. It buffers exactly, using io.ReadFull, so a link that
// delivers bytes in arbitrary chunk boundaries still yields correct frames.
type frameReader struct {
	conn io.Reader
	hdr  [dataHdrLen]byte
}

// next returns the next frame. For DATA it returns (frameData, offset, payload, 0). For ACK it
// returns (frameAck, cumulative, nil, 0) — the value rides in the offset slot. For PING/PONG it
// returns (typ, 0, nil, nonce). A corrupt DATA frame (CRC or length mismatch) is reported as an
// error so the caller drops the link; the same bytes arrive intact on another link or a replay.
func (r *frameReader) next() (typ byte, offset uint64, payload []byte, nonce uint64, err error) {
	var t [1]byte
	if _, err = io.ReadFull(r.conn, t[:]); err != nil {
		return 0, 0, nil, 0, err
	}
	switch t[0] {
	case frameData:
		if _, err = io.ReadFull(r.conn, r.hdr[1:dataHdrLen]); err != nil {
			return 0, 0, nil, 0, err
		}
		offset = binary.BigEndian.Uint64(r.hdr[1:])
		n := binary.BigEndian.Uint16(r.hdr[9:])
		want := binary.BigEndian.Uint32(r.hdr[11:])
		if n > maxSegment {
			return 0, 0, nil, 0, errCorruptFrame
		}
		buf := make([]byte, n)
		if _, err = io.ReadFull(r.conn, buf); err != nil {
			return 0, 0, nil, 0, err
		}
		if crc32.ChecksumIEEE(buf) != want {
			return 0, 0, nil, 0, errCorruptFrame
		}
		return frameData, offset, buf, 0, nil
	case frameAck:
		var b [8]byte
		if _, err = io.ReadFull(r.conn, b[:]); err != nil {
			return 0, 0, nil, 0, err
		}
		return frameAck, binary.BigEndian.Uint64(b[:]), nil, 0, nil
	case framePing, framePong:
		var b [8]byte
		if _, err = io.ReadFull(r.conn, b[:]); err != nil {
			return 0, 0, nil, 0, err
		}
		return t[0], 0, nil, binary.BigEndian.Uint64(b[:]), nil
	default:
		return 0, 0, nil, 0, errCorruptFrame
	}
}
