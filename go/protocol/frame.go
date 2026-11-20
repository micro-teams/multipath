// Link frame codecs (PROTOCOL.md section 4): every frame is `type u8 | length u16 | payload`, and
// each type's payload has a fixed layout. Encoders return a whole frame, ready to be written as one
// WebSocket message; decoders take a payload as ParseFrame returned it and check its size exactly,
// because a frame of a known type with the wrong size is a protocol violation, not something to
// guess around. ParseFrame itself knows no types, so a frame of an unknown type is handed back like
// any other, for the caller to skip.
//
//   - ParseFrame(msg, maxPayload) (typ, payload, err): splits one link message, which must hold
//     exactly one frame (PROTOCOL.md section 3); a declared length above maxPayload is ErrTooLarge.
//
//   - EncodeHello / DecodeHello, EncodeWelcome / DecodeWelcome, EncodeReject / DecodeReject: the
//     handshake (section 4.2), with the HelloFrame, WelcomeFrame and RejectFrame values.
//
//   - EncodeData / DecodeData: offset, CRC-32 and up to MaxData bytes; a CRC mismatch is ErrCorrupt.
//
//   - EncodeAck, EncodePing, EncodePong / DecodeU64: the three 8-byte frames.
//
//   - EncodeGoodbye / DecodeReason: GOODBYE's reason (DecodeReason also reads RST reasons).
//
//     f := protocol.EncodeData(0, []byte("hi"))
//     typ, payload, err := protocol.ParseFrame(f, protocol.DefaultLimits().MaxPayload)
//     off, data, err := protocol.DecodeData(payload) // 0, "hi"
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

// ErrTooLarge is returned by ParseFrame for a frame longer than the caller's limit.
var ErrTooLarge = errors.New("multipath: frame too large")

// ErrCorrupt is returned for a frame whose payload has the wrong size or a bad CRC.
var ErrCorrupt = errors.New("multipath: corrupt frame")

// HelloFrame is the client's first frame on every link.
type HelloFrame struct {
	Version uint16
	Session [16]byte
	Link    uint8
	Mode    byte
	Impl    string
}

// WelcomeFrame accepts a link. Created is true when this link created the session.
type WelcomeFrame struct {
	Version uint16
	Created bool
	Limits  Limits
	Impl    string
}

// RejectFrame refuses a link, with one of the Reject codes.
type RejectFrame struct {
	Code   byte
	Reason string
}

// ParseFrame splits one link message into its frame type and payload; the payload aliases msg.
func ParseFrame(msg []byte, maxPayload int) (byte, []byte, error) {
	if len(msg) < HeaderLen {
		return 0, nil, fmt.Errorf("%w: message of %d bytes", ErrCorrupt, len(msg))
	}
	n := int(binary.BigEndian.Uint16(msg[1:]))
	if n > maxPayload {
		return 0, nil, fmt.Errorf("%w: type 0x%02x length %d > %d", ErrTooLarge, msg[0], n, maxPayload)
	}
	if len(msg) != HeaderLen+n {
		return 0, nil, fmt.Errorf("%w: message of %d bytes holds a frame of %d", ErrCorrupt, len(msg), HeaderLen+n)
	}
	return msg[0], msg[HeaderLen:], nil
}

// Fixed parts of the HELLO and WELCOME payloads, before the impl string.
const (
	helloLen   = 20
	welcomeLen = 24
)

func frame(typ byte, size int) []byte {
	b := make([]byte, HeaderLen, HeaderLen+size)
	b[0] = typ
	binary.BigEndian.PutUint16(b[1:], uint16(size))
	return b
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// EncodeHello frames a HELLO; Impl is cut to MaxImpl bytes.
func EncodeHello(h HelloFrame) []byte {
	impl := clip(h.Impl, MaxImpl)
	b := frame(Hello, helloLen+len(impl))
	b = binary.BigEndian.AppendUint16(b, h.Version)
	b = append(b, h.Session[:]...)
	b = append(b, h.Link, h.Mode)
	return append(b, impl...)
}

// DecodeHello parses a HELLO payload. The version is returned as sent; checking it is the server's.
func DecodeHello(p []byte) (HelloFrame, error) {
	if len(p) < helloLen || len(p) > helloLen+MaxImpl || p[19] > ModeJoin {
		return HelloFrame{}, fmt.Errorf("%w: hello of %d bytes", ErrCorrupt, len(p))
	}
	h := HelloFrame{Version: binary.BigEndian.Uint16(p), Link: p[18], Mode: p[19], Impl: string(p[helloLen:])}
	copy(h.Session[:], p[2:18])
	return h, nil
}

// EncodeWelcome frames a WELCOME; the limits must already be valid.
func EncodeWelcome(w WelcomeFrame) []byte {
	impl := clip(w.Impl, MaxImpl)
	b := frame(Welcome, welcomeLen+len(impl))
	b = binary.BigEndian.AppendUint16(b, w.Version)
	flags := byte(0)
	if w.Created {
		flags = 1
	}
	l := w.Limits
	b = append(b, flags)
	b = binary.BigEndian.AppendUint16(b, uint16(l.MaxPayload))
	b = binary.BigEndian.AppendUint32(b, uint32(l.Window))
	b = binary.BigEndian.AppendUint32(b, uint32(l.StreamWindow))
	b = binary.BigEndian.AppendUint16(b, uint16(l.MaxStreams))
	b = append(b, byte(l.MaxLinks))
	for _, d := range []time.Duration{l.Ping, l.ClientDead, l.ServerDead, l.Progress} {
		b = binary.BigEndian.AppendUint16(b, uint16(d/time.Millisecond))
	}
	return append(b, impl...)
}

// DecodeWelcome parses a WELCOME payload and validates its limits.
func DecodeWelcome(p []byte) (WelcomeFrame, error) {
	if len(p) < welcomeLen || len(p) > welcomeLen+MaxImpl {
		return WelcomeFrame{}, fmt.Errorf("%w: welcome of %d bytes", ErrCorrupt, len(p))
	}
	ms := func(i int) time.Duration { return time.Duration(binary.BigEndian.Uint16(p[i:])) * time.Millisecond }
	w := WelcomeFrame{
		Version: binary.BigEndian.Uint16(p),
		Created: p[2]&1 != 0,
		Limits: Limits{
			MaxPayload:   int(binary.BigEndian.Uint16(p[3:])),
			Window:       int(binary.BigEndian.Uint32(p[5:])),
			StreamWindow: int(binary.BigEndian.Uint32(p[9:])),
			MaxStreams:   int(binary.BigEndian.Uint16(p[13:])),
			MaxLinks:     int(p[15]),
			Ping:         ms(16),
			ClientDead:   ms(18),
			ServerDead:   ms(20),
			Progress:     ms(22),
		},
		Impl: string(p[welcomeLen:]),
	}
	if err := w.Limits.Validate(); err != nil {
		return WelcomeFrame{}, fmt.Errorf("%w: welcome limits: %v", ErrCorrupt, err)
	}
	return w, nil
}

// EncodeReject frames a REJECT; the reason is cut to MaxReason bytes.
func EncodeReject(code byte, reason string) []byte {
	reason = clip(reason, MaxReason)
	b := frame(Reject, 1+len(reason))
	b = append(b, code)
	return append(b, reason...)
}

// DecodeReject parses a REJECT payload.
func DecodeReject(p []byte) (RejectFrame, error) {
	if len(p) < 1 || len(p) > 1+MaxReason {
		return RejectFrame{}, fmt.Errorf("%w: reject of %d bytes", ErrCorrupt, len(p))
	}
	return RejectFrame{Code: p[0], Reason: string(p[1:])}, nil
}

// EncodeData frames up to MaxData stream bytes starting at offset.
func EncodeData(offset uint64, data []byte) []byte {
	b := frame(Data, DataOverhead+len(data))
	b = binary.BigEndian.AppendUint64(b, offset)
	b = binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(data))
	return append(b, data...)
}

// DecodeData parses a DATA payload; the returned bytes alias p.
func DecodeData(p []byte) (uint64, []byte, error) {
	if len(p) <= DataOverhead || len(p) > DataOverhead+MaxData {
		return 0, nil, fmt.Errorf("%w: data of %d bytes", ErrCorrupt, len(p))
	}
	data := p[DataOverhead:]
	if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(p[8:]) {
		return 0, nil, fmt.Errorf("%w: data crc mismatch", ErrCorrupt)
	}
	return binary.BigEndian.Uint64(p), data, nil
}

func encodeU64(typ byte, v uint64) []byte { return binary.BigEndian.AppendUint64(frame(typ, 8), v) }

// EncodeAck frames an ACK of every byte below offset.
func EncodeAck(offset uint64) []byte { return encodeU64(Ack, offset) }

// EncodePing frames a PING.
func EncodePing(nonce uint64) []byte { return encodeU64(Ping, nonce) }

// EncodePong frames the PONG answering a PING with nonce.
func EncodePong(nonce uint64) []byte { return encodeU64(Pong, nonce) }

// DecodeU64 parses an ACK, PING or PONG payload.
func DecodeU64(p []byte) (uint64, error) {
	if len(p) != 8 {
		return 0, fmt.Errorf("%w: 8-byte frame of %d bytes", ErrCorrupt, len(p))
	}
	return binary.BigEndian.Uint64(p), nil
}

// EncodeGoodbye frames a GOODBYE; the reason is cut to MaxReason bytes.
func EncodeGoodbye(reason string) []byte {
	reason = clip(reason, MaxReason)
	return append(frame(Goodbye, len(reason)), reason...)
}

// DecodeReason parses a GOODBYE payload or an RST body.
func DecodeReason(p []byte) (string, error) {
	if len(p) > MaxReason {
		return "", fmt.Errorf("%w: reason of %d bytes", ErrCorrupt, len(p))
	}
	return string(p), nil
}
