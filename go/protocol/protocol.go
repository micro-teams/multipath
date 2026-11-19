// Package protocol holds the MultiPath version-3 constants, limits and frame codecs that every other
// package shares. It is the Go mirror of PROTOCOL.md: a number that appears on the wire is defined
// here and nowhere else, so the client and the server cannot disagree about it.
//
// This file: versions, frame type codes, fixed bounds, and the negotiated Limits.
//
//   - Version is the protocol version (3); Implementation is the impl string sent in HELLO and
//     WELCOME, for example "multipath-go/0.3.0-dev". ImplVersion comes from version.go.
//
//   - The link frame types (Hello ... Goodbye), the HELLO modes (ModeNew, ModeJoin) and the REJECT
//     codes (RejectVersion ... RejectMalformed) are the values of PROTOCOL.md section 4.
//
//   - MaxData, MaxReason, MaxImpl, ControlSlack and HandshakeTimeout are fixed by the protocol.
//
//   - Limits is what a server announces in WELCOME and both sides then enforce. DefaultLimits returns
//     the defaults of PROTOCOL.md section 7; Validate checks a Limits against the protocol's bounds.
//
//     l := protocol.DefaultLimits()
//     l.Window = 4 << 20
//     if err := l.Validate(); err != nil { log.Fatal(err) }
package protocol

import (
	"fmt"
	"time"
)

// Version is the protocol version this package speaks.
const Version = 3

// Implementation names this implementation in HELLO and WELCOME, so each side can log its peer's.
const Implementation = "multipath-go/" + ImplVersion

// Link frame types (PROTOCOL.md section 4.1).
const (
	Hello   byte = 0x01
	Welcome byte = 0x02
	Reject  byte = 0x03
	Data    byte = 0x04
	Ack     byte = 0x05
	Ping    byte = 0x06
	Pong    byte = 0x07
	Goodbye byte = 0x08
)

// HELLO modes: a link of a session the server has not confirmed yet, or of one it has.
const (
	ModeNew  byte = 0
	ModeJoin byte = 1
)

// REJECT codes.
const (
	RejectVersion        byte = 1
	RejectUnknownSession byte = 2
	RejectLimit          byte = 3
	RejectMalformed      byte = 4
)

const (
	// HeaderLen is the frame envelope: type u8 and length u16.
	HeaderLen = 3
	// MaxData is the largest number of stream bytes in one DATA frame.
	MaxData = 32768
	// DataOverhead is the DATA payload before its bytes: offset u64 and crc32 u32.
	DataOverhead = 12
	// MaxReason bounds REJECT, GOODBYE and RST reasons.
	MaxReason = 256
	// MaxImpl bounds the impl string of HELLO and WELCOME.
	MaxImpl = 64
	// ControlSlack is how far mux control frames may run past the session window.
	ControlSlack = 65536
	// HandshakeTimeout bounds the WebSocket upgrade plus HELLO on the server, and HELLO to WELCOME
	// on the client.
	HandshakeTimeout = 5 * time.Second
)

// Limits are the values a server announces in WELCOME. Both sides enforce them for the session.
type Limits struct {
	MaxPayload   int           // largest frame payload accepted
	Window       int           // session send window and reorder span, in bytes
	StreamWindow int           // initial mux credit per stream and direction, in bytes
	MaxStreams   int           // open streams per session
	MaxLinks     int           // links per session; link indexes are 0 to MaxLinks-1
	Ping         time.Duration // client PING interval
	ClientDead   time.Duration // client: silence after which a link is dead
	ServerDead   time.Duration // server: silence after which a link is dead
	Progress     time.Duration // longest wait for an ACK while data is outstanding on a link
}

// DefaultLimits returns the defaults of PROTOCOL.md section 7.
func DefaultLimits() Limits {
	return Limits{
		MaxPayload:   65535,
		Window:       2 << 20,
		StreamWindow: 256 << 10,
		MaxStreams:   256,
		MaxLinks:     8,
		Ping:         500 * time.Millisecond,
		ClientDead:   time.Second,
		ServerDead:   3 * time.Second,
		Progress:     time.Second,
	}
}

// MinWindow is the smallest session window a server may announce; a client uses it until the first
// WELCOME, so that bytes it buffers before then always fit the server's reorder span.
const MinWindow = 256 << 10

// Validate reports the first limit outside the bounds of PROTOCOL.md section 7.
func (l Limits) Validate() error {
	ms := func(d time.Duration) bool { return d >= time.Millisecond && d <= 65535*time.Millisecond }
	switch {
	case l.MaxPayload < DataOverhead+MaxData || l.MaxPayload > 65535:
		return fmt.Errorf("max payload %d outside %d..65535", l.MaxPayload, DataOverhead+MaxData)
	case l.Window < MinWindow || l.Window > 64<<20:
		return fmt.Errorf("window %d outside %d..%d", l.Window, MinWindow, 64<<20)
	case l.StreamWindow < 64<<10 || l.StreamWindow > 16<<20:
		return fmt.Errorf("stream window %d outside %d..%d", l.StreamWindow, 64<<10, 16<<20)
	case l.MaxStreams < 1 || l.MaxStreams > 65535:
		return fmt.Errorf("max streams %d outside 1..65535", l.MaxStreams)
	case l.MaxLinks < 1 || l.MaxLinks > 255:
		return fmt.Errorf("max links %d outside 1..255", l.MaxLinks)
	case !ms(l.Ping) || !ms(l.ClientDead) || !ms(l.ServerDead) || !ms(l.Progress):
		return fmt.Errorf("timings must be 1 ms to 65535 ms: %+v", l)
	case l.ClientDead <= l.Ping || l.ServerDead <= l.Ping:
		return fmt.Errorf("dead times %v and %v must exceed the ping interval %v", l.ClientDead, l.ServerDead, l.Ping)
	}
	return nil
}
