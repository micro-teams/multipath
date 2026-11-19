// Package link carries MultiPath frames over WebSocket connections (PROTOCOL.md section 3): one
// binary message is one frame. It holds a minimal RFC 6455 implementation rather than a dependency,
// because a link needs only binary messages and the few control frames, and because the bounds that
// keep a server safe (the largest message, a close that never blocks) must be under our control.
//
// This file: the connection and the server side of the handshake.
//
//   - Accept(w, r) upgrades an HTTP request to a server-side *Conn, or answers 400 and returns an
//     error. The request is left to the caller, which reads the client address from its headers.
//
//   - (*Conn).ReadMessage(max) returns the payload of the next binary message, answering pings and
//     skipping pongs on the way. A message longer than max fails before its payload is read; a text
//     message or a close fails too, since neither belongs on a link.
//
//   - (*Conn).WriteMessage(p) writes p as one binary message. Writes are serialised; callers bound
//     their duration with SetWriteDeadline.
//
//   - (*Conn).Close() closes the connection without ever blocking: it sends a close frame only when
//     no write is in progress, because a write stuck on a full TCP buffer must not hold up a close.
//
//   - RemoteAddr, SetDeadline, SetReadDeadline and SetWriteDeadline pass through to the socket.
//
//     http.HandleFunc("/multipath/v3/link", func(w http.ResponseWriter, r *http.Request) {
//     c, err := link.Accept(w, r)
//     if err != nil { log.Print(err); return }
//     msg, err := c.ReadMessage(65538)
//     ...
//     })
package link

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// wsGUID is the RFC 6455 value mixed into the handshake so an accept token cannot be forged from the
// key alone.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// maxControl is the largest control frame payload RFC 6455 allows.
const maxControl = 125

// ErrTooLarge is returned by ReadMessage for a message longer than its limit.
var ErrTooLarge = errors.New("multipath: websocket message too large")

// Conn is one WebSocket connection carrying binary messages.
type Conn struct {
	raw    net.Conn
	br     *bufio.Reader
	client bool // a client masks what it writes, as RFC 6455 requires

	writeMu sync.Mutex
	closeMu sync.Mutex
	closed  bool
}

// Accept performs the server handshake on an HTTP upgrade request.
func Accept(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if r.Method != http.MethodGet || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || key == "" {
		http.Error(w, "expected a WebSocket upgrade", http.StatusBadRequest)
		return nil, fmt.Errorf("multipath: not a websocket upgrade from %s", r.RemoteAddr)
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "connection cannot be upgraded", http.StatusInternalServerError)
		return nil, errors.New("multipath: response writer cannot hijack")
	}
	raw, brw, err := hj.Hijack()
	if err != nil {
		return nil, fmt.Errorf("multipath: hijack: %w", err)
	}
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptToken(key) + "\r\n\r\n"
	if _, err := io.WriteString(raw, response); err != nil {
		_ = raw.Close() // the write error is what the caller needs; the close adds nothing
		return nil, err
	}
	return &Conn{raw: raw, br: brw.Reader}, nil
}

// acceptToken computes the Sec-WebSocket-Accept value for a key.
func acceptToken(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// ReadMessage returns the next binary message of at most max bytes.
func (c *Conn) ReadMessage(max int) ([]byte, error) {
	var msg []byte
	for {
		fin, op, payload, err := c.readFrame(max - len(msg))
		if err != nil {
			return nil, err
		}
		switch op {
		case opBinary, opContinuation:
			msg = append(msg, payload...)
			if fin {
				return msg, nil
			}
		case opPing:
			if err := c.writeFrame(opPong, payload); err != nil {
				return nil, err
			}
		case opPong:
		case opClose:
			return nil, io.EOF
		default:
			return nil, fmt.Errorf("multipath: websocket opcode %d on a link", op)
		}
	}
}

// readFrame reads one WebSocket frame whose data payload is at most max bytes.
func (c *Conn) readFrame(max int) (fin bool, op byte, payload []byte, err error) {
	var h [2]byte
	if _, err = io.ReadFull(c.br, h[:]); err != nil {
		return
	}
	fin, op = h[0]&0x80 != 0, h[0]&0x0f
	length := uint64(h[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	limit := uint64(max)
	if op >= opClose {
		limit = maxControl
	}
	if length > limit {
		err = fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, length, limit)
		return
	}
	var mask [4]byte
	masked := h[1]&0x80 != 0
	if masked {
		if _, err = io.ReadFull(c.br, mask[:]); err != nil {
			return
		}
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(c.br, payload); err != nil {
		return
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return
}

// WriteMessage writes p as one binary message.
func (c *Conn) WriteMessage(p []byte) error { return c.writeFrame(opBinary, p) }

// writeFrame writes one unfragmented frame, masked when this end is the client.
func (c *Conn) writeFrame(op byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrameLocked(op, payload)
}

func (c *Conn) writeFrameLocked(op byte, payload []byte) error {
	b := make([]byte, 0, 14+len(payload))
	b = append(b, 0x80|op)
	maskBit := byte(0)
	if c.client {
		maskBit = 0x80
	}
	switch n := len(payload); {
	case n < 126:
		b = append(b, maskBit|byte(n))
	case n < 1<<16:
		b = binary.BigEndian.AppendUint16(append(b, maskBit|126), uint16(n))
	default:
		b = binary.BigEndian.AppendUint64(append(b, maskBit|127), uint64(n))
	}
	if !c.client {
		_, err := c.raw.Write(append(b, payload...))
		return err
	}
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	b = append(b, mask[:]...)
	for i, v := range payload {
		b = append(b, v^mask[i%4])
	}
	_, err := c.raw.Write(b)
	return err
}

// Close closes the connection. It never waits for a write in progress.
func (c *Conn) Close() error {
	c.closeMu.Lock()
	already := c.closed
	c.closed = true
	c.closeMu.Unlock()
	if already {
		return nil
	}
	if c.writeMu.TryLock() {
		// The close frame is a courtesy to the peer; a socket that cannot take it within a moment is
		// being closed anyway, so its write error carries no information.
		_ = c.raw.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_ = c.writeFrameLocked(opClose, nil)
		c.writeMu.Unlock()
	}
	return c.raw.Close()
}

// RemoteAddr returns the peer address of the socket.
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }

// SetDeadline sets the socket's read and write deadlines.
func (c *Conn) SetDeadline(t time.Time) error { return c.raw.SetDeadline(t) }

// SetReadDeadline sets the socket's read deadline.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.raw.SetReadDeadline(t) }

// SetWriteDeadline sets the socket's write deadline.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }
