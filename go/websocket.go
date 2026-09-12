// A minimal RFC 6455 WebSocket, client and server, just enough to carry a link over a CDN edge that
// terminates TLS and forwards only HTTP. It is deliberately tiny: a link is an opaque byte stream,
// so the only opcode that matters is binary, and control frames (ping/pong/close) are handled just
// enough to keep the stream honest. There are no extensions, no compression, no text frames.
//
// This lives in the package rather than in a dependency because MultiPath ships with none: a
// WebSocket used only as a byte pipe is a page of framing, not a reason to take on a library whose
// message semantics we do not want anyway. The wsConn it returns is a plain net.Conn — a byte
// stream, not a message stream — so everything above it (redundant frames, mux) is unchanged
// whether a link is raw TLS or a WebSocket.

package multipath

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// wsGUID is the RFC 6455 magic value mixed into the handshake so an accept token cannot be forged
// from the key alone.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	wsOpContinuation = 0x0
	wsOpText         = 0x1 // unused by the link layer (links never carry text frames); appws.go does
	wsOpBinary       = 0x2
	wsOpClose        = 0x8
	wsOpPing         = 0x9
	wsOpPong         = 0xA
)

// clientWebSocket performs the client handshake over an already-connected (and, for wss, already
// TLS-wrapped) conn, then returns a byte-stream net.Conn carrying binary frames.
func clientWebSocket(ctx context.Context, raw net.Conn, host, path string) (net.Conn, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = raw.SetDeadline(deadline)
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		_ = raw.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	request := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(raw, request); err != nil {
		_ = raw.Close()
		return nil, err
	}

	br := bufio.NewReader(raw)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("multipath: websocket handshake read: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = raw.Close()
		return nil, fmt.Errorf("multipath: websocket handshake got %s", resp.Status)
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") ||
		resp.Header.Get("Sec-WebSocket-Accept") != wsAccept(key) {
		_ = raw.Close()
		return nil, fmt.Errorf("multipath: websocket handshake response did not confirm the upgrade")
	}
	_ = raw.SetDeadline(time.Time{})
	return newWSConn(raw, br, true), nil
}

// acceptWebSocket performs the server handshake on a freshly accepted conn whose first bytes are an
// HTTP upgrade request. The link acceptor calls it for ws/wss lines; path must match what the
// client dialled.
func acceptWebSocket(raw net.Conn, path string) (net.Conn, error) {
	br := bufio.NewReader(raw)
	req, err := http.ReadRequest(br)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("multipath: websocket accept read: %w", err)
	}
	key := req.Header.Get("Sec-WebSocket-Key")
	upgrade := strings.EqualFold(req.Header.Get("Upgrade"), "websocket")
	if req.Method != http.MethodGet || !upgrade || key == "" || (path != "" && req.URL.Path != path) {
		_, _ = io.WriteString(raw, "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n")
		_ = raw.Close()
		return nil, fmt.Errorf("multipath: not a websocket upgrade for %q", path)
	}
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + wsAccept(key) + "\r\n\r\n"
	if _, err := io.WriteString(raw, response); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return newWSConn(raw, br, false), nil
}

// wsAccept computes the Sec-WebSocket-Accept token for a key.
func wsAccept(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// wsConn is a byte-stream net.Conn over a WebSocket: each Write is one binary frame, Read spans
// frames transparently, and control frames are absorbed. A client-side conn masks its writes, as
// RFC 6455 requires; a server-side conn does not.
type wsConn struct {
	raw    net.Conn
	br     *bufio.Reader
	client bool

	readMu    sync.Mutex
	remaining int64  // payload bytes left in the frame currently being read
	pending   []byte // decoded bytes not yet handed to Read

	writeMu sync.Mutex
	closed  bool
}

func newWSConn(raw net.Conn, br *bufio.Reader, client bool) *wsConn {
	return &wsConn{raw: raw, br: br, client: client}
}

func (c *wsConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for len(c.pending) == 0 {
		if err := c.readFrame(); err != nil {
			return 0, err
		}
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

// readFrame reads one data frame's payload into c.pending, answering control frames along the way.
func (c *wsConn) readFrame() error {
	var header [2]byte
	if _, err := io.ReadFull(c.br, header[:]); err != nil {
		return err
	}
	opcode := header[0] & 0x0f
	masked := header[1]&0x80 != 0
	length := int64(header[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return err
		}
		length = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return err
		}
		length = int64(binary.BigEndian.Uint64(ext[:]))
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.br, mask[:]); err != nil {
			return err
		}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}

	switch opcode {
	case wsOpBinary, wsOpContinuation:
		c.pending = append(c.pending, payload...)
		return nil
	case wsOpPing:
		return c.writeFrame(wsOpPong, payload)
	case wsOpPong:
		return nil
	case wsOpClose:
		_ = c.writeFrame(wsOpClose, nil)
		return io.EOF
	default:
		return fmt.Errorf("multipath: websocket unexpected opcode %d", opcode)
	}
}

func (c *wsConn) Write(p []byte) (int, error) {
	if err := c.writeFrame(wsOpBinary, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// writeFrame emits one frame, masking the payload when this end is the client.
func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed && opcode != wsOpClose {
		return net.ErrClosed
	}

	var head []byte
	head = append(head, 0x80|opcode) // FIN set; a Write is one self-contained message
	length := len(payload)
	maskBit := byte(0)
	if c.client {
		maskBit = 0x80
	}
	switch {
	case length < 126:
		head = append(head, maskBit|byte(length))
	case length < 1<<16:
		head = append(head, maskBit|126)
		head = binary.BigEndian.AppendUint16(head, uint16(length))
	default:
		head = append(head, maskBit|127)
		head = binary.BigEndian.AppendUint64(head, uint64(length))
	}

	if c.client {
		var mask [4]byte
		if _, err := rand.Read(mask[:]); err != nil {
			return err
		}
		head = append(head, mask[:]...)
		masked := make([]byte, length)
		for i := range payload {
			masked[i] = payload[i] ^ mask[i%4]
		}
		if _, err := c.raw.Write(append(head, masked...)); err != nil {
			return err
		}
		return nil
	}
	if _, err := c.raw.Write(append(head, payload...)); err != nil {
		return err
	}
	return nil
}

func (c *wsConn) Close() error {
	c.writeMu.Lock()
	already := c.closed
	c.closed = true
	c.writeMu.Unlock()
	if !already {
		_ = c.writeFrame(wsOpClose, nil)
	}
	return c.raw.Close()
}

func (c *wsConn) LocalAddr() net.Addr                { return c.raw.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr               { return c.raw.RemoteAddr() }
func (c *wsConn) SetDeadline(t time.Time) error      { return c.raw.SetDeadline(t) }
func (c *wsConn) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }
