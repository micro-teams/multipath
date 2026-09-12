// Application-level WebSocket over the substrate: a consumer that wants real WebSocket message
// semantics (not just an opaque byte pipe) on top of a stream to a registered service. This is a
// different thing from websocket.go's wsConn, which carries a LINK — an opaque byte stream where
// message boundaries never mattered. Here they do: a caller sends and receives whole messages
// (text or binary), exactly like any RFC 6455 client, except the bytes travel over the redundant
// mux substrate to whatever real WebSocket server the origin's service handler is fronting.
//
// The handshake and framing are the client half of RFC 6455 — the same rules websocket.go's
// clientWebSocket already follows for a link, generalized to run over any io.ReadWriteCloser (a
// MuxStream has no deadlines or addresses, so this does not require net.Conn) and, unlike the link
// path, preserving message boundaries: continuation frames are reassembled until FIN, and the
// caller gets back one complete message per ReadMessage call.

package multipath

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// WSMessageType distinguishes the two data opcodes RFC 6455 defines; control frames never surface
// to the caller (ping/pong are answered internally, close ends the connection).
type WSMessageType int

const (
	WSText   WSMessageType = 1
	WSBinary WSMessageType = 2
)

// WSConn is an application-level WebSocket: ReadMessage/WriteMessage carry whole messages, not
// arbitrary byte chunks. The client always masks outgoing frames per RFC 6455; the far end is a
// real WebSocket server (or whatever the origin's service handler dials to) and is expected to
// speak standard, unmasked server frames.
type WSConn struct {
	rw io.ReadWriteCloser
	br *bufio.Reader

	writeMu sync.Mutex
	closed  bool
}

// OpenWebSocket opens a stream to the named service (as Open does) and performs the RFC 6455
// client handshake over it at path, carrying ticket for the service handler to authorise the
// stream and header as additional HTTP request headers (e.g. a sub-protocol or bearer token the
// real WebSocket server on the other end expects). If the origin refuses the service the stream's
// rejection reason surfaces here, before any handshake bytes are sent.
func (c *Client) OpenWebSocket(service string, ticket []byte, path string, header http.Header) (*WSConn, error) {
	st, err := c.Open(service, ticket)
	if err != nil {
		return nil, err
	}
	ws, err := DialWebSocket(st, path, header)
	if err != nil {
		_ = st.Reset()
		return nil, err
	}
	return ws, nil
}

// DialWebSocket performs the RFC 6455 client handshake over rw (an already-open transport — a
// mux stream, or any other duplex byte pipe) and returns a message-oriented WSConn. It never sets
// a deadline and never inspects rw beyond Read/Write/Close, so it works equally over a real
// net.Conn or a MuxStream.
func DialWebSocket(rw io.ReadWriteCloser, path string, header http.Header) (*WSConn, error) {
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: multipath\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n"
	for name, vals := range header {
		for _, v := range vals {
			req += name + ": " + v + "\r\n"
		}
	}
	req += "\r\n"
	if _, err := io.WriteString(rw, req); err != nil {
		return nil, err
	}

	br := bufio.NewReader(rw)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		return nil, fmt.Errorf("multipath: websocket handshake read: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("multipath: websocket handshake got %s", resp.Status)
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") ||
		resp.Header.Get("Sec-WebSocket-Accept") != wsAccept(key) {
		return nil, fmt.Errorf("multipath: websocket handshake response did not confirm the upgrade")
	}
	return &WSConn{rw: rw, br: br}, nil
}

// ReadMessage returns the next complete message (fragmented sends reassembled across continuation
// frames), or an error once the connection is closed (locally, by the peer's close frame, or by
// the underlying stream failing). Ping frames are answered with pong and never returned; a close
// frame is acknowledged and surfaces as io.EOF.
func (c *WSConn) ReadMessage() (WSMessageType, []byte, error) {
	var msgType WSMessageType
	var payload []byte
	started := false
	for {
		var header [2]byte
		if _, err := io.ReadFull(c.br, header[:]); err != nil {
			return 0, nil, err
		}
		fin := header[0]&0x80 != 0
		opcode := header[0] & 0x0f
		masked := header[1]&0x80 != 0
		length := int64(header[1] & 0x7f)
		switch length {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return 0, nil, err
			}
			length = int64(binary.BigEndian.Uint16(ext[:]))
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return 0, nil, err
			}
			length = int64(binary.BigEndian.Uint64(ext[:]))
		}
		var mask [4]byte
		if masked { // a compliant server never masks, but unmask anyway if it does
			if _, err := io.ReadFull(c.br, mask[:]); err != nil {
				return 0, nil, err
			}
		}
		frame := make([]byte, length)
		if _, err := io.ReadFull(c.br, frame); err != nil {
			return 0, nil, err
		}
		if masked {
			for i := range frame {
				frame[i] ^= mask[i%4]
			}
		}
		switch opcode {
		case wsOpText, wsOpBinary:
			if started {
				return 0, nil, fmt.Errorf("multipath: websocket: new message opcode mid-fragment")
			}
			started = true
			msgType = WSMessageType(opcode)
			payload = frame
		case wsOpContinuation:
			if !started {
				return 0, nil, fmt.Errorf("multipath: websocket: continuation with no message open")
			}
			payload = append(payload, frame...)
		case wsOpPing:
			if err := c.writeFrame(wsOpPong, frame); err != nil {
				return 0, nil, err
			}
			continue
		case wsOpPong:
			continue
		case wsOpClose:
			_ = c.writeFrame(wsOpClose, nil)
			return 0, nil, io.EOF
		default:
			return 0, nil, fmt.Errorf("multipath: websocket: unexpected opcode %d", opcode)
		}
		if fin {
			return msgType, payload, nil
		}
	}
}

// WriteMessage sends data as one complete message of the given type (never fragmented — the
// substrate has no MTU pressure that would make fragmentation worthwhile).
func (c *WSConn) WriteMessage(t WSMessageType, data []byte) error {
	return c.writeFrame(byte(t), data)
}

func (c *WSConn) writeFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed && opcode != wsOpClose {
		return fmt.Errorf("multipath: websocket closed")
	}
	head := []byte{0x80 | opcode} // FIN set; every write is one complete, unfragmented frame
	length := len(payload)
	switch {
	case length < 126:
		head = append(head, 0x80|byte(length)) // mask bit always set: this end is always the client
	case length < 1<<16:
		head = append(head, 0x80|126)
		head = binary.BigEndian.AppendUint16(head, uint16(length))
	default:
		head = append(head, 0x80|127)
		head = binary.BigEndian.AppendUint64(head, uint64(length))
	}
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	head = append(head, mask[:]...)
	masked := make([]byte, length)
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}
	_, err := c.rw.Write(append(head, masked...))
	return err
}

// Close sends a close frame and closes the underlying stream.
func (c *WSConn) Close() error {
	c.writeMu.Lock()
	already := c.closed
	c.closed = true
	c.writeMu.Unlock()
	if !already {
		_ = c.writeFrame(wsOpClose, nil)
	}
	return c.rw.Close()
}
