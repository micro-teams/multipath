package multipath

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestAppWebSocketRoundTrip proves the client handshake and message framing interoperate with the
// link layer's server-side handshake/byte-stream (acceptWebSocket) — the same wire format, read
// through the new message-preserving ReadMessage/WriteMessage instead of a plain byte stream. Real
// TCP loopback, not net.Pipe: net.Pipe's fully synchronous, unbuffered rendezvous does not tolerate
// a bufio.Reader prefetching ahead of a peer's granular Read/Write calls the way a real socket does.
func TestAppWebSocketRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		server, err := acceptWebSocket(c, "/x")
		if err != nil {
			return
		}
		defer server.Close()
		buf := make([]byte, 4096)
		for {
			n, err := server.Read(buf)
			if err != nil {
				return
			}
			if _, err := server.Write(buf[:n]); err != nil {
				return
			}
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ws, err := DialWebSocket(conn, "/x", nil)
	if err != nil {
		t.Fatalf("DialWebSocket: %v", err)
	}

	// (No zero-length message here: the link-layer's byte-stream wsConn used as this test's echo
	// server treats "pending still empty after a frame" as "need another frame", so a genuinely
	// empty binary frame makes ITS Read loop forever — a pre-existing quirk of that byte-stream
	// abstraction, irrelevant to real links (which never intentionally write zero bytes) but not
	// this test's to fix. WSConn.ReadMessage itself has no such issue — see TestAppWebSocketEmptyMessage.)
	for i, msg := range [][]byte{[]byte("hello"), randBytes(200)} {
		if err := ws.WriteMessage(WSBinary, msg); err != nil {
			t.Fatalf("WriteMessage %d: %v", i, err)
		}
		typ, got, err := ws.ReadMessage()
		if err != nil {
			t.Fatalf("ReadMessage %d: %v", i, err)
		}
		if typ != WSBinary {
			t.Fatalf("message %d: type = %v, want WSBinary", i, typ)
		}
		if string(got) != string(msg) {
			t.Fatalf("message %d: got %q want %q", i, got, msg)
		}
	}
}

// TestAppWebSocketFragmentedMessage constructs a message split across two continuation frames by
// hand and checks ReadMessage reassembles it whole, only returning once FIN arrives.
func TestAppWebSocketFragmentedMessage(t *testing.T) {
	r, w := io.Pipe()
	ws := &WSConn{rw: discardWriter{w}, br: bufio.NewReader(r)}
	go func() {
		// First fragment: opcode=binary, FIN=0.
		_, _ = w.Write([]byte{0x02, 0x03, 'a', 'b', 'c'})
		// Final fragment: opcode=continuation, FIN=1.
		_, _ = w.Write([]byte{0x80, 0x02, 'd', 'e'})
	}()
	typ, got, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if typ != WSBinary || string(got) != "abcde" {
		t.Fatalf("got type=%v data=%q, want WSBinary \"abcde\"", typ, got)
	}
}

// TestAppWebSocketEmptyMessage checks a genuinely empty (zero-length payload) message round-trips
// as one complete message, not as "no message yet" — unlike the link-layer's byte-stream wsConn
// (see the note in TestAppWebSocketRoundTrip), ReadMessage delimits by frame boundary/FIN, not by
// "do I have any bytes", so an empty frame is a valid, immediately-returned message.
func TestAppWebSocketEmptyMessage(t *testing.T) {
	r, w := io.Pipe()
	ws := &WSConn{rw: discardWriter{w}, br: bufio.NewReader(r)}
	go func() {
		_, _ = w.Write([]byte{0x82, 0x00}) // FIN|binary, length 0, no payload
	}()
	typ, got, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if typ != WSBinary || len(got) != 0 {
		t.Fatalf("got type=%v data=%q (len %d), want WSBinary empty", typ, got, len(got))
	}
}

// TestAppWebSocketPingAnsweredAndSkipped feeds a ping frame ahead of a data frame over real TCP and
// checks the ping is answered with a pong on the wire and never surfaces as a message. The server
// side is hand-rolled (not acceptWebSocket) because it needs to emit a raw ping frame, which the
// link layer's byte-stream wsConn has no way to ask for — it only ever writes OP_BINARY.
func TestAppWebSocketPingAnsweredAndSkipped(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	pongPayload := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		br := bufio.NewReader(c)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		key := req.Header.Get("Sec-WebSocket-Key")
		_, _ = io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\n"+
			"Upgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Accept: "+wsAccept(key)+"\r\n\r\n")

		_, _ = c.Write([]byte{0x89, 0x04, 'p', 'i', 'n', 'g'}) // unmasked ping, as a server must send
		_, _ = c.Write([]byte{0x82, 0x02, 'h', 'i'})           // unmasked binary "hi"

		// Read back the client's PONG — a masked frame, since this end is the client.
		var header [2]byte
		if _, err := io.ReadFull(br, header[:]); err != nil {
			return
		}
		length := header[1] & 0x7f // small payload only; this test never sends more than 4 bytes
		var mask [4]byte
		if _, err := io.ReadFull(br, mask[:]); err != nil {
			return
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(br, payload); err != nil {
			return
		}
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
		if header[0] != 0x8A { // FIN|PONG
			return
		}
		pongPayload <- string(payload)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ws, err := DialWebSocket(conn, "/x", nil)
	if err != nil {
		t.Fatalf("DialWebSocket: %v", err)
	}

	typ, got, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if typ != WSBinary || string(got) != "hi" {
		t.Fatalf("got type=%v data=%q, want WSBinary \"hi\" (ping must not surface as a message)", typ, got)
	}
	select {
	case p := <-pongPayload:
		if p != "ping" {
			t.Fatalf("pong payload = %q, want the ping's own payload %q", p, "ping")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the pong to be observed")
	}
}

// discardWriter satisfies io.ReadWriteCloser for tests that drive WSConn.br directly and never
// need WSConn.rw's Read (WriteMessage/writeFrame only ever call Write).
type discardWriter struct{ w io.Writer }

func (discardWriter) Read(p []byte) (int, error)    { return 0, io.EOF }
func (d discardWriter) Write(p []byte) (int, error) { return d.w.Write(p) }
func (discardWriter) Close() error                  { return nil }
