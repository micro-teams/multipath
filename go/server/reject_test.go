// Tests for the handshake table of PROTOCOL.md section 4.2: a new session is created and later
// links join it, and every HELLO the server cannot accept is answered with REJECT and the code that
// says why, never with a silent close.
package server

import (
	"testing"

	"github.com/micro-teams/multipath/go/protocol"
)

func expectReject(t *testing.T, url string, h protocol.HelloFrame, code byte) {
	t.Helper()
	c, typ, p := handshake(t, url, h)
	defer c.Close()
	if typ != protocol.Reject {
		t.Fatalf("answer 0x%02x, want REJECT", typ)
	}
	r, err := protocol.DecodeReject(p)
	if err != nil || r.Code != code {
		t.Fatalf("reject %+v %v, want code %d", r, err, code)
	}
	t.Logf("rejected: %s", r.Reason)
}

func TestNewThenJoin(t *testing.T) {
	srv, _, url := newServer(t, "127.0.0.1:1", nil)
	key := newKey()
	for i, want := range []bool{true, false, false} {
		mode := protocol.ModeNew
		if i == 2 {
			mode = protocol.ModeJoin
		}
		c, typ, p := handshake(t, url, protocol.HelloFrame{Session: key, Link: uint8(i), Mode: mode})
		defer c.Close()
		w, err := protocol.DecodeWelcome(p)
		if typ != protocol.Welcome || err != nil || w.Created != want {
			t.Fatalf("link %d: answer 0x%02x %+v %v", i, typ, w, err)
		}
	}
	waitFor(t, "three links on one session", func() bool { st := srv.Status(); return st.Sessions == 1 && st.Links == 3 })
}

func TestJoinOfAnUnknownSessionIsRejected(t *testing.T) {
	srv, _, url := newServer(t, "127.0.0.1:1", nil)
	expectReject(t, url, protocol.HelloFrame{Session: newKey(), Mode: protocol.ModeJoin}, protocol.RejectUnknownSession)
	if n := srv.Status().Sessions; n != 0 {
		t.Fatalf("%d sessions after a rejected join", n)
	}
}

func TestLinkIndexOutOfRangeIsRejected(t *testing.T) {
	_, _, url := newServer(t, "127.0.0.1:1", nil)
	expectReject(t, url, protocol.HelloFrame{Session: newKey(), Link: uint8(protocol.DefaultLimits().MaxLinks)}, protocol.RejectLimit)
}

func TestOtherVersionIsRejected(t *testing.T) {
	_, _, url := newServer(t, "127.0.0.1:1", nil)
	expectReject(t, url, protocol.HelloFrame{Version: 2, Session: newKey()}, protocol.RejectVersion)
}

func TestMalformedHelloIsRejected(t *testing.T) {
	_, _, url := newServer(t, "127.0.0.1:1", nil)
	c, typ, _ := handshake(t, url, protocol.HelloFrame{Session: newKey()})
	_ = c.Close() // only a fresh link matters below
	if typ != protocol.Welcome {
		t.Fatal("a valid hello was not welcomed")
	}
	raw, err := dialRaw(url)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := raw.WriteMessage(protocol.EncodePing(1)); err != nil {
		t.Fatal(err)
	}
	msg, err := raw.ReadMessage(1 << 16)
	if err != nil || msg[0] != protocol.Reject || msg[protocol.HeaderLen] != protocol.RejectMalformed {
		t.Fatalf("answer % x, %v", msg, err)
	}
}
