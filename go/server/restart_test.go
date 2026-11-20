// Tests for a server going away under live sessions: a restarted server does not know the session,
// so a reconnecting link is refused with "unknown session" (and the client starts a new session)
// instead of being silently taken for a new session at offset 0; and a server shutting down says
// goodbye to its sessions, so their clients move on at once.
package server

import (
	"context"
	"testing"
	"time"

	"github.com/micro-teams/multipath/go/link"
	"github.com/micro-teams/multipath/go/protocol"
)

func dialRaw(url string) (*link.Conn, error) {
	return link.Dial(context.Background(), url, link.Options{})
}

func TestRestartedServerRejectsTheOldSession(t *testing.T) {
	_, _, first := newServer(t, "127.0.0.1:1", nil)
	key := newKey()
	c, typ, _ := handshake(t, first, protocol.HelloFrame{Session: key})
	defer c.Close()
	if typ != protocol.Welcome {
		t.Fatal("first server did not welcome the session")
	}
	// A second server process stands for the first one after a restart: it never saw the session.
	_, _, restarted := newServer(t, "127.0.0.1:1", nil)
	expectReject(t, restarted, protocol.HelloFrame{Session: key, Mode: protocol.ModeJoin}, protocol.RejectUnknownSession)
}

func TestShutdownSaysGoodbye(t *testing.T) {
	srv, _, url := newServer(t, "127.0.0.1:1", nil)
	_, s := clientMux(t, url)
	// The client has its WELCOME a moment before the server has attached the link.
	waitFor(t, "the link to attach", func() bool { return srv.Status().Links == 1 })
	srv.Shutdown()
	select {
	case <-s.Done():
		if r := s.Reason(); r != "the peer said goodbye: server shutting down" {
			t.Fatalf("client session ended with %q", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the client did not hear goodbye")
	}
	expectReject(t, url, protocol.HelloFrame{Session: newKey()}, protocol.RejectLimit)
}
