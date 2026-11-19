// One client session and its lines (PROTOCOL.md sections 4.2 and 5.5). Every line has a slot: a
// goroutine that dials the line, performs the handshake, attaches the link, waits for it to die and
// dials again, with backoff, for as long as the session lasts and the line is in the list. A
// REJECT for an unknown session, or a second WELCOME that says it created the session, means the
// server lost the session (it restarted): the session ends at once and the client starts a new one.
//
// Nothing here is exported; the session loop creates sessions and the status page reads them.

package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/micro-teams/multipath/go/link"
	"github.com/micro-teams/multipath/go/mux"
	"github.com/micro-teams/multipath/go/protocol"
	"github.com/micro-teams/multipath/go/session"
)

type clientSession struct {
	c       *Client
	key     [16]byte
	id      string
	started time.Time
	s       *session.Session
	live    chan struct{} // closed when the first link is attached; m is set by then
	ctx     context.Context
	cancel  context.CancelFunc

	serverGone atomic.Bool // the server lost the session; reconnect after a random delay

	mu          sync.Mutex
	m           *mux.Mux // created with the first WELCOME
	welcomed    bool     // a WELCOME has arrived; later HELLOs join
	createdSeen bool     // a WELCOME said it created the session
	maxLinks    int
	slots       map[int]*slot
	server      string // the server's impl string
}

type slot struct {
	index  int
	line   protocol.Line
	cancel context.CancelFunc
	down   chan string // the link's reason for going down

	mu         sync.Mutex
	state      string // "connecting", "up" or "down"
	reconnects int
	reason     string
}

func newClientSession(c *Client, lines []protocol.Line) *clientSession {
	cs := &clientSession{c: c, started: time.Now(), live: make(chan struct{}), slots: map[int]*slot{}}
	_, _ = rand.Read(cs.key[:]) // crypto/rand does not fail on supported platforms
	cs.id = hex.EncodeToString(cs.key[:4])
	cs.ctx, cs.cancel = context.WithCancel(context.Background())
	// Until the first WELCOME the window is the smallest a server may announce, so whatever is
	// buffered before then fits any server's reorder span.
	lim := protocol.DefaultLimits()
	lim.Window = protocol.MinWindow
	cs.maxLinks = lim.MaxLinks
	var liveOnce sync.Once
	cs.s = session.New(session.Options{
		Limits:  lim,
		Client:  true,
		Clock:   c.cfg.Clock,
		Deliver: func(p []byte) error { return cs.m.Feed(p) }, // data only flows after the first WELCOME
		OnLink: func(i int, up bool, why string) {
			if up {
				liveOnce.Do(func() { close(cs.live) })
				return
			}
			cs.mu.Lock()
			sl := cs.slots[i]
			cs.mu.Unlock()
			if sl != nil {
				select {
				case sl.down <- why:
				default:
				}
			}
		},
		OnEnd: func(why string) {
			cs.cancel()
			cs.mu.Lock()
			m := cs.m
			cs.mu.Unlock()
			if m != nil {
				m.Close(why)
			}
			cs.c.cfg.Logf("session %s end after %v: %s", cs.id, time.Since(cs.started).Round(time.Millisecond), why)
		},
	})
	c.cfg.Logf("session %s start over %d lines", cs.id, len(lines))
	cs.applyLines(lines)
	return cs
}

// applyLines makes the session's lines match the list: lines that left it are closed, new lines
// take a free link index and are dialled.
func (cs *clientSession) applyLines(lines []protocol.Line) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.ctx.Err() != nil {
		return
	}
	keep := map[protocol.Line]bool{}
	for _, l := range lines {
		keep[l] = true
	}
	for i, sl := range cs.slots {
		if !keep[sl.line] {
			sl.cancel()
			delete(cs.slots, i)
			cs.c.cfg.Logf("session %s line %s left the list", cs.id, sl.line.ID)
			go cs.s.Detach(i, "line "+sl.line.ID+" left the list")
		} else {
			delete(keep, sl.line)
		}
	}
	for _, l := range lines {
		if !keep[l] {
			continue
		}
		index := -1
		for i := 0; i < cs.maxLinks && index < 0; i++ {
			if cs.slots[i] == nil {
				index = i
			}
		}
		if index < 0 {
			cs.c.cfg.Logf("session %s line %s ignored: the server allows %d links", cs.id, l.ID, cs.maxLinks)
			continue
		}
		ctx, cancel := context.WithCancel(cs.ctx)
		sl := &slot{index: index, line: l, cancel: cancel, down: make(chan string, 1), state: "connecting"}
		cs.slots[index] = sl
		go cs.runSlot(ctx, sl)
	}
}

func (sl *slot) set(state, reason string) {
	sl.mu.Lock()
	if state == "up" && sl.state == "down" {
		sl.reconnects++
	}
	sl.state = state
	if reason != "" {
		sl.reason = reason
	}
	sl.mu.Unlock()
}

// errServerLost marks a handshake that shows the server no longer holds the session.
var errServerLost = errors.New("the server lost the session")

// runSlot keeps one line linked for as long as ctx lasts.
func (cs *clientSession) runSlot(ctx context.Context, sl *slot) {
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		conn, err := cs.dial(ctx, sl)
		if err == nil {
			err = cs.s.Attach(sl.index, conn)
			if err != nil {
				_ = conn.Close() // the session ended meanwhile; nothing to report beyond its end
				return
			}
			sl.set("up", "")
			cs.c.cfg.Logf("session %s line %s up", cs.id, sl.line.ID)
			backoff = 200 * time.Millisecond
			select {
			case why := <-sl.down:
				sl.set("down", why)
				cs.c.cfg.Logf("session %s line %s down: %s", cs.id, sl.line.ID, why)
			case <-ctx.Done():
				return
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errServerLost) {
			cs.serverGone.Store(true)
			cs.s.End(err.Error()) // the server does not know the session, so there is no one to say goodbye to
			return
		}
		sl.set("down", err.Error())
		// Jitter keeps many clients, or many lines of one client, from retrying in lockstep.
		wait := backoff/2 + time.Duration(mathrand.Int64N(int64(backoff)))
		cs.c.cfg.Logf("session %s line %s: %v; retrying in %v", cs.id, sl.line.ID, err, wait.Round(time.Millisecond))
		sleep(ctx, wait)
		backoff = min(2*backoff, 5*time.Second)
	}
}

// dial opens a link over the slot's line and performs the handshake.
func (cs *clientSession) dial(ctx context.Context, sl *slot) (*link.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, protocol.HandshakeTimeout)
	defer cancel()
	conn, err := link.Dial(ctx, protocol.LinkURL(sl.line, cs.c.cfg.LinkPath), link.Options{Proxy: cs.c.cfg.Proxy, TLSConfig: cs.c.cfg.TLSConfig})
	if err != nil {
		return nil, err
	}
	w, err := cs.handshake(ctx, conn, sl)
	if err != nil {
		_ = conn.Close() // the handshake error is what the caller reports
		return nil, err
	}
	if err := cs.accept(w); err != nil {
		_ = conn.Close() // the error is what the caller reports
		return nil, err
	}
	return conn, nil
}

func (cs *clientSession) handshake(ctx context.Context, conn *link.Conn, sl *slot) (protocol.WelcomeFrame, error) {
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline) // a socket that refuses a deadline fails the exchange below
	cs.mu.Lock()
	mode := protocol.ModeNew
	if cs.welcomed {
		mode = protocol.ModeJoin
	}
	cs.mu.Unlock()
	hello := protocol.HelloFrame{Version: protocol.Version, Session: cs.key, Link: uint8(sl.index), Mode: mode, Impl: protocol.Implementation}
	if err := conn.WriteMessage(protocol.EncodeHello(hello)); err != nil {
		return protocol.WelcomeFrame{}, err
	}
	msg, err := conn.ReadMessage(65535 + protocol.HeaderLen)
	if err != nil {
		return protocol.WelcomeFrame{}, fmt.Errorf("no answer to hello: %w", err)
	}
	typ, p, err := protocol.ParseFrame(msg, 65535)
	if err != nil {
		return protocol.WelcomeFrame{}, err
	}
	switch typ {
	case protocol.Welcome:
		w, err := protocol.DecodeWelcome(p)
		if err != nil {
			return w, err
		}
		if w.Version != protocol.Version {
			return w, fmt.Errorf("server speaks protocol %d, want %d", w.Version, protocol.Version)
		}
		return w, conn.SetDeadline(time.Time{})
	case protocol.Reject:
		r, err := protocol.DecodeReject(p)
		if err != nil {
			return protocol.WelcomeFrame{}, err
		}
		if r.Code == protocol.RejectUnknownSession {
			return protocol.WelcomeFrame{}, fmt.Errorf("%w: rejected: %s", errServerLost, r.Reason)
		}
		return protocol.WelcomeFrame{}, fmt.Errorf("rejected (code %d): %s", r.Code, r.Reason)
	}
	return protocol.WelcomeFrame{}, fmt.Errorf("answer to hello has type 0x%02x", typ)
}

// accept takes a WELCOME's limits on the first one and checks that the server still holds the
// session on later ones.
func (cs *clientSession) accept(w protocol.WelcomeFrame) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if w.Created && cs.createdSeen {
		return fmt.Errorf("%w: it created the session a second time", errServerLost)
	}
	cs.createdSeen = cs.createdSeen || w.Created
	if cs.welcomed {
		return nil
	}
	if err := cs.s.SetLimits(w.Limits); err != nil {
		return err
	}
	cs.welcomed = true
	// The mux exists from the first WELCOME on, because its credit is the server's stream window;
	// local connections wait for a live session, so no stream is opened earlier.
	cs.m = mux.New(cs.s, true, w.Limits, nil)
	cs.maxLinks = w.Limits.MaxLinks
	cs.server = w.Impl
	for i, sl := range cs.slots {
		if i >= cs.maxLinks {
			sl.cancel()
			delete(cs.slots, i)
			cs.c.cfg.Logf("session %s line %s dropped: the server allows %d links", cs.id, sl.line.ID, cs.maxLinks)
		}
	}
	cs.c.cfg.Logf("session %s accepted by %s, protocol %d", cs.id, w.Impl, w.Version)
	return nil
}
