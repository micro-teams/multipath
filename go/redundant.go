// Redundant multi-link byte stream: aggregate several unreliable links into ONE reliable, ordered,
// never-interrupted duplex byte stream.
//
// This is a different primitive from the rest of the package. The registry/health/ranking layer and
// the request strategies (hedged read, write failover) operate at HTTP-request granularity and can
// see inside the payload. This layer operates on OPAQUE bytes — the motivating payload is a TLS
// CONNECT tunnel, which request-level hedging cannot look into — so redundancy has to live at the
// byte/stream level instead.
//
// The model is "redundant, not striped": every byte is written to ALL live links; the receiver
// delivers each byte exactly once, in order, taking whichever link delivered it first. The cost is
// N× bandwidth; the benefit is that as long as ONE link is healthy the logical stream never stalls
// and never breaks. A link that dies reconnects and resumes in the background, invisibly to the
// caller — because DATA frames carry absolute stream offsets, a reconnecting link that replays
// already-delivered bytes is simply de-duplicated by offset.
//
// The public surface is deliberately a plain io.ReadWriteCloser: the caller Reads and Writes and
// never learns that more than one link exists. All fan-out, de-duplication, reassembly, keepalive
// and reconnect are hidden behind it. That is the whole point — a consumer swaps one Dial call and
// nothing else in its code changes.
//
// Wire framing (all integers big-endian):
//
//	DATA  0x01 | offset:u64 | len:u16 | crc32:u32 | payload[len]
//	ACK   0x02 | cumulative:u64
//	PING  0x03 | nonce:u64
//	PONG  0x04 | nonce:u64
//
// The CRC guards against a link (or a bug) silently corrupting the logical stream even if that link
// is not itself TLS-protected; a bad frame is dropped and recovered from another link or a replay,
// so the delivered stream stays byte-exact. There is no HELLO/resume frame: correctness of resume
// falls out of absolute offsets + cumulative ACK (a reconnecting link just replays the unacked
// buffer and the receiver de-duplicates), which keeps the protocol to four frame types.

package multipath

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// errCorruptFrame is raised by the frame reader on a CRC/length mismatch or an unknown frame tag; the
// caller drops that link and the bytes are recovered from another link or a replay.
var errCorruptFrame = errors.New("multipath: corrupt frame")

// Frame type tags.
const (
	frameData   = 0x01
	frameAck    = 0x02
	framePing   = 0x03
	framePong   = 0x04
	frameHello  = 0x05
	frameReject = 0x06 // server→client: this link is refused, with a human reason; do not retry it
)

const (
	// maxSegment is the largest DATA payload per frame (fits the u16 length).
	maxSegment = 32 * 1024
	dataHdrLen = 1 + 8 + 2 + 4 // type + offset + len + crc
)

// LinkState is reported to RedundantOptions.OnLinkState the moment a link's up/down state changes —
// the "edge" a log wants. Up is true when the link (re)connected, false when it dropped. Reason is
// the cause on a drop ("" on an up). Duration is how long the link spent in the state it just left
// (uptime on a drop; downtime/connecting-time on an up).
type LinkState struct {
	Index    int
	Up       bool
	Reason   string
	Duration time.Duration
}

// LinkStat is a point-in-time snapshot of one link, returned by RedundantStream.Stats — the "level" a
// status view wants. State is "up", "connecting" (never yet up, still dialling) or "down" (was up,
// now broken and reconnecting). LastByte is when a frame last arrived on this link (zero if never).
// Reconnects counts how many times it recovered after a drop. Reason is the last drop/dial cause.
type LinkStat struct {
	Index      int
	State      string
	LastByte   time.Time
	Reconnects int
	Reason     string
}

// RedundantOptions configures a RedundantStream. Zero values take the defaults.
type RedundantOptions struct {
	// OnLinkState, if set, is called once per link up/down transition (never on the hot path, and
	// never while holding an internal lock — the callback may do anything, including log or block
	// briefly). It is the edge-triggered channel: a repeatedly-failing reconnect does not re-fire it,
	// but the latest reason is visible via Stats. Optional.
	OnLinkState func(LinkState)

	// N is the number of underlying links.
	N int
	// Dial opens link i (0..N-1). It is called once per link at start and again on every reconnect,
	// so it must be able to re-establish the same logical path. The returned conn is owned by the
	// stream and closed by it. Bring your own transport (TCP, TLS, WebSocket wrapped as a conn).
	Dial func(ctx context.Context, i int) (io.ReadWriteCloser, error)

	// Window is the maximum number of unacknowledged bytes held in the send buffer. Writes block
	// once this is reached and unblock as ACKs arrive — this is the flow-control point. Default 4MiB.
	Window int
	// PingInterval is how often a PING is sent on each link to prove it still carries bytes. Default 5s.
	PingInterval time.Duration
	// DeadAfter marks a link dead if nothing (PONG or any frame) has arrived on it for this long,
	// then closes and reconnects it. This is what catches the "connected but no data" black-hole: a
	// link whose TCP stays ESTABLISHED while a middlebox silently drops bytes looks alive to the OS
	// but produces no frames, so DeadAfter reaps it. Default 15s. Must exceed PingInterval.
	DeadAfter time.Duration
	// AckInterval bounds how often a cumulative ACK is flushed (an ACK is also sent immediately when
	// delivery advances). Default 50ms.
	AckInterval time.Duration
	// ReconnectDelay is the first reconnect backoff; it doubles per consecutive failure to MaxDelay.
	ReconnectDelay time.Duration
	MaxDelay       time.Duration
	// ConnID identifies this logical stream to the server across all its links and reconnects. If
	// left zero, DialRedundant generates a random one. Only the client (DialRedundant) uses it.
	ConnID [16]byte
}

func (o *RedundantOptions) withDefaults() {
	if o.Window <= 0 {
		o.Window = 4 << 20
	}
	if o.PingInterval <= 0 {
		o.PingInterval = 5 * time.Second
	}
	if o.DeadAfter <= 0 {
		o.DeadAfter = 15 * time.Second
	}
	if o.AckInterval <= 0 {
		o.AckInterval = 50 * time.Millisecond
	}
	if o.ReconnectDelay <= 0 {
		o.ReconnectDelay = 200 * time.Millisecond
	}
	if o.MaxDelay <= 0 {
		o.MaxDelay = 5 * time.Second
	}
}

// ErrStreamClosed is returned by Read/Write after the stream is closed or all links are permanently
// gone.
var ErrStreamClosed = errors.New("multipath: redundant stream closed")

// RedundantStream is a reliable, ordered, duplex byte stream carried redundantly over N links.
// It implements io.ReadWriteCloser.
type RedundantStream struct {
	opt    RedundantOptions
	ctx    context.Context
	cancel context.CancelFunc

	client bool     // true if this end dials + reconnects; false if it accepts links (server)
	connID [16]byte // identifies this logical stream across its links (set by the client)

	mu       sync.Mutex
	readable *sync.Cond // signalled when delivered bytes are available or the stream closes
	writable *sync.Cond // signalled when the send window frees or the stream closes

	// Send side.
	sendBase uint64           // offset of the first byte still in buf (== receiver's cumulative ack)
	sendNext uint64           // offset to assign to the next byte written
	sendBuf  []byte           // unacknowledged bytes [sendBase, sendNext)
	links    []*redundantLink // per-link writer handle; entry may be nil while reconnecting

	// Receive side.
	deliverNext uint64            // next offset to hand to the reader
	reasm       map[uint64][]byte // buffered out-of-order segments, keyed by their start offset
	inbox       []byte            // delivered, not-yet-Read bytes

	lastSeen []time.Time // per-link time of last inbound frame; drives the keepalive reaper

	// Per-link observability (see LinkState/LinkStat). state ∈ {"up","connecting","down"}.
	linkState    []string
	linkSince    []time.Time // when the current state was entered (for Duration)
	linkReconn   []int       // count of recoveries after a drop
	linkReason   []string    // last drop/dial cause
	reapReason   []string    // pending "why" set by the reaper before it closes a silent link
	linkRejected []bool      // link was refused by the origin (REJECT) — never retry it

	closed   bool
	closeErr error
}

// initObserve sets the per-link observability slices to their starting state ("connecting"). Called
// from both stream constructors.
func (s *RedundantStream) initObserve(now time.Time) {
	n := s.opt.N
	s.linkState = make([]string, n)
	s.linkSince = make([]time.Time, n)
	s.linkReconn = make([]int, n)
	s.linkReason = make([]string, n)
	s.reapReason = make([]string, n)
	s.linkRejected = make([]bool, n)
	for i := 0; i < n; i++ {
		s.linkState[i] = "connecting"
		s.linkSince[i] = now
	}
}

// markUp records link i as connected and fires OnLinkState if that is a real transition. Must NOT be
// called while holding s.mu.
func (s *RedundantStream) markUp(i int) {
	s.mu.Lock()
	if i < 0 || i >= len(s.linkState) || s.linkState[i] == "up" {
		s.mu.Unlock()
		return
	}
	was := s.linkState[i]
	dur := time.Since(s.linkSince[i])
	s.linkState[i] = "up"
	s.linkSince[i] = time.Now()
	if was == "down" {
		s.linkReconn[i]++
	}
	cb := s.opt.OnLinkState
	s.mu.Unlock()
	if cb != nil {
		cb(LinkState{Index: i, Up: true, Duration: dur})
	}
}

// markDown records link i as down with a reason. It always updates the last reason, but fires
// OnLinkState only on a real up→down (or connecting→down) transition, so repeated failed reconnects
// don't spam. Must NOT be called while holding s.mu.
func (s *RedundantStream) markDown(i int, reason string) {
	s.mu.Lock()
	if i < 0 || i >= len(s.linkState) {
		s.mu.Unlock()
		return
	}
	s.linkReason[i] = reason
	if s.linkState[i] == "down" {
		s.mu.Unlock()
		return
	}
	dur := time.Since(s.linkSince[i])
	s.linkState[i] = "down"
	s.linkSince[i] = time.Now()
	cb := s.opt.OnLinkState
	s.mu.Unlock()
	if cb != nil {
		cb(LinkState{Index: i, Up: false, Reason: reason, Duration: dur})
	}
}

// Stats returns a snapshot of every link's current health. It never blocks on the network and is safe
// to call at any time — a status view or health check reads it; a log reacts to OnLinkState instead.
func (s *RedundantStream) Stats() []LinkStat {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LinkStat, s.opt.N)
	for i := 0; i < s.opt.N; i++ {
		out[i] = LinkStat{
			Index:      i,
			State:      s.linkState[i],
			LastByte:   s.lastSeen[i],
			Reconnects: s.linkReconn[i],
			Reason:     s.linkReason[i],
		}
	}
	return out
}

// redundantLink is the writer half of one link. Reads happen in a per-link goroutine.
type redundantLink struct {
	conn io.ReadWriteCloser
	wmu  sync.Mutex // serialises frame writes on this link
}

func (l *redundantLink) write(frame []byte) error {
	l.wmu.Lock()
	defer l.wmu.Unlock()
	_, err := l.conn.Write(frame)
	return err
}

// DialRedundant establishes a redundant stream by dialling all N links. It returns once at least one
// link is up (the rest continue connecting in the background); it fails only if every link's first
// dial fails.
func DialRedundant(ctx context.Context, opt RedundantOptions) (*RedundantStream, error) {
	opt.withDefaults()
	if opt.N <= 0 || opt.Dial == nil {
		return nil, errors.New("multipath: RedundantOptions needs N>0 and Dial")
	}
	if opt.ConnID == ([16]byte{}) {
		_, _ = rand.Read(opt.ConnID[:])
	}
	sctx, cancel := context.WithCancel(context.Background())
	s := &RedundantStream{
		opt:      opt,
		ctx:      sctx,
		cancel:   cancel,
		client:   true,
		connID:   opt.ConnID,
		links:    make([]*redundantLink, opt.N),
		reasm:    make(map[uint64][]byte),
		lastSeen: make([]time.Time, opt.N),
	}
	now := time.Now()
	for i := range s.lastSeen {
		s.lastSeen[i] = now
	}
	s.initObserve(now)
	s.readable = sync.NewCond(&s.mu)
	s.writable = sync.NewCond(&s.mu)

	// Dial all links. Block only until the first success so the caller gets a usable stream promptly.
	var wg sync.WaitGroup
	up := make(chan struct{}, opt.N)
	for i := 0; i < opt.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if s.connectLink(ctx, i, true) {
				select {
				case up <- struct{}{}:
				default:
				}
			}
		}(i)
	}
	go func() { wg.Wait(); close(up) }()

	select {
	case _, ok := <-up:
		if !ok {
			// Every link failed its first dial.
			cancel()
			return nil, errors.New("multipath: all links failed to connect")
		}
	case <-ctx.Done():
		cancel()
		return nil, ctx.Err()
	}
	go s.keepaliveLoop()
	go s.ackLoop()
	return s, nil
}

// newServerStream builds the accept-side end of a redundant stream (it never dials; links are
// attached by the acceptor as they arrive) and starts its keepalive and ACK loops.
func newServerStream(opt RedundantOptions, connID [16]byte) *RedundantStream {
	opt.withDefaults()
	sctx, cancel := context.WithCancel(context.Background())
	s := &RedundantStream{
		opt:      opt,
		ctx:      sctx,
		cancel:   cancel,
		client:   false,
		connID:   connID,
		links:    make([]*redundantLink, opt.N),
		reasm:    make(map[uint64][]byte),
		lastSeen: make([]time.Time, opt.N),
	}
	now := time.Now()
	for i := range s.lastSeen {
		s.lastSeen[i] = now
	}
	s.initObserve(now)
	s.readable = sync.NewCond(&s.mu)
	s.writable = sync.NewCond(&s.mu)
	go s.keepaliveLoop()
	go s.ackLoop()
	return s
}

// Acceptor accepts raw links off a net.Listener and groups them by connID into server-side
// RedundantStreams. A new connID yields a new stream delivered on Accept; a reconnecting link of a
// known connID re-attaches to the existing stream.
type Acceptor struct {
	ln      net.Listener
	opt     RedundantOptions
	handOff chan *RedundantStream

	mu      sync.Mutex
	streams map[[16]byte]*RedundantStream
}

// Listen starts accepting redundant links on ln. Each logical connection is surfaced once via
// Accept; its N links attach underneath it as they arrive and reconnect.
func Listen(ln net.Listener, opt RedundantOptions) *Acceptor {
	opt.withDefaults()
	a := &Acceptor{
		ln:      ln,
		opt:     opt,
		handOff: make(chan *RedundantStream, 16),
		streams: make(map[[16]byte]*RedundantStream),
	}
	go a.loop()
	return a
}

func (a *Acceptor) loop() {
	for {
		c, err := a.ln.Accept()
		if err != nil {
			return
		}
		go a.onLink(c)
	}
}

// onLink reads the HELLO off a freshly accepted link and attaches it to the right logical stream.
func (a *Acceptor) onLink(c net.Conn) {
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	r := &frameReader{conn: c}
	f, err := r.next()
	if err != nil || f.typ != frameHello {
		_ = c.Close()
		return
	}
	_ = c.SetReadDeadline(time.Time{})
	idx := int(f.linkIdx)
	if idx < 0 || idx >= a.opt.N {
		// Tell the client why before closing, and log it — a silent close here is indistinguishable
		// from a flaky network and sends the client into an endless reconnect loop.
		reason := fmt.Sprintf("link index %d out of range (n=%d)", idx, a.opt.N)
		_, _ = c.Write(encodeReject(reason))
		log.Printf("multipath: rejecting link from %v: %s", c.RemoteAddr(), reason)
		_ = c.Close()
		return
	}
	a.mu.Lock()
	s, known := a.streams[f.connID]
	if !known {
		s = newServerStream(a.opt, f.connID)
		a.streams[f.connID] = s
	}
	a.mu.Unlock()
	if !known {
		select {
		case a.handOff <- s:
		default:
			// Nobody is accepting; drop the whole logical stream rather than leak it.
			a.mu.Lock()
			delete(a.streams, f.connID)
			a.mu.Unlock()
			s.Close()
			_ = c.Close()
			return
		}
	}
	if !s.attachLink(idx, c) {
		// Stream was closed; forget it so a later connID re-use starts fresh.
		a.mu.Lock()
		delete(a.streams, f.connID)
		a.mu.Unlock()
	}
}

// Accept returns the next new logical stream. It blocks until one arrives or the acceptor closes.
func (a *Acceptor) Accept() (*RedundantStream, error) {
	s, ok := <-a.handOff
	if !ok {
		return nil, ErrStreamClosed
	}
	return s, nil
}

// Close stops accepting new links. In-flight streams are unaffected.
func (a *Acceptor) Close() error { return a.ln.Close() }

// connectLink dials link i and, on success, sends HELLO then installs it. firstTry controls whether
// a single failed dial returns immediately (startup) or keeps retrying (reconnect). Client-only.
func (s *RedundantStream) connectLink(dialCtx context.Context, i int, firstTry bool) bool {
	delay := s.opt.ReconnectDelay
	for {
		if s.isClosed() {
			return false
		}
		conn, err := s.opt.Dial(dialCtx, i)
		if err == nil {
			// HELLO first so the server can group this link (and a reconnect re-attaches).
			if _, werr := conn.Write(encodeHello(s.connID, uint16(i))); werr != nil {
				_ = conn.Close()
				s.markDown(i, "hello write: "+werr.Error())
			} else if s.attachLink(i, conn) {
				return true
			}
			// attach failed because the stream is closing.
			if s.isClosed() {
				return false
			}
		} else {
			s.markDown(i, "dial: "+err.Error())
		}
		if firstTry {
			return false
		}
		select {
		case <-time.After(delay):
		case <-s.ctx.Done():
			return false
		}
		if delay < s.opt.MaxDelay {
			delay *= 2
			if delay > s.opt.MaxDelay {
				delay = s.opt.MaxDelay
			}
		}
	}
}

// attachLink installs conn as link i, replays the unacknowledged send buffer onto it (so a fresh or
// reconnected link resumes without a gap — the peer de-duplicates by offset), and starts its reader.
// Used by the client after HELLO and by the server after reading a HELLO. Returns false if the
// stream is already closed.
func (s *RedundantStream) attachLink(i int, conn io.ReadWriteCloser) bool {
	l := &redundantLink{conn: conn}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close()
		return false
	}
	if old := s.links[i]; old != nil {
		_ = old.conn.Close() // a stale link in this slot is superseded
	}
	s.links[i] = l
	s.lastSeen[i] = time.Now()
	replay := s.snapshotUnackedLocked()
	s.mu.Unlock()
	for _, f := range replay {
		if l.write(f) != nil {
			break
		}
	}
	go s.readLoop(i, l)
	s.markUp(i)
	return true
}

// snapshotUnackedLocked returns DATA frames covering the whole current send buffer, for replay onto
// a freshly (re)connected link. Caller holds mu.
func (s *RedundantStream) snapshotUnackedLocked() [][]byte {
	var frames [][]byte
	for off := 0; off < len(s.sendBuf); off += maxSegment {
		end := off + maxSegment
		if end > len(s.sendBuf) {
			end = len(s.sendBuf)
		}
		seg := s.sendBuf[off:end]
		frames = append(frames, encodeData(s.sendBase+uint64(off), seg))
	}
	return frames
}

// readLoop parses frames off one link until it errors or the link is reaped. On the client a dropped
// link is reconnected here; on the server it is simply dropped and the slot waits for the client to
// reconnect it (the listener re-attaches the new link).
func (s *RedundantStream) readLoop(i int, l *redundantLink) {
	r := &frameReader{conn: l.conn}
	var readErr error
	var rejected bool
	var rejectReason string
	for {
		f, err := r.next()
		if err != nil {
			readErr = err
			break
		}
		switch f.typ {
		case frameData:
			s.onData(f.offset, f.payload)
		case frameAck:
			s.onAck(f.offset)
		case framePing:
			_ = l.write(encodePong(f.nonce))
		case framePong:
			// liveness only
		case frameHello:
			// A server reads HELLO in the listener before attaching; a stray HELLO here is ignored.
		case frameReject:
			// The origin refused this link and said why. Stop reading; do not retry it.
			rejected = true
			rejectReason = f.reason
		}
		s.touch(i)
		if rejected {
			break
		}
	}
	// Link i died. Drop it; the client reconnects a transient drop, waits for re-accept on the server,
	// but never retries a link the origin explicitly rejected.
	s.mu.Lock()
	if s.links[i] == l {
		s.links[i] = nil
	}
	closing := s.closed
	client := s.client
	reason := s.reapReason[i] // reaper's "why" wins over the resulting read error
	s.reapReason[i] = ""
	if rejected {
		s.linkRejected[i] = true
	}
	allRejected := rejected
	if rejected {
		for _, rj := range s.linkRejected {
			if !rj {
				allRejected = false
				break
			}
		}
	}
	s.mu.Unlock()
	_ = l.conn.Close()
	if rejected {
		reason = "origin rejected: " + rejectReason
	} else if reason == "" {
		if readErr != nil {
			reason = readErr.Error()
		} else {
			reason = "link closed"
		}
	}
	if !closing {
		s.markDown(i, reason)
	}
	if rejected {
		// Every link refused → the stream can never work; close it with the reason so a Dial that
		// already returned success now fails its reads/writes fast instead of hanging forever.
		if allRejected && !closing {
			s.closeWithError(errors.New("multipath: all links rejected by origin: " + rejectReason))
		}
		return
	}
	if !closing && client {
		go s.connectLink(s.ctx, i, false)
	}
}

// closeWithError tears the stream down like Close but records err as the cause, so blocked or
// subsequent Read/Write calls return it instead of the generic closed error.
func (s *RedundantStream) closeWithError(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closeErr = err
	s.mu.Unlock()
	_ = s.Close()
}

// touch records recent liveness on link i (any inbound frame). Used by the keepalive reaper.
func (s *RedundantStream) touch(i int) {
	s.mu.Lock()
	if i >= 0 && i < len(s.lastSeen) {
		s.lastSeen[i] = time.Now()
	}
	s.mu.Unlock()
}

// --- receive side ---

func (s *RedundantStream) onData(off uint64, payload []byte) {
	if len(payload) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	end := off + uint64(len(payload))
	if end <= s.deliverNext {
		return // wholly old — the first link to deliver this range already won; drop the duplicate
	}
	if off < s.deliverNext {
		payload = payload[s.deliverNext-off:] // partial overlap: keep only the new tail
		off = s.deliverNext
	}
	if off > s.deliverNext {
		// Out of order: a faster link jumped ahead. Buffer until the gap fills. Keep the longest
		// segment seen at this offset.
		if prev, ok := s.reasm[off]; !ok || len(payload) > len(prev) {
			s.reasm[off] = append([]byte(nil), payload...)
		}
		return
	}
	// off == deliverNext: deliver, then drain any now-contiguous buffered segments.
	s.inbox = append(s.inbox, payload...)
	s.deliverNext = end
	s.drainReasmLocked()
	s.readable.Broadcast()
}

// drainReasmLocked applies buffered out-of-order segments that have become contiguous. Caller holds mu.
func (s *RedundantStream) drainReasmLocked() {
	for {
		seg, ok := s.reasm[s.deliverNext]
		if ok {
			delete(s.reasm, s.deliverNext)
			s.inbox = append(s.inbox, seg...)
			s.deliverNext += uint64(len(seg))
			continue
		}
		// Also absorb any buffered segment that starts before deliverNext but extends past it.
		advanced := false
		for start, buf := range s.reasm {
			bend := start + uint64(len(buf))
			if start < s.deliverNext && bend > s.deliverNext {
				s.inbox = append(s.inbox, buf[s.deliverNext-start:]...)
				s.deliverNext = bend
				delete(s.reasm, start)
				advanced = true
				break
			}
			if bend <= s.deliverNext {
				delete(s.reasm, start) // fully superseded
			}
		}
		if !advanced {
			return
		}
	}
}

// --- send side ---

func (s *RedundantStream) onAck(cumulative uint64) {
	s.mu.Lock()
	if cumulative > s.sendBase {
		drop := cumulative - s.sendBase
		if drop >= uint64(len(s.sendBuf)) {
			s.sendBuf = s.sendBuf[:0]
		} else {
			s.sendBuf = s.sendBuf[drop:]
		}
		s.sendBase = cumulative
		s.writable.Broadcast()
	}
	s.mu.Unlock()
}

// Write appends bytes to the logical stream, fanning each segment out to all live links. It blocks
// while the send window is full (all links behind on ACKs) — natural backpressure.
func (s *RedundantStream) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		s.mu.Lock()
		for !s.closed && len(s.sendBuf) >= s.opt.Window {
			s.writable.Wait()
		}
		if s.closed {
			err := s.closeErr
			s.mu.Unlock()
			if err == nil {
				err = ErrStreamClosed
			}
			return total, err
		}
		room := s.opt.Window - len(s.sendBuf)
		n := len(p)
		if n > room {
			n = room
		}
		if n > maxSegment {
			n = maxSegment
		}
		seg := p[:n]
		off := s.sendNext
		s.sendBuf = append(s.sendBuf, seg...)
		s.sendNext += uint64(n)
		frame := encodeData(off, seg)
		live := make([]*redundantLink, 0, len(s.links))
		for _, l := range s.links {
			if l != nil {
				live = append(live, l)
			}
		}
		s.mu.Unlock()

		for _, l := range live {
			_ = l.write(frame) // a failed write just means that link is dying; its reader will reap it
		}
		p = p[n:]
		total += n
	}
	return total, nil
}

// Read returns delivered bytes in order. It blocks until at least one byte is available or the
// stream closes.
func (s *RedundantStream) Read(p []byte) (int, error) {
	s.mu.Lock()
	for len(s.inbox) == 0 && !s.closed {
		s.readable.Wait()
	}
	if len(s.inbox) == 0 && s.closed {
		err := s.closeErr
		s.mu.Unlock()
		if err == nil {
			err = io.EOF
		}
		return 0, err
	}
	n := copy(p, s.inbox)
	s.inbox = s.inbox[n:]
	s.mu.Unlock()
	return n, nil
}

// Close tears down the stream and all links.
func (s *RedundantStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.closeErr == nil {
		s.closeErr = ErrStreamClosed
	}
	links := s.links
	s.links = make([]*redundantLink, s.opt.N)
	s.readable.Broadcast()
	s.writable.Broadcast()
	s.mu.Unlock()
	s.cancel()
	for _, l := range links {
		if l != nil {
			_ = l.conn.Close()
		}
	}
	return nil
}

func (s *RedundantStream) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// keepaliveLoop pings each live link and reaps any that has gone silent past DeadAfter.
func (s *RedundantStream) keepaliveLoop() {
	t := time.NewTicker(s.opt.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		s.mu.Lock()
		type reap struct {
			i int
			l *redundantLink
		}
		var toReap []reap
		var toPing []*redundantLink
		nonce := uint64(now.UnixNano())
		for i, l := range s.links {
			if l == nil {
				continue
			}
			if now.Sub(s.lastSeen[i]) > s.opt.DeadAfter {
				toReap = append(toReap, reap{i, l})
				s.links[i] = nil
				s.reapReason[i] = "no data for " + s.opt.DeadAfter.String()
			} else {
				toPing = append(toPing, l)
			}
		}
		s.mu.Unlock()
		for _, l := range toPing {
			_ = l.write(encodePing(nonce))
		}
		for _, r := range toReap {
			_ = r.l.conn.Close() // its readLoop errors out and triggers reconnect
		}
	}
}

// ackLoop periodically flushes a cumulative ACK so a sender can release its buffer even during a
// long one-directional transfer. Immediate ACKs on delivery advance are sent from onData's caller
// path via flushAck; this is the safety-net tick.
func (s *RedundantStream) ackLoop() {
	t := time.NewTicker(s.opt.AckInterval)
	defer t.Stop()
	var lastAcked uint64
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		s.mu.Lock()
		cum := s.deliverNext
		links := make([]*redundantLink, 0, len(s.links))
		for _, l := range s.links {
			if l != nil {
				links = append(links, l)
			}
		}
		s.mu.Unlock()
		if cum == lastAcked && cum != 0 {
			// Still resend occasionally in case a prior ACK was lost on a since-dead link.
		}
		lastAcked = cum
		frame := encodeAck(cum)
		for _, l := range links {
			_ = l.write(frame)
		}
	}
}
