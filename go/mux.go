// Stream multiplexing over one reliable transport: carry many independent logical connections
// ("streams") over a single io.ReadWriteCloser — in practice a RedundantStream, so a machine holds
// ONE bundle of N redundant links and multiplexes all of its connections over it, instead of paying
// N links per connection.
//
// This is the missing half of the redundant primitive: the redundant stream gives you one
// never-interrupted ordered byte pipe; the mux Session gives you as many independent
// io.ReadWriteCloser streams as you want on top of it. A yamux/smux/HTTP-2-style design: SYN opens a
// stream, DATA carries bytes, FIN half-closes, RST aborts, and a per-stream credit window
// (WINDOW_UPDATE) provides flow control so one stream can neither starve the others nor buffer
// without bound. DATA is chunked so a long write cannot monopolise the shared transport.
//
// Session-level ordering caveat, stated plainly: the streams share one underlying ordered byte
// stream, so a stalled transport blocks all of them together (head-of-line coupling, like HTTP/2
// over TCP). That is the deliberate trade for a bounded link count; it is fine when the payload is
// small and the transport is a redundant stream that rarely stalls.
//
// Wire framing over the transport (all integers big-endian), identical in Go and JVM:
//
//	type:u8 | streamID:u32 | length:u32 | payload[length]
//
//	SYN           0x01  length 0            open streamID
//	DATA          0x02  length n            n bytes for streamID
//	FIN           0x03  length 0            no more data from this side of streamID
//	RST           0x04  length 0            abort streamID immediately
//	WINDOW_UPDATE 0x05  length 4            grant streamID this many more bytes of send credit
//
// Stream IDs: the client (session opener) uses odd IDs, the server uses even, so both may open
// without colliding.

package multipath

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

const (
	muxSyn    = 0x01
	muxData   = 0x02
	muxFin    = 0x03
	muxRst    = 0x04
	muxWindow = 0x05

	muxHdrLen    = 1 + 4 + 4
	muxMaxChunk  = 16 * 1024  // max DATA payload per frame — bounds one stream's grab of the transport
	muxDefWindow = 256 * 1024 // per-stream receive window
)

// ErrSessionClosed is returned once the session (or its transport) is gone.
var ErrSessionClosed = errors.New("multipath: mux session closed")

// ErrStreamReset is returned to the peer of a stream that was RST.
var ErrStreamReset = errors.New("multipath: stream reset")

// Session multiplexes many Streams over one transport.
type Session struct {
	transport io.ReadWriteCloser
	client    bool

	wmu sync.Mutex // serialises frame writes to the transport

	mu       sync.Mutex
	streams  map[uint32]*MuxStream
	nextID   uint32
	accept   chan *MuxStream
	closed   bool
	closeErr error
}

// NewClientSession and NewServerSession wrap a transport (e.g. a *RedundantStream). The two ends must
// agree on who is client — the client opens odd stream IDs, the server even.
func NewClientSession(transport io.ReadWriteCloser) *Session { return newSession(transport, true) }
func NewServerSession(transport io.ReadWriteCloser) *Session { return newSession(transport, false) }

func newSession(transport io.ReadWriteCloser, client bool) *Session {
	s := &Session{
		transport: transport,
		client:    client,
		streams:   make(map[uint32]*MuxStream),
		accept:    make(chan *MuxStream, 64),
	}
	if client {
		s.nextID = 1
	} else {
		s.nextID = 2
	}
	go s.readLoop()
	return s
}

// OpenStream opens a new outbound stream.
func (s *Session) OpenStream() (*MuxStream, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, s.err()
	}
	id := s.nextID
	s.nextID += 2
	st := newMuxStream(s, id)
	s.streams[id] = st
	s.mu.Unlock()
	if err := s.writeFrame(muxSyn, id, nil); err != nil {
		s.removeStream(id)
		return nil, err
	}
	return st, nil
}

// AcceptStream returns the next inbound stream opened by the peer.
func (s *Session) AcceptStream() (*MuxStream, error) {
	st, ok := <-s.accept
	if !ok {
		return nil, s.err()
	}
	return st, nil
}

// Close tears down the session, its transport, and every stream.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.closeErr == nil {
		s.closeErr = ErrSessionClosed
	}
	streams := make([]*MuxStream, 0, len(s.streams))
	for _, st := range s.streams {
		streams = append(streams, st)
	}
	close(s.accept)
	s.mu.Unlock()
	for _, st := range streams {
		st.shutdown(ErrSessionClosed)
	}
	return s.transport.Close()
}

func (s *Session) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeErr != nil {
		return s.closeErr
	}
	return ErrSessionClosed
}

func (s *Session) removeStream(id uint32) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
}

func (s *Session) writeFrame(typ byte, id uint32, payload []byte) error {
	hdr := make([]byte, muxHdrLen)
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], id)
	binary.BigEndian.PutUint32(hdr[5:], uint32(len(payload)))
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.isClosed() {
		return s.err()
	}
	if _, err := s.transport.Write(hdr); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := s.transport.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// readLoop demultiplexes frames off the transport into per-stream buffers.
func (s *Session) readLoop() {
	var hdr [muxHdrLen]byte
	for {
		if _, err := io.ReadFull(s.transport, hdr[:]); err != nil {
			s.fail(err)
			return
		}
		typ := hdr[0]
		id := binary.BigEndian.Uint32(hdr[1:])
		n := binary.BigEndian.Uint32(hdr[5:])
		var payload []byte
		if n > 0 {
			payload = make([]byte, n)
			if _, err := io.ReadFull(s.transport, payload); err != nil {
				s.fail(err)
				return
			}
		}
		switch typ {
		case muxSyn:
			s.onSyn(id)
		case muxData:
			s.onData(id, payload)
		case muxFin:
			if st := s.getStream(id); st != nil {
				st.remoteFin()
			}
		case muxRst:
			if st := s.getStream(id); st != nil {
				st.shutdown(ErrStreamReset)
				s.removeStream(id)
			}
		case muxWindow:
			if st := s.getStream(id); st != nil && len(payload) == 4 {
				st.grantSend(binary.BigEndian.Uint32(payload))
			}
		}
	}
}

func (s *Session) onSyn(id uint32) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if _, exists := s.streams[id]; exists {
		s.mu.Unlock()
		return
	}
	st := newMuxStream(s, id)
	s.streams[id] = st
	s.mu.Unlock()
	select {
	case s.accept <- st:
	default:
		// Accept backlog full: refuse the stream rather than block the read loop.
		s.removeStream(id)
		_ = s.writeFrame(muxRst, id, nil)
	}
}

func (s *Session) onData(id uint32, payload []byte) {
	st := s.getStream(id)
	if st == nil {
		return // unknown/closed stream — drop
	}
	st.deliver(payload)
}

func (s *Session) getStream(id uint32) *MuxStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[id]
}

func (s *Session) fail(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.closeErr = err
	streams := make([]*MuxStream, 0, len(s.streams))
	for _, st := range s.streams {
		streams = append(streams, st)
	}
	close(s.accept)
	s.mu.Unlock()
	for _, st := range streams {
		st.shutdown(err)
	}
	_ = s.transport.Close()
}

// MuxStream is one logical connection within a Session. It implements io.ReadWriteCloser.
type MuxStream struct {
	sess *Session
	id   uint32

	mu        sync.Mutex
	readable  *sync.Cond
	writable  *sync.Cond
	inbox     []byte
	sendWin   int   // credit the peer has granted us (starts at the default receive window)
	remoteEOF bool  // peer sent FIN
	localFIN  bool  // we sent FIN
	err       error // set on reset/session failure
	closed    bool
}

func newMuxStream(sess *Session, id uint32) *MuxStream {
	st := &MuxStream{sess: sess, id: id, sendWin: muxDefWindow}
	st.readable = sync.NewCond(&st.mu)
	st.writable = sync.NewCond(&st.mu)
	return st
}

func (st *MuxStream) deliver(p []byte) {
	st.mu.Lock()
	if st.closed || st.err != nil {
		st.mu.Unlock()
		return
	}
	st.inbox = append(st.inbox, p...)
	st.readable.Broadcast()
	st.mu.Unlock()
}

func (st *MuxStream) remoteFin() {
	st.mu.Lock()
	st.remoteEOF = true
	st.readable.Broadcast()
	st.mu.Unlock()
}

func (st *MuxStream) grantSend(n uint32) {
	st.mu.Lock()
	st.sendWin += int(n)
	st.writable.Broadcast()
	st.mu.Unlock()
}

func (st *MuxStream) shutdown(err error) {
	st.mu.Lock()
	if st.err == nil {
		st.err = err
	}
	st.readable.Broadcast()
	st.writable.Broadcast()
	st.mu.Unlock()
}

// Read returns delivered bytes in order, replenishing the peer's send window as data is consumed.
func (st *MuxStream) Read(p []byte) (int, error) {
	st.mu.Lock()
	for len(st.inbox) == 0 && st.err == nil && !st.remoteEOF {
		st.readable.Wait()
	}
	if len(st.inbox) == 0 {
		err := st.err
		if err == nil && st.remoteEOF {
			err = io.EOF
		}
		st.mu.Unlock()
		return 0, err
	}
	n := copy(p, st.inbox)
	st.inbox = st.inbox[n:]
	st.mu.Unlock()
	// Replenish flow-control credit for what we just consumed.
	_ = st.sess.writeFrame(muxWindow, st.id, u32bytes(uint32(n)))
	return n, nil
}

// Write sends bytes, chunked and flow-controlled; blocks while the peer's window is exhausted.
func (st *MuxStream) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		st.mu.Lock()
		for st.sendWin == 0 && st.err == nil && !st.closed && !st.localFIN {
			st.writable.Wait()
		}
		if st.err != nil || st.closed || st.localFIN {
			e := st.err
			if e == nil {
				e = io.ErrClosedPipe
			}
			st.mu.Unlock()
			return total, e
		}
		n := len(p)
		if n > st.sendWin {
			n = st.sendWin
		}
		if n > muxMaxChunk {
			n = muxMaxChunk
		}
		st.sendWin -= n
		st.mu.Unlock()

		if err := st.sess.writeFrame(muxData, st.id, p[:n]); err != nil {
			return total, err
		}
		p = p[n:]
		total += n
	}
	return total, nil
}

// Close half-closes the write side (sends FIN). Reads may continue until the peer's FIN/EOF.
func (st *MuxStream) Close() error {
	st.mu.Lock()
	if st.localFIN || st.closed {
		st.mu.Unlock()
		return nil
	}
	st.localFIN = true
	st.writable.Broadcast()
	st.mu.Unlock()
	return st.sess.writeFrame(muxFin, st.id, nil)
}

// Reset aborts the stream immediately in both directions.
func (st *MuxStream) Reset() error {
	st.mu.Lock()
	st.closed = true
	if st.err == nil {
		st.err = ErrStreamReset
	}
	st.readable.Broadcast()
	st.writable.Broadcast()
	st.mu.Unlock()
	st.sess.removeStream(st.id)
	return st.sess.writeFrame(muxRst, st.id, nil)
}

func u32bytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}
