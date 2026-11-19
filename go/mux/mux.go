// Package mux carries many streams over one session (PROTOCOL.md section 6): SYN opens a stream,
// DATA carries its bytes, FIN half-closes it, RST aborts it, and WINDOW returns credit. Every stream
// has its own credit window in each direction, enforced by the receiver, so one stream can neither
// starve the others nor make the receiver buffer without bound.
//
// The mux never blocks the session's reader: Feed only appends to stream buffers and sends control
// frames, which the session never makes wait. A stream's Write waits for credit and for the session
// window, in the writer's own goroutine. Streams share the session's one ordered byte stream, so a
// stalled session stalls all of them together; that is the price of the bounded link count, and the
// session is built not to stall.
//
//   - Transport is what the mux writes to; *session.Session implements it.
//
//   - New(t, client, lim, onStream) creates a mux. Only the client opens streams. On the server,
//     onStream is called for each stream the client opens; it must not block (start a goroutine).
//     Beyond lim.MaxStreams open streams, a new one is reset with "too many streams".
//
//   - (*Mux).Feed(p) consumes in-order session bytes; it returns an error for a protocol violation,
//     which ends the session.
//
//   - (*Mux).Open() opens a stream (client only).
//
//   - (*Mux).Close(why) fails every stream at once; the owner calls it when the session ends.
//
//   - (*Mux).Streams() is a snapshot of the open streams, for status pages.
//
//   - *Stream is an io.ReadWriteCloser: Read returns bytes in order and returns credit as they are
//     consumed; Write waits for credit; Close sends FIN (reads continue until the peer's FIN);
//     Reset(why) aborts both directions and tells the peer why.
//
//   - Splice(st, conn) (splice.go) relays a stream and a TCP connection until both are done.
//
//     m := mux.New(sess, true, protocol.DefaultLimits(), nil)
//     st, err := m.Open()
//     _, err = st.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
package mux

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/micro-teams/multipath/go/protocol"
)

// ErrClosed is returned once the mux has closed and no reason is known.
var ErrClosed = errors.New("multipath: mux closed")

// Transport carries mux frames; control frames must never wait.
type Transport interface {
	Send(frame []byte, control bool) error
}

// Mux multiplexes streams over one Transport.
type Mux struct {
	t        Transport
	client   bool
	window   int
	max      int
	onStream func(*Stream)

	openMu  sync.Mutex
	mu      sync.Mutex
	streams map[uint32]*Stream
	nextID  uint32 // client: the next id to open
	lastID  uint32 // server: the highest id opened by the peer
	err     error  // set once the mux is closed
	buf     []byte // a partial frame carried over between Feed calls
}

// New creates a mux over t.
func New(t Transport, client bool, lim protocol.Limits, onStream func(*Stream)) *Mux {
	return &Mux{t: t, client: client, window: lim.StreamWindow, max: lim.MaxStreams, onStream: onStream,
		streams: make(map[uint32]*Stream), nextID: 1}
}

// Open opens a stream.
func (m *Mux) Open() (*Stream, error) {
	if !m.client {
		return nil, errors.New("multipath: only the client opens streams")
	}
	// Ids must reach the server in increasing order, so allocating one and sending its SYN happen
	// under one lock. It is not mu, which the session's end takes through Close.
	m.openMu.Lock()
	defer m.openMu.Unlock()
	m.mu.Lock()
	if m.err != nil {
		m.mu.Unlock()
		return nil, m.err
	}
	st := newStream(m, m.nextID)
	m.nextID += 2
	m.streams[st.id] = st
	m.mu.Unlock()
	if err := m.t.Send(protocol.EncodeMux(protocol.MuxSyn, st.id, nil), true); err != nil {
		m.remove(st.id)
		return nil, err
	}
	return st, nil
}

// Close fails every stream with why.
func (m *Mux) Close(why string) {
	m.mu.Lock()
	if m.err != nil {
		m.mu.Unlock()
		return
	}
	m.err = fmt.Errorf("multipath: session ended: %s", why)
	streams := m.streams
	m.streams = map[uint32]*Stream{}
	m.mu.Unlock()
	for _, st := range streams {
		st.fail(m.err)
	}
}

func (m *Mux) remove(id uint32) {
	m.mu.Lock()
	delete(m.streams, id)
	m.mu.Unlock()
}

func (m *Mux) get(id uint32) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[id]
}

// Feed consumes in-order session bytes.
func (m *Mux) Feed(p []byte) error {
	m.buf = append(m.buf, p...)
	for {
		f, n, err := protocol.ParseMux(m.buf)
		if err != nil {
			return err
		}
		if n == 0 {
			// Keep only the partial frame, so the buffer never holds more than one frame.
			m.buf = append(m.buf[:0:0], m.buf...)
			return nil
		}
		if err := m.handle(f); err != nil {
			return err
		}
		m.buf = m.buf[n:]
	}
}

func (m *Mux) handle(f protocol.MuxFrame) error {
	switch f.Type {
	case protocol.MuxSyn:
		return m.onSyn(f.Stream)
	case protocol.MuxData:
		if len(f.Body) == 0 || len(f.Body) > protocol.MaxMuxData {
			return fmt.Errorf("mux data of %d bytes on stream %d", len(f.Body), f.Stream)
		}
		if st := m.get(f.Stream); st != nil {
			return st.deliver(f.Body)
		}
	case protocol.MuxFin:
		if st := m.get(f.Stream); st != nil {
			st.remoteFin()
		}
	case protocol.MuxRst:
		why, err := protocol.DecodeReason(f.Body)
		if err != nil {
			return err
		}
		if st := m.get(f.Stream); st != nil {
			msg := "multipath: stream reset by peer"
			if why != "" {
				msg += ": " + why
			}
			st.fail(errors.New(msg))
			m.remove(f.Stream)
		}
	case protocol.MuxWindow:
		if len(f.Body) != 4 {
			return fmt.Errorf("mux window of %d bytes on stream %d", len(f.Body), f.Stream)
		}
		if st := m.get(f.Stream); st != nil {
			return st.grant(binary.BigEndian.Uint32(f.Body))
		}
	}
	// Frames for a stream that is already gone are dropped (they crossed our RST), and unknown
	// types are skipped: later versions may add them.
	return nil
}

func (m *Mux) onSyn(id uint32) error {
	if m.client {
		return fmt.Errorf("mux syn %d from the server", id)
	}
	m.mu.Lock()
	if id%2 == 0 || id <= m.lastID {
		m.mu.Unlock()
		return fmt.Errorf("mux syn %d after %d", id, m.lastID)
	}
	m.lastID = id
	if m.err != nil {
		m.mu.Unlock()
		return nil
	}
	if len(m.streams) >= m.max {
		m.mu.Unlock()
		return m.t.Send(protocol.EncodeMux(protocol.MuxRst, id, []byte("too many streams")), true)
	}
	st := newStream(m, id)
	m.streams[id] = st
	m.mu.Unlock()
	m.onStream(st)
	return nil
}

// StreamStat is one stream in a Streams snapshot.
type StreamStat struct {
	ID         uint32
	BytesIn    int64 // bytes received
	BytesOut   int64 // bytes sent
	Buffered   int   // received bytes not yet read
	SendCredit int   // bytes the peer still accepts
}

// Streams returns a snapshot of the open streams.
func (m *Mux) Streams() []StreamStat {
	m.mu.Lock()
	streams := make([]*Stream, 0, len(m.streams))
	for _, st := range m.streams {
		streams = append(streams, st)
	}
	m.mu.Unlock()
	out := make([]StreamStat, 0, len(streams))
	for _, st := range streams {
		st.mu.Lock()
		out = append(out, StreamStat{ID: st.id, BytesIn: st.bytesIn, BytesOut: st.bytesOut, Buffered: len(st.inbox), SendCredit: st.sendCredit})
		st.mu.Unlock()
	}
	return out
}

// Stream is one stream of a mux.
type Stream struct {
	m  *Mux
	id uint32

	mu         sync.Mutex
	readable   *sync.Cond
	writable   *sync.Cond
	inbox      []byte
	sendCredit int   // bytes we may still send
	recvCredit int   // bytes the peer may still send
	consumed   int   // bytes read but not yet returned as credit
	remoteFIN  bool  // the peer will send no more
	localFIN   bool  // we will send no more
	err        error // set on reset or session end
	bytesIn    int64
	bytesOut   int64
}

func newStream(m *Mux, id uint32) *Stream {
	st := &Stream{m: m, id: id, sendCredit: m.window, recvCredit: m.window}
	st.readable = sync.NewCond(&st.mu)
	st.writable = sync.NewCond(&st.mu)
	return st
}

// ID returns the stream id.
func (st *Stream) ID() uint32 { return st.id }

func (st *Stream) deliver(p []byte) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(p) > st.recvCredit {
		return fmt.Errorf("mux stream %d sent %d bytes with %d of credit", st.id, len(p), st.recvCredit)
	}
	st.recvCredit -= len(p)
	if st.err != nil || st.remoteFIN {
		return nil
	}
	st.inbox = append(st.inbox, p...)
	st.bytesIn += int64(len(p))
	st.readable.Broadcast()
	return nil
}

func (st *Stream) grant(n uint32) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if int64(st.sendCredit)+int64(n) > 1<<31 {
		return fmt.Errorf("mux stream %d credit overflow", st.id)
	}
	st.sendCredit += int(n)
	st.writable.Broadcast()
	return nil
}

func (st *Stream) remoteFin() {
	st.mu.Lock()
	st.remoteFIN = true
	done := st.localFIN
	st.readable.Broadcast()
	st.mu.Unlock()
	if done {
		st.m.remove(st.id)
	}
}

func (st *Stream) fail(err error) {
	st.mu.Lock()
	if st.err == nil {
		st.err = err
	}
	st.readable.Broadcast()
	st.writable.Broadcast()
	st.mu.Unlock()
}

// Read returns received bytes in order, then io.EOF after the peer's FIN.
func (st *Stream) Read(p []byte) (int, error) {
	st.mu.Lock()
	for len(st.inbox) == 0 && st.err == nil && !st.remoteFIN {
		st.readable.Wait()
	}
	if len(st.inbox) == 0 {
		err := st.err
		if err == nil {
			err = io.EOF
		}
		st.mu.Unlock()
		return 0, err
	}
	n := copy(p, st.inbox)
	st.inbox = st.inbox[n:]
	st.consumed += n
	// Credit goes back in batches, or at once when everything has been read, so a request and its
	// response do not wait on each other and a bulk transfer does not send a WINDOW per read.
	var grant int
	if st.consumed >= st.m.window/4 || len(st.inbox) == 0 {
		grant, st.consumed = st.consumed, 0
		st.recvCredit += grant
	}
	st.mu.Unlock()
	if grant > 0 && !st.finished() {
		if err := st.m.t.Send(protocol.EncodeMux(protocol.MuxWindow, st.id, binary.BigEndian.AppendUint32(nil, uint32(grant))), true); err != nil {
			return n, err
		}
	}
	return n, nil
}

// finished reports whether the peer will send nothing more, so credit is of no use to it.
func (st *Stream) finished() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.remoteFIN || st.err != nil
}

// Write sends p, waiting for credit as needed.
func (st *Stream) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		st.mu.Lock()
		for st.sendCredit == 0 && st.err == nil && !st.localFIN {
			st.writable.Wait()
		}
		if st.err != nil || st.localFIN {
			err := st.err
			if err == nil {
				err = io.ErrClosedPipe
			}
			st.mu.Unlock()
			return total, err
		}
		n := min(len(p), st.sendCredit, protocol.MaxMuxData)
		st.sendCredit -= n
		st.bytesOut += int64(n)
		st.mu.Unlock()
		if err := st.m.t.Send(protocol.EncodeMux(protocol.MuxData, st.id, p[:n]), false); err != nil {
			return total, err
		}
		p = p[n:]
		total += n
	}
	return total, nil
}

// Close sends FIN; the stream is released once the peer has sent FIN too.
func (st *Stream) Close() error {
	st.mu.Lock()
	if st.localFIN || st.err != nil {
		st.mu.Unlock()
		return nil
	}
	st.localFIN = true
	done := st.remoteFIN
	st.writable.Broadcast()
	st.mu.Unlock()
	err := st.m.t.Send(protocol.EncodeMux(protocol.MuxFin, st.id, nil), true)
	if done {
		st.m.remove(st.id)
	}
	return err
}

// Reset aborts the stream in both directions and tells the peer why.
func (st *Stream) Reset(why string) error {
	st.fail(errors.New("multipath: stream reset: " + why))
	st.m.remove(st.id)
	if len(why) > protocol.MaxReason {
		why = why[:protocol.MaxReason]
	}
	return st.m.t.Send(protocol.EncodeMux(protocol.MuxRst, st.id, []byte(why)), true)
}
