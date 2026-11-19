// Streams over one session (PROTOCOL.md section 6), the client's half of the Go package `mux`: SYN
// opens a stream, DATA carries its bytes, FIN half-closes it, RST aborts it, and WINDOW returns
// credit. Every stream has its own credit in each direction, enforced by the receiver, so one stream
// can neither starve the others nor make the receiver buffer without bound.
//
// Feeding never waits: feed() only appends to stream buffers and sends control frames, which the
// session never makes wait. A write waits for the stream's credit and for room in the session window.
// Credit goes back in batches, when a quarter of the window has been read or everything has, so a
// bulk transfer does not cost a WINDOW per read.
//
// A stream's receive credit starts at the server's streamWindow. A client may grant more right after
// the SYN (Mux's recvWindow): the server's send buffer is bounded by the session window whatever the
// credit, so the extra credit costs the server nothing, and it lets one download use more than
// streamWindow per round trip. The client's own buffer per stream is then bounded by recvWindow.
//
//   - Transport: what the mux writes to; Session implements it.
//
//   - new Mux(transport, limits, recvWindow) creates the client side of a mux; recvWindow is each
//     stream's receive credit, at least limits.streamWindow.
//
//   - mux.feed(bytes) consumes in-order session bytes; it throws on a protocol violation (a SYN from
//     the server, DATA beyond the credit, a malformed frame), which ends the session.
//
//   - mux.open() opens a stream. mux.close(why) fails every stream at once; the client calls it when
//     the session ends. mux.streams() is a snapshot (StreamStat) of the open streams. mux.remove(id)
//     forgets a released stream; streams call it themselves.
//
//   - Stream: read() resolves with the next chunk, or null after the peer's FIN, and rejects once the
//     stream is reset or the session ended. write(bytes) resolves once every byte is sent, waiting for
//     credit as needed; writes are kept in order. closeWrite() sends FIN; reads continue until the
//     peer's FIN. reset(why) aborts both directions and tells the peer why. id is the stream id;
//     released says whether both sides have sent FIN or the stream was reset, after which it holds
//     nothing on either side. deliver, grant, remoteFin, fail and stat are the mux's side of a stream.
//
//     const mux = new Mux(session, limits, 1 << 20);
//     const st = mux.open();
//     await st.write(request);
//     st.closeWrite();
//     for (let c = await st.read(); c !== null; c = await st.read()) use(c);
import {
  MAX_MUX_DATA,
  MAX_REASON,
  MuxType,
  decodeReason,
  encodeMux,
  parseMux,
  type Limits,
} from './protocol.js';
import { SessionEndedError } from './session.js';

export interface Transport {
  send(frame: Uint8Array, control: boolean): void;
  fits(n: number): boolean;
  room(n: number): Promise<void>;
}

export interface StreamStat {
  id: number;
  bytesIn: number;
  bytesOut: number;
  buffered: number;
  sendCredit: number;
}

const MUX_HEADER = 7;

export class Mux {
  private streamMap = new Map<number, Stream>();
  private nextId = 1;
  private failure: Error | null = null;
  private buf = new Uint8Array(0);

  constructor(
    readonly transport: Transport,
    private limits: Limits,
    readonly recvWindow: number,
  ) {
    if (recvWindow < limits.streamWindow)
      throw new Error('multipath: recvWindow below the streamWindow');
  }

  open(): Stream {
    if (this.failure) throw this.failure;
    const st = new Stream(this, this.nextId, this.limits.streamWindow, this.recvWindow);
    this.nextId += 2;
    this.streamMap.set(st.id, st);
    this.transport.send(encodeMux(MuxType.Syn, st.id), true);
    const extra = this.recvWindow - this.limits.streamWindow;
    if (extra > 0) this.transport.send(encodeMux(MuxType.Window, st.id, u32(extra)), true);
    return st;
  }

  close(why: string): void {
    if (this.failure) return;
    this.failure = new Error(`multipath: session ended: ${why}`);
    const streams = [...this.streamMap.values()];
    this.streamMap.clear();
    for (const st of streams) st.fail(this.failure);
  }

  remove(id: number): void {
    this.streamMap.delete(id);
  }

  feed(chunk: Uint8Array): void {
    const buf = this.buf.length ? concat(this.buf, chunk) : chunk;
    let at = 0;
    for (;;) {
      const parsed = parseMux(buf, at);
      if (parsed === null) break;
      this.handle(parsed.frame.type, parsed.frame.stream, parsed.frame.body);
      at += parsed.n;
    }
    // Only a partial frame is kept, copied, so the buffer never holds more than one frame.
    this.buf = buf.slice(at);
  }

  private handle(type: number, id: number, body: Uint8Array): void {
    const st = this.streamMap.get(id);
    switch (type) {
      case MuxType.Syn:
        throw new Error(`mux syn ${id} from the server`);
      case MuxType.Data:
        if (body.length === 0 || body.length > MAX_MUX_DATA) {
          throw new Error(`mux data of ${body.length} bytes on stream ${id}`);
        }
        st?.deliver(body);
        return;
      case MuxType.Fin:
        st?.remoteFin();
        return;
      case MuxType.Rst: {
        const why = decodeReason(body);
        if (st) {
          st.fail(new Error(`multipath: stream reset by peer${why ? `: ${why}` : ''}`));
          this.remove(id);
        }
        return;
      }
      case MuxType.Window:
        if (body.length !== 4)
          throw new Error(`mux window of ${body.length} bytes on stream ${id}`);
        st?.grant(new DataView(body.buffer, body.byteOffset, 4).getUint32(0));
        return;
    }
    // Frames for a stream that is gone crossed its RST and are dropped; unknown types are skipped.
  }

  streams(): StreamStat[] {
    return [...this.streamMap.values()].map((st) => st.stat());
  }
}

export class Stream {
  private inbox: Uint8Array[] = [];
  private inboxBytes = 0;
  private consumed = 0;
  private remoteFIN = false;
  private localFIN = false;
  private err: Error | null = null;
  private readers: (() => void)[] = [];
  private writers: (() => void)[] = [];
  private writing: Promise<void> = Promise.resolve();
  private bytesIn = 0;
  private bytesOut = 0;

  constructor(
    private mux: Mux,
    readonly id: number,
    private sendCredit: number,
    private recvCredit: number,
  ) {}

  stat(): StreamStat {
    return {
      id: this.id,
      bytesIn: this.bytesIn,
      bytesOut: this.bytesOut,
      buffered: this.inboxBytes,
      sendCredit: this.sendCredit,
    };
  }

  deliver(p: Uint8Array): void {
    if (p.length > this.recvCredit) {
      throw new Error(
        `mux stream ${this.id} sent ${p.length} bytes with ${this.recvCredit} of credit`,
      );
    }
    this.recvCredit -= p.length;
    if (this.err || this.remoteFIN) return;
    this.inbox.push(p.slice());
    this.inboxBytes += p.length;
    this.bytesIn += p.length;
    wakeAll(this.readers);
  }

  grant(n: number): void {
    if (this.sendCredit + n > 2 ** 31) throw new Error(`mux stream ${this.id} credit overflow`);
    this.sendCredit += n;
    wakeAll(this.writers);
  }

  remoteFin(): void {
    this.remoteFIN = true;
    wakeAll(this.readers);
    if (this.localFIN) this.mux.remove(this.id);
  }

  fail(err: Error): void {
    this.err ??= err;
    wakeAll(this.readers);
    wakeAll(this.writers);
  }

  async read(): Promise<Uint8Array | null> {
    while (this.inbox.length === 0) {
      if (this.err) throw this.err;
      if (this.remoteFIN) return null;
      await new Promise<void>((r) => this.readers.push(r));
    }
    const chunk = this.inbox.shift()!;
    this.inboxBytes -= chunk.length;
    this.consumed += chunk.length;
    if (
      (this.consumed >= this.mux.recvWindow / 4 || this.inbox.length === 0) &&
      !this.remoteFIN &&
      !this.err
    ) {
      const grant = this.consumed;
      this.consumed = 0;
      this.recvCredit += grant;
      this.mux.transport.send(encodeMux(MuxType.Window, this.id, u32(grant)), true);
    }
    return chunk;
  }

  write(bytes: Uint8Array): Promise<void> {
    const next = this.writing.then(() => this.writeNow(bytes));
    // The chain continues after a failed write; that write's caller sees its error.
    this.writing = next.catch(() => undefined);
    return next;
  }

  private async writeNow(bytes: Uint8Array): Promise<void> {
    for (let p = 0; p < bytes.length;) {
      while (this.sendCredit === 0 && !this.err && !this.localFIN) {
        await new Promise<void>((r) => this.writers.push(r));
      }
      if (this.err) throw this.err;
      if (this.localFIN) throw new Error('multipath: write after closeWrite');
      const n = Math.min(bytes.length - p, this.sendCredit, MAX_MUX_DATA);
      if (!this.mux.transport.fits(n + MUX_HEADER)) {
        await this.mux.transport.room(n + MUX_HEADER);
        continue; // credit and state may have changed meanwhile
      }
      this.sendCredit -= n;
      this.bytesOut += n;
      this.mux.transport.send(encodeMux(MuxType.Data, this.id, bytes.subarray(p, p + n)), false);
      p += n;
    }
  }

  get released(): boolean {
    return (this.localFIN && this.remoteFIN) || this.err !== null;
  }

  closeWrite(): void {
    if (this.localFIN || this.err) return;
    this.localFIN = true;
    wakeAll(this.writers);
    this.mux.transport.send(encodeMux(MuxType.Fin, this.id), true);
    if (this.remoteFIN) this.mux.remove(this.id);
  }

  reset(why: string): void {
    if (this.err) return;
    this.fail(new Error(`multipath: stream reset: ${why}`));
    this.mux.remove(this.id);
    const reason = new TextEncoder().encode(why).subarray(0, MAX_REASON);
    try {
      this.mux.transport.send(encodeMux(MuxType.Rst, this.id, reason), true);
    } catch (err) {
      // The session ended this very moment, which released the stream on both sides as well.
      if (!(err instanceof SessionEndedError)) throw err;
    }
  }
}

function wakeAll(waiters: (() => void)[]): void {
  for (const w of waiters.splice(0)) w();
}

function u32(n: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n);
  return b;
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
}
