// L4 — many logical streams over the one redundant stream, the browser's async mirror of the Go/JVM
// mux. Frame: type:u8 | streamID:u32 | length:u32 | payload. The opener uses odd stream IDs (a
// browser client opens; the origin accepts), a per-stream credit window bounds one stream's grab of
// the shared transport, FIN half-closes and RST aborts, and a stream fully half-closed both ways is
// freed without an RST so a graceful close never discards the peer's buffered bytes.

import { RedundantStream } from './redundant.js';

const Mux = { Syn: 0x01, Data: 0x02, Fin: 0x03, Rst: 0x04, Window: 0x05 } as const;
const MUX_HDR = 9;
const MUX_MAX_CHUNK = 16 * 1024;
const STREAM_WINDOW = 256 * 1024;

export class MuxSession {
  private streams = new Map<number, MuxStream>();
  private parseBuf = new Uint8Array(0);
  private nextId: number;
  private writeChain: Promise<void> = Promise.resolve();
  private failure: Error | null = null;

  private constructor(
    private transport: RedundantStream,
    client: boolean,
  ) {
    this.nextId = client ? 1 : 2;
    transport.onDeliver = (bytes) => this.onBytes(bytes);
  }

  static client(transport: RedundantStream): MuxSession {
    return new MuxSession(transport, true);
  }

  openStream(): MuxStream {
    const id = this.nextId;
    this.nextId += 2;
    const st = new MuxStream(this, id);
    this.streams.set(id, st);
    void this.send(Mux.Syn, id, null);
    return st;
  }

  close(): void {
    this.failAll(new Error('multipath: mux session closed'));
    this.transport.close();
  }

  /** Serializes every frame write so complete frames reach the transport in order. */
  send(type: number, id: number, payload: Uint8Array | null): Promise<void> {
    const frame = new Uint8Array(MUX_HDR + (payload?.length ?? 0));
    const v = new DataView(frame.buffer);
    frame[0] = type;
    v.setUint32(1, id);
    v.setUint32(5, payload?.length ?? 0);
    if (payload) frame.set(payload, MUX_HDR);
    this.writeChain = this.writeChain.then(() => this.transport.write(frame)).catch(() => {});
    return this.writeChain;
  }

  removeStream(id: number): void {
    this.streams.delete(id);
  }

  private onBytes(chunk: Uint8Array): void {
    const next = new Uint8Array(this.parseBuf.length + chunk.length);
    next.set(this.parseBuf);
    next.set(chunk, this.parseBuf.length);
    this.parseBuf = next;

    while (this.parseBuf.length >= MUX_HDR) {
      const v = new DataView(
        this.parseBuf.buffer,
        this.parseBuf.byteOffset,
        this.parseBuf.byteLength,
      );
      const type = this.parseBuf[0];
      const id = v.getUint32(1);
      const n = v.getUint32(5);
      if (this.parseBuf.length < MUX_HDR + n) break;
      const payload = this.parseBuf.slice(MUX_HDR, MUX_HDR + n);
      this.parseBuf = this.parseBuf.slice(MUX_HDR + n);
      this.dispatch(type, id, payload);
    }
  }

  private dispatch(type: number, id: number, payload: Uint8Array): void {
    const st = this.streams.get(id);
    switch (type) {
      case Mux.Data:
        st?.onData(payload);
        break;
      case Mux.Fin:
        st?.remoteFin();
        break;
      case Mux.Rst:
        if (st) {
          st.onReset();
          this.streams.delete(id);
        }
        break;
      case Mux.Window:
        if (st && payload.length === 4)
          st.grantSend(new DataView(payload.buffer, payload.byteOffset).getUint32(0));
        break;
      // Syn is the accept side; a browser client only opens, so an inbound Syn is ignored.
    }
  }

  private failAll(err: Error): void {
    this.failure = err;
    for (const st of this.streams.values()) st.onReset(err);
    this.streams.clear();
  }

  get failed(): Error | null {
    return this.failure;
  }
}

export class MuxStream {
  private inbox: Uint8Array[] = [];
  private inboxLen = 0;
  private remoteEOF = false;
  private localFIN = false;
  private err: Error | null = null;
  private sendWin = STREAM_WINDOW;
  private readWaiters: Array<() => void> = [];
  private writeWaiters: Array<() => void> = [];

  constructor(
    private sess: MuxSession,
    readonly id: number,
  ) {}

  /** Reads the next chunk of delivered bytes, or null at a clean EOF; throws on reset. */
  async read(): Promise<Uint8Array | null> {
    for (;;) {
      if (this.inbox.length > 0) {
        const chunk = this.inbox.shift()!;
        this.inboxLen -= chunk.length;
        void this.sess.send(0x05, this.id, u32(chunk.length)); // WINDOW: replenish the peer's credit
        return chunk;
      }
      if (this.err) throw this.err;
      if (this.remoteEOF) return null;
      await new Promise<void>((resolve) => this.readWaiters.push(resolve));
    }
  }

  /** Writes bytes, chunked and flow-controlled; awaits the peer's window when it is exhausted. */
  async write(bytes: Uint8Array): Promise<void> {
    let p = 0;
    while (p < bytes.length) {
      while (this.sendWin === 0 && this.err === null && !this.localFIN) {
        await new Promise<void>((resolve) => this.writeWaiters.push(resolve));
      }
      if (this.err) throw this.err;
      if (this.localFIN) throw new Error('multipath: write after close');
      const n = Math.min(bytes.length - p, this.sendWin, MUX_MAX_CHUNK);
      this.sendWin -= n;
      await this.sess.send(0x02, this.id, bytes.subarray(p, p + n));
      p += n;
    }
  }

  /** Half-closes the write side (FIN); reads may continue until the peer's EOF. */
  closeWrite(): void {
    if (this.localFIN) return;
    this.localFIN = true;
    this.wakeWriters();
    void this.sess.send(0x03, this.id, null);
    this.cleanupIfClosed();
  }

  /** Aborts the stream in both directions (RST). */
  reset(): void {
    if (this.err === null) this.err = new Error('multipath: stream reset');
    this.wakeReaders();
    this.wakeWriters();
    this.sess.removeStream(this.id);
    void this.sess.send(0x04, this.id, null);
  }

  onData(payload: Uint8Array): void {
    if (this.err || this.remoteEOF) return;
    this.inbox.push(payload);
    this.inboxLen += payload.length;
    this.wakeReaders();
  }

  remoteFin(): void {
    this.remoteEOF = true;
    this.wakeReaders();
    this.cleanupIfClosed();
  }

  onReset(err?: Error): void {
    if (this.err === null) this.err = err ?? new Error('multipath: stream reset');
    this.wakeReaders();
    this.wakeWriters();
  }

  grantSend(n: number): void {
    this.sendWin += n;
    this.wakeWriters();
  }

  // Frees the stream once both sides have half-closed: a clean close is torn down by the FINs, never
  // by an RST, so the peer keeps whatever it has not yet drained.
  private cleanupIfClosed(): void {
    if (this.localFIN && this.remoteEOF) this.sess.removeStream(this.id);
  }

  private wakeReaders(): void {
    const w = this.readWaiters;
    this.readWaiters = [];
    for (const r of w) r();
  }

  private wakeWriters(): void {
    const w = this.writeWaiters;
    this.writeWaiters = [];
    for (const r of w) r();
  }
}

function u32(n: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n);
  return b;
}
