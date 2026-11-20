// One link (PROTOCOL.md sections 3, 4.2 and 5.4): a WebSocket over one line, one binary message per
// frame. A browser cannot open a raw socket, so every link is a WebSocket on every platform.
//
// Link.dial opens the socket, sends HELLO and waits for WELCOME or REJECT, at most the handshake
// timeout. From then on the session owns the link: it queues frames with send() and receives every
// message through start()'s handlers.
//
// A WebSocket's send() never blocks; the platform buffers whatever it is given, and its
// bufferedAmount says how much. So each link keeps a queue of its own and hands frames to the socket
// only while bufferedAmount is below a high-water mark: a frame still in the queue can be skipped once
// the peer has acknowledged it through another link, so a slow link only carries what the peer still
// lacks. The queue plus bufferedAmount is bounded by window + 128 KiB; a frame that does not fit
// after the acknowledged ones are dropped makes send() return false, and the session closes the
// link. A link whose bufferedAmount stays above zero without shrinking for the dead time is stuck, and
// check() says so.
//
// React Native's WebSocket keeps no reliable bufferedAmount. Where it is missing, the link counts it
// as zero: frames go to the socket at once, the bound covers only this queue, and a link that stops
// writing is caught by the liveness rules instead (no inbound byte, or no ACK progress, for 1 s),
// which the stuck check would otherwise catch first.
//
//   - WebSocketLike / WebSocketConstructor: the part of the WebSocket API a link uses; the browser's,
//     Node's and React Native's all fit.
//   - RejectedError: the server refused the link; code is the REJECT code.
//   - Link.dial(url, hello, WebSocket, timeoutMs) resolves with the link and the WELCOME, or rejects
//     with a RejectedError or the reason the handshake failed. A socket error is "connection failed",
//     followed by the platform's message when the error event has one (React Native's does: a TLS
//     or DNS failure names itself there; a browser's error event says nothing).
//   - link.start(handlers, limit, acked): sets the queue's bound (limit) and the peer's acknowledged
//     offset (acked()), and from the next microtask on delivers messages to handlers.onMessage, those
//     that arrived since the WELCOME first; handlers.onClose(reason) is called when the socket closes
//     or fails by itself, including before start.
//   - link.send(frame, end) queues a frame; end is the stream offset after a DATA frame, else 0.
//     Returns false when the queue is full.
//   - link.check(now, deadMs) returns why the link is stuck, or null; it also hands queued frames on.
//   - link.queued() is the bytes waiting (queue plus bufferedAmount); link.close() closes the socket
//     and returns whether this call closed it. bytesIn, bytesOut and lastIn are counters for status.
//
//     const { link, welcome } = await Link.dial(url, hello, WebSocket, HANDSHAKE_TIMEOUT_MS);
//     link.start({ onMessage: (m) => handle(m), onClose: (why) => drop(why) }, limit, () => acked);
//     if (!link.send(encodePing(1n), 0)) drop('send queue full');
import {
  FrameType,
  RejectCode,
  decodeReject,
  decodeWelcome,
  encodeHello,
  parseFrame,
  type HelloFrame,
  type WelcomeFrame,
} from './protocol.js';

export interface WebSocketLike {
  binaryType: string;
  readonly bufferedAmount?: number;
  send(data: Uint8Array<ArrayBuffer>): void;
  close(): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onclose: ((ev: { code?: number; reason?: string }) => void) | null;
}

export type WebSocketConstructor = new (url: string) => WebSocketLike;

export class RejectedError extends Error {
  constructor(
    readonly code: number,
    reason: string,
  ) {
    super(`rejected (code ${code}): ${reason}`);
  }
}

// Frames go to the socket while its own buffer holds less than this; more would only hide frames
// that could still be skipped.
const HIGH_WATER = 256 << 10;
// How soon a link whose socket buffer was full looks again, since WebSocket has no drain event.
const REPUMP_MS = 10;

interface Queued {
  f: Uint8Array<ArrayBuffer>;
  end: number;
}

export interface LinkHandlers {
  onMessage(msg: Uint8Array): void;
  onClose(reason: string): void;
}

function failed(ev: unknown): string {
  const message = (ev as { message?: unknown } | null)?.message;
  return typeof message === 'string' && message !== ''
    ? `connection failed: ${message}`
    : 'connection failed';
}

function bytesOf(data: unknown): Uint8Array | null {
  return data instanceof ArrayBuffer ? new Uint8Array(data) : null;
}

export class Link {
  bytesIn = 0;
  bytesOut = 0;
  lastIn = Date.now();

  private handlers: LinkHandlers | null = null;
  private early: Uint8Array[] = [];
  private queue: Queued[] = [];
  private head = 0;
  private queueBytes = 0;
  private limit = 0;
  private acked: () => number = () => 0;
  private closed = false;
  private failure: string | null = null;
  private repump: ReturnType<typeof setTimeout> | null = null;
  private written = 0;
  private drainedAt = Date.now();

  private constructor(private ws: WebSocketLike) {
    ws.onmessage = (ev) => {
      const msg = bytesOf(ev.data);
      if (msg === null) return this.fail('a text message on a link');
      this.lastIn = Date.now();
      this.bytesIn += msg.length;
      if (this.handlers) this.handlers.onMessage(msg);
      else this.early.push(msg);
    };
    ws.onerror = (ev) => this.fail(failed(ev));
    ws.onclose = (ev) => this.fail(`connection closed (code ${ev.code ?? 'none'})`);
  }

  static dial(
    url: string,
    hello: HelloFrame,
    WebSocket: WebSocketConstructor,
    timeoutMs: number,
  ): Promise<{ link: Link; welcome: WelcomeFrame }> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      ws.binaryType = 'arraybuffer';
      let settled = false;
      const fail = (err: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        ws.close();
        reject(err);
      };
      const timer = setTimeout(
        () => fail(new Error(`no answer to hello within ${timeoutMs} ms`)),
        timeoutMs,
      );
      ws.onopen = () => ws.send(encodeHello(hello));
      // A browser's error event carries no detail; the close event that follows has the code.
      ws.onerror = (ev) => fail(new Error(failed(ev)));
      ws.onclose = (ev) => fail(new Error(`connection closed (code ${ev.code ?? 'none'})`));
      ws.onmessage = (ev) => {
        try {
          const msg = bytesOf(ev.data);
          if (msg === null) throw new Error('a text message during the handshake');
          const { type, payload } = parseFrame(msg, 65535);
          if (type === FrameType.Reject) {
            const r = decodeReject(payload);
            throw new RejectedError(r.code, r.reason);
          }
          if (type !== FrameType.Welcome)
            throw new Error(`answer to hello has type 0x${type.toString(16)}`);
          const welcome = decodeWelcome(payload);
          if (welcome.version !== hello.version) {
            throw new RejectedError(
              RejectCode.Version,
              `server speaks protocol ${welcome.version}, want ${hello.version}`,
            );
          }
          settled = true;
          clearTimeout(timer);
          resolve({ link: new Link(ws), welcome });
        } catch (err) {
          fail(err instanceof Error ? err : new Error(String(err)));
        }
      };
    });
  }

  start(handlers: LinkHandlers, limit: number, acked: () => number): void {
    this.limit = limit;
    this.acked = acked;
    // What arrived before start is handed over once the caller's current work is done, so a session
    // can queue the link's replay first and nothing it handles can overtake that replay.
    queueMicrotask(() => {
      this.handlers = handlers;
      const early = this.early;
      this.early = [];
      for (const msg of early) if (!this.closed) handlers.onMessage(msg);
      if (this.failure !== null) handlers.onClose(this.failure);
    });
  }

  private buffered(): number {
    return typeof this.ws.bufferedAmount === 'number' ? this.ws.bufferedAmount : 0;
  }

  queued(): number {
    return this.queueBytes + this.buffered();
  }

  send(f: Uint8Array<ArrayBuffer>, end: number): boolean {
    if (this.closed) return true;
    if (this.queued() + f.length > this.limit) this.dropAcked();
    if (this.queued() + f.length > this.limit) return false;
    this.queue.push({ f, end });
    this.queueBytes += f.length;
    this.pump();
    return true;
  }

  // dropAcked removes queued DATA the peer already has.
  private dropAcked(): void {
    const acked = this.acked();
    const kept: Queued[] = [];
    for (let i = this.head; i < this.queue.length; i++) {
      const q = this.queue[i];
      if (q.end > 0 && q.end <= acked) this.queueBytes -= q.f.length;
      else kept.push(q);
    }
    this.queue = kept;
    this.head = 0;
  }

  private pump(): void {
    if (this.closed) return;
    const acked = this.acked();
    while (this.head < this.queue.length && this.buffered() < HIGH_WATER) {
      const q = this.queue[this.head];
      this.queue[this.head++] = undefined as unknown as Queued;
      this.queueBytes -= q.f.length;
      if (q.end > 0 && q.end <= acked) continue;
      try {
        this.ws.send(q.f);
      } catch (err) {
        return this.fail(`send: ${String(err)}`);
      }
      this.bytesOut += q.f.length;
    }
    if (this.head === this.queue.length) {
      this.queue = [];
      this.head = 0;
    } else if (this.repump === null) {
      this.repump = setTimeout(() => {
        this.repump = null;
        this.pump();
      }, REPUMP_MS);
    }
  }

  check(now: number, deadMs: number): string | null {
    this.pump();
    // The socket has written whatever it was given minus what it still buffers; a link is stuck when
    // that stops growing while bytes wait. Comparing bufferedAmount alone would take a socket that
    // drains and is refilled between two checks for a stuck one.
    const b = this.buffered();
    const written = this.bytesOut - b;
    if (b === 0 || written > this.written) this.drainedAt = now;
    this.written = written;
    if (now - this.drainedAt > deadMs)
      return `write stuck for ${deadMs} ms with ${b} bytes buffered`;
    return null;
  }

  close(): boolean {
    if (this.closed) return false;
    this.closed = true;
    if (this.repump !== null) clearTimeout(this.repump);
    this.queue = [];
    this.queueBytes = 0;
    this.ws.close();
    return true;
  }

  // fail reports a link that closed or broke by itself; the session then drops it.
  private fail(reason: string): void {
    if (this.closed) return;
    if (this.handlers) this.handlers.onClose(reason);
    else this.failure ??= reason; // reported by start(), which the owner calls next
  }
}
