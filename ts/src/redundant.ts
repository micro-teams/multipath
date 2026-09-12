// L3 — aggregate several unreliable links into ONE reliable, ordered, never-interrupted duplex byte
// stream, the browser's async mirror of the Go peer (redundant.go). The model is "redundant, not
// striped": every byte is written to ALL live links, the receiver delivers each byte once in order
// by absolute offset, and a link that dies reconnects and replays the unacknowledged tail behind the
// scenes. As long as one link is healthy the logical stream never stalls and never breaks.
//
// Where Go blocks a goroutine, the browser awaits a promise: write() resolves when the send window
// has room, and delivered bytes are handed up through onDeliver. connID is the client's, minted once
// with the platform CSPRNG and carried in every link's HELLO so the origin groups them.

import { FrameType, MAX_SEGMENT, encodeAck, encodeData, encodeNonce } from './frames.js';
import { Link, openWebSocketLink } from './link.js';

export interface RedundantOptions {
  urls: string[]; // one per line; a link is opened (and reconnected) to each
  window?: number; // max unacknowledged bytes before write() awaits ACKs; default 4MiB
  pingIntervalMs?: number; // default 5000
  deadAfterMs?: number; // default 15000; a link silent this long is reconnected
  ackIntervalMs?: number; // default 50
  reconnectDelayMs?: number; // default 200, doubling to maxDelayMs
  maxDelayMs?: number; // default 5000
  wsCtor?: typeof WebSocket; // injected in tests
  now?: () => number; // injected in tests
}

interface LinkSlot {
  link: Link | null;
  lastSeen: number;
  backoff: number;
  reconnectTimer: ReturnType<typeof setTimeout> | null;
}

export class RedundantStream {
  readonly connID: Uint8Array;
  private opt: Required<Omit<RedundantOptions, 'wsCtor' | 'now'>>;
  private wsCtor: typeof WebSocket;
  private now: () => number;

  private slots: LinkSlot[];
  private sendBuf = new Uint8Array(0); // unacknowledged bytes: [ackedOffset, sendOffset)
  private sendOffset = 0n;
  private ackedOffset = 0n;
  private recvOffset = 0n;
  private ackScheduled = false;
  private windowWaiters: Array<() => void> = [];
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private closed = false;
  private opened = false;
  private onOpenResolve: (() => void) | null = null;
  private openPromise: Promise<void>;

  /** Delivered, in-order bytes. Set by the mux above before any traffic flows. */
  onDeliver: (bytes: Uint8Array) => void = () => {};

  constructor(opt: RedundantOptions) {
    this.opt = {
      urls: opt.urls,
      window: opt.window ?? 4 << 20,
      pingIntervalMs: opt.pingIntervalMs ?? 5000,
      deadAfterMs: opt.deadAfterMs ?? 15000,
      ackIntervalMs: opt.ackIntervalMs ?? 50,
      reconnectDelayMs: opt.reconnectDelayMs ?? 200,
      maxDelayMs: opt.maxDelayMs ?? 5000,
    };
    this.wsCtor = opt.wsCtor ?? WebSocket;
    this.now = opt.now ?? Date.now;
    this.connID = new Uint8Array(16);
    crypto.getRandomValues(this.connID);
    this.slots = this.opt.urls.map(() => ({
      link: null,
      lastSeen: this.now(),
      backoff: this.opt.reconnectDelayMs,
      reconnectTimer: null,
    }));
    this.openPromise = new Promise((resolve) => (this.onOpenResolve = resolve));
  }

  /** Opens the links and resolves once at least one is up. */
  async dial(): Promise<void> {
    for (let i = 0; i < this.slots.length; i++) this.connect(i);
    this.pingTimer = setInterval(() => this.tick(), this.opt.pingIntervalMs);
    await this.openPromise;
  }

  private connect(i: number): void {
    if (this.closed) return;
    const slot = this.slots[i];
    slot.link = openWebSocketLink(
      this.opt.urls[i],
      this.connID,
      i,
      {
        onOpen: () => {
          slot.backoff = this.opt.reconnectDelayMs;
          slot.lastSeen = this.now();
          this.replay(slot); // resume: resend everything still unacknowledged
          if (!this.opened) {
            this.opened = true;
            this.onOpenResolve?.();
          }
        },
        onFrame: (frame) => {
          slot.lastSeen = this.now();
          this.onFrame(frame, slot);
        },
        onClose: () => {
          slot.link = null;
          this.scheduleReconnect(i);
        },
      },
      this.wsCtor,
    );
  }

  private scheduleReconnect(i: number): void {
    if (this.closed) return;
    const slot = this.slots[i];
    if (slot.reconnectTimer !== null) return;
    slot.reconnectTimer = setTimeout(() => {
      slot.reconnectTimer = null;
      slot.backoff = Math.min(slot.backoff * 2, this.opt.maxDelayMs);
      this.connect(i);
    }, slot.backoff);
  }

  private onFrame(
    frame: {
      type: number;
      offset?: bigint;
      payload?: Uint8Array;
      cumulative?: bigint;
      nonce?: bigint;
    },
    slot: LinkSlot,
  ): void {
    switch (frame.type) {
      case FrameType.Data: {
        const offset = frame.offset!;
        const payload = frame.payload!;
        const end = offset + BigInt(payload.length);
        if (end <= this.recvOffset) return; // fully seen already
        if (offset > this.recvOffset) return; // a gap cannot happen on a live link; ignore
        const start = Number(this.recvOffset - offset);
        this.recvOffset = end;
        this.onDeliver(payload.subarray(start));
        this.scheduleAck();
        break;
      }
      case FrameType.Ack: {
        const cum = frame.cumulative!;
        if (cum > this.ackedOffset) {
          const drop = Number(cum - this.ackedOffset);
          this.sendBuf = this.sendBuf.subarray(Math.min(drop, this.sendBuf.length));
          this.ackedOffset = cum;
          this.wakeWriters();
        }
        break;
      }
      case FrameType.Ping:
        slot.link?.send(encodeNonce(FrameType.Pong, frame.nonce!));
        break;
      case FrameType.Pong:
        break;
    }
  }

  /** Writes bytes, awaiting the send window when it is full. */
  async write(bytes: Uint8Array): Promise<void> {
    if (this.closed) throw new Error('multipath: redundant stream closed');
    let sent = 0;
    while (sent < bytes.length) {
      await this.awaitWindow();
      if (this.closed) throw new Error('multipath: redundant stream closed');
      const room = this.opt.window - this.inFlight();
      const n = Math.min(bytes.length - sent, room);
      const chunk = bytes.subarray(sent, sent + n);
      this.append(chunk);
      // Fan the new bytes out to every live link, in maxSegment frames from their absolute offset.
      let off = this.sendOffset - BigInt(n);
      for (let p = 0; p < n; p += MAX_SEGMENT) {
        const seg = chunk.subarray(p, Math.min(p + MAX_SEGMENT, n));
        const framed = encodeData(off, seg);
        for (const slot of this.slots) slot.link?.send(framed);
        off += BigInt(seg.length);
      }
      sent += n;
    }
  }

  private append(chunk: Uint8Array): void {
    const next = new Uint8Array(this.sendBuf.length + chunk.length);
    next.set(this.sendBuf);
    next.set(chunk, this.sendBuf.length);
    this.sendBuf = next;
    this.sendOffset += BigInt(chunk.length);
  }

  // replay resends the whole unacknowledged tail to one freshly (re)connected link, so a dropped
  // link resumes with no gap. The receiver de-duplicates by offset, so re-sending is harmless.
  private replay(slot: LinkSlot): void {
    if (this.sendBuf.length === 0) return;
    let off = this.ackedOffset;
    for (let p = 0; p < this.sendBuf.length; p += MAX_SEGMENT) {
      const seg = this.sendBuf.subarray(p, Math.min(p + MAX_SEGMENT, this.sendBuf.length));
      slot.link?.send(encodeData(off, seg));
      off += BigInt(seg.length);
    }
  }

  private inFlight(): number {
    return Number(this.sendOffset - this.ackedOffset);
  }

  private awaitWindow(): Promise<void> {
    if (this.inFlight() < this.opt.window) return Promise.resolve();
    return new Promise((resolve) => this.windowWaiters.push(resolve));
  }

  private wakeWriters(): void {
    const waiters = this.windowWaiters;
    this.windowWaiters = [];
    for (const w of waiters) w();
  }

  private scheduleAck(): void {
    if (this.ackScheduled) return;
    this.ackScheduled = true;
    setTimeout(() => {
      this.ackScheduled = false;
      const ack = encodeAck(this.recvOffset);
      for (const slot of this.slots) slot.link?.send(ack);
    }, this.opt.ackIntervalMs);
  }

  private tick(): void {
    const now = this.now();
    for (let i = 0; i < this.slots.length; i++) {
      const slot = this.slots[i];
      if (slot.link === null) continue;
      if (now - slot.lastSeen > this.opt.deadAfterMs) {
        const dead = slot.link;
        slot.link = null;
        dead.close();
        this.scheduleReconnect(i);
        continue;
      }
      slot.link.send(encodeNonce(FrameType.Ping, BigInt(i)));
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.pingTimer !== null) clearInterval(this.pingTimer);
    for (const slot of this.slots) {
      if (slot.reconnectTimer !== null) clearTimeout(slot.reconnectTimer);
      slot.link?.close();
    }
    this.wakeWriters();
    this.onOpenResolve?.();
  }
}
