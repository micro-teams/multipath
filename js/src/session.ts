// The session (PROTOCOL.md section 5): several links joined into one reliable, ordered, duplex byte
// stream, the client's half of what the Go package `session` does on both ends. Every byte is sent on
// every live link that answers; the receiver keeps the first copy of each byte, in order, and drops
// the rest. As long as one link works the session works, and the session outlives its links: through
// an outage of every link its owner redials into it, until the server says it no longer holds it or
// no link has answered for serverDeadMs (by then the server has ended it too). When it ends,
// everything it held is released at once.
//
// The session does not dial or handshake: the client brings each link up to its WELCOME and then
// attaches it. On attach the link first gets the current ACK and the replay of every unacknowledged
// byte, queued before the link is published, so no newer frame can overtake the replay. Frames ahead
// of a gap wait in a bounded reorder buffer. ACKs go out only when the delivered offset has changed:
// on the next tick, or at once when a quarter of the window is unacknowledged.
//
// Liveness is judged for the session as a whole. A tick every 50 ms flushes ACKs, and every pingMs
// sends one PING round on all links at once. A link answers a round with any inbound byte after its
// PING (the PONG itself may wait behind data the server queued earlier). A link that has not answered
// the round sent clientDeadMs ago is soft down: it gets no new DATA but stays attached, and when it
// answers again it first receives the DATA it missed, so no byte is stranded on it. When no link
// answers, the session is silent and onSilent is called once, for the owner to redial every line
// into this session. A link is closed after max(LINK_DEAD_MIN_MS, LINK_DEAD_RTTS x its smoothed PONG
// round trip) without an inbound byte or with its writes stuck, after progressMs without ACK
// progress while it carries unacknowledged data, or when its send queue is full.
//
//   - SessionOptions: the limits, and the callbacks. deliver(bytes) receives the in-order bytes; if it
//     throws, the session ends with a GOODBYE naming the violation (the mux reports violations this
//     way). onLink(index, up, why) reports each link coming up or going down (a link replaced by a
//     new one at its index is not reported going down); onSoft(index, soft) a link going soft down or
//     answering again; onSilent() that no link answers, once each time the session falls silent;
//     onEnd(why) the end, once. PEER_GOODBYE starts the reason of a session the server said goodbye
//     to.
//
//   - new Session(opt) creates a session with no links. Bytes sent before the first attach wait in
//     the send buffer and are replayed onto the first link.
//
//   - session.setLimits(l) replaces the limits before the first attach (the client does so with the
//     server's WELCOME). session.limits is the limits in force.
//
//   - session.attach(index, link) adds a link whose handshake is done, replacing the link with the
//     same index; it throws once the session has ended.
//
//   - session.send(bytes, control) appends 1 to MAX_DATA bytes as one DATA frame on every link. Data
//     must fit the window (check fits(n), or await room(n)); a control send (mux SYN, FIN, RST,
//     WINDOW) may run past it by at most CONTROL_SLACK, beyond which the peer is not acknowledging
//     and the session ends. Throws SessionEndedError once the session has ended.
//
//   - session.fits(n) says whether n data bytes fit the window now; session.room(n) resolves once
//     they do, or the session has ended.
//
//   - session.detach(index, why) closes one link as if it had died.
//
//   - session.linkState(index) says whether a link is attached at index and whether it answers
//     (attached and not soft down).
//
//   - session.end(why) ends the session at once; session.goodbye(why) first sends GOODBYE on every
//     link and resolves once it has left, or after half a second.
//
//   - session.ended and session.reason say whether and why the session ended.
//
//   - session.stats() is a snapshot of the offsets and of every live link (its last byte, bytes in
//     and out, queued bytes, soft state and smoothed round trip), for status views.
//
//     const s = new Session({ limits, deliver: (b) => mux.feed(b), onEnd: (why) => mux.close(why) });
//     s.attach(0, link);
//     if (s.fits(p.length)) s.send(p, false);
import type { Link } from './link.js';
import {
  CONTROL_SLACK,
  DATA_OVERHEAD,
  FrameType,
  LINK_DEAD_MIN_MS,
  LINK_DEAD_RTTS,
  MAX_DATA,
  decodeData,
  decodeReason,
  decodeU64,
  encodeAck,
  encodeData,
  encodeGoodbye,
  encodePing,
  encodePong,
  parseFrame,
  type Limits,
} from './protocol.js';

export const PEER_GOODBYE = 'the peer said goodbye: ';

export class SessionEndedError extends Error {
  constructor(reason: string) {
    super(`multipath: session ended: ${reason}`);
  }
}

export interface SessionOptions {
  limits: Limits;
  deliver(bytes: Uint8Array): void;
  onLink?(index: number, up: boolean, why: string): void;
  onSoft?(index: number, soft: boolean): void;
  onSilent?(): void;
  onEnd?(why: string): void;
}

export interface SessionStats {
  sendNext: number;
  sendBase: number;
  deliverNext: number;
  reorder: number;
  links: {
    index: number;
    lastIn: number;
    bytesIn: number;
    bytesOut: number;
    queued: number;
    soft: boolean;
    rttMs: number;
  }[];
}

interface Attached {
  link: Link;
  index: number;
  ackSeen: number; // highest ACK received on this link
  sentEnd: number; // offset after the last DATA queued on it
  progressAt: number; // when ackSeen last advanced, or data became outstanding
  soft: boolean; // missed the due round; gets no new DATA until it answers
  srtt: number; // smoothed PONG round trip in ms, 0 before the first
}

const TICK_MS = 50;
// Offsets beyond this cannot be held exactly in a number; no session gets near it.
const MAX_OFFSET = Number.MAX_SAFE_INTEGER;

export class Session {
  limits: Limits;
  ended = false;
  reason = '';

  private links: (Attached | null)[];
  private live = false;
  private sendBase = 0;
  private sendNext = 0;
  private unacked: { off: number; data: Uint8Array }[] = [];
  private unackedHead = 0;
  private lastAck = 0;
  private deliverNext = 0;
  private reorder = new Map<number, Uint8Array>();
  private reorderBytes = 0;
  private waiters: (() => void)[] = [];
  private timer: ReturnType<typeof setInterval>;
  private rounds: number[] = []; // send times of the PING rounds not older than the due one
  private nextRound = 0;
  private silent = false; // no attached link answered the due round; onSilent was called
  private heardAt = 0; // the last inbound byte on any link

  constructor(private opt: SessionOptions) {
    this.limits = opt.limits;
    this.links = Array.from({ length: opt.limits.maxLinks }, () => null);
    this.timer = setInterval(() => this.tick(Date.now()), TICK_MS);
  }

  setLimits(l: Limits): void {
    if (this.live) throw new Error('multipath: limits are fixed once a link is attached');
    this.limits = l;
    this.links = Array.from({ length: l.maxLinks }, () => null);
    this.wake();
  }

  attach(index: number, link: Link): void {
    if (this.ended) {
      link.close();
      throw new SessionEndedError(this.reason);
    }
    if (index < 0 || index >= this.links.length) {
      link.close();
      throw new Error(`multipath: link index ${index} outside 0..${this.links.length - 1}`);
    }
    const now = Date.now();
    const a: Attached = {
      link,
      index,
      ackSeen: this.sendBase,
      sentEnd: this.sendNext,
      progressAt: now,
      soft: false,
      srtt: 0,
    };
    link.start(
      { onMessage: (m) => this.handle(a, m), onClose: (why) => this.drop(a, why) },
      this.limits.window + 2 * CONTROL_SLACK,
      () => this.sendBase,
    );
    let fits = link.send(encodeAck(BigInt(this.deliverNext)), 0);
    for (let i = this.unackedHead; fits && i < this.unacked.length; i++) {
      const { off, data } = this.unacked[i];
      const skip = Math.max(0, this.sendBase - off);
      fits = link.send(encodeData(BigInt(off + skip), data.subarray(skip)), off + data.length);
    }
    if (!fits) {
      // The queue holds a whole window plus slack, so this means the limits are inconsistent.
      link.close();
      throw new Error(`multipath: the replay does not fit link ${index}'s send queue`);
    }
    const old = this.links[index];
    this.links[index] = a;
    this.live = true;
    this.heardAt = now;
    if (old) this.drop(old, 'replaced by a new link');
    this.opt.onLink?.(index, true, '');
  }

  detach(index: number, why: string): void {
    const a = this.links[index];
    if (a) this.drop(a, why);
  }

  linkState(index: number): { attached: boolean; answering: boolean } {
    const a = this.links[index] ?? null;
    return { attached: a !== null, answering: a !== null && !a.soft };
  }

  private drop(a: Attached, why: string): void {
    if (!a.link.close()) return;
    if (this.links[a.index] !== a || this.ended) return;
    this.links[a.index] = null;
    this.opt.onLink?.(a.index, false, why);
  }

  end(why: string): void {
    void this.finish(why, false);
  }

  goodbye(why: string): Promise<void> {
    return this.finish(why, true);
  }

  private async finish(why: string, goodbye: boolean): Promise<void> {
    if (this.ended) return;
    this.ended = true;
    this.reason = why;
    clearInterval(this.timer);
    const links = this.links.filter((a): a is Attached => a !== null);
    this.links.fill(null);
    this.unacked = [];
    this.reorder.clear();
    this.wake();
    this.opt.onEnd?.(why);
    if (goodbye) {
      for (const a of links) a.link.send(encodeGoodbye(why), 0);
      const deadline = Date.now() + 500;
      while (Date.now() < deadline && links.some((a) => a.link.queued() > 0)) {
        await new Promise((r) => setTimeout(r, 10));
      }
    }
    for (const a of links) a.link.close();
  }

  fits(n: number): boolean {
    return this.sendNext - this.sendBase + n <= this.limits.window;
  }

  async room(n: number): Promise<void> {
    while (!this.ended && !this.fits(n)) await new Promise<void>((r) => this.waiters.push(r));
  }

  private wake(): void {
    const w = this.waiters;
    this.waiters = [];
    for (const r of w) r();
  }

  send(p: Uint8Array, control: boolean): void {
    if (p.length === 0 || p.length > MAX_DATA) {
      throw new Error(`multipath: send of ${p.length} bytes, want 1 to ${MAX_DATA}`);
    }
    if (this.ended) throw new SessionEndedError(this.reason);
    if (!control && !this.fits(p.length)) throw new Error('multipath: data sent past the window');
    const unacked = this.sendNext - this.sendBase;
    if (unacked + p.length > this.limits.window + CONTROL_SLACK) {
      this.end(`the peer has not acknowledged ${unacked} bytes`);
      throw new SessionEndedError(this.reason);
    }
    const off = this.sendNext;
    this.unacked.push({ off, data: p });
    this.sendNext += p.length;
    this.fanout(encodeData(BigInt(off), p), this.sendNext);
  }

  // fanout queues f on every live link; end is the offset after f when f is DATA, else 0. DATA skips
  // links that are soft down.
  private fanout(f: Uint8Array<ArrayBuffer>, end: number): void {
    const now = Date.now();
    for (const a of this.links) {
      if (a === null || (end > 0 && a.soft)) continue;
      if (!a.link.send(f, end)) {
        this.drop(a, 'send queue full');
        continue;
      }
      if (end > 0) {
        if (a.ackSeen >= a.sentEnd) a.progressAt = now; // the progress clock starts with outstanding data
        a.sentEnd = end;
      }
    }
  }

  private handle(a: Attached, msg: Uint8Array): void {
    if (this.ended) return;
    const why = this.handleFrame(a, msg);
    if (why) this.drop(a, why);
  }

  // handleFrame processes one message and returns a reason to drop its link, or null.
  private handleFrame(a: Attached, msg: Uint8Array): string | null {
    try {
      const { type, payload } = parseFrame(msg, this.limits.maxPayload);
      switch (type) {
        case FrameType.Data:
          return this.onData(payload);
        case FrameType.Ack:
          return this.onAck(a, decodeU64(payload));
        case FrameType.Ping:
          return a.link.send(encodePong(decodeU64(payload)), 0) ? null : 'send queue full';
        case FrameType.Pong:
          this.onPong(a, decodeU64(payload));
          return null;
        case FrameType.Goodbye:
          this.end(PEER_GOODBYE + decodeReason(payload));
          return null;
        case FrameType.Hello:
        case FrameType.Welcome:
        case FrameType.Reject:
          return `protocol violation: handshake frame 0x${type.toString(16)} on an established link`;
      }
      return null; // unknown frame types are skipped: later versions may add them
    } catch (err) {
      return `protocol violation: ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  private onData(payload: Uint8Array): string | null {
    // A copy that is already delivered is dropped before its CRC is computed: most copies are.
    if (payload.length > DATA_OVERHEAD) {
      const head = new DataView(payload.buffer, payload.byteOffset, 8).getBigUint64(0);
      if (
        head <= BigInt(MAX_OFFSET) &&
        Number(head) + payload.length - DATA_OVERHEAD <= this.deliverNext
      ) {
        return null;
      }
    }
    const { offset, data } = decodeData(payload);
    const next = this.deliverNext;
    const window = this.limits.window;
    if (offset > BigInt(next + window + CONTROL_SLACK)) {
      return `protocol violation: data at ${offset} is beyond the window (delivered ${next})`;
    }
    const off = Number(offset);
    const end = off + data.length;
    if (end > next + window + CONTROL_SLACK) {
      return `protocol violation: data up to ${end} is beyond the window (delivered ${next})`;
    }
    if (off > next) {
      const prev = this.reorder.get(off);
      if (prev && prev.length >= data.length) return null;
      const grow = data.length - (prev?.length ?? 0);
      if (this.reorderBytes + grow > 2 * window) {
        return `protocol violation: reorder buffer over ${2 * window} bytes`;
      }
      this.reorder.set(off, data.slice());
      this.reorderBytes += grow;
      return null;
    }
    if (!this.deliver(data.subarray(next - off), end)) return null;
    this.drain();
    if (!this.ended && this.deliverNext - this.lastAck >= window / 4) this.flushAck();
    return null;
  }

  // deliver hands bytes ending at end to the owner; a refusal ends the session.
  private deliver(p: Uint8Array, end: number): boolean {
    try {
      this.opt.deliver(p);
    } catch (err) {
      void this.goodbye(`protocol violation: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
    this.deliverNext = end;
    return true;
  }

  // drain delivers buffered frames that have become contiguous.
  private drain(): void {
    for (let advanced = true; advanced && !this.ended;) {
      advanced = false;
      for (const [start, buf] of this.reorder) {
        if (start > this.deliverNext) continue;
        this.reorder.delete(start);
        this.reorderBytes -= buf.length;
        const end = start + buf.length;
        if (end > this.deliverNext) {
          if (!this.deliver(buf.subarray(this.deliverNext - start), end)) return;
          advanced = true;
          break;
        }
      }
    }
  }

  private onAck(a: Attached, value: bigint): string | null {
    if (value > BigInt(this.sendNext)) {
      return `protocol violation: ack ${value} beyond the ${this.sendNext} bytes sent`;
    }
    const off = Number(value);
    if (off > a.ackSeen) {
      a.ackSeen = off;
      a.progressAt = Date.now();
    }
    if (off > this.sendBase) {
      this.sendBase = off;
      while (this.unackedHead < this.unacked.length) {
        const u = this.unacked[this.unackedHead];
        if (u.off + u.data.length > off) break;
        this.unackedHead++;
      }
      if (this.unackedHead > 1024) {
        this.unacked = this.unacked.slice(this.unackedHead);
        this.unackedHead = 0;
      }
      this.wake();
    }
    return null;
  }

  // flushAck acknowledges the delivered offset on every link if it changed since the last ACK.
  private flushAck(): void {
    if (this.ended || this.deliverNext === this.lastAck) return;
    this.lastAck = this.deliverNext;
    this.fanout(encodeAck(BigInt(this.deliverNext)), 0);
  }

  // onPong folds the round trip of a PING sent at `sent` (its nonce, in ms) into the link's smoothed
  // round trip, with the weight of RFC 6298.
  private onPong(a: Attached, sent: bigint): void {
    const elapsed = Date.now() - Number(sent);
    if (elapsed < 0 || elapsed > 60_000) return; // not a nonce this client sent; nothing to learn
    const sample = Math.max(1, elapsed); // the clock counts whole ms; 0 would read as no measurement
    a.srtt = a.srtt === 0 ? sample : a.srtt + (sample - a.srtt) / 8;
  }

  // catchUp queues on a, which answers again after being soft down, the unacknowledged bytes it was
  // not given meanwhile; it returns false when they do not fit its queue.
  private catchUp(a: Attached, now: number): boolean {
    const from = Math.max(a.sentEnd, this.sendBase);
    if (from >= this.sendNext) return true;
    if (a.ackSeen >= a.sentEnd) a.progressAt = now; // the progress clock starts with outstanding data
    for (let i = this.unackedHead; i < this.unacked.length; i++) {
      const { off, data } = this.unacked[i];
      if (off + data.length <= from) continue;
      const skip = Math.max(0, from - off);
      if (!a.link.send(encodeData(BigInt(off + skip), data.subarray(skip)), off + data.length)) {
        return false;
      }
    }
    a.sentEnd = this.sendNext;
    return true;
  }

  // dueRound returns when the newest PING round at least clientDeadMs old was sent, or 0, and forgets
  // the older rounds.
  private dueRound(now: number): number {
    let n = -1;
    while (n + 1 < this.rounds.length && now - this.rounds[n + 1] >= this.limits.clientDeadMs) n++;
    if (n < 0) return 0;
    this.rounds.splice(0, n);
    return this.rounds[0];
  }

  private tick(now: number): void {
    this.flushAck();
    if (this.ended) return;
    const l = this.limits;
    const due = this.dueRound(now);
    const round = now >= this.nextRound;
    if (round) {
      this.nextRound = now + l.pingMs;
      this.rounds.push(now);
    }
    let attached = 0;
    let answering = 0;
    for (const a of this.links) {
      if (a === null) continue;
      attached++;
      this.heardAt = Math.max(this.heardAt, a.link.lastIn);
      const deadMs = Math.max(LINK_DEAD_MIN_MS, LINK_DEAD_RTTS * a.srtt);
      let why: string | null = null;
      if (now - a.link.lastIn > deadMs) {
        why = `no inbound byte for ${Math.round(deadMs)} ms`;
      } else if (a.ackSeen < a.sentEnd && now - a.progressAt > l.progressMs) {
        why = `acknowledgement stalled for ${l.progressMs} ms with ${a.sentEnd - a.ackSeen} bytes outstanding`;
      } else {
        why = a.link.check(now, Math.round(deadMs));
      }
      if (why === null) {
        const soft = a.link.lastIn < due;
        if (soft !== a.soft) {
          a.soft = soft;
          if (!soft && !this.catchUp(a, now)) why = 'send queue full';
          this.opt.onSoft?.(a.index, soft);
        }
      }
      if (why === null && round && !a.link.send(encodePing(BigInt(now)), 0)) {
        why = 'send queue full';
      }
      if (why !== null) this.drop(a, why);
      else if (!a.soft) answering++;
      if (this.ended) return;
    }
    const silent = attached > 0 && answering === 0;
    const tell = silent && !this.silent;
    this.silent = silent;
    if (tell) this.opt.onSilent?.();
    if (this.live && !this.ended && now - this.heardAt > l.serverDeadMs) {
      // The server ends a session whose links were all silent for its dead time; holding it longer
      // would only keep streams waiting for nothing.
      this.end(
        `no line has answered for ${l.serverDeadMs} ms, so the server has ended the session`,
      );
    }
  }

  stats(): SessionStats {
    return {
      sendNext: this.sendNext,
      sendBase: this.sendBase,
      deliverNext: this.deliverNext,
      reorder: this.reorderBytes,
      links: this.links
        .filter((a): a is Attached => a !== null)
        .map((a) => ({
          index: a.index,
          lastIn: a.link.lastIn,
          bytesIn: a.link.bytesIn,
          bytesOut: a.link.bytesOut,
          queued: a.link.queued(),
          soft: a.soft,
          rttMs: Math.ceil(a.srtt),
        })),
    };
  }
}
