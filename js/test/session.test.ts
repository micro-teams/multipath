// The session and its links (src/session.ts, src/link.ts) against an in-memory server half: the
// replay on attach and its order, the reorder buffer and its bounds, ACKs only on change, the PING
// rounds, soft down and its catch-up, the silent session, the lenient dead times, the bounded send
// queue, the end of a session after the server's dead time or on the peer's GOODBYE, the two dial
// deadlines, and the reason a link's socket failed.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Session } from '../src/session.js';
import {
  FrameType,
  IMPLEMENTATION,
  MIN_WINDOW,
  Mode,
  VERSION,
  decodeData,
  decodeU64,
  defaultLimits,
  encodeAck,
  encodeData,
  encodeGoodbye,
  encodePong,
  type Limits,
} from '../src/protocol.js';
import { Link } from '../src/link.js';
import { FakeWebSocket, dialFake, frames, welcomeFor } from './fakes.js';

const limits: Limits = { ...defaultLimits(), window: MIN_WINDOW };
const bytes = (n: number, seed = 0) =>
  Uint8Array.from({ length: n }, (_, i) => (i * 7 + seed) & 0xff);

function newSession() {
  const delivered: number[] = [];
  const events: string[] = [];
  let ended = '';
  const s = new Session({
    limits,
    deliver: (p) => delivered.push(...p),
    onLink: (i, up, why) => events.push(`${i} ${up ? 'up' : 'down'} ${why}`),
    onSoft: (i, soft) => events.push(`${i} ${soft ? 'soft' : 'answers'}`),
    onSilent: () => events.push('silent'),
    onEnd: (why) => (ended = why),
  });
  return { s, delivered, events, ended: () => ended };
}

const tick = () => new Promise((r) => setTimeout(r, 0));

beforeEach(() => {
  FakeWebSocket.all = [];
  FakeWebSocket.openDelayMs = 0;
});
afterEach(() => {
  vi.useRealTimers();
});

describe('session', () => {
  it('replays unacknowledged bytes on attach, after the ACK and before any newer frame', async () => {
    const { s } = newSession();
    s.send(bytes(100), false); // before any link: waits in the send buffer
    const { link, ws } = await dialFake(limits);
    s.attach(0, link);
    s.send(bytes(10, 1), false);
    await tick();
    expect(ws.sent.map((f) => f[0])).toEqual([FrameType.Ack, FrameType.Data, FrameType.Data]);
    const [replay, newer] = frames(ws, FrameType.Data).map(decodeData);
    expect([replay.offset, replay.data.length, newer.offset]).toEqual([0n, 100, 100n]);
    ws.receive(encodeAck(110n));
    // A second link attached now gets nothing to replay.
    const second = await dialFake(limits, 1);
    s.attach(1, second.link);
    await tick();
    expect(frames(second.ws, FrameType.Data)).toEqual([]);
    expect(decodeU64(frames(second.ws, FrameType.Ack)[0])).toBe(0n);
    s.end('test over');
  });

  it('delivers in order across a gap and drops duplicates', async () => {
    const { s, delivered } = newSession();
    const { link, ws } = await dialFake(limits);
    s.attach(0, link);
    await tick();
    const all = bytes(300);
    ws.receive(encodeData(200n, all.subarray(200)));
    ws.receive(encodeData(100n, all.subarray(100, 200)));
    expect(delivered.length).toBe(0);
    ws.receive(encodeData(0n, all.subarray(0, 150))); // overlaps the buffered frame
    ws.receive(encodeData(0n, all.subarray(0, 100))); // a duplicate
    expect(delivered).toEqual([...all]);
    expect(s.stats().reorder).toBe(0);
    s.end('test over');
  });

  it('closes a link that sends data beyond the reorder bounds', async () => {
    const { s, events } = newSession();
    const { link, ws } = await dialFake(limits);
    s.attach(0, link);
    await tick();
    ws.receive(encodeData(BigInt(limits.window + 65536), bytes(10)));
    expect(events.at(-1)).toMatch(/0 down protocol violation: data up to .* beyond the window/);
    expect(ws.closed).toBe(true);
  });

  it('acknowledges only when the delivered offset changed', async () => {
    vi.useFakeTimers();
    const { s } = newSession();
    const { link, ws } = await dialFakeWithFakeTimers();
    s.attach(0, link);
    ws.receive(encodeData(0n, bytes(10)));
    await vi.advanceTimersByTimeAsync(200);
    ws.receive(encodeData(0n, bytes(10))); // a duplicate changes nothing
    await vi.advanceTimersByTimeAsync(200);
    const acks = frames(ws, FrameType.Ack).map(decodeU64);
    expect(acks).toEqual([0n, 10n]); // the one on attach, then one for the new bytes
    s.end('test over');
  });

  it('pings in rounds; a silent link goes soft down, is closed after 5 s, and the session lasts 10 s', async () => {
    vi.useFakeTimers();
    const { s, events, ended } = newSession();
    const { link, ws } = await dialFakeWithFakeTimers();
    s.attach(0, link);
    await vi.advanceTimersByTimeAsync(900);
    expect(frames(ws, FrameType.Ping).length).toBe(2);
    await vi.advanceTimersByTimeAsync(700);
    expect(events).toEqual(['0 up ', '0 soft', 'silent']);
    await vi.advanceTimersByTimeAsync(3500);
    expect(events.at(-1)).toBe('0 down no inbound byte for 5000 ms');
    expect(s.ended).toBe(false);
    await vi.advanceTimersByTimeAsync(5000);
    expect(ended()).toBe('no line has answered for 10000 ms, so the server has ended the session');
  });

  it('keeps a jittery link: soft down while it is late, then caught up on what it missed', async () => {
    vi.useFakeTimers();
    const { s, events } = newSession();
    const a = await dialFakeWithFakeTimers();
    s.attach(0, a.link);
    const b = await dialFakeWithFakeTimers(1);
    s.attach(1, b.link);
    const answer = (...links: (typeof a)[]) => {
      for (const l of links) {
        const pings = frames(l.ws, FrameType.Ping);
        if (pings.length) l.ws.receive(encodePong(decodeU64(pings.at(-1)!)));
      }
    };
    for (let t = 0; t < 1000; t += 100) {
      answer(a, b);
      await vi.advanceTimersByTimeAsync(100);
    }
    for (let t = 0; t < 2500; t += 100) {
      answer(b); // a's answers are late by 2.5 s
      await vi.advanceTimersByTimeAsync(100);
    }
    expect(events).toContain('0 soft');
    s.send(bytes(100), false);
    const data = (l: typeof a) => frames(l.ws, FrameType.Data).map((f) => decodeData(f).offset);
    expect(data(a)).toEqual([]);
    expect(data(b)).toEqual([0n]);
    answer(a, b);
    await vi.advanceTimersByTimeAsync(100);
    expect(events).toContain('0 answers');
    expect(data(a)).toEqual([0n]);
    expect(events.filter((e) => e.includes('down'))).toEqual([]);
    expect(events).not.toContain('silent');
    expect(s.stats().links.map((l) => l.rttMs > 0)).toEqual([true, true]);
    s.end('test over');
  });

  it('drops a link whose acknowledgement does not advance while data is outstanding', async () => {
    vi.useFakeTimers();
    const { s, events } = newSession();
    const a = await dialFakeWithFakeTimers();
    s.attach(0, a.link);
    const b = await dialFakeWithFakeTimers(1);
    s.attach(1, b.link);
    s.send(bytes(100), false);
    for (let t = 0; t < 10_200; t += 100) {
      // Both links keep talking, but only b acknowledges.
      a.ws.receive(encodeData(0n, bytes(1)));
      b.ws.receive(encodeAck(100n));
      await vi.advanceTimersByTimeAsync(100);
    }
    expect(events).toContain(
      '0 down acknowledgement stalled for 10000 ms with 100 bytes outstanding',
    );
    expect(s.ended).toBe(false);
    s.end('test over');
  });

  it('closes a link whose writes are stuck, or whose queue is full', async () => {
    vi.useFakeTimers();
    const { s, events } = newSession();
    const stuck = await dialFakeWithFakeTimers();
    s.attach(0, stuck.link);
    const full = await dialFakeWithFakeTimers(1);
    s.attach(1, full.link);
    const live = await dialFakeWithFakeTimers(2);
    s.attach(2, live.link);
    stuck.ws.stuck = true; // never drains
    full.ws.bufferedAmount = limits.window + 2 * 65536; // no room for one more frame
    for (let t = 0; t < 5200; t += 100) {
      for (const l of [stuck, full, live]) l.ws.receive(encodeAck(0n));
      await vi.advanceTimersByTimeAsync(100);
    }
    expect(events).toContain('1 down send queue full');
    expect(events.some((e) => e.startsWith('0 down write stuck for 5000 ms'))).toBe(true);
    expect(s.ended).toBe(false);
    s.end('test over');
  });

  it('ends at once on the peer GOODBYE, and sends GOODBYE on its own', async () => {
    const one = newSession();
    const a = await dialFake(limits);
    one.s.attach(0, a.link);
    await tick();
    a.ws.receive(encodeGoodbye('server shutting down'));
    expect(one.ended()).toBe('the peer said goodbye: server shutting down');

    const two = newSession();
    const b = await dialFake(limits);
    two.s.attach(0, b.link);
    await tick();
    await two.s.goodbye('client shutting down');
    expect(b.ws.sent.at(-1)![0]).toBe(FrameType.Goodbye);
    expect(b.ws.closed).toBe(true);
    expect(() => two.s.send(bytes(1), false)).toThrow(/session ended: client shutting down/);
  });

  it('a session ending through its deliver callback says goodbye with the violation', async () => {
    const s = new Session({
      limits,
      deliver: () => {
        throw new Error('mux syn 2 from the server');
      },
    });
    const { link, ws } = await dialFake(limits);
    s.attach(0, link);
    await tick();
    ws.receive(encodeData(0n, bytes(5)));
    expect(s.reason).toBe('protocol violation: mux syn 2 from the server');
    await vi.waitFor(() => expect(ws.closed).toBe(true));
    expect(ws.sent.at(-1)![0]).toBe(FrameType.Goodbye);
  });
});

// dialFake under fake timers: the dial's own microtasks still run, its timeout is faked.
async function dialFakeWithFakeTimers(index = 0) {
  const p = dialFake(limits, index);
  await vi.advanceTimersByTimeAsync(0);
  return p;
}

describe('a failed socket', () => {
  const hello = {
    version: VERSION,
    session: new Uint8Array(16),
    link: 0,
    mode: Mode.New,
    impl: IMPLEMENTATION,
  };

  it('times the hello from the open socket, and the connection on its own', async () => {
    vi.useFakeTimers();
    FakeWebSocket.openDelayMs = 8000; // a slow DNS, TCP, TLS and upgrade through a distant CDN
    FakeWebSocket.answer = () => null;
    const silent = Link.dial('ws://fake/link', hello, FakeWebSocket, 5000, 10_000);
    const late = expect(silent).rejects.toThrow('no answer to hello within 5000 ms');
    await vi.advanceTimersByTimeAsync(12_900);
    expect(FakeWebSocket.all[0].closed).toBe(false);
    await vi.advanceTimersByTimeAsync(200);
    await late;

    FakeWebSocket.answer = () => welcomeFor(limits, true);
    const slow = Link.dial('ws://fake/link', hello, FakeWebSocket, 5000, 10_000);
    await vi.advanceTimersByTimeAsync(8000);
    expect((await slow).welcome.created).toBe(true);

    FakeWebSocket.openDelayMs = 20_000;
    const never = Link.dial('ws://fake/link', hello, FakeWebSocket, 5000, 10_000);
    const refused = expect(never).rejects.toThrow('no connection within 10000 ms');
    await vi.advanceTimersByTimeAsync(10_100);
    await refused;
  });

  it('names what the platform said, when its error event says anything', async () => {
    FakeWebSocket.answer = () => null;
    const dialing = Link.dial('ws://fake/link', hello, FakeWebSocket, 1000, 1000);
    await tick(); // open, and the HELLO sent
    FakeWebSocket.all[FakeWebSocket.all.length - 1].onerror?.({
      message: 'SSLHandshakeException: connection closed',
    });
    await expect(dialing).rejects.toThrow(
      'connection failed: SSLHandshakeException: connection closed',
    );
  });

  it('is only "connection failed" for a browser, whose error event says nothing', async () => {
    FakeWebSocket.answer = () => null;
    const dialing = Link.dial('ws://fake/link', hello, FakeWebSocket, 1000, 1000);
    await tick(); // open, and the HELLO sent
    FakeWebSocket.all[FakeWebSocket.all.length - 1].onerror?.({});
    await expect(dialing).rejects.toThrow(/^connection failed$/);
  });

  it('names it for a live link too', async () => {
    const { link, ws } = await dialFake(limits);
    let why = '';
    link.start({ onMessage: () => {}, onClose: (r) => (why = r) }, 1 << 20, () => 0);
    await tick();
    ws.onerror?.({ message: 'Software caused connection abort' });
    expect(why).toBe('connection failed: Software caused connection abort');
  });
});
