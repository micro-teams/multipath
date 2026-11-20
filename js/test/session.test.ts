// The session and its links (src/session.ts, src/link.ts) against an in-memory server half: the
// replay on attach and its order, the reorder buffer and its bounds, ACKs only on change, the three
// liveness rules, the bounded send queue, and the end of a session with its last link or the peer's
// GOODBYE.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Session } from '../src/session.js';
import {
  FrameType,
  MIN_WINDOW,
  decodeData,
  decodeU64,
  defaultLimits,
  encodeAck,
  encodeData,
  encodeGoodbye,
  type Limits,
} from '../src/protocol.js';
import { FakeWebSocket, dialFake, frames } from './fakes.js';

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
    onEnd: (why) => (ended = why),
  });
  return { s, delivered, events, ended: () => ended };
}

const tick = () => new Promise((r) => setTimeout(r, 0));

beforeEach(() => {
  FakeWebSocket.all = [];
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

  it('pings every pingMs and drops a silent link after clientDeadMs, ending the session', async () => {
    vi.useFakeTimers();
    const { s, events, ended } = newSession();
    const { link, ws } = await dialFakeWithFakeTimers();
    s.attach(0, link);
    await vi.advanceTimersByTimeAsync(900);
    expect(frames(ws, FrameType.Ping).length).toBe(2);
    await vi.advanceTimersByTimeAsync(200);
    expect(events.at(-1)).toBe('0 down no inbound byte for 1000 ms');
    expect(ended()).toMatch(/^all links are dead, the last one: no inbound byte/);
    expect(s.ended).toBe(true);
  });

  it('drops a link whose acknowledgement does not advance while data is outstanding', async () => {
    vi.useFakeTimers();
    const { s, events } = newSession();
    const a = await dialFakeWithFakeTimers();
    s.attach(0, a.link);
    const b = await dialFakeWithFakeTimers(1);
    s.attach(1, b.link);
    s.send(bytes(100), false);
    for (let t = 0; t < 3200; t += 100) {
      // Both links keep talking, but only b acknowledges.
      a.ws.receive(encodeData(0n, bytes(1)));
      b.ws.receive(encodeAck(100n));
      await vi.advanceTimersByTimeAsync(100);
    }
    expect(events).toContain(
      '0 down acknowledgement stalled for 3000 ms with 100 bytes outstanding',
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
    for (let t = 0; t < 1200; t += 100) {
      for (const l of [stuck, full, live]) l.ws.receive(encodeAck(0n));
      await vi.advanceTimersByTimeAsync(100);
    }
    expect(events).toContain('1 down send queue full');
    expect(events.some((e) => e.startsWith('0 down write stuck for 1000 ms'))).toBe(true);
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
