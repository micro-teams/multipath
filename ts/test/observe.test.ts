import { describe, expect, it } from 'vitest';

import { LinkState, RedundantStream } from '../src/redundant.js';

// A minimal fake WebSocket that lets the test drive open/close by hand, so link up/down transitions
// can be observed without a real server. It records what was sent and exposes open()/drop() helpers.
class FakeWS {
  static instances: FakeWS[] = [];
  binaryType = '';
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: ArrayBuffer }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  closed = false;

  constructor(public url: string) {
    FakeWS.instances.push(this);
  }

  send(_data: Uint8Array): void {}

  close(): void {
    this.closed = true;
  }

  open(): void {
    this.onopen?.();
  }

  drop(): void {
    this.onclose?.();
  }
}

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('redundant link observability', () => {
  it('reports up, down (with reason) and recovery through onLinkState and stats', async () => {
    FakeWS.instances = [];
    const events: LinkState[] = [];
    const rs = new RedundantStream({
      urls: ['ws://a', 'ws://b'],
      pingIntervalMs: 100_000, // don't let the reaper tick during the test
      deadAfterMs: 100_000,
      reconnectDelayMs: 5,
      onLinkState: (e) => events.push(e),
      wsCtor: FakeWS as unknown as typeof WebSocket,
    });

    // Before anything connects, both lines are "connecting".
    expect(rs.stats().map((s) => s.state)).toEqual(['connecting', 'connecting']);

    const dialed = rs.dial();
    // Two links were created; bring both up.
    expect(FakeWS.instances.length).toBe(2);
    FakeWS.instances[0].open();
    FakeWS.instances[1].open();
    await dialed;

    expect(rs.stats().every((s) => s.state === 'up')).toBe(true);
    expect(
      events
        .filter((e) => e.up)
        .map((e) => e.index)
        .sort(),
    ).toEqual([0, 1]);

    // Drop line 0: it must be reported down with a reason; line 1 stays up.
    FakeWS.instances[0].drop();
    const down = events.find((e) => e.index === 0 && !e.up);
    expect(down).toBeTruthy();
    expect(down!.reason).not.toBe('');
    expect(rs.stats()[0].state).toBe('down');
    expect(rs.stats()[1].state).toBe('up');

    // It reconnects (a fresh FakeWS); opening it flips line 0 back to up and counts a recovery.
    await wait(20);
    const reconnected = FakeWS.instances[FakeWS.instances.length - 1];
    reconnected.open();
    expect(rs.stats()[0].state).toBe('up');
    expect(rs.stats()[0].reconnects).toBeGreaterThanOrEqual(1);

    rs.close();
  });
});
