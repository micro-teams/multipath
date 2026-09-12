import { describe, expect, it } from 'vitest';

import { encodeReject } from '../src/frames.js';
import { RedundantStream } from '../src/redundant.js';

// A fake WebSocket the test drives by hand: open() fires onopen, message(bytes) delivers a frame,
// close() fires onclose.
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
  message(bytes: Uint8Array): void {
    this.onmessage?.({
      data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length),
    });
  }
}

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('redundant link rejection', () => {
  it('surfaces an origin REJECT, never retries the link, and closes with the reason', async () => {
    FakeWS.instances = [];
    const events: { index: number; up: boolean; reason: string }[] = [];
    let closeErr: Error | undefined;
    const rs = new RedundantStream({
      urls: ['ws://only'],
      pingIntervalMs: 100_000,
      deadAfterMs: 100_000,
      reconnectDelayMs: 5,
      onLinkState: (e) => events.push(e),
      wsCtor: FakeWS as unknown as typeof WebSocket,
    });
    rs.onClose = (e) => {
      closeErr = e;
    };

    const dialed = rs.dial();
    FakeWS.instances[0].open();
    await dialed;

    // The origin refuses the link with a reason.
    FakeWS.instances[0].message(encodeReject('link index 5 out of range (n=1)'));

    const down = events.find((e) => e.index === 0 && !e.up);
    expect(down).toBeTruthy();
    expect(down!.reason).toContain('origin rejected');
    expect(down!.reason).toContain('out of range');
    expect(rs.stats()[0].state).toBe('down');

    // The single link was refused → the whole stream closes with the reason (fail fast, no hang).
    expect(closeErr).toBeTruthy();
    expect(closeErr!.message).toContain('rejected');

    // It must not be retried: no new FakeWS, reconnect count stays 0.
    await wait(30);
    expect(FakeWS.instances.length).toBe(1);
    expect(rs.stats()[0].reconnects).toBe(0);
  });
});
