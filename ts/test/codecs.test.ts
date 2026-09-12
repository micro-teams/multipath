import { describe, expect, it } from 'vitest';
import {
  FrameReader,
  FrameType,
  encodeAck,
  encodeData,
  encodeHello,
  encodeNonce,
} from '../src/frames.js';
import { encodeHeader, readHeader } from '../src/header.js';

// Feeds bytes to a FrameReader one byte at a time — the worst case a WebSocket can hand it — and
// collects every frame that falls out, proving reassembly across arbitrary chunk boundaries.
function drainByteByByte(bytes: Uint8Array) {
  const r = new FrameReader();
  const frames = [];
  for (const b of bytes) {
    r.push(Uint8Array.of(b));
    let f;
    while ((f = r.next()) !== null) frames.push(f);
  }
  return frames;
}

describe('redundant frames', () => {
  it('round-trips every frame type, reassembled byte by byte', () => {
    const connID = new Uint8Array(16).map((_, i) => i + 1);
    const payload = new Uint8Array(300).map((_, i) => i & 0xff); // spans the u16 length path
    const wire = new Uint8Array([
      ...encodeHello(connID, 7),
      ...encodeData(1234567890123n, payload),
      ...encodeAck(999n),
      ...encodeNonce(FrameType.Ping, 42n),
      ...encodeNonce(FrameType.Pong, 43n),
    ]);
    const frames = drainByteByByte(wire);
    expect(frames).toHaveLength(5);
    expect(frames[0]).toMatchObject({ type: FrameType.Hello, linkIdx: 7 });
    expect((frames[0] as { connID: Uint8Array }).connID).toEqual(connID);
    expect(frames[1]).toMatchObject({ type: FrameType.Data, offset: 1234567890123n });
    expect((frames[1] as { payload: Uint8Array }).payload).toEqual(payload);
    expect(frames[2]).toMatchObject({ type: FrameType.Ack, cumulative: 999n });
    expect(frames[3]).toMatchObject({ type: FrameType.Ping, nonce: 42n });
    expect(frames[4]).toMatchObject({ type: FrameType.Pong, nonce: 43n });
  });

  it('detects a corrupt DATA payload', () => {
    const wire = encodeData(0n, Uint8Array.of(1, 2, 3, 4));
    wire[wire.length - 1] ^= 0xff; // flip a payload byte; CRC no longer matches
    const r = new FrameReader();
    r.push(wire);
    expect(() => r.next()).toThrow(/corrupt/);
  });

  it('computes IEEE CRC32 (pinned to the Go/JVM peers)', () => {
    // crc32("123456789") === 0xCBF43926, the canonical IEEE check value.
    const data = new TextEncoder().encode('123456789');
    const wire = encodeData(0n, data);
    const crc = new DataView(wire.buffer).getUint32(11);
    expect(crc >>> 0).toBe(0xcbf43926);
  });
});

describe('L5 header', () => {
  it('round-trips service and ticket, stopping at the boundary', () => {
    for (const h of [
      { service: 'anthropic' },
      { service: 'greeter', ticket: Uint8Array.of(9, 8, 7) },
    ]) {
      const wire = new Uint8Array([...encodeHeader(h), 0x58]); // a payload byte follows
      const out = readHeader(wire);
      expect(out).not.toBeNull();
      expect(out!.header.service).toBe(h.service);
      expect(wire[out!.consumed]).toBe(0x58); // consumed exactly the header
    }
  });

  it('asks for more bytes when the header is incomplete', () => {
    const wire = encodeHeader({ service: 'echo' });
    expect(readHeader(wire.subarray(0, wire.length - 1))).toBeNull();
  });
});
