// The client mux (src/mux.ts) over a recording transport: stream ids and the extra receive credit,
// credit enforced on what the server sends, WINDOW in batches, writes that wait for credit, and the
// release of a stream by FINs or RST.
import { describe, expect, it } from 'vitest';
import { Mux, type Transport } from '../src/mux.js';
import { MuxType, defaultLimits, encodeMux, parseMux } from '../src/protocol.js';

const limits = { ...defaultLimits(), streamWindow: 64 << 10 };

function recorder(): Transport & { sent: { type: number; stream: number; body: number[] }[] } {
  const sent: { type: number; stream: number; body: number[] }[] = [];
  return {
    sent,
    send: (f) => {
      const { frame } = parseMux(f, 0)!;
      sent.push({ type: frame.type, stream: frame.stream, body: [...frame.body] });
    },
    fits: () => true,
    room: async () => undefined,
  };
}

const u32 = (n: number) => [(n >>> 24) & 0xff, (n >>> 16) & 0xff, (n >>> 8) & 0xff, n & 0xff];

describe('mux', () => {
  it('opens odd, increasing streams and grants the extra receive credit at once', () => {
    const t = recorder();
    const m = new Mux(t, limits, 256 << 10);
    expect([m.open().id, m.open().id]).toEqual([1, 3]);
    expect(t.sent.slice(0, 2)).toEqual([
      { type: MuxType.Syn, stream: 1, body: [] },
      { type: MuxType.Window, stream: 1, body: u32((256 - 64) << 10) },
    ]);
  });

  it('enforces the credit it granted', () => {
    const m = new Mux(recorder(), limits, limits.streamWindow);
    m.open();
    const chunk = new Uint8Array(16384);
    for (let i = 0; i < 4; i++) m.feed(encodeMux(MuxType.Data, 1, chunk));
    expect(() => m.feed(encodeMux(MuxType.Data, 1, Uint8Array.of(1)))).toThrow(/with 0 of credit/);
  });

  it('treats a SYN from the server as a violation and skips unknown frame types', () => {
    const m = new Mux(recorder(), limits, limits.streamWindow);
    expect(() => m.feed(encodeMux(0x7f, 1, Uint8Array.of(1)))).not.toThrow();
    expect(() => m.feed(encodeMux(MuxType.Syn, 2))).toThrow(/syn 2 from the server/);
  });

  it('returns credit in batches, and frames split across feeds still arrive', async () => {
    const t = recorder();
    const m = new Mux(t, limits, limits.streamWindow);
    const st = m.open();
    const frame = encodeMux(MuxType.Data, 1, new Uint8Array(8192).fill(7));
    const two = new Uint8Array([...frame, ...frame]);
    m.feed(two.subarray(0, 5));
    m.feed(two.subarray(5));
    expect((await st.read())!.length).toBe(8192); // one of two: no WINDOW yet (a quarter is 16384)
    expect(t.sent.filter((f) => f.type === MuxType.Window)).toEqual([]);
    await st.read();
    expect(t.sent.filter((f) => f.type === MuxType.Window)).toEqual([
      { type: MuxType.Window, stream: 1, body: u32(16384) },
    ]);
  });

  it('waits for credit to write, and is released by both FINs or by RST', async () => {
    const t = recorder();
    const m = new Mux(t, limits, limits.streamWindow);
    const st = m.open();
    const done = st.write(new Uint8Array((64 << 10) + 10));
    await new Promise((r) => setTimeout(r, 0));
    const sentBytes = () =>
      t.sent.filter((f) => f.type === MuxType.Data).reduce((n, f) => n + f.body.length, 0);
    expect(sentBytes()).toBe(64 << 10);
    m.feed(encodeMux(MuxType.Window, 1, Uint8Array.from(u32(10))));
    await done;
    expect(sentBytes()).toBe((64 << 10) + 10);
    st.closeWrite();
    expect(st.released).toBe(false);
    m.feed(encodeMux(MuxType.Fin, 1));
    expect(await st.read()).toBeNull();
    expect(st.released).toBe(true);
    expect(m.streams()).toEqual([]);

    const other = m.open();
    m.feed(encodeMux(MuxType.Rst, other.id, new TextEncoder().encode('target unreachable')));
    await expect(other.read()).rejects.toThrow('stream reset by peer: target unreachable');
    expect(m.streams()).toEqual([]);
  });

  it('fails every stream when the session ends', async () => {
    const m = new Mux(recorder(), limits, limits.streamWindow);
    const st = m.open();
    m.close('all links are dead');
    await expect(st.read()).rejects.toThrow('session ended: all links are dead');
    expect(() => m.open()).toThrow('session ended');
  });
});
