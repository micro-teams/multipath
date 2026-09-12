import { describe, expect, it, vi } from 'vitest';

import { MultipathWebSocket, WSFrameReader } from '../src/appws.js';
import type { MuxStream } from '../src/mux.js';

describe('WSFrameReader', () => {
  it('parses a small unmasked frame in one push', () => {
    const r = new WSFrameReader();
    r.push(new Uint8Array([0x82, 0x02, 0x68, 0x69])); // FIN|binary, len 2, "hi"
    const f = r.next();
    expect(f).not.toBeNull();
    expect(f!.fin).toBe(true);
    expect(f!.opcode).toBe(0x2);
    expect([...f!.data]).toEqual([0x68, 0x69]);
    expect(r.next()).toBeNull();
  });

  it('unmasks a masked frame', () => {
    const mask = [0x01, 0x02, 0x03, 0x04];
    const payload = [0x68, 0x69, 0x21]; // "hi!"
    const masked = payload.map((b, i) => b ^ mask[i % 4]);
    const r = new WSFrameReader();
    r.push(new Uint8Array([0x82, 0x80 | 3, ...mask, ...masked]));
    const f = r.next()!;
    expect([...f.data]).toEqual(payload);
  });

  it('returns null until enough bytes have been pushed, byte by byte', () => {
    const bytes = [0x82, 0x05, 1, 2, 3, 4, 5];
    const r = new WSFrameReader();
    for (let i = 0; i < bytes.length - 1; i++) {
      r.push(new Uint8Array([bytes[i]]));
      expect(r.next()).toBeNull();
    }
    r.push(new Uint8Array([bytes[bytes.length - 1]]));
    const f = r.next()!;
    expect([...f.data]).toEqual([1, 2, 3, 4, 5]);
  });

  it('decodes the 16-bit extended length', () => {
    const payload = new Uint8Array(300).fill(7);
    const r = new WSFrameReader();
    r.push(new Uint8Array([0x82, 126, 300 >> 8, 300 & 0xff]));
    r.push(payload);
    const f = r.next()!;
    expect(f.data.length).toBe(300);
    expect(f.data.every((b) => b === 7)).toBe(true);
  });

  it('parses two frames queued back to back', () => {
    const r = new WSFrameReader();
    r.push(new Uint8Array([0x82, 0x01, 0x41, 0x82, 0x01, 0x42])); // "A" then "B"
    expect([...r.next()!.data]).toEqual([0x41]);
    expect([...r.next()!.data]).toEqual([0x42]);
    expect(r.next()).toBeNull();
  });
});

/** A fake MuxStream: write() records every call; read() serves a pre-queued list of responses (a
 * chunk, or null for EOF), one per call, blocking on a promise if the queue runs dry so a test can
 * feed a response after the client has already sent its request. */
class FakeStream {
  writes: Uint8Array[] = [];
  private queue: Array<Uint8Array | null> = [];
  private waiters: Array<(v: Uint8Array | null) => void> = [];

  write(bytes: Uint8Array): Promise<void> {
    this.writes.push(bytes);
    return Promise.resolve();
  }

  read(): Promise<Uint8Array | null> {
    if (this.queue.length > 0) return Promise.resolve(this.queue.shift()!);
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  push(chunk: Uint8Array | null): void {
    const waiter = this.waiters.shift();
    if (waiter) waiter(chunk);
    else this.queue.push(chunk);
  }

  reset(): void {}
}

const encoder = new TextEncoder();

async function acceptedHandshakeBytes(fake: FakeStream): Promise<{ key: string }> {
  // Wait a tick for the client to have written its request, then parse the Sec-WebSocket-Key so the
  // test can compute a valid Sec-WebSocket-Accept, exactly like a real server would.
  await Promise.resolve();
  await Promise.resolve();
  const req = new TextDecoder().decode(fake.writes[0]);
  const m = /Sec-WebSocket-Key: (\S+)/.exec(req);
  if (!m) throw new Error('no Sec-WebSocket-Key in request: ' + req);
  return { key: m[1] };
}

async function wsAccept(key: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    'SHA-1',
    encoder.encode(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'),
  );
  return btoa(String.fromCharCode(...new Uint8Array(digest)));
}

describe('MultipathWebSocket', () => {
  it('performs the handshake, delivers a message, and answers a ping without surfacing it', async () => {
    const fake = new FakeStream();
    const openPromise = MultipathWebSocket.open(fake as unknown as MuxStream, '/chat');

    const { key } = await acceptedHandshakeBytes(fake);
    const accept = await wsAccept(key);
    fake.push(
      encoder.encode(
        `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
          `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
      ),
    );

    const ws = await openPromise;
    expect(ws.readyState).toBe(MultipathWebSocket.OPEN);

    const onmessage = vi.fn();
    ws.onmessage = onmessage;

    fake.push(new Uint8Array([0x89, 0x04, 0x70, 0x69, 0x6e, 0x67])); // unmasked ping "ping"
    fake.push(new Uint8Array([0x81, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f])); // unmasked text "hello"

    await new Promise((r) => setTimeout(r, 10));

    expect(onmessage).toHaveBeenCalledTimes(1);
    expect(onmessage.mock.calls[0][0].data).toBe('hello');

    // A pong echoing the ping's own payload, and nothing else, must have followed the handshake
    // request. The client always masks, so unmask before comparing.
    const pong = fake.writes[1];
    expect(pong[0]).toBe(0x8a); // FIN|pong
    const mask = pong.slice(2, 6);
    const masked = pong.slice(6, 10);
    const unmasked = [...masked].map((b, i) => b ^ mask[i]);
    expect(new TextDecoder().decode(new Uint8Array(unmasked))).toBe('ping');
  });

  it('send() masks and frames text as one complete message', async () => {
    const fake = new FakeStream();
    const openPromise = MultipathWebSocket.open(fake as unknown as MuxStream, '/chat');
    const { key } = await acceptedHandshakeBytes(fake);
    const accept = await wsAccept(key);
    fake.push(
      encoder.encode(
        `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
          `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
      ),
    );
    const ws = await openPromise;

    ws.send('hi');
    await new Promise((r) => setTimeout(r, 10));

    const frame = fake.writes[1];
    expect(frame[0]).toBe(0x81); // FIN|text
    expect(frame[1] & 0x80).toBe(0x80); // masked
    const len = frame[1] & 0x7f;
    expect(len).toBe(2);
    const mask = frame.slice(2, 6);
    const masked = frame.slice(6, 8);
    const unmasked = [...masked].map((b, i) => b ^ mask[i]);
    expect(new TextDecoder().decode(new Uint8Array(unmasked))).toBe('hi');
  });

  it('calls onclose when the stream reaches EOF', async () => {
    const fake = new FakeStream();
    const openPromise = MultipathWebSocket.open(fake as unknown as MuxStream, '/chat');
    const { key } = await acceptedHandshakeBytes(fake);
    const accept = await wsAccept(key);
    fake.push(
      encoder.encode(
        `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
          `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
      ),
    );
    const ws = await openPromise;
    const onclose = vi.fn();
    ws.onclose = onclose;

    fake.push(null); // EOF
    await new Promise((r) => setTimeout(r, 10));

    expect(onclose).toHaveBeenCalledTimes(1);
    expect(ws.readyState).toBe(MultipathWebSocket.CLOSED);
  });
});
