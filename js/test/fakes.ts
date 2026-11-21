// Test doubles shared by the unit tests: a WebSocket that plays the server's half of a link in
// memory, so links, sessions and clients can be driven frame by frame without any socket.
//
//   - FakeWebSocket: what Link.dial and Client take as their WebSocket. Each instance opens on the
//     next microtask, or FakeWebSocket.openDelayMs later when that is set (a slow connection setup); FakeWebSocket.answer(hello, ws) decides how the "server" answers a HELLO (a
//     WELCOME frame, a REJECT frame, or null for silence); every frame the client sends after the
//     HELLO is kept in `sent`, and receive(frame) delivers a frame to the client. bufferedAmount can
//     be set, and stuck makes every send stay in it, to simulate a socket that does not drain. FakeWebSocket.all lists every instance.
//   - welcomeFor(limits, created) is a WELCOME frame; frames(ws, type) the frames of one type that
//     the client sent; dialFake(limits) dials a link over a fresh FakeWebSocket.
//
//     const { link, ws } = await dialFake(limits);
//     ws.receive(encodeAck(0n));
import { Link } from '../src/link.js';
import {
  FrameType,
  IMPLEMENTATION,
  Mode,
  VERSION,
  decodeHello,
  encodeWelcome,
  parseFrame,
  type HelloFrame,
  type Limits,
} from '../src/protocol.js';

export function welcomeFor(limits: Limits, created: boolean): Uint8Array {
  return encodeWelcome({ version: VERSION, created, limits, impl: 'fake-server/1' });
}

export class FakeWebSocket {
  static all: FakeWebSocket[] = [];
  static answer: (hello: HelloFrame, ws: FakeWebSocket) => Uint8Array | null = () => null;
  static openDelayMs = 0;

  binaryType = '';
  bufferedAmount = 0;
  stuck = false; // every send then stays in bufferedAmount
  closed = false;
  hello: HelloFrame | null = null;
  sent: Uint8Array[] = [];
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onclose: ((ev: { code?: number }) => void) | null = null;

  constructor(readonly url: string) {
    FakeWebSocket.all.push(this);
    const open = () => {
      if (!this.closed) this.onopen?.({});
    };
    if (FakeWebSocket.openDelayMs > 0) setTimeout(open, FakeWebSocket.openDelayMs);
    else queueMicrotask(open);
  }

  send(data: Uint8Array): void {
    if (this.closed) throw new Error('send on a closed socket');
    if (this.hello === null) {
      const { payload } = parseFrame(data, 65535);
      this.hello = decodeHello(payload);
      const reply = FakeWebSocket.answer(this.hello, this);
      if (reply) queueMicrotask(() => this.receive(reply));
      return;
    }
    this.sent.push(data.slice());
    if (this.stuck) this.bufferedAmount += data.length;
  }

  receive(frame: Uint8Array): void {
    if (!this.closed) this.onmessage?.({ data: frame.slice().buffer });
  }

  close(): void {
    this.closed = true;
  }

  // drop makes the socket close by itself, as a network failure would.
  drop(): void {
    if (this.closed) return;
    this.closed = true;
    this.onclose?.({ code: 1006 });
  }
}

export function frames(ws: FakeWebSocket, type: number): Uint8Array[] {
  return ws.sent.filter((f) => f[0] === type).map((f) => f.subarray(3));
}

export async function dialFake(
  limits: Limits,
  index = 0,
): Promise<{ link: Link; ws: FakeWebSocket }> {
  FakeWebSocket.answer = () => welcomeFor(limits, true);
  const hello = {
    version: VERSION,
    session: new Uint8Array(16),
    link: index,
    mode: Mode.New,
    impl: IMPLEMENTATION,
  };
  const { link } = await Link.dial('ws://fake/link', hello, FakeWebSocket, 1000, 1000);
  return { link, ws: FakeWebSocket.all[FakeWebSocket.all.length - 1] };
}

export { FrameType };
