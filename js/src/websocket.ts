// An application WebSocket carried over one stream: an RFC 6455 client written here, because no
// platform offers one on top of an arbitrary byte stream. The browser's WebSocket can only dial a
// real URL, and a service worker never sees WebSocket traffic. The far end is the target's own
// WebSocket server (nginx in front of the application, say), which cannot tell that its bytes travel
// over several lines.
//
// MultipathWebSocket mirrors the browser's WebSocket (readyState, protocol, onopen, onmessage,
// onerror, onclose, send, close), so `new WebSocket(url)` becomes `await client.openWebSocket(path)`.
// The stream is reset when the socket closes, which releases it on both sides.
//
//   - WSFrameReader: push(bytes) what the stream delivers, next() the next whole frame or null; a
//     masked frame is unmasked.
//   - MultipathWebSocket.open(stream, path, { host, protocols }) performs the handshake (Host, and
//     Sec-WebSocket-Protocol when protocols are given) and resolves with an open socket, or rejects
//     with why the server did not upgrade. Constants CONNECTING, OPEN, CLOSING, CLOSED.
//   - ws.send(data) sends one text (string) or binary (Uint8Array) message; a failure is reported
//     through onerror, as the browser's fire-and-forget send() would.
//   - ws.close() sends a close frame; onclose({ code, reason }) follows once the peer answers or the
//     stream ends. onmessage({ data }) gets a string for text and an ArrayBuffer for binary.
//
//     const ws = await client.openWebSocket('/mt/ws', ['v12.stomp']);
//     ws.onmessage = (ev) => console.log(ev.data);
//     ws.send('hello');
import { sha1 } from './hash.js';
import type { Stream } from './mux.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const OP_CONTINUATION = 0x0;
const OP_TEXT = 0x1;
const OP_BINARY = 0x2;
const OP_CLOSE = 0x8;
const OP_PING = 0x9;
const OP_PONG = 0xa;

interface WSFrame {
  fin: boolean;
  opcode: number;
  data: Uint8Array;
}

export class WSFrameReader {
  private buf = new Uint8Array(0);

  push(chunk: Uint8Array): void {
    const next = new Uint8Array(this.buf.length + chunk.length);
    next.set(this.buf);
    next.set(chunk, this.buf.length);
    this.buf = next;
  }

  next(): WSFrame | null {
    if (this.buf.length < 2) return null;
    const v = new DataView(this.buf.buffer, this.buf.byteOffset, this.buf.byteLength);
    const fin = (this.buf[0] & 0x80) !== 0;
    const opcode = this.buf[0] & 0x0f;
    const masked = (this.buf[1] & 0x80) !== 0;
    let length = this.buf[1] & 0x7f;
    let headerLen = 2;
    if (length === 126) {
      if (this.buf.length < 4) return null;
      length = v.getUint16(2);
      headerLen = 4;
    } else if (length === 127) {
      if (this.buf.length < 10) return null;
      length = Number(v.getBigUint64(2));
      headerLen = 10;
    }
    const maskLen = masked ? 4 : 0;
    const total = headerLen + maskLen + length;
    if (this.buf.length < total) return null;
    const data = this.buf.slice(headerLen + maskLen, total);
    if (masked) {
      const mask = this.buf.subarray(headerLen, headerLen + 4);
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i % 4];
    }
    this.buf = this.buf.slice(total);
    return { fin, opcode, data };
  }
}

/** Frames one complete, unfragmented message. A client always masks (RFC 6455 requires it). */
function encodeFrame(opcode: number, payload: Uint8Array): Uint8Array {
  const length = payload.length;
  let head: number[];
  if (length < 126) head = [0x80 | opcode, 0x80 | length];
  else if (length < 1 << 16)
    head = [0x80 | opcode, 0x80 | 126, (length >> 8) & 0xff, length & 0xff];
  else {
    head = [0x80 | opcode, 0x80 | 127, 0, 0, 0, 0];
    for (const shift of [24, 16, 8, 0]) head.push((length >>> shift) & 0xff);
  }
  const mask = crypto.getRandomValues(new Uint8Array(4));
  const out = new Uint8Array(head.length + 4 + length);
  out.set(head, 0);
  out.set(mask, head.length);
  for (let i = 0; i < length; i++) out[head.length + 4 + i] = payload[i] ^ mask[i % 4];
  return out;
}

function findDoubleCRLF(buf: Uint8Array): number {
  for (let i = 0; i + 3 < buf.length; i++) {
    if (buf[i] === 13 && buf[i + 1] === 10 && buf[i + 2] === 13 && buf[i + 3] === 10) return i;
  }
  return -1;
}

export type MultipathWebSocketState = 0 | 1 | 2 | 3;

export class MultipathWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState: MultipathWebSocketState = MultipathWebSocket.CONNECTING;
  protocol = '';
  binaryType = 'arraybuffer';
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: ArrayBuffer | string }) => void) | null = null;
  onerror: ((err: Error) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;

  private reader = new WSFrameReader();
  private writing: Promise<void> = Promise.resolve();
  private fragType = 0;
  private fragData: Uint8Array = new Uint8Array(0);
  private closeCode = 1005;
  private closeReason = '';

  private constructor(private stream: Stream) {}

  static async open(
    stream: Stream,
    path: string,
    opt: { host: string; protocols?: string[] },
  ): Promise<MultipathWebSocket> {
    const ws = new MultipathWebSocket(stream);
    try {
      await ws.handshake(path, opt.host, opt.protocols ?? []);
    } catch (err) {
      stream.reset('websocket handshake failed');
      throw err;
    }
    ws.readyState = MultipathWebSocket.OPEN;
    queueMicrotask(() => ws.onopen?.());
    void ws.readLoop();
    return ws;
  }

  private async handshake(path: string, host: string, protocols: string[]): Promise<void> {
    const key = btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(16))));
    let req =
      `GET ${path} HTTP/1.1\r\nHost: ${host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
      `Sec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n`;
    if (protocols.length) req += `Sec-WebSocket-Protocol: ${protocols.join(', ')}\r\n`;
    await this.stream.write(encoder.encode(req + '\r\n'));

    let head = new Uint8Array(0);
    let sep = -1;
    while (sep < 0) {
      if (head.length > 64 << 10) throw new Error('multipath: websocket handshake: head too large');
      const chunk = await this.stream.read();
      if (chunk === null) throw new Error('multipath: websocket handshake: stream closed');
      const next = new Uint8Array(head.length + chunk.length);
      next.set(head);
      next.set(chunk, head.length);
      head = next;
      sep = findDoubleCRLF(head);
    }
    this.reader.push(head.subarray(sep + 4)); // bytes past the head are already frames
    const lines = decoder.decode(head.subarray(0, sep)).split('\r\n');
    if (lines[0].split(' ')[1] !== '101') {
      throw new Error(`multipath: websocket handshake got "${lines[0]}"`);
    }
    const headers = new Map<string, string>();
    for (const line of lines.slice(1)) {
      const i = line.indexOf(':');
      if (i > 0) headers.set(line.slice(0, i).trim().toLowerCase(), line.slice(i + 1).trim());
    }
    const accept = btoa(String.fromCharCode(...sha1(encoder.encode(key + WS_GUID))));
    if (
      (headers.get('upgrade') ?? '').toLowerCase() !== 'websocket' ||
      headers.get('sec-websocket-accept') !== accept
    ) {
      throw new Error('multipath: websocket handshake response did not confirm the upgrade');
    }
    this.protocol = headers.get('sec-websocket-protocol') ?? '';
    if (this.protocol && !protocols.includes(this.protocol)) {
      throw new Error(`multipath: websocket server chose protocol "${this.protocol}", not offered`);
    }
  }

  private async readLoop(): Promise<void> {
    try {
      for (;;) {
        let frame = this.reader.next();
        while (frame === null) {
          const chunk = await this.stream.read();
          if (chunk === null) return this.finish();
          this.reader.push(chunk);
          frame = this.reader.next();
        }
        if (!(await this.handleFrame(frame))) return;
      }
    } catch (err) {
      if (this.readyState !== MultipathWebSocket.CLOSED) {
        this.closeCode = 1006;
        this.onerror?.(err instanceof Error ? err : new Error(String(err)));
      }
      this.finish();
    }
  }

  /** Returns false once the socket is closed. */
  private async handleFrame(frame: WSFrame): Promise<boolean> {
    switch (frame.opcode) {
      case OP_TEXT:
      case OP_BINARY:
        this.fragType = frame.opcode;
        this.fragData = frame.data;
        break;
      case OP_CONTINUATION: {
        const next = new Uint8Array(this.fragData.length + frame.data.length);
        next.set(this.fragData);
        next.set(frame.data, this.fragData.length);
        this.fragData = next;
        break;
      }
      case OP_PING:
        await this.rawSend(OP_PONG, frame.data);
        return true;
      case OP_PONG:
        return true;
      case OP_CLOSE:
        if (frame.data.length >= 2) {
          this.closeCode = (frame.data[0] << 8) | frame.data[1];
          this.closeReason = decoder.decode(frame.data.subarray(2));
        }
        if (this.readyState === MultipathWebSocket.OPEN)
          await this.rawSend(OP_CLOSE, frame.data.subarray(0, 2));
        this.finish();
        return false;
      default:
        throw new Error(`multipath: websocket: unexpected opcode ${frame.opcode}`);
    }
    if (frame.fin) {
      const d = this.fragData;
      const data =
        this.fragType === OP_TEXT
          ? decoder.decode(d)
          : (d.buffer.slice(d.byteOffset, d.byteOffset + d.length) as ArrayBuffer);
      this.fragData = new Uint8Array(0);
      this.onmessage?.({ data });
    }
    return true;
  }

  send(data: string | Uint8Array): void {
    if (this.readyState !== MultipathWebSocket.OPEN) {
      this.onerror?.(new Error('multipath: websocket send while not open'));
      return;
    }
    const text = typeof data === 'string';
    this.rawSend(text ? OP_TEXT : OP_BINARY, text ? encoder.encode(data) : data).catch((err) =>
      this.onerror?.(err instanceof Error ? err : new Error(String(err))),
    );
  }

  private rawSend(opcode: number, payload: Uint8Array): Promise<void> {
    const next = this.writing.then(() => this.stream.write(encodeFrame(opcode, payload)));
    // The chain continues after a failed write; that write's caller reports its error.
    this.writing = next.catch(() => undefined);
    return next;
  }

  close(): void {
    if (this.readyState !== MultipathWebSocket.OPEN) return;
    this.readyState = MultipathWebSocket.CLOSING;
    this.rawSend(OP_CLOSE, new Uint8Array([0x03, 0xe8])).catch((err) => {
      this.onerror?.(err instanceof Error ? err : new Error(String(err)));
      this.finish();
    });
  }

  private finish(): void {
    if (this.readyState === MultipathWebSocket.CLOSED) return;
    this.readyState = MultipathWebSocket.CLOSED;
    this.stream.reset('websocket closed');
    this.onclose?.({ code: this.closeCode, reason: this.closeReason });
  }
}
