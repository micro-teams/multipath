// Application-level WebSocket over the substrate: a real RFC 6455 client, written from scratch,
// because the browser platform gives no other way to speak WebSocket on top of an arbitrary byte
// stream. The browser's own `WebSocket` can only dial a real network URL; it cannot be handed a
// MuxStream. And a service worker's fetch handler never sees WebSocket traffic at all — `fetch`
// simply isn't the event WebSocket upgrades fire. So an app that wants its WebSocket calls to ride
// the redundant substrate has no platform primitive to lean on; multipath provides one, exactly the
// way Client.fetch already provides one for HTTP.
//
// MultipathWebSocket mirrors the standard browser WebSocket surface (onopen/onmessage/onerror/
// onclose, send, close, readyState) so swapping `new WebSocket(url)` for
// `client.openWebSocket(service, path)` is close to a drop-in. The wire format is standard RFC 6455:
// the far end is a genuine WebSocket server the origin's service handler is fronting, unaware its
// bytes are travelling over a redundant mux instead of a plain TCP socket.

import { MuxStream } from './mux.js';

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

/** Pure, synchronous RFC 6455 frame parser: push bytes as they arrive off the stream, pull whole
 * frames as they become available. Split out from MultipathWebSocket so the wire format is testable
 * without any I/O, the same way frames.ts's FrameReader is tested independently of a real link. */
export class WSFrameReader {
  private buf = new Uint8Array(0);

  push(chunk: Uint8Array): void {
    const next = new Uint8Array(this.buf.length + chunk.length);
    next.set(this.buf);
    next.set(chunk, this.buf.length);
    this.buf = next;
  }

  /** Returns the next complete frame, or null if more bytes are needed. Unmasks the payload if the
   * frame was masked (a compliant server never masks, but this tolerates it either way). */
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

/** Frames one complete, unfragmented message. The client always masks (RFC 6455 requires it). */
function encodeFrame(opcode: number, payload: Uint8Array): Uint8Array {
  const length = payload.length;
  let head: number[];
  if (length < 126) head = [0x80 | opcode, 0x80 | length];
  else if (length < 1 << 16) {
    head = [0x80 | opcode, 0x80 | 126, (length >> 8) & 0xff, length & 0xff];
  } else {
    head = [
      0x80 | opcode,
      0x80 | 127,
      0,
      0,
      0,
      0,
      (length >>> 24) & 0xff,
      (length >>> 16) & 0xff,
      (length >>> 8) & 0xff,
      length & 0xff,
    ];
  }
  const mask = crypto.getRandomValues(new Uint8Array(4));
  const masked = new Uint8Array(length);
  for (let i = 0; i < length; i++) masked[i] = payload[i] ^ mask[i % 4];
  const out = new Uint8Array(head.length + 4 + length);
  out.set(head, 0);
  out.set(mask, head.length);
  out.set(masked, head.length + 4);
  return out;
}

async function wsAccept(key: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-1', encoder.encode(key + WS_GUID));
  return btoa(String.fromCharCode(...new Uint8Array(digest)));
}

function findDoubleCRLF(buf: Uint8Array): number {
  for (let i = 0; i + 3 < buf.length; i++) {
    if (buf[i] === 13 && buf[i + 1] === 10 && buf[i + 2] === 13 && buf[i + 3] === 10) return i;
  }
  return -1;
}

export type MultipathWebSocketState = 0 | 1 | 2 | 3; // CONNECTING | OPEN | CLOSING | CLOSED

/**
 * An application-level WebSocket carried over one multipath stream. Constructed via
 * Client.openWebSocket; do not construct directly (the handshake must complete first).
 */
export class MultipathWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState: MultipathWebSocketState = MultipathWebSocket.CONNECTING;
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: ArrayBuffer | string }) => void) | null = null;
  onerror: ((err: Error) => void) | null = null;
  onclose: (() => void) | null = null;

  private reader = new WSFrameReader();
  private writeChain: Promise<void> = Promise.resolve();
  private fragType = 0;
  private fragData: Uint8Array = new Uint8Array(0);

  private constructor(private stream: MuxStream) {}

  /** Opens stream, performs the RFC 6455 client handshake at path, and starts the read loop. Extra
   * headers (e.g. a sub-protocol) are sent verbatim; the far end must be a real WebSocket server. */
  static async open(
    stream: MuxStream,
    path: string,
    headers: Record<string, string> = {},
  ): Promise<MultipathWebSocket> {
    const ws = new MultipathWebSocket(stream);
    await ws.handshake(path, headers);
    ws.readyState = MultipathWebSocket.OPEN;
    ws.onopen?.();
    void ws.readLoop();
    return ws;
  }

  private async handshake(path: string, headers: Record<string, string>): Promise<void> {
    const keyBytes = crypto.getRandomValues(new Uint8Array(16));
    const key = btoa(String.fromCharCode(...keyBytes));
    let req =
      `GET ${path} HTTP/1.1\r\nHost: multipath\r\nUpgrade: websocket\r\n` +
      `Connection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n`;
    for (const [name, value] of Object.entries(headers)) req += `${name}: ${value}\r\n`;
    req += '\r\n';
    await this.stream.write(encoder.encode(req));

    let head = new Uint8Array(0);
    let sep = -1;
    while (sep < 0) {
      const chunk = await this.stream.read();
      if (chunk === null) throw new Error('multipath: websocket handshake: stream closed');
      const next = new Uint8Array(head.length + chunk.length);
      next.set(head);
      next.set(chunk, head.length);
      head = next;
      sep = findDoubleCRLF(head);
    }
    const headText = decoder.decode(head.subarray(0, sep));
    this.reader.push(head.subarray(sep + 4)); // any bytes past the header are already frame data

    const lines = headText.split('\r\n');
    const statusParts = lines[0].split(' ');
    if (statusParts[1] !== '101') {
      throw new Error(`multipath: websocket handshake got ${lines[0]}`);
    }
    const respHeaders = new Map<string, string>();
    for (const line of lines.slice(1)) {
      const idx = line.indexOf(':');
      if (idx > 0)
        respHeaders.set(line.slice(0, idx).trim().toLowerCase(), line.slice(idx + 1).trim());
    }
    const accept = await wsAccept(key);
    if (
      (respHeaders.get('upgrade') ?? '').toLowerCase() !== 'websocket' ||
      respHeaders.get('sec-websocket-accept') !== accept
    ) {
      throw new Error('multipath: websocket handshake response did not confirm the upgrade');
    }
  }

  private async readLoop(): Promise<void> {
    try {
      for (;;) {
        let frame = this.reader.next();
        while (frame === null) {
          const chunk = await this.stream.read();
          if (chunk === null) {
            this.finishClose();
            return;
          }
          this.reader.push(chunk);
          frame = this.reader.next();
        }
        if (!(await this.handleFrame(frame))) return;
      }
    } catch (err) {
      if (this.readyState !== MultipathWebSocket.CLOSED) {
        this.onerror?.(err instanceof Error ? err : new Error(String(err)));
      }
      this.finishClose();
    }
  }

  /** Returns false when the loop should stop (a close frame was seen or is being sent). */
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
        await this.rawSend(OP_CLOSE, new Uint8Array(0));
        this.finishClose();
        return false;
      default:
        this.onerror?.(new Error(`multipath: websocket: unexpected opcode ${frame.opcode}`));
        this.finishClose();
        return false;
    }
    if (frame.fin) {
      const data =
        this.fragType === OP_TEXT
          ? decoder.decode(this.fragData)
          : (this.fragData.buffer.slice(
              this.fragData.byteOffset,
              this.fragData.byteOffset + this.fragData.length,
            ) as ArrayBuffer);
      this.onmessage?.({ data });
      this.fragData = new Uint8Array(0);
    }
    return true;
  }

  /** Sends one complete text or binary message. Errors surface via onerror, matching the browser
   * WebSocket's fire-and-forget send() rather than returning a rejected promise the caller must
   * remember to catch. */
  send(data: string | Uint8Array): void {
    const payload = typeof data === 'string' ? encoder.encode(data) : data;
    const opcode = typeof data === 'string' ? OP_TEXT : OP_BINARY;
    void this.rawSend(opcode, payload).catch((err) =>
      this.onerror?.(err instanceof Error ? err : new Error(String(err))),
    );
  }

  private rawSend(opcode: number, payload: Uint8Array): Promise<void> {
    this.writeChain = this.writeChain.then(() => this.stream.write(encodeFrame(opcode, payload)));
    return this.writeChain;
  }

  /** Initiates a close: sends a close frame; onclose fires once the peer's close (or EOF) arrives. */
  close(): void {
    if (
      this.readyState === MultipathWebSocket.CLOSED ||
      this.readyState === MultipathWebSocket.CLOSING
    )
      return;
    this.readyState = MultipathWebSocket.CLOSING;
    void this.rawSend(OP_CLOSE, new Uint8Array(0)).catch(() => {});
  }

  private finishClose(): void {
    if (this.readyState === MultipathWebSocket.CLOSED) return;
    this.readyState = MultipathWebSocket.CLOSED;
    this.stream.reset();
    this.onclose?.();
  }
}
