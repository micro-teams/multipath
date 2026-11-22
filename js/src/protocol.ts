// The MultiPath version-3 constants, limits and frame codecs (PROTOCOL.md sections 4, 6 and 7), the
// JavaScript mirror of the Go package `protocol`. A number that appears on the wire is defined here
// and nowhere else in this package; the shared vectors in spec/vectors/frames.json pin every encoding
// (test/protocol.test.ts runs them).
//
// Encoders return one whole frame, ready to be sent as one WebSocket message. Decoders take a payload
// as parseFrame returned it and check its size exactly: a frame of a known type with the wrong size is
// a protocol violation, never something to guess around. parseFrame itself knows no types, so a frame
// of an unknown type comes back like any other, for the caller to skip. Every decoding failure is a
// ProtocolError. Integers wider than 32 bits (offsets, nonces) are bigint.
//
//   - VERSION (3) and IMPLEMENTATION ("multipath-js/<version>", sent in HELLO).
//   - FrameType, Mode and RejectCode: the codes of section 4.
//   - HEADER_LEN, MAX_DATA, DATA_OVERHEAD, MAX_REASON, MAX_IMPL, CONTROL_SLACK, HANDSHAKE_TIMEOUT_MS,
//     CONNECT_TIMEOUT_MS, LINK_DEAD_MIN_MS, LINK_DEAD_RTTS and MIN_WINDOW: the protocol's fixed
//     bounds. HANDSHAKE_TIMEOUT_MS runs from the sent HELLO, CONNECT_TIMEOUT_MS bounds the connection
//     before it; a client link is closed after max(LINK_DEAD_MIN_MS, LINK_DEAD_RTTS x its smoothed
//     round trip) without an inbound byte.
//   - Limits: what a server announces in WELCOME. defaultLimits() returns section 7's defaults;
//     limitsError(l) names the first limit outside the protocol's bounds, or returns null.
//   - ProtocolError: kind "corrupt" (wrong size, bad CRC) or "too large" (above maxPayload).
//   - parseFrame(msg, maxPayload) -> { type, payload }: one link message, which holds one frame.
//   - encodeHello / decodeHello (HelloFrame), encodeWelcome / decodeWelcome (WelcomeFrame, limits
//     validated), encodeReject / decodeReject (RejectFrame).
//   - encodeData / decodeData: offset, CRC-32 and 1 to MAX_DATA bytes; a CRC mismatch is corrupt.
//   - encodeAck, encodePing, encodePong / decodeU64; encodeGoodbye / decodeReason (also RST reasons).
//   - MuxType, MAX_MUX_DATA, encodeMux(type, stream, body) and parseMux(buf, at): the mux frames of
//     section 6. parseMux returns null while buf holds no whole frame at `at`, else the frame and the
//     number of bytes it used.
//   - crc32(bytes): IEEE CRC-32, as Go's hash/crc32.ChecksumIEEE.
//
//     const f = encodeData(0n, new TextEncoder().encode('hi'));
//     const { type, payload } = parseFrame(f, defaultLimits().maxPayload); // type === FrameType.Data
//     const { offset, data } = decodeData(payload); // 0n, 'hi'
import { IMPL_VERSION } from './version.js';

export const VERSION = 3;
export const IMPLEMENTATION = `multipath-js/${IMPL_VERSION}`;

export const FrameType = {
  Hello: 0x01,
  Welcome: 0x02,
  Reject: 0x03,
  Data: 0x04,
  Ack: 0x05,
  Ping: 0x06,
  Pong: 0x07,
  Goodbye: 0x08,
} as const;

export const Mode = { New: 0, Join: 1 } as const;

export const RejectCode = { Version: 1, UnknownSession: 2, Limit: 3, Malformed: 4 } as const;

export const HEADER_LEN = 3;
export const MAX_DATA = 32768;
export const DATA_OVERHEAD = 12;
export const MAX_REASON = 256;
export const MAX_IMPL = 64;
export const CONTROL_SLACK = 65536;
export const HANDSHAKE_TIMEOUT_MS = 5000;
// Through a distant CDN, DNS, TCP, TLS and the upgrade alone can take several seconds.
export const CONNECT_TIMEOUT_MS = 10_000;
// Jitter or one lost segment on a working line must never cost a link.
export const LINK_DEAD_MIN_MS = 5000;
export const LINK_DEAD_RTTS = 4;
export const MIN_WINDOW = 256 << 10;

export interface Limits {
  maxPayload: number;
  window: number;
  streamWindow: number;
  maxStreams: number;
  maxLinks: number;
  pingMs: number;
  clientDeadMs: number;
  serverDeadMs: number;
  progressMs: number;
}

export function defaultLimits(): Limits {
  return {
    maxPayload: 65535,
    window: 2 << 20,
    streamWindow: 256 << 10,
    maxStreams: 256,
    maxLinks: 8,
    pingMs: 500,
    clientDeadMs: 1000,
    serverDeadMs: 10_000,
    progressMs: 10_000,
  };
}

export function limitsError(l: Limits): string | null {
  const ms = (v: number) => v >= 1 && v <= 65535;
  if (l.maxPayload < DATA_OVERHEAD + MAX_DATA || l.maxPayload > 65535)
    return `max payload ${l.maxPayload} outside ${DATA_OVERHEAD + MAX_DATA}..65535`;
  if (l.window < MIN_WINDOW || l.window > 64 << 20)
    return `window ${l.window} outside ${MIN_WINDOW}..${64 << 20}`;
  if (l.streamWindow < 64 << 10 || l.streamWindow > 16 << 20)
    return `stream window ${l.streamWindow} outside ${64 << 10}..${16 << 20}`;
  if (l.maxStreams < 1 || l.maxStreams > 65535)
    return `max streams ${l.maxStreams} outside 1..65535`;
  if (l.maxLinks < 1 || l.maxLinks > 255) return `max links ${l.maxLinks} outside 1..255`;
  if (![l.pingMs, l.clientDeadMs, l.serverDeadMs, l.progressMs].every(ms))
    return 'timings must be 1 ms to 65535 ms';
  if (l.clientDeadMs <= l.pingMs || l.serverDeadMs <= l.pingMs)
    return `dead times ${l.clientDeadMs} and ${l.serverDeadMs} must exceed the ping interval ${l.pingMs}`;
  return null;
}

export class ProtocolError extends Error {
  constructor(
    readonly kind: 'corrupt' | 'too large',
    detail: string,
  ) {
    super(`multipath: ${kind === 'corrupt' ? 'corrupt frame' : 'frame too large'}: ${detail}`);
  }
}

const corrupt = (detail: string) => new ProtocolError('corrupt', detail);
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function parseFrame(
  msg: Uint8Array,
  maxPayload: number,
): { type: number; payload: Uint8Array } {
  if (msg.length < HEADER_LEN) throw corrupt(`message of ${msg.length} bytes`);
  const n = (msg[1] << 8) | msg[2];
  if (n > maxPayload)
    throw new ProtocolError(
      'too large',
      `type 0x${msg[0].toString(16)} length ${n} > ${maxPayload}`,
    );
  if (msg.length !== HEADER_LEN + n)
    throw corrupt(`message of ${msg.length} bytes holds a frame of ${HEADER_LEN + n}`);
  return { type: msg[0], payload: msg.subarray(HEADER_LEN) };
}

// frame allocates a frame of the given payload size with its envelope written.
function frame(type: number, size: number): { b: Uint8Array<ArrayBuffer>; v: DataView } {
  const b = new Uint8Array(HEADER_LEN + size);
  const v = new DataView(b.buffer);
  b[0] = type;
  v.setUint16(1, size);
  return { b, v };
}

function view(p: Uint8Array): DataView {
  return new DataView(p.buffer, p.byteOffset, p.byteLength);
}

function clip(s: string, max: number): Uint8Array {
  const b = encoder.encode(s);
  return b.length > max ? b.subarray(0, max) : b;
}

export interface HelloFrame {
  version: number;
  session: Uint8Array; // 16 bytes
  link: number;
  mode: number;
  impl: string;
}

const HELLO_LEN = 20;
const WELCOME_LEN = 24;

export function encodeHello(h: HelloFrame): Uint8Array<ArrayBuffer> {
  const impl = clip(h.impl, MAX_IMPL);
  const { b, v } = frame(FrameType.Hello, HELLO_LEN + impl.length);
  v.setUint16(3, h.version);
  b.set(h.session.subarray(0, 16), 5);
  b[21] = h.link;
  b[22] = h.mode;
  b.set(impl, 3 + HELLO_LEN);
  return b;
}

export function decodeHello(p: Uint8Array): HelloFrame {
  if (p.length < HELLO_LEN || p.length > HELLO_LEN + MAX_IMPL || p[19] > Mode.Join)
    throw corrupt(`hello of ${p.length} bytes`);
  return {
    version: view(p).getUint16(0),
    session: p.slice(2, 18),
    link: p[18],
    mode: p[19],
    impl: decoder.decode(p.subarray(HELLO_LEN)),
  };
}

export interface WelcomeFrame {
  version: number;
  created: boolean;
  limits: Limits;
  impl: string;
}

export function encodeWelcome(w: WelcomeFrame): Uint8Array<ArrayBuffer> {
  const impl = clip(w.impl, MAX_IMPL);
  const { b, v } = frame(FrameType.Welcome, WELCOME_LEN + impl.length);
  const l = w.limits;
  v.setUint16(3, w.version);
  b[5] = w.created ? 1 : 0;
  v.setUint16(6, l.maxPayload);
  v.setUint32(8, l.window);
  v.setUint32(12, l.streamWindow);
  v.setUint16(16, l.maxStreams);
  b[18] = l.maxLinks;
  [l.pingMs, l.clientDeadMs, l.serverDeadMs, l.progressMs].forEach((ms, i) =>
    v.setUint16(19 + 2 * i, ms),
  );
  b.set(impl, 3 + WELCOME_LEN);
  return b;
}

export function decodeWelcome(p: Uint8Array): WelcomeFrame {
  if (p.length < WELCOME_LEN || p.length > WELCOME_LEN + MAX_IMPL)
    throw corrupt(`welcome of ${p.length} bytes`);
  const v = view(p);
  const limits: Limits = {
    maxPayload: v.getUint16(3),
    window: v.getUint32(5),
    streamWindow: v.getUint32(9),
    maxStreams: v.getUint16(13),
    maxLinks: p[15],
    pingMs: v.getUint16(16),
    clientDeadMs: v.getUint16(18),
    serverDeadMs: v.getUint16(20),
    progressMs: v.getUint16(22),
  };
  const bad = limitsError(limits);
  if (bad) throw corrupt(`welcome limits: ${bad}`);
  return {
    version: v.getUint16(0),
    created: (p[2] & 1) !== 0,
    limits,
    impl: decoder.decode(p.subarray(WELCOME_LEN)),
  };
}

export interface RejectFrame {
  code: number;
  reason: string;
}

export function encodeReject(code: number, reason: string): Uint8Array<ArrayBuffer> {
  const r = clip(reason, MAX_REASON);
  const { b } = frame(FrameType.Reject, 1 + r.length);
  b[3] = code;
  b.set(r, 4);
  return b;
}

export function decodeReject(p: Uint8Array): RejectFrame {
  if (p.length < 1 || p.length > 1 + MAX_REASON) throw corrupt(`reject of ${p.length} bytes`);
  return { code: p[0], reason: decoder.decode(p.subarray(1)) };
}

export function encodeData(offset: bigint, data: Uint8Array): Uint8Array<ArrayBuffer> {
  const { b, v } = frame(FrameType.Data, DATA_OVERHEAD + data.length);
  v.setBigUint64(3, offset);
  v.setUint32(11, crc32(data));
  b.set(data, 3 + DATA_OVERHEAD);
  return b;
}

export function decodeData(p: Uint8Array): { offset: bigint; data: Uint8Array } {
  if (p.length <= DATA_OVERHEAD || p.length > DATA_OVERHEAD + MAX_DATA)
    throw corrupt(`data of ${p.length} bytes`);
  const v = view(p);
  const data = p.subarray(DATA_OVERHEAD);
  if (crc32(data) !== v.getUint32(8)) throw corrupt('data crc mismatch');
  return { offset: v.getBigUint64(0), data };
}

function encodeU64(type: number, value: bigint): Uint8Array<ArrayBuffer> {
  const { b, v } = frame(type, 8);
  v.setBigUint64(3, value);
  return b;
}

export const encodeAck = (offset: bigint) => encodeU64(FrameType.Ack, offset);
export const encodePing = (nonce: bigint) => encodeU64(FrameType.Ping, nonce);
export const encodePong = (nonce: bigint) => encodeU64(FrameType.Pong, nonce);

export function decodeU64(p: Uint8Array): bigint {
  if (p.length !== 8) throw corrupt(`8-byte frame of ${p.length} bytes`);
  return view(p).getBigUint64(0);
}

export function encodeGoodbye(reason: string): Uint8Array<ArrayBuffer> {
  const r = clip(reason, MAX_REASON);
  const { b } = frame(FrameType.Goodbye, r.length);
  b.set(r, 3);
  return b;
}

export function decodeReason(p: Uint8Array): string {
  if (p.length > MAX_REASON) throw corrupt(`reason of ${p.length} bytes`);
  return decoder.decode(p);
}

export const MuxType = { Syn: 0x01, Data: 0x02, Fin: 0x03, Rst: 0x04, Window: 0x05 } as const;

export const MAX_MUX_DATA = 16384;

const MUX_HEADER = 7;

export interface MuxFrame {
  type: number;
  stream: number;
  body: Uint8Array;
}

export function encodeMux(
  type: number,
  stream: number,
  body?: Uint8Array,
): Uint8Array<ArrayBuffer> {
  const n = body?.length ?? 0;
  const b = new Uint8Array(MUX_HEADER + n);
  const v = new DataView(b.buffer);
  b[0] = type;
  v.setUint16(1, 4 + n);
  v.setUint32(3, stream);
  if (body) b.set(body, MUX_HEADER);
  return b;
}

export function parseMux(buf: Uint8Array, at: number): { frame: MuxFrame; n: number } | null {
  if (buf.length - at < HEADER_LEN) return null;
  const length = (buf[at + 1] << 8) | buf[at + 2];
  if (length < 4) throw corrupt(`mux frame length ${length}`);
  if (buf.length - at < HEADER_LEN + length) return null;
  const v = new DataView(buf.buffer, buf.byteOffset + at, HEADER_LEN + length);
  const frame = {
    type: buf[at],
    stream: v.getUint32(3),
    body: buf.subarray(at + MUX_HEADER, at + HEADER_LEN + length),
  };
  return { frame, n: HEADER_LEN + length };
}

let crcTable: Uint32Array | null = null;

export function crc32(data: Uint8Array): number {
  if (crcTable === null) {
    crcTable = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      crcTable[n] = c >>> 0;
    }
  }
  const t = crcTable;
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) crc = t[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}
