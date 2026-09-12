// Wire framing for the redundant stream: encode the frame types and a streaming reader that
// reassembles frames off a byte-oriented link. Byte-identical to the Go peer (redundant_frame.go),
// because the whole point is that a browser link and a connector link speak one wire format.
//
// Frame layout (all integers big-endian):
//
//   HELLO 0x05 | connID[16] | linkIndex:u16
//   DATA  0x01 | offset:u64  | len:u16 | crc32:u32 | payload[len]
//   ACK   0x02 | cumulative:u64
//   PING  0x03 | nonce:u64
//   PONG  0x04 | nonce:u64

export const FrameType = {
  Data: 0x01,
  Ack: 0x02,
  Ping: 0x03,
  Pong: 0x04,
  Hello: 0x05,
  Reject: 0x06, // server→client: link refused, with a UTF-8 reason; do not retry it
} as const;

export const MAX_SEGMENT = 32 * 1024; // largest DATA payload, fits the u16 length
const DATA_HDR_LEN = 1 + 8 + 2 + 4; // type + offset + len + crc

export type Frame =
  | { type: typeof FrameType.Hello; connID: Uint8Array; linkIdx: number }
  | { type: typeof FrameType.Data; offset: bigint; payload: Uint8Array }
  | { type: typeof FrameType.Ack; cumulative: bigint }
  | { type: typeof FrameType.Ping | typeof FrameType.Pong; nonce: bigint }
  | { type: typeof FrameType.Reject; reason: string };

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();

export function encodeReject(reason: string): Uint8Array {
  let r = textEncoder.encode(reason);
  if (r.length > MAX_SEGMENT) r = r.subarray(0, MAX_SEGMENT);
  const b = new Uint8Array(3 + r.length);
  b[0] = FrameType.Reject;
  new DataView(b.buffer).setUint16(1, r.length);
  b.set(r, 3);
  return b;
}

export function encodeHello(connID: Uint8Array, linkIdx: number): Uint8Array {
  const b = new Uint8Array(1 + 16 + 2);
  b[0] = FrameType.Hello;
  b.set(connID.subarray(0, 16), 1);
  new DataView(b.buffer).setUint16(17, linkIdx);
  return b;
}

export function encodeData(offset: bigint, payload: Uint8Array): Uint8Array {
  const b = new Uint8Array(DATA_HDR_LEN + payload.length);
  const v = new DataView(b.buffer);
  b[0] = FrameType.Data;
  v.setBigUint64(1, offset);
  v.setUint16(9, payload.length);
  v.setUint32(11, crc32(payload));
  b.set(payload, DATA_HDR_LEN);
  return b;
}

export function encodeAck(cumulative: bigint): Uint8Array {
  const b = new Uint8Array(9);
  b[0] = FrameType.Ack;
  new DataView(b.buffer).setBigUint64(1, cumulative);
  return b;
}

export function encodeNonce(type: number, nonce: bigint): Uint8Array {
  const b = new Uint8Array(9);
  b[0] = type;
  new DataView(b.buffer).setBigUint64(1, nonce);
  return b;
}

// FrameReader reassembles frames off a link that delivers bytes in arbitrary chunk boundaries: feed
// it whatever a WebSocket message carried, then pull whole frames until it wants more. A corrupt
// DATA frame (CRC or length) or an unknown tag throws, and the caller drops the link — the same
// bytes arrive intact on another link or a replay.
export class FrameReader {
  private buf = new Uint8Array(0);

  push(chunk: Uint8Array): void {
    const next = new Uint8Array(this.buf.length + chunk.length);
    next.set(this.buf);
    next.set(chunk, this.buf.length);
    this.buf = next;
  }

  // next returns the next complete frame, or null if more bytes are needed.
  next(): Frame | null {
    if (this.buf.length < 1) return null;
    const v = new DataView(this.buf.buffer, this.buf.byteOffset, this.buf.byteLength);
    switch (this.buf[0]) {
      case FrameType.Hello: {
        if (this.buf.length < 1 + 18) return null;
        const connID = this.buf.slice(1, 17);
        const linkIdx = v.getUint16(17);
        this.consume(1 + 18);
        return { type: FrameType.Hello, connID, linkIdx };
      }
      case FrameType.Data: {
        if (this.buf.length < DATA_HDR_LEN) return null;
        const offset = v.getBigUint64(1);
        const n = v.getUint16(9);
        const want = v.getUint32(11);
        if (n > MAX_SEGMENT) throw new Error('multipath: corrupt frame (oversize DATA)');
        if (this.buf.length < DATA_HDR_LEN + n) return null;
        const payload = this.buf.slice(DATA_HDR_LEN, DATA_HDR_LEN + n);
        if (crc32(payload) !== want) throw new Error('multipath: corrupt frame (CRC)');
        this.consume(DATA_HDR_LEN + n);
        return { type: FrameType.Data, offset, payload };
      }
      case FrameType.Ack: {
        if (this.buf.length < 9) return null;
        const cumulative = v.getBigUint64(1);
        this.consume(9);
        return { type: FrameType.Ack, cumulative };
      }
      case FrameType.Ping:
      case FrameType.Pong: {
        if (this.buf.length < 9) return null;
        const type = this.buf[0] as typeof FrameType.Ping | typeof FrameType.Pong;
        const nonce = v.getBigUint64(1);
        this.consume(9);
        return { type, nonce };
      }
      case FrameType.Reject: {
        if (this.buf.length < 3) return null;
        const n = v.getUint16(1);
        if (n > MAX_SEGMENT) throw new Error('multipath: corrupt frame (oversize REJECT)');
        if (this.buf.length < 3 + n) return null;
        const reason = textDecoder.decode(this.buf.subarray(3, 3 + n));
        this.consume(3 + n);
        return { type: FrameType.Reject, reason };
      }
      default:
        throw new Error(`multipath: corrupt frame (unknown tag ${this.buf[0]})`);
    }
  }

  private consume(n: number): void {
    this.buf = this.buf.slice(n);
  }
}

// crc32 (IEEE, the same polynomial hash/crc32.ChecksumIEEE uses), computed with a lazily built
// table so the DATA frames a browser sends verify byte-for-byte against the Go and JVM peers.
let crcTable: Uint32Array | null = null;
function crc32(data: Uint8Array): number {
  if (crcTable === null) {
    crcTable = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      crcTable[n] = c >>> 0;
    }
  }
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) crc = crcTable[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}
