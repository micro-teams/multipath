// L5 — the one thing said at the start of a mux stream: what it is and, for a tunnel, where it goes.
// Byte-identical to the Go peer (header.go): version:u8 | kind:u8 | targetLen:u16 | target |
// ticketLen:u16 | ticket, big-endian. The library carries the ticket as opaque bytes and reads it
// never; kind, target policy, and ticket meaning are the origin handler's concern.

export const StreamKind = {
  Normal: 0,
  Tunnel: 1,
} as const;

export type StreamKind = (typeof StreamKind)[keyof typeof StreamKind];

export interface Header {
  kind: StreamKind;
  target?: string;
  ticket?: Uint8Array;
}

const HEADER_VERSION = 1;
const MAX_FIELD = 4096;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeHeader(h: Header): Uint8Array {
  const target = encoder.encode(h.target ?? '');
  const ticket = h.ticket ?? new Uint8Array(0);
  if (target.length > MAX_FIELD || ticket.length > MAX_FIELD) {
    throw new Error(`multipath: stream header field exceeds ${MAX_FIELD} bytes`);
  }
  const b = new Uint8Array(6 + target.length + ticket.length);
  const v = new DataView(b.buffer);
  b[0] = HEADER_VERSION;
  b[1] = h.kind;
  v.setUint16(2, target.length);
  b.set(target, 4);
  v.setUint16(4 + target.length, ticket.length);
  b.set(ticket, 6 + target.length);
  return b;
}

// A pull reader for the header, so a stream can hand its leading bytes here and start splicing the
// rest. Returns the header and how many bytes it consumed, or null if more are needed.
export function readHeader(buf: Uint8Array): { header: Header; consumed: number } | null {
  if (buf.length < 2) return null;
  if (buf[0] !== HEADER_VERSION)
    throw new Error(`multipath: unsupported stream header version ${buf[0]}`);
  const kind = buf[1] as StreamKind;
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

  let off = 2;
  const target = readField(buf, v, off);
  if (target === null) return null;
  off = target.next;
  const ticket = readField(buf, v, off);
  if (ticket === null) return null;

  return {
    header: { kind, target: decoder.decode(target.body), ticket: ticket.body },
    consumed: ticket.next,
  };
}

function readField(
  buf: Uint8Array,
  v: DataView,
  off: number,
): { body: Uint8Array; next: number } | null {
  if (buf.length < off + 2) return null;
  const n = v.getUint16(off);
  if (n > MAX_FIELD)
    throw new Error(`multipath: stream header field length ${n} exceeds ${MAX_FIELD}`);
  if (buf.length < off + 2 + n) return null;
  return { body: buf.slice(off + 2, off + 2 + n), next: off + 2 + n };
}
