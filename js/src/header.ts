// L5 — the one thing said at the start of a mux stream: which named service it wants. Byte-identical
// to the Go peer (header.go): version:u8 | serviceLen:u16 | service | ticketLen:u16 | ticket,
// big-endian. Every stream names a service the origin registered — there is no "main service" and no
// client-chosen address; the library carries the ticket as opaque bytes and never reads it.

export interface Header {
  service: string;
  ticket?: Uint8Array;
}

const HEADER_VERSION = 2;
const MAX_FIELD = 4096;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function encodeHeader(h: Header): Uint8Array {
  const service = encoder.encode(h.service);
  const ticket = h.ticket ?? new Uint8Array(0);
  if (service.length > MAX_FIELD || ticket.length > MAX_FIELD) {
    throw new Error(`multipath: stream header field exceeds ${MAX_FIELD} bytes`);
  }
  const b = new Uint8Array(5 + service.length + ticket.length);
  const v = new DataView(b.buffer);
  b[0] = HEADER_VERSION;
  v.setUint16(1, service.length);
  b.set(service, 3);
  v.setUint16(3 + service.length, ticket.length);
  b.set(ticket, 5 + service.length);
  return b;
}

// A pull reader for the header, so a stream can hand its leading bytes here and start using the
// rest. Returns the header and how many bytes it consumed, or null if more are needed.
export function readHeader(buf: Uint8Array): { header: Header; consumed: number } | null {
  if (buf.length < 1) return null;
  if (buf[0] !== HEADER_VERSION)
    throw new Error(`multipath: unsupported stream header version ${buf[0]}`);
  const v = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

  let off = 1;
  const service = readField(buf, v, off);
  if (service === null) return null;
  off = service.next;
  const ticket = readField(buf, v, off);
  if (ticket === null) return null;

  return {
    header: { service: decoder.decode(service.body), ticket: ticket.body },
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
