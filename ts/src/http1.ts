// One HTTP/1.1 exchange as bytes on a stream: the request written out, and the response read back —
// its body de-chunked when the server sent it with Transfer-Encoding: chunked, which any HTTP/1.1
// server may do for a body whose length it did not know up front. The Response handed back carries
// the decoded body and a Content-Length, so a caller sees what fetch() would have given it.
//
// serializeRequest(request) — the request line, headers and body, asking the far end to close
//   after answering so the response ends where the stream does.
// parseResponse(bytes) — the whole response, read to the stream's end, as a Response.
// dechunk(body) — a chunked body's payload; throws on a malformed one rather than passing the
//   framing through as if it were content.

const CRLF = '\r\n';
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export async function serializeRequest(request: Request): Promise<Uint8Array> {
  const url = new URL(request.url);
  const path = url.pathname + url.search;
  // Read the body whatever the Request says about having one: React Native's Request (whatwg-fetch)
  // has no `body` property at all, and a GET simply reads as empty.
  const body = new Uint8Array(await request.arrayBuffer());
  const headers = new Headers(request.headers);
  if (!headers.has('host')) headers.set('host', url.host);
  headers.set('content-length', String(body.length));
  // The origin's app closes the connection per response; ask for it so the response boundary is the
  // stream's EOF and there is no keep-alive to disambiguate.
  headers.set('connection', 'close');

  let head = `${request.method} ${path} HTTP/1.1${CRLF}`;
  for (const [name, value] of headers) head += `${name}: ${value}${CRLF}`;
  head += CRLF;

  const headBytes = encoder.encode(head);
  const out = new Uint8Array(headBytes.length + body.length);
  out.set(headBytes);
  out.set(body, headBytes.length);
  return out;
}

export function parseResponse(bytes: Uint8Array): Response {
  const sep = findDoubleCRLF(bytes, 0);
  if (sep < 0) throw new Error('multipath: malformed HTTP response');
  const headText = decoder.decode(bytes.subarray(0, sep));
  let body = bytes.subarray(sep + 4);
  const lines = headText.split(CRLF);
  const statusLine = lines[0].split(' ');
  const status = Number(statusLine[1] ?? 200);
  const headers = new Headers();
  for (let i = 1; i < lines.length; i++) {
    const idx = lines[i].indexOf(':');
    if (idx > 0) headers.append(lines[i].slice(0, idx).trim(), lines[i].slice(idx + 1).trim());
  }
  if (/\bchunked\b/i.test(headers.get('transfer-encoding') ?? '')) {
    body = dechunk(body);
    headers.delete('transfer-encoding');
    headers.set('content-length', String(body.length));
  }
  // 204/304 must not carry a body per the Response constructor. Hand it a copied ArrayBuffer, a
  // BodyInit the DOM types accept without quarrelling over Uint8Array's buffer generic.
  const bodyInit = status === 204 || status === 304 ? null : body.slice().buffer;
  return new Response(bodyInit, { status, headers });
}

export function dechunk(body: Uint8Array): Uint8Array {
  const parts: Uint8Array[] = [];
  let total = 0;
  let at = 0;
  for (;;) {
    const eol = findCRLF(body, at);
    if (eol < 0) throw new Error('multipath: truncated chunked body');
    // A chunk-size line may carry extensions after a ';'; they are ignored.
    const sizeText = decoder.decode(body.subarray(at, eol)).split(';')[0].trim();
    const size = parseInt(sizeText, 16);
    if (!Number.isFinite(size) || size < 0 || !/^[0-9a-f]+$/i.test(sizeText)) {
      throw new Error(`multipath: bad chunk size "${sizeText}"`);
    }
    at = eol + 2;
    if (size === 0) break; // trailers, if any, follow and are not part of the body
    if (at + size + 2 > body.length) throw new Error('multipath: truncated chunked body');
    parts.push(body.subarray(at, at + size));
    total += size;
    at += size + 2;
  }
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

function findCRLF(bytes: Uint8Array, from: number): number {
  for (let i = from; i + 1 < bytes.length; i++) {
    if (bytes[i] === 13 && bytes[i + 1] === 10) return i;
  }
  return -1;
}

function findDoubleCRLF(bytes: Uint8Array, from: number): number {
  for (let i = from; i + 3 < bytes.length; i++) {
    if (bytes[i] === 13 && bytes[i + 1] === 10 && bytes[i + 2] === 13 && bytes[i + 3] === 10)
      return i;
  }
  return -1;
}
