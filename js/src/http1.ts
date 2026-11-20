// One HTTP/1.1 exchange on a stream: the request written out, the response read back as a Response
// whose body arrives as the bytes do. The stream is released (reset, unless both sides have already
// sent FIN) as soon as the response body is complete or abandoned, so an exchange never holds a
// stream, and the server a target connection, beyond its answer. The request side is not half-closed
// after the request: the server would pass the FIN on to the target, and nginx takes a client's
// half-close for an abort and answers a proxied request with 499.
//
// The body is framed as HTTP/1.1 frames it: none for HEAD, 1xx, 204 and 304; Content-Length; chunked
// (decoded here); or up to the end of the stream. Interim 1xx answers are skipped. With gzip, the
// request asks for it unless it names its own Accept-Encoding, and a gzip answer is decoded with
// DecompressionStream; the Response then carries the decoded body without Content-Encoding and
// Content-Length, as fetch() would hand it over. An encoding the request did not ask for is an error.
// A malformed response is an error too, never passed through as if it were content.
//
// With buffered, the whole body is read before the Response is made, for platforms whose Response
// cannot take a stream (React Native). A textual body is then handed over as a string, because React
// Native's Response reads a binary body as Latin-1.
//
//   - serializeRequest(request, gzip) returns the request bytes: request line, headers (Host,
//     Content-Length and Connection: close added) and body.
//   - readResponse(stream, { method, gzip, buffered }) resolves with the Response once its head has
//     arrived; reading its body to the end releases the stream, and cancelling the body resets it.
//
//     const st = mux.open();
//     await st.write(await serializeRequest(request, true));
//     const response = await readResponse(st, { method: request.method, gzip: true, buffered: false });
import type { Stream } from './mux.js';

const CRLF = '\r\n';
const MAX_HEAD = 64 << 10;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

export async function serializeRequest(request: Request, gzip: boolean): Promise<Uint8Array> {
  const url = new URL(request.url);
  // Read the body whatever the Request says about having one: React Native's Request has no `body`
  // property at all, and a GET simply reads as empty.
  const body = new Uint8Array(await request.arrayBuffer());
  const headers = new Headers(request.headers);
  if (!headers.has('host')) headers.set('host', url.host);
  if (gzip && !headers.has('accept-encoding')) headers.set('accept-encoding', 'gzip');
  headers.set('content-length', String(body.length));
  // The response then ends where the stream does, and no keep-alive needs telling apart.
  headers.set('connection', 'close');
  let head = `${request.method} ${url.pathname}${url.search} HTTP/1.1${CRLF}`;
  for (const [name, value] of headers) head += `${name}: ${value}${CRLF}`;
  const headBytes = encoder.encode(head + CRLF);
  const out = new Uint8Array(headBytes.length + body.length);
  out.set(headBytes);
  out.set(body, headBytes.length);
  return out;
}

export interface ResponseOptions {
  method: string;
  gzip: boolean; // the request asked for gzip
  buffered: boolean;
}

// Source hands out the stream's bytes, starting with what was read past the response head.
class Source {
  private pending: Uint8Array;

  constructor(
    readonly st: Stream,
    leftover: Uint8Array,
  ) {
    this.pending = leftover;
  }

  // next returns up to max bytes, or null at the end of the stream.
  async next(max = Infinity): Promise<Uint8Array | null> {
    if (this.pending.length === 0) {
      const chunk = await this.st.read();
      if (chunk === null) return null;
      this.pending = chunk;
    }
    const out = this.pending.subarray(0, Math.min(max, this.pending.length));
    this.pending = this.pending.subarray(out.length);
    return out;
  }

  // line returns the next CRLF-terminated line, without the CRLF.
  async line(): Promise<string> {
    let acc: Uint8Array = new Uint8Array(0);
    for (;;) {
      const i = findCRLF(acc);
      if (i >= 0) {
        this.pending = concat(acc.subarray(i + 2), this.pending);
        return decoder.decode(acc.subarray(0, i));
      }
      if (acc.length > 4096) throw new Error('multipath: chunk line too long');
      const chunk = await this.next();
      if (chunk === null) throw new Error('multipath: truncated chunked body');
      acc = concat(acc, chunk);
    }
  }
}

export async function readResponse(st: Stream, opt: ResponseOptions): Promise<Response> {
  let head: { status: number; statusText: string; headers: Headers; rest: Uint8Array };
  let leftover: Uint8Array = new Uint8Array(0);
  do {
    head = await readHead(st, leftover);
    leftover = head.rest;
  } while (head.status >= 100 && head.status < 200);
  const { status, statusText, headers } = head;
  const src = new Source(st, leftover);
  const noBody = opt.method === 'HEAD' || status === 204 || status === 304;
  const chunked = /\bchunked\b/i.test(headers.get('transfer-encoding') ?? '');
  const length = headers.get('content-length');
  let left = !noBody && !chunked && length !== null ? Number(length) : Infinity;
  if (!Number.isSafeInteger(left) && left !== Infinity)
    throw new Error(`multipath: bad content-length "${length}"`);
  let chunkLeft = 0;
  let done = noBody;

  const release = () => {
    if (!st.released) st.reset('response complete');
  };
  // next returns the next piece of the body, or null at its end.
  const next = async (): Promise<Uint8Array | null> => {
    if (done) return null;
    if (chunked) {
      if (chunkLeft === 0) {
        const sizeText = (await src.line()).split(';')[0].trim();
        if (!/^[0-9a-f]+$/i.test(sizeText))
          throw new Error(`multipath: bad chunk size "${sizeText}"`);
        chunkLeft = parseInt(sizeText, 16);
        if (chunkLeft === 0) {
          while ((await src.line()) !== ''); // trailers are not part of the body
          done = true;
          return null;
        }
      }
      const piece = await src.next(chunkLeft);
      if (piece === null) throw new Error('multipath: truncated chunked body');
      chunkLeft -= piece.length;
      if (chunkLeft === 0 && (await src.line()) !== '')
        throw new Error('multipath: malformed chunk');
      return piece;
    }
    if (left === 0) {
      done = true;
      return null;
    }
    const piece = await src.next(left);
    if (piece === null) {
      if (left !== Infinity) throw new Error(`multipath: body ended ${left} bytes short`);
      done = true;
      return null;
    }
    left -= piece.length;
    return piece;
  };

  if (chunked) {
    headers.delete('transfer-encoding');
  }
  const encoding = (headers.get('content-encoding') ?? 'identity').toLowerCase();
  const decode = opt.gzip && encoding === 'gzip';
  if (opt.gzip && !decode && encoding !== 'identity') {
    release();
    throw new Error(`multipath: the answer is encoded as ${encoding}, which was not asked for`);
  }
  if (decode) {
    headers.delete('content-encoding');
    headers.delete('content-length');
  }

  if (opt.buffered) {
    const parts: Uint8Array[] = [];
    try {
      for (let p = await next(); p !== null; p = await next()) parts.push(p.slice());
    } finally {
      release();
    }
    let body = concat(...parts);
    if (decode) body = await gunzip(body);
    if (chunked && !decode) headers.set('content-length', String(body.length));
    if (noBody) return new Response(null, { status, statusText, headers });
    const textual = /^text\/|json|xml|javascript/i.test(headers.get('content-type') ?? '');
    return new Response(textual ? decoder.decode(body) : body.slice().buffer, {
      status,
      statusText,
      headers,
    });
  }

  if (noBody) {
    release();
    return new Response(null, { status, statusText, headers });
  }
  let body: ReadableStream<Uint8Array> = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const piece = await next();
        if (piece === null) {
          release();
          controller.close();
        } else {
          controller.enqueue(piece.slice());
        }
      } catch (err) {
        release();
        controller.error(err);
      }
    },
    cancel() {
      release();
    },
  });
  if (decode)
    body = body.pipeThrough(
      new DecompressionStream('gzip') as unknown as TransformStream<Uint8Array, Uint8Array>,
    );
  return new Response(body, { status, statusText, headers });
}

async function readHead(
  st: Stream,
  start: Uint8Array,
): Promise<{ status: number; statusText: string; headers: Headers; rest: Uint8Array }> {
  let buf = start;
  let sep = findDoubleCRLF(buf);
  while (sep < 0) {
    if (buf.length > MAX_HEAD) throw new Error('multipath: response head too large');
    const chunk = await st.read();
    if (chunk === null) throw new Error('multipath: the stream ended before a response head');
    buf = concat(buf, chunk);
    sep = findDoubleCRLF(buf);
  }
  const lines = decoder.decode(buf.subarray(0, sep)).split(CRLF);
  const m = /^HTTP\/1\.[01] (\d{3})(?: (.*))?$/.exec(lines[0]);
  if (!m) throw new Error(`multipath: malformed status line "${lines[0].slice(0, 80)}"`);
  const headers = new Headers();
  for (const line of lines.slice(1)) {
    const i = line.indexOf(':');
    if (i <= 0) throw new Error(`multipath: malformed header line "${line.slice(0, 80)}"`);
    headers.append(line.slice(0, i).trim(), line.slice(i + 1).trim());
  }
  return { status: Number(m[1]), statusText: m[2] ?? '', headers, rest: buf.subarray(sep + 4) };
}

async function gunzip(bytes: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([bytes.slice().buffer])
    .stream()
    .pipeThrough(new DecompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

function findCRLF(b: Uint8Array): number {
  for (let i = 0; i + 1 < b.length; i++) if (b[i] === 13 && b[i + 1] === 10) return i;
  return -1;
}

function findDoubleCRLF(b: Uint8Array): number {
  for (let i = 0; i + 3 < b.length; i++) {
    if (b[i] === 13 && b[i + 1] === 10 && b[i + 2] === 13 && b[i + 3] === 10) return i;
  }
  return -1;
}
