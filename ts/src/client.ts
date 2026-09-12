// The client end of the substrate for the browser: one redundant mux to the origin over every line,
// then each exchange a mux stream on top. There is no line-picking and no per-request strategy — a
// request is a stream over the one redundant transport, and the redundancy is underneath, in the
// bytes. A caller opens a tunnel to a target, opens a normal stream to the origin's own service, or
// uses fetch(), which carries one HTTP exchange over a normal stream (the drop-in the service worker
// routes intercepted requests through).

import { Header, StreamKind, encodeHeader } from './header.js';
import { MuxSession, MuxStream } from './mux.js';
import { RedundantOptions, RedundantStream } from './redundant.js';

export interface ClientOptions extends Omit<RedundantOptions, 'urls'> {
  // The WebSocket upgrade path the origin's link acceptor listens on; joined to each line. Defaults
  // to "/mt/link".
  linkPath?: string;
}

export class Client {
  private constructor(
    private rs: RedundantStream,
    private sess: MuxSession,
  ) {}

  // dial brings up the redundant transport over every line and resolves once at least one link is
  // up. Each line is a WebSocket origin (wss:// in production, ws:// for a testbed).
  static async dial(lines: string[], opts: ClientOptions = {}): Promise<Client> {
    const linkPath = opts.linkPath ?? '/mt/link';
    const urls = lines.map((l) => l.replace(/\/$/, '') + linkPath);
    const rs = new RedundantStream({ ...opts, urls });
    const sess = MuxSession.client(rs);
    await rs.dial();
    return new Client(rs, sess);
  }

  /** Opens an opaque tunnel to target, carrying ticket for the origin to authorise egress. */
  openTunnel(target: string, ticket?: Uint8Array): MuxStream {
    return this.open({ kind: StreamKind.Tunnel, target, ticket });
  }

  /** Opens a stream bound for the origin's own service. */
  openNormal(): MuxStream {
    return this.open({ kind: StreamKind.Normal });
  }

  private open(header: Header): MuxStream {
    const st = this.sess.openStream();
    void st.write(encodeHeader(header));
    return st;
  }

  /**
   * Carries one HTTP exchange over a normal stream: the request is serialized to HTTP/1.1, written to
   * a fresh stream, and the response read back and parsed. Line redundancy happens underneath without
   * the caller knowing more than one path exists.
   */
  async fetch(request: Request): Promise<Response> {
    const st = this.openNormal();
    await st.write(await serializeRequest(request));
    const bytes = await readToEnd(st);
    return parseResponse(bytes);
  }

  close(): void {
    this.sess.close();
  }
}

const CRLF = '\r\n';
const encoder = new TextEncoder();

async function serializeRequest(request: Request): Promise<Uint8Array> {
  const url = new URL(request.url);
  const path = url.pathname + url.search;
  const body = request.body ? new Uint8Array(await request.arrayBuffer()) : new Uint8Array(0);
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

async function readToEnd(st: MuxStream): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const chunk = await st.read();
    if (chunk === null) break;
    chunks.push(chunk);
    total += chunk.length;
  }
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

const decoder = new TextDecoder();

function parseResponse(bytes: Uint8Array): Response {
  const sep = findDoubleCRLF(bytes);
  if (sep < 0) throw new Error('multipath: malformed HTTP response');
  const headText = decoder.decode(bytes.subarray(0, sep));
  const body = bytes.subarray(sep + 4);
  const lines = headText.split(CRLF);
  const statusLine = lines[0].split(' ');
  const status = Number(statusLine[1] ?? 200);
  const headers = new Headers();
  for (let i = 1; i < lines.length; i++) {
    const idx = lines[i].indexOf(':');
    if (idx > 0) headers.append(lines[i].slice(0, idx).trim(), lines[i].slice(idx + 1).trim());
  }
  // 204/304 must not carry a body per the Response constructor. Hand it a copied ArrayBuffer, a
  // BodyInit the DOM types accept without quarrelling over Uint8Array's buffer generic.
  const bodyInit = status === 204 || status === 304 ? null : body.slice().buffer;
  return new Response(bodyInit, { status, headers });
}

function findDoubleCRLF(bytes: Uint8Array): number {
  for (let i = 0; i + 3 < bytes.length; i++) {
    if (bytes[i] === 13 && bytes[i + 1] === 10 && bytes[i + 2] === 13 && bytes[i + 3] === 10)
      return i;
  }
  return -1;
}
