// The client end of the substrate for the browser: one redundant mux to the origin over every line,
// then each exchange a mux stream on top. There is no line-picking and no per-request strategy — a
// request is a stream over the one redundant transport, and the redundancy is underneath, in the
// bytes. Every stream names a service the origin registered (there is no "main service" and no
// client-chosen address): a caller opens a service by name, or uses fetch(service, request), which
// carries one HTTP exchange over such a stream (the drop-in the service worker routes through).

import { MultipathWebSocket } from './appws.js';
import { encodeHeader } from './header.js';
import { parseResponse, serializeRequest } from './http1.js';
import { MuxSession, MuxStream } from './mux.js';
import { LinkStat, RedundantOptions, RedundantStream } from './redundant.js';

export interface ClientOptions extends Omit<RedundantOptions, 'urls'> {
  // The WebSocket upgrade path the origin's link acceptor listens on; joined to each line. Defaults
  // to "/mt/link".
  linkPath?: string;
}

export class Client {
  private constructor(
    private rs: RedundantStream,
    private sess: MuxSession,
    private up: Promise<void>,
    private linkPath: string,
  ) {}

  // dial brings up the redundant transport over every line and resolves once at least one link is
  // up. Each line is a WebSocket origin (wss:// in production, ws:// for a testbed).
  static async dial(lines: string[], opts: ClientOptions = {}): Promise<Client> {
    const client = Client.connect(lines, opts);
    await client.ready();
    return client;
  }

  /**
   * Starts dialling every line and returns at once, so lines learnt meanwhile can join the race with
   * addLines; ready() resolves once a link is up. Streams opened before then wait for it.
   */
  static connect(lines: string[], opts: ClientOptions = {}): Client {
    const linkPath = opts.linkPath ?? '/mt/link';
    const rs = new RedundantStream({ ...opts, urls: lines.map((l) => joinPath(l, linkPath)) });
    const sess = MuxSession.client(rs);
    return new Client(rs, sess, rs.dial(), linkPath);
  }

  /** Resolves once at least one line is up, or the client has closed (see closed). */
  ready(): Promise<void> {
    return this.up;
  }

  /** Whether the client is finished: closed by close(), or refused by the origin on every line. */
  get closed(): boolean {
    return this.rs.isClosed;
  }

  /**
   * Adds lines to a running client; each one it does not have yet starts connecting at once, joining
   * a dial still in progress. Returns how many were new.
   */
  addLines(lines: string[]): number {
    return lines.filter((l) => this.rs.addLine(joinPath(l, this.linkPath))).length;
  }

  /**
   * Opens a stream to the named service, carrying ticket for the origin's handler to authorise it.
   * If the origin has not registered the name it resets the stream with a reason, which surfaces as
   * an error on the first read.
   */
  open(service: string, ticket?: Uint8Array): MuxStream {
    const st = this.sess.openStream();
    void st.write(encodeHeader({ service, ticket }));
    return st;
  }

  /**
   * Carries one HTTP exchange over a stream to the named service: the request is serialized to
   * HTTP/1.1, written to a fresh stream, and the response read back and parsed. Line redundancy
   * happens underneath without the caller knowing more than one path exists.
   */
  async fetch(service: string, request: Request, ticket?: Uint8Array): Promise<Response> {
    const st = this.open(service, ticket);
    await st.write(await serializeRequest(request));
    const bytes = await readToEnd(st);
    return parseResponse(bytes);
  }

  /**
   * Opens a stream to the named service and performs the RFC 6455 client handshake at path,
   * returning a WebSocket-shaped object (onopen/onmessage/onerror/onclose, send, close) — the
   * browser platform gives no other way to run WebSocket traffic over the substrate (fetch/service
   * workers never see it, and the native WebSocket can't be pointed at an arbitrary byte stream).
   * The far end must be a real WebSocket server; headers are sent verbatim in the handshake request
   * (e.g. a sub-protocol the target expects).
   */
  async openWebSocket(
    service: string,
    path: string,
    ticket?: Uint8Array,
    headers?: Record<string, string>,
  ): Promise<MultipathWebSocket> {
    const st = this.open(service, ticket);
    return MultipathWebSocket.open(st, path, headers);
  }

  /**
   * A snapshot of every underlying line's health (up/connecting/down, last byte seen, reconnect
   * count, last drop reason). A status view reads this; to react to changes as they happen, pass
   * onLinkState in ClientOptions. Redundancy hides line failure from the data path on purpose — this
   * is how a caller sees the failures it is surviving.
   */
  stats(): LinkStat[] {
    return this.rs.stats();
  }

  close(): void {
    this.sess.close();
  }
}

function joinPath(line: string, path: string): string {
  return line.replace(/\/$/, '') + path;
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
