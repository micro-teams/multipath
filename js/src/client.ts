// The MultiPath client for browsers, service workers, React Native and Node: one session to the
// server, carried over every line of its entry at once, with each HTTP exchange and each WebSocket a
// stream of it (PROTOCOL.md). A page or worker holds one Client and sends everything through it.
//
// The client runs one session at a time. It fetches the line list from its entry, dials every line,
// and keeps every line dialled for the life of the session: a line that fails is retried with backoff
// (200 ms to 5 s, with jitter), never given up. When no link answers the session's PING round, every
// line is dialled again at once into the same session, so a short outage of every line (a network
// handover, a lift) costs no stream: a line whose link is still attached dials a replacement while
// the old link stays, and a line waiting out its backoff dials now. When the session ends (the server
// lost it, the server said goodbye, or no line answered for the server's dead time) its streams fail
// at once and the client starts a new session;
// after the server lost the session or said goodbye it first waits a random 0 to 2 s, so the clients
// of a restarted server do not all return in the same instant. The line list is fetched again for
// every session and every refreshMs, and the fetched list replaces the old one: new lines are
// dialled, and the links of lines that left it are closed as soon as a line still in it is up. The
// last list the entry served is kept (in memory, and in the store across restarts). A new session
// dials the kept list at once while the entry is asked again, so a slow or unreachable entry delays
// nothing when the lines themselves work; the entry is asked again with backoff (1 s to 30 s) until
// it answers, and its answer replaces the kept list in that session. A stored list is used only by a
// client of the entry that served it. Every event is logged with the session's first 8 hex digits.
//
// Options (ClientOptions):
//   entry        the origin the line list is fetched from, e.g. "https://example.app" (required)
//   linkPath     path of the link endpoint on every line; default "/multipath/v3/link"
//   linesPath    path of the line list on the entry; default "/multipath/v3/lines"
//   site         lines must be on this host or its subdomains; default the entry's hostname, null for
//                any host (PROTOCOL.md section 10). An https entry accepts only https lines.
//   store        keeps the last fetched list across restarts (LineStore: load() and save(stored), of
//                a StoredLines: the entry and its lines; load() resolves null when nothing is kept)
//   refreshMs    interval of line list refreshes; default 10 minutes
//   waitMs       how long a request waits for a live session before it fails; default 10 s
//   recvWindow   receive credit of each stream; default 1 MiB (see mux.ts and PROTOCOL.md section 6)
//   compression  ask for gzip and decode it with DecompressionStream; default true. A platform
//                without DecompressionStream (React Native) must pass false; the constructor throws
//                otherwise, rather than quietly sending everything uncompressed.
//   buffered     read each response whole before handing it over, for platforms whose Response cannot
//                take a stream (React Native); default false
//   onEvent      told about every ClientEvent: a line up or down, a session start or end (and, from
//                the router, every fallback)
//   log          one line per event; default console.info
//   dialAddresses  dial a line that has an address (lines.ts) at that address, with the line's host
//                in the upgrade's Host header, given as React Native's WebSocket options ({headers});
//                default false. Only for a WebSocket that then verifies the certificate against
//                that host, not the address: React Native's does once the app installs a hostname
//                verifier for it. A browser can do neither, and dials every line at its url.
//   WebSocket, fetch  the platform's, unless given (tests)
//
// API:
//   - new Client(options) validates the options and starts at once.
//   - client.fetch(input, init) carries one HTTP/1.1 exchange over a fresh stream and resolves with
//     the Response as soon as its head arrives; the body streams (see http1.ts). A relative URL is
//     resolved against the entry. It rejects when no session is live within waitMs, or the stream
//     fails; an abort signal on the request resets the stream.
//   - client.openWebSocket(path, protocols) opens an application WebSocket over a fresh stream
//     (websocket.ts), with the entry's host.
//   - client.live(timeoutMs) resolves true once a session is live (a link is attached), false if none
//     is within timeoutMs.
//   - client.stats() is a ClientStats snapshot: the session (id, state, age, server implementation,
//     bytes sent, unacknowledged and received, reorder buffer), every line (url, address when it is
//     dialled, state, whether it is soft
//     down, its smoothed round trip, milliseconds since its last byte, bytes in and out, reconnects,
//     last reason; the answering line with the smallest round trip is the quickest way to the
//     server), every open stream (bytes in and out,
//     buffered bytes, send credit), the number of sessions started and why the previous one ended.
//     It has the shape of the Go client's /status.
//   - client.close() says goodbye to the server and stops; later requests fail.
//
//     const client = new Client({ entry: 'https://example.app', onEvent: (e) => console.log(e) });
//     const res = await client.fetch('/api/items');
//     const ws = await client.openWebSocket('/updates', []);
import { readResponse, serializeRequest } from './http1.js';
import { RejectedError, Link, type WebSocketConstructor } from './link.js';
import { checkLines, linkUrl, parseLines, type Line } from './lines.js';
import { Mux } from './mux.js';
import {
  CONNECT_TIMEOUT_MS,
  HANDSHAKE_TIMEOUT_MS,
  IMPLEMENTATION,
  MIN_WINDOW,
  Mode,
  RejectCode,
  VERSION,
  defaultLimits,
  type WelcomeFrame,
} from './protocol.js';
import { PEER_GOODBYE, Session } from './session.js';
import { MultipathWebSocket } from './websocket.js';

// What a LineStore keeps: the last list and the entry that served it. A loaded one is untrusted
// until the client has checked it.
export interface StoredLines {
  entry: string;
  lines: Line[];
}

export interface LineStore {
  load(): Promise<StoredLines | null>;
  save(stored: StoredLines): Promise<void>;
}

export type ClientEvent =
  | { type: 'line'; session: string; index: number; id: string; up: boolean; reason: string }
  | { type: 'session-start'; session: string; lines: number }
  | { type: 'session-end'; session: string; ageMs: number; reason: string }
  | { type: 'fallback'; url: string; reason: string };

export interface ClientOptions {
  entry: string;
  linkPath?: string;
  linesPath?: string;
  site?: string | null;
  store?: LineStore;
  refreshMs?: number;
  waitMs?: number;
  recvWindow?: number;
  compression?: boolean;
  buffered?: boolean;
  onEvent?: (e: ClientEvent) => void;
  log?: (message: string) => void;
  dialAddresses?: boolean;
  WebSocket?: WebSocketConstructor;
  fetch?: typeof fetch;
}

export interface ClientStats {
  session: {
    id: string;
    state: 'connecting' | 'live' | 'ended';
    ageMs: number;
    server: string;
    sent: number;
    unacked: number;
    received: number;
    reorder: number;
  } | null;
  lines: {
    index: number;
    id: string;
    url: string;
    address?: string;
    state: 'connecting' | 'up' | 'down';
    soft: boolean;
    rttMs: number;
    lastByteAgoMs: number;
    bytesIn: number;
    bytesOut: number;
    reconnects: number;
    lastReason: string;
  }[];
  streams: {
    id: number;
    bytesIn: number;
    bytesOut: number;
    buffered: number;
    sendCredit: number;
  }[];
  sessionsStarted: number;
  lastSessionEnd: string;
}

const DEFAULT_RECV_WINDOW = 1 << 20;

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const t = setTimeout(done, ms);
    function done() {
      clearTimeout(t);
      signal.removeEventListener('abort', done);
      resolve();
    }
    signal.addEventListener('abort', done);
  });
}

const hex = (b: Uint8Array) => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');

export class Client {
  private readonly entry: string;
  private readonly linkPath: string;
  private readonly linesPath: string;
  private readonly site: string | null;
  private readonly refreshMs: number;
  private readonly waitMs: number;
  private readonly recvWindow: number;
  private readonly compression: boolean;
  private readonly buffered: boolean;
  private readonly WebSocket: WebSocketConstructor;
  private readonly fetchLines: typeof fetch;
  private readonly stop = new AbortController();
  private readonly ctx: SessionContext;

  private lines: Line[] | null = null;
  private current: ClientSession | null = null;
  private sessionsStarted = 0;
  private lastSessionEnd = '';
  private waiters: (() => void)[] = [];

  constructor(private opts: ClientOptions) {
    const u = new URL(opts.entry);
    if (
      (u.protocol !== 'https:' && u.protocol !== 'http:') ||
      u.pathname !== '/' ||
      u.search ||
      u.hash
    ) {
      throw new Error(`entry ${opts.entry} must be an http(s) origin such as https://example.app`);
    }
    this.entry = u.origin;
    this.linkPath = opts.linkPath ?? '/multipath/v3/link';
    this.linesPath = opts.linesPath ?? '/multipath/v3/lines';
    this.site = opts.site === undefined ? u.hostname : opts.site;
    this.refreshMs = opts.refreshMs ?? 10 * 60_000;
    this.waitMs = opts.waitMs ?? 10_000;
    this.recvWindow = opts.recvWindow ?? DEFAULT_RECV_WINDOW;
    this.compression = opts.compression ?? true;
    this.buffered = opts.buffered ?? false;
    if (this.compression && typeof DecompressionStream !== 'function') {
      throw new Error(
        'compression needs DecompressionStream, which this platform lacks: pass compression: false',
      );
    }
    if (!this.buffered && typeof ReadableStream !== 'function') {
      throw new Error(
        'streamed bodies need ReadableStream, which this platform lacks: pass buffered: true',
      );
    }
    const ws = opts.WebSocket ?? (globalThis as { WebSocket?: WebSocketConstructor }).WebSocket;
    if (!ws) throw new Error('this platform has no WebSocket: pass one in the options');
    this.WebSocket = ws;
    this.fetchLines = opts.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.ctx = {
      log: opts.log ?? ((m) => console.info(`multipath: ${m}`)),
      emit: (e) => opts.onEvent?.(e),
      notify: () => this.notify(),
      WebSocket: this.WebSocket,
      dialAddresses: opts.dialAddresses ?? false,
      linkPath: this.linkPath,
      recvWindow: this.recvWindow,
    };
    void this.run();
    void this.refreshLoop();
  }

  // run starts one session after another until the client is closed.
  private async run(): Promise<void> {
    const signal = this.stop.signal;
    if (this.opts.store) this.lines = await this.loadStored();
    let retry = 250;
    while (!signal.aborted) {
      const kept = this.lines !== null;
      if (!kept) {
        // Nothing known yet: the list has to come first.
        if (!(await this.fetchList())) {
          await sleep(retry, signal);
          retry = Math.min(2 * retry, 30_000);
          continue;
        }
        retry = 250;
      }
      if (signal.aborted) return;
      const cs = new ClientSession(this.ctx, this.lines!);
      this.current = cs;
      this.sessionsStarted++;
      this.notify();
      if (kept) {
        this.ctx.log(`session ${cs.id} uses the kept line list while the entry is asked again`);
        void this.renew(cs);
      }
      const why = await cs.ended;
      this.lastSessionEnd = why;
      this.notify();
      if (cs.serverGone || why.startsWith(PEER_GOODBYE)) {
        const d = Math.floor(Math.random() * 2000);
        this.ctx.log(`starting a new session in ${d} ms`);
        await sleep(d, signal);
      }
    }
  }

  // renew asks the entry for the list until it answers or the session ends, and applies the answer
  // to the session, which until then runs on the kept list.
  private async renew(cs: ClientSession): Promise<void> {
    let retry = 1000;
    while (!cs.session.ended && !this.stop.signal.aborted) {
      if (await this.fetchList()) {
        cs.applyLines(this.lines!);
        return;
      }
      this.ctx.log(`session ${cs.id} stays on the kept line list, asking again in ${retry} ms`);
      await sleep(retry, cs.signal);
      retry = Math.min(2 * retry, 30_000);
    }
  }

  private async loadStored(): Promise<Line[] | null> {
    try {
      const stored = await this.opts.store!.load();
      if (stored === null) return null;
      if (stored.entry !== this.entry) {
        throw new Error(`it holds the list of entry ${String(stored.entry)}, not ${this.entry}`);
      }
      const checked = parseLines(stored);
      checkLines(checked, this.entry, this.site);
      this.ctx.log(`stored line list: ${checked.length} lines`);
      return checked;
    } catch (err) {
      this.ctx.log(`stored line list unusable, waiting for the entry's: ${String(err)}`);
      return null;
    }
  }

  // fetchList fetches the entry's list into this.lines and the store; it returns whether it did.
  private async fetchList(): Promise<boolean> {
    const url = this.entry + this.linesPath;
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), 10_000);
    try {
      const res = await this.fetchLines(url, { signal: abort.signal });
      if (!res.ok) throw new Error(`GET ${url}: ${res.status}`);
      const text = await res.text();
      if (text.length > 1 << 20) throw new Error(`GET ${url}: a line list of ${text.length} bytes`);
      const lines = parseLines(JSON.parse(text));
      checkLines(lines, this.entry, this.site);
      this.lines = lines;
      if (this.opts.store) {
        await this.opts.store
          .save({ entry: this.entry, lines })
          .catch((err: unknown) => this.ctx.log(`could not store the line list: ${String(err)}`));
      }
      return true;
    } catch (err) {
      this.ctx.log(`line list: ${String(err)}${this.lines ? '; keeping the lines known' : ''}`);
      return false;
    } finally {
      clearTimeout(timer);
    }
  }

  private async refreshLoop(): Promise<void> {
    const signal = this.stop.signal;
    for (;;) {
      await sleep(this.refreshMs, signal);
      if (signal.aborted) return;
      if ((await this.fetchList()) && this.current) this.current.applyLines(this.lines!);
    }
  }

  // notify wakes everyone waiting for the state to change.
  private notify(): void {
    for (const w of this.waiters.splice(0)) w();
  }

  async live(timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if (this.current?.isLive()) return true;
      const left = deadline - Date.now();
      if (left <= 0 || this.stop.signal.aborted) return false;
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, left);
        this.waiters.push(() => (clearTimeout(t), resolve()));
      });
    }
  }

  private async liveMux(): Promise<Mux> {
    if (this.stop.signal.aborted) throw new Error('multipath: client closed');
    if (!(await this.live(this.waitMs))) {
      throw new Error(`multipath: no live session within ${this.waitMs} ms`);
    }
    return this.current!.mux!;
  }

  async fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const request =
      input instanceof Request && !init
        ? input
        : new Request(input instanceof Request ? input : new URL(String(input), this.entry), init);
    const body = await serializeRequest(request, this.compression);
    const st = (await this.liveMux()).open();
    const abort = () => st.reset('aborted by the caller');
    request.signal?.addEventListener('abort', abort);
    try {
      if (request.signal?.aborted) throw new Error('multipath: aborted by the caller');
      await st.write(body);
      return await readResponse(st, {
        method: request.method,
        gzip: this.compression,
        buffered: this.buffered,
      });
    } catch (err) {
      st.reset(`request failed: ${String(err)}`);
      throw err;
    } finally {
      request.signal?.removeEventListener('abort', abort);
    }
  }

  async openWebSocket(path: string, protocols: string[] = []): Promise<MultipathWebSocket> {
    const st = (await this.liveMux()).open();
    return MultipathWebSocket.open(st, path, { host: new URL(this.entry).host, protocols });
  }

  stats(): ClientStats {
    const cs = this.current;
    return {
      session: cs ? cs.sessionStats() : null,
      lines: cs ? cs.lineStats() : [],
      streams: cs?.mux ? cs.mux.streams().sort((a, b) => a.id - b.id) : [],
      sessionsStarted: this.sessionsStarted,
      lastSessionEnd: this.lastSessionEnd,
    };
  }

  async close(): Promise<void> {
    if (this.stop.signal.aborted) return;
    this.stop.abort();
    this.notify();
    await this.current?.session.goodbye('client shutting down');
  }
}

// SessionContext is what a session needs of its client.
interface SessionContext {
  log(message: string): void;
  emit(e: ClientEvent): void;
  notify(): void;
  WebSocket: WebSocketConstructor;
  dialAddresses: boolean;
  linkPath: string;
  recvWindow: number;
}

interface Slot {
  index: number;
  line: Line;
  stop: AbortController;
  wake: (() => void) | null; // ends the slot's current wait: its link went down, or a redial is due
  redial: boolean; // the session fell silent: dial again even while a link is attached
  state: 'connecting' | 'up' | 'down';
  reconnects: number;
  reason: string;
}

class ServerLostError extends Error {}

// ClientSession is one session and its lines (PROTOCOL.md sections 4.2, 5.4 and 5.5). Every line has
// a slot: a loop that dials the line, performs the handshake, attaches the link, waits for it to die
// or for the session to fall silent, and dials again. Connection setup has CONNECT_TIMEOUT_MS, and
// the HELLO its own HANDSHAKE_TIMEOUT_MS from the moment the socket is open. A REJECT for an unknown session, or a second WELCOME that says it created the
// session, means the server lost the session (it restarted): the session ends at once.
class ClientSession {
  readonly key = crypto.getRandomValues(new Uint8Array(16));
  readonly id = hex(this.key.subarray(0, 4));
  readonly started = Date.now();
  readonly session: Session;
  readonly ended: Promise<string>;
  mux: Mux | null = null;
  serverGone = false;

  private stop = new AbortController();
  readonly signal = this.stop.signal; // aborted when the session ends
  private welcomed = false;
  private createdSeen = false;
  private maxLinks = defaultLimits().maxLinks;
  private server = '';
  private slots = new Map<number, Slot>();
  private leaving = new Map<number, Slot>(); // left the list; their links close once another is up

  constructor(
    private c: SessionContext,
    lines: Line[],
  ) {
    let ended: (why: string) => void = () => {};
    this.ended = new Promise((resolve) => (ended = resolve));
    // Until the first WELCOME the window is the smallest a server may announce, so whatever is
    // buffered before then fits any server's reorder span.
    this.session = new Session({
      limits: { ...defaultLimits(), window: MIN_WINDOW },
      deliver: (p) => this.mux!.feed(p), // data flows only after the first WELCOME, which made the mux
      onLink: (index, up, why) => {
        const sl = this.slots.get(index);
        if (!up && sl) {
          this.set(sl, 'down', why);
          c.log(`session ${this.id} line ${sl.line.id} down: ${why}`);
          sl.wake?.();
        }
        c.notify();
      },
      onSoft: (index, soft) => {
        const sl = this.slots.get(index);
        if (!sl) return;
        c.log(
          soft
            ? `session ${this.id} line ${sl.line.id} soft down: silent since the due ping round; no new data on it`
            : `session ${this.id} line ${sl.line.id} answers again`,
        );
      },
      onSilent: () => {
        c.log(`session ${this.id} silent: no line answers; redialling every line into it`);
        for (const sl of this.slots.values()) {
          sl.redial = true;
          sl.wake?.();
        }
      },
      onEnd: (why) => {
        this.stop.abort();
        this.mux?.close(why);
        const ageMs = Date.now() - this.started;
        c.log(`session ${this.id} end after ${ageMs} ms: ${why}`);
        c.emit({ type: 'session-end', session: this.id, ageMs, reason: why });
        ended(why);
      },
    });
    c.log(`session ${this.id} start over ${lines.length} lines`);
    c.emit({ type: 'session-start', session: this.id, lines: lines.length });
    this.applyLines(lines);
  }

  isLive(): boolean {
    return (
      this.mux !== null &&
      !this.session.ended &&
      [...this.slots.values()].some((s) => s.state === 'up')
    );
  }

  // applyLines makes the session's lines match the list: lines that left it are closed, new lines
  // take a free link index and are dialled.
  applyLines(lines: Line[]): void {
    if (this.session.ended) return;
    const key = (l: Line) => `${l.id} ${l.url} ${l.address ?? ''}`;
    const keep = new Set(lines.map(key));
    for (const [i, sl] of this.slots) {
      if (keep.delete(key(sl.line))) continue;
      sl.stop.abort();
      this.slots.delete(i);
      this.leaving.set(i, sl);
      this.c.log(`session ${this.id} line ${sl.line.id} left the list`);
    }
    this.closeLeaving();
    for (const line of lines) {
      if (!keep.has(key(line))) continue;
      let index = 0;
      while (index < this.maxLinks && (this.slots.has(index) || this.leaving.has(index))) index++;
      if (index >= this.maxLinks) {
        this.c.log(
          `session ${this.id} line ${line.id} ignored: the server allows ${this.maxLinks} links`,
        );
        continue;
      }
      const stop = new AbortController();
      this.stop.signal.addEventListener('abort', () => stop.abort());
      const sl: Slot = {
        index,
        line,
        stop,
        wake: null,
        redial: false,
        state: 'connecting',
        reconnects: 0,
        reason: '',
      };
      this.slots.set(index, sl);
      void this.runSlot(sl);
    }
  }

  // closeLeaving closes the links of lines that left the list, once a line still in it is up: a
  // replaced list must not end a session that works.
  private closeLeaving(): void {
    if (![...this.slots.values()].some((s) => s.state === 'up')) return;
    for (const [i, sl] of this.leaving) this.session.detach(i, `line ${sl.line.id} left the list`);
    this.leaving.clear();
  }

  private set(sl: Slot, state: Slot['state'], reason: string): void {
    if (state === 'up' && sl.state !== 'connecting') sl.reconnects++; // every attach after the first
    const was = sl.state;
    sl.state = state;
    if (reason) sl.reason = reason;
    if ((state === 'up') !== (was === 'up')) {
      this.c.emit({
        type: 'line',
        session: this.id,
        index: sl.index,
        id: sl.line.id,
        up: state === 'up',
        reason,
      });
    }
    if (state === 'up') this.closeLeaving();
    this.c.notify();
  }

  // runSlot keeps one line linked for as long as the slot lasts.
  private async runSlot(sl: Slot): Promise<void> {
    const signal = sl.stop.signal;
    let backoff = 200;
    while (!signal.aborted) {
      try {
        const link = await this.dial(sl);
        if (signal.aborted) {
          link.close(); // the slot ended while dialling; nothing is waiting for this link
          return;
        }
        this.session.attach(sl.index, link);
        sl.redial = false; // a silence reported while this link was dialled is answered by it
        this.set(sl, 'up', '');
        this.c.log(`session ${this.id} line ${sl.line.id} up`);
        backoff = 200;
        await this.waitLink(sl);
      } catch (err) {
        if (signal.aborted) return;
        if (err instanceof ServerLostError) {
          this.serverGone = true;
          this.session.end(err.message); // the server does not know the session: no one to say goodbye to
          return;
        }
        const why = err instanceof Error ? err.message : String(err);
        // A link this dial was to replace may still be attached; the line is up while it is.
        if (this.session.linkState(sl.index).attached) sl.reason = why;
        else this.set(sl, 'down', why);
        // Jitter keeps many clients, or many lines of one client, from retrying in lockstep.
        const wait = Math.floor(backoff / 2 + Math.random() * backoff);
        this.c.log(`session ${this.id} line ${sl.line.id}: ${why}; retrying in ${wait} ms`);
        if (!sl.redial) await this.pause(sl, wait);
        backoff = Math.min(2 * backoff, 5000);
        // An old link that answers again needs no replacement until it dies or the session is
        // silent again.
        if (sl.redial) sl.redial = false;
        else if (this.session.linkState(sl.index).answering) await this.waitLink(sl);
      }
    }
  }

  // waitLink resolves once the slot's link is gone, the session asks for a redial, or the slot ends.
  private async waitLink(sl: Slot): Promise<void> {
    while (!sl.stop.signal.aborted && !sl.redial && this.session.linkState(sl.index).attached) {
      await this.pause(sl, Infinity);
    }
    if (sl.redial && !sl.stop.signal.aborted) {
      this.c.log(`session ${this.id} line ${sl.line.id}: redialling while its link stays attached`);
    }
    sl.redial = false;
  }

  // pause resolves after ms, or sooner when the slot is woken or ends.
  private pause(sl: Slot, ms: number): Promise<void> {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(t);
        sl.wake = null;
        sl.stop.signal.removeEventListener('abort', done);
        resolve();
      };
      const t = Number.isFinite(ms) ? setTimeout(done, ms) : undefined;
      sl.wake = done;
      if (sl.stop.signal.aborted) return done();
      sl.stop.signal.addEventListener('abort', done);
    });
  }

  private async dial(sl: Slot): Promise<Link> {
    const cfg = this.c;
    const hello = {
      version: VERSION,
      session: this.key,
      link: sl.index,
      mode: this.welcomed ? Mode.Join : Mode.New,
      impl: IMPLEMENTATION,
    };
    const address = cfg.dialAddresses && sl.line.address !== undefined;
    let dialled: { link: Link; welcome: WelcomeFrame };
    try {
      dialled = await Link.dial(
        linkUrl(sl.line, cfg.linkPath, address),
        hello,
        cfg.WebSocket,
        HANDSHAKE_TIMEOUT_MS,
        CONNECT_TIMEOUT_MS,
        address ? new URL(sl.line.url).host : undefined,
      );
    } catch (err) {
      if (err instanceof RejectedError && err.code === RejectCode.UnknownSession) {
        throw new ServerLostError(`the server lost the session: ${err.message}`);
      }
      throw err;
    }
    try {
      this.accept(dialled.welcome, cfg.recvWindow);
    } catch (err) {
      dialled.link.close(); // the error is what the caller reports
      throw err;
    }
    return dialled.link;
  }

  // accept takes the limits of the first WELCOME and checks that later ones still hold the session.
  private accept(w: WelcomeFrame, recvWindow: number): void {
    if (w.created && this.createdSeen) {
      throw new ServerLostError(
        'the server lost the session: it created the session a second time',
      );
    }
    this.createdSeen ||= w.created;
    if (this.welcomed) return;
    this.session.setLimits(w.limits);
    this.welcomed = true;
    this.mux = new Mux(this.session, w.limits, Math.max(recvWindow, w.limits.streamWindow));
    this.maxLinks = w.limits.maxLinks;
    this.server = w.impl;
    for (const [i, sl] of this.slots) {
      if (i < this.maxLinks) continue;
      sl.stop.abort();
      this.slots.delete(i);
      this.c.log(
        `session ${this.id} line ${sl.line.id} dropped: the server allows ${this.maxLinks} links`,
      );
    }
    this.c.log(`session ${this.id} accepted by ${w.impl}, protocol ${w.version}`);
  }

  sessionStats(): NonNullable<ClientStats['session']> {
    const s = this.session.stats();
    return {
      id: this.id,
      state: this.session.ended ? 'ended' : this.welcomed ? 'live' : 'connecting',
      ageMs: Date.now() - this.started,
      server: this.server,
      sent: s.sendNext,
      unacked: s.sendNext - s.sendBase,
      received: s.deliverNext,
      reorder: s.reorder,
    };
  }

  lineStats(): ClientStats['lines'] {
    const links = this.session.stats().links;
    const now = Date.now();
    return [...this.slots.values()]
      .sort((a, b) => a.index - b.index)
      .map((sl) => {
        const l = links.find((x) => x.index === sl.index);
        return {
          index: sl.index,
          id: sl.line.id,
          url: sl.line.url,
          ...(this.c.dialAddresses && sl.line.address ? { address: sl.line.address } : {}),
          state: sl.state,
          soft: l?.soft ?? false,
          rttMs: l?.rttMs ?? 0,
          lastByteAgoMs: l ? now - l.lastIn : 0,
          bytesIn: l?.bytesIn ?? 0,
          bytesOut: l?.bytesOut ?? 0,
          reconnects: sl.reconnects,
          lastReason: sl.reason,
        };
      });
  }
}
