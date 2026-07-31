/*
 *  Description: The line manager — the one place an outbound request chooses a network path.
 *
 *               MP-1 scope, deliberately: it resolves a path against the registry and issues the
 *               request over the selected line, and that is all. No probing, no hedging, no
 *               failover — those are MP-3/MP-4 and land here without changing this file's shape,
 *               because "which line" is already a single method (`select`) and "issue it" is
 *               already a single method (`dispatch`).
 *
 *               Note what is *not* public: there is no `request(path)`. The only outbound door is
 *               `fetchApi`, the adapter for a client generated from the API contract, so the only
 *               requests that can reach a line are ones the contract describes. Hand-rolled fetches
 *               are excluded by construction rather than by convention.
 *
 *               Doing the plumbing first, under a one-line registry, is what makes the racing
 *               stages safe to add: by then the app already routes every request through here,
 *               and the only thing left to change is the choice.
 *
 *  Author(s):
 *      agent4
 */

import { HealthTable, type HealthOptions, type LineHealth } from "./health.js";
import { Prober, type ProberOptions } from "./prober.js";
import { SAME_ORIGIN_REGISTRY, type Line, type Registry } from "./registry.js";
import {
  STRATEGY_DEFAULTS,
  hedgedRead,
  writeWithFailover,
  type Attemptor,
  type StrategyOptions,
} from "./strategy.js";

/**
 * The origin the OpenAPI-generated client is configured with. It is never contacted: the generated
 * client insists on building absolute URLs from its `basePath`, so we hand it a reserved-by-RFC2606
 * `.invalid` host and strip it back off inside `fetchApi`. If one ever escaped the adapter it would
 * fail DNS resolution immediately rather than reaching some real server — which is exactly the
 * failure mode you want from a sentinel.
 */
export const SENTINEL_ORIGIN = "https://multipath.invalid";

export interface LineManagerOptions {
  /** The lines to route over. Defaults to a single same-origin line: no behaviour change. */
  readonly registry?: Registry;
  /** Injected for tests; defaults to the global `fetch`. */
  readonly fetch?: typeof globalThis.fetch;
  /** Called after every attempt. The developer panel (MP-5) is a consumer of this, nothing more. */
  readonly onAttempt?: (attempt: Attempt) => void;
  /**
   * Methods whose requests get an idempotency key. Reads are absent because they are already
   * idempotent; PUT because a well-formed PUT is too.
   */
  readonly idempotentMethods?: readonly string[];
  /** Header the key travels in. Must match the server filter's. */
  readonly idempotencyHeader?: string;
  /** Injected for tests; defaults to `crypto.randomUUID`. */
  readonly newKey?: () => string;
  /** Tuning for the health table: smoothing, how many failures mean down, what counts as slow. */
  readonly health?: HealthOptions;
  /** Tuning for the measurement loop. Probing does not begin until `start()` is called. */
  readonly probe?: ProberOptions;
  /** Hedge delay and timeouts. */
  readonly strategy?: StrategyOptions;
  /**
   * Where to keep measurements between visits. Omit and nothing is persisted.
   *
   * Worth doing because the alternative for a cold start is the registry's fixed order, which is a
   * guess that never improves. With it, the second visit onward begins from what was actually
   * measured — which is the only way to tell a stable-but-slow line from a fast one, since both
   * answer a probe promptly.
   */
  readonly storage?: Storage;
  /** Key under which measurements are kept. */
  readonly storageKey?: string;
  /** Measurements older than this are ignored: last month's network says nothing about today's. */
  readonly storageMaxAgeMs?: number;
}

/** What one request-over-one-line did. Purely observational — never load-bearing. */
export interface Attempt {
  readonly lineId: string;
  readonly method: string;
  readonly path: string;
  /** Wall-clock milliseconds from issue to response or failure. */
  readonly durationMs: number;
  readonly status?: number;
  readonly error?: unknown;
}

export class NoLineAvailableError extends Error {
  constructor() {
    super("no line available to serve the request");
    this.name = "NoLineAvailableError";
  }
}

export class LineManager {
  private registry: Registry;
  private readonly fetchImpl: typeof globalThis.fetch;
  private readonly onAttempt: ((attempt: Attempt) => void) | undefined;
  private readonly idempotentMethods: ReadonlySet<string>;
  private readonly idempotencyHeader: string;
  private readonly newKey: () => string;
  private readonly healthTable: HealthTable;
  private readonly prober: Prober;
  private readonly strategy: Required<StrategyOptions>;
  private readonly storage: Storage | undefined;
  private readonly storageKey: string;
  private readonly storageMaxAgeMs: number;

  constructor(options: LineManagerOptions = {}) {
    this.registry = options.registry ?? SAME_ORIGIN_REGISTRY;
    // Bound: an unbound global fetch throws "Illegal invocation" in a browser.
    this.fetchImpl = (options.fetch ?? globalThis.fetch).bind(globalThis);
    this.onAttempt = options.onAttempt;
    this.idempotentMethods = new Set(
      (options.idempotentMethods ?? ["POST", "PATCH", "DELETE"]).map((m) => m.toUpperCase()),
    );
    this.idempotencyHeader = options.idempotencyHeader ?? "Idempotency-Key";
    this.newKey = options.newKey ?? (() => crypto.randomUUID());
    this.healthTable = new HealthTable(options.health);
    this.storage = options.storage;
    this.storageKey = options.storageKey ?? "multipath:health";
    this.storageMaxAgeMs = options.storageMaxAgeMs ?? 7 * 24 * 60 * 60 * 1000;
    this.restoreHealth();
    this.strategy = { ...STRATEGY_DEFAULTS, ...options.strategy };
    this.prober = new Prober(
      () => this.registry.lines,
      this.healthTable,
      this.fetchImpl,
      (path, line) => this.resolve(path, line),
      options.probe ?? {},
    );
  }

  /**
   * Begin measuring the lines.
   *
   * Separate from construction because probing costs real requests, and a library that starts
   * making them the moment it is instantiated is one that surprises people. Until this is called
   * the manager still works — it just ranks on configured weight, having measured nothing.
   */
  start(): void {
    this.prober.start();
  }

  stop(): void {
    this.prober.stop();
  }

  /** Probe every line now and wait for the answers. */
  async probeNow(): Promise<void> {
    await this.prober.probeAll();
    // Saved as measurements arrive rather than on unload: a page can be closed, backgrounded or
    // discarded at any moment, and an unload handler is the least reliable place to do work.
    this.saveHealth();
  }

  /** Everything measured about every line — the developer panel's whole data source. */
  health(): LineHealth[] {
    return this.registry.lines.map((line) => this.healthTable.get(line.id));
  }

  /**
   * Line ids in remembered preference order, for a launcher to use on the next cold start.
   *
   * The launcher races every line regardless; this only decides who is asked first among lines that
   * are all reachable, which is the part racing cannot settle.
   */
  preferredLineIds(): string[] {
    return this.ranked().map((line) => line.id);
  }

  /** Keep what has been measured, so the next visit does not start from a guess. */
  saveHealth(): void {
    if (!this.storage) return;
    try {
      this.storage.setItem(this.storageKey, JSON.stringify(this.healthTable.export_()));
    } catch {
      // A full or unavailable store is not worth failing a request over.
    }
  }

  private restoreHealth(): void {
    if (!this.storage) return;
    try {
      const raw = this.storage.getItem(this.storageKey);
      if (!raw) return;
      const parsed: unknown = JSON.parse(raw);
      if (!Array.isArray(parsed)) return;
      this.healthTable.import_(parsed, this.storageMaxAgeMs, Date.now());
    } catch {
      // Corrupt or foreign data: start from nothing rather than from something misread.
    }
  }

  /** Swap the registry at runtime — a refreshed `GET /mt/lines` is the expected caller. */
  setRegistry(registry: Registry): void {
    this.registry = registry;
    // Drop health for lines that no longer exist, so a re-added id cannot inherit the reputation of
    // a line that happened to share its name.
    this.healthTable.retain(registry.lines.map((line) => line.id));
  }

  get lines(): readonly Line[] {
    return this.registry.lines;
  }

  /**
   * Lines in preference order, best first.
   *
   * Down lines are ranked last but not dropped: if everything is down the caller still has to send
   * the request somewhere, and refusing to try is worse than trying the least-bad option.
   */
  ranked(): Line[] {
    return this.healthTable.rank(this.registry.lines);
  }

  /** The line a request starts on. With hedging and failover, others may follow it. */
  select(): Line {
    const line = this.ranked()[0];
    if (!line) throw new NoLineAvailableError();
    return line;
  }

  /**
   * Absolute-or-relative URL for `path` over `line`.
   *
   * A same-origin line returns the path unchanged, so a single-line deployment emits byte-identical
   * URLs to the ones it emitted before MultiPath was introduced. That equivalence is the whole
   * acceptance criterion for MP-1, so it is one line of code on purpose.
   */
  resolve(path: string, line: Line = this.select()): string {
    if (!path.startsWith("/")) {
      throw new TypeError(`path must start with "/", got ${JSON.stringify(path)}`);
    }
    return line.url === "" ? path : line.url + path;
  }

  /**
   * Issue one request over the selected line.
   *
   * Private on purpose. `fetchApi` is the only way out of this class, which means the only way to
   * get a request onto MultiPath's lines is through a client generated from the API contract. A
   * hand-rolled `fetch` cannot opt in — not because it is inelegant, but because it is unreviewed
   * against the contract, and a rule that is merely written down is a rule that erodes. Anything
   * that needs a line therefore has to be in the contract first.
   */
  private async dispatch(path: string, init: RequestInit = {}): Promise<Response> {
    const lines = this.ranked();
    if (lines.length === 0) throw new NoLineAvailableError();

    const method = (init.method ?? "GET").toUpperCase();
    const attempt: Attemptor = (line, signal) =>
      this.dispatchOver(line, path, withSignal(init, signal));

    // The split is the strategy: a read may be asked of several lines because two copies of an
    // answer are one answer; a write may not, because two writes are two writes.
    return this.idempotentMethods.has(method)
      ? writeWithFailover(lines, attempt, this.strategy, init.signal ?? undefined)
      : hedgedRead(lines, attempt, this.strategy, init.signal ?? undefined);
  }

  /** One request, one line. Everything above decides which lines and how many. */
  private async dispatchOver(line: Line, path: string, init: RequestInit): Promise<Response> {
    // Real traffic is what the throughput probe waits to be absent, so it has to be told.
    this.prober.noteTraffic();
    const url = this.resolve(path, line);
    const method = (init.method ?? "GET").toUpperCase();
    // Any line with an absolute url is a different origin from the page — a different subdomain is
    // enough, and a different port on the same host is too. `fetch` defaults to
    // `credentials: "same-origin"`, so without this the request goes out with no cookie and the
    // server sees an anonymous caller. It fails as "logged out on that line", which looks like an
    // auth bug rather than a transport one.
    //
    // The same-origin line is left exactly as it came in, so the single-line no-op holds literally.
    const effective: RequestInit =
      line.url === "" ? init : { credentials: "include" as const, ...init };

    const started = now();
    try {
      const response = await this.fetchImpl(url, effective);
      this.report({
        lineId: line.id,
        method,
        path,
        durationMs: now() - started,
        status: response.status,
      });
      return response;
    } catch (error) {
      this.report({ lineId: line.id, method, path, durationMs: now() - started, error });
      throw error;
    }
  }

  /**
   * Give a write a key to be recognised by, unless the caller supplied one.
   *
   * Reads are returned exactly as they came in — same object, no clone — so that the single-line
   * no-op guarantee holds for them literally and not just morally.
   *
   * An explicit key always wins: the caller may know that two calls are the same logical write in
   * a way MultiPath cannot, and that knowledge must not be overwritten.
   */
  private withIdempotencyKey(init: RequestInit): RequestInit {
    const method = (init.method ?? "GET").toUpperCase();
    if (!this.idempotentMethods.has(method)) return init;

    const headers = new Headers(init.headers);
    if (headers.has(this.idempotencyHeader)) return init;

    headers.set(this.idempotencyHeader, this.newKey());
    return { ...init, headers };
  }

  private report(attempt: Attempt): void {
    // Observation must never be able to fail a request.
    try {
      this.onAttempt?.(attempt);
    } catch {
      /* ignored on purpose */
    }
  }

  /**
   * The one way out. A `fetch`-shaped function to hand to a generated OpenAPI client's
   * `Configuration.fetchApi`.
   *
   * The generated client has already concatenated `basePath` and the operation path by the time it
   * calls this, so the adapter's job is to undo that: strip the sentinel origin back off and route
   * the remaining path itself. This is what lets an entirely generated client — which we must not
   * edit, since codegen overwrites it on every build — go over MultiPath with no patch at all.
   *
   * It is also the entire public outbound surface, deliberately: see `dispatch`.
   */
  fetchApi(): typeof globalThis.fetch {
    return this.boundFetchApi;
  }

  /**
   * Stable identity: the same function every time, so a caller that rebuilds its client
   * configuration per request is not handed a fresh closure per request.
   */
  private readonly boundFetchApi: typeof globalThis.fetch = (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (!url.startsWith(SENTINEL_ORIGIN)) {
      // Not ours to route (an absolute URL to some third party); pass it straight through rather
      // than guessing.
      return this.fetchImpl(input, init);
    }
    const path = url.slice(SENTINEL_ORIGIN.length) || "/";

    if (typeof input === "string" || input instanceof URL) {
      return this.dispatch(path, this.withIdempotencyKey(init ?? {}));
    }
    // A Request carries its body as a stream that can be read once. Buffer it before doing
    // anything else: dropping it would silently send a write with no payload, and MP-4 has to be
    // able to send those same bytes again over another line, which a consumed stream cannot do.
    return requestInitFrom(input, init).then((merged) =>
      this.dispatch(path, this.withIdempotencyKey(merged)),
    );
  };
}

/**
 * Flatten a `Request` into an init, buffering its body.
 *
 * Everything a duplicate or a failover would need has to survive being sent more than once, and a
 * request body arrives as a single-use stream. Reading it here is what makes a retry possible at
 * all — and what stops a write from going out empty.
 */
async function requestInitFrom(request: Request, override?: RequestInit): Promise<RequestInit> {
  const base: RequestInit = {
    method: request.method,
    headers: request.headers,
    credentials: request.credentials,
    mode: request.mode,
    signal: request.signal,
  };
  // GET and HEAD may not carry one, and asking for it throws in some runtimes.
  if (request.method !== "GET" && request.method !== "HEAD") {
    const body = await request.clone().arrayBuffer();
    if (body.byteLength > 0) base.body = body;
  }
  return { ...base, ...override };
}

/**
 * Attach the strategy's abort signal without discarding the caller's own init.
 *
 * A hedge has to be able to cancel the losers, and a write attempt has to be able to time out; both
 * need a signal of their own. The caller's signal is honoured separately, at the strategy level.
 */
function withSignal(init: RequestInit, signal: AbortSignal): RequestInit {
  return { ...init, signal };
}

function now(): number {
  return typeof performance !== "undefined" ? performance.now() : Date.now();
}
