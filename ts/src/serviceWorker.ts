// The browser integration point: a service worker that intercepts fetch and routes it over the
// multipath substrate instead of the network. The application keeps calling fetch() as it always
// did; underneath, every request becomes a mux stream over one redundant transport across all lines,
// and nothing in the app learns that more than one path exists.
//
// The scope and event are typed by the minimum this needs, so the package compiles against the DOM
// lib alone without pulling in the WebWorker lib (whose globals collide with the DOM's).

import { Client, ClientOptions } from './client.js';

export interface FetchEventLike {
  readonly request: Request;
  respondWith(response: Response | Promise<Response>): void;
}

export interface FetchScope {
  addEventListener(type: 'fetch', listener: (event: FetchEventLike) => void): void;
}

export interface RouterOptions extends ClientOptions {
  // The registered service name intercepted requests are routed to. The origin must have registered
  // it (e.g. the app's own HTTP service). Default "app".
  service?: string;
  // An opaque ticket sent with each routed request for the origin's handler to authorise.
  ticket?: Uint8Array;
  // Decide per request whether to route it over multipath; return false to let it hit the network
  // untouched. Default: route everything.
  shouldRoute?: (request: Request) => boolean;
  // Injected in tests; defaults to Client.dial.
  dial?: (lines: string[], opts: ClientOptions) => Promise<Client>;
}

// installFetchRouter wires a service worker's fetch handler to the substrate. The client is dialled
// lazily on the first routed request and reused; a failed dial is retried on the next request rather
// than cached, so a transient startup failure does not wedge the worker.
export function installFetchRouter(
  scope: FetchScope,
  lines: string[],
  opts: RouterOptions = {},
): void {
  const dial = opts.dial ?? Client.dial;
  const service = opts.service ?? 'app';
  let pending: Promise<Client> | null = null;
  const client = (): Promise<Client> => {
    if (pending === null) {
      pending = dial(lines, opts).catch((err) => {
        pending = null; // let the next request try again
        throw err;
      });
    }
    return pending;
  };

  scope.addEventListener('fetch', (event) => {
    if (opts.shouldRoute && !opts.shouldRoute(event.request)) return;
    event.respondWith(client().then((c) => c.fetch(service, event.request, opts.ticket)));
  });
}

// ---------------------------------------------------------------------------------------------
// A router that knows where its lines come from.
//
// installFetchRouter is handed its lines once. A worker the browser restarts knows nothing, and the
// lines a deployment publishes change, so createLineRouter keeps them: it dials at once over the
// lines it can name without asking (seed, typically the worker's own origin) plus the ones it held
// last time (store), asks for the current list in the background (discover), adds every line it
// had not heard of to the client — a dial still in progress included, so a new line can win the
// race it joins — and keeps the union for next time.
//
// The substrate is the way out, not the only way: a request that finds no line up within
// fallbackAfterMs, or a GET/HEAD the substrate failed, goes to the network as it would have without
// a worker. So does a GET/HEAD while no line is up at all, and one the substrate has not answered
// within requestTimeoutMs: a line that died silently is only noticed when it has been quiet for a
// while, and until then a request over it neither succeeds nor fails — a page waiting for its
// "load" would wait forever. The request left behind on the substrate is not cancelled; an answer
// arriving late is simply dropped. A substrate the origin refused outright is thrown away and the next request dials a
// fresh one.

// LineStore keeps the line list across worker restarts. load() resolves with what was saved, [] when
// nothing was (and never rejects for a missing entry).
export interface LineStore {
  load(): Promise<string[]>;
  save(lines: string[]): Promise<void>;
}

interface CachesLike {
  open(name: string): Promise<{
    match(key: string): Promise<Response | undefined>;
    put(key: string, response: Response): Promise<void>;
  }>;
}

// cacheLineStore keeps the list as JSON under key in the Cache Storage cache named cacheName — the
// one store a service worker has that survives it being stopped. key is a URL nothing serves.
export function cacheLineStore(
  cacheName: string,
  key: string,
  caches: CachesLike = (globalThis as unknown as { caches: CachesLike }).caches,
): LineStore {
  return {
    async load() {
      try {
        const held = await (await caches.open(cacheName)).match(key);
        const lines: unknown = held ? await held.json() : [];
        return Array.isArray(lines) ? lines.filter((l): l is string => typeof l === 'string') : [];
      } catch {
        return [];
      }
    },
    async save(lines) {
      const body = JSON.stringify(lines);
      await (
        await caches.open(cacheName)
      ).put(key, new Response(body, { headers: { 'Content-Type': 'application/json' } }));
    },
  };
}

// The part of Client a router uses; injected in tests.
export interface RoutedClient {
  ready(): Promise<void>;
  readonly closed: boolean;
  stats?(): { state: string }[];
  addLines(lines: string[]): number;
  fetch(service: string, request: Request, ticket?: Uint8Array): Promise<Response>;
}

export interface LineRouterOptions extends ClientOptions {
  // Lines known without asking anybody — e.g. [wss://<the worker's host>]. Always dialled.
  seed: string[];
  // Where the list is kept between worker restarts. Default: kept in memory only.
  store?: LineStore;
  // Fetches the deployment's current lines. Called once per router (and again by refresh()); a
  // rejection keeps the lines already known.
  discover?: () => Promise<string[]>;
  // The registered service requests are routed to. Default "app".
  service?: string;
  ticket?: Uint8Array;
  // How long after a dial starts requests wait for its first line before going to the network.
  // Counted from the dial, not from each request: once it has passed, a request that finds no line
  // up goes to the network at once instead of waiting again. Default 3000.
  fallbackAfterMs?: number;
  // How long a GET/HEAD waits for the substrate's answer before going to the network. Default 10000.
  requestTimeoutMs?: number;
  // Where a request goes when the substrate cannot take it. Default: globalThis.fetch.
  fallback?: (request: Request) => Promise<Response>;
  // Injected in tests; defaults to Client.connect.
  connect?: (lines: string[], opts: ClientOptions) => RoutedClient;
  // Told about every change of the known lines (after discovery, say), for a log.
  onLines?: (lines: string[]) => void;
}

export interface LineRouter {
  // fetch(request) over the substrate when it can, over the network when it cannot.
  fetch(request: Request): Promise<Response>;
  // The lines known now: seed, stored and discovered, without repeats.
  lines(): string[];
  // Ask discover() again and add what it names.
  refresh(): Promise<void>;
}

export function createLineRouter(opts: LineRouterOptions): LineRouter {
  const connect = opts.connect ?? ((lines: string[], o: ClientOptions) => Client.connect(lines, o));
  const service = opts.service ?? 'app';
  const fallbackAfterMs = opts.fallbackAfterMs ?? 3000;
  const requestTimeoutMs = opts.requestTimeoutMs ?? 10000;
  const fallback = opts.fallback ?? ((r: Request) => fetch(r));
  let known: string[] = [...new Set(opts.seed)];
  let loaded: Promise<void> | null = null;
  let client: RoutedClient | null = null;
  let dialledAt = 0;

  const learn = (lines: string[]): boolean => {
    const fresh = lines.filter((l) => !known.includes(l));
    if (!fresh.length) return false;
    known = [...known, ...fresh];
    client?.addLines(fresh);
    opts.onLines?.(known);
    return true;
  };

  const refresh = async () => {
    if (!opts.discover) return;
    try {
      if (learn(await opts.discover())) await opts.store?.save(known);
    } catch {
      // The lines already known still work; the next refresh may do better.
    }
  };

  const start = (): Promise<void> => {
    if (!loaded) {
      loaded = (async () => {
        learn((await opts.store?.load()) ?? []);
        void refresh();
      })();
    }
    return loaded;
  };

  const live = async (): Promise<RoutedClient> => {
    await start();
    if (!client || client.closed) {
      client = connect(known, opts);
      dialledAt = Date.now();
    }
    return client;
  };

  const within = <T>(p: Promise<T>, ms: number): Promise<T | 'late'> =>
    new Promise((resolve, reject) => {
      const t = setTimeout(() => resolve('late'), ms);
      p.then(
        (v) => (clearTimeout(t), resolve(v)),
        (e) => (clearTimeout(t), reject(e)),
      );
    });

  return {
    lines: () => [...known],
    refresh,
    async fetch(request) {
      const retryable = request.method === 'GET' || request.method === 'HEAD';
      const spare = retryable ? null : request.clone();
      const c = await live();
      const wait = Math.max(0, fallbackAfterMs - (Date.now() - dialledAt));
      if ((await within(c.ready(), wait)) === 'late' || c.closed) {
        return fallback(spare ?? request);
      }
      try {
        if (!retryable) return await c.fetch(service, request, opts.ticket);
        if (c.stats && !c.stats().some((s) => s.state === 'up')) return await fallback(request);
        const answer = await within(
          c.fetch(service, request.clone(), opts.ticket),
          requestTimeoutMs,
        );
        return answer === 'late' ? await fallback(request) : answer;
      } catch (err) {
        if (!retryable) throw err;
        return fallback(request);
      }
    },
  };
}
