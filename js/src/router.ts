// The service worker's side: requests the browser makes for the page (the document, scripts, fonts,
// images) are what a worker can intercept, and the line router carries them over a Client instead of
// the network. The worker serves only those; the page holds its own Client for everything it fetches
// itself, so a page has one session and its worker another.
//
// The router is a way out, not the only way. A request goes to the network instead (the fallback),
// logged and reported as a "fallback" ClientEvent with its reason, when no session is live within
// fallbackAfterMs of the router's start (counted once, so later requests do not wait again), and a
// GET or HEAD also when the client fails it or has not answered its head within requestTimeoutMs;
// the abandoned stream is reset. Any other method is never sent twice: once it may have reached the
// server, its failure is the caller's.
//
// Static-file integrity. A file whose path matches an IntegrityRule carries a hash of its content in
// its name; its body is checked against that hash as it streams through. Every byte but the last
// piece passes at once, so progress stays visible; the last piece is released only after the digest
// matched. On a mismatch the body fails instead of ending, so no cache that stores it (the worker's,
// a loader's) ever keeps it, the mismatch is logged and reported as a fallback, and from then on that
// URL goes to the network. A rule's pattern has one capture group, the hex hash, which must be a
// prefix of the content's digest under the rule's algorithm; the first rule whose pattern matches
// the path decides.
//
//   - createLineRouter(options) starts a Client at once (LineRouterOptions: the ClientOptions plus
//     fallbackAfterMs, default 3000; requestTimeoutMs, default 10000; fallback, default the
//     platform's fetch; integrity, default none; client, a stand-in for tests) and returns a
//     LineRouter: fetch(request) answers a request over the lines or the network, stats() is the
//     client's ClientStats, close() closes the client.
//   - cacheLineStore(cacheName, key, caches) is a LineStore in Cache Storage, the one store a service
//     worker keeps across restarts: the StoredLines as JSON under key (a URL nothing serves) in
//     cacheName. The client checks what it loads.
//
//     const router = createLineRouter({
//       entry: self.location.origin,
//       store: cacheLineStore('lines', new URL('__lines', self.location.href).href),
//       integrity: [{ pattern: /-([0-9a-f]{32})\.js$/, algorithm: 'MD5' }],
//     });
//     self.addEventListener('fetch', (e) => e.respondWith(router.fetch(e.request)));
import {
  Client,
  type ClientOptions,
  type ClientStats,
  type LineStore,
  type StoredLines,
} from './client.js';
import { hexDigest, type HashAlgorithm } from './hash.js';

export interface IntegrityRule {
  pattern: RegExp;
  algorithm: HashAlgorithm;
}

// The part of Client a router uses.
export interface RouterClient {
  live(timeoutMs: number): Promise<boolean>;
  fetch(request: Request): Promise<Response>;
  stats(): ClientStats;
  close(): Promise<void>;
}

export interface LineRouterOptions extends ClientOptions {
  fallbackAfterMs?: number;
  requestTimeoutMs?: number;
  fallback?: (request: Request) => Promise<Response>;
  integrity?: IntegrityRule[];
  client?: RouterClient;
}

export interface LineRouter {
  fetch(request: Request): Promise<Response>;
  stats(): ClientStats;
  close(): Promise<void>;
}

interface CachesLike {
  open(name: string): Promise<{
    match(key: string): Promise<Response | undefined>;
    put(key: string, response: Response): Promise<void>;
  }>;
}

export function cacheLineStore(
  cacheName: string,
  key: string,
  caches: CachesLike = (globalThis as unknown as { caches: CachesLike }).caches,
): LineStore {
  return {
    async load(): Promise<StoredLines | null> {
      const held = await (await caches.open(cacheName)).match(key);
      return held ? ((await held.json()) as StoredLines) : null;
    },
    async save(stored) {
      const body = JSON.stringify(stored);
      const headers = { 'Content-Type': 'application/json' };
      await (await caches.open(cacheName)).put(key, new Response(body, { headers }));
    },
  };
}

export function createLineRouter(opts: LineRouterOptions): LineRouter {
  const log = opts.log ?? ((m: string) => console.info(`multipath: ${m}`));
  const client = opts.client ?? new Client(opts);
  const fallbackAfterMs = opts.fallbackAfterMs ?? 3000;
  const requestTimeoutMs = opts.requestTimeoutMs ?? 10_000;
  const network = opts.fallback ?? ((r: Request) => fetch(r));
  const rules = opts.integrity ?? [];
  const tainted = new Set<string>();
  const started = Date.now();

  const fallback = (request: Request, reason: string): Promise<Response> => {
    log(`${request.method} ${request.url} goes to the network: ${reason}`);
    opts.onEvent?.({ type: 'fallback', url: request.url, reason });
    return network(request);
  };

  // verified passes response through, checking its body against the hash its URL names.
  const verified = (url: string, response: Response): Response => {
    const rule = rules.find((r) => r.pattern.test(new URL(url).pathname));
    const want = rule?.pattern.exec(new URL(url).pathname)?.[1]?.toLowerCase();
    if (!rule || !want || response.status !== 200 || response.body === null) return response;
    const parts: Uint8Array[] = [];
    let last: Uint8Array | null = null;
    const check = new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        parts.push(chunk);
        if (last) controller.enqueue(last);
        last = chunk;
      },
      async flush(controller) {
        const all = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
        let off = 0;
        for (const p of parts) {
          all.set(p, off);
          off += p.length;
        }
        const got = await hexDigest(rule.algorithm, all);
        if (!got.startsWith(want)) {
          tainted.add(url);
          const reason = `${rule.algorithm} of the content is ${got}, the name says ${want}`;
          log(
            `integrity mismatch on ${url}: ${reason}; not kept, and fetched from the network from now on`,
          );
          opts.onEvent?.({ type: 'fallback', url, reason: `integrity mismatch: ${reason}` });
          controller.error(new Error(`multipath: integrity mismatch on ${url}`));
          return;
        }
        if (last) controller.enqueue(last);
      },
    });
    const { status, statusText, headers } = response;
    return new Response(response.body.pipeThrough(check), { status, statusText, headers });
  };

  const within = <T>(p: Promise<T>, ms: number): Promise<T | 'late'> =>
    new Promise((resolve, reject) => {
      const t = setTimeout(() => resolve('late'), ms);
      p.then(
        (v) => (clearTimeout(t), resolve(v)),
        (e: unknown) => (clearTimeout(t), reject(e)),
      );
    });

  return {
    stats: () => client.stats(),
    close: () => client.close(),
    async fetch(request) {
      const retryable = request.method === 'GET' || request.method === 'HEAD';
      if (tainted.has(request.url))
        return fallback(request, 'its content failed the integrity check before');
      const spare = retryable ? null : request.clone();
      const wait = Math.max(0, fallbackAfterMs - (Date.now() - started));
      if (!(await client.live(wait))) {
        return fallback(
          spare ?? request,
          `no live session ${wait > 0 ? `within ${wait} ms` : 'now'}`,
        );
      }
      if (!retryable) return client.fetch(request);
      // A plain copy, since a navigation request cannot be re-made with a signal of its own.
      const abort = new AbortController();
      const copy = new Request(request.url, {
        method: request.method,
        headers: request.headers,
        signal: abort.signal,
      });
      try {
        const answer = await within(client.fetch(copy), requestTimeoutMs);
        if (answer !== 'late') return verified(request.url, answer);
        abort.abort();
        return fallback(request, `no answer over the lines within ${requestTimeoutMs} ms`);
      } catch (err) {
        return fallback(request, `failed over the lines: ${String(err)}`);
      }
    },
  };
}
