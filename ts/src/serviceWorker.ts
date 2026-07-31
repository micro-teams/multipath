/*
 *  Description: The Service Worker runtime: what makes starting the app independent of the lines.
 *
 *               Precaching is what takes "load the application" out of the network entirely. After
 *               the first visit, every build artefact is on disk, and opening the app is a cache
 *               read — no line has to be alive, fast, or even reachable. That is the whole of the
 *               design's "as long as there is still a cache, it starts" claim, and it is why this
 *               is a worker rather than a clever fetch wrapper: only a worker is consulted before
 *               the page exists.
 *
 *               Two boundaries are load-bearing.
 *
 *               Only build artefacts are cached. An API response must never be served from here —
 *               staleness in data is the application's business, and a transport layer that
 *               silently answered a request with yesterday's data would be lying about what it is.
 *
 *               A cache miss still goes over the lines. The assets are on every line, so a miss
 *               during a partial outage is recoverable rather than fatal; falling back to the one
 *               origin the page happened to come from would throw that away.
 *
 *  Author(s):
 *      agent4
 */

import type { Line, Registry } from "./registry.js";

export interface PrecacheOptions {
  /**
   * Every URL the application needs to start, as the build emitted them.
   *
   * Supplied by the consumer, because a product-agnostic library cannot know what a build produces.
   * Generating it is the consumer's build step; getting it wrong shows up as an app that will not
   * start offline, so it is worth generating rather than hand-writing.
   */
  readonly manifest: readonly string[];
  /**
   * Changes whenever the manifest does — a build hash is the obvious source.
   *
   * The cache is keyed by it, so a new version installs alongside the old and the old is deleted
   * only once the new one is in charge. A user mid-session is never left with half of one build and
   * half of another.
   */
  readonly version: string;
  /** Lines to try on a cache miss. Baked in at build time, since a worker has no page to ask. */
  readonly registry?: Registry;
  /** Prefixes that must always go to the network. Defaults to the MultiPath endpoints. */
  readonly networkOnly?: readonly string[];
  readonly cachePrefix?: string;
  /** Injected in tests. */
  readonly fetch?: typeof globalThis.fetch;
  readonly caches?: CacheStorage;
}

const DEFAULTS = {
  cachePrefix: "multipath-precache-",
  // The line endpoints themselves must never be answered from cache: a cached probe would report
  // the latency of the disk, and a cached registry would hide the line you just added.
  networkOnly: ["/mt/probe", "/mt/lines", "/mt/bandwidth"],
} as const;

/**
 * The handlers a consumer's `sw.js` wires up.
 *
 * Returned rather than registered, so the consumer keeps control of its own worker and can add
 * behaviour of its own. A library that called `addEventListener` for you would own a file it does
 * not own.
 */
export function createPrecache(options: PrecacheOptions) {
  const cacheName = `${options.cachePrefix ?? DEFAULTS.cachePrefix}${options.version}`;
  const networkOnly = options.networkOnly ?? DEFAULTS.networkOnly;
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const cacheStorage = options.caches ?? globalThis.caches;
  const lines = options.registry?.lines ?? [];

  return {
    cacheName,

    /**
     * Fetch and store every artefact.
     *
     * One failure fails the install, deliberately: a half-populated cache is worse than none,
     * because it starts the app and then breaks on the first missing chunk — a failure that looks
     * like a bug in the app rather than a bad install.
     */
    async install(): Promise<void> {
      const cache = await cacheStorage.open(cacheName);
      const responses = await Promise.all(
        options.manifest.map(async (url) => {
          const response = await fetchOverLines(url, lines, fetchImpl);
          if (!response.ok) throw new Error(`precache failed for ${url}: ${response.status}`);
          return [url, response] as const;
        }),
      );
      await Promise.all(responses.map(([url, response]) => cache.put(url, response)));
    },

    /** Delete every cache from an older version, once this one is the one in charge. */
    async activate(): Promise<void> {
      const keys = await cacheStorage.keys();
      await Promise.all(
        keys
          .filter((key) => key.startsWith(options.cachePrefix ?? DEFAULTS.cachePrefix))
          .filter((key) => key !== cacheName)
          .map((key) => cacheStorage.delete(key)),
      );
    },

    /**
     * Answer a request, or return null to let it go to the network untouched.
     *
     * Null rather than a fetch, so the decision not to interfere is explicit and a consumer can see
     * exactly which requests this layer declines to touch.
     */
    async handle(request: Request): Promise<Response | null> {
      // A worker sees every request the page makes, including ones to other origins entirely.
      if (request.method !== "GET") return null;

      const url = new URL(request.url);
      if (networkOnly.some((prefix) => url.pathname.startsWith(prefix))) return null;

      const cache = await cacheStorage.open(cacheName);
      // Matched by the manifest-relative path, not the full URL. An artefact cached during install
      // was stored under "/main.js" while the page may well request it from a line as
      // "https://cf.example/main.js" — matching on the full URL would miss every single time, and
      // miss silently, degrading into "it works but always goes to the network".
      const cached = await cache.match(url.pathname + url.search);
      // The point of the whole exercise: a hit costs no network at all, so which lines are alive
      // stops mattering for starting the app.
      if (cached) return cached;

      // A miss during a partial outage is recoverable, because the assets are on every line.
      // Falling back only to the origin this page came from would throw that away.
      if (isPrecached(url, options.manifest)) {
        return fetchOverLines(url.pathname + url.search, lines, fetchImpl);
      }
      return null;
    },
  };
}

/** Did the manifest claim this URL? Compared by path, since the manifest is origin-relative. */
function isPrecached(url: URL, manifest: readonly string[]): boolean {
  return manifest.some((entry) => entry === url.pathname || entry === url.pathname + url.search);
}

/**
 * Try each line in turn until one answers.
 *
 * Sequential rather than raced: this runs during install, where finishing matters and finishing
 * quickly does not, and racing N lines for every artefact of a large build would mean N times the
 * bytes for a saving nobody is waiting on.
 */
async function fetchOverLines(
  path: string,
  lines: readonly Line[],
  fetchImpl: typeof globalThis.fetch,
): Promise<Response> {
  if (lines.length === 0) return fetchImpl(path, { cache: "no-store" });

  let lastError: unknown = new Error(`no line could serve ${path}`);
  for (const line of lines) {
    try {
      const response = await fetchImpl(line.url === "" ? path : line.url + path, {
        cache: "no-store",
        ...(line.url === "" ? {} : { credentials: "include" as const }),
      });
      if (response.ok) return response;
      lastError = new Error(`${line.id} answered ${response.status} for ${path}`);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError;
}
