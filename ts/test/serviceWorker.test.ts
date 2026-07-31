import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPrecache } from "../src/serviceWorker.js";
import { parseRegistry } from "../src/registry.js";

/** A CacheStorage good enough to hold Responses and be inspected. */
function fakeCaches() {
  const store = new Map<string, Map<string, Response>>();
  const api: CacheStorage = {
    async open(name: string) {
      if (!store.has(name)) store.set(name, new Map());
      const entries = store.get(name)!;
      return {
        // Keyed by the exact URL string, as a real Cache is. An earlier version of this fake
        // normalised keys to a path and hid a real bug: assets stored under "/main.js" were being
        // looked up as "https://line.example/main.js" and never matched.
        async put(request: RequestInfo | URL, response: Response) {
          entries.set(urlOf(request), response);
        },
        async match(request: RequestInfo | URL) {
          return entries.get(urlOf(request));
        },
      } as unknown as Cache;
    },
    async keys() {
      return [...store.keys()];
    },
    async delete(name: string) {
      return store.delete(name);
    },
    async has(name: string) {
      return store.has(name);
    },
    async match() {
      return undefined;
    },
  };
  return { api, store };
}

function urlOf(request: RequestInfo | URL): string {
  return typeof request === "string"
    ? request
    : request instanceof URL
      ? request.href
      : request.url;
}

const MANIFEST = ["/index.js", "/chunk-a.js", "/styles.css"];

function build(
  overrides: Partial<Parameters<typeof createPrecache>[0]> = {},
  fetchImpl?: typeof globalThis.fetch,
) {
  const caches = fakeCaches();
  const requested: string[] = [];
  const impl =
    fetchImpl ??
    ((async (input: RequestInfo | URL) => {
      requested.push(String(input));
      return new Response("asset", { status: 200 });
    }) as unknown as typeof globalThis.fetch);

  const precache = createPrecache({
    manifest: MANIFEST,
    version: "v1",
    fetch: impl,
    caches: caches.api,
    ...overrides,
  });
  return { precache, caches, requested };
}

const get = (path: string) => new Request(`https://app.example${path}`);

describe("createPrecache, install", () => {
  it("stores every artefact the manifest names", async () => {
    const { precache, caches } = build();
    await precache.install();
    const cache = await caches.api.open(precache.cacheName);
    for (const url of MANIFEST) expect(await cache.match(url)).toBeTruthy();
  });

  /**
   * A half-populated cache is worse than none: it starts the app and then breaks on the first
   * missing chunk, which looks like a bug in the app rather than a bad install.
   */
  it("fails the install if any artefact cannot be fetched", async () => {
    const impl = (async (input: RequestInfo | URL) =>
      String(input).includes("chunk-a")
        ? new Response("nope", { status: 404 })
        : new Response("asset", { status: 200 })) as unknown as typeof globalThis.fetch;

    const { precache } = build({}, impl);
    await expect(precache.install()).rejects.toThrow(/chunk-a/);
  });

  it("fetches over the lines rather than only the origin the page came from", async () => {
    const registry = parseRegistry({
      lines: [
        { id: "cf", url: "https://cf.example" },
        { id: "ipv6", url: "https://ipv6.example" },
      ],
    });
    const { precache, requested } = build({ registry });
    await precache.install();
    expect(requested.every((url) => url.startsWith("https://cf.example"))).toBe(true);
  });

  it("moves to the next line when one cannot serve an artefact", async () => {
    const registry = parseRegistry({
      lines: [
        { id: "broken", url: "https://broken.example" },
        { id: "good", url: "https://good.example" },
      ],
    });
    const requested: string[] = [];
    const impl = (async (input: RequestInfo | URL) => {
      requested.push(String(input));
      return String(input).includes("broken.example")
        ? new Response("no", { status: 502 })
        : new Response("asset", { status: 200 });
    }) as unknown as typeof globalThis.fetch;

    const { precache } = build({ registry }, impl);
    await precache.install();
    expect(requested.some((url) => url.startsWith("https://good.example"))).toBe(true);
  });
});

describe("createPrecache, activate", () => {
  it("removes caches from older versions", async () => {
    const { caches } = build();
    await caches.api.open("multipath-precache-v0");
    const { precache } = build({ version: "v1" }, undefined);
    // Rebuild against the same storage so both versions are visible at once.
    const shared = createPrecache({
      manifest: MANIFEST,
      version: "v1",
      caches: caches.api,
      fetch: (async () => new Response("asset")) as unknown as typeof globalThis.fetch,
    });
    await shared.install();
    await shared.activate();

    expect(await caches.api.keys()).toEqual(["multipath-precache-v1"]);
    expect(precache.cacheName).toBe("multipath-precache-v1");
  });

  it("leaves caches that are not ours alone", async () => {
    const { caches } = build();
    await caches.api.open("some-other-app");
    const shared = createPrecache({
      manifest: [],
      version: "v1",
      caches: caches.api,
      fetch: (async () => new Response("asset")) as unknown as typeof globalThis.fetch,
    });
    await shared.activate();
    expect(await caches.api.keys()).toContain("some-other-app");
  });
});

describe("createPrecache, handling requests", () => {
  /**
   * The asset was cached under its manifest path but is requested from a line, so the request URL
   * is absolute and on another origin. Matching on the full URL would miss every time — and miss
   * quietly, leaving something that still works but never actually uses the cache.
   */
  it("serves a cached asset even when the page asks a line for it", async () => {
    const built = build();
    await built.precache.install();
    built.requested.length = 0;

    const response = await built.precache.handle(new Request("https://cf.example/index.js"));
    expect(response).not.toBeNull();
    expect(built.requested).toHaveLength(0);
  });

  let built: ReturnType<typeof build>;
  beforeEach(async () => {
    built = build();
    await built.precache.install();
  });

  it("serves a precached asset from the cache", async () => {
    const response = await built.precache.handle(get("/index.js"));
    expect(response).not.toBeNull();
    expect(await response!.text()).toBe("asset");
  });

  it("costs no network on a hit — which is the entire point", async () => {
    built.requested.length = 0;
    await built.precache.handle(get("/index.js"));
    expect(built.requested).toHaveLength(0);
  });

  // Staleness in data is the application's business. A transport layer answering a request with
  // yesterday's data would be lying about what it is.
  it("never answers an API request", async () => {
    expect(await built.precache.handle(get("/mt/chat/threads"))).toBeNull();
  });

  it.each(["/mt/probe", "/mt/lines", "/mt/bandwidth"])(
    "always lets %s go to the network",
    async (path) => {
      // A cached probe would report the latency of the disk; a cached registry would hide the line
      // you just added.
      expect(await built.precache.handle(get(path))).toBeNull();
    },
  );

  it("declines anything that is not a GET", async () => {
    const post = new Request("https://app.example/index.js", { method: "POST" });
    expect(await built.precache.handle(post)).toBeNull();
  });

  it("declines a URL the manifest never claimed", async () => {
    expect(await built.precache.handle(get("/somebody-elses.js"))).toBeNull();
  });

  /**
   * On a miss the worker gets out of the way rather than fetching over the lines itself.
   *
   * It used to do the latter, and it was worse than redundant: the client already races the lines,
   * so every parallel attempt became its own sequential re-race inside the worker and the two
   * schemes fought each other into intermittent timeouts. The worker owns the cache; choosing a
   * line belongs to whoever already knows what has been measured and asked.
   */
  it("declines a precached asset that is missing from the cache, rather than re-routing it", async () => {
    const { precache, requested } = build();
    expect(await precache.handle(get("/index.js"))).toBeNull();
    expect(requested).toHaveLength(0);
  });
});

describe("createPrecache, versioning", () => {
  it("keys the cache by version so a new build installs alongside the old", async () => {
    const a = createPrecache({ manifest: [], version: "abc123", caches: fakeCaches().api });
    const b = createPrecache({ manifest: [], version: "def456", caches: fakeCaches().api });
    // A user mid-session must never end up with half of one build and half of another.
    expect(a.cacheName).not.toBe(b.cacheName);
  });
});

vi.setConfig({ testTimeout: 10_000 });
