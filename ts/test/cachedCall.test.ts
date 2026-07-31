import { describe, expect, it, vi } from "vitest";
import { LineManager, SENTINEL_ORIGIN } from "../src/lineManager.js";

/** Stands in for a generated client: builds a URL, awaits, then calls fetchApi — as they do. */
function client(manager: LineManager) {
  const fetchApi = manager.fetchApi();
  return {
    async get<T>(path: string): Promise<T> {
      // The await before reaching the network is what a real typescript-fetch client does, and is
      // the reason `cached` cannot be a synchronous property.
      await Promise.resolve();
      const response = await fetchApi(`${SENTINEL_ORIGIN}${path}`);
      if (!response.ok) throw new Error(String(response.status));
      return (await response.json()) as T;
    },
    async post<T>(path: string, body: unknown): Promise<T> {
      await Promise.resolve();
      const response = await fetchApi(`${SENTINEL_ORIGIN}${path}`, {
        method: "POST",
        body: JSON.stringify(body),
      });
      return (await response.json()) as T;
    },
  };
}

function build(responder: (url: string) => unknown) {
  let calls = 0;
  const fetchImpl = (async (input: RequestInfo | URL) => {
    calls += 1;
    const body = responder(String(input));
    if (body instanceof Error) throw body;
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as unknown as typeof globalThis.fetch;

  const manager = new LineManager({ fetch: fetchImpl, cache: {} });
  return { manager, api: client(manager), calls: () => calls };
}

describe("cached()", () => {
  it("returns the real result, exactly as an uncached call would", async () => {
    const { manager, api } = build(() => ({ items: [1, 2] }));
    const call = manager.cached(() => api.get<{ items: number[] }>("/mt/threads"));
    expect(await call).toEqual({ items: [1, 2] });
  });

  it("has nothing to offer the first time", async () => {
    const { manager, api } = build(() => ({ items: [1] }));
    const call = manager.cached(() => api.get("/mt/threads"));
    expect(await call.cached).toBeNull();
    await call;
  });

  it("offers the previous answer on the next identical call", async () => {
    let n = 0;
    const { manager, api } = build(() => ({ n: ++n }));

    await manager.cached(() => api.get("/mt/threads"));
    const second = manager.cached(() => api.get<{ n: number }>("/mt/threads"));

    expect(await second.cached).toEqual({ n: 1 });
    expect(await second).toEqual({ n: 2 });
  });

  /**
   * The request always goes out. This is the difference from a browser cache, which answers instead
   * of asking: here the cached value is offered beside the truth, never in place of it.
   */
  it("still makes the request when it has something cached", async () => {
    const { manager, api, calls } = build(() => ({ ok: true }));
    await manager.cached(() => api.get("/mt/threads"));
    const before = calls();
    await manager.cached(() => api.get("/mt/threads"));
    expect(calls()).toBe(before + 1);
  });

  /**
   * A failure stays a failure. Falling back to stale data here is precisely the behaviour that
   * makes browser caches untrustworthy — you believe you succeeded, and you did not.
   */
  it("rejects when the request fails, even with something cached", async () => {
    let fail = false;
    const { manager, api } = build(() => (fail ? new Error("network down") : { ok: true }));

    await manager.cached(() => api.get("/mt/threads"));
    fail = true;
    const second = manager.cached(() => api.get("/mt/threads"));

    await expect(second).rejects.toThrow();
    // ...and the cached value is still there to paint with, which is the point of offering it.
    expect(await second.cached).toEqual({ ok: true });
  });

  it("does not remember a failed request as an answer", async () => {
    let fail = true;
    const { manager, api } = build(() => (fail ? new Error("nope") : { ok: true }));
    await expect(manager.cached(() => api.get("/mt/threads"))).rejects.toThrow();
    fail = false;
    expect(await manager.cached(() => api.get("/mt/threads")).cached).toBeNull();
  });

  it("keeps different requests apart", async () => {
    const { manager, api } = build((url) => ({ url }));
    await manager.cached(() => api.get("/mt/a"));
    expect(await manager.cached(() => api.get("/mt/b")).cached).toBeNull();
  });

  /**
   * A write's answer is a receipt for something that already happened. Offering it to a later
   * identical-looking write is the one place this could mislead rather than merely be unhelpful.
   */
  it("remembers nothing about writes", async () => {
    const { manager, api } = build(() => ({ created: true }));
    await manager.cached(() => api.post("/mt/messages", { text: "hi" }));
    const second = manager.cached(() => api.post("/mt/messages", { text: "hi" }));
    expect(await second.cached).toBeNull();
    await second;
  });

  it("settles cached long before the response arrives", async () => {
    let release: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const fetchImpl = (async () => {
      await gate;
      return new Response("{}", { headers: { "content-type": "application/json" } });
    }) as unknown as typeof globalThis.fetch;

    const manager = new LineManager({ fetch: fetchImpl, cache: {} });
    const api = client(manager);
    const call = manager.cached(() => api.get("/mt/threads"));

    // Resolves while the request is still in flight, which is what makes it usable for painting.
    expect(await call.cached).toBeNull();
    release!();
    await call;
  });

  /**
   * Two calls started in the same tick must not be handed each other's URL. A cache that
   * occasionally shows one screen's data on another is worse than no cache, because it is believed.
   */
  it("keeps concurrent calls from crossing wires", async () => {
    let n = 0;
    const { manager, api } = build((url) => ({ url, n: ++n }));

    await Promise.all([
      manager.cached(() => api.get("/mt/a")),
      manager.cached(() => api.get("/mt/b")),
    ]);

    const [a, b] = [manager.cached(() => api.get("/mt/a")), manager.cached(() => api.get("/mt/b"))];
    const [cachedA, cachedB] = await Promise.all([a.cached, b.cached]);
    await Promise.all([a, b]);

    expect((cachedA as { url: string }).url).toContain("/mt/a");
    expect((cachedB as { url: string }).url).toContain("/mt/b");
  });

  it("is a no-op when no cache was configured", async () => {
    const fetchImpl = (async () =>
      new Response('{"ok":true}', {
        headers: { "content-type": "application/json" },
      })) as unknown as typeof globalThis.fetch;
    const manager = new LineManager({ fetch: fetchImpl });
    const api = client(manager);

    const call = manager.cached(() => api.get("/mt/threads"));
    expect(await call.cached).toBeNull();
    expect(await call).toEqual({ ok: true });
  });
});

describe("cached(), invalidation", () => {
  it("forgets what the application says is stale", async () => {
    const { manager, api } = build(() => ({ ok: true }));
    await manager.cached(() => api.get("/mt/threads"));

    // The application decides when; this layer only knows how.
    manager.cache!.invalidate("GET /mt/threads");

    expect(await manager.cached(() => api.get("/mt/threads")).cached).toBeNull();
  });

  it("forgets everything when the tenant changes", async () => {
    const fetchImpl = (async () =>
      new Response('{"ok":true}', {
        headers: { "content-type": "application/json" },
      })) as unknown as typeof globalThis.fetch;
    const manager = new LineManager({ fetch: fetchImpl, cache: { scope: "user-1" } });
    const api = client(manager);

    await manager.cached(() => api.get("/mt/me"));
    manager.cache!.setScope("user-2");
    expect(await manager.cached(() => api.get("/mt/me")).cached).toBeNull();
  });
});

vi.setConfig({ testTimeout: 10_000 });
