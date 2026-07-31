import { describe, expect, it } from "vitest";
import { RequestCache } from "../src/cache.js";

describe("RequestCache.keyFor", () => {
  it("keys by method and path", () => {
    expect(RequestCache.keyFor("get", "/mt/chat/threads")).toBe("GET /mt/chat/threads");
  });

  /**
   * The same resource fetched over two lines is the same resource. Keying by full URL would give
   * every line its own cache and lose most hits the moment a second line existed — the exact
   * moment the cache becomes worth having.
   */
  it("ignores which line the request went over", () => {
    expect(RequestCache.keyFor("GET", "https://cf.example/mt/threads")).toBe(
      RequestCache.keyFor("GET", "https://ipv6.example/mt/threads"),
    );
  });

  it("keeps the query string, which changes what was asked for", () => {
    expect(RequestCache.keyFor("GET", "/mt/threads?page=1")).not.toBe(
      RequestCache.keyFor("GET", "/mt/threads?page=2"),
    );
  });
});

describe("RequestCache", () => {
  it("returns what was stored", () => {
    const cache = new RequestCache();
    cache.set("GET /a", { items: [1, 2] });
    expect(cache.get("GET /a")).toEqual({ items: [1, 2] });
  });

  it("returns null for something never seen", () => {
    expect(new RequestCache().get("GET /a")).toBeNull();
  });

  it("keeps the exact value, not a copy through JSON", () => {
    const cache = new RequestCache();
    const value = { when: new Date("2026-01-01") };
    cache.set("GET /a", value);
    // A JSON round trip would have turned this into a string, which is the kind of bug that
    // poisons types quietly.
    expect((cache.get<typeof value>("GET /a")!.when as Date) instanceof Date).toBe(true);
  });

  it("stops offering an answer that has gone stale", () => {
    let now = 1000;
    const cache = new RequestCache({ maxAgeMs: 500, now: () => now });
    cache.set("GET /a", "old");
    now = 2000;
    expect(cache.get("GET /a")).toBeNull();
  });

  it("drops the least recently stored when full", () => {
    const cache = new RequestCache({ maxEntries: 2 });
    cache.set("GET /a", 1);
    cache.set("GET /b", 2);
    cache.set("GET /c", 3);
    expect(cache.get("GET /a")).toBeNull();
    expect(cache.get("GET /c")).toBe(3);
  });

  it("counts a re-store as recent, so a busy entry is not evicted", () => {
    const cache = new RequestCache({ maxEntries: 2 });
    cache.set("GET /a", 1);
    cache.set("GET /b", 2);
    cache.set("GET /a", 3);
    cache.set("GET /c", 4);
    expect(cache.get("GET /a")).toBe(3);
    expect(cache.get("GET /b")).toBeNull();
  });
});

describe("RequestCache, invalidation", () => {
  /**
   * By request shape, because that is all this layer can honestly reason about. Which reads a write
   * invalidates is a question about the application's semantics, and a transport layer guessing at
   * it would be inventing business rules.
   */
  it("forgets everything under a prefix", () => {
    const cache = new RequestCache();
    cache.set("GET /mt/threads", 1);
    cache.set("GET /mt/threads/7", 2);
    cache.set("GET /mt/teams", 3);

    expect(cache.invalidate("GET /mt/threads")).toBe(2);
    expect(cache.get("GET /mt/threads")).toBeNull();
    expect(cache.get("GET /mt/teams")).toBe(3);
  });

  it("clears everything on request", () => {
    const cache = new RequestCache();
    cache.set("GET /a", 1);
    cache.clear();
    expect(cache.size).toBe(0);
  });
});

describe("RequestCache, scope", () => {
  it("keeps tenants apart", () => {
    const cache = new RequestCache({ scope: "user-1" });
    cache.set("GET /me", "first");
    cache.setScope("user-2");
    // The single failure that would matter most: one account seeing another's data.
    expect(cache.get("GET /me")).toBeNull();
  });

  /**
   * Dropped rather than hidden. A signed-out user's answers sitting in memory waiting to become
   * reachable again is exactly the accident this exists to prevent.
   */
  it("drops the old tenant's entries rather than shelving them", () => {
    const cache = new RequestCache({ scope: "user-1" });
    cache.set("GET /me", "first");
    cache.setScope("user-2");
    cache.setScope("user-1");
    expect(cache.get("GET /me")).toBeNull();
  });

  it("does nothing when the scope has not actually changed", () => {
    const cache = new RequestCache({ scope: "user-1" });
    cache.set("GET /me", "first");
    cache.setScope("user-1");
    expect(cache.get("GET /me")).toBe("first");
  });
});
