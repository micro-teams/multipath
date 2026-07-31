import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  LineManager,
  NoLineAvailableError,
  SENTINEL_ORIGIN,
  type Attempt,
} from "../src/lineManager.js";
import { parseRegistry } from "../src/registry.js";

/** Records every call the manager makes and answers 200. */
function recordingFetch() {
  const calls: Array<{ url: string; init: RequestInit | undefined }> = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ url: String(input), init });
    return new Response("ok", { status: 200 });
  }) as unknown as typeof globalThis.fetch;
  return { calls, fetchImpl };
}

/**
 * How a generated OpenAPI client calls in: it has already glued its `basePath` (the sentinel) to
 * the operation path. Every test goes through this, because it is the only door there is.
 */
function send(manager: LineManager, path: string, init?: RequestInit): Promise<Response> {
  return manager.fetchApi()(`${SENTINEL_ORIGIN}${path}`, init);
}

const twoLines = parseRegistry({
  lines: [
    { id: "cf", url: "https://cf.mt.example.app", transport: "cloudflare" },
    { id: "ipv6-1", url: "https://ipv6-1.mt.example.app", transport: "ipv6" },
  ],
});

describe("LineManager, default (same-origin, single line)", () => {
  // MP-1's entire acceptance criterion: with one same-origin line, MultiPath must be invisible.
  // If any of these drift, the "zero behaviour change" claim is false.
  it("emits the path unchanged, exactly as the pre-MultiPath client did", () => {
    expect(new LineManager().resolve("/mt/chat/threads")).toBe("/mt/chat/threads");
  });

  /**
   * The no-op guarantee, stated precisely.
   *
   * Everything the caller set is passed through, and the URL is the bare path. What is added is an
   * abort signal, because since hedging landed every request carries one — a hedge has to be able
   * to cancel its losers, and a read now has an overall timeout instead of hanging forever. With a
   * single line nothing ever aborts it short of that timeout.
   */
  it("passes a read's init through, adding only the signal it needs to cancel with", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const init: RequestInit = { headers: { Accept: "application/json" } };
    await send(new LineManager({ fetch: fetchImpl }), "/mt/chat/threads", init);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.url).toBe("/mt/chat/threads");
    expect(calls[0]?.init?.headers).toBe(init.headers);
    expect(calls[0]?.init?.signal).toBeInstanceOf(AbortSignal);
    expect(Object.keys(calls[0]!.init!).sort()).toEqual(["headers", "signal"]);
  });

  it("does not add credentials to a same-origin line", async () => {
    const { calls, fetchImpl } = recordingFetch();
    await send(new LineManager({ fetch: fetchImpl }), "/mt/probe");
    expect(calls[0]?.init && "credentials" in calls[0].init).toBe(false);
  });
});

describe("LineManager, idempotency keys", () => {
  const keyed = (calls: Array<{ init: RequestInit | undefined }>) =>
    new Headers(calls[0]?.init?.headers).get("Idempotency-Key");

  it("gives a write a key so the server can recognise a duplicate", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ fetch: fetchImpl, newKey: () => "generated-key" });
    await send(manager, "/mt/chat/messages", { method: "POST", body: "{}" });
    expect(keyed(calls)).toBe("generated-key");
  });

  it.each(["POST", "PATCH", "DELETE"])("keys %s", async (method) => {
    const { calls, fetchImpl } = recordingFetch();
    await send(new LineManager({ fetch: fetchImpl, newKey: () => "k" }), "/mt/thing", { method });
    expect(keyed(calls)).toBe("k");
  });

  // Already idempotent, so a key would add a de-duplication window to a request that never needed
  // one — and would make the read path stop being a literal no-op.
  it.each(["GET", "HEAD", "PUT"])("does not key %s", async (method) => {
    const { calls, fetchImpl } = recordingFetch();
    await send(new LineManager({ fetch: fetchImpl, newKey: () => "k" }), "/mt/thing", { method });
    expect(keyed(calls)).toBeNull();
  });

  // The caller may know two calls are one logical write in a way MultiPath cannot.
  it("never overwrites a key the caller supplied", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ fetch: fetchImpl, newKey: () => "generated" });
    await send(manager, "/mt/chat/messages", {
      method: "POST",
      headers: { "Idempotency-Key": "caller's own" },
    });
    expect(keyed(calls)).toBe("caller's own");
  });

  it("keeps the caller's other headers", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ fetch: fetchImpl, newKey: () => "k" });
    await send(manager, "/mt/chat/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    expect(new Headers(calls[0]?.init?.headers).get("Content-Type")).toBe("application/json");
  });

  it("gives distinct writes distinct keys", async () => {
    const { calls, fetchImpl } = recordingFetch();
    let n = 0;
    const manager = new LineManager({ fetch: fetchImpl, newKey: () => `key-${++n}` });
    await send(manager, "/mt/chat/messages", { method: "POST" });
    await send(manager, "/mt/chat/messages", { method: "POST" });
    const keys = calls.map((c) => new Headers(c.init?.headers).get("Idempotency-Key"));
    expect(keys).toEqual(["key-1", "key-2"]);
  });

  it("honours a configured header name", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({
      fetch: fetchImpl,
      newKey: () => "k",
      idempotencyHeader: "X-Request-Id",
    });
    await send(manager, "/mt/thing", { method: "POST" });
    expect(new Headers(calls[0]?.init?.headers).get("X-Request-Id")).toBe("k");
  });
});

describe("LineManager, resolution", () => {
  it("prefixes the selected line's origin", () => {
    expect(new LineManager({ registry: twoLines }).resolve("/mt/probe")).toBe(
      "https://cf.mt.example.app/mt/probe",
    );
  });

  it("resolves against an explicitly named line", () => {
    const manager = new LineManager({ registry: twoLines });
    expect(manager.resolve("/mt/probe", manager.lines[1]!)).toBe(
      "https://ipv6-1.mt.example.app/mt/probe",
    );
  });

  it("rejects a path that is not rooted, rather than producing a mangled url", () => {
    expect(() => new LineManager().resolve("mt/probe")).toThrow(TypeError);
  });

  it("throws when the registry has been emptied out from under it", () => {
    const manager = new LineManager({ registry: twoLines });
    manager.setRegistry({ lines: [] });
    expect(() => manager.select()).toThrow(NoLineAvailableError);
  });

  it("routes over the new registry after a swap", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ fetch: fetchImpl });
    await send(manager, "/mt/probe");
    manager.setRegistry(twoLines);
    await send(manager, "/mt/probe");
    expect(calls.map((c) => c.url)).toEqual(["/mt/probe", "https://cf.mt.example.app/mt/probe"]);
  });
});

describe("LineManager, credentials", () => {
  // Every absolute url is a different origin from the page — a different subdomain is enough, and
  // a different port on the same host is too. fetch defaults to credentials:"same-origin", so
  // without asking the cookie is simply not sent and the server sees an anonymous caller. That
  // presents as "logged out on that line", which reads as an auth bug rather than a transport one.
  it.each([
    ["another subdomain of ours", "https://cf.mt.example.app"],
    ["our own host on a non-standard port", "https://mt.example.app:8443"],
    ["someone else's domain entirely", "https://free.provider.example"],
  ])("sends credentials to %s", async (_name, url) => {
    const { calls, fetchImpl } = recordingFetch();
    const registry = parseRegistry({ lines: [{ id: "line", url }] });
    await send(new LineManager({ registry, fetch: fetchImpl }), "/mt/probe");
    expect(calls[0]?.init?.credentials).toBe("include");
  });

  it("leaves the same-origin line untouched, so the no-op stays literal", async () => {
    const { calls, fetchImpl } = recordingFetch();
    await send(new LineManager({ fetch: fetchImpl }), "/mt/probe");
    expect(calls[0]?.init && "credentials" in calls[0].init).toBe(false);
  });

  it("lets an explicit caller override it", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const registry = parseRegistry({ lines: [{ id: "cf", url: "https://cf.mt.example.app" }] });
    await send(new LineManager({ registry, fetch: fetchImpl }), "/mt/probe", {
      credentials: "omit",
    });
    expect(calls[0]?.init?.credentials).toBe("omit");
  });
});

describe("LineManager, non-standard ports", () => {
  // A port is part of an origin but not part of a URL's path, so it only has to survive
  // concatenation — and cookies are not port-scoped, so a line on our own host at another port
  // still shares the session cookie.
  it("keeps the port when resolving", () => {
    const registry = parseRegistry({ lines: [{ id: "alt", url: "https://mt.example.app:8443" }] });
    expect(new LineManager({ registry }).resolve("/mt/probe")).toBe(
      "https://mt.example.app:8443/mt/probe",
    );
  });

  it("accepts a bracketed IPv6 literal with a port", () => {
    const registry = parseRegistry({ lines: [{ id: "v6", url: "https://[2001:db8::1]:8443" }] });
    expect(new LineManager({ registry }).resolve("/mt/probe")).toBe(
      "https://[2001:db8::1]:8443/mt/probe",
    );
  });
});

describe("LineManager, attempt reporting", () => {
  let attempts: Attempt[];
  beforeEach(() => {
    attempts = [];
  });

  it("reports the line, method and status of a successful attempt", async () => {
    const { fetchImpl } = recordingFetch();
    const manager = new LineManager({
      registry: twoLines,
      fetch: fetchImpl,
      onAttempt: (a) => attempts.push(a),
    });
    await send(manager, "/mt/chat/messages", { method: "post" });
    expect(attempts).toHaveLength(1);
    expect(attempts[0]).toMatchObject({
      lineId: "cf",
      method: "POST",
      path: "/mt/chat/messages",
      status: 200,
    });
    expect(attempts[0]?.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("reports a failed attempt and still propagates the error", async () => {
    const boom = new Error("network down");
    const manager = new LineManager({
      fetch: (() => Promise.reject(boom)) as unknown as typeof globalThis.fetch,
      onAttempt: (a) => attempts.push(a),
    });
    await expect(send(manager, "/mt/probe")).rejects.toBe(boom);
    expect(attempts[0]?.error).toBe(boom);
    expect(attempts[0]?.status).toBeUndefined();
  });

  // Observation is a debugging aid; it must never be able to break the app it observes.
  it("survives an observer that throws", async () => {
    const { fetchImpl } = recordingFetch();
    const manager = new LineManager({
      fetch: fetchImpl,
      onAttempt: () => {
        throw new Error("panel bug");
      },
    });
    await expect(send(manager, "/mt/probe")).resolves.toMatchObject({ status: 200 });
  });
});

describe("LineManager.fetchApi (the only outbound door)", () => {
  // The contract-generated client is the only supported caller, so the adapter's handling of what
  // that client emits — sentinel + path, sometimes a Request object — is load-bearing.
  it("strips the sentinel origin and routes the remaining path", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ registry: twoLines, fetch: fetchImpl });
    await send(manager, "/mt/chat/threads?limit=20");
    expect(calls[0]?.url).toBe("https://cf.mt.example.app/mt/chat/threads?limit=20");
  });

  it("keeps a same-origin deployment byte-identical through the adapter too", async () => {
    const { calls, fetchImpl } = recordingFetch();
    await send(new LineManager({ fetch: fetchImpl }), "/mt/chat/threads");
    expect(calls[0]?.url).toBe("/mt/chat/threads");
  });

  it("accepts a URL object", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ registry: twoLines, fetch: fetchImpl });
    await manager.fetchApi()(new URL(`${SENTINEL_ORIGIN}/mt/probe`));
    expect(calls[0]?.url).toBe("https://cf.mt.example.app/mt/probe");
  });

  /**
   * A Request's body is a stream that can be read once. Dropped, a write goes out empty — and MP-4
   * has to be able to send the very same bytes again over another line, which a consumed stream
   * cannot do. So it is buffered rather than forwarded.
   */
  it("keeps the body of a Request, rather than silently sending an empty write", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ registry: twoLines, fetch: fetchImpl });
    await manager.fetchApi()(
      new Request(`${SENTINEL_ORIGIN}/mt/chat/messages`, {
        method: "POST",
        body: JSON.stringify({ text: "hello" }),
        headers: { "Content-Type": "application/json" },
      }),
    );
    const body = calls[0]?.init?.body as ArrayBuffer;
    expect(body).toBeInstanceOf(ArrayBuffer);
    expect(new TextDecoder().decode(body)).toBe('{"text":"hello"}');
  });

  it("does not try to read a body a GET cannot have", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ registry: twoLines, fetch: fetchImpl });
    await manager.fetchApi()(new Request(`${SENTINEL_ORIGIN}/mt/probe`));
    expect(calls[0]?.init?.body).toBeUndefined();
  });

  // A caller that rebuilds its client configuration per request should not be handed a fresh
  // closure per request.
  it("returns a stable function", () => {
    const manager = new LineManager();
    expect(manager.fetchApi()).toBe(manager.fetchApi());
  });

  it("carries method and headers over from a Request object", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ registry: twoLines, fetch: fetchImpl });
    await manager.fetchApi()(new Request(`${SENTINEL_ORIGIN}/mt/probe`, { method: "DELETE" }));
    expect(calls[0]?.url).toBe("https://cf.mt.example.app/mt/probe");
    expect(calls[0]?.init?.method).toBe("DELETE");
  });

  // An absolute third-party URL is not ours to reroute; guessing would send someone else's
  // request down our lines.
  it("passes a non-sentinel absolute url straight through", async () => {
    const { calls, fetchImpl } = recordingFetch();
    const manager = new LineManager({ registry: twoLines, fetch: fetchImpl });
    await manager.fetchApi()("https://avatars.example.com/1.png");
    expect(calls[0]?.url).toBe("https://avatars.example.com/1.png");
  });

  it("maps the bare sentinel origin to the root path", async () => {
    const { calls, fetchImpl } = recordingFetch();
    await new LineManager({ registry: twoLines, fetch: fetchImpl }).fetchApi()(SENTINEL_ORIGIN);
    expect(calls[0]?.url).toBe("https://cf.mt.example.app/");
  });
});

describe("LineManager, remembering across visits", () => {
  /** A localStorage good enough to be inspected and to be made to fail. */
  function fakeStorage(failing = false): Storage {
    const map = new Map<string, string>();
    return {
      getItem: (k) => map.get(k) ?? null,
      setItem: (k, v) => {
        if (failing) throw new Error("quota exceeded");
        map.set(k, v);
      },
      removeItem: (k) => void map.delete(k),
      clear: () => map.clear(),
      key: (i) => [...map.keys()][i] ?? null,
      get length() {
        return map.size;
      },
    } as Storage;
  }

  const registry = parseRegistry({
    lines: [
      { id: "cf", url: "https://cf.example" },
      { id: "ipv6", url: "https://ipv6.example" },
    ],
  });

  it("starts the next visit from what was measured, not from registry order", async () => {
    const storage = fakeStorage();
    const { fetchImpl } = recordingFetch();

    const first = new LineManager({ registry, fetch: fetchImpl, storage });
    // ipv6 is listed second but measures far faster.
    first.health(); // touch, so the table exists
    (
      first as unknown as {
        healthTable: { recordSuccess: (a: string, b: number, c: number) => void };
      }
    ).healthTable.recordSuccess("ipv6", 10, Date.now());
    (
      first as unknown as {
        healthTable: { recordSuccess: (a: string, b: number, c: number) => void };
      }
    ).healthTable.recordSuccess("cf", 400, Date.now());
    first.saveHealth();

    const second = new LineManager({ registry, fetch: fetchImpl, storage });
    // Without persistence this would be "cf", purely because it is listed first.
    expect(second.preferredLineIds()[0]).toBe("ipv6");
  });

  it("works perfectly well with no storage at all", () => {
    const manager = new LineManager({ registry });
    expect(() => manager.saveHealth()).not.toThrow();
    expect(manager.preferredLineIds()).toEqual(["cf", "ipv6"]);
  });

  // A full or disabled store is not worth failing a request over.
  it("survives a storage that throws", () => {
    const manager = new LineManager({ registry, storage: fakeStorage(true) });
    expect(() => manager.saveHealth()).not.toThrow();
  });

  it("ignores corrupt stored data rather than starting from something misread", () => {
    const storage = fakeStorage();
    storage.setItem("multipath:health", "{not json");
    expect(() => new LineManager({ registry, storage })).not.toThrow();
  });
});
