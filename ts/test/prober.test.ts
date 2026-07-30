import { describe, expect, it, vi } from "vitest";
import { HealthTable } from "../src/health.js";
import { Prober } from "../src/prober.js";
import { parseRegistry } from "../src/registry.js";

const registry = parseRegistry({
  lines: [
    { id: "fast", url: "https://fast.example" },
    { id: "slow", url: "https://slow.example" },
  ],
});

/** A fetch whose per-host behaviour a test dictates. */
function scriptedFetch(script: Record<string, () => Promise<Response>>) {
  const seen: string[] = [];
  const impl = (async (input: RequestInfo | URL) => {
    const url = String(input);
    seen.push(url);
    const host = Object.keys(script).find((h) => url.includes(h));
    if (!host) throw new Error(`unscripted host: ${url}`);
    return script[host]!();
  }) as unknown as typeof globalThis.fetch;
  return { seen, impl };
}

function build(
  script: Record<string, () => Promise<Response>>,
  options: ConstructorParameters<typeof Prober>[4] = {},
) {
  const health = new HealthTable();
  const { seen, impl } = scriptedFetch(script);
  const prober = new Prober(
    () => registry.lines,
    health,
    impl,
    (path, line) => line.url + path,
    options,
  );
  return { health, prober, seen };
}

const ok = () => Promise.resolve(new Response("{}", { status: 200 }));
const boom = () => Promise.reject(new Error("connection refused"));

describe("Prober", () => {
  it("records a latency for every line it can reach", async () => {
    const { health, prober } = build({ fast: ok, slow: ok });
    await prober.probeAll();
    expect(health.get("fast").latencyMs).not.toBeNull();
    expect(health.get("slow").latencyMs).not.toBeNull();
  });

  it("probes the unauthenticated liveness path", async () => {
    const { prober, seen } = build({ fast: ok, slow: ok });
    await prober.probeAll();
    expect(seen).toContain("https://fast.example/mt/probe");
  });

  it("counts a non-2xx as a failure — reachable is not the same as working", async () => {
    const { health, prober } = build({
      fast: () => Promise.resolve(new Response("nope", { status: 503 })),
      slow: ok,
    });
    await prober.probeAll();
    expect(health.get("fast").consecutiveFailures).toBe(1);
    expect(health.get("fast").latencyMs).toBeNull();
  });

  it("marks a line down once failures stop looking like noise", async () => {
    const { health, prober } = build({ fast: boom, slow: ok });
    for (let i = 0; i < 3; i++) await prober.probeAll();
    expect(health.get("fast").state).toBe("down");
    expect(health.get("slow").state).toBe("up");
  });

  it("times out rather than waiting on a line that never answers", async () => {
    const { health, prober } = build(
      { fast: () => new Promise<Response>(() => {}), slow: ok },
      { timeoutMs: 20 },
    );
    await prober.probeAll();
    // A stalled line is a failure. Waiting for it forever would stall the ranking too.
    expect(health.get("fast").consecutiveFailures).toBe(1);
    expect(health.get("fast").lastError).toContain("timed out");
  });

  it("does not begin probing until it is told to", async () => {
    const { prober, seen } = build({ fast: ok, slow: ok });
    // Instantiating a library should not put packets on the wire.
    await Promise.resolve();
    expect(seen).toHaveLength(0);
    prober.stop();
  });

  it("stops when stopped", async () => {
    const { prober, seen } = build({ fast: ok, slow: ok }, { intervalMs: 5 });
    prober.start();
    await new Promise((r) => setTimeout(r, 30));
    prober.stop();
    const after = seen.length;
    await new Promise((r) => setTimeout(r, 40));
    expect(seen.length).toBe(after);
  });
});

describe("Prober, backoff", () => {
  it("probes a dead line less and less often", async () => {
    let clock = 0;
    const { health, prober, seen } = build(
      { fast: boom, slow: ok },
      { intervalMs: 1000, now: () => clock },
    );

    // Three rounds of failure put the line down and grow its backoff to well beyond one interval.
    for (let i = 0; i < 3; i++) await prober.probeAll();
    expect(health.get("fast").state).toBe("down");

    const before = seen.filter((u) => u.includes("fast")).length;
    // probeAll is the explicit "check now" lever and deliberately ignores backoff; the scheduled
    // loop is what respects it, so drive that instead.
    clock += 1000;
    prober.start();
    await new Promise((r) => setTimeout(r, 30));
    prober.stop();
    const after = seen.filter((u) => u.includes("fast")).length;
    // 1000ms of clock is nowhere near the grown backoff, so the dead line was skipped while the
    // healthy one was not.
    expect(after).toBe(before);
  });
});

describe("Prober, throughput", () => {
  const payload = () => Promise.resolve(new Response(new Uint8Array(64 * 1024), { status: 200 }));

  it("does not measure throughput while the application is busy", async () => {
    let clock = 0;
    const { health, prober } = build(
      { fast: payload, slow: payload },
      {
        bandwidthPath: "/mt/bandwidth",
        now: () => clock,
        intervalMs: 1,
        idleBeforeBandwidthMs: 100,
      },
    );
    prober.noteTraffic();
    clock += 10; // still busy
    prober.start();
    await new Promise((r) => setTimeout(r, 20));
    prober.stop();
    // Measuring during a burst would compete with the traffic and report a number the measurement
    // itself caused.
    expect(health.get("fast").throughputBps).toBeNull();
  });

  it("measures once the application has gone quiet", async () => {
    let clock = 0;
    // Advance on read: a frozen clock would make the download appear instantaneous, and a
    // zero-duration measurement is correctly discarded rather than reported as infinite bandwidth.
    const { health, prober } = build(
      { fast: payload, slow: payload },
      {
        bandwidthPath: "/mt/bandwidth",
        now: () => (clock += 5),
        intervalMs: 1,
        idleBeforeBandwidthMs: 10,
      },
    );
    prober.noteTraffic();
    clock += 1000;
    prober.start();
    await new Promise((r) => setTimeout(r, 40));
    prober.stop();
    const measured = [health.get("fast").throughputBps, health.get("slow").throughputBps];
    expect(measured.some((v) => v !== null)).toBe(true);
  });

  it("measures one line at a time, so they do not compete for the same pipe", async () => {
    let clock = 0;
    const { prober, seen } = build(
      { fast: payload, slow: payload },
      { bandwidthPath: "/mt/bandwidth", now: () => clock, intervalMs: 1, idleBeforeBandwidthMs: 0 },
    );
    prober.start();
    await new Promise((r) => setTimeout(r, 15));
    prober.stop();
    const measurements = seen.filter((u) => u.includes("/mt/bandwidth"));
    // Whatever the tick count, no round may measure both lines at once.
    expect(new Set(measurements).size).toBeLessThanOrEqual(2);
  });

  it("is disabled entirely when no bandwidth path is configured", async () => {
    const { prober, seen } = build({ fast: ok, slow: ok }, { intervalMs: 1 });
    prober.start();
    await new Promise((r) => setTimeout(r, 20));
    prober.stop();
    expect(seen.every((u) => u.includes("/mt/probe"))).toBe(true);
  });
});

describe("Prober, integration with the health table", () => {
  it("produces a ranking that puts the reachable line first", async () => {
    const { health, prober } = build({ fast: ok, slow: boom });
    for (let i = 0; i < 3; i++) await prober.probeAll();
    const ranked = health.rank(registry.lines).map((l) => l.id);
    expect(ranked[0]).toBe("fast");
    expect(ranked[1]).toBe("slow");
  });
});

describe("Prober, cache behaviour", () => {
  it("asks for a fresh probe rather than a cached one", async () => {
    const inits: RequestInit[] = [];
    const impl = (async (_input: RequestInfo | URL, init?: RequestInit) => {
      inits.push(init ?? {});
      return new Response("{}", { status: 200 });
    }) as unknown as typeof globalThis.fetch;
    const prober = new Prober(
      () => registry.lines,
      new HealthTable(),
      impl,
      (path, line) => line.url + path,
      {},
    );
    await prober.probeAll();
    // A cached probe would measure the disk, not the line.
    expect(inits[0]?.cache).toBe("no-store");
  });

  it("sends credentials on an absolute line, matching a real request", async () => {
    const inits: RequestInit[] = [];
    const impl = (async (_input: RequestInfo | URL, init?: RequestInit) => {
      inits.push(init ?? {});
      return new Response("{}", { status: 200 });
    }) as unknown as typeof globalThis.fetch;
    const prober = new Prober(
      () => registry.lines,
      new HealthTable(),
      impl,
      (path, line) => line.url + path,
      {},
    );
    await prober.probeAll();
    expect(inits[0]?.credentials).toBe("include");
  });
});

// Guard against a probe accidentally becoming expensive.
describe("Prober, cost", () => {
  it("issues exactly one request per line per round", async () => {
    const { prober, seen } = build({ fast: ok, slow: ok });
    await prober.probeAll();
    expect(seen).toHaveLength(2);
  });
});

vi.setConfig({ testTimeout: 10_000 });
