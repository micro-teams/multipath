import { describe, expect, it } from "vitest";
import { HealthTable } from "../src/health.js";

const lines = [
  { id: "cf", weight: 100 },
  { id: "ipv6", weight: 50 },
  { id: "frp", weight: 10 },
];

const ids = (ranked: Array<{ id: string }>) => ranked.map((l) => l.id);

describe("HealthTable, before anything has been measured", () => {
  // A client that has only just started still has to send requests somewhere.
  it("treats an unmeasured line as up, not down", () => {
    expect(new HealthTable().get("cf").state).toBe("up");
  });

  it("falls back to configured weight when nothing is known", () => {
    expect(ids(new HealthTable().rank(lines))).toEqual(["cf", "ipv6", "frp"]);
  });
});

describe("HealthTable, ranking", () => {
  it("puts the fastest measured line first, regardless of weight", () => {
    const health = new HealthTable();
    health.recordSuccess("cf", 300, 1);
    health.recordSuccess("ipv6", 20, 1);
    health.recordSuccess("frp", 90, 1);
    // frp has the lowest weight and the second-best latency; measurement outranks configuration.
    expect(ids(health.rank(lines))).toEqual(["ipv6", "frp", "cf"]);
  });

  it("prefers a line that has answered over one that never has", () => {
    const health = new HealthTable();
    health.recordSuccess("frp", 500, 1);
    // Even at 500ms, frp is known to work and the others are only assumed to.
    expect(ids(health.rank(lines))[0]).toBe("frp");
  });

  it("ranks a down line last but still offers it", () => {
    const health = new HealthTable();
    health.recordSuccess("cf", 10, 1);
    for (let i = 0; i < 3; i++) health.recordFailure("ipv6", new Error("nope"), 1);
    const ranked = ids(health.rank(lines));
    expect(ranked[ranked.length - 1]).toBe("ipv6");
    expect(ranked).toHaveLength(3);
  });

  it("breaks a latency tie on measured throughput", () => {
    const health = new HealthTable();
    health.recordSuccess("cf", 50, 1);
    health.recordSuccess("ipv6", 50, 1);
    health.recordThroughput("cf", 1_000);
    health.recordThroughput("ipv6", 9_000);
    // The point of measuring throughput at all: equal latency, very unequal usefulness.
    expect(ids(health.rank([lines[0]!, lines[1]!]))).toEqual(["ipv6", "cf"]);
  });
});

describe("HealthTable, failure handling", () => {
  it("does not demote a line for a single failure", () => {
    const health = new HealthTable();
    health.recordSuccess("cf", 10, 1);
    health.recordFailure("cf", new Error("blip"), 2);
    // One failure is noise. Demoting here would take a line out of service for a dropped packet.
    expect(health.get("cf").state).toBe("up");
    expect(health.get("cf").consecutiveFailures).toBe(1);
  });

  it("demotes after a run of failures", () => {
    const health = new HealthTable();
    for (let i = 0; i < 3; i++) health.recordFailure("cf", new Error("nope"), i);
    expect(health.get("cf").state).toBe("down");
  });

  it("recovers immediately on the next success", () => {
    const health = new HealthTable();
    for (let i = 0; i < 3; i++) health.recordFailure("cf", new Error("nope"), i);
    health.recordSuccess("cf", 12, 9);
    expect(health.get("cf").state).toBe("up");
    expect(health.get("cf").consecutiveFailures).toBe(0);
    expect(health.get("cf").lastError).toBeNull();
  });

  it("keeps the last error for the panel to show", () => {
    const health = new HealthTable();
    health.recordFailure("cf", new Error("connection refused"), 1);
    expect(health.get("cf").lastError).toBe("connection refused");
  });
});

describe("HealthTable, smoothing", () => {
  it("seeds on the first sample instead of averaging up from zero", () => {
    const health = new HealthTable();
    health.recordSuccess("cf", 100, 1);
    // Starting from zero would make a new line look impossibly fast and win a ranking it has not
    // earned.
    expect(health.get("cf").latencyMs).toBe(100);
  });

  it("moves toward a new sample without jumping to it", () => {
    const health = new HealthTable({ smoothing: 0.5 });
    health.recordSuccess("cf", 100, 1);
    health.recordSuccess("cf", 200, 2);
    expect(health.get("cf").latencyMs).toBe(150);
  });

  /**
   * An outlier does move the ranking, and that is the accepted trade-off rather than an oversight.
   *
   * Smoothing at 0.3 means one 20x sample is enough to swap the leader for a probe or two. The
   * alternative — heavier smoothing, or hysteresis — buys stability at the cost of being slow to
   * notice a line that has genuinely gone bad, and the cost of a wrong leader is small: both lines
   * still answer, one just slower. So the property worth guaranteeing is not "never moves", it is
   * "comes back on its own".
   */
  it("recovers the ranking within a couple of samples after an outlier", () => {
    const health = new HealthTable({ smoothing: 0.3 });
    for (let i = 0; i < 10; i++) health.recordSuccess("cf", 20, i);
    for (let i = 0; i < 10; i++) health.recordSuccess("ipv6", 60, i);

    health.recordSuccess("cf", 400, 11); // one unlucky probe
    expect(ids(health.rank([lines[0]!, lines[1]!]))[0]).toBe("ipv6");

    health.recordSuccess("cf", 20, 12);
    health.recordSuccess("cf", 20, 13);
    health.recordSuccess("cf", 20, 14);
    expect(ids(health.rank([lines[0]!, lines[1]!]))[0]).toBe("cf");
  });

  it("does reorder for a sustained change, which is the point", () => {
    const health = new HealthTable({ smoothing: 0.3 });
    for (let i = 0; i < 10; i++) health.recordSuccess("cf", 20, i);
    for (let i = 0; i < 10; i++) health.recordSuccess("ipv6", 60, i);
    for (let i = 0; i < 10; i++) health.recordSuccess("cf", 400, i);
    expect(ids(health.rank([lines[0]!, lines[1]!]))[0]).toBe("ipv6");
  });
});

describe("HealthTable, degraded", () => {
  it("labels a line that answers far slower than the best", () => {
    const health = new HealthTable({ degradedFactor: 4 });
    health.recordSuccess("cf", 20, 1);
    health.recordSuccess("ipv6", 500, 1);
    health.reconcileDegraded();
    expect(health.get("cf").state).toBe("up");
    expect(health.get("ipv6").state).toBe("degraded");
  });

  it("clears the label when the line catches up", () => {
    const health = new HealthTable({ degradedFactor: 4, smoothing: 1 });
    health.recordSuccess("cf", 20, 1);
    health.recordSuccess("ipv6", 500, 1);
    health.reconcileDegraded();
    health.recordSuccess("ipv6", 25, 2);
    health.reconcileDegraded();
    expect(health.get("ipv6").state).toBe("up");
  });

  it("never promotes a down line to degraded", () => {
    const health = new HealthTable();
    health.recordSuccess("cf", 20, 1);
    for (let i = 0; i < 3; i++) health.recordFailure("ipv6", new Error("nope"), i);
    health.reconcileDegraded();
    expect(health.get("ipv6").state).toBe("down");
  });
});

describe("HealthTable, registry changes", () => {
  // A re-added id must not inherit the reputation of whatever used to be called that.
  it("forgets lines that have left the registry", () => {
    const health = new HealthTable();
    health.recordSuccess("cf", 10, 1);
    health.recordSuccess("gone", 10, 1);
    health.retain(["cf"]);
    expect(health.all().map((h) => h.lineId)).toEqual(["cf"]);
    expect(health.get("gone").latencyMs).toBeNull();
  });
});

describe("HealthTable, remembering between visits", () => {
  it("exports only lines it has actually measured", () => {
    const health = new HealthTable();
    health.recordSuccess("cf", 20, 1000);
    expect(health.export_().map((e) => e.lineId)).toEqual(["cf"]);
  });

  it("restores measurements so a cold start does not begin from a guess", () => {
    const health = new HealthTable();
    health.import_(
      [{ lineId: "ipv6", latencyMs: 15, throughputBps: null, at: 1000 }],
      10_000,
      1500,
    );
    expect(health.get("ipv6").latencyMs).toBe(15);
    expect(ids(health.rank(lines))[0]).toBe("ipv6");
  });

  /**
   * Seeded as measurements, not certainties: the next probe blends into them normally, so a line
   * that has genuinely changed is corrected within a few samples.
   */
  it("lets a new probe correct what was remembered", () => {
    const health = new HealthTable({ smoothing: 1 });
    health.import_([{ lineId: "cf", latencyMs: 10, throughputBps: null, at: 1000 }], 10_000, 1000);
    health.recordSuccess("cf", 900, 2000);
    expect(health.get("cf").latencyMs).toBe(900);
  });

  // The network the user was on last month says nothing about the one they are on now.
  it("ignores measurements that have gone stale", () => {
    const health = new HealthTable();
    health.import_([{ lineId: "cf", latencyMs: 10, throughputBps: null, at: 1000 }], 500, 9999);
    expect(health.get("cf").latencyMs).toBeNull();
  });

  /**
   * State is never carried over. "Down" is a fact about a moment — a line that was unreachable on a
   * train yesterday must not begin today already demoted.
   */
  it("never restores a line as down", () => {
    const health = new HealthTable();
    for (let i = 0; i < 5; i++) health.recordFailure("cf", new Error("no"), i);
    const restored = new HealthTable();
    restored.import_(health.export_(), 10_000, 1);
    expect(restored.get("cf").state).toBe("up");
  });

  it("round-trips through JSON, which is how it will actually be stored", () => {
    const health = new HealthTable();
    health.recordSuccess("cf", 42, 1000);
    health.recordThroughput("cf", 5000);
    const restored = new HealthTable();
    restored.import_(JSON.parse(JSON.stringify(health.export_())), 10_000, 1000);
    expect(restored.get("cf").latencyMs).toBe(42);
    expect(restored.get("cf").throughputBps).toBe(5000);
  });
});
