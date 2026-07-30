/*
 *  Description: What the line manager knows about each line, and how it comes to know it.
 *
 *               Two measurements, because one is not enough and the second is not free.
 *
 *               Round-trip time is cheap, so it is measured constantly and drives the ranking.
 *               Throughput is not, so it is measured rarely and only when nobody is waiting on
 *               anything — and it matters because the problem this whole library exists for is a
 *               line that is *slow*, not a line that is far. A CDN edge can answer a probe in 20ms
 *               and still take five seconds to deliver a bundle. Ranking on latency alone would
 *               happily route everything through it.
 *
 *  Author(s):
 *      agent4
 */

/** How usable a line looks right now. */
export type LineState =
  /** Answering. */
  | "up"
  /** Answering, but slowly or with recent errors — usable, ranked last. */
  | "degraded"
  /** Not answering. Skipped entirely until a probe says otherwise. */
  | "down";

/** Everything measured about one line. Read by the ranking, and by the developer panel. */
export interface LineHealth {
  readonly lineId: string;
  readonly state: LineState;
  /** Smoothed round-trip time in milliseconds, or null before the first probe answers. */
  readonly latencyMs: number | null;
  /** Smoothed throughput in bytes per second, or null until an idle measurement has happened. */
  readonly throughputBps: number | null;
  /** Consecutive probe failures. Drives both the state and the backoff. */
  readonly consecutiveFailures: number;
  readonly lastError: string | null;
  /** When the last probe completed, as epoch milliseconds. */
  readonly lastProbedAt: number | null;
}

export interface HealthOptions {
  /**
   * Weight of each new sample in the EWMA, between 0 and 1.
   *
   * 0.3 keeps roughly the last handful of probes in view: fast enough to notice a line going bad
   * within seconds, slow enough that one unlucky sample does not reorder the ranking. A ranking
   * that flaps is worse than a slightly stale one, because every flap moves traffic.
   */
  readonly smoothing?: number;
  /** Consecutive failures before a line is considered down rather than merely unlucky. */
  readonly failuresBeforeDown?: number;
  /** A line answering slower than this multiple of the best line is degraded. */
  readonly degradedFactor?: number;
}

const DEFAULTS = {
  smoothing: 0.3,
  failuresBeforeDown: 3,
  degradedFactor: 4,
} as const;

/**
 * The health table.
 *
 * Separate from the line manager because ranking and measuring are different jobs with different
 * lifetimes: measurement runs on a timer whether or not anybody is making requests, and the
 * developer panel wants to read it without touching the request path at all.
 */
export class HealthTable {
  private readonly entries = new Map<string, LineHealth>();
  private readonly smoothing: number;
  private readonly failuresBeforeDown: number;
  private readonly degradedFactor: number;

  constructor(options: HealthOptions = {}) {
    this.smoothing = options.smoothing ?? DEFAULTS.smoothing;
    this.failuresBeforeDown = options.failuresBeforeDown ?? DEFAULTS.failuresBeforeDown;
    this.degradedFactor = options.degradedFactor ?? DEFAULTS.degradedFactor;
  }

  /**
   * A line nobody has measured yet is `up`, not `down`.
   *
   * Optimism is the safe default here: a fresh line with no data must be usable, or a client that
   * has only just started has nothing to send anything over.
   */
  get(lineId: string): LineHealth {
    return (
      this.entries.get(lineId) ?? {
        lineId,
        state: "up",
        latencyMs: null,
        throughputBps: null,
        consecutiveFailures: 0,
        lastError: null,
        lastProbedAt: null,
      }
    );
  }

  all(): LineHealth[] {
    return [...this.entries.values()];
  }

  /** Forget everything — used when the registry changes out from under us. */
  retain(lineIds: Iterable<string>): void {
    const keep = new Set(lineIds);
    for (const id of [...this.entries.keys()]) {
      if (!keep.has(id)) this.entries.delete(id);
    }
  }

  recordSuccess(lineId: string, latencyMs: number, at: number): void {
    const previous = this.get(lineId);
    this.entries.set(lineId, {
      ...previous,
      state: "up",
      latencyMs: this.blend(previous.latencyMs, latencyMs),
      consecutiveFailures: 0,
      lastError: null,
      lastProbedAt: at,
    });
  }

  recordFailure(lineId: string, error: unknown, at: number): void {
    const previous = this.get(lineId);
    const consecutiveFailures = previous.consecutiveFailures + 1;
    this.entries.set(lineId, {
      ...previous,
      // One failure is noise; a run of them is a fact. Demoting on the first would take a line out
      // of service for a dropped packet.
      state: consecutiveFailures >= this.failuresBeforeDown ? "down" : previous.state,
      consecutiveFailures,
      lastError: describe(error),
      lastProbedAt: at,
    });
  }

  recordThroughput(lineId: string, bytesPerSecond: number): void {
    const previous = this.get(lineId);
    this.entries.set(lineId, {
      ...previous,
      throughputBps: this.blend(previous.throughputBps, bytesPerSecond),
    });
  }

  /**
   * Rank lines best-first.
   *
   * `down` lines go last rather than being removed: if every line is down, the caller still has to
   * send the request somewhere, and refusing to try is strictly worse than trying the least-bad
   * option. Among usable lines, measured latency decides; throughput breaks ties the same way,
   * because two lines that answer equally fast are distinguished by how much they can actually
   * carry. Configured weight decides only when nothing has been measured at all — a hand-set number
   * goes stale, and a measurement does not.
   */
  rank<T extends { id: string; weight?: number }>(lines: readonly T[]): T[] {
    return [...lines].sort((a, b) => {
      const ha = this.get(a.id);
      const hb = this.get(b.id);

      const byState = stateRank(ha.state) - stateRank(hb.state);
      if (byState !== 0) return byState;

      if (ha.latencyMs !== null && hb.latencyMs !== null && ha.latencyMs !== hb.latencyMs) {
        return ha.latencyMs - hb.latencyMs;
      }
      // A line that has answered outranks one that never has: measured beats unknown.
      if (ha.latencyMs !== null && hb.latencyMs === null) return -1;
      if (ha.latencyMs === null && hb.latencyMs !== null) return 1;

      if (
        ha.throughputBps !== null &&
        hb.throughputBps !== null &&
        ha.throughputBps !== hb.throughputBps
      ) {
        return hb.throughputBps - ha.throughputBps;
      }

      return (b.weight ?? 0) - (a.weight ?? 0);
    });
  }

  /** Mark lines that answer far slower than the best one. Purely a label for the panel. */
  reconcileDegraded(): void {
    const latencies = this.all()
      .filter((h) => h.state !== "down" && h.latencyMs !== null)
      .map((h) => h.latencyMs!);
    if (latencies.length === 0) return;
    const best = Math.min(...latencies);

    for (const health of this.all()) {
      if (health.state === "down" || health.latencyMs === null) continue;
      const degraded = health.latencyMs > best * this.degradedFactor;
      const state: LineState = degraded ? "degraded" : "up";
      if (state !== health.state) this.entries.set(health.lineId, { ...health, state });
    }
  }

  /**
   * Exponentially weighted moving average, seeded by the first sample.
   *
   * Seeding rather than starting from zero matters: starting from zero would make a line look
   * impossibly fast for its first few probes and win a ranking it has not earned.
   */
  private blend(previous: number | null, sample: number): number {
    if (previous === null) return sample;
    return previous * (1 - this.smoothing) + sample * this.smoothing;
  }
}

function stateRank(state: LineState): number {
  return state === "up" ? 0 : state === "degraded" ? 1 : 2;
}

function describe(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}
