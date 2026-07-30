/*
 *  Description: The measurement loop: cheap and constant for latency, rare and careful for
 *               throughput.
 *
 *               Two rules shape everything here.
 *
 *               A probe must not distort what it measures. Latency probes are tiny and spaced out;
 *               the throughput probe downloads real bytes, so it only runs when the application has
 *               been quiet for a while — measuring bandwidth during a burst of user traffic would
 *               compete with that traffic and report a number caused by the measurement itself.
 *
 *               A failing line must not be probed at the same rate as a healthy one. A line that is
 *               down gets exponential backoff, so a dead route costs a request every few minutes
 *               rather than every few seconds.
 *
 *  Author(s):
 *      agent4
 */

import type { HealthTable } from "./health.js";
import type { Line } from "./registry.js";

export interface ProberOptions {
  /** Path of the unauthenticated liveness endpoint. */
  readonly probePath?: string;
  /** Gap between latency probes of a healthy line. */
  readonly intervalMs?: number;
  /** A probe that has not answered within this is a failure. */
  readonly timeoutMs?: number;
  /** Backoff cap for a line that keeps failing. */
  readonly maxBackoffMs?: number;

  /** Path of a download used to measure throughput. Absent disables throughput probing entirely. */
  readonly bandwidthPath?: string;
  /** Minimum gap between throughput measurements of the same line. */
  readonly bandwidthIntervalMs?: number;
  /** How long the application must have been quiet before a throughput probe may run. */
  readonly idleBeforeBandwidthMs?: number;

  /** Injected in tests. */
  readonly now?: () => number;
}

const DEFAULTS = {
  probePath: "/mt/probe",
  intervalMs: 15_000,
  timeoutMs: 5_000,
  maxBackoffMs: 300_000,
  // Conservative on purpose, and provisional: the design doc leaves these to be settled against
  // real lines. Ten minutes between measurements of a number that changes slowly, after half a
  // minute of quiet, downloading a quarter of a megabyte — small enough not to matter on a metered
  // connection, large enough to outlast connection setup and actually measure the pipe.
  bandwidthIntervalMs: 600_000,
  idleBeforeBandwidthMs: 30_000,
} as const;

/**
 * Drives the health table.
 *
 * Deliberately not started by the constructor: probing is a background activity with a real cost,
 * and a library that begins making network requests the moment it is instantiated is a library
 * that surprises people. The caller says when.
 */
export class Prober {
  private readonly options: Required<Omit<ProberOptions, "bandwidthPath" | "now">> & {
    bandwidthPath: string | undefined;
    now: () => number;
  };
  private timer: ReturnType<typeof setTimeout> | null = null;
  private running = false;
  /** Next allowed probe time per line, which is how backoff is expressed. */
  private readonly nextProbeAt = new Map<string, number>();
  private readonly nextBandwidthAt = new Map<string, number>();
  /** When the application last issued a real request, so quiet can be detected. */
  private lastTrafficAt = 0;

  constructor(
    private readonly lines: () => readonly Line[],
    private readonly health: HealthTable,
    private readonly fetchImpl: typeof globalThis.fetch,
    private readonly resolve: (path: string, line: Line) => string,
    options: ProberOptions = {},
  ) {
    this.options = {
      probePath: options.probePath ?? DEFAULTS.probePath,
      intervalMs: options.intervalMs ?? DEFAULTS.intervalMs,
      timeoutMs: options.timeoutMs ?? DEFAULTS.timeoutMs,
      maxBackoffMs: options.maxBackoffMs ?? DEFAULTS.maxBackoffMs,
      bandwidthIntervalMs: options.bandwidthIntervalMs ?? DEFAULTS.bandwidthIntervalMs,
      idleBeforeBandwidthMs: options.idleBeforeBandwidthMs ?? DEFAULTS.idleBeforeBandwidthMs,
      bandwidthPath: options.bandwidthPath,
      now: options.now ?? (() => Date.now()),
    };
  }

  /** Called by the line manager on every real request, so the prober can tell quiet from busy. */
  noteTraffic(): void {
    this.lastTrafficAt = this.options.now();
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    void this.tick();
  }

  stop(): void {
    this.running = false;
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
  }

  /** Probe everything now and wait for it. The panel's refresh button, and every test's lever. */
  async probeAll(): Promise<void> {
    await Promise.all(this.lines().map((line) => this.probe(line)));
    this.health.reconcileDegraded();
  }

  private async tick(): Promise<void> {
    if (!this.running) return;

    const now = this.options.now();
    const due = this.lines().filter((line) => (this.nextProbeAt.get(line.id) ?? 0) <= now);
    await Promise.all(due.map((line) => this.probe(line)));
    this.health.reconcileDegraded();

    if (this.options.bandwidthPath !== undefined) await this.maybeMeasureThroughput();

    if (!this.running) return;
    // One timer for all lines rather than one per line: the schedule is already carried by
    // nextProbeAt, and a single wakeup keeps a phone's radio asleep between rounds.
    this.timer = setTimeout(() => void this.tick(), this.options.intervalMs);
  }

  private async probe(line: Line): Promise<void> {
    const started = this.options.now();
    try {
      const response = await this.withTimeout(
        this.fetchImpl(this.resolve(this.options.probePath, line), {
          method: "GET",
          cache: "no-store",
          ...(line.url === "" ? {} : { credentials: "include" as const }),
        }),
      );
      if (!response.ok) throw new Error(`probe answered ${response.status}`);
      this.health.recordSuccess(line.id, this.options.now() - started, this.options.now());
      this.nextProbeAt.set(line.id, this.options.now() + this.options.intervalMs);
    } catch (error) {
      this.health.recordFailure(line.id, error, this.options.now());
      const failures = this.health.get(line.id).consecutiveFailures;
      this.nextProbeAt.set(line.id, this.options.now() + this.backoff(failures));
    }
  }

  /**
   * Measure throughput on one line, if the application has been quiet long enough.
   *
   * One line per round, never all of them: measuring several at once would have them compete for
   * the same pipe and each report a fraction of the truth.
   */
  private async maybeMeasureThroughput(): Promise<void> {
    const now = this.options.now();
    if (now - this.lastTrafficAt < this.options.idleBeforeBandwidthMs) return;

    const candidate = this.lines().find(
      (line) =>
        this.health.get(line.id).state !== "down" &&
        (this.nextBandwidthAt.get(line.id) ?? 0) <= now,
    );
    if (!candidate) return;

    // Claim the slot before measuring, so a failure cannot make it retry on the very next tick.
    this.nextBandwidthAt.set(candidate.id, now + this.options.bandwidthIntervalMs);

    const started = this.options.now();
    try {
      const response = await this.fetchImpl(
        this.resolve(this.options.bandwidthPath!, candidate),
        // no-store, or a second measurement would read from the disk cache and report the speed of
        // the disk.
        { method: "GET", cache: "no-store" },
      );
      const bytes = (await response.arrayBuffer()).byteLength;
      const seconds = (this.options.now() - started) / 1000;
      if (seconds > 0 && bytes > 0) this.health.recordThroughput(candidate.id, bytes / seconds);
    } catch (error) {
      this.health.recordFailure(candidate.id, error, this.options.now());
    }
  }

  /** Exponential, capped: a dead line costs one request every few minutes, not every few seconds. */
  private backoff(consecutiveFailures: number): number {
    const grown = this.options.intervalMs * 2 ** Math.min(consecutiveFailures, 10);
    return Math.min(grown, this.options.maxBackoffMs);
  }

  private withTimeout(promise: Promise<Response>): Promise<Response> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`probe timed out after ${this.options.timeoutMs}ms`)),
        this.options.timeoutMs,
      );
      promise.then(resolve, reject).finally(() => clearTimeout(timer));
    });
  }
}
