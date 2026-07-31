/*
 *  Description: How many lines a request is allowed to use, and when.
 *
 *               Reads and writes get opposite treatment, and the asymmetry is the whole point.
 *
 *               A read can be repeated freely — two copies of an answer are one answer — so the
 *               only cost of asking twice is bandwidth. It is therefore *hedged*: ask the best
 *               line, and if it has not answered shortly, ask the others too and take whichever
 *               replies first. Hedging rather than always fanning out, because fanning out
 *               multiplies every request by the number of lines to buy an improvement that only
 *               exists on the slow tail.
 *
 *               A write cannot be repeated freely, so it is never raced. It goes to one line, and
 *               only a failure moves it to the next — carrying the same idempotency key, so the
 *               server can recognise the second arrival as the same attempt. Concurrency is the
 *               fallback here, not the default.
 *
 *  Author(s):
 *      agent4
 */

import type { Line } from "./registry.js";

export interface StrategyOptions {
  /**
   * How long the best line gets alone before the others are asked as well.
   *
   * Small enough that a stalled line does not cost the user a visible pause, large enough that a
   * healthy line answers well within it and no second request is ever sent. On a good line this
   * budget is never spent, which is what makes hedging cheap in the common case.
   */
  readonly hedgeAfterMs?: number;
  /** Give up on a read entirely once every line has had this long. */
  readonly readTimeoutMs?: number;
  /** Give up on one write attempt after this, and try the next line. */
  readonly writeTimeoutMs?: number;
  /** How many lines a write may be attempted on before the failure is reported. */
  readonly maxWriteAttempts?: number;
}

export const STRATEGY_DEFAULTS = {
  hedgeAfterMs: 150,
  readTimeoutMs: 20_000,
  writeTimeoutMs: 10_000,
  maxWriteAttempts: 3,
} as const;

/** One attempt, as the strategies issue it. */
export type Attemptor = (line: Line, signal: AbortSignal) => Promise<Response>;

/**
 * Ask the best line; if it is slow, ask the rest; take the first answer.
 *
 * The losers are aborted as soon as somebody wins, so a hedge costs one extra request for a moment
 * rather than N requests to completion.
 *
 * "First answer" means the first *response*, including an error status. A 404 from the fastest line
 * is the truth about the resource, not a failure to route around — treating it as one would have
 * every line asked for something that does not exist, N times, on every such request.
 */
export async function hedgedRead(
  lines: readonly Line[],
  attempt: Attemptor,
  options: Required<Pick<StrategyOptions, "hedgeAfterMs" | "readTimeoutMs">>,
  signal?: AbortSignal,
): Promise<Response> {
  if (lines.length === 0) throw new Error("no line available to serve the request");

  const controllers: AbortController[] = [];
  const abortAll = () => controllers.forEach((c) => c.abort());
  const onExternalAbort = () => abortAll();
  signal?.addEventListener("abort", onExternalAbort, { once: true });

  const failures: unknown[] = [];
  let settled = false;

  try {
    return await new Promise<Response>((resolve, reject) => {
      let launched = 0;
      let hedgeTimer: ReturnType<typeof setTimeout> | null = null;

      const overall = setTimeout(() => {
        if (settled) return;
        settled = true;
        reject(new Error(`read timed out after ${options.readTimeoutMs}ms on every line`));
      }, options.readTimeoutMs);

      const finish = (fn: () => void) => {
        settled = true;
        clearTimeout(overall);
        if (hedgeTimer !== null) clearTimeout(hedgeTimer);
        fn();
      };

      const launch = (line: Line) => {
        launched += 1;
        const controller = new AbortController();
        controllers.push(controller);
        attempt(line, controller.signal).then(
          (response) => {
            if (settled) return;
            finish(() => resolve(response));
          },
          (error) => {
            if (settled) return;
            failures.push(error);
            // Only when every line has been tried and every one has failed is the read a failure.
            // Until then a loser is just a loser.
            if (failures.length === lines.length) {
              finish(() => reject(failures[0]));
              return;
            }
            // A line that fails immediately should not leave the request waiting out the hedge
            // delay for company it will never get.
            if (launched < lines.length) hedgeNow();
          },
        );
      };

      const hedgeNow = () => {
        if (hedgeTimer !== null) clearTimeout(hedgeTimer);
        hedgeTimer = null;
        for (const line of lines.slice(launched)) launch(line);
      };

      launch(lines[0]!);
      if (lines.length > 1) hedgeTimer = setTimeout(hedgeNow, options.hedgeAfterMs);
    });
  } finally {
    signal?.removeEventListener("abort", onExternalAbort);
    abortAll();
  }
}

/**
 * Send a write down one line; on failure, send the same write down the next.
 *
 * Never in parallel. A write that is genuinely raced relies entirely on the server to collapse the
 * duplicates, which works but wastes a request every time to save one occasionally. Sequential
 * failover pays only when something has actually gone wrong.
 *
 * A *response* ends the matter, whatever its status. Only a transport failure — no answer at all —
 * justifies another line, because only then is it unknown whether the write happened. A 500 is an
 * answer: the request arrived, the server decided, and re-sending it over a different route asks
 * the same server the same question. Whether to retry that is the caller's business, not the
 * transport's.
 */
export async function writeWithFailover(
  lines: readonly Line[],
  attempt: Attemptor,
  options: Required<Pick<StrategyOptions, "writeTimeoutMs" | "maxWriteAttempts">>,
  signal?: AbortSignal,
): Promise<Response> {
  if (lines.length === 0) throw new Error("no line available to serve the request");

  const candidates = lines.slice(0, Math.max(1, options.maxWriteAttempts));
  let lastError: unknown;

  for (const line of candidates) {
    const controller = new AbortController();
    const onExternalAbort = () => controller.abort();
    signal?.addEventListener("abort", onExternalAbort, { once: true });

    try {
      // The deadline is enforced here rather than left to the transport. Aborting the signal asks
      // the transport to stop, and a real `fetch` obliges — but a timeout that only works when the
      // transport cooperates is not a timeout, and a hung attempt would otherwise pin the whole
      // write to a line that has stopped answering.
      return await deadline(
        attempt(line, controller.signal),
        options.writeTimeoutMs,
        () => controller.abort(),
        `write timed out after ${options.writeTimeoutMs}ms on line ${line.id}`,
      );
    } catch (error) {
      lastError = error;
      // The caller cancelled. Trying another line would be sending a write they asked us not to.
      if (signal?.aborted) throw error;
    } finally {
      signal?.removeEventListener("abort", onExternalAbort);
    }
  }

  throw lastError;
}

/**
 * Resolve with the promise, or reject when the time is up — and say so on the way out.
 *
 * `onExpiry` is the request to stop; the rejection is what makes the caller stop waiting. Both are
 * needed: without the first the abandoned attempt keeps a connection open, and without the second a
 * transport that ignores abort would hang forever.
 */
function deadline<T>(
  promise: Promise<T>,
  ms: number,
  onExpiry: () => void,
  message: string,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      onExpiry();
      reject(new Error(message));
    }, ms);
    promise.then(resolve, reject).finally(() => clearTimeout(timer));
  });
}
