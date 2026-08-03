/*
 *  Description: Choosing a line for a stream, and choosing again when it breaks.
 *
 *               A stream cannot be raced. Two connections are two conversations, each with its own
 *               state — a terminal session, a subscription, a message cursor — so the most that is
 *               possible here is: pick the best line, and when it breaks, pick again. That is the
 *               honest ceiling, not a shortcut.
 *
 *               The part that is easy to miss: HTTP health says almost nothing about whether a line
 *               can carry a WebSocket. A cheap reverse proxy will serve requests perfectly and
 *               refuse the Upgrade; a corporate middlebox will allow the handshake and then sever
 *               anything long-lived. So a line's ability to hold a stream is tracked separately
 *               from its latency, and a line that fails at this is skipped for streams while still
 *               being perfectly good for requests.
 *
 *  Author(s):
 *      agent4
 */

import type { Line } from "./registry.js";

/** Just enough of a WebSocket to drive one. Anything with these members will do. */
export interface SocketLike {
  close(code?: number, reason?: string): void;
  addEventListener(type: string, listener: (event: unknown) => void): void;
}

export interface SocketOptions {
  /** Lines in preference order — normally `manager.ranked()`, read afresh on every attempt. */
  readonly lines: () => readonly Line[];
  /** Path to connect to, joined to each line's origin. */
  readonly path: string;
  /** Builds the socket. Injected so this is testable, and so any client library can be used. */
  readonly createSocket: (url: string) => SocketLike;
  /**
   * How long a connection must survive before it counts as working.
   *
   * Without this, a line that accepts the handshake and drops it immediately looks like a success
   * every time, and the client reconnects to it in a tight loop forever — the failure mode is a
   * flat line on a graph and a very warm laptop.
   */
  readonly stableAfterMs?: number;
  /** First reconnect delay; doubles per consecutive failure up to the cap. */
  readonly retryDelayMs?: number;
  readonly maxRetryDelayMs?: number;
  /** How long a line is skipped for streams after it fails to hold one. */
  readonly penaltyMs?: number;
  readonly onOpen?: (line: Line) => void;
  readonly onClose?: (line: Line, reason: string) => void;
  readonly onMessage?: (event: unknown) => void;
  /** Injected in tests. */
  readonly now?: () => number;
  readonly setTimeout?: typeof globalThis.setTimeout;
  readonly clearTimeout?: typeof globalThis.clearTimeout;
}

const DEFAULTS = {
  stableAfterMs: 5_000,
  retryDelayMs: 500,
  maxRetryDelayMs: 30_000,
  // Long enough that a proxy which cannot do WebSockets at all stops being tried every few seconds,
  // short enough that a line which was merely having a bad minute comes back into rotation.
  penaltyMs: 60_000,
} as const;

export interface SocketConnection {
  /** The line currently carrying the stream, or null while between attempts. */
  current(): Line | null;
  /** Lines that recently failed to hold a stream, and until when. For the developer panel. */
  penalties(): ReadonlyMap<string, number>;
  /** Stop, and stay stopped. */
  close(): void;
}

/**
 * Keep a stream connected, over whichever line can hold one.
 *
 * Reconnection is not optional politeness here: every line in a MultiPath deployment is expected to
 * be less reliable than a single well-chosen one, and the whole bet is that having several beats
 * having one good one. That only pays if breaking is routine and recovering is automatic.
 */
export function connectOverLines(options: SocketOptions): SocketConnection {
  const stableAfterMs = options.stableAfterMs ?? DEFAULTS.stableAfterMs;
  const retryDelayMs = options.retryDelayMs ?? DEFAULTS.retryDelayMs;
  const maxRetryDelayMs = options.maxRetryDelayMs ?? DEFAULTS.maxRetryDelayMs;
  const penaltyMs = options.penaltyMs ?? DEFAULTS.penaltyMs;
  const now = options.now ?? (() => Date.now());
  const start = options.setTimeout ?? globalThis.setTimeout;
  const stop = options.clearTimeout ?? globalThis.clearTimeout;

  /** Lines that failed to hold a stream, and the time they may be tried again. */
  const penalised = new Map<string, number>();
  let socket: SocketLike | null = null;
  let line: Line | null = null;
  let consecutiveFailures = 0;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let closed = false;

  /**
   * Best line that is not currently serving a penalty.
   *
   * If every line is penalised, the least-recently-penalised is used anyway rather than giving up:
   * a client with no connection is worse than a client on a flaky one, and the penalties may all be
   * stale.
   */
  const pick = (): Line | null => {
    const candidates = options.lines();
    if (candidates.length === 0) return null;

    const at = now();
    const usable = candidates.filter((candidate) => (penalised.get(candidate.id) ?? 0) <= at);
    if (usable.length > 0) return usable[0]!;

    return [...candidates].sort(
      (a, b) => (penalised.get(a.id) ?? 0) - (penalised.get(b.id) ?? 0),
    )[0]!;
  };

  const scheduleRetry = () => {
    if (closed) return;
    // The first retry waits the base delay, not double it. Most disconnections are one-offs, and
    // making the common case wait twice as long as necessary is a poor trade for arithmetic tidiness.
    const exponent = Math.min(Math.max(consecutiveFailures - 1, 0), 8);
    const delay = Math.min(retryDelayMs * 2 ** exponent, maxRetryDelayMs);
    timer = start(attempt, delay);
  };

  const attempt = () => {
    if (closed) return;

    const chosen = pick();
    if (!chosen) {
      // Nothing to connect to yet — a registry that has not loaded, say. Keep trying rather than
      // giving up permanently, since the registry can arrive at any moment.
      consecutiveFailures += 1;
      scheduleRetry();
      return;
    }

    line = chosen;
    const openedAt = now();
    let settled = false;

    const failed = (reason: string) => {
      if (settled) return;
      settled = true;
      const held = now() - openedAt;
      // A connection that never became stable is evidence about this line's ability to carry a
      // stream — which is a different question from whether it answers requests quickly.
      if (held < stableAfterMs) {
        penalised.set(chosen.id, now() + penaltyMs);
        consecutiveFailures += 1;
      } else {
        // It worked for a while; an ordinary disconnection, so reconnect promptly.
        consecutiveFailures = 0;
      }
      socket = null;
      line = null;
      options.onClose?.(chosen, reason);
      scheduleRetry();
    };

    try {
      const created = options.createSocket(resolve(chosen, options.path));
      socket = created;

      created.addEventListener("open", () => {
        options.onOpen?.(chosen);
        // Not cleared here: opening proves the handshake, not that the line can hold a stream.
        // Only surviving `stableAfterMs` proves that, and `failed` decides based on how long it
        // actually lasted.
        start(() => {
          if (!settled && socket === created) {
            consecutiveFailures = 0;
            penalised.delete(chosen.id);
          }
        }, stableAfterMs);
      });
      created.addEventListener("message", (event) => options.onMessage?.(event));
      created.addEventListener("close", () => failed("closed"));
      created.addEventListener("error", () => failed("error"));
    } catch (error) {
      failed(String(error));
    }
  };

  attempt();

  return {
    current: () => line,
    penalties: () => penalised,
    close: () => {
      closed = true;
      if (timer !== null) stop(timer);
      timer = null;
      socket?.close();
      socket = null;
      line = null;
    },
  };
}

/** ws:// for http:// lines, wss:// for https://, and the page's own scheme for a same-origin line. */
export function resolve(line: Line, path: string): string {
  if (line.url === "") return path;
  return `${line.url.replace(/^http/, "ws")}${path}`;
}
