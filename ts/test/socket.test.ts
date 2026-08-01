import { beforeEach, describe, expect, it, vi } from "vitest";
import { connectOverLines, resolve, type SocketLike } from "../src/socket.js";
import type { Line } from "../src/registry.js";

const lines: Line[] = [
  { id: "cf", url: "https://cf.example" },
  { id: "ipv6", url: "https://ipv6.example" },
  { id: "frp", url: "https://frp.example" },
];

/** A socket a test drives by hand: open it, close it, error it, whenever it likes. */
class FakeSocket implements SocketLike {
  readonly listeners = new Map<string, Array<(event: unknown) => void>>();
  closed = false;

  constructor(readonly url: string) {}

  addEventListener(type: string, listener: (event: unknown) => void): void {
    const existing = this.listeners.get(type) ?? [];
    existing.push(listener);
    this.listeners.set(type, existing);
  }

  close(): void {
    this.closed = true;
  }

  emit(type: string, event: unknown = {}): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

/** A controllable clock and timer queue, so nothing here waits on real time. */
function harness() {
  const created: FakeSocket[] = [];
  let clock = 0;
  const pending: Array<{ at: number; fn: () => void; id: number }> = [];
  let nextId = 1;

  const setTimeoutFake = ((fn: () => void, ms = 0) => {
    const id = nextId++;
    pending.push({ at: clock + ms, fn, id });
    return id;
  }) as unknown as typeof globalThis.setTimeout;

  const clearTimeoutFake = ((id: number) => {
    const at = pending.findIndex((entry) => entry.id === id);
    if (at !== -1) pending.splice(at, 1);
  }) as unknown as typeof globalThis.clearTimeout;

  const advance = (ms: number) => {
    clock += ms;
    for (const entry of pending.filter((e) => e.at <= clock).sort((a, b) => a.at - b.at)) {
      const at = pending.indexOf(entry);
      if (at !== -1) pending.splice(at, 1);
      entry.fn();
    }
  };

  return {
    created,
    advance,
    now: () => clock,
    options: {
      createSocket: (url: string) => {
        const socket = new FakeSocket(url);
        created.push(socket);
        return socket;
      },
      now: () => clock,
      setTimeout: setTimeoutFake,
      clearTimeout: clearTimeoutFake,
    },
  };
}

let h: ReturnType<typeof harness>;
beforeEach(() => {
  h = harness();
});

describe("connectOverLines", () => {
  it("connects to the best line", () => {
    const connection = connectOverLines({ lines: () => lines, path: "/mt/ws", ...h.options });
    expect(h.created[0]!.url).toBe("wss://cf.example/mt/ws");
    expect(connection.current()?.id).toBe("cf");
    connection.close();
  });

  it("reports the line once it opens", () => {
    const opened: string[] = [];
    const connection = connectOverLines({
      lines: () => lines,
      path: "/mt/ws",
      onOpen: (line) => opened.push(line.id),
      ...h.options,
    });
    h.created[0]!.emit("open");
    expect(opened).toEqual(["cf"]);
    connection.close();
  });

  /**
   * A stream is stateful, so it cannot be raced: two connections are two conversations, each with
   * its own cursor and its own history. One at a time is the ceiling, not a shortcut.
   */
  it("opens exactly one connection, never a race", () => {
    const connection = connectOverLines({ lines: () => lines, path: "/mt/ws", ...h.options });
    expect(h.created).toHaveLength(1);
    connection.close();
  });

  it("moves to another line when the first will not hold", () => {
    const connection = connectOverLines({ lines: () => lines, path: "/mt/ws", ...h.options });
    h.created[0]!.emit("close");
    h.advance(1000);

    expect(h.created).toHaveLength(2);
    expect(h.created[1]!.url).toContain("ipv6");
    connection.close();
  });

  /**
   * HTTP health says almost nothing about whether a line can carry a stream. A cheap proxy serves
   * requests perfectly and refuses the Upgrade; a middlebox allows the handshake and then severs
   * anything long-lived. So failing at streams is remembered separately.
   */
  it("stops choosing a line that cannot hold a stream", () => {
    const connection = connectOverLines({ lines: () => lines, path: "/mt/ws", ...h.options });

    h.created[0]!.emit("close"); // cf cannot hold one
    h.advance(1000);
    h.created[1]!.emit("close"); // nor can ipv6
    h.advance(2000);

    expect(h.created[2]!.url).toContain("frp");
    expect([...connection.penalties().keys()].sort()).toEqual(["cf", "ipv6"]);
    connection.close();
  });

  it("lets a penalised line back in once its penalty expires", () => {
    const connection = connectOverLines({
      lines: () => [lines[0]!, lines[1]!],
      path: "/mt/ws",
      penaltyMs: 5_000,
      ...h.options,
    });

    h.created[0]!.emit("close");
    h.advance(1000);
    expect(h.created[1]!.url).toContain("ipv6");

    h.created[1]!.emit("close");
    h.advance(10_000); // both penalties have now expired

    // A line that was merely having a bad minute has to come back into rotation.
    expect(h.created[2]!.url).toContain("cf");
    connection.close();
  });

  /**
   * A client with no connection is worse than a client on a flaky one, and the penalties may all be
   * stale anyway.
   */
  it("still connects when every line is penalised", () => {
    const connection = connectOverLines({
      lines: () => [lines[0]!, lines[1]!],
      path: "/mt/ws",
      ...h.options,
    });

    h.created[0]!.emit("close");
    h.advance(1000);
    h.created[1]!.emit("close");
    h.advance(5000);

    expect(h.created.length).toBeGreaterThan(2);
    connection.close();
  });

  /**
   * Without this, a line that accepts the handshake and drops it immediately looks like a success
   * every time, and the client reconnects in a tight loop forever — a flat line on a graph and a
   * very warm laptop.
   */
  it("treats a connection that dies immediately as a failure, however cleanly it opened", () => {
    const connection = connectOverLines({
      lines: () => [lines[0]!],
      path: "/mt/ws",
      stableAfterMs: 5_000,
      ...h.options,
    });

    h.created[0]!.emit("open");
    h.advance(100);
    h.created[0]!.emit("close");

    expect(connection.penalties().has("cf")).toBe(true);
    connection.close();
  });

  it("treats a connection that lasted as an ordinary disconnection", () => {
    const connection = connectOverLines({
      lines: () => [lines[0]!],
      path: "/mt/ws",
      stableAfterMs: 1_000,
      ...h.options,
    });

    h.created[0]!.emit("open");
    h.advance(10_000);
    h.created[0]!.emit("close");

    // It worked for ten seconds; that is not evidence against the line.
    expect(connection.penalties().has("cf")).toBe(false);
    connection.close();
  });

  it("backs off further on each consecutive failure", () => {
    const connection = connectOverLines({
      lines: () => [lines[0]!],
      path: "/mt/ws",
      retryDelayMs: 100,
      ...h.options,
    });

    h.created[0]!.emit("close");
    // The first retry waits the base delay rather than double it: most disconnections are one-offs,
    // and making the common case wait twice as long is a poor trade for arithmetic tidiness.
    h.advance(100);
    expect(h.created).toHaveLength(2);

    h.created[1]!.emit("close");
    h.advance(100);
    // The second delay is doubled, so nothing new yet.
    expect(h.created).toHaveLength(2);
    h.advance(200);
    expect(h.created).toHaveLength(3);

    connection.close();
  });

  it("caps the backoff so a stream does not go unrecovered for hours", () => {
    const connection = connectOverLines({
      lines: () => [lines[0]!],
      path: "/mt/ws",
      retryDelayMs: 1000,
      maxRetryDelayMs: 4000,
      ...h.options,
    });

    for (let i = 0; i < 10; i++) {
      h.created[h.created.length - 1]!.emit("close");
      h.advance(4000);
    }
    expect(h.created.length).toBeGreaterThan(5);
    connection.close();
  });

  it("keeps trying when the registry has nothing yet", () => {
    let available: Line[] = [];
    const connection = connectOverLines({
      lines: () => available,
      path: "/mt/ws",
      retryDelayMs: 100,
      ...h.options,
    });

    expect(h.created).toHaveLength(0);
    // A registry can arrive at any moment; giving up permanently would strand the stream.
    available = lines;
    h.advance(1000);
    expect(h.created).toHaveLength(1);

    connection.close();
  });

  it("stops for good when closed", () => {
    const connection = connectOverLines({
      lines: () => lines,
      path: "/mt/ws",
      retryDelayMs: 10,
      ...h.options,
    });

    connection.close();
    expect(h.created[0]!.closed).toBe(true);

    h.created[0]!.emit("close");
    h.advance(10_000);
    // Reconnecting after an explicit close would be ignoring the caller.
    expect(h.created).toHaveLength(1);
  });

  it("passes messages through", () => {
    const seen: unknown[] = [];
    const connection = connectOverLines({
      lines: () => lines,
      path: "/mt/ws",
      onMessage: (event) => seen.push(event),
      ...h.options,
    });

    h.created[0]!.emit("message", { data: "hello" });
    expect(seen).toEqual([{ data: "hello" }]);
    connection.close();
  });

  it("survives a socket that throws on construction", () => {
    const connection = connectOverLines({
      lines: () => lines,
      path: "/mt/ws",
      ...h.options,
      createSocket: () => {
        throw new Error("blocked");
      },
      retryDelayMs: 100,
    });

    // A browser can refuse outright — a mixed-content block, a CSP rule. That is a line failing,
    // not the client crashing.
    expect(connection.penalties().has("cf")).toBe(true);
    connection.close();
  });
});

describe("resolve", () => {
  it("upgrades the scheme", () => {
    expect(resolve({ id: "a", url: "https://a.example" }, "/mt/ws")).toBe("wss://a.example/mt/ws");
    expect(resolve({ id: "a", url: "http://a.example" }, "/mt/ws")).toBe("ws://a.example/mt/ws");
  });

  it("leaves a same-origin line as a bare path, for the page's own scheme to decide", () => {
    expect(resolve({ id: "self", url: "" }, "/mt/ws")).toBe("/mt/ws");
  });
});

vi.setConfig({ testTimeout: 10_000 });
