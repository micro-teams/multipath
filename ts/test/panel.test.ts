// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";
import { mountLinePanel, type PanelSource } from "../src/panel.js";
import type { LineHealth } from "../src/health.js";
import type { Line } from "../src/registry.js";
import type { Attempt } from "../src/lineManager.js";

function health(over: Partial<LineHealth> & { lineId: string }): LineHealth {
  return {
    state: "up",
    latencyMs: null,
    throughputBps: null,
    consecutiveFailures: 0,
    lastError: null,
    lastProbedAt: null,
    ...over,
  };
}

function attempt(over: Partial<Attempt> & { lineId: string }): Attempt {
  return { method: "GET", path: "/mt/probe", durationMs: 10, status: 200, ...over };
}

function source(over: Partial<PanelSource> = {}): PanelSource {
  const lines: Line[] = [
    { id: "cf", url: "https://cf.example", transport: "cloudflare" },
    { id: "ipv6", url: "https://ipv6.example", transport: "ipv6" },
  ];
  return {
    lines,
    ranked: () => lines,
    health: () => lines.map((l) => health({ lineId: l.id })),
    recentAttempts: () => [],
    probeNow: () => Promise.resolve(),
    ...over,
  };
}

let container: HTMLElement;
beforeEach(() => {
  document.body.innerHTML = "";
  container = document.createElement("div");
  document.body.appendChild(container);
});

describe("mountLinePanel", () => {
  it("shows every line", () => {
    mountLinePanel(source(), container);
    expect(container.textContent).toContain("cf");
    expect(container.textContent).toContain("ipv6");
  });

  // "Why is it using that one" should be answerable at a glance, without reading code.
  it("says which line requests are going to", () => {
    mountLinePanel(source(), container);
    expect(container.textContent).toContain("routing to cf");
  });

  it("shows measured latency, and a dash where nothing has been measured", () => {
    const s = source({
      health: () => [
        health({ lineId: "cf", latencyMs: 42.4 }),
        health({ lineId: "ipv6", latencyMs: null }),
      ],
    });
    mountLinePanel(s, container);
    expect(container.textContent).toContain("42ms");
    expect(container.textContent).toContain("—");
  });

  it("shows throughput in units a person reads", () => {
    const s = source({ health: () => [health({ lineId: "cf", throughputBps: 2048 })] });
    mountLinePanel(s, container);
    expect(container.textContent).toContain("2 KB/s");
  });

  it("shows the last error, which is usually the whole story", () => {
    const s = source({
      health: () => [health({ lineId: "cf", state: "down", lastError: "connection refused" })],
    });
    mountLinePanel(s, container);
    expect(container.textContent).toContain("connection refused");
  });

  it("marks a down line visibly rather than only in a column", () => {
    const s = source({ health: () => [health({ lineId: "cf", state: "down" })] });
    mountLinePanel(s, container);
    expect(container.querySelector(".mp-state-down")).not.toBeNull();
  });

  /**
   * Health says what a line is *like*; this says what actually happened. The two disagree more
   * often than expected, and the disagreement is usually where the bug is.
   */
  it("shows how the recent requests were distributed", () => {
    const s = source({
      recentAttempts: () => [
        attempt({ lineId: "cf" }),
        attempt({ lineId: "cf" }),
        attempt({ lineId: "ipv6" }),
        attempt({ lineId: "cf" }),
      ],
    });
    mountLinePanel(s, container);
    expect(container.textContent).toContain("3 (75%)");
    expect(container.textContent).toContain("1 (25%)");
  });

  it("lists the recent requests themselves", () => {
    const s = source({
      recentAttempts: () => [attempt({ lineId: "ipv6", method: "POST", path: "/mt/messages" })],
    });
    mountLinePanel(s, container);
    expect(container.textContent).toContain("POST");
    expect(container.textContent).toContain("/mt/messages");
  });

  it("shows a failure as its error rather than a blank cell", () => {
    const s = source({
      recentAttempts: () => [
        { lineId: "cf", method: "GET", path: "/x", durationMs: 5, error: new Error("boom") },
      ],
    });
    mountLinePanel(s, container);
    expect(container.textContent).toContain("boom");
  });

  it("copes with no lines at all", () => {
    const s = source({ lines: [], ranked: () => [], health: () => [] });
    expect(() => mountLinePanel(s, container)).not.toThrow();
    expect(container.textContent).toContain("no line available");
  });
});

describe("mountLinePanel, safety", () => {
  /**
   * Line ids, paths and error messages all arrive from elsewhere — a registry served over the
   * network, a server's error text. A diagnostic that can be made to execute what it displays is a
   * vulnerability wearing a lab coat.
   */
  it("displays hostile text rather than executing it", () => {
    const s = source({
      lines: [{ id: "<img src=x onerror=alert(1)>", url: "" }],
      ranked: () => [{ id: "<img src=x onerror=alert(1)>", url: "" }],
      health: () => [health({ lineId: "<img src=x onerror=alert(1)>" })],
    });
    mountLinePanel(s, container);
    expect(container.querySelector("img")).toBeNull();
    expect(container.textContent).toContain("<img src=x onerror=alert(1)>");
  });

  // Dropping a diagnostic into an application must not restyle the application.
  it("scopes its styles", () => {
    mountLinePanel(source(), container);
    const css = container.querySelector("style")!.textContent!;
    const selectors = css.match(/^\s*([^{@]+)\{/gm) ?? [];
    expect(selectors.every((s) => s.includes(".mp-"))).toBe(true);
  });
});

describe("mountLinePanel, lifecycle", () => {
  it("keeps itself current", () => {
    let latency = 10;
    const s = source({ health: () => [health({ lineId: "cf", latencyMs: latency })] });
    const timers: Array<() => void> = [];
    mountLinePanel(s, container, {
      setInterval: ((fn: () => void) => {
        timers.push(fn);
        return 1;
      }) as unknown as typeof globalThis.setInterval,
    });

    expect(container.textContent).toContain("10ms");
    latency = 250;
    timers.forEach((t) => t());
    expect(container.textContent).toContain("250ms");
  });

  // Mounting something that polls forever with no way to stop is how a debugging aid becomes a
  // background task nobody remembers starting.
  it("stops when unmounted", () => {
    const cleared: number[] = [];
    const unmount = mountLinePanel(source(), container, {
      setInterval: (() => 42) as unknown as typeof globalThis.setInterval,
      clearInterval: ((id: number) =>
        cleared.push(id)) as unknown as typeof globalThis.clearInterval,
    });
    unmount();
    expect(cleared).toEqual([42]);
  });

  it("probes on demand and redraws with the answer", async () => {
    let probed = 0;
    const s = source({
      probeNow: async () => {
        probed += 1;
      },
    });
    mountLinePanel(s, container);
    const button = container.querySelector("button")!;
    button.click();
    await vi.waitFor(() => expect(probed).toBe(1));
  });

  it("replaces whatever was in the container", () => {
    container.innerHTML = "<p>stale</p>";
    mountLinePanel(source(), container);
    expect(container.textContent).not.toContain("stale");
  });
});
