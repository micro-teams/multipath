/*
 *  Description: The developer line panel — the only way to see, from outside, what MultiPath is
 *               actually doing.
 *
 *               Everything this library decides is invisible by construction. Requests go out over
 *               whichever line is winning, duplicates are absorbed before any controller sees them,
 *               the cache answers before the network is consulted. That invisibility is the point,
 *               and it is also what makes the whole thing hard to trust: when it is working there
 *               is nothing to see, and when it is misbehaving there is *also* nothing to see.
 *
 *               So the panel shows two things side by side. What each line is *like* — measured
 *               latency, throughput, state, last error — and what actually *happened*: which line
 *               served the recent requests. Those two disagree more often than you would expect,
 *               and the disagreement is usually where the bug is.
 *
 *               Plain DOM, no framework, no styling framework. It has to be droppable into a React
 *               app, a Vue app, or a blank page, and a diagnostic that drags in dependencies is one
 *               that gets excluded from the production build — which is exactly where you need it.
 *
 *  Author(s):
 *      agent4
 */

import type { Attempt } from "./lineManager.js";
import type { LineHealth } from "./health.js";
import type { Line } from "./registry.js";

/** What the panel needs. An interface rather than the class, so it can be driven by a fake. */
export interface PanelSource {
  readonly lines: readonly Line[];
  ranked(): Line[];
  health(): LineHealth[];
  recentAttempts(): readonly Attempt[];
  probeNow(): Promise<void>;
}

export interface PanelOptions {
  /** How often to redraw. */
  readonly refreshMs?: number;
  /** Injected in tests. */
  readonly setInterval?: typeof globalThis.setInterval;
  readonly clearInterval?: typeof globalThis.clearInterval;
}

/**
 * Render the panel into an element and keep it current.
 *
 * Returns the function that stops it. Mounting something that polls forever with no way to stop is
 * how a debugging aid becomes a background task nobody remembers starting.
 */
export function mountLinePanel(
  source: PanelSource,
  container: HTMLElement,
  options: PanelOptions = {},
): () => void {
  const refreshMs = options.refreshMs ?? 1000;
  const start = options.setInterval ?? globalThis.setInterval;
  const stop = options.clearInterval ?? globalThis.clearInterval;

  container.innerHTML = "";
  container.appendChild(styles());

  const root = document.createElement("div");
  root.className = "mp-panel";
  container.appendChild(root);

  const draw = () => {
    // Rebuilt wholesale rather than diffed. The panel is small and redrawn once a second; a diffing
    // scheme here would be more code than the thing it optimises, and a stale cell in a diagnostic
    // is worse than a flicker.
    root.replaceChildren(header(source, draw), linesTable(source), attemptsTable(source));
  };

  draw();
  const timer = start(draw, refreshMs);
  return () => stop(timer);
}

function header(source: PanelSource, redraw: () => void): HTMLElement {
  const bar = element("div", "mp-bar");
  bar.appendChild(text("strong", "MultiPath"));

  const best = source.ranked()[0];
  bar.appendChild(text("span", best ? `routing to ${best.id}` : "no line available", "mp-current"));

  const probe = element("button", "mp-button") as HTMLButtonElement;
  probe.textContent = "probe now";
  probe.onclick = () => {
    probe.disabled = true;
    probe.textContent = "probing…";
    void source.probeNow().finally(() => {
      probe.disabled = false;
      probe.textContent = "probe now";
      redraw();
    });
  };
  bar.appendChild(probe);
  return bar;
}

function linesTable(source: PanelSource): HTMLElement {
  const health = new Map(source.health().map((h) => [h.lineId, h]));
  const attempts = source.recentAttempts();
  const served = new Map<string, number>();
  for (const attempt of attempts) served.set(attempt.lineId, (served.get(attempt.lineId) ?? 0) + 1);

  const table = element("table", "mp-table");
  table.appendChild(
    row("th", ["#", "line", "transport", "state", "latency", "throughput", "served", "last error"]),
  );

  source.ranked().forEach((line, index) => {
    const h = health.get(line.id);
    const count = served.get(line.id) ?? 0;
    const tr = row("td", [
      // Position in the ranking, so "why is it using that one" is answerable at a glance.
      String(index + 1),
      line.id,
      line.transport ?? "",
      h?.state ?? "up",
      h?.latencyMs === null || h?.latencyMs === undefined ? "—" : `${Math.round(h.latencyMs)}ms`,
      h?.throughputBps === null || h?.throughputBps === undefined
        ? "—"
        : `${Math.round(h.throughputBps / 1024)} KB/s`,
      attempts.length ? `${count} (${Math.round((count / attempts.length) * 100)}%)` : "0",
      h?.lastError ?? "",
    ]);
    tr.className = `mp-state-${h?.state ?? "up"}`;
    table.appendChild(tr);
  });

  return table;
}

function attemptsTable(source: PanelSource): HTMLElement {
  const wrapper = element("div", "mp-recent");
  wrapper.appendChild(text("strong", "recent requests"));

  const table = element("table", "mp-table");
  table.appendChild(row("th", ["line", "method", "path", "took", "result"]));

  // Twenty is enough to see a pattern and short enough to read. The rest stays in memory for
  // whoever wants to inspect it from the console.
  for (const attempt of source.recentAttempts().slice(0, 20)) {
    table.appendChild(
      row("td", [
        attempt.lineId,
        attempt.method,
        attempt.path,
        `${Math.round(attempt.durationMs)}ms`,
        attempt.error ? String(attempt.error) : String(attempt.status ?? ""),
      ]),
    );
  }

  wrapper.appendChild(table);
  return wrapper;
}

function row(cell: "th" | "td", values: readonly string[]): HTMLTableRowElement {
  const tr = document.createElement("tr");
  for (const value of values) {
    const td = document.createElement(cell);
    // textContent, never innerHTML: a line id, a path and an error message all arrive from
    // elsewhere, and a diagnostic that can be made to execute what it displays is a vulnerability
    // wearing a lab coat.
    td.textContent = value;
    tr.appendChild(td);
  }
  return tr;
}

function element(tag: string, className: string): HTMLElement {
  const node = document.createElement(tag);
  node.className = className;
  return node;
}

function text(tag: string, content: string, className = ""): HTMLElement {
  const node = element(tag, className);
  node.textContent = content;
  return node;
}

/** Scoped to `.mp-panel`, so dropping this into an app cannot restyle the app. */
function styles(): HTMLStyleElement {
  const style = document.createElement("style");
  style.textContent = `
.mp-panel { font: 12px/1.5 ui-monospace, monospace; padding: 8px; }
.mp-bar { display: flex; gap: 12px; align-items: center; margin-bottom: 8px; }
.mp-current { opacity: 0.8; }
.mp-button { font: inherit; cursor: pointer; }
.mp-table { border-collapse: collapse; width: 100%; margin-bottom: 12px; }
.mp-table th, .mp-table td { text-align: left; padding: 2px 8px 2px 0; border-bottom: 1px solid currentColor; }
.mp-table th { opacity: 0.6; font-weight: normal; }
.mp-state-degraded { opacity: 0.75; }
.mp-state-down { opacity: 0.45; text-decoration: line-through; }
`;
  return style;
}
