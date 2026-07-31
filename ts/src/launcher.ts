/*
 *  Description: The launcher: one small self-contained HTML file whose only job is to start the
 *               real application.
 *
 *               There is exactly one moment in the whole system that cannot be spread across lines.
 *               A browser opening a URL knows one host, and no amount of client-side cleverness
 *               changes that — the first document must come from the domain the user typed. That
 *               single request is therefore the one thing worth making as small as possible, and
 *               isolating so nothing else is stuck behind it.
 *
 *               So: no framework, no bundle, no imports of its own. It registers the Service
 *               Worker, carries the line registry inline so the app can route before it has fetched
 *               anything, and then loads the real entry point. Everything after that comes from the
 *               cache or from whichever line is fastest, and the domain the user typed stops
 *               mattering.
 *
 *               Once the worker is installed even this file comes from the cache, at which point
 *               starting the application involves no network at all.
 *
 *  Author(s):
 *      agent4
 */

import type { Registry } from "./registry.js";

export interface LauncherOptions {
  /**
   * The application's real entry point, as an origin-relative path.
   *
   * A path rather than a URL, because the launcher races it across the lines: naming one host here
   * would be choosing, on the one visit where nothing is known yet, which line the whole first load
   * depends on.
   */
  readonly appEntry: string;
  /** Where the Service Worker lives. Omit to skip registration entirely. */
  readonly serviceWorker?: string;
  /**
   * The registry, inlined.
   *
   * Inlined rather than fetched, because fetching it would put a network round trip on the one path
   * that has no redundancy — and a failure there would leave the app unable to reach any line,
   * having failed to learn that the lines exist.
   */
  readonly registry?: Registry;
  /** URL to refresh the registry from once the app is running. */
  readonly registryUrl?: string;
  /**
   * Lines remembered as fastest from previous visits, best first.
   *
   * The race decides the entry point on its own, so this is not needed for correctness. It matters
   * for everything *after* the entry point: the application inherits this order for its own
   * requests, which are hedged rather than raced and therefore do care which line is tried first.
   */
  readonly preferredLineIds?: readonly string[];
  readonly title?: string;
  /** Extra markup inside the body — a splash screen, a spinner, a noscript notice. */
  readonly bodyHtml?: string;
  /** Scope for the Service Worker registration. */
  readonly scope?: string;
  /**
   * Whether the worker file is an ES module.
   *
   * It matters more than it looks: a worker containing `import` registered as `"classic"` fails to
   * parse, and the failure is quiet — registration rejects, the page carries on working perfectly
   * from the network, and the only symptom is that the cache is never populated. Defaults to
   * `"classic"`, which is the safer assumption for a hand-written worker.
   */
  readonly serviceWorkerType?: "classic" | "module";
}

/**
 * Build the launcher document.
 *
 * A string rather than a written file: where it goes is the consumer's build's business, and a
 * library that wrote to disk would need to know about their output directory.
 */
/*
 * What the emitted script does, and why — kept here rather than in the document itself, because
 * every byte of that document is on the one request that has no redundancy and no cache.
 *
 * Registration is fire-and-forget: the application starts whether or not the worker installs.
 * Waiting for it would make the cache a prerequisite for starting, which is the opposite of the
 * point.
 *
 * `__race` asks every line at once, with no stagger and no head start for whichever is listed
 * first. Staggering looked frugal and was exactly backwards: a stable-but-slow line answers within
 * any head start you give it, wins by default, and the fast line is never asked. Asking all at once
 * also means a dead tunnel, a blocked route or a network without IPv6 costs nothing — it was asked,
 * it did not answer, nobody waited.
 *
 * The winner is whichever line *finishes delivering*, not whichever answers first. A stable edge
 * can return headers in 20ms and still take seconds to hand over a megabyte; first-byte racing
 * would pick it every time, which is the outcome this library exists to avoid. The bottleneck is
 * the line rather than the client's connection, so the copies do not meaningfully compete, and some
 * duplicated data is a fair price for never being stuck on the slow one.
 *
 * `__import` then falls over between lines. Importing is a *second* request for the same bytes:
 * usually served from the HTTP cache, but that is a convenience rather than a guarantee, and a line
 * that fails one request in three can win the race and then fail the import. It did, in CI, about
 * one run in three.
 *
 * The bytes are never executed from memory. A module built from a blob has the blob as its base
 * URL, so every relative chunk import in a code-split application would resolve to nowhere.
 */
export function buildLauncher(options: LauncherOptions): string {
  const config = {
    appEntry: options.appEntry,
    registry: options.registry ?? null,
    registryUrl: options.registryUrl ?? null,
  };

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(options.title ?? "Loading")}</title>
</head>
<body>
${options.bodyHtml ?? ""}
<script>
// Configuration, inline: the application can choose a line before it has fetched anything, so no
// request has to succeed before requests can be routed.
window.__multipath__ = ${JSON.stringify(config)};
</script>
<script type="module">
${
  options.serviceWorker
    ? `if ("serviceWorker" in navigator) {
navigator.serviceWorker.register(${JSON.stringify(options.serviceWorker)}${registrationOptions(options)}).catch(() => {});
}
`
    : ""
}const __mp = window.__multipath__;
const __lines = (__mp.registry && __mp.registry.lines) || [];
const __pref = ${JSON.stringify(options.preferredLineIds ?? [])};
const __at = (l) => (__pref.indexOf(l.id) === -1 ? __pref.length : __pref.indexOf(l.id));
const __urls = (__pref.length ? [...__lines].sort((a, b) => __at(a) - __at(b)) : __lines).map(
  (l) => (l.url || "") + ${JSON.stringify(options.appEntry)},
);
function __race() {
  if (__urls.length < 2) return Promise.resolve(__urls.length ? __urls : [${JSON.stringify(options.appEntry)}]);
  return new Promise((resolve, reject) => {
    let failed = 0, done = false;
    const cs = __urls.map(() => new AbortController());
    __urls.forEach((url, i) => {
      fetch(url, { signal: cs[i].signal, credentials: url.startsWith("http") ? "include" : "same-origin" })
        .then((r) => { if (!r.ok) throw new Error(r.status); return r.blob(); })
        .then(() => {
          if (done) return;
          done = true;
          cs.forEach((c, j) => j !== i && c.abort());
          resolve([url].concat(__urls.filter((u) => u !== url)));
        })
        .catch(() => { if (!done && ++failed === __urls.length) reject(new Error("no line")); });
    });
  });
}
function __load(urls, i) {
  return import(urls[i]).catch((e) => (i + 1 < urls.length ? __load(urls, i + 1) : Promise.reject(e)));
}
__race()
  .then((urls) => __load(urls, 0))
  .catch((error) => {
    console.error("multipath: could not start the application", error);
    document.body.insertAdjacentHTML("beforeend", '<p data-multipath-error>Could not start. Check your connection and reload.</p>');
  });
</script>
</body>
</html>
`;
}

function registrationOptions(options: LauncherOptions): string {
  const parts: string[] = [];
  if (options.scope) parts.push(`scope: ${JSON.stringify(options.scope)}`);
  if (options.serviceWorkerType) parts.push(`type: ${JSON.stringify(options.serviceWorkerType)}`);
  return parts.length ? `, { ${parts.join(", ")} }` : "";
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
