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
    ? `// Registration is fire-and-forget. The application must start whether or not the worker
// installs — a browser with workers disabled, a private window, an install that fails — because a
// launcher that waits for the cache to be ready has made the cache a dependency of starting, which
// is the opposite of the point.
if ("serviceWorker" in navigator) {
  navigator.serviceWorker
    .register(${JSON.stringify(options.serviceWorker)}${registrationOptions(options)})
    .catch((error) => console.warn("multipath: service worker registration failed", error));
}
`
    : ""
}
// Cold start: no cache, no worker, and nothing measured, so the registry's order is only a guess.
// Racing the entry point over the lines is what stops a wrong guess from costing the whole visit —
// without it, one dead line in the wrong position means the app simply never appears.
//
// It races to find a line that can *serve the bundle*, then imports normally from that line. The
// bytes are not executed from memory: a module built from a blob has the blob as its base URL, so
// every relative chunk import inside a code-split application would resolve to nowhere. Importing
// from the winner's URL keeps module semantics exactly as the bundler intended, and the browser's
// HTTP cache normally satisfies the second request from the first.
const __mp = window.__multipath__;
const __lines = (__mp.registry && __mp.registry.lines) || [];
const __entry = ${JSON.stringify(options.appEntry)};
const __preferred = ${JSON.stringify(options.preferredLineIds ?? [])};

// Every line is asked at once, with no stagger and no head start for whatever happens to be listed
// first. Staggering looked frugal and was exactly wrong for the case this library exists for: the
// stable-but-slow line answers within any head start you give it, wins by default, and the fast
// line is never even asked. A line that is dead, blocked, or unsupported by this network therefore
// costs nothing at all — it was asked, it did not answer, and nobody waited for it.
//
// The winner is whichever line *finishes delivering* first, not whichever answers first. That
// distinction is the entire point: a stable CDN edge can return headers in 20ms and still take
// seconds to hand over a megabyte, and picking on first byte would choose it every time — the very
// outcome this library exists to avoid. The bottleneck is the line, not the user's connection, so
// the copies do not meaningfully compete; the cost is some extra data, which is worth paying to
// never be stuck on the slow one.
function __race(path) {
  const ordered = __preferred.length
    ? [...__lines].sort((a, b) => rank(a) - rank(b))
    : __lines;
  function rank(line) {
    const at = __preferred.indexOf(line.id);
    return at === -1 ? __preferred.length : at;
  }
  if (ordered.length === 0) return Promise.resolve([path]);
  if (ordered.length === 1) return Promise.resolve([(ordered[0].url || "") + path]);

  return new Promise((resolve, reject) => {
    let failed = 0;
    let done = false;
    const controllers = [];
    ordered.forEach((line) => {
      const url = (line.url || "") + path;
      const controller = new AbortController();
      controllers.push(controller);
      fetch(url, {
        signal: controller.signal,
        credentials: line.url ? "include" : "same-origin",
      })
        .then((response) => {
          if (!response.ok) throw new Error(String(response.status));
          // Drain it fully before claiming victory. Finishing is the thing being raced, and the
          // bytes land in the HTTP cache so the import below does not fetch them again.
          return response.blob();
        })
        .then(() => {
          if (done) return;
          done = true;
          controllers.forEach((c) => c !== controller && c.abort());
          // Everything else, winner first: the import that follows is a second request, and a line
          // that answered once is not promised to answer twice.
          resolve([url].concat(ordered.filter((l) => (l.url || "") + path !== url).map((l) => (l.url || "") + path)));
        })
        .catch(() => {
          if (done) return;
          if (++failed === ordered.length) reject(new Error("no line could serve " + path));
        });
    });
  });
}

// Importing is a *second* request for the same bytes. In production the first one has usually left
// them in the HTTP cache, but that is a convenience and not a guarantee — and an intermittently
// failing line, which is precisely the kind of cheap tunnel this library is meant to tolerate, can
// win the race and then fail the import. So the import falls over to the next line rather than
// treating one bad response as the end of the visit.
function __import(urls, at) {
  return import(urls[at]).catch((error) => {
    if (at + 1 >= urls.length) throw error;
    return __import(urls, at + 1);
  });
}

__race(__entry)
  .then((urls) => __import(urls, 0))
  .catch((error) => {
  // The one failure with nothing behind it: the entry point could not be loaded from any line and
  // is not in the cache. Say so plainly rather than leaving a blank page.
  console.error("multipath: could not start the application", error);
  document.body.insertAdjacentHTML(
    "beforeend",
    '<p data-multipath-error>Could not start. Check your connection and reload.</p>',
  );
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
