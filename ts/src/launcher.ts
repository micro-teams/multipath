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
  /** The application's real entry point — an ES module URL. */
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
}import(${JSON.stringify(options.appEntry)}).catch((error) => {
  // The one failure with nothing behind it: the entry point itself could not be loaded from any
  // line and is not in the cache. Say so plainly rather than leaving a blank page.
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

/** The second argument to `register`, or nothing at all when neither option was set. */
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
