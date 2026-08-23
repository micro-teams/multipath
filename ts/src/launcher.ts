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

/**
 * One artefact to fetch before the entry point, and how big it is.
 *
 * The size comes from the build, and it is the file's own size rather than what the wire carries.
 * That distinction is the whole reason this exists: a compressed response's `Content-Length` counts
 * COMPRESSED bytes while a stream reader hands over DECOMPRESSED ones, so a bar that trusted the
 * header was comparing two different units and reached 99% after the first few chunks. A build
 * knows the real number; nothing at runtime does.
 */
export interface PreloadFile {
  readonly url: string;
  /** Uncompressed size in bytes. Omit only if the build genuinely does not know. */
  readonly bytes?: number;
  /**
   * A JavaScript expression, evaluated in the page: the file is fetched only if it is truthy.
   *
   * For builds that ship alternatives and choose between them at runtime — a wasm engine with a
   * variant per browser being the case this exists for. Preloading both would waste megabytes on
   * every visit; preloading the wrong one wastes them AND leaves the real one to be fetched
   * afterwards, outside the progress bar. The condition belongs to whoever knows how the choice is
   * made, which is the build, not this library.
   *
   * It is emitted into the launcher as written, so it must be an expression the build itself
   * authored — never anything derived from user input.
   */
  readonly when?: string;
}

export type PreloadEntry = string | PreloadFile;

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
  /**
   * Artefacts to fetch on the winning line before the entry point is imported, largest first.
   *
   * The entry point of a modern build is often a stub: it is small, it wins the race in
   * milliseconds, and then the application spends seconds fetching the megabytes it actually runs
   * on — a Flutter engine, a wasm module, a chunked bundle. Naming those here does two things at
   * once. They arrive on the line that just proved itself fastest, warm in the HTTP cache by the
   * time the application asks; and their bytes are what the progress report is a percentage OF,
   * which is the only way a percentage can mean anything. A percentage of the stub alone would go
   * 0 to 100 and then sit there while the real download happened.
   */
  readonly preload?: readonly PreloadEntry[];
  readonly title?: string;
  /**
   * Extra markup inside the head.
   *
   * Chiefly for `<base>`, which has to be in the head and has to be right: a framework that
   * resolves its own assets against the document's base URL loads them from the wrong place on
   * every deep link without it — the app opens at "/" and nowhere else, which is the one failure a
   * launcher must not introduce.
   */
  readonly headHtml?: string;
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
  /**
   * This build's version, and where to ask what the server has.
   *
   * The one question a cached client cannot answer for itself: "am I the build that is deployed?"
   * Everything else it holds — the document, the code, the engine — may be its own stale copy, and
   * a copy has no way to notice that it is one. So the version travels INSIDE the launcher, and the
   * launcher asks the server for the current one on every start.
   *
   * When they disagree, everything cached under this origin is from the older build: caches, the
   * request cache in local storage, and the worker holding them. All of it goes, and the page
   * reloads once into the new build. That is blunt on purpose — a half-updated client is the state
   * that produces the failures nobody can reproduce.
   */
  readonly version?: string;
  /** Where the deployed version is served, unfudged and uncached. Requires [version]. */
  readonly versionUrl?: string;
  /**
   * Local-storage keys to drop when the version changed, matched by prefix.
   *
   * The consumer's own caches, which this library cannot recognise: a framework's persistence
   * layer usually prefixes what it writes, and a build change is precisely when a remembered
   * response may no longer mean what it says.
   */
  readonly clearOnUpdate?: readonly string[];
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
 * The race sends no credentials, and that is not a detail. A build artefact has no session behind
 * it, so there is nothing for a cookie to do here — but asking for one costs a great deal: CORS
 * forbids `Access-Control-Allow-Origin: *` on a credentialed request, so every consumer would have
 * had to echo each line's origin back from its static file server, with `Allow-Credentials: true`,
 * to serve files that are identical for everybody. It was found by pointing a real browser at two
 * lines behind a plain `*` — the testbed's own proxies echo the origin, so they never showed it.
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
 * The preloaded files are asked for all at once, on the one line that has already proved itself, so
 * the total is known early and the bar moves smoothly rather than in one jump per file. Each is
 * streamed rather than consumed with `.blob()`, which reports nothing until it is finished — which
 * is the moment a progress report stops being useful.
 *
 * Progress is reported over the PRELOADED bytes, not the entry point's, and is deliberately
 * capped below 100 until the application's module has actually been imported. A bar that reaches
 * 100% and then waits is read as a hang; the last percent is worth keeping for the moment there is
 * something on screen.
 *
 * Content-Length is trusted where it is given and the file is skipped in the total where it is not
 * — a compressed response reports the compressed length, which is the number of bytes actually
 * coming down the line, and is exactly right for this. Where it is absent (chunked, or a proxy that
 * strips it) that file simply does not move the bar rather than the bar becoming a lie.
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
${options.headHtml ?? ""}
</head>
<body>
${options.bodyHtml ?? ""}
<script>
// Configuration, inline: the application can choose a line before it has fetched anything, so no
// request has to succeed before requests can be routed.
window.__multipath__ = ${JSON.stringify(config)};
</script>
<script type="module">
${versionGuard(options)}${
  options.serviceWorker
    ? `if ("serviceWorker" in navigator) {
navigator.serviceWorker.register(${JSON.stringify(options.serviceWorker)}${registrationOptions(options)}).catch(() => {});
}
`
    : ""
}const __entry = ${JSON.stringify(options.appEntry)};
const __mp = window.__multipath__;
const __lines = (__mp.registry && __mp.registry.lines) || [];
const __pref = ${JSON.stringify(options.preferredLineIds ?? [])};
const __at = (l) => (__pref.indexOf(l.id) === -1 ? __pref.length : __pref.indexOf(l.id));
const __urls = (__pref.length ? [...__lines].sort((a, b) => __at(a) - __at(b)) : __lines).map(
  (l) => (l.url || "") + __entry,
);
const __base = (url) => url.slice(0, url.length - __entry.length);
function __race() {
  if (__urls.length < 2) return Promise.resolve(__urls.length ? __urls : [__entry]);
  return new Promise((resolve, reject) => {
    let failed = 0, done = false;
    const cs = __urls.map(() => new AbortController());
    __urls.forEach((url, i) => {
      fetch(url, { signal: cs[i].signal, credentials: "omit" })
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
${
  (options.preload ?? []).length
    ? `const __pre = ${JSON.stringify(
        (options.preload ?? []).map((entry) =>
          typeof entry === "string"
            ? { url: entry }
            : { url: entry.url, bytes: entry.bytes, when: entry.when },
        ),
      )};
// Known up front, from the build, so the first byte already moves a bar that means something. Only
// the files this browser will actually ask for: a build that ships alternatives says which is
// which, and preloading the other one spends megabytes warming a cache nobody reads.
const __want_files = __pre.filter((f) => !f.when || __cond(f.when));
let __got = 0, __want = __want_files.reduce((sum, f) => sum + (f.bytes || 0), 0);
function __cond(expression) {
  try { return !!eval(expression); } catch (e) { return false; }
}
function __say(p) {
  document.querySelectorAll("[data-multipath-progress]").forEach((e) => { e.textContent = p + "%"; });
  window.dispatchEvent(new CustomEvent("multipath:progress", { detail: { percent: p, loaded: __got, total: __want } }));
}
function __drain(r, known) {
  // Content-Length only when the build did not say and the response is not compressed: the header
  // counts wire bytes, and what a reader hands over is decoded ones. Mixing the two makes a bar
  // that leaps to 99 and then crawls.
  if (!known && !r.headers.get("content-encoding")) {
    const length = Number(r.headers.get("content-length"));
    if (length > 0) __want += length;
  }
  if (!r.body || !r.body.getReader) return r.blob().then(() => {});
  const reader = r.body.getReader();
  return (function pump() {
    return reader.read().then((c) => {
      if (c.done) return;
      __got += c.value.length;
      if (__want > 0) __say(Math.min(99, Math.floor((__got / __want) * 100)));
      return pump();
    });
  })();
}
function __warm(base) {
  if (!__pre.length) return Promise.resolve();
  __say(0);
  return Promise.all(
    __want_files.map((f) =>
      fetch(base + f.url, { credentials: "omit" })
        .then((r) => (r.ok ? __drain(r, !!f.bytes) : 0))
        .catch(() => {}),
    ),
  ).then(() => {});
}`
    : "function __warm() { return Promise.resolve(); }\nfunction __say() {}\n"
}
function __load(urls, i) {
  return import(urls[i]).catch((e) => (i + 1 < urls.length ? __load(urls, i + 1) : Promise.reject(e)));
}
__race()
  .then((urls) => __warm(__base(urls[0])).then(() => __load(urls, 0)))
  .then(() => __say(100))
  .catch((error) => {
    console.error("multipath: could not start the application", error);
    document.body.insertAdjacentHTML("beforeend", '<p data-multipath-error>Could not start. Check your connection and reload.</p>');
  });
</script>
</body>
</html>
`;
}

/**
 * The version check, emitted before anything else runs.
 *
 * It is first because everything after it is a decision made with cached material: which worker
 * answers, what it answers with, and what the application believes it already knows. Asking
 * afterwards would mean acting on the old build and correcting later, which is the half-updated
 * state this exists to prevent.
 *
 * There are two ways to be out of date and they need different questions. What is CACHED here may
 * belong to an older build — asked locally, by remembering which version filled these caches, and
 * it is the case that matters most because a fresh document with a stale engine does not start.
 * And this DOCUMENT may itself be an old copy served while a newer build is deployed — which only
 * the server can answer, on the one request of a page load that is never answered from a cache.
 *
 * A failed check is silence, not an error: offline is ordinary, and a client that refused to start
 * because it could not confirm its version would be broken far more often than a stale one is.
 *
 * The reload is guarded per tab, so a disagreement that somehow never resolves cannot become a
 * loop — one attempt, then it carries on with what it has.
 */
function versionGuard(options: LauncherOptions): string {
  if (!options.version) return "";
  const prefixes = options.clearOnUpdate ?? [];
  const askServer = options.versionUrl
    ? `    if (!stale) {
      // The other way to be out of date: this document is itself a cached copy, served while a
      // newer build sits on the server. Only the server can answer that, and this is the one
      // request in a page load that is never answered from a cache.
      try {
        const response = await fetch(${JSON.stringify(options.versionUrl)}, { cache: "no-store" });
        if (response.ok) {
          const deployed = (await response.text()).trim();
          if (deployed && deployed !== __version) stale = deployed;
        }
      } catch (e) {
        // Offline. Carrying on with what we have is exactly right.
      }
    }
`
    : "";
  return `const __version = ${JSON.stringify(options.version)};
await (async function () {
  const KEY = "multipath:version";
  try {
    // What the caches on this machine were filled for. A build change makes every one of them a
    // copy of something that no longer exists — and mixing them with new code is the failure this
    // guard exists to prevent: new application code running against the previous build's engine
    // does not start, and nothing on screen says why.
    let stale = null;
    const held = localStorage.getItem(KEY);
    if (held && held !== __version) stale = held;
${askServer}    if (!stale) {
      localStorage.setItem(KEY, __version);
      return;
    }
    if (sessionStorage.getItem("multipath:updating") === __version + ">" + stale) return;
    sessionStorage.setItem("multipath:updating", __version + ">" + stale);
    console.warn("multipath: " + __version + " meeting " + stale + " — starting over");

    if (window.caches) {
      const names = await caches.keys();
      await Promise.all(names.map((name) => caches.delete(name)));
    }
    const prefixes = ${JSON.stringify(prefixes)};
    for (const key of Object.keys(localStorage)) {
      if (prefixes.some((p) => key.startsWith(p))) localStorage.removeItem(key);
    }
    localStorage.setItem(KEY, __version);
    // The worker included: it is code from the build being replaced, and it is what would answer
    // the reload out of its own memory.
    if (navigator.serviceWorker) {
      const registrations = await navigator.serviceWorker.getRegistrations();
      await Promise.all(registrations.map((r) => r.unregister()));
    }
    location.reload();
    await new Promise(() => {});
  } catch (e) {
    // A guard that throws would stop the application starting, which is strictly worse than the
    // staleness it is guarding against.
  }
})();
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
