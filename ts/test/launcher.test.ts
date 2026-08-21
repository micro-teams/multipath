import { describe, expect, it } from "vitest";
import { buildLauncher } from "../src/launcher.js";
import { parseRegistry } from "../src/registry.js";

const registry = parseRegistry({
  lines: [
    { id: "cf", url: "https://cf.example" },
    { id: "ipv6", url: "https://ipv6.example" },
  ],
});

describe("buildLauncher", () => {
  it("loads the application entry point", () => {
    const html = buildLauncher({ appEntry: "/assets/main-abc123.js" });
    expect(html).toContain('"/assets/main-abc123.js"');
    expect(html).toContain("import(urls[i])");
  });

  /**
   * The first visit has no cache, no worker and nothing measured, so the registry's order is only a
   * guess. Racing the entry across the lines is what stops a wrong guess from costing the whole
   * visit — before this, one dead line in the wrong position meant the app simply never appeared.
   */
  it("races the entry point across the lines", () => {
    const html = buildLauncher({ appEntry: "/main.js", registry });
    expect(html).toContain("__race");
    expect(html).toContain("AbortController");
  });

  /**
   * Every line is asked at once, with no head start for whichever is listed first.
   *
   * Staggering looked frugal and was exactly wrong for the case this exists for: a stable-but-slow
   * line answers inside any head start you give it, wins by default, and the genuinely fast line is
   * never even asked.
   */
  it("asks every line simultaneously, with no stagger", () => {
    const html = buildLauncher({ appEntry: "/main.js", registry });
    expect(html).toContain("__urls.forEach");
    expect(html).not.toContain("setTimeout");
  });

  /**
   * The winner is whichever line finishes delivering, not whichever answers first.
   *
   * That distinction is the whole point: a stable edge can return headers in 20ms and still take
   * seconds to hand over a megabyte. Racing on first byte would pick it every time — precisely the
   * outcome this library exists to avoid.
   */
  /**
   * Importing is a second request for the same bytes, and a line that answered once is not promised
   * to answer twice — an intermittently failing tunnel, exactly the kind this library exists to
   * tolerate, can win the race and then fail the import.
   */
  it("falls over to another line if the import itself fails", () => {
    const html = buildLauncher({ appEntry: "/main.js", registry });
    expect(html).toContain("__load(urls, i + 1)");
  });

  it("races to completion, not to first byte", () => {
    const html = buildLauncher({ appEntry: "/main.js", registry });
    expect(html).toContain("r.blob()");
    expect(html).toContain("j !== i && c.abort()");
  });

  it("asks remembered-fastest lines first, since racing cannot tell fast from merely reachable", () => {
    const html = buildLauncher({
      appEntry: "/main.js",
      registry,
      preferredLineIds: ["ipv6", "cf"],
    });
    expect(html).toContain('["ipv6","cf"]');
  });

  /**
   * The bytes are fetched but never executed from memory. A module built from a blob has the blob
   * as its base URL, so every relative chunk import inside a code-split application would resolve
   * to nowhere. Importing from the winner's URL keeps module semantics as the bundler intended.
   */
  it("imports from the winning line rather than executing fetched bytes", () => {
    const html = buildLauncher({ appEntry: "/main.js", registry });
    expect(html).not.toContain("createObjectURL");
    expect(html).not.toContain("new Function");
  });

  it("registers the service worker when given one", () => {
    const html = buildLauncher({ appEntry: "/main.js", serviceWorker: "/sw.js" });
    expect(html).toContain('navigator.serviceWorker.register("/sw.js")');
  });

  /**
   * A worker containing `import` registered as classic fails to parse, and fails quietly: the page
   * still works from the network and the only symptom is a cache that never fills.
   */
  it("can register a module worker", () => {
    const html = buildLauncher({
      appEntry: "/main.js",
      serviceWorker: "/sw.js",
      serviceWorkerType: "module",
    });
    expect(html).toContain('{ type: "module" }');
  });

  it("combines scope and type when both are given", () => {
    const html = buildLauncher({
      appEntry: "/main.js",
      serviceWorker: "/sw.js",
      scope: "/",
      serviceWorkerType: "module",
    });
    expect(html).toContain('{ scope: "/", type: "module" }');
  });

  it("skips registration entirely when not given one", () => {
    const html = buildLauncher({ appEntry: "/main.js" });
    expect(html).not.toContain("serviceWorker");
  });

  /**
   * Fetching the registry would put a network round trip on the one path with no redundancy, and a
   * failure there would leave the app unable to reach any line because it never learned they exist.
   */
  it("inlines the registry rather than making the app fetch it", () => {
    const html = buildLauncher({ appEntry: "/main.js", registry });
    expect(html).toContain("https://cf.example");
    expect(html).toContain("https://ipv6.example");
  });

  it("carries a url to refresh the registry from later", () => {
    const html = buildLauncher({ appEntry: "/main.js", registryUrl: "/mt/lines" });
    expect(html).toContain('"registryUrl":"/mt/lines"');
  });

  /**
   * The launcher must start the app whether or not the worker installs — private windows, disabled
   * workers, a failed install. A launcher that waited for the cache would have made the cache a
   * prerequisite for starting, which is the opposite of the point.
   */
  it("does not make starting depend on the worker installing", () => {
    const html = buildLauncher({ appEntry: "/main.js", serviceWorker: "/sw.js" });
    const registration = html.indexOf(".register(");
    const start = html.indexOf("__race()");
    expect(registration).toBeLessThan(start);
    expect(html).toContain(".catch(");
    // No await between the two: registration is fire-and-forget.
    expect(html.slice(registration, start)).not.toContain("await");
  });

  it("says something when the app cannot be loaded at all", () => {
    // The one failure with nothing behind it. A blank page tells the user nothing.
    expect(buildLauncher({ appEntry: "/main.js" })).toContain("data-multipath-error");
  });

  it("escapes the title rather than letting it close the tag", () => {
    const html = buildLauncher({
      appEntry: "/main.js",
      title: "</title><script>alert(1)</script>",
    });
    expect(html).not.toContain("<script>alert(1)</script>");
    expect(html).toContain("&lt;/title&gt;");
  });

  it("includes any splash markup the consumer supplies", () => {
    const html = buildLauncher({ appEntry: "/main.js", bodyHtml: '<div id="splash">…</div>' });
    expect(html).toContain('<div id="splash">…</div>');
  });

  /**
   * This is the one document that cannot be spread across lines, so its size is the floor on how
   * slow a cold start can be. Worth a test, because size is exactly the property that erodes
   * quietly as options accumulate.
   */
  it("stays small", () => {
    const html = buildLauncher({
      appEntry: "/main.js",
      serviceWorker: "/sw.js",
      registry,
      registryUrl: "/mt/lines",
    });
    // The budget caught this growing to 5793 and forced the right fix rather than a bigger number:
    // the explanatory comments were being shipped inside the document, on the one request that has
    // no redundancy and no cache. They now live in the library source, where they help a reader
    // without costing every visitor.
    expect(html.length).toBeLessThan(3000);
  });

  it("is a complete document", () => {
    const html = buildLauncher({ appEntry: "/main.js" });
    expect(html.startsWith("<!doctype html>")).toBe(true);
    expect(html).toContain('<meta charset="utf-8">');
    expect(html.trimEnd().endsWith("</html>")).toBe(true);
  });
});

// Found with a real browser in front of two lines, behind the `Access-Control-Allow-Origin: *` that
// any sane static file server sets: CORS refuses a credentialed cross-origin request against a
// wildcard, so the race failed and the application started only when the origin it was already on
// happened to win. A build artefact has no session behind it, so the credentials bought nothing and
// cost every consumer an origin-echoing CORS policy on their static files.
describe("the race and credentials", () => {
  it("asks for build artefacts without credentials", () => {
    const html = buildLauncher({
      appEntry: "/app.js",
      registry: {
        lines: [
          { id: "a", url: "https://a.example" },
          { id: "b", url: "https://b.example" },
        ],
      },
    });

    expect(html).toContain('credentials: "omit"');
    expect(html).not.toContain('"include"');
  });

  /**
   * A stub entry point wins the race in milliseconds and then the application spends seconds
   * fetching what it actually runs on. Naming those files here puts them on the line that just
   * proved itself fastest, and makes them the bytes the percentage is a percentage of.
   */
  it("warms the named artefacts on the winning line", () => {
    const html = buildLauncher({
      appEntry: "/flutter_bootstrap.js",
      preload: ["/main.dart.js"],
      registry,
    });
    expect(html).toContain('["/main.dart.js"]');
    expect(html).toContain("__warm(__base(urls[0]))");
  });

  /**
   * The progress machinery is not emitted at all for a launcher that preloads nothing. Every byte
   * of this document rides the one request with no redundancy and no cache, so a feature nobody
   * asked for must not be paid for by everybody.
   */
  it("ships no progress code when nothing was named to preload", () => {
    const html = buildLauncher({ appEntry: "/main.js", registry });
    expect(html).not.toContain("data-multipath-progress");
    expect(html).not.toContain("getReader");
    expect(html).toContain("function __warm()");
  });

  /**
   * Streamed, not awaited whole: a response consumed with .blob() reports nothing until it is
   * finished, which is precisely when a progress report has stopped being useful.
   */
  it("counts bytes as they arrive rather than when a file finishes", () => {
    const html = buildLauncher({ appEntry: "/main.js", preload: ["/big.js"] });
    expect(html).toContain("getReader()");
    expect(html).toContain("reader.read()");
  });

  /**
   * 100% and then a wait reads as a hang. The last percent belongs to the moment the application's
   * module has actually been imported and there is something to look at.
   */
  it("holds the last percent back until the app has been imported", () => {
    const html = buildLauncher({ appEntry: "/main.js", preload: ["/big.js"] });
    expect(html).toContain("Math.min(99");
    expect(html).toMatch(/__load\(urls, 0\)\)\)\s*\n\s*\.then\(\(\) => __say\(100\)\)/);
  });

  /** Two ways to read it, because a splash screen may be markup or may be a canvas. */
  it("reports progress to the document and to a listener", () => {
    const html = buildLauncher({ appEntry: "/main.js", preload: ["/big.js"] });
    expect(html).toContain("[data-multipath-progress]");
    expect(html).toContain('CustomEvent("multipath:progress"');
  });

  /**
   * A file served without Content-Length must not move the bar rather than making it lie: the
   * total it would have contributed is unknown, so its bytes are counted and its size is not.
   */
  it("only counts a file in the total when its length was given", () => {
    const html = buildLauncher({ appEntry: "/main.js", preload: ["/big.js"] });
    expect(html).toContain("if (length > 0) __want += length");
  });
});
