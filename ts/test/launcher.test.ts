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
    expect(html).toContain('import("/assets/main-abc123.js")');
  });

  it("registers the service worker when given one", () => {
    const html = buildLauncher({ appEntry: "/main.js", serviceWorker: "/sw.js" });
    expect(html).toContain('navigator.serviceWorker\n    .register("/sw.js")');
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
    const start = html.indexOf('import("/main.js")');
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
    expect(html.length).toBeLessThan(3000);
  });

  it("is a complete document", () => {
    const html = buildLauncher({ appEntry: "/main.js" });
    expect(html.startsWith("<!doctype html>")).toBe(true);
    expect(html).toContain('<meta charset="utf-8">');
    expect(html.trimEnd().endsWith("</html>")).toBe(true);
  });
});
