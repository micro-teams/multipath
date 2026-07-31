#!/usr/bin/env node
/*
 *  Description: Generates the testbed's launcher and Service Worker from the built package.
 *
 *               This is the consumer's build step, standing in for whatever a real application
 *               would do: decide the manifest, pick a version, and emit the two files. Doing it
 *               here rather than committing the output means the specs always run against what the
 *               library currently produces, not against a snapshot of what it produced once.
 *
 *  Author(s):
 *      agent4
 */

import { writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildLauncher } from "../../ts/dist/launcher.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));

/** The lines run.sh starts, as the launcher will carry them inline. */
const registry = {
  lines: JSON.parse(process.env.TESTBED_REGISTRY_JSON ?? "[]"),
};

// The "application" is served by the origin, so it reaches the browser over the lines exactly as a
// real build artefact would.
//
// The manifest entry is the origin-relative path — that is the key the cache is filled under. The
// launcher, though, has to import an absolute URL on a line, because on a *cold* start no worker
// exists yet to route a relative one. That asymmetry is the honest shape of the problem: the very
// first load of the bundle has to name a host, and only afterwards does the cache make the host
// irrelevant.
const APP_PATH = "/app/main.js";
const firstLine = registry.lines[0];
const APP_ENTRY = firstLine ? firstLine.url + APP_PATH : APP_PATH;

await writeFile(
  path.join(HERE, "launcher.html"),
  buildLauncher({
    appEntry: APP_ENTRY,
    serviceWorker: "/sw.js",
    // The worker below imports the library, so it has to be registered as a module.
    serviceWorkerType: "module",
    registry,
    registryUrl: "/mt/lines",
    title: "MultiPath testbed launcher",
    bodyHtml: '<p data-launcher-ready>launcher running</p>',
  }),
  "utf8",
);

// The worker the consumer writes: import the runtime, hand it the manifest, wire the events. Kept
// deliberately thin — everything interesting is in the library, which is what is under test.
await writeFile(
  path.join(HERE, "sw.js"),
  `import { createPrecache } from "./vendor/serviceWorker.js";

const precache = createPrecache({
  manifest: ${JSON.stringify([APP_PATH])},
  version: ${JSON.stringify(process.env.TESTBED_BUILD_VERSION ?? "test")},
  registry: ${JSON.stringify(registry)},
});

self.addEventListener("install", (event) => {
  // Take over immediately rather than waiting for every tab to close: a testbed that needed a
  // manual reload dance would be testing the dance.
  event.waitUntil(precache.install().then(() => self.skipWaiting()));
});

self.addEventListener("activate", (event) => {
  event.waitUntil(precache.activate().then(() => self.clients.claim()));
});

self.addEventListener("fetch", (event) => {
  event.respondWith(
    precache.handle(event.request).then((response) => response ?? fetch(event.request)),
  );
});
`,
  "utf8",
);

console.log("launcher.html and sw.js written");
