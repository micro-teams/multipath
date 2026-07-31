/*
 *  Description: MP-5 — the launcher, and starting the app with the lines dead.
 *
 *               The claim under test is the design's strongest and least verifiable-by-inspection:
 *               once the cache is warm, starting the application involves no network at all, so it
 *               does not matter which lines are alive.
 *
 *               So the specs stage exactly that. The application bundle is served by the origin and
 *               reaches the browser over a line, like any real build artefact. One spec loads the
 *               app normally, then black-holes every line it could possibly come from, reloads, and
 *               asserts the app still starts. If that passes, "as long as there is a cache, it
 *               starts" is a fact this suite re-establishes on every run rather than a sentence in
 *               a design document.
 *
 *  Author(s):
 *      agent4
 */

import { expect, test, type Page } from "@playwright/test";

const LINE_PORTS = [9001, 9002, 9003, 9004];

/** Black-hole or revive a line while the browser keeps running. */
async function setStalling(
  request: import("@playwright/test").APIRequestContext,
  port: number,
  stalling: boolean,
) {
  await request.get(`http://localhost:${port}/__line/${stalling ? "stall" : "revive"}`);
}

async function reviveAll(request: import("@playwright/test").APIRequestContext) {
  for (const port of LINE_PORTS) await setStalling(request, port, port === 9004);
}

/** Wait for the worker to be in charge, since a hot cache is the precondition for every check. */
async function waitForWorker(page: Page) {
  await page.waitForFunction(() => navigator.serviceWorker.controller !== null, null, {
    timeout: 15_000,
  });
}

test.describe.configure({ mode: "serial" });

test.describe("MP-5: the launcher starts the application", () => {
  test.beforeEach(async ({ request }) => {
    await reviveAll(request);
  });

  test("loads the app through the launcher", async ({ page }) => {
    await page.goto("/launcher.html");
    await expect(page.locator("[data-launcher-ready]")).toBeVisible();
    // The app itself came from the origin over a line, not from the page's own server.
    await expect(page.locator("[data-app-ready]")).toBeVisible({ timeout: 15_000 });
  });

  test("carries the registry inline, so routing works before anything is fetched", async ({
    page,
  }) => {
    await page.goto("/launcher.html");
    const config = await page.evaluate(() => window.__multipath__);
    expect(config.registry?.lines.length).toBeGreaterThan(0);
    // Absolute, on a line: a cold start has no worker yet, so the first load of the bundle has to
    // name a host. Everything after that comes from the cache and the host stops mattering.
    expect(config.appEntry).toContain("/app/main.js");
    expect(config.appEntry.startsWith("http://")).toBe(true);
  });

  test("installs a service worker that takes charge", async ({ page }) => {
    await page.goto("/launcher.html");
    await waitForWorker(page);
    expect(await page.evaluate(() => navigator.serviceWorker.controller !== null)).toBe(true);
  });
});

test.describe("MP-5: a warm cache makes the lines irrelevant", () => {
  test("the app still starts after every line has been black-holed", async ({ page, request }) => {
    await reviveAll(request);

    // First visit: the worker installs and precaches the bundle.
    await page.goto("/launcher.html");
    await expect(page.locator("[data-app-ready]")).toBeVisible({ timeout: 15_000 });
    await waitForWorker(page);

    // Now take out every route the bundle could possibly come from. Not a restart: the processes
    // stay up and simply stop answering, which is what a black-holed route looks like.
    for (const port of LINE_PORTS) await setStalling(request, port, true);

    await page.reload();

    // No line can answer, so this can only have come from the cache.
    await expect(page.locator("[data-app-ready]")).toBeVisible({ timeout: 15_000 });
    expect(await page.evaluate(() => window.__app_started__)).toBe(true);

    await reviveAll(request);
  });

  test("and starts quickly, because it is not waiting on the network at all", async ({
    page,
    request,
  }) => {
    await reviveAll(request);
    await page.goto("/launcher.html");
    await expect(page.locator("[data-app-ready]")).toBeVisible({ timeout: 15_000 });
    await waitForWorker(page);

    for (const port of LINE_PORTS) await setStalling(request, port, true);

    const started = Date.now();
    await page.reload();
    await expect(page.locator("[data-app-ready]")).toBeVisible({ timeout: 15_000 });
    const elapsed = Date.now() - started;

    // Generous, because this measures a browser doing a full reload. The point is only that it did
    // not sit waiting for a dead line to time out.
    expect(elapsed).toBeLessThan(8000);

    await reviveAll(request);
  });
});
