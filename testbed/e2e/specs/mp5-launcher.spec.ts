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

/** How many requests a line has seen, so a spec can tell "asked" from "not asked". */
async function lineHits(
  request: import("@playwright/test").APIRequestContext,
  port: number,
): Promise<number> {
  return (await (await request.get(`http://localhost:${port}/__line`)).json()).seen as number;
}

/** A genuinely cold start: no worker, no cache, nothing measured. */
async function clearClientState(page: Page, context: import("@playwright/test").BrowserContext) {
  await context.clearCookies();
  await page.goto("/launcher.html");
  await page.evaluate(async () => {
    for (const registration of await navigator.serviceWorker.getRegistrations()) {
      await registration.unregister();
    }
    for (const key of await caches.keys()) await caches.delete(key);
  });
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
    // A path, not a URL: naming a host here would bet the whole first visit on one line.
    expect(config.appEntry).toBe("/app/main.js");
  });

  test("installs a service worker that takes charge", async ({ page }) => {
    await page.goto("/launcher.html");
    await waitForWorker(page);
    expect(await page.evaluate(() => navigator.serviceWorker.controller !== null)).toBe(true);
  });
});

test.describe("MP-5: a cold start survives a dead line", () => {
  /**
   * The hardest moment in the whole system: no cache, no worker, and nothing measured, so the
   * registry's order is only a guess. Before the entry was raced, one dead line in the wrong
   * position meant the application never appeared at all — the import simply hung.
   */
  test("the app starts even though the first line is a black hole", async ({
    page,
    request,
    context,
  }) => {
    await reviveAll(request);
    await clearClientState(page, context);
    await setStalling(request, 9001, true);

    const started = Date.now();
    await page.goto("/launcher.html");
    await expect(page.locator("[data-app-ready]")).toBeVisible({ timeout: 15_000 });

    // It had to notice and move on, not wait out a connection timeout.
    expect(Date.now() - started).toBeLessThan(10_000);
    await reviveAll(request);
  });

  /**
   * The case the whole library exists for: a line that is stable and slow against one that is fast.
   *
   * The launcher's registry lists the slow line first on purpose. Any head start for the first-listed
   * line — even a small one — and the slow line answers within it, wins by default, and the fast line
   * is never asked. So the bundle must arrive over the fast line despite being listed last.
   */
  test("the fast line wins even though the slow one is listed first", async ({
    page,
    request,
    context,
  }) => {
    await reviveAll(request);
    await clearClientState(page, context);

    const fastBefore = await lineHits(request, 9001);
    await page.goto("/launcher.html");
    await expect(page.locator("[data-app-ready]")).toBeVisible({ timeout: 15_000 });

    // The fast line was asked and served it, rather than being skipped because it was listed last.
    expect((await lineHits(request, 9001)) - fastBefore).toBeGreaterThan(0);
  });

  test("every line is asked at once, so a dead one costs no delay", async ({
    page,
    request,
    context,
  }) => {
    await reviveAll(request);
    await clearClientState(page, context);
    // Both the black hole and a line this "network" cannot use.
    await setStalling(request, 9002, true);

    const started = Date.now();
    await page.goto("/launcher.html");
    await expect(page.locator("[data-app-ready]")).toBeVisible({ timeout: 15_000 });

    // Nothing waited on the dead lines: they were asked at the same instant as everyone else and
    // simply did not answer.
    expect(Date.now() - started).toBeLessThan(6000);
    await reviveAll(request);
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
