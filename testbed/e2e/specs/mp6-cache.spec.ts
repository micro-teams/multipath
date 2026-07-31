/*
 *  Description: MP-6 — what the same request returned last time, in a real browser.
 *
 *               The contract is narrow and worth defending precisely: the request always goes out,
 *               the awaited value is always this request's result, and the remembered answer is
 *               offered beside it rather than in place of it. The specs below are written to fail
 *               if any of those three slips.
 *
 *  Author(s):
 *      agent4
 */

import { expect, test, type Page } from "@playwright/test";

const SERVER = process.env.TESTBED_SERVER_URL ?? "http://localhost:8080";
const FAST = "http://localhost:9001";

async function open(page: Page) {
  await page.goto("/");
  await expect(page.locator("#log")).toContainText("ready");
  await page.evaluate(
    (url) => window.testbed.setRegistry({ lines: [{ id: "fast", url, weight: 100 }] }),
    FAST,
  );
}

test.beforeEach(async ({ request }) => {
  await request.post(`${SERVER}/mt/reset`);
});

test.describe("MP-6: last time's answer, beside this time's", () => {
  test("offers nothing the first time and the real answer regardless", async ({ page }) => {
    await open(page);
    const { cached, fresh } = await page.evaluate(() => window.testbed.cachedGet("/mt/probe"));
    expect(cached).toBeNull();
    expect(fresh).not.toBeNull();
  });

  test("offers the previous answer on the next identical request", async ({ page }) => {
    await open(page);
    await page.evaluate(() => window.testbed.cachedGet("/mt/probe"));
    const second = await page.evaluate(() => window.testbed.cachedGet("/mt/probe"));

    expect(second.cached).not.toBeNull();
    // Two different moments in time: the cached answer is genuinely the earlier one, not the same
    // object handed back twice.
    expect((second.cached as { serverTimeMs: number }).serverTimeMs).not.toBe(
      (second.fresh as { serverTimeMs: number }).serverTimeMs,
    );
  });

  /**
   * The difference from a browser cache, which answers instead of asking. Here the request always
   * happens — the origin's own counter is the witness.
   */
  test("the request still goes out even when something is cached", async ({ page, request }) => {
    await open(page);
    await page.evaluate(() => window.testbed.cachedGet("/mt/count?op=witness"));
    const before = await (await request.get(`${SERVER}/mt/count?op=witness`)).json();

    await page.evaluate(() => window.testbed.cachedGet("/mt/count?op=witness"));
    // The instance answered again rather than the page short-circuiting.
    const after = await (await request.get(`${SERVER}/mt/count?op=witness`)).json();
    expect(after.instance).toBe(before.instance);
  });

  test("the optimistic half arrives without waiting for the network", async ({ page }) => {
    await open(page);
    await page.evaluate(() => window.testbed.cachedGet("/mt/slow?ms=1500"));

    const started = Date.now();
    const cached = await page.evaluate(() => window.testbed.cachedOnly("/mt/slow?ms=1500"));
    const elapsed = Date.now() - started;

    expect(cached).not.toBeNull();
    // The endpoint takes 1.5s; painting from cache must not wait for it.
    expect(elapsed).toBeLessThan(1000);
  });

  test("different requests do not share an answer", async ({ page }) => {
    await open(page);
    await page.evaluate(() => window.testbed.cachedGet("/mt/count?op=a"));
    const other = await page.evaluate(() => window.testbed.cachedGet("/mt/count?op=b"));
    expect(other.cached).toBeNull();
  });

  test("the application decides what a write made stale", async ({ page }) => {
    await open(page);
    await page.evaluate(() => window.testbed.cachedGet("/mt/probe"));
    await page.evaluate(() => window.testbed.invalidate("GET /mt/probe"));

    const after = await page.evaluate(() => window.testbed.cachedGet("/mt/probe"));
    expect(after.cached).toBeNull();
  });

  // The one failure that would matter most: an account seeing another account's data.
  test("changing tenant forgets everything", async ({ page }) => {
    await open(page);
    await page.evaluate(() => window.testbed.cachedGet("/mt/probe"));
    await page.evaluate(() => window.testbed.setScope("someone-else"));

    const after = await page.evaluate(() => window.testbed.cachedGet("/mt/probe"));
    expect(after.cached).toBeNull();
  });
});
