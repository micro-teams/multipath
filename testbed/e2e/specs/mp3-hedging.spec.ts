/*
 *  Description: MP-3 second half and MP-4 — hedging a read, failing a write over.
 *
 *               The unit tests drive fake attemptors. These drive a real browser against a line
 *               that accepts the connection and then never answers, which is the failure the whole
 *               strategy exists for and the one that is hardest to fake convincingly: the socket is
 *               open, the request is gone, and nothing will ever come back.
 *
 *  Author(s):
 *      agent4
 */

import { expect, test, type Page } from "@playwright/test";

const SERVER = process.env.TESTBED_SERVER_URL ?? "http://localhost:8080";

const LINES = {
  fast: "http://localhost:9001",
  slow: "http://localhost:9002",
  stalled: "http://localhost:9004",
} as const;

async function open(page: Page, ids: Array<keyof typeof LINES>) {
  await page.goto("/");
  await expect(page.locator("#log")).toContainText("ready");
  await page.evaluate(
    (lines) => window.testbed.setRegistry({ lines }),
    ids.map((id) => ({ id, url: LINES[id], transport: "test", weight: 100 })),
  );
}

test.beforeEach(async ({ request }) => {
  await request.post(`${SERVER}/mt/reset`);
});

test.describe("MP-3: a read survives a line that never answers", () => {
  test("the stalled line does not stop the read", async ({ page }) => {
    // Stalled first: the ranking has no measurements yet, so the read genuinely starts on the line
    // that will never answer.
    await open(page, ["stalled", "fast"]);

    const started = Date.now();
    const result = await page.evaluate(() => window.testbed.get("/mt/probe"));

    expect(result.status).toBe(200);
    expect(result.line).toBe("fast");
    // The hedge fires at 150ms; anything near the read timeout would mean it never fired.
    expect(Date.now() - started).toBeLessThan(5000);
  });

  test("a healthy best line is never hedged, so no second request is sent", async ({ page }) => {
    await open(page, ["fast", "slow"]);
    await page.evaluate(() => window.testbed.reset());
    await page.evaluate(() => window.testbed.get("/mt/probe"));

    const attempts = await page.evaluate(() => window.testbed.attempts);
    // The whole reason for hedging rather than fanning out: on a good line the extra request never
    // happens at all.
    expect(attempts).toHaveLength(1);
    expect(attempts[0]!.lineId).toBe("fast");
  });

  test("a slow best line is hedged and the faster answer wins", async ({ page }) => {
    await open(page, ["slow", "fast"]);
    await page.evaluate(() => window.testbed.reset());

    const result = await page.evaluate(() => window.testbed.get("/mt/probe"));
    expect(result.line).toBe("fast");

    const attempts = await page.evaluate(() => window.testbed.attempts);
    expect(attempts.map((a) => a.lineId)).toContain("slow");
    expect(attempts.map((a) => a.lineId)).toContain("fast");
  });
});

test.describe("MP-4: a write fails over, and still takes effect once", () => {
  test("a write starting on a stalled line completes on the next one", async ({ page, request }) => {
    await open(page, ["stalled", "fast"]);

    const result = await page.evaluate(() =>
      window.testbed.post("/mt/echo", { op: "failover" }, { "Idempotency-Key": "fo-1" }),
    );

    expect(result.status).toBe(200);
    expect(result.line).toBe("fast");

    const counted = await (await request.get(`${SERVER}/mt/count?op=failover`)).json();
    // The heart of it: the write moved lines and still happened exactly once.
    expect(counted.count).toBe(1);
  });

  test("a write is never raced — the second line is only used after the first fails", async ({
    page,
    request,
  }) => {
    await open(page, ["fast", "slow"]);
    await page.evaluate(() => window.testbed.reset());

    await page.evaluate(() =>
      window.testbed.post("/mt/echo", { op: "single" }, { "Idempotency-Key": "single-1" }),
    );

    const attempts = await page.evaluate(() => window.testbed.attempts);
    expect(attempts).toHaveLength(1);

    const counted = await (await request.get(`${SERVER}/mt/count?op=single`)).json();
    expect(counted.count).toBe(1);
  });

  test("the same key survives the failover, which is what makes it safe", async ({
    page,
    request,
  }) => {
    await open(page, ["stalled", "fast"]);

    // The stalled line black-holes the request rather than refusing it, so the write may well have
    // arrived at the origin before the client gave up. That is precisely when re-sending is
    // dangerous, and precisely when the key earns its keep.
    await page.evaluate(() =>
      window.testbed.post("/mt/echo", { op: "same-key" }, { "Idempotency-Key": "sk-1" }),
    );

    const counted = await (await request.get(`${SERVER}/mt/count?op=same-key`)).json();
    expect(counted.count).toBe(1);
  });
});
