/*
 *  Description: MP-2 end to end — the same write, over two real lines, from a real browser.
 *
 *               The JVM unit tests already prove the filter under a thread race. This proves the
 *               thing those cannot: that the property survives the whole stack — a browser issuing
 *               two requests, two separate proxies, two sockets, arriving at one origin.
 *
 *  Author(s):
 *      agent4
 */

import { expect, test, type Page } from "@playwright/test";

const SERVER = process.env.TESTBED_SERVER_URL ?? "http://localhost:8080";

const LINES = {
  fast: "http://localhost:9001",
  slow: "http://localhost:9002",
} as const;

async function open(page: Page) {
  await page.goto("/");
  await expect(page.locator("#log")).toContainText("ready");
  await page.evaluate(
    (lines) => window.testbed.setRegistry({ lines }),
    Object.entries(LINES).map(([id, url]) => ({ id, url, transport: "test", weight: 100 })),
  );
}

async function countOf(request: import("@playwright/test").APIRequestContext, op: string) {
  return (await (await request.get(`${SERVER}/mt/count?op=${op}`)).json()).count as number;
}

test.beforeEach(async ({ request }) => {
  await request.post(`${SERVER}/mt/reset`);
});

test.describe("MP-2: a write that arrives twice takes effect once", () => {
  test("the same key over two lines at once executes once", async ({ page, request }) => {
    await open(page);
    const responses = await page.evaluate(() =>
      window.testbed.postOverAllLines(
        "/mt/echo",
        { op: "once" },
        { "Idempotency-Key": "e2e-key-1" },
      ),
    );

    expect(await countOf(request, "once")).toBe(1);

    // Both callers were answered, and answered the same thing — the duplicate was replayed, not
    // rejected. A client told "duplicate" would believe its write had failed.
    expect(responses).toHaveLength(2);
    responses.forEach((r) => expect(r.status).toBe(200));
    expect(new Set(responses.map((r) => JSON.stringify(r.body))).size).toBe(1);
  });

  test("the second line's answer arrives even though it never reached the handler", async ({
    page,
    request,
  }) => {
    await open(page);
    // The slow line adds 400ms, so its request lands well after the fast one has finished: this is
    // the sequential replay path rather than the race, over real network paths.
    const [fast, slow] = await page.evaluate(() =>
      window.testbed.postOverAllLines(
        "/mt/echo",
        { op: "sequential" },
        { "Idempotency-Key": "e2e-key-2" },
      ),
    );

    expect(await countOf(request, "sequential")).toBe(1);
    expect(slow!.body).toEqual(fast!.body);
    expect(slow!.body.count).toBe(1);
  });

  test("different keys are different writes", async ({ page, request }) => {
    await open(page);
    await page.evaluate(() =>
      window.testbed.postOverAllLines("/mt/echo", { op: "distinct" }, { "Idempotency-Key": "a" }),
    );
    await page.evaluate(() =>
      window.testbed.postOverAllLines("/mt/echo", { op: "distinct" }, { "Idempotency-Key": "b" }),
    );

    // Two logical writes, four arrivals: de-duplication must collapse the duplicates without
    // collapsing the writes.
    expect(await countOf(request, "distinct")).toBe(2);
  });
});
