/*
 *  Description: MP-1 end to end — the plumbing, in a real browser, over real separate lines.
 *
 *               MP-1 claims two things and this file is where they are either true or not:
 *               configured with one same-origin line MultiPath changes nothing observable, and
 *               configured with real lines every request goes out over the line the registry says
 *               it should. Racing and de-duplication (MP-2 …) get their own specs as they land.
 *
 *  Author(s):
 *      agent4
 */

import { expect, test, type Page } from "@playwright/test";

const SERVER = process.env.TESTBED_SERVER_URL ?? "http://localhost:8080";

/** The lines run.sh starts. Kept here as an expectation, so a topology change fails loudly. */
const LINES = {
  fast: "http://localhost:9001",
  slow: "http://localhost:9002",
  flaky: "http://localhost:9003",
} as const;

async function open(page: Page) {
  await page.goto("/");
  await expect(page.locator("#log")).toContainText("ready");
}

/** Route the page over exactly the lines named, in order. */
async function useLines(page: Page, ids: Array<keyof typeof LINES>) {
  await page.evaluate(
    (lines) => window.testbed.setRegistry({ lines }),
    ids.map((id) => ({ id, url: LINES[id], transport: "test", weight: 100 })),
  );
}

test.beforeEach(async ({ request }) => {
  await request.post(`${SERVER}/mt/reset`);
});

test.describe("MP-1: a single same-origin line changes nothing", () => {
  test("resolves to the bare relative path, as a plain fetch would", async ({ page }) => {
    await open(page);
    await page.evaluate(() => window.testbed.setRegistry({ lines: [{ id: "self", url: "" }] }));
    expect(await page.evaluate(() => window.testbed.resolve("/mt/probe"))).toBe("/mt/probe");
  });
});

test.describe("MP-1: routing over real lines", () => {
  test("sends the request over the first line in the registry", async ({ page }) => {
    await open(page);
    await useLines(page, ["fast"]);
    const result = await page.evaluate(() => window.testbed.get("/mt/probe"));
    expect(result.status).toBe(200);
    // The line proxy stamps its own name on the way back, so this is the line that really carried
    // it rather than the line we hoped would.
    expect(result.line).toBe("fast");
  });

  test("follows the registry when it changes, with no other change to the app", async ({
    page,
  }) => {
    await open(page);
    await useLines(page, ["fast"]);
    expect((await page.evaluate(() => window.testbed.get("/mt/probe"))).line).toBe("fast");

    await useLines(page, ["slow"]);
    expect((await page.evaluate(() => window.testbed.get("/mt/probe"))).line).toBe("slow");
  });

  test("loads the registry the server serves and routes by it", async ({ page }) => {
    await open(page);
    const registry = await page.evaluate((base) => window.testbed.loadRegistry(base), LINES.fast);
    expect(registry.lines.map((l) => l.id)).toEqual(["fast", "slow", "flaky"]);
    expect((await page.evaluate(() => window.testbed.get("/mt/probe"))).line).toBe("fast");
  });
});

test.describe("MP-1: the lines really are separate paths to one origin", () => {
  // If this ever fails, the testbed has stopped modelling the premise the whole design rests on,
  // and every de-duplication result it reports afterwards is meaningless.
  test("every line reaches the same single instance", async ({ page }) => {
    await open(page);
    const servers = new Set<string>();
    for (const id of ["fast", "slow", "flaky"] as const) {
      await useLines(page, [id]);
      // The flaky line fails one request in three by design; ask until it answers.
      for (let attempt = 0; attempt < 5; attempt++) {
        const result = await page.evaluate(() => window.testbed.post("/mt/echo", { op: "who" }));
        if (result.status === 200) {
          servers.add(result.body.servedBy);
          break;
        }
      }
    }
    expect(servers.size).toBe(1);
  });

  test("a slow line really is slower, end to end", async ({ page }) => {
    await open(page);
    await useLines(page, ["fast"]);
    await page.evaluate(() => window.testbed.get("/mt/probe"));
    await useLines(page, ["slow"]);
    await page.evaluate(() => window.testbed.get("/mt/probe"));

    const attempts = await page.evaluate(() => window.testbed.attempts);
    const fast = attempts.find((a) => a.lineId === "fast")!;
    const slow = attempts.find((a) => a.lineId === "slow")!;
    // The slow line adds 400ms; asserting on a 200ms margin keeps this from turning into a
    // measurement of the CI runner's mood.
    expect(slow.durationMs - fast.durationMs).toBeGreaterThan(200);
  });
});

test.describe("MP-1: the counting write is honest", () => {
  // The control case for every MP-2 assertion. Without a key nothing de-duplicates, so a count of
  // 2 here is what proves that MP-2's count of 1 is the filter's doing and not an accident of the
  // testbed — a duplicate that never really arrived would produce 1 either way.
  test("a write with no idempotency key is not de-duplicated", async ({ page, request }) => {
    await open(page);
    await useLines(page, ["fast", "slow"]);
    await page.evaluate(() => window.testbed.postOverAllLines("/mt/echo", { op: "not-yet" }));

    const counted = await (await request.get(`${SERVER}/mt/count?op=not-yet`)).json();
    expect(counted.count).toBe(2);
  });
});
