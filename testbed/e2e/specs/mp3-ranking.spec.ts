/*
 *  Description: MP-3, first half — measuring real lines and ranking them.
 *
 *               The unit tests prove the arithmetic. This proves the thing they cannot: that the
 *               ranking reflects the network. The slow line here is slow because a proxy really
 *               delays its bytes, and the dead line is dead because nothing is listening.
 *
 *  Author(s):
 *      agent4
 */

import { expect, test, type Page } from "@playwright/test";

const LINES = {
  fast: "http://localhost:9001",
  slow: "http://localhost:9002",
  // Nothing listens here. A registry may name a line that is simply gone.
  dead: "http://localhost:9099",
} as const;

async function open(page: Page, ids: Array<keyof typeof LINES>) {
  await page.goto("/");
  await expect(page.locator("#log")).toContainText("ready");
  await page.evaluate(
    (lines) => window.testbed.setRegistry({ lines }),
    ids.map((id) => ({ id, url: LINES[id], transport: "test", weight: 100 })),
  );
}

test.describe("MP-3: the ranking follows the network", () => {
  test("the genuinely faster line ends up first", async ({ page }) => {
    // Registered slow-first, so passing means measurement overrode registry order rather than
    // happening to agree with it.
    await open(page, ["slow", "fast"]);
    const { ranked, health } = await page.evaluate(() => window.testbed.probe());

    expect(ranked[0]).toBe("fast");
    const fast = health.find((h) => h.lineId === "fast")!;
    const slow = health.find((h) => h.lineId === "slow")!;
    expect(fast.latencyMs).not.toBeNull();
    // The proxy adds 400ms; a margin of 200 keeps this from measuring the CI runner's mood.
    expect(slow.latencyMs! - fast.latencyMs!).toBeGreaterThan(200);
  });

  test("weight loses to measurement", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("#log")).toContainText("ready");
    await page.evaluate(
      (lines) => window.testbed.setRegistry({ lines }),
      [
        // The slow line is given every configured advantage.
        { id: "slow", url: LINES.slow, weight: 1000 },
        { id: "fast", url: LINES.fast, weight: 1 },
      ],
    );
    const { ranked } = await page.evaluate(() => window.testbed.probe());
    expect(ranked[0]).toBe("fast");
  });

  test("an unreachable line is recorded as failing and ranked last", async ({ page }) => {
    await open(page, ["dead", "fast"]);

    // One failure is noise by design, so probe until the run is long enough to be a fact.
    let health = (await page.evaluate(() => window.testbed.probe())).health;
    for (let i = 0; i < 3; i++) {
      health = (await page.evaluate(() => window.testbed.probe())).health;
    }
    const dead = health.find((h) => h.lineId === "dead")!;
    expect(dead.state).toBe("down");
    expect(dead.lastError).not.toBeNull();

    const { ranked } = await page.evaluate(() => window.testbed.probe());
    expect(ranked[ranked.length - 1]).toBe("dead");
  });

  test("requests then go over the line that measured fastest", async ({ page }) => {
    await open(page, ["slow", "fast"]);
    await page.evaluate(() => window.testbed.probe());

    // The proxy stamps its own name on the way back, so this is the line that really carried it.
    const result = await page.evaluate(() => window.testbed.get("/mt/probe"));
    expect(result.line).toBe("fast");
  });

  test("a dead first line does not stop the app from working", async ({ page }) => {
    await open(page, ["dead", "fast"]);
    for (let i = 0; i < 3; i++) await page.evaluate(() => window.testbed.probe());

    const result = await page.evaluate(() => window.testbed.get("/mt/probe"));
    expect(result.status).toBe(200);
    expect(result.line).toBe("fast");
  });
});
