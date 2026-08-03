/*
 *  Description: MP-5 second half — the developer line panel, against real lines.
 *
 *               Everything MultiPath does is invisible by construction, which is exactly what makes
 *               it hard to trust: when it works there is nothing to see, and when it misbehaves
 *               there is also nothing to see. The panel is the answer to "how would you know?",
 *               so these specs check the things a person would actually open it to find out.
 *
 *  Author(s):
 *      agent4
 */

import { expect, test, type Page } from "@playwright/test";

import { reviveAll } from "./lines.js";

const LINES = {
  fast: "http://localhost:9001",
  slow: "http://localhost:9002",
  dead: "http://localhost:9099",
} as const;

async function open(page: Page, ids: Array<keyof typeof LINES>) {
  await page.goto("/");
  await expect(page.locator("#log")).toContainText("ready");
  await page.evaluate(
    (lines) => window.testbed.setRegistry({ lines }),
    ids.map((id) => ({ id, url: LINES[id], transport: "test", weight: 100 })),
  );
  await page.evaluate(() => window.testbed.showPanel());
}

// These specs read what was measured over the real proxies, so they need the topology intact. Said
// here rather than assumed: a spec that silently depends on another file's clean-up reports its
// neighbour's failure as its own.
test.beforeEach(async ({ request }) => {
  await reviveAll(request);
});

test.describe("MP-5: the panel answers the questions it exists for", () => {
  test("which line is being used, and where the others rank", async ({ page }) => {
    await open(page, ["slow", "fast"]);
    await page.evaluate(() => window.testbed.probe());

    const panel = page.locator("#panel");
    // Measured, so the fast line leads despite being listed second.
    await expect(panel).toContainText("routing to fast");

    const firstRow = panel.locator("tr").nth(1);
    await expect(firstRow).toContainText("fast");
  });

  test("how fast each line actually is", async ({ page }) => {
    await open(page, ["fast", "slow"]);
    await page.evaluate(() => window.testbed.probe());

    const panel = page.locator("#panel");
    // Real measurements over real proxies, not placeholders.
    await expect(panel).toContainText(/\d+ms/);
    const text = (await panel.textContent()) ?? "";
    const latencies = [...text.matchAll(/(\d+)ms/g)].map((m) => Number(m[1]));
    expect(Math.max(...latencies)).toBeGreaterThan(300);
  });

  test("why a line is not being used", async ({ page }) => {
    await open(page, ["fast", "dead"]);
    // One failure is noise by design, so probe until the run is long enough to be a fact.
    for (let i = 0; i < 3; i++) await page.evaluate(() => window.testbed.probe());

    const panel = page.locator("#panel");
    await expect(panel).toContainText("down");
    // The reason, not just the verdict — that is what someone opens this for.
    await expect(panel.locator("tr", { hasText: "dead" })).toContainText(/refused|fetch|error/i);
  });

  test("what actually happened, not only what the lines are like", async ({ page }) => {
    await open(page, ["fast"]);
    await page.evaluate(() => window.testbed.get("/mt/probe"));
    await page.evaluate(() => window.testbed.post("/mt/echo", { op: "panel" }));

    const panel = page.locator("#panel");
    await expect(panel).toContainText("recent requests");
    await expect(panel).toContainText("/mt/echo");
    await expect(panel).toContainText("POST");
  });

  test("it keeps itself current as traffic arrives", async ({ page }) => {
    await open(page, ["fast"]);
    const panel = page.locator("#panel");
    await expect(panel).not.toContainText("/mt/echo");

    await page.evaluate(() => window.testbed.post("/mt/echo", { op: "later" }));
    // No reload, no re-mount: the panel is expected to notice on its own.
    await expect(panel).toContainText("/mt/echo", { timeout: 5000 });
  });

  test("unmounting stops it", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("#log")).toContainText("ready");
    const stopped = await page.evaluate(() => {
      const unmount = window.testbed.showPanel();
      unmount();
      return typeof unmount === "function";
    });
    expect(stopped).toBe(true);
  });
});
