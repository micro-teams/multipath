import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./specs",
  // Serial. The origin holds one shared set of counters, and specs assert on exact counts —
  // running them concurrently would have them invalidating each other's arithmetic.
  workers: 1,
  fullyParallel: false,
  // Zero retries on purpose: these specs stage their own timing, so a flake is a real defect
  // (in MultiPath or in the testbed) and hiding it behind a retry is how a suite stops meaning
  // anything.
  retries: 0,
  timeout: 30_000,
  reporter: process.env.CI ? [["github"], ["list"]] : [["list"]],
  use: {
    baseURL: process.env.TESTBED_WEB_URL ?? "http://localhost:8000",
    trace: "retain-on-failure",
  },
});
