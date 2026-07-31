/*
 *  Description: The shape the testbed page exposes to the specs. Typed so a spec that drifts from
 *               the page fails at compile time rather than as a confusing runtime undefined.
 *
 *  Author(s):
 *      agent4
 */

interface TestbedAttempt {
  lineId: string;
  method: string;
  path: string;
  durationMs: number;
  status?: number;
  error?: unknown;
}

interface TestbedResult {
  status: number;
  line: string | null;
  // The testbed's endpoints answer with small, known objects; specs read fields off them directly.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  body: any;
}

interface TestbedLine {
  id: string;
  url: string;
  transport?: string;
  weight?: number;
}

interface Testbed {
  attempts: TestbedAttempt[];
  loadRegistry(from: string): Promise<{ lines: TestbedLine[] }>;
  setRegistry(registry: { lines: TestbedLine[] }): void;
  get(path: string): Promise<TestbedResult>;
  post(path: string, body: unknown, headers?: Record<string, string>): Promise<TestbedResult>;
  postOverAllLines(
    path: string,
    body: unknown,
    headers?: Record<string, string>,
  ): Promise<TestbedResult[]>;
  resolve(path: string): string;
  probe(): Promise<{
    ranked: string[];
    health: Array<{
      lineId: string;
      state: "up" | "degraded" | "down";
      latencyMs: number | null;
      throughputBps: number | null;
      consecutiveFailures: number;
      lastError: string | null;
    }>;
  }>;
  reset(): void;
}

/** What the launcher inlines for the application to read before it has fetched anything. */
interface MultipathBootConfig {
  appEntry: string;
  registry: { lines: TestbedLine[] } | null;
  registryUrl: string | null;
}

declare global {
  interface Window {
    testbed: Testbed;
    __multipath__: MultipathBootConfig;
    __app_started__?: boolean;
  }
}

export {};
