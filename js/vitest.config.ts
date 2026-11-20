// The unit test runner's configuration: every test/*.test.ts file, in Node.
import { defineConfig } from 'vitest/config';
export default defineConfig({ test: { include: ['test/**/*.test.ts'] } });
