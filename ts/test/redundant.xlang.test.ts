import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { describe, expect, it } from 'vitest';
import { RedundantStream } from '../src/redundant.js';

// Cross-language proof for the redundant layer: the browser RedundantStream (run here in Node, whose
// global WebSocket stands in for the browser's) against the real JVM RedundantServer echoing bytes,
// over WebSocket links. Skipped unless MP_JVM_CP is set (see testbed/run.sh).
const cp = process.env.MP_JVM_CP;
const maybe = cp ? it : it.skip;

async function startEcho(n: number): Promise<{ port: number; kill: () => void }> {
  const proc = spawn('java', [
    '-cp',
    cp!,
    'app.microteams.multipath.redundant.EchoServerMain',
    '0',
    String(n),
  ]);
  proc.stderr.pipe(process.stderr);
  const rl = createInterface({ input: proc.stdout });
  const port = await new Promise<number>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('java echo did not report LISTENING')), 30000);
    rl.on('line', (line) => {
      const m = /^LISTENING (\d+)/.exec(line);
      if (m) {
        clearTimeout(timer);
        resolve(Number(m[1]));
      }
    });
  });
  return { port, kill: () => proc.kill() };
}

describe('redundant stream cross-language', () => {
  maybe(
    'echoes a payload through a JVM redundant server over WebSocket links',
    async () => {
      const n = 3;
      const echo = await startEcho(n);
      try {
        const urls = Array.from({ length: n }, () => `ws://127.0.0.1:${echo.port}/mt/link`);
        const chunks: Uint8Array[] = [];
        let received = 0;
        let done!: () => void;
        const payload = new Uint8Array(64 * 1024).map((_, i) => (i * 31 + 7) & 0xff);
        const complete = new Promise<void>((resolve) => (done = resolve));

        const stream = new RedundantStream({
          urls,
          pingIntervalMs: 20,
          deadAfterMs: 2000,
          ackIntervalMs: 8,
          reconnectDelayMs: 20,
          maxDelayMs: 200,
        });
        stream.onDeliver = (bytes) => {
          chunks.push(bytes.slice());
          received += bytes.length;
          if (received >= payload.length) done();
        };
        await stream.dial();
        await stream.write(payload);
        await complete;
        stream.close();

        const got = new Uint8Array(received);
        let off = 0;
        for (const c of chunks) {
          got.set(c, off);
          off += c.length;
        }
        expect(got.subarray(0, payload.length)).toEqual(payload);
      } finally {
        echo.kill();
      }
    },
    40000,
  );
});
