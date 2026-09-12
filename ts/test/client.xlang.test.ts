import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { describe, expect, it } from 'vitest';
import { Client } from '../src/client.js';

// Cross-language proof for the whole browser client: the TypeScript Client (global WebSocket standing
// in for the browser's) against a real JVM Origin, driving both routed paths — a tunnel spliced to an
// echo and a normal HTTP exchange spliced to a greeter. Skipped unless MP_JVM_CP is set.
const cp = process.env.MP_JVM_CP;
const maybe = cp ? it : it.skip;

async function startOrigin(n: number): Promise<{ port: number; kill: () => void }> {
  const proc = spawn('java', ['-cp', cp!, 'app.microteams.multipath.OriginMain', String(n)]);
  proc.stderr.pipe(process.stderr);
  const rl = createInterface({ input: proc.stdout });
  const port = await new Promise<number>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error('java origin did not report LISTENING')),
      30000,
    );
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

const fast = {
  pingIntervalMs: 20,
  deadAfterMs: 2000,
  ackIntervalMs: 8,
  reconnectDelayMs: 20,
  maxDelayMs: 200,
};

describe('browser client cross-language', () => {
  maybe(
    'tunnels and round-trips HTTP through a JVM origin over WebSocket links',
    async () => {
      const n = 3;
      const origin = await startOrigin(n);
      try {
        const lines = Array.from({ length: n }, () => `ws://127.0.0.1:${origin.port}`);
        const client = await Client.dial(lines, fast);

        // Tunnel: write, half-close, read the echo back to EOF.
        const st = client.open('echo', new TextEncoder().encode('ticket'));
        const msg = new Uint8Array(20 * 1024).map((_, i) => (i * 7 + 3) & 0xff);
        await st.write(msg);
        st.closeWrite();
        const echoed: number[] = [];
        for (;;) {
          const chunk = await st.read();
          if (chunk === null) break;
          echoed.push(...chunk);
        }
        expect(Uint8Array.from(echoed)).toEqual(msg);

        // Normal: an HTTP round trip to the origin's own greeter.
        const resp = await client.fetch('greeter', new Request('http://origin/xlang'));
        expect(resp.status).toBe(200);
        expect(await resp.text()).toBe('hello /xlang');

        // WebSocket: a real RFC 6455 handshake and message round trip through the substrate to the
        // origin's ws-echo backend — proves the browser client's application-level WebSocket, not
        // just a byte tunnel. Binary only: the origin's test echo reuses the link layer's
        // byte-stream WebSocketConn, which (by original design — see websocket.go's own header
        // comment) only understands OP_BINARY; a text frame throws there and drops the connection.
        const ws = await client.openWebSocket('ws-echo', '/echo');
        const gotMessages: ArrayBuffer[] = [];
        ws.onmessage = (ev) => gotMessages.push(ev.data as ArrayBuffer);
        ws.send(new TextEncoder().encode('hello ws'));
        ws.send(new TextEncoder().encode('second message'));
        await new Promise((resolve) => setTimeout(resolve, 500));
        expect(gotMessages.length).toBe(2);
        expect(new TextDecoder().decode(gotMessages[0])).toBe('hello ws');
        expect(new TextDecoder().decode(gotMessages[1])).toBe('second message');
        ws.close();

        client.close();
      } finally {
        origin.kill();
      }
    },
    40000,
  );
});
