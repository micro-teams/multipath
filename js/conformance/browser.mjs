// The browser leg of the conformance suite: the built package (js/dist) running in headless Chromium
// against the real server, driven by conformance/browser_test.go, which owns the server, the target
// and the faulty lines and cuts lines while this runs.
//
//   node js/conformance/browser.mjs -entry http://127.0.0.1:8080 -seconds 6
//
// The page is served at <entry>/smoke/ by Playwright itself, so it shares the server's origin and
// fetches the real line list. In the page, a Client fetches /small, /big (checked byte for byte) and
// /echo (checked by its SHA-256) over and over for -seconds, opens a WebSocket to the target's /ws
// echo, and a line router fetches a file whose name carries its MD5, once with the right hash and once
// with a wrong one, which must fail and be reported as a fallback. It prints one MEASURE line and
// exits 0 when every check passed, 1 otherwise.
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { chromium } from 'playwright';

const args = Object.fromEntries(
  process.argv
    .slice(2)
    .flatMap((a, i, all) => (a.startsWith('-') ? [[a.replace(/^-+/, ''), all[i + 1]]] : [])),
);
if (!args.entry) {
  console.error('usage: browser.mjs -entry origin [-seconds n]');
  process.exit(2);
}
const seconds = Number(args.seconds ?? 6);
const dist = new URL('../dist/', import.meta.url);
// The target's /file/ answer, whose MD5 names it in the router check.
const sample = 'multipath integrity sample\n'.repeat(1000);
const md5 = createHash('md5').update(sample).digest('hex');

const page = `<!doctype html><meta charset="utf-8"><script type="module">
import { Client, createLineRouter } from './dist/index.js';
const pattern = (i) => (i * 31 + (i >> 13)) & 0xff;
const hex = (b) => Array.from(new Uint8Array(b), (x) => x.toString(16).padStart(2, '0')).join('');
async function run(seconds, md5) {
  const events = [];
  const client = new Client({ entry: location.origin, onEvent: (e) => events.push(e) });
  if (!(await client.live(5000))) throw new Error('no live session');
  let rounds = 0;
  for (const end = Date.now() + seconds * 1000; Date.now() < end; rounds++) {
    const small = await (await client.fetch('/small')).text();
    if (small !== 'ok') throw new Error('small: ' + small);
    const big = new Uint8Array(await (await client.fetch('/big?n=1048576')).arrayBuffer());
    if (big.length !== 1048576 || !big.every((v, i) => v === pattern(i))) throw new Error('big arrived altered');
    const up = crypto.getRandomValues(new Uint8Array(65536));
    const sum = await (await client.fetch('/echo', { method: 'POST', body: up })).text();
    if (sum !== hex(await crypto.subtle.digest('SHA-256', up))) throw new Error('echo arrived altered');
  }
  const ws = await client.openWebSocket('/ws');
  const echoed = await new Promise((resolve, reject) => {
    ws.onmessage = (ev) => resolve(new Uint8Array(ev.data));
    ws.onerror = reject;
    ws.send(new Uint8Array([1, 2, 3]));
  });
  if (echoed.join() !== '1,2,3') throw new Error('websocket echo: ' + echoed.join());
  ws.close();
  const routed = [];
  const router = createLineRouter({
    entry: location.origin,
    integrity: [{ pattern: /-([0-9a-f]{32})\\.txt$/, algorithm: 'MD5' }],
    onEvent: (e) => routed.push(e),
  });
  const good = await (await router.fetch(new Request(location.origin + '/file/sample-' + md5 + '.txt'))).text();
  if (good.length !== ${sample.length}) throw new Error('router: wrong sample');
  let refused = '';
  try {
    await (await router.fetch(new Request(location.origin + '/file/sample-' + '0'.repeat(32) + '.txt'))).text();
  } catch (err) {
    refused = String(err);
  }
  // Chromium reports a body that failed as "Failed to fetch", whatever the cause; the event says why.
  if (!refused || !routed.some((e) => e.type === 'fallback' && /^integrity mismatch/.test(e.reason))) {
    throw new Error('router: a wrong hash passed: ' + refused);
  }
  const stats = client.stats();
  await client.close();
  await router.close();
  return { rounds, sessions: stats.sessionsStarted, reconnects: stats.lines.map((l) => l.reconnects), events: events.length };
}
window.smoke = run;
</script>`;

// A page Playwright fulfils has no network address, so Chromium counts it as public and refuses its
// connections to loopback lines; in this test every line is on loopback by construction.
const browser = await chromium.launch({ args: ['--disable-features=LocalNetworkAccessChecks'] });
let failed = false;
try {
  const context = await browser.newContext();
  const tab = await context.newPage();
  tab.on('console', (m) => console.log(`page: ${m.text()}`));
  await tab.route(`${args.entry}/smoke/**`, async (route) => {
    const path = new URL(route.request().url()).pathname.slice('/smoke/'.length);
    if (path === '') return route.fulfill({ contentType: 'text/html', body: page });
    const body = await readFile(new URL(path.slice('dist/'.length), dist));
    return route.fulfill({ contentType: 'text/javascript', body });
  });
  await tab.goto(`${args.entry}/smoke/`);
  await tab.waitForFunction(() => typeof window.smoke === 'function');
  const started = Date.now();
  const result = await tab.evaluate(([s, m]) => window.smoke(s, m), [seconds, md5]);
  console.log(
    `MEASURE browser: ${result.rounds} rounds of small + 1 MiB down + 64 KiB up in ${Date.now() - started} ms; ` +
      `sessions ${result.sessions}; reconnects per line ${result.reconnects.join('/')}; websocket echo and integrity checks passed`,
  );
  failed = result.sessions !== 1;
} catch (err) {
  console.error(`browser leg failed: ${err}`);
  failed = true;
} finally {
  await browser.close();
}
process.exit(failed ? 1 : 0);
