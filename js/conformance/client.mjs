// The JavaScript client as a process the conformance suite can drive like multipath-client: the same
// flags, a local HTTP port whose every request is carried by Client.fetch over the lines (the body
// streamed back as it arrives), and a status port answering GET /status with Client.stats(), which
// has the Go client's /status shape. It runs the built package (js/dist) in Node with the client's
// production defaults; conformance/run.sh builds it first.
//
//   node js/conformance/client.mjs -listen 127.0.0.1:7000 -entry http://127.0.0.1:8080 -status 127.0.0.1:7001
//
// -recv-window n sets the client's per-stream receive credit (default: the client's own), for
// measuring its effect on slow lines.
//
// A request the client fails before the response head is answered 502 with the reason; one that
// fails during the body is cut off, so the caller sees a broken response, never a short one. SIGTERM
// or SIGINT closes the client (GOODBYE) and exits.
import { createServer } from 'node:http';
import { Client } from '../dist/index.js';

const args = Object.fromEntries(
  process.argv
    .slice(2)
    .flatMap((a, i, all) => (a.startsWith('-') ? [[a.replace(/^-+/, ''), all[i + 1]]] : [])),
);
for (const flag of ['listen', 'entry', 'status']) {
  if (!args[flag]) {
    console.error(
      `usage: client.mjs -listen host:port -entry origin -status host:port (missing -${flag})`,
    );
    process.exit(2);
  }
}

const log = (m) => console.log(`${new Date().toISOString()} ${m}`);
const recvWindow = args['recv-window'] ? Number(args['recv-window']) : undefined;
const client = new Client({ entry: args.entry, log, recvWindow });

// Headers the local hop owns, or that fetch() sets itself.
const HOP = new Set([
  'host',
  'connection',
  'keep-alive',
  'content-length',
  'transfer-encoding',
  'accept-encoding',
]);

async function carry(req, res) {
  const parts = [];
  for await (const chunk of req) parts.push(chunk);
  const headers = new Headers();
  for (const [name, value] of Object.entries(req.headers))
    if (!HOP.has(name)) headers.set(name, String(value));
  const hasBody = req.method !== 'GET' && req.method !== 'HEAD';
  let response;
  try {
    response = await client.fetch(
      new Request(new URL(req.url, args.entry), {
        method: req.method,
        headers,
        body: hasBody ? Buffer.concat(parts) : undefined,
      }),
    );
  } catch (err) {
    log(`${req.method} ${req.url}: ${err}`);
    res.writeHead(502, { 'content-type': 'text/plain' }).end(String(err));
    return;
  }
  res.writeHead(response.status, Object.fromEntries(response.headers));
  if (!response.body) return void res.end();
  try {
    for await (const chunk of response.body) {
      if (!res.write(chunk)) await new Promise((r) => res.once('drain', r));
    }
    res.end();
  } catch (err) {
    log(`${req.method} ${req.url}: body: ${err}`);
    res.destroy(err);
  }
}

const host = (addr) => addr.slice(0, addr.lastIndexOf(':'));
const port = (addr) => Number(addr.slice(addr.lastIndexOf(':') + 1));
const local = createServer((req, res) => void carry(req, res));
local.listen(port(args.listen), host(args.listen));
createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify(client.stats()));
}).listen(port(args.status), host(args.status));

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => void client.close().then(() => process.exit(0)));
}
