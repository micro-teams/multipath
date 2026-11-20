# MultiPath

MultiPath carries one client's traffic redundantly over several network paths ("lines") to one
server, so that the traffic survives lines dying. Every byte is sent on every live line and the
receiver keeps the first copy of each byte; as long as one line works, nothing stalls and nothing
breaks. When every line dies at once (a laptop changing networks, a server restart), the client
notices within a second and starts over.

```
  program --TCP--> multipath-client === line A (CDN) ======+
  (any TCP,         127.0.0.1:7000  === line B (direct) ===+===> multipath-server --TCP--> target
   e.g. HTTP)                       === line C (tunnel) ===+     (behind nginx/CDN)        (e.g. nginx:80)
```

Each TCP connection a program opens to the client's local port becomes one stream; the server
splices every stream to its one configured target. There is no destination in the protocol, so the
server is never an open relay. The wire protocol is defined in [`PROTOCOL.md`](./PROTOCOL.md).

## Components

| Path | What it is |
|---|---|
| `go/cmd/multipath-server` | The server: a reverse proxy that accepts links on one HTTP port, serves the line list, and splices every stream to one TCP target. |
| `go/cmd/multipath-client` | The client: a local TCP port whose every connection becomes one stream over all lines. |
| `go/` packages | `protocol` (frames, limits, line list), `link` (WebSocket links, HTTP proxies), `session` (the redundant byte stream), `mux` (streams with credit), `server`, `client`. |
| `spec/vectors/` | Encodings of every frame, shared by all implementations. |
| `conformance/` | A fault-injecting proxy and scenarios that run the real binaries through broken lines and attacks. |
| `js/` | `@micro-teams/multipath`, the JavaScript client for browsers, service workers, React Native and Node: `Client` (HTTP exchanges and WebSockets as streams) and the service worker's line router. |

## Running the server

```sh
cd go && go build ./cmd/multipath-server
./multipath-server -listen :8080 -target 127.0.0.1:80 -lines-file /etc/multipath/lines.json
```

| Flag | Default | Meaning |
|---|---|---|
| `-listen` | `:8080` | Plain HTTP address for links and the line list. TLS is terminated in front. |
| `-target` | (required) | `host:port` every stream is spliced to. |
| `-lines-file` | (required) | The line list served at `-lines-path`; read on every request, so it can change without a restart. |
| `-link-path` | `/multipath/v3/link` | WebSocket endpoint for links. |
| `-lines-path` | `/multipath/v3/lines` | Endpoint for the line list. |
| `-proxy-protocol` | off | Prefix every target connection with a PROXY protocol v2 header carrying the client address. |
| `-status` | off | Local address serving the counters as JSON. |
| `-status-every` | `1m` | Interval of the counters log line; `0` turns it off. |
| `-window`, `-stream-window`, `-max-streams`, `-max-links` | 2 MiB, 256 KiB, 256, 8 | Limits announced to clients (PROTOCOL.md section 7). |

The line list names every public path to the server, each an origin without a path:

```json
{"lines": [
  {"id": "cf", "url": "https://cf.example.app"},
  {"id": "direct", "url": "https://direct.example.app"},
  {"id": "frp", "url": "https://frp.example.app"}
]}
```

Every line reaches the same server process through whatever front it has. Behind nginx:

```nginx
map $http_upgrade $connection_upgrade { default upgrade; '' close; }

location /multipath/v3/ {
    proxy_pass http://multipath-server:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_buffering off;
}
```

With `-proxy-protocol`, the target must expect the header, for example an nginx server block with
`listen 8081 proxy_protocol;` and `set_real_ip_from` the server's address; nginx cannot mix
connections with and without the header on one port. The client address comes from
`CF-Connecting-IP`, else the first `X-Forwarded-For` entry, else the socket peer; enable the option
only when the server is reachable solely through fronts that set these headers.

A version-2 origin (the JVM origin on `/mt/link`) can keep running beside this server during a
migration: the paths differ, and each client speaks one version.

On SIGINT or SIGTERM the server sends GOODBYE to every session and exits; clients start a new
session with the next server process at once, after a random delay of up to two seconds.

## Running the client

```sh
cd go && go build ./cmd/multipath-client
./multipath-client -listen 127.0.0.1:7000 -entry https://example.app -status 127.0.0.1:7001
curl -H 'Host: example.app' http://127.0.0.1:7000/
```

| Flag | Default | Meaning |
|---|---|---|
| `-entry` | (required) | The origin the line list is fetched from. An `https` entry accepts only `https` lines. |
| `-listen` | `127.0.0.1:7000` | Local address programs connect to. |
| `-status` | off | Local address serving `GET /status` as JSON. |
| `-proxy` | from the environment | `http://` proxy for lines and the line list. Without it, `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` apply. |
| `-link-path`, `-lines-path` | the server's defaults | Paths on the lines and on the entry. |
| `-refresh` | `10m` | Interval of line list refreshes; a refreshed list replaces the old one. |
| `-wait` | `10s` | How long a local connection waits for a live session before it is closed. |

The client dials every line and keeps redialling a failed one with backoff (200 ms to 5 s) for the
whole session. Liveness timings come from the server: the client pings every 0.5 s and drops a
link that is silent for 1 s, or whose acknowledgements stop for 3 s while it has data in flight.
Each stream grants the server 1 MiB of credit, so one download moves up to 1 MiB per round trip.
When the last link dies, every open stream fails at once and the client starts a new session.

## The JavaScript client

`js/` is the npm package `@micro-teams/multipath` (published to GitHub Packages from release tags).
It speaks the same protocol as the Go client, inside the program instead of as a local port: a page,
a service worker or an app holds one `Client`, and every request it sends is one stream of one
session over every line.

```ts
import { Client } from '@micro-teams/multipath';

const client = new Client({
  entry: 'https://example.app', // the line list comes from here, and replaces any earlier one
  onEvent: (e) => console.log(e), // line up/down, session start/end, router fallbacks
});
const res = await client.fetch('/api/items'); // one HTTP/1.1 exchange on a fresh stream
const ws = await client.openWebSocket('/updates', ['v1']); // a WebSocket on a fresh stream
console.log(client.stats()); // the same shape as the Go client's /status
await client.close(); // GOODBYE
```

| Option | Default | Meaning |
|---|---|---|
| `entry` | (required) | Origin the line list is fetched from. An `https` entry accepts only `https` lines. |
| `site` | the entry's host | Every line must be on this host or a subdomain of it; `null` allows any host. |
| `store` | none | Keeps the last list across restarts; it is dialled at once while the fresh list is fetched. |
| `linkPath`, `linesPath` | the server's defaults | Paths on the lines and on the entry. |
| `refreshMs` | 10 minutes | Interval of line list refreshes. |
| `waitMs` | 10 s | How long a request waits for a live session before it fails. |
| `recvWindow` | 1 MiB | Receive credit of each stream (PROTOCOL.md section 6). |
| `compression` | `true` | Ask for gzip and decode it with `DecompressionStream`. |
| `buffered` | `false` | Read each response whole before returning it, for platforms whose `Response` cannot stream. |
| `log` | `console.info` | One line per event. |

How it behaves:

- `fetch` resolves as soon as the response head arrives and streams the body. The stream is reset
  as soon as the body is complete or the caller abandons it, so no request holds a stream, or the
  server a target connection, beyond its answer. The request side is not half-closed after the
  request: nginx takes a client's half-close for an abort and answers a proxied request with 499.
- With `compression`, a request without its own `Accept-Encoding` asks for gzip and the body is
  decoded before the caller sees it (without `Content-Encoding` and `Content-Length`, as `fetch`
  would hand it over). The target decides whether to compress: for nginx, `gzip on` with
  `gzip_types` for scripts, stylesheets, JSON and fonts. Over a slow line a first load shrinks to
  roughly a quarter.
- Every link writes from a queue of its own, handing frames to the WebSocket only while its
  `bufferedAmount` is below 256 KiB, so frames another link has already delivered are skipped rather
  than sent. Queue plus `bufferedAmount` is bounded by the window plus 128 KiB, and a link whose
  socket stops draining for 1 s is closed.

Platforms:

- Browsers and service workers need nothing special. A page and its service worker each hold their
  own client: the worker carries the requests the browser makes (documents, scripts, fonts), the
  page everything it fetches itself.
- React Native: pass `compression: false` and `buffered: true` (there is no `DecompressionStream`,
  and its `Response` cannot take a stream; the constructor refuses the defaults rather than
  degrading quietly). Its WebSocket keeps no reliable `bufferedAmount`, so frames go to the socket at
  once: the queue bound then covers only the client's own queue, and a link that stops writing is
  caught by the liveness rules (1 s without a byte, or 3 s without acknowledgement progress).
- Node 22 and later: the built-in `WebSocket` and `fetch` are used. Browsers throttle timers in
  background tabs; a session that misses its pings there ends and is replaced, as after a network
  change.

### The service worker's line router

```ts
import { cacheLineStore, createLineRouter } from '@micro-teams/multipath';

const router = createLineRouter({
  entry: self.location.origin,
  store: cacheLineStore('lines', new URL('__lines', self.location.href).href),
  fallbackAfterMs: 3000, // no live session by then: send requests to the network
  requestTimeoutMs: 10000, // a GET without an answer head by then: send it to the network
  integrity: [
    { pattern: /\/_expo\/static\/fonts\/[^/]+\.([0-9a-f]{32})\.\w+$/, algorithm: 'SHA-256' },
    { pattern: /\/_expo\/static\/(?:js|css|assets)\/.*[-.]([0-9a-f]{32})\.\w+$/, algorithm: 'MD5' },
  ],
});
self.addEventListener('fetch', (e) => e.respondWith(router.fetch(e.request)));
```

- A request goes to the network instead, logged and reported as a `fallback` event with its reason,
  when no session is live, or for a GET or HEAD when the lines fail it or do not answer in time. Any
  other method is sent at most once over the lines.
- Integrity: a file whose path matches a rule carries a hash of its content in its name (the
  capture group, a prefix of the hex digest). Its body streams through at once except the last
  piece, which is released only once the digest matches; on a mismatch the body fails, so no cache
  stores it, and that URL goes to the network from then on. `MD5`, `SHA-1`, `SHA-256`, `SHA-384`
  and `SHA-512` are supported.

## Observability

Both processes log one line per event, naming the session by the first 8 hex digits of its id:
session start (with the peer's implementation and the client address), every link up and down with
the reason, every REJECT with the reason, stream failures, session end with the reason, and every
limit violation. Nothing is retried or given up silently.

- The server's `-status` endpoint and its periodic log line give the live counts of sessions, links,
  streams, target connections and goroutines.
- The client's `/status` gives the session (id, state, age, bytes sent, unacknowledged and received),
  every line (state, milliseconds since its last byte, bytes in and out, reconnects, last reason),
  every open stream (bytes in and out, buffered bytes, send credit), and why the previous session
  ended.

## Resources and limits

Every server resource is bounded by the number of live TCP connections times a constant
(PROTOCOL.md section 8): a session needs a live link and ends with its last one; a link costs one
reader and one writer goroutine and at most `window + 128 KiB` of queued bytes; a stream costs one
target connection and two splice goroutines; all timers run on one shared clock. A connection that
does not finish its handshake within 5 seconds is closed.

`-stream-window` is the credit the server grants each stream, and so bounds what it buffers per
stream for uploads; downloads move as much per round trip as the client grants (1 MiB in both
clients, PROTOCOL.md section 6).

## Development

- `cd go && go vet ./... && go test -race ./...` runs the unit tests.
- `cd js && npm ci && npm test` runs the JavaScript unit tests, including the shared vectors;
  `npm run lint`, `npm run format:check` and `npm run build` are what CI runs besides.
- `conformance/run.sh` builds both binaries and the JavaScript package and runs the conformance
  scenarios against them, first with the Go client, then with the JavaScript client in Node:
  impaired lines, a stalled line, links cut hundreds of times, server restarts, a network switch, a
  long session, a slow distant line, and attacks. `MP_BROWSER=1` adds the Chromium leg (after
  `npx playwright install chromium` in `js/`). It prints the measured recovery times and resource
  counts at the end.
- A change to the wire changes `PROTOCOL.md`, `spec/vectors/frames.json` and the code together.
  `go test ./protocol -run Vectors -update` rewrites the vectors' hex from their fields.
- Read [`AGENTS.md`](./AGENTS.md) for the repository's rules.

## Releasing

`scripts/version.sh` sets the version in `VERSION`, the Go and JavaScript implementation strings and
`js/package.json`, and its header describes how a release is tagged. CI refuses a tag that disagrees
with any of them, and publishes `@micro-teams/multipath` to GitHub Packages from the tag; nothing is
published from a branch. The Go module needs no publishing:
`go get github.com/micro-teams/multipath/go@go/v<version>` resolves the tag.

## License

MIT, see [LICENSE](./LICENSE).
