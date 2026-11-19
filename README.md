# MultiPath

MultiPath carries one client's traffic redundantly over several network paths ("lines") to one
server, so that the traffic survives lines dying. Every byte is sent on every live line and the
receiver keeps the first copy of each byte; as long as one line works, nothing stalls and nothing
breaks. When every line dies at once (a laptop changing networks, a server restart), the client
notices within a second and starts over.

```
  program ──TCP──▶ multipath-client ══ line A (CDN) ═════╗
  (any TCP,         127.0.0.1:7000  ══ line B (direct) ══╬══▶ multipath-server ──TCP──▶ target
   e.g. HTTP)                       ══ line C (tunnel) ══╝     (behind nginx/CDN)       (e.g. nginx:80)
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
| `ts/` | Legacy: the browser client of protocol version 2, kept buildable until its successor exists. It does not speak version 3. |

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
link that is silent for 1 s, or whose acknowledgements stop for 1 s while it has data in flight.
When the last link dies, every open stream fails at once and the client starts a new session.

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

One stream moves at most `stream-window` bytes per round trip of its fastest line; raise
`-stream-window` for high-latency lines that carry bulk transfers.

## Development

- `cd go && go vet ./... && go test -race ./...` runs the unit tests.
- `conformance/run.sh` builds both binaries and runs the conformance scenarios against them: impaired
  lines, a stalled line, links cut hundreds of times, server restarts, a network switch, a long
  session, and attacks. It prints the measured recovery times and resource counts at the end.
- A change to the wire changes `PROTOCOL.md`, `spec/vectors/frames.json` and the code together.
  `go test ./protocol -run Vectors -update` rewrites the vectors' hex from their fields.
- Read [`AGENTS.md`](./AGENTS.md) for the repository's rules.

## Releasing

`scripts/version.sh` sets the version in `VERSION` and in the Go implementation string, and its
header describes how a release is tagged. CI refuses a tag that disagrees with either. The Go module
needs no publishing: `go get github.com/micro-teams/multipath/go@go/v<version>` resolves the tag.

## License

MIT, see [LICENSE](./LICENSE).
