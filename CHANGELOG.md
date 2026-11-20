# Changelog

## 0.3.0 (unreleased)

Protocol version 3 (`PROTOCOL.md`), a redesign around two standalone processes. It is not
compatible with 0.2 on the wire and is served on its own path, so a 0.2 origin can run beside it.

- `multipath-server`: one port for links (WebSocket) and the line list; every stream is spliced to
  one configured TCP target; optional PROXY protocol v2 towards the target; one log line per event;
  counters as JSON; GOODBYE to every session on shutdown.
- `multipath-client`: every connection to a local port becomes one stream over all lines; lines are
  redialled for the whole session; the line list is fetched from one entry and replaces the old one
  on every new session and every refresh; the last list is kept (in memory, and across restarts
  with `-lines-cache`) and starts a session at once while the entry is asked again, so an
  unreachable entry does not stop working lines; HTTP proxies are honoured; status as JSON.
- Frames: one envelope (`type`, `u16 length`, payload) for everything; unknown types are skipped.
  The handshake (HELLO, WELCOME, REJECT) carries the protocol and implementation versions, and the
  server's limits and liveness timings.
- Session: a reattached link receives its replay before any newer frame; frames ahead of a gap are
  kept in a bounded reorder buffer; each link writes from its own bounded queue; ACKs only when the
  acknowledged offset changes; a session ends on both sides when its last link dies.
- Liveness: the client pings every 0.5 s; a link is dead after 1 s of silence (3 s on the server)
  or 3 s without acknowledgement progress while data is outstanding. With 1 s for progress, a line
  of 600 to 900 ms round trip capped at 1 MB/s ended the session in half of the measured runs: an
  ACK waits behind the bytes queued ahead of it.
- Stream credit: both clients grant every stream 1 MiB of receive credit right after its SYN, so a
  2.6 MB download over a line of 600 to 900 ms round trip takes about 2.7 s instead of 9 s; the
  server's own credit, which bounds what it buffers, stays 256 KiB.
- Mux: credit is enforced by the receiver; each mux frame is written in one piece.
- Every length, offset and count is bounded, and every server resource is bounded by the number of
  live connections.
- Removed: the service header and tickets, the named service registry, the JVM origin, the Dart
  client, the Go application-WebSocket and HTTP round-trip helpers, and the testbed (replaced by
  `conformance/`).
- `@micro-teams/multipath` (`js/`, formerly `ts/`) speaks protocol version 3: `Client` with `fetch`
  (one stream per exchange, released when the response is complete; optional gzip decoding),
  `openWebSocket`, `live`, `stats` and events; `createLineRouter` for service workers, with a line
  store that replaces the list, lines only from the entry and its site, fallbacks that are logged
  and reported, and content-hash checks of static files before anything can cache them. It runs in
  browsers, service workers, React Native and Node, and is checked by the conformance suite in Node
  and in Chromium. Removed from it: the service header and tickets, `installFetchRouter`, the
  version-2 frames, and the exports of its internals.
- The npm package is published from release tags only, never from branches.

## 0.2.0-rc.7 and earlier

Protocol version 2, with a Go client and origin, a JVM origin, and TypeScript and Dart clients. See
the `v0.2.0-rc.*` tags.
