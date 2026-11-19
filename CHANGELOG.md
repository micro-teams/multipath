# Changelog

## 0.3.0 (unreleased)

Protocol version 3 (`PROTOCOL.md`), a redesign around two standalone processes. It is not
compatible with 0.2 on the wire and is served on its own path, so a 0.2 origin can run beside it.

- `multipath-server`: one port for links (WebSocket) and the line list; every stream is spliced to
  one configured TCP target; optional PROXY protocol v2 towards the target; one log line per event;
  counters as JSON; GOODBYE to every session on shutdown.
- `multipath-client`: every connection to a local port becomes one stream over all lines; lines are
  redialled for the whole session; the line list is fetched from one entry and replaces the old one
  on every new session and every refresh; HTTP proxies are honoured; status as JSON.
- Frames: one envelope (`type`, `u16 length`, payload) for everything; unknown types are skipped.
  The handshake (HELLO, WELCOME, REJECT) carries the protocol and implementation versions, and the
  server's limits and liveness timings.
- Session: a reattached link receives its replay before any newer frame; frames ahead of a gap are
  kept in a bounded reorder buffer; each link writes from its own bounded queue; ACKs only when the
  acknowledged offset changes; a session ends on both sides when its last link dies.
- Liveness: the client pings every 0.5 s; a link is dead after 1 s of silence (3 s on the server)
  or 1 s without acknowledgement progress while data is outstanding.
- Mux: credit is enforced by the receiver; each mux frame is written in one piece.
- Every length, offset and count is bounded, and every server resource is bounded by the number of
  live connections.
- Removed: the service header and tickets, the named service registry, the JVM origin, the Dart
  client, the Go application-WebSocket and HTTP round-trip helpers, and the testbed (replaced by
  `conformance/`).
- `ts/` is the 0.2 browser client, unchanged; it speaks protocol version 2 only.

## 0.2.0-rc.7 and earlier

Protocol version 2, with a Go client and origin, a JVM origin, and TypeScript and Dart clients. See
the `v0.2.0-rc.*` tags.
