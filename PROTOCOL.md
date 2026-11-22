# MultiPath protocol, version 3

This document is the single normative description of the MultiPath wire protocol. Every
implementation (the Go server and client in `go/`, the JavaScript client to come) follows it, and the
shared test vectors in [`spec/vectors/`](./spec/vectors) pin its encodings. Where an implementation
and this document disagree, the implementation is wrong.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## 1. Model

One **client** reaches one **server** over several network paths called **lines** (for example a
CDN-fronted hostname, a direct hostname, an frp tunnel). Every line leads to the same server process.

- A **link** is one live connection over one line.
- A **session** joins up to `maxLinks` links into one reliable, ordered, duplex byte stream. Every
  byte is sent on every live link; the receiver keeps the first copy of each byte and drops the
  rest. As long as one link works, the session works.
- A **stream** is one multiplexed, flow-controlled byte stream inside a session. The client opens a
  stream for each local TCP connection it accepts; the server splices each stream to its one
  configured TCP target.

```
  stream  (mux, section 6)       many streams, each with its own credit window
  session (section 5)            one redundant, ordered byte stream, offsets and acknowledgements
  link    (sections 3 and 4)     WebSocket binary messages, one message = one frame
```

There is no per-stream header naming a destination: every stream of every session reaches the same
target. The server is therefore never an open relay.

## 2. Discovery: the line list

The server answers `GET <linesPath>` (default `/multipath/v3/lines`) with a JSON document taken from
its configuration:

```json
{"lines": [
  {"id": "cf", "url": "https://cf.example.app"},
  {"id": "direct", "url": "https://direct.example.app"}
]}
```

- `lines` MUST contain at least one entry and at most 255.
- `id` MUST be non-empty and unique within the document.
- `url` MUST be an absolute origin with scheme `https` or `http`, a host, an optional port, and no
  path, query, fragment or credentials.
- `address` is OPTIONAL. When present it MUST be an IP literal in its canonical form (dotted
  decimal without leading zeros, or RFC 5952 in brackets, with no zone) and a port from 1 to 65535,
  such as `203.0.113.7:8443` or `[2001:db8::7]:8443`.
- Other members are ignored, so a server may carry extra fields for other consumers.

A client is configured with one **entry** (an origin such as `https://example.app`). It fetches the
line list from the entry, and links each line by issuing a WebSocket upgrade to
`<line url><linkPath>` (default `/multipath/v3/link`); `https` lines use `wss`, `http` lines use `ws`.

A line with an `address` is for a network that resets TLS connections by the server name in their
ClientHello. A client that honours the address connects to it instead of resolving the URL's host,
sends no server name indication (RFC 6066 has none for an IP address), verifies the server's
certificate exactly as it would for the URL (the chain to its trusted roots, and the URL's host as
the name), and sends the URL's host and port in the upgrade's `Host` header. A client MUST NOT
honour an address unless it can verify the certificate against the URL's host; it then dials the
URL and ignores the address (a browser does). The front of such a line answers a handshake without
a server name with the line's certificate.

Rules for the client:

- The fetched list **replaces** the previous one; lines are never accumulated from earlier lists.
- The client fetches the list for every new session and refreshes it periodically (both clients
  every 10 minutes). A client that kept an earlier list (from the previous session, or stored across
  restarts) MAY dial it while the fetch runs, so that a slow or unreachable entry delays nothing; the
  fetched list then applies to that session. Both clients do, and ask an entry that does not answer
  again with backoff (1 s to 30 s) until it does. A kept list MUST have been served by the same
  entry, and passes the same checks as a fetched one. A list applies to the running session: added lines are
  dialled and links of removed lines are closed, which a client MAY defer until a line still in the
  list is up, so that a replaced list never ends a working session.
- When the entry is `https`, every line MUST be `https`; a list that mixes in `http` lines is
  refused, so that a tampered or misconfigured list cannot downgrade confidentiality.
- A client uses only lines from its entry's list. It uses at most `maxLinks` lines (section 4.2);
  further lines are logged and ignored.
- A browser client additionally accepts only lines on its own site (the JavaScript client: the
  entry's host or a configured parent domain, and its subdomains) and refuses any other list.

## 3. Link layer

A link is a WebSocket (RFC 6455) connection. TLS, when present, is terminated by whatever sits in
front of the server (nginx, a CDN, a tunnel); the server itself speaks plain HTTP.

- Every WebSocket **binary message carries exactly one frame** (section 4). A sender MUST NOT split
  a frame across messages or put two frames in one message. Text messages MUST NOT be sent; a
  receiver that gets one closes the link.
- Since every frame is self-delimiting, a receiver MAY parse the payload as a byte stream.
- A WebSocket message longer than `maxPayload + 3` bytes closes the link before its payload is read.
- WebSocket ping, pong and close control frames are handled as RFC 6455 requires; MultiPath's own
  liveness (section 5.4) does not rely on them.

## 4. Frames

### 4.1 Frame envelope

Every frame on a link is

```
type: u8 | length: u16 | payload: length bytes
```

All integers in this document are big-endian. `length` is the payload length only. One length width
for every frame keeps one parser for everything: every frame of this protocol fits in 64 KiB.

- A frame whose `type` the receiver does not know is **skipped** (its payload is read and dropped).
  It is never treated as corruption, so later versions can add frame types.
- A known frame whose payload does not have the size its type requires (below) is a protocol
  violation: the receiver closes that link.
- A frame longer than the receiver's `maxPayload` closes the link.

| Type | Name | Direction | Payload |
|---|---|---|---|
| 0x01 | HELLO | client -> server | `version: u16, session: [16]u8, link: u8, mode: u8, impl: UTF-8` |
| 0x02 | WELCOME | server -> client | `version: u16, flags: u8, maxPayload: u16, window: u32, streamWindow: u32, maxStreams: u16, maxLinks: u8, pingMs: u16, clientDeadMs: u16, serverDeadMs: u16, progressMs: u16, impl: UTF-8` |
| 0x03 | REJECT | server -> client | `code: u8, reason: UTF-8` |
| 0x04 | DATA | both | `offset: u64, crc32: u32, bytes` |
| 0x05 | ACK | both | `offset: u64` |
| 0x06 | PING | client -> server | `nonce: u64` |
| 0x07 | PONG | server -> client | `nonce: u64` |
| 0x08 | GOODBYE | both | `reason: UTF-8` |

Sizes and bounds:

- HELLO: 20 bytes plus `impl` of at most 64 bytes. `mode` is 0 (new) or 1 (join).
- WELCOME: 24 bytes plus `impl` of at most 64 bytes. `flags` bit 0 is `created` (section 4.2).
- REJECT and GOODBYE: `reason` at most 256 bytes. REJECT has 1 more byte for `code`.
- DATA: 12 bytes plus 1 to 32768 bytes of data. `crc32` is IEEE CRC-32 of the data bytes; a
  mismatch closes the link (the bytes arrive intact on another link or in a replay).
- ACK, PING, PONG: exactly 8 bytes.
- `impl` is the implementation's name and version, for example `multipath-go/0.3.0`. Both sides
  log the peer's `impl` and protocol version.

### 4.2 Handshake

On every new link the client sends HELLO as its first frame and waits for WELCOME or REJECT.

- `version` is the protocol version the client speaks: 3.
- `session` is the session id: 16 random bytes chosen by the client for each new session. It is a
  bearer secret: whoever knows it can attach links to the session.
- `link` is the link index, 0 to `maxLinks - 1`, unique among the session's lines. A new link with
  the index of a live link replaces it.
- `mode` is `new` until the client has received a WELCOME for this session, `join` afterwards.

The server answers:

| Case | Answer |
|---|---|
| `version` is not 3 | REJECT code 1 (version) |
| malformed HELLO | REJECT code 4 (malformed) |
| `link >= maxLinks` | REJECT code 3 (limit) |
| `join` and the session is unknown | REJECT code 2 (unknown session) |
| `new` and the session is unknown | create it, WELCOME with `created = 1` |
| known session (any mode) | attach the link, WELCOME with `created = 0` |

After a REJECT the server closes the link. It SHOULD let the REJECT reach the client before the
connection resets (the Go server half-closes and drains for up to one second).

The client:

- On REJECT code 2, or on a WELCOME with `created = 1` when an earlier WELCOME of the same session
  also had `created = 1`, the server has lost the session (it restarted). Several links may race to
  be first with mode `new`; exactly one of them creates the session, so one `created = 1` is normal
  and a second one is not. The client ends the whole session at once
  (section 5.5) and starts a new one after a random delay of 0 to 2 seconds, so that many clients do
  not reconnect in the same instant after a server restart.
- On other REJECT codes it logs the reason and retries that line with backoff.
- Uses the limits in WELCOME for the rest of the session. Until the first WELCOME it uses the
  minimums of section 7 for its send window.

Handshake timeouts: the server closes a connection that has not completed the WebSocket upgrade and
sent HELLO within 5 seconds. The client gives up on a connection that is not open (DNS, TCP, proxy,
TLS and the WebSocket upgrade) within 10 seconds, and on a link whose HELLO has not been answered
within 5 seconds of being sent, and retries it with backoff. The two client deadlines are separate
because a connection set up through a distant CDN can take several seconds by itself, which must
not count against the server's time to answer; the server's own deadline starts only when the
connection reaches it.

## 5. Session

### 5.1 Data

Each side numbers the bytes it sends from offset 0. A DATA frame carries the bytes starting at
`offset`.

- The sender sends every DATA frame on **every** live link and keeps the bytes until they are
  acknowledged. It MUST NOT have more than `window` unacknowledged bytes, plus at most 65536 bytes of
  mux control frames (section 6), which never wait for the window.
- The receiver delivers bytes in order, each exactly once. A frame entirely below the delivered
  offset is a duplicate and is dropped. A frame partly below it delivers only its new tail.
- A frame above the delivered offset is kept in a **reorder buffer** until the gap is filled. The
  reorder buffer accepts only frames that end at most `window + 65536` bytes past the delivered
  offset, and holds at most `2 x window` bytes. A frame outside these bounds closes that link.

### 5.2 Acknowledgements

ACK carries the receiver's delivered offset: every byte below it has been delivered.

- ACK frames are sent only when the delivered offset has changed since the last ACK, on every live
  link. There are no periodic idle ACKs. The Go implementation flushes ACKs at most every 50 ms, or
  at once when a quarter of the window is unacknowledged.
- When a link attaches, each side sends its current delivered offset as an ACK on that link, so the
  new link starts with a known acknowledgement.
- An ACK beyond what the sender has sent closes that link.

### 5.3 Attaching a link and replay

When a link attaches to a live session (the first link, a new line, or a reconnect), the sender
queues the current ACK and then **replays** every unacknowledged byte on it, starting at the
acknowledged offset. No newer frame may reach that link before the replay: the Go sender builds
the replay into the link's queue and publishes the link in one critical section, so a concurrent
write can only follow the replay. A receiver that gets a frame ahead of a gap keeps it in the
reorder buffer, so even a reordering link cannot stall the session.

### 5.4 Liveness

The values are announced in WELCOME; the defaults are:

| Rule | Default |
|---|---|
| The client sends a PING round, one PING on every link at once, every `pingMs` | 500 ms |
| The server answers each PING with a PONG carrying the same nonce; it never sends PING itself | |
| Client: a link that has not answered the round sent `clientDeadMs` ago is soft down | 1000 ms |
| Client: when no link answers that round, the session is silent: every line is dialled again | 1000 ms |
| Client: a link with no inbound byte for max(5 s, 4 x its smoothed round trip) is dead | 5 s |
| Server: a link with no inbound byte for `serverDeadMs` is dead | 10000 ms |
| Progress: while a link carries sent-but-unacknowledged data, the ACK received on that link must advance within `progressMs`, else the link is dead | 10000 ms |

Only the client pings, because the client is the side that can act on a dead link (redial it), and
one ping stream per link is enough to prove both directions: the client hears the PONG, the server
hears the PING.

**Rounds and soft down.** Liveness is judged for the session, not link by link: a jittery line, or a
line on which one TCP segment was retransmitted, must not lose its link. A link **answers** a round
when any byte arrives on it after the round's PING was sent: the PONG itself waits behind every
byte the server queued on that link before it, so on a slow line carrying a download the data
arrives first. A link that has not answered the newest round sent at least `clientDeadMs` ago is
**soft down**: the client puts no new DATA on it, but keeps it attached, keeps pinging it and keeps
reading it. When it answers again, the client first queues on it every unacknowledged byte it was
not given meanwhile (from the offset after the last DATA it carried), then uses it like any other
link. Since every byte also goes out on every link that answers, and stays in the send buffer until
acknowledged, no byte is stranded on a soft link.

**A silent session.** When no attached link answers that round, the session is **silent**. The
client then dials every line again at once, with mode `join`, into the same session; a link that is
still attached stays until the new link with its index replaces it (section 4.2), and a line waiting
out a retry backoff dials at once. The new links receive the ACK and the replay (section 5.3), so a
short outage of every line (a network handover, a lift) costs no stream.

**Dead links.** The client closes a link after max(5 s, 4 x its smoothed round trip) without an
inbound byte. The round trip is measured from each PONG (its nonce is the time its PING was sent)
and smoothed as in RFC 6298 (weight 1/8); before the first PONG the dead time is 5 s. The server
closes a link after `serverDeadMs` without an inbound byte; that is long enough for a client to
notice a silent session and redial into it while its old links still hold the session.

The progress rule catches a link that carries bytes in one direction only, or whose data stopped
moving while control frames still flow. Its deadline covers more than a round trip: an ACK waits in
the line behind every byte queued ahead of it in that direction, so on a line that is both distant and
slow an ACK can take well over a second while the line works. Measured with one line of 600 to
900 ms round trip capped at 1 MB/s carrying a 2.6 MB download, a 1 s deadline ended the session
within about a second in half of the runs, with the Go and the JavaScript client alike (and with 256
KiB/s in every run); 3 s carried every run at 1 MB/s and 512 KB/s. The deadline must also be no
shorter than `serverDeadMs`: during an outage of every line the server's ACKs stop moving too, and
a shorter deadline would end the session on the server before the client has redialled into it.

A line whose round trip itself exceeds `clientDeadMs` is soft down for good: it carries data only
when no other line answers.

A dead link is closed. Each link has its own writer with a bounded send queue of `window + 131072`
bytes (enough for a full replay with its frame headers); a full queue, or a single write blocked for
longer than the side's dead time, closes only that link. A slow link never blocks the session or the
other links. A link's writer MAY skip DATA frames that have been acknowledged meanwhile, and SHOULD
drop them from a full queue before declaring it full, so that a slow link only carries bytes the
peer still lacks.

**Compatibility.** The wire is unchanged; only the default timings and the client's rules are. A
client that treats `clientDeadMs` as the silence after which a link is dead still works with these
defaults; it merely closes links sooner. A client following this section against a server that
announces shorter `serverDeadMs` and `progressMs` survives an outage of every line only as long as
that server keeps the session.

### 5.5 Lifecycle

- On the server, a session exists only while at least one of its links is alive. When the last link
  dies, the session **ends for good** there: every stream fails at once, and all of its state is
  released. There is no detached or orphaned session on the server and no grace period.
- The client keeps a session after its last link died, dialling every line into it, and ends it
  when the server shows it has lost the session (section 4.2), on GOODBYE, or when no link has
  received a byte for `serverDeadMs`: by then the server has ended it too. Its streams then fail at
  once.
- The client then starts a **new** session with a new id, over the line list it fetches again
  (section 2), or the list it kept while that fetch runs.
- GOODBYE ends a session explicitly. The client sends it on every link when it shuts down; the server
  sends it on every link when it shuts down, and the client then reconnects after the random delay of
  section 4.2. The receiver of a GOODBYE ends the session at once.
- A protocol violation in the session or mux layer (section 6) ends the session with a GOODBYE whose
  reason names the violation.

## 6. Streams (mux)

Mux frames are carried in the session byte stream, so they are reliable and ordered. Each is

```
type: u8 | length: u16 | stream: u32 | body: length - 4 bytes
```

| Type | Name | Body |
|---|---|---|
| 0x01 | SYN | empty: the client opens `stream` |
| 0x02 | DATA | 1 to 16384 bytes for `stream` |
| 0x03 | FIN | empty: the sender will send no more bytes on `stream` |
| 0x04 | RST | optional UTF-8 reason, at most 256 bytes: `stream` is aborted in both directions |
| 0x05 | WINDOW | `increment: u32`: the receiver grants the sender this many more bytes on `stream` |

- A mux frame is written to the session in one piece, so it travels as one DATA frame.
- Unknown mux frame types are skipped.
- Only the client opens streams. Stream ids are odd and strictly increasing within a session. A SYN
  from the server, or a SYN that is even or not above the previous id, is a violation.
- A stream starts with `streamWindow` bytes of credit in each direction. The sender MUST NOT send more
  DATA than its credit; the **receiver enforces it** and treats an excess as a violation. The
  receiver returns credit with WINDOW as the application consumes bytes.
- A receiver MAY grant more credit than it has consumed, at any time. Both clients grant each stream
  1 MiB in all, with a WINDOW right after its SYN. One stream moves at most its credit per round
  trip: with 256 KiB a 2.6 MB download over a line of 600 to 900 ms round trip takes about 9 s, with
  1 MiB about 2.7 s (2 MiB: 1.8 s). The grant costs the server nothing, since what it sends waits in
  its send buffer, bounded by `window`, whatever the credit; the credit the server grants
  (`streamWindow`), which bounds what it buffers for each stream, stays 256 KiB.
- SYN, FIN, RST and WINDOW are control frames: they are sent even when the session window is full
  (section 5.1).
- A stream is released when both sides have sent FIN, or when either side sends RST. The server
  answers a SYN beyond `maxStreams` open streams with RST reason `too many streams`.
- The server splices each accepted stream to its configured target over TCP. If the target cannot be
  reached, the stream is reset with the reason.

## 7. Limits

| Limit | Default | Bounds | Where enforced |
|---|---|---|---|
| `maxPayload` (frame payload) | 65535 | at least 32780 | both, per frame |
| `window` (session send window, reorder span) | 2 MiB | 256 KiB to 64 MiB | sender waits; receiver closes the link |
| reorder buffer | 2 x `window` | | receiver closes the link |
| link send queue | `window` + 128 KiB | | sender closes the link |
| `streamWindow` (mux credit) | 256 KiB | 64 KiB to 16 MiB | sender waits; receiver ends the session |
| client receive credit per stream | 1 MiB | at least `streamWindow` | client ends the session |
| `maxStreams` per session | 256 | 1 to 65535 | server resets the stream |
| `maxLinks` per session | 8 | 1 to 255 | server REJECTs |
| handshake | 5 s | | both close the link |
| client connection setup | 10 s | | client closes the connection |
| WebSocket message | `maxPayload` + 3 | | both close the link |

## 8. Resource principle

This is a requirement on every server implementation: **every server resource is bounded by the
number of live TCP connections times a constant**.

- A session needs at least one live link, and a link is one TCP connection; a session ends when its
  last link dies.
- Per link: one reader and one writer task, and at most `window + 128 KiB` of queued bytes.
- Per session: at most `window` of unacknowledged bytes, `2 x window` of reorder buffer, and at most
  `maxStreams` streams, each with at most `streamWindow` of received bytes and one target connection
  with one splice task per direction. There are no per-session threads or goroutines.
- All timers (pings, liveness, delayed ACKs) run on one shared timer for the whole process.
- A connection that has not finished its handshake is closed after 5 seconds.

## 9. Versioning

- This is protocol version 3. Version 2 (the HELLO2/REJECT frames, the service header and the
  `/mt/link` path of MultiPath 0.2) is a different protocol; there is no compatibility between the
  two inside the wire format.
- A version-3 server is served on its own link path (`/multipath/v3/link` by default), so a
  version-2 origin can keep running beside it on the old path while clients migrate.
- New frame types and new mux frame types can be added without a version change, because unknown
  types are skipped. A change to an existing frame's layout requires a new version and a new path.

## 10. Security

- **No open relay.** The server splices every stream to one configured target. Nothing in the
  protocol names a destination.
- **The server trusts nothing from the client except bytes.** Every length and offset is bounded
  (section 7), unknown sessions cannot be joined, and every limit is enforced by the receiver. The
  client address the server reports to the target (PROXY protocol, if enabled) comes from the HTTP
  upgrade request of the session's first link: `CF-Connecting-IP`, else the first
  `X-Forwarded-For` entry, else the socket peer. Those headers are only trustworthy when the server
  is reachable solely through a proxy that sets them; a deployment MUST NOT enable PROXY protocol
  otherwise.
- **Session ids are bearer secrets.** They are 16 random bytes and travel only inside the link
  (TLS-protected on `https` lines).
- **Line lists are replaced, not merged**, and come only from the configured entry; an `https`
  entry accepts only `https` lines (section 2).
- **An address does not widen where a line leads.** A line dialled at its `address` is still
  verified against its URL's host, which every policy on lines (the `https` rule, a browser's site)
  applies to, so a list cannot send a link to a server that does not hold that host's certificate.
- **Denial of service.** Every server resource is bounded per TCP connection (section 8); a
  connection that does not complete its handshake in 5 seconds, a link whose peer stops reading, and
  a link that exceeds any limit are all closed. Limiting the number of connections per address is
  left to the front proxy.
