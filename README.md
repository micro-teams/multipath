# MultiPath

**Transport resilience across redundant public network paths.** Put one origin behind several public
routes (different subdomains of one origin), open a link over every route at once, and carry
everything — application requests, WebSockets, opaque tunnels — as multiplexed streams over the
lot. Every byte is written to every live link and delivered once, from whichever link arrived first,
so a link that dies is simply never the fastest: there is no line to pick, no timeout to wait out,
and no request to retry.

MultiPath is a **library, not a service**, and it is product-agnostic.

| Package | What it is | Consumed as |
|---|---|---|
| [`go/`](./go) | The client (`Dial` once, then `OpenTunnel` / `OpenNormal` / `RoundTrip`) and the origin (`Serve`: demux and splice). | `go get` the repo path + tag |
| [`jvm/`](./jvm) | The origin on the JVM (`Origin`: accept redundant streams, demux, splice), wire-compatible with the Go peer. | Maven (GitHub Packages) |

## The one idea

Send on every path at once, and de-duplicate at the receiver. That is the whole library, applied at
one level:

```
                 registry ── the routes to the origin (a shared JSON document)
                    │
   link  (L2) ──────┤   one duplex link per route: TLS direct, WebSocket over a CDN, or plaintext
                    │
   redundant (L3) ──┤   N links → one reliable, ordered, never-interrupted byte stream
                    │   (every byte on every link; delivered once, in order, by offset; reconnects
                    │    resume with no gap; a send window bounds it)
                    │
   mux  (L4) ───────┤   many logical streams over the one redundant stream
                    │
   header (L5) ─────┘   kind + target + an opaque egress ticket, once at the start of each stream
```

The client `Dial`s once and opens a stream per exchange; the origin accepts the redundant stream,
demultiplexes it, reads the one-line header, and splices — a **normal** stream to its own service on
loopback (the application stays an ordinary server that never learns a line existed), a **tunnel** to
its target once the consumer's policy allows it. There is no request middleware, no idempotency, no
coalescing: the redundant layer already delivered each byte exactly once, so the origin sees each
request exactly once.

## The one assumption everything rests on

**Every route leads to the same origin.** Routes are different *network paths*, not different
*replicas*. That single constraint is what makes MultiPath small: de-duplicating is "deliver each
byte once" inside one ordered stream, not distributed consensus. It is only worth using where it
holds.

## What it deliberately does not do

- Not load balancing, not geo-routing, not multi-master, not horizontal scale-out. The goal is
  **latency and availability redundancy**, not capacity. The cost is N× bandwidth by design.
- **No identity in the transport.** Opening a link and opening a stream require nothing; a link is
  server-authenticated TLS for confidentiality, not client identity. Who may reach a tunnel's target
  is the consumer's policy, carried in the opaque ticket and enforced in its own handler — never in
  this library.
- **No business-code involvement.** A normal stream reaches the application as an ordinary
  connection. If using MultiPath requires editing a request handler, that is a bug in MultiPath.

## Proven over the wire

The Go and JVM halves are held to one wire format by a cross-language end-to-end: a real Go client
drives a real JVM origin through a per-link fault middlebox that black-holes, one-directionally cuts,
and hard-disconnects links — so "works" means "works while links are being cut", which is the only
claim the redundant primitive exists to make. See [`testbed/`](./testbed).

## License

MIT — see [LICENSE](./LICENSE). (The products in this org are AGPL; a library meant to be embedded is
not.)
