# MultiPath

**Transport resilience across redundant public network paths.** Put one origin behind several public
routes (different subdomains of one origin), open a link over every route at once, and carry every
exchange as a multiplexed stream over the lot. Every byte is written to every live link and delivered
once, from whichever link arrived first, so a link that dies is simply never the fastest: there is no
line to pick, no timeout to wait out, and no request to retry.

Every stream names a **service** the origin registered — there is no "main service" and no
client-chosen address, so a client can only ever reach a service the origin put in its registry.

MultiPath is a **library, not a service**, and it is product-agnostic.

| Package | What it is | Consumed as |
|---|---|---|
| [`go/`](./go) | The client (`Dial` once, then `Open(service)` / `RoundTrip(service, …)`) and the origin (`Serve`: demux and dispatch to a service registry). | `go get` the repo path + tag |
| [`jvm/`](./jvm) | The origin on the JVM (`Origin`: accept redundant streams, demux, dispatch), wire-compatible with the Go peer. | Maven (GitHub Packages) |

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
   header (L5) ─────┘   a service name + an opaque ticket, once at the start of each stream
```

The client `Dial`s once and opens a stream per exchange, each naming a service; the origin accepts
the redundant stream, demultiplexes it, reads the one-line header, looks the name up in its registry,
and hands the stream to that service's handler — an unregistered name is refused with a reason. A
handler may serve its stream in process (no listening port anywhere) or splice it to a real backend.
There is no request middleware, no idempotency, no coalescing: the redundant layer already delivered
each byte exactly once, so a handler sees each request exactly once.

## The one assumption everything rests on

**Every route leads to the same origin.** Routes are different *network paths*, not different
*replicas*. That single constraint is what makes MultiPath small: de-duplicating is "deliver each
byte once" inside one ordered stream, not distributed consensus. It is only worth using where it
holds.

## What it deliberately does not do

- Not load balancing, not geo-routing, not multi-master, not horizontal scale-out. The goal is
  **latency and availability redundancy**, not capacity. The cost is N× bandwidth by design.
- **No client-chosen destinations.** A client names a service; it cannot name an address. The
  reachable set is exactly what the origin registered, so there is no open-relay/SSRF surface. A link
  is server-authenticated TLS for confidentiality, not client identity; whether a given ticket may use
  a service is the handler's policy, carried in the opaque ticket — never in this library.
- **No business-code involvement.** A service handler that splices to the application reaches it as an
  ordinary connection. If using MultiPath requires editing a request handler, that is a bug in MultiPath.

## Proven over the wire

The Go and JVM halves are held to one wire format by a cross-language end-to-end: a real Go client
drives a real JVM origin through a per-link fault middlebox that black-holes, one-directionally cuts,
and hard-disconnects links — so "works" means "works while links are being cut", which is the only
claim the redundant primitive exists to make. See [`testbed/`](./testbed).

## License

MIT — see [LICENSE](./LICENSE). (The products in this org are AGPL; a library meant to be embedded is
not.)
