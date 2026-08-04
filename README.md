# MultiPath

**Transport resilience across redundant public network paths.** Put one service behind several
public routes (different subdomains of one origin), let the client connect over all of them, race
reads and take the first answer, and de-duplicate writes so that a request arriving twice over two
paths takes effect exactly once.

MultiPath is a **library, not a service**, and it is product-agnostic. It ships three packages that
speak one shared vocabulary:

| Package | What it is | Consumed as |
|---|---|---|
| [`ts/`](./ts) | **Line manager** — probing, latency EWMA, read hedging, failover, idempotency keys. Takes over the app's outbound requests. | npm (GitHub Packages) |
| [`jvm/`](./jvm) | **Idempotency interceptor** — a Spring Boot starter that absorbs duplicate writes *before* they reach a controller. | Maven (GitHub Packages) |
| [`go/`](./go) | Same line management for connectors / CLIs. | `go get` the repo path + tag |

## The one assumption everything rests on

**Every line leads to the same origin** — the same backend process, the same database. Lines are
different *network paths*, not different *replicas*.

That single constraint is what makes MultiPath small. De-duplicating a write is not distributed
consensus; it is "first writer wins" inside one process. Racing a read is not a consistency problem;
it is two copies of one answer. Everything hard about multi-homing went away with that assumption,
and MultiPath is only worth using where it holds.

Corollary, stated plainly because it is easy to violate by accident: **the JVM package's in-memory
de-duplication requires exactly one backend instance.** Two instances each keep their own map and
two lines terminating on different instances will both execute the write. Running more than one
instance means moving that layer to shared storage — see [`jvm/README.md`](./jvm/README.md).

## What it deliberately does not do

- Not load balancing, not geo-routing, not multi-master, not horizontal scale-out. The goal is
  **latency and availability redundancy**, not capacity.
- No cross-line consistency guarantees beyond "one logical write takes effect once".
- **No business-code involvement.** Racing lives in the client; de-duplication lives in an
  interceptor. A controller never learns that more than one line exists. If using MultiPath
  requires editing a controller, that is a bug in MultiPath.

## Request strategy

| Kind | Strategy |
|---|---|
| **Reads** (GET) | **Hedge.** Send to the fastest line; if it has not answered within `T` ms, fan out to the rest, take the first answer, abort the others. Hedging rather than always fanning out, because always fanning out multiplies bandwidth by the number of lines for a benefit you only need on the slow tail. |
| **Writes** (POST/PATCH/DELETE) | **One line + an idempotency key.** Only on timeout or failure is the *same key* replayed over the next line. Concurrency is the fallback, not the default; the key is what makes the fallback safe. |
| **WebSocket / streams** | Stateful, so not raceable. Pick one line; on disconnect, reconnect down the ranking. |

## Layout

```
ts/     TypeScript line manager        (vitest)
jvm/    Kotlin Spring Boot starter     (Maven, spotless/ktfmt, MockMvc tests — no database)
go/     Go line manager                (go test)
```

Each subdirectory builds, tests and versions independently; see its own README.

## Status

**0.1.1**, and in production: MicroTeams runs on it, over a single line today and a second one being
brought up. Everything the design specifies for the client and the connector is implemented —
registry, health and ranking, probing, hedged reads, write failover, stream line selection, the
Spring idempotency filter, the service worker and launcher, the request cache, and the developer
panel.

0.x means the interfaces may still move. What has been shaken out by a real consumer is worth
saying plainly: a JSON `null` on an optional registry field is treated as absent (0.1.1) — before
that, a registry produced by an ordinary serializer was rejected whole and the client silently fell
back to one line.

## License

MIT — see [LICENSE](./LICENSE). (The products in this org are AGPL; a library meant to be embedded
is not, matching `cheese-auth`.)
