# MultiPath testbed

A complete, self-contained end-to-end for MultiPath with **no business logic in it at all**, so the
library is proven over the wire — a real origin, a real client, real links being cut underneath —
without depending on any consumer.

That independence is the point. Verifying MultiPath by installing it into a consumer would make that
consumer's test suite the judge of MultiPath's correctness, and would leave MultiPath unable to
stage the situations that matter most: the same bytes arriving over two links at once, a link that
goes silent, a link that dies mid-stream. Those are trivial here and nearly impossible against a
real deployment.

```
                 ┌── middlebox ─ cut ──┐
Go client ───────┼── middlebox ─ cut ──┼──→  JVM origin  (one process)
   (Dial once,   └── middlebox ─ cut ──┘         demux → splice:
    N links)                                       normal → local app
                                                   tunnel → target
```

Each link is a separate path to the **same single origin process** — the assumption the whole design
rests on — and each is fronted by its own fault middlebox that black-holes, one-directionally cuts,
and hard-disconnects it on a random schedule. The substrate has to deliver correctly anyway.

## Parts

| | |
|---|---|
| origin | `app.microteams.multipath` (the `jvm/` package), run from `OriginMain`: accepts redundant streams, demuxes, reads the one-line header, splices a normal stream to a local app and a tunnel to its target. |
| client | The Go `Client` (`go/`): `Dial` once over all links, then `OpenTunnel` / `RoundTrip`. The connector's language. The TS and Dart clients drive the same origin and scenario too — TS under Node (`npx vitest run xlang`), Dart both natively (`link_io.dart`) and inside a real headless Chrome (`dart test -p chrome`, `link_web.dart` — the only way to actually exercise the browser-native L2 link rather than just compile it). |
| middlebox | One fault-injecting per-link TCP cutter (in the Go test package), the single implementation every client routes through. |
| scenario | One flow each client drives: open a tunnel and echo bytes, open a normal stream and round-trip HTTP. |

## Run it locally

```sh
testbed/run.sh    # build the origin + classpath, then run the cross-language e2e through the middlebox
```

It runs as ordinary processes — no containers. The cross-language legs are gated on `MP_JVM_CP`,
which `run.sh` builds from the JVM classpath; the same script is what CI's `e2e` job runs.

## Why cross-language, and why through faults

Compiling the Go and JVM halves proves nothing about whether they agree on the wire. The e2e drives
a real Go client against a real JVM origin so the redundant frames, the mux, and the L5 header must
agree byte for byte — and it does so *through the middlebox*, so "works" means "works while links are
being cut", which is the only claim the redundant primitive exists to make.
