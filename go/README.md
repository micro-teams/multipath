# MultiPath — Go client

The same line management as [`../ts`](../ts), for connectors and CLIs: read the registry, rank the
lines, race reads, replay a failed write over the next line under the same idempotency key.

No package registry is involved — Go does not need one. Consume it by repository path and tag:

```sh
go get github.com/micro-teams/multipath/go@v0.1.0
```

## Status

The registry parser is implemented and tested; the line manager itself is **MP-3 / MP-4**; the TypeScript package leads and this one follows
it, deliberately, so that the two cannot drift into two different meanings of "a line". The registry
JSON is the shared contract — see [`../ts/src/registry.ts`](../ts/src/registry.ts).
