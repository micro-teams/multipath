# MultiPath — Go client

The same line management as [`../ts`](../ts), for connectors and CLIs: read the registry, rank the
lines, race reads, replay a failed write over the next line under the same idempotency key.

No package registry is involved — Go does not need one. Consume it by repository path and tag:

```sh
go get github.com/micro-teams/multipath/go@v0.1.0
```

## Use

```go
client := multipath.New(multipath.Options{Registry: registry})

client.Probe(ctx, "/mt/probe")            // measure; nothing is measured until you ask
response, err := client.Get(ctx, "/mt/x") // hedged read
response, err := client.Write(ctx, http.MethodPost, "/mt/x", body, key) // one line, then failover
```

Reads are hedged: the best line first, and only if it has not answered within `HedgeAfter` are the
others asked. Writes are never raced — one line, and only a transport failure moves the write to the
next, carrying the same idempotency key so the server recognises the second arrival as one attempt
rather than two writes. An error status is an answer rather than something to route around; only
silence leaves it unknown whether anything happened.

## Status

The registry, health table, ranking, probing, hedged reads and write failover are implemented, and
held to the same test cases as the TypeScript package — the connector and the browser have to mean
the same thing by "a line", and a behaviour only one of them checks is one they will eventually
disagree about. WebSocket line selection is still to come.

The TypeScript package leads and this one follows, deliberately. The registry JSON is the shared
contract — see [`../ts/src/registry.ts`](../ts/src/registry.ts).
