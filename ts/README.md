# MultiPath — TypeScript client

The line manager: the one place in an application where an outbound request chooses which network
path to leave by.

```sh
npm install @micro-teams/multipath
```

## Use

A `typescript-fetch` client builds absolute URLs from its `basePath`, and its source is regenerated
on every build so it cannot be patched. Point it at the sentinel origin and hand it the adapter:

```ts
import { Configuration } from "@/api";
import { LineManager, SENTINEL_ORIGIN, parseRegistry } from "@micro-teams/multipath";

const manager = new LineManager({
  registry: parseRegistry(await (await fetch("/mt/lines")).json()),
});

new Configuration({
  basePath: `${SENTINEL_ORIGIN}/mt`,
  fetchApi: manager.fetchApi(),
});
```

`SENTINEL_ORIGIN` is `https://multipath.invalid` — `.invalid` is reserved by RFC 2606 and resolves
nowhere, so a request that ever escaped the adapter fails loudly instead of quietly reaching some
real host.

With no registry at all, the manager routes over a single same-origin line, which means it emits
the exact relative URL a plain `fetch` would have. That is the intended way to adopt it: route
everything through the manager first and change nothing observable, then add lines to the registry
once the plumbing is proven.

### `fetchApi` is the only way in — on purpose

There is no `manager.request(path)`. A request reaches a line only by coming from a client
generated from the API contract, which means **an endpoint has to be in the contract before it can
have redundancy**. Hand-rolled `fetch` calls are excluded by construction rather than by a rule in
a document, because a rule in a document erodes and a private method does not.

Adopting MultiPath in an app that has drifted away from its contract therefore surfaces that drift
as work: the hand-written calls have to come back to the contract first. That is the intended
pressure, not a side effect.

## Observing it

`onAttempt` fires after every attempt with the line, method, path, duration and outcome. It is the
data behind the developer line panel, and it is strictly observational: an `onAttempt` that throws
is swallowed, because a debugging aid must never be able to break the app it observes.

## Measuring the lines

Nothing is measured until you say so, because probing costs real requests and a library that starts
making them on construction is one that surprises people:

```ts
manager.start();          // begin measuring in the background
await manager.probeNow(); // or measure once, and wait
manager.health();         // what is known about every line
manager.ranked();         // lines best-first
```

Two measurements, because one is not enough and the second is not free. Round-trip time is cheap,
so it runs constantly and drives the ranking. Throughput is not, so it runs rarely and only after
the application has been quiet — and it matters because the problem this library exists for is a
line that is *slow*, not one that is far: an edge can answer a probe in 20ms and still take seconds
to deliver a bundle.

A failing line backs off exponentially, so a dead route costs a request every few minutes rather
than every few seconds. A line that is down is ranked last but never removed — if everything is
down the caller still has to send the request somewhere, and refusing to try is worse than trying
the least-bad option.

Measurement outranks configuration: `weight` breaks ties between lines that have not been
distinguished by measurement, and nothing more. A hand-set number goes stale; an average does not.

## Reads and writes are treated oppositely

A read can be repeated freely, because two copies of an answer are one answer. So it is **hedged**:
the best line is asked first, and if it has not answered within `hedgeAfterMs` (150ms by default)
the rest are asked too and the first response wins. On a healthy line that budget is never spent
and no second request is ever sent — which is what makes hedging affordable, where always fanning
out would multiply every request by the number of lines to buy an improvement that only exists on
the slow tail.

A write cannot be repeated freely, so it is **never raced**. It goes to one line; only a transport
failure moves it to the next, carrying the same idempotency key so the server recognises the second
arrival as the same attempt.

In both cases an error *status* is an answer, not a routing failure. A 404 hedged across every line
is still a 404, asked N times. A 500 means the request arrived and the server decided; whether to
retry that is the caller's business, not the transport's. Only silence — no response at all —
justifies another line, because only then is it unknown whether anything happened.

## Starting the app without the network

Two pieces make launching independent of the lines.

`createPrecache` is the Service Worker runtime. Give it the manifest your build emits and a version,
and it stores every artefact on install, serves them cache-first, and deletes old versions once the
new one is in charge. A cache miss still goes over the lines, because the assets are on all of them.
It never answers an API request: staleness in data is the application's business, and a transport
layer quietly returning yesterday's data would be lying about what it is.

`buildLauncher` produces one small self-contained HTML document. There is exactly one moment that
cannot be spread across lines — a browser opening a URL knows one host — so that document is made as
small as possible and does only three things: register the worker, carry the registry inline, and
import the real entry point. The registry is inlined rather than fetched, because fetching it would
put a round trip on the one path with no redundancy, and failing there would leave the app unable to
reach any line, having never learned that the lines exist.

```ts
// your build step
writeFileSync("dist/index.html", buildLauncher({
  appEntry: "/assets/main-abc123.js",
  serviceWorker: "/sw.js",
  serviceWorkerType: "module",
  registry,
  registryUrl: "/mt/lines",
}));
```

Registration is fire-and-forget: the app starts whether or not the worker installs. A launcher that
waited for the cache would have made the cache a prerequisite for starting, which is the opposite of
the point.

`appEntry` is a **path**, not a URL, because the launcher races it across the lines. Every line is
asked at once, with no stagger and no head start for whichever is listed first — a dead, blocked or
unsupported line therefore costs nothing at all, because nobody was waiting on it.

The winner is whichever line **finishes delivering**, not whichever answers first. That distinction
is the whole point: a stable edge can return headers in 20ms and still take seconds to hand over a
megabyte, and picking on first byte would choose it every time — the exact outcome this library
exists to avoid. The losers are aborted the moment somebody finishes. The cost is some duplicated
data; the bottleneck is the line rather than the client's connection, so the copies do not
meaningfully compete for it.

The winner is imported by URL rather than executed from the fetched bytes. A module built from a
blob has the blob as its base URL, so every relative chunk import in a code-split application would
resolve to nowhere; importing from the winning line keeps module semantics exactly as the bundler
intended, and its chunks continue to come from that same line.

`LineManager` can persist what it measures (`storage`), so the *second* visit onward starts from
measurements rather than from the registry's fixed order. Racing settles the entry point on its own;
persistence matters for everything after it, which is hedged rather than raced and so does care
which line is tried first.

Only the very first HTML document cannot be raced — a browser opening a URL knows one host. That one
request is why the launcher is kept small, and after the worker installs even it comes from cache.

## Seeing what it is doing

Everything here is invisible by construction — requests leave over whichever line is winning,
duplicates are absorbed before any controller sees them. That is the point, and it is also what
makes it hard to trust: when it works there is nothing to see, and when it misbehaves there is also
nothing to see.

```ts
import { mountLinePanel } from "@micro-teams/multipath";

const unmount = mountLinePanel(manager, document.getElementById("panel")!);
```

Put it behind a route nothing links to. It shows what each line is *like* — rank, state, measured
latency and throughput, last error — beside what actually *happened*: which line served each recent
request, and the share each one won. Those two disagree more often than you would expect, and the
disagreement is usually where the bug is.

Plain DOM, no framework, styles scoped to the panel. A diagnostic that drags in dependencies is one
that gets excluded from the production build, which is exactly where it is needed.

## Last time's answer, beside this time's

```ts
const call = manager.cached(() => chatApi().listChats(args));
paint(await call.cached);   // what the identical request returned last time, or null
paint(await call);          // this request's real result
```

The request always goes out and the awaited value is always **this** request's result. A failure
stays a failure; it never quietly becomes stale data wearing a success. That is the difference from
a browser cache, which answers instead of asking — the remembered answer is offered beside the truth
so the caller can paint with it while waiting, never in place of it.

A thenable rather than a `{ cached, fresh }` pair, so call sites keep their shape: anything that
does not want optimistic rendering ignores `cached` and behaves exactly as before.

Keyed by the request — method and path, with the line's origin deliberately excluded, since the same
resource fetched over two lines is the same resource. Nothing here decides *when* to forget: what a
write makes stale is business knowledge, so the cache offers invalidation by request shape and
leaves the timing to the application. Tenants are separated by an opaque `scope` string, so this
layer never learns what a "user" is.

## Status

MP-1 through MP-6 are implemented on the client: registry, health, probing, ranking, hedged reads,
write failover, precache, launcher, the developer panel, the request cache, the generated-client
adapter, and idempotency keys. Still to come: the Go connector catching up, and WebSocket line
selection.

## Develop

```sh
npm install
npm test
npm run format && npm run lint && npm run build
```
