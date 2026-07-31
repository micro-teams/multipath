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

## Status

MP-1, the client half of MP-2, and the first half of MP-3 are implemented: registry, health,
probing, ranking, resolution, the generated-client adapter, and idempotency keys on writes.
Read hedging and write failover slot into `LineManager.ranked` and `LineManager.dispatch` without
changing this API.

## Develop

```sh
npm install
npm test
npm run format && npm run lint && npm run build
```
