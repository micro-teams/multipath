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

## Status

MP-1 and the client half of MP-2 are implemented: registry, validation, selection, resolution, the
generated-client adapter, and idempotency keys on writes.
Probing, read hedging and write failover (MP-3 / MP-4) slot into `LineManager.select` and
`LineManager.dispatch` without changing this API.

## Develop

```sh
npm install
npm test
npm run format && npm run lint && npm run build
```
