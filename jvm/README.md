# MultiPath — Spring Boot starter

Absorbs a write that arrives more than once, so that a client free to retry over another network
path cannot make the same thing happen twice.

Add the dependency and duplicates stop reaching your controllers. **There is no step two** — no
annotation on the controller, no service to inject, no change to a single line of business code.
That is the design constraint, not a convenience: a controller that had to know about redundant
paths would have to be re-audited every time a path was added.

## How it works

A client attaches an `Idempotency-Key` header to each *logical* write, and reuses that key for
every transport-level retry of that same attempt. Around the handler:

- **First arrival wins the key** via an atomic `compute` on a `ConcurrentHashMap` — one indivisible
  step, so there is no window in which two simultaneous arrivals both decide they are first. It
  proceeds through the controller; on the way out its status and body are completed into a future.
- **Every later arrival awaits that future** and is served a byte-identical copy of the first
  answer. It never reaches the controller.

Replay rather than reject, because the duplicate is a real client that is really waiting: answering
it with "duplicate" would tell it the write failed when the write in fact succeeded.

## The status code is never consulted

A key names **one attempt**. Whatever the server said about that attempt — `200`, `400`, `500` —
is that attempt's outcome, and every later arrival of the same key is replayed exactly that.

The tempting refinement is to re-run "transient" failures: release the key on a 5xx so a retry
really executes. It is wrong, and not just extra code. Deciding that some failures deserve another
go is **retry policy**, and retry policy belongs to whoever mints keys, not to the layer that
de-duplicates them. A business-level retry — the user clicks again, the app retries — is a new
logical write carrying a new key, and it executes for real regardless of what happened before. The
same key reappearing means something quite different: the transport is retrying one attempt, over
another line, and turning that into a second execution is the exact thing this filter exists to
prevent.

Most 5xx reproduce anyway, so the complexity bought almost nothing even on its own terms.

### Async controllers

A controller returning `CompletableFuture` or `DeferredResult` promises an answer rather than
producing one, and `chain.doFilter` returns immediately. The filter therefore also runs on the
async dispatch and only settles the slot once the body is real. Getting this wrong is not subtle:
before it was handled, an async endpoint answered the *original* caller with an empty 200.

Two things still release a key, and neither reads a status:

- **The handler threw and produced no response at all.** There is nothing to replay, so the waiters
  are failed immediately rather than left blocked on an answer that is never coming.
- **The response exceeded `max-response-bytes`.** A question about heap, not about meaning.

(Decided by nictheboy, 2026-07-30, against an earlier version of this filter that split 2xx/4xx from
5xx. The earlier version also mis-classified 429 — which is how the argument started.)

Entries expire on a TTL that need only span the retry window — a few minutes. Capacity is write
rate × TTL, which is nothing.

## The assumption, stated once and loudly

**Exactly one backend instance.** The map lives in that JVM's heap. Two instances each keep their
own, and two paths terminating on different instances will both execute the write — de-duplication
silently stops working, which is the worst way for it to fail.

This is not an oversight; it is the same assumption that makes the whole approach cheap, and it
matches the premise that every path leads to one origin. If you ever run more than one instance,
this layer must move to shared storage (a Postgres uniqueness constraint, or Redis). `IdempotencyStore`
is an interface for exactly that reason: declare your own bean and the filter is unchanged. Do not
pre-emptively build one — but do not quietly cross the line either.

A restart drops the map. A duplicate arriving after a restart, inside its few-second race window,
would execute twice. The exposure is one occasional duplicate against a restart that happens rarely,
and it is not worth a database to close.

## Configuration

Everything has a working default; these exist because they are policy questions, not because
anyone should need to answer them.

```properties
multipath.idempotency.enabled=true
multipath.idempotency.header=Idempotency-Key
multipath.idempotency.ttl=5m
multipath.idempotency.methods=POST,PATCH,DELETE
multipath.idempotency.wait=30s
multipath.idempotency.max-response-bytes=1048576
```

`wait` must exceed the slowest guarded handler. A duplicate that waits it out is told to retry; it
is never let through, because "the first one is taking a while" is not a reason to execute a write
twice.

A replayed response carries `Idempotency-Replayed: true`.

## Status

**MP-2 is implemented.** 15 tests run against a real embedded server with real worker threads — not
MockMvc, whose single-threaded dispatch cannot fail the central claim. The race test was verified
by mutation: rewriting the store as check-then-claim makes it fail with two executions, which is
the only way to know a concurrency test is testing anything.

No database, no docker, seconds to run. Behaviour over genuinely separate network paths, from a
browser, is covered by [`../testbed`](../testbed).

## Build

```sh
./mvnw verify          # compiles, formats, runs tests
./mvnw spotless:apply  # formatting only
```
