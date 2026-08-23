# MultiPath — Dart line manager

The registry, what is known about each line, and the two strategies that follow from treating reads
and writes oppositely. For Flutter clients and Dart command-line tools.

**No dependencies, and no transport.** This package does not send anything. You supply the attempt;
it decides which line to send over, when to send again, and what to learn from the result. That is
what lets one package serve a Flutter app on Dio, a CLI on `package:http`, and a test on a function
that returns a canned answer — and it is why there is no `dio` in the pubspec.

```yaml
dependencies:
  multipath:
    git:
      url: https://github.com/micro-teams/multipath.git
      path: dart
      ref: dart-v0.1.1
```

## Use

```dart
final manager = LineManager(
  registry: parseRegistry(await fetchLineRegistry()),
);

// A read: hedged across the ranked lines, first answer wins.
final response = await manager.read((line, {required cancelled}) async {
  return send(line.resolve('/mt/chat'), cancelToken: cancelToken(cancelled));
});

// A write: one line at a time, one key for the whole logical write.
final key = newIdempotencyKey();
await manager.write((line, {required cancelled}) async {
  return send(
    line.resolve('/mt/chat/1/messages'),
    headers: {idempotencyHeader: key},
    body: body,
  );
});
```

With a single same-origin line — `{"lines":[{"id":"origin","url":""}]}` — `line.resolve(path)`
returns the path unchanged, so adoption changes nothing observable. That is the intended way in:
route everything through the manager first while the routing decision is still trivial, then add
lines to the registry once the plumbing is proven. Doing it the other way round introduces the
plumbing and the risk on the same day.

## The one thing to get right

**Only silence justifies another line.** An error *status* is an answer: a 404 hedged across every
line is still a 404 asked N times, and a 500 means the request arrived and the server decided.

This package routes on whether your attempt **completed or threw**, so an HTTP layer that converts
non-2xx into a thrown exception will get those retried across every line — including writes, which
is the one place it actually costs something. Return the response for any status you received, and
throw only when nothing came back.

## The cache is offered alongside the answer, never in place of it

`RequestCache` stores what an identical request returned last time, keyed by method and path with
the origin stripped — the same resource over two lines is the same resource, and keying by full URL
would give every line its own cache and lose most hits the moment a second line existed.

The boundary is the whole design: **the request always goes out**, and what you await is always
this request's result. A failure is a failure; it never quietly becomes stale data wearing a
success. What the cache is for is painting something while you wait.

```dart
final key = RequestCache.keyFor('GET', '/mt/chat');
final previous = cache.get<List<Object?>>(key);   // paint this now, if you like
final fresh = await manager.read(...);            // and this is the answer
cache.set(key, fresh);
```

Nothing here decides *when* to forget. What a write makes stale is business knowledge; this layer
knows only that two requests looked identical. It offers `invalidate(prefix)` and leaves the timing
to the application.

`setScope` is how one account never paints another's data. The marker is opaque — this layer must
not learn what a "user" is — and changing it DROPS the old entries rather than hiding them.

One addition over the TypeScript version: an optional `CacheStore`, so a client that is killed and
reopened constantly can paint before the network answers. It is an interface, not an
implementation: a Flutter app backs it with shared_preferences, a CLI with a file, a test with
nothing at all.

## Streams

A stream cannot be raced: two connections are two conversations, each with its own state. So the
most that is possible is pick the best line, and when it breaks, pick again — [`StreamSelector`].

It keeps its own memory, separate from latency, because HTTP health says almost nothing about
whether a line can carry a stream: a cheap reverse proxy will serve requests perfectly and refuse
the Upgrade, and a middlebox will allow the handshake and then sever anything long-lived. A line
that fails at holding a stream is skipped for streams and stays perfectly good for requests.

There is no reconnect loop here, on purpose. Consumers that need this already own one — a transport
that speaks a handshake, sends heartbeats and decides what a drop means has a loop whose shape
belongs to that protocol. What it needs from us is which line to dial next.

## What is deliberately not here

**The launcher and the service worker.** Those are `ts/`'s, and they are browser-specific: racing
the entry *document* across lines is a problem only a page has. Worth stating for Flutter
specifically — a native build is installed, so it has no entry-document problem at all and needs
lines only for requests and streams. Flutter **web** does have one, but its entry is
`flutter_bootstrap.js` plus the engine, which is not a single module that can be swapped the way a
bundler's entry chunk can.

**A developer panel.** `ts/`'s `mountLinePanel` draws DOM. `onAttempt` and `recentAttempts` give
you the same data; drawing it is the consumer's business.

## Measuring, and remembering

`start()` begins the probe loop and `stop()` ends it — nothing is measured until you ask, because a
library that starts making network requests the moment it is constructed is one that surprises
people. `probeNow()` measures every line and waits, which is what a refresh button wants.

Give it a `storage` and the ranking survives the visit:

```dart
final manager = LineManager(
  registry: parseRegistry(await fetchLineRegistry()),
  send: probeSender(origin: origin),
  storage: PrefsHealthStore(await SharedPreferences.getInstance()),
);
await manager.restoreHealth();   // optional: without it, start() seeds in the background
manager.start();
```

Worth doing because the alternative for a cold start is the registry's fixed order, which is a guess
that never improves — and it is the only way to tell a stable-but-slow line from a fast one, since
both answer a probe promptly. Only the measurements are kept, never the states: a line that was
unreachable on a train yesterday must not start today demoted. Anything older than `storageMaxAge`
(a week) is ignored, because last month's network says nothing about today's. The record is
milliseconds-on-the-wire, the same shape `ts/` writes, so the two clients can read each other's.

`preferredLineIds` is that memory in the form a launcher wants: the launcher races every line
regardless, and this only decides who is asked first among lines that are all reachable — which is
the part racing cannot settle.

## Kept in step with the others

Everything here mirrors `ts/` and `go/` down to the defaults, because every client has to mean the
same thing by "a line" — an app that ranked lines differently from the connector would make "which
line is slow" a question with two answers. The tests are written against the same cases for the same
reason: a rule tightened in one parser and not the others is a disagreement that surfaces only in
production, on whichever client happens to be strictest.

One thing this implementation needs that the others get for free: `List.sort` in Dart is **not
stable**, so `HealthTable.rank` uses its own merge sort. Without it, two lines that no measurement
distinguishes swap places between calls — which looks like traffic moving for no reason.

## Develop

```sh
dart pub get
dart format .
dart analyze --fatal-infos
dart test
```
