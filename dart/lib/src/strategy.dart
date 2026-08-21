/// Reads and writes are treated oppositely, and this is where that lives.
///
/// The mirror of `ts/src/strategy.ts` and the strategy half of `go/client.go`. The reasoning is the
/// same and worth having in front of you while reading the code:
///
/// A read can be repeated freely, because two copies of an answer are one answer. So it is HEDGED:
/// the best line is asked first, and if it has not answered within [StrategyOptions.hedgeAfter] the
/// rest are asked too and the first response wins. On a healthy line that budget is never spent and
/// no second request is ever sent — which is what makes hedging affordable, where always fanning
/// out would multiply every request by the number of lines to buy an improvement that only exists
/// on the slow tail.
///
/// A write cannot be repeated freely, so it is NEVER raced. It goes to one line; only a transport
/// failure moves it to the next, carrying the same idempotency key so the server recognises the
/// second arrival as the same attempt.
///
/// In both cases an error *status* is an answer, not a routing failure. A 404 hedged across every
/// line is still a 404, asked N times. A 500 means the request arrived and the server decided.
/// Only silence — no response at all — justifies another line, because only then is it unknown
/// whether anything happened. That distinction is the CALLER's to make here: this file routes on
/// whether the attempt future completed or threw, so an HTTP layer that turns a 500 into a thrown
/// error will get it retried across lines. Do not do that; see `README.md`.
///
/// Generic over the result, and given attempts as a callback, so the package keeps its zero
/// dependencies. What is being decided here is which line to send over and when to send again,
/// which is not a question about any particular HTTP library.
library;

import 'dart:async';

import 'registry.dart';

/// Runs one attempt of a request over one line.
///
/// [cancelled] completes when this attempt has lost a race and its result will be discarded. An
/// implementation that can abort in-flight work should listen to it; one that cannot may ignore it,
/// at the cost of some wasted bytes.
typedef Attempt<T> = Future<T> Function(Line line,
    {required Future<void> cancelled});

class StrategyOptions {
  const StrategyOptions({
    this.hedgeAfter = const Duration(milliseconds: 150),
    this.maxWriteAttempts = 3,
  });

  /// How long the best line gets alone before the rest are asked, for reads.
  final Duration hedgeAfter;

  /// How many lines a write may be sent to. A client that fails over forever hammers a struggling
  /// origin from every direction at once.
  final int maxWriteAttempts;
}

/// Thrown when the registry has nothing to send over.
class NoLineAvailable implements Exception {
  const NoLineAvailable();

  @override
  String toString() => 'multipath: no line available to serve the request';
}

/// Sends [attempt] over the best line, and over the rest if it is slow. First answer wins.
///
/// The losers are told they lost through their `cancelled` future and their results are dropped.
/// A line that fails IMMEDIATELY does not leave the request waiting out the hedge delay for company
/// it will never get — the next line starts at once.
Future<T> hedgedRead<T>(
  List<Line> lines,
  Attempt<T> attempt, {
  StrategyOptions options = const StrategyOptions(),
}) async {
  if (lines.isEmpty) throw const NoLineAvailable();

  final winner = Completer<T>();
  final cancellations = <Completer<void>>[];
  var launched = 0;
  var failures = 0;
  Object? lastError;
  StackTrace? lastStack;
  Timer? hedge;

  void cancelLosers(int keep) {
    for (var i = 0; i < cancellations.length; i++) {
      if (i != keep && !cancellations[i].isCompleted) {
        cancellations[i].complete();
      }
    }
  }

  void launch(int index) {
    final cancelled = Completer<void>();
    cancellations.add(cancelled);
    launched++;
    // Deliberately not awaited: the whole point is that several of these are in flight and only the
    // first to answer matters.
    unawaited(
      attempt(lines[index], cancelled: cancelled.future).then(
        (value) {
          if (winner.isCompleted) return;
          hedge?.cancel();
          cancelLosers(index);
          winner.complete(value);
        },
        onError: (Object error, StackTrace stack) {
          if (winner.isCompleted) return;
          lastError = error;
          lastStack = stack;
          failures++;
          if (failures == lines.length) {
            hedge?.cancel();
            winner.completeError(error, stack);
            return;
          }
          // Somebody has to answer, and this line will not. Do not wait out the hedge.
          if (launched < lines.length) launch(launched);
        },
      ),
    );
  }

  launch(0);
  if (lines.length > 1) {
    hedge = Timer(options.hedgeAfter, () {
      if (winner.isCompleted) return;
      while (launched < lines.length) {
        launch(launched);
      }
    });
  }

  try {
    return await winner.future;
  } catch (_) {
    // Rethrow with the original stack: the caller wants to know where the request came from, not
    // that it passed through here.
    Error.throwWithStackTrace(lastError!, lastStack ?? StackTrace.current);
  }
}

/// Sends [attempt] over one line at a time, moving on only when an attempt throws.
///
/// Never parallel, and never more than [StrategyOptions.maxWriteAttempts] lines. The idempotency
/// key is the caller's to mint ONCE for the logical write, before any line is chosen — that is
/// precisely what makes failover safe, and it is why this function does not mint one per attempt.
Future<T> writeWithFailover<T>(
  List<Line> lines,
  Attempt<T> attempt, {
  StrategyOptions options = const StrategyOptions(),
}) async {
  if (lines.isEmpty) throw const NoLineAvailable();

  final candidates = lines.length > options.maxWriteAttempts
      ? lines.sublist(0, options.maxWriteAttempts)
      : lines;

  Object? lastError;
  StackTrace? lastStack;
  final never = Completer<void>().future;
  for (final line in candidates) {
    try {
      return await attempt(line, cancelled: never);
    } catch (error, stack) {
      lastError = error;
      lastStack = stack;
    }
  }
  Error.throwWithStackTrace(lastError!, lastStack ?? StackTrace.current);
}
