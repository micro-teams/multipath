/// Choosing a line for a stream, and choosing again when it breaks.
///
/// The mirror of `ts/src/socket.ts` and `go/socket.go`, and the same ceiling: a stream cannot be
/// raced, because two connections are two conversations, each with its own state. So the most that
/// is possible is pick the best line, and when it breaks, pick again.
///
/// The part that is easy to miss is that HTTP health says almost nothing about whether a line can
/// carry a stream. A cheap reverse proxy will serve requests perfectly and refuse the Upgrade; a
/// middlebox will allow the handshake and then sever anything long-lived. So a line's ability to
/// hold a stream is remembered SEPARATELY from its latency, and a line that fails at this is
/// skipped for streams while remaining perfectly good for requests.
///
/// This is the policy alone, with no reconnect loop, because the consumers that need it already own
/// one: a transport that has to speak a handshake, send heartbeats and decide what a drop means has
/// a loop whose shape belongs to that protocol. What it still needs is this — which line to dial
/// next, and somewhere to say how the last attempt went.
library;

import 'registry.dart';

class StreamSelector {
  StreamSelector({
    required List<Line> Function() lines,
    this.stableAfter = const Duration(seconds: 5),
    this.penalty = const Duration(seconds: 60),
    DateTime Function()? now,
  })  : _lines = lines,
        _now = now ?? DateTime.now;

  /// Read afresh before every attempt, so a re-ranking takes effect on the next reconnect rather
  /// than at the next restart.
  final List<Line> Function() _lines;

  /// How long a connection must last before it counts as working.
  ///
  /// Without it, a line that accepts the handshake and drops it immediately looks like a success
  /// every time and the client reconnects in a tight loop forever.
  final Duration stableAfter;

  /// How long a line is skipped for streams after failing to hold one.
  ///
  /// Long enough that a proxy which cannot do WebSockets stops being tried every few seconds, short
  /// enough that a line merely having a bad minute comes back into rotation.
  final Duration penalty;

  final DateTime Function() _now;

  final Map<String, DateTime> _penalties = {};
  Line? _current;

  /// Which line is carrying the stream, or null between attempts.
  Line? get current => _current;

  /// Lines recently unable to hold a stream, and until when.
  Map<String, DateTime> get penalties => Map.unmodifiable(_penalties);

  /// The line to dial, or null when there is nothing to dial at all.
  ///
  /// If every line is serving a penalty the least-recently-penalised is returned anyway: a client
  /// with no connection is worse than one on a flaky connection, and the penalties may all be
  /// stale.
  Line? next() {
    final candidates = _lines();
    if (candidates.isEmpty) return null;

    final at = _now();
    for (final candidate in candidates) {
      final until = _penalties[candidate.id];
      if (until == null || !until.isAfter(at)) return candidate;
    }

    var best = candidates.first;
    for (final candidate in candidates.skip(1)) {
      if (_penalties[candidate.id]!.isBefore(_penalties[best.id]!)) {
        best = candidate;
      }
    }
    return best;
  }

  /// Records that a connection to [line] is now up.
  void opened(Line line) => _current = line;

  /// Records how an attempt ended, and is where a line earns a penalty.
  ///
  /// [held] is how long the connection lasted — [Duration.zero] if it never opened. A connection
  /// that did not last is evidence about this line's ability to carry a stream, which is a
  /// different question from whether it answers requests quickly. One that lasted and then dropped
  /// is an ordinary disconnection and is not held against it, or every line would slowly be
  /// penalised for the network being a network.
  void closed(Line line, Duration held) {
    _current = null;
    if (held < stableAfter) {
      _penalties[line.id] = _now().add(penalty);
    }
  }
}
