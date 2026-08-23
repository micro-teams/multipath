/// The one place in an application where an outbound request chooses which network path to leave
/// by.
///
/// The Dart equivalent of `LineManager` (TypeScript) and `Client` (Go), minus the HTTP: this
/// package has no dependencies and no opinion about how a request is actually sent, so the manager
/// holds the registry, the health table and the strategies, and the consumer supplies the attempt.
/// The Flutter client wires this to Dio; a CLI could wire it to `package:http`; a test wires it to
/// a function that returns a canned answer.
///
/// Nothing is measured until [probe] is called. A library that starts making network requests the
/// moment it is constructed is one that surprises people.
library;

import 'health.dart';
import 'prober.dart';
import 'registry.dart';
import 'strategy.dart';

/// One attempt, as reported to [LineManager.onAttempt]. Strictly observational.
class AttemptReport {
  const AttemptReport({
    required this.line,
    required this.duration,
    required this.ok,
    this.error,
  });

  final Line line;
  final Duration duration;
  final bool ok;
  final Object? error;
}

class LineManager {
  LineManager({
    required Registry registry,
    HealthOptions health = const HealthOptions(),
    this.strategy = const StrategyOptions(),
    this.onAttempt,
    SendProbe? send,
    ProberOptions probe = const ProberOptions(),
    DateTime Function()? now,
  })  : _registry = registry,
        health = HealthTable(health),
        _now = now ?? DateTime.now {
    // Only if the consumer said how to send one. Without it the manager still works — it just ranks
    // on configured weight, having measured nothing, which is what the Dart port did for everybody
    // until now.
    if (send != null) {
      _prober = Prober(
        lines: () => _registry.lines,
        health: this.health,
        send: send,
        resolve: (path, line) => line.resolve(path),
        options: probe,
        now: _now,
      );
    }
  }

  Prober? _prober;

  Registry _registry;
  final HealthTable health;
  final StrategyOptions strategy;
  final DateTime Function() _now;

  /// Fires after every attempt. A debugging aid must never be able to break the app it observes, so
  /// anything this throws is swallowed.
  final void Function(AttemptReport)? onAttempt;

  Registry get registry => _registry;

  /// The lines in their configured order.
  List<Line> get lines => List.unmodifiable(_registry.lines);

  /// The lines best-first.
  List<Line> get ranked => health.rank(_registry.lines);

  /// Begin measuring the lines.
  ///
  /// Separate from construction because probing costs real requests, and a library that starts
  /// making them the moment it is instantiated is one that surprises people. Until this is called
  /// the manager still works — it just ranks on configured weight, having measured nothing.
  void start() => _prober?.start();

  void stop() => _prober?.stop();

  /// Probe every line now and wait for the answers. The panel's refresh button.
  Future<void> probeNow() async => _prober?.probeAll();

  /// Swaps the lines at runtime, forgetting health for lines that have gone.
  set registry(Registry next) {
    _registry = next;
    health.retain(next.ids);
  }

  /// A hedged read over the ranked lines. See [hedgedRead].
  Future<T> read<T>(Attempt<T> attempt) =>
      hedgedRead(ranked, _observed(attempt), options: strategy);

  /// A write over one line at a time. See [writeWithFailover].
  ///
  /// The idempotency key belongs to the logical write and must be minted by the caller ONCE,
  /// outside this call — every attempt carries the same key, which is what makes failover safe.
  Future<T> write<T>(Attempt<T> attempt) =>
      writeWithFailover(ranked, _observed(attempt), options: strategy);

  /// Measures every line once and updates the ranking.
  ///
  /// [probeLine] decides for itself what an answer means, and it must: a 500 is a perfectly prompt
  /// reply, and a probe that recorded it as a success would leave a broken line ranked first. It
  /// returns normally for a healthy line and throws for an unhealthy one.
  Future<void> probe(Future<void> Function(Line line) probeLine) async {
    await Future.wait([
      for (final line in _registry.lines)
        () async {
          final started = _now();
          try {
            await probeLine(line);
            health.recordSuccess(line.id, _now().difference(started), _now());
          } catch (error) {
            health.recordFailure(line.id, error, _now());
          }
        }(),
    ]);
    health.reconcileDegraded();
  }

  /// Wraps an attempt so that every send is timed, recorded and reported.
  ///
  /// Recorded on the same table the probes write to, on purpose: real traffic is a better and
  /// cheaper measurement than a synthetic probe, and a client that only learned from probes would
  /// keep sending over a line it has watched fail all morning.
  Attempt<T> _observed<T>(Attempt<T> attempt) {
    return (Line line, {required Future<void> cancelled}) async {
      // The throughput probe downloads real bytes, so it must not run while the application is
      // using the pipe — it would be measuring itself competing with the user.
      _prober?.noteTraffic();
      final started = _now();
      try {
        final value = await attempt(line, cancelled: cancelled);
        final took = _now().difference(started);
        health.recordSuccess(line.id, took, _now());
        _report(AttemptReport(line: line, duration: took, ok: true));
        return value;
      } catch (error) {
        final took = _now().difference(started);
        health.recordFailure(line.id, error, _now());
        _report(
          AttemptReport(
            line: line,
            duration: took,
            ok: false,
            error: error,
          ),
        );
        rethrow;
      }
    };
  }

  void _report(AttemptReport report) {
    final observer = onAttempt;
    if (observer == null) return;
    try {
      observer(report);
    } catch (_) {
      // Deliberately swallowed. See the field's documentation.
    }
  }
}
