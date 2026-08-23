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

import 'dart:async';

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

/// Where measurements are kept between visits.
///
/// Asynchronous, unlike the browser's `Storage`, because every Dart store worth using is —
/// `shared_preferences`, a file, a database. Worth wiring at all because the alternative for a cold
/// start is the registry's fixed order, which is a guess that never improves; from the second visit
/// on, the ranking begins from what was actually measured, which is the only way to tell a
/// stable-but-slow line from a fast one, since both answer a probe promptly.
abstract class HealthStore {
  const HealthStore();

  /// The last thing [save] wrote, or null on a first visit.
  Future<String?> load();

  Future<void> save(String encoded);
}

class LineManager {
  LineManager({
    required Registry registry,
    HealthOptions health = const HealthOptions(),
    this.strategy = const StrategyOptions(),
    this.onAttempt,
    SendProbe? send,
    ProberOptions probe = const ProberOptions(),
    HealthStore? storage,
    this.storageMaxAge = const Duration(days: 7),
    int attemptHistory = 100,
    DateTime Function()? now,
  })  : _registry = registry,
        health = HealthTable(health),
        _storage = storage,
        _historyLimit = attemptHistory,
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

  final HealthStore? _storage;

  /// Measurements older than this are ignored: last month's network says nothing about today's.
  final Duration storageMaxAge;

  final int _historyLimit;
  final List<AttemptReport> _history = [];

  Future<void>? _restored;

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

  /// Recent attempts, newest first.
  ///
  /// The developer panel's second half: health says what each line is like, this says what actually
  /// happened — which line served the last request, and how often each one wins. The two disagree
  /// more often than you would expect, and the disagreement is usually where the bug is.
  ///
  /// Bounded, because a diagnostic that grows without limit is a memory leak with good intentions.
  List<AttemptReport> get recentAttempts => List.unmodifiable(_history);

  /// Line ids in remembered preference order, for a launcher to use on the next cold start.
  ///
  /// The launcher races every line regardless; this only decides who is asked first among lines
  /// that are all reachable, which is the part racing cannot settle.
  List<String> get preferredLineIds => [for (final line in ranked) line.id];

  /// Seeds the table from the last visit. Safe to call more than once; only the first does work.
  ///
  /// Separate from construction, and a future, because reading a store is asynchronous here. A
  /// caller that does not await it loses nothing but the first few requests' head start.
  Future<void> restoreHealth() => _restored ??= _restoreHealth();

  Future<void> _restoreHealth() async {
    final storage = _storage;
    if (storage == null) return;
    try {
      health.import(
        PersistedHealth.decode(await storage.load()),
        storageMaxAge,
        _now(),
      );
    } catch (_) {
      // Corrupt or unavailable: start from nothing rather than from something misread. Never worth
      // failing an application's startup over a performance hint.
    }
  }

  /// Keeps what has been measured, so the next visit does not start from a guess.
  Future<void> saveHealth() async {
    final storage = _storage;
    if (storage == null) return;
    try {
      await storage.save(PersistedHealth.encode(health.export()));
    } catch (_) {
      // A full or unavailable store is not worth failing a request over.
    }
  }

  /// Begin measuring the lines.
  ///
  /// Separate from construction because probing costs real requests, and a library that starts
  /// making them the moment it is instantiated is one that surprises people. Until this is called
  /// the manager still works — it just ranks on configured weight, having measured nothing.
  void start() {
    // The seeding is only a head start, so it must not delay the measuring that will correct it.
    if (_storage != null) unawaited(restoreHealth());
    _prober?.start();
  }

  void stop() {
    _prober?.stop();
    // Written on the way out rather than after every probe: a browser tab can be discarded at any
    // moment, so this is a best effort either way, and writing on every sample would be a store
    // write every fifteen seconds for a hint.
    unawaited(saveHealth());
  }

  /// Probe every line now and wait for the answers. The panel's refresh button.
  Future<void> probeNow() async {
    await _prober?.probeAll();
    await saveHealth();
  }

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
    if (_historyLimit > 0) {
      _history.insert(0, report);
      if (_history.length > _historyLimit) _history.removeLast();
    }
    final observer = onAttempt;
    if (observer == null) return;
    try {
      observer(report);
    } catch (_) {
      // Deliberately swallowed. See the field's documentation.
    }
  }
}
