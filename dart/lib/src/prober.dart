/*
 *  Description: The measurement loop for Dart, ported from ts/src/prober.ts.
 *
 *               Two rules shape everything here, and they are the TypeScript ones because a client
 *               that measured its lines differently would rank them differently, and two clients
 *               disagreeing about which line is fastest is worse than neither measuring at all.
 *
 *               A probe must not distort what it measures. Latency probes are tiny and spaced out;
 *               the throughput probe downloads real bytes, so it only runs when the application has
 *               been quiet for a while — measuring bandwidth during a burst of user traffic would
 *               compete with that traffic and report a number caused by the measurement itself.
 *
 *               A failing line must not be probed at the same rate as a healthy one. A line that is
 *               down gets exponential backoff, so a dead route costs a request every few minutes
 *               rather than every few seconds.
 *
 *               Why this had to exist: the Dart port shipped with a one-shot `probe()` and nothing
 *               to call it. Real traffic only ever touches the line it was routed over, so every
 *               OTHER line stayed "never measured" forever — and a line nobody has measured cannot
 *               be ranked, which quietly turns multi-line routing back into single-line routing.
 *
 *  Author(s):
 *      Nictheboy Li    <nictheboy@outlook.com>
 */

import 'dart:async';
import 'dart:math' as math;

import 'health.dart';
import 'registry.dart';

/// What one probe request came back with.
///
/// Deliberately not a `Response`: this package does not choose an HTTP client, and the shape of the
/// answer it needs is this small.
class ProbeOutcome {
  const ProbeOutcome({required this.ok, this.bytes = 0});

  final bool ok;

  /// How many bytes arrived. Only the throughput probe cares.
  final int bytes;
}

/// Sends one probe. The consumer supplies this, because it owns the client and the credentials.
typedef SendProbe = Future<ProbeOutcome> Function(String url);

class ProberOptions {
  const ProberOptions({
    this.probePath = '/mt/probe',
    this.interval = const Duration(seconds: 15),
    this.timeout = const Duration(seconds: 5),
    this.maxBackoff = const Duration(minutes: 5),
    this.bandwidthPath,
    this.bandwidthInterval = const Duration(minutes: 10),
    this.idleBeforeBandwidth = const Duration(seconds: 30),
  });

  /// Path of the unauthenticated liveness endpoint.
  final String probePath;

  /// Gap between latency probes of a healthy line.
  final Duration interval;

  /// A probe that has not answered within this is a failure.
  final Duration timeout;

  /// Backoff cap for a line that keeps failing.
  final Duration maxBackoff;

  /// Path of a download used to measure throughput. Null disables throughput probing entirely.
  final String? bandwidthPath;

  /// Minimum gap between throughput measurements of the same line.
  final Duration bandwidthInterval;

  /// How long the application must have been quiet before a throughput probe may run.
  final Duration idleBeforeBandwidth;
}

/// Drives the health table.
///
/// Deliberately not started by the constructor: probing is a background activity with a real cost,
/// and a library that begins making network requests the moment it is instantiated is a library
/// that surprises people. The caller says when.
class Prober {
  Prober({
    required List<Line> Function() lines,
    required HealthTable health,
    required SendProbe send,
    required String Function(String path, Line line) resolve,
    this.options = const ProberOptions(),
    DateTime Function()? now,
  })  : _lines = lines,
        _health = health,
        _send = send,
        _resolve = resolve,
        _now = now ?? DateTime.now;

  final List<Line> Function() _lines;
  final HealthTable _health;
  final SendProbe _send;
  final String Function(String path, Line line) _resolve;
  final ProberOptions options;
  final DateTime Function() _now;

  /// Next allowed probe time per line, which is how backoff is expressed.
  final Map<String, DateTime> _nextProbeAt = {};
  final Map<String, DateTime> _nextBandwidthAt = {};

  /// When the application last issued a real request, so quiet can be told from busy.
  DateTime? _lastTrafficAt;

  Timer? _timer;
  bool _running = false;

  /// Called by the line manager on every real request, so the prober can tell quiet from busy.
  void noteTraffic() => _lastTrafficAt = _now();

  void start() {
    if (_running) return;
    _running = true;
    unawaited(_tick());
  }

  void stop() {
    _running = false;
    _timer?.cancel();
    _timer = null;
  }

  /// Probe everything now and wait for it. The panel's refresh button, and every test's lever.
  Future<void> probeAll() async {
    await Future.wait(_lines().map(_probe));
    _health.reconcileDegraded();
  }

  Future<void> _tick() async {
    if (!_running) return;

    final now = _now();
    final due = _lines()
        .where((line) => !(_nextProbeAt[line.id]?.isAfter(now) ?? false))
        .toList();
    await Future.wait(due.map(_probe));
    _health.reconcileDegraded();

    if (options.bandwidthPath != null) await _maybeMeasureThroughput();

    if (!_running) return;
    // One timer for all lines rather than one per line: the schedule is already carried by
    // _nextProbeAt, and a single wakeup keeps a phone's radio asleep between rounds.
    _timer = Timer(options.interval, () => unawaited(_tick()));
  }

  Future<void> _probe(Line line) async {
    final started = _now();
    try {
      final outcome = await _send(
        _resolve(options.probePath, line),
      ).timeout(options.timeout);
      if (!outcome.ok) throw StateError('probe for ${line.id} was refused');
      _health.recordSuccess(line.id, _now().difference(started), _now());
      _nextProbeAt[line.id] = _now().add(options.interval);
    } catch (error) {
      _health.recordFailure(line.id, error, _now());
      _nextProbeAt[line.id] = _now().add(
        _backoff(_health[line.id].consecutiveFailures),
      );
    }
  }

  /// Measure throughput on one line, if the application has been quiet long enough.
  ///
  /// One line per round, never all of them: measuring several at once would have them compete for
  /// the same pipe and each report a fraction of the truth.
  Future<void> _maybeMeasureThroughput() async {
    final now = _now();
    final quietSince = _lastTrafficAt;
    if (quietSince != null &&
        now.difference(quietSince) < options.idleBeforeBandwidth) {
      return;
    }

    Line? candidate;
    for (final line in _lines()) {
      final ready = !(_nextBandwidthAt[line.id]?.isAfter(now) ?? false);
      if (_health[line.id].state != LineState.down && ready) {
        candidate = line;
        break;
      }
    }
    if (candidate == null) return;

    // Claim the slot before measuring, so a failure cannot make it retry on the very next tick.
    _nextBandwidthAt[candidate.id] = now.add(options.bandwidthInterval);

    final started = _now();
    try {
      final outcome = await _send(_resolve(options.bandwidthPath!, candidate));
      final seconds = _now().difference(started).inMicroseconds / 1000000;
      if (seconds > 0 && outcome.bytes > 0) {
        _health.recordThroughput(candidate.id, outcome.bytes / seconds);
      }
    } catch (error) {
      _health.recordFailure(candidate.id, error, _now());
    }
  }

  /// Exponential, capped: a dead line costs one request every few minutes, not every few seconds.
  Duration _backoff(int consecutiveFailures) {
    final grown = options.interval *
        math.pow(2, math.min(consecutiveFailures, 10)).toDouble();
    return grown > options.maxBackoff ? options.maxBackoff : grown;
  }
}
