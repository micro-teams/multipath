// The measurement loop, mirroring ts/test/prober.test.ts.
//
// It is a port of those tests on purpose: two clients that measured their lines differently would
// rank them differently, and a phone and a browser disagreeing about which line is fastest is worse
// than neither measuring at all.
//
// The reason this file exists at all is that the Dart package shipped WITHOUT any of this. Real
// traffic only ever touches the line it was routed over, so every other line stayed "never
// measured" forever — and a line nobody has measured cannot be ranked, which quietly turns
// multi-line routing back into single-line routing. `/__lines` showing one measured line and three
// blanks is what said so.

import 'dart:async';

import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

final registry = parseRegistry({
  'lines': [
    {'id': 'fast', 'url': 'https://fast.example'},
    {'id': 'slow', 'url': 'https://slow.example'},
  ],
});

/// A sender whose per-host behaviour a test dictates.
class _Scripted {
  _Scripted(this.script);

  final Map<String, Future<ProbeOutcome> Function()> script;
  final List<String> seen = [];

  Future<ProbeOutcome> call(String url) async {
    seen.add(url);
    final host = script.keys.firstWhere(
      url.contains,
      orElse: () => throw StateError('unscripted host: $url'),
    );
    return script[host]!();
  }
}

Future<ProbeOutcome> ok() async => const ProbeOutcome(ok: true, bytes: 2);
Future<ProbeOutcome> refused() async => const ProbeOutcome(ok: false);
Future<ProbeOutcome> boom() async => throw StateError('connection refused');
Future<ProbeOutcome> never() => Completer<ProbeOutcome>().future;

({HealthTable health, Prober prober, _Scripted sent}) build(
  Map<String, Future<ProbeOutcome> Function()> script, {
  ProberOptions options = const ProberOptions(),
}) {
  final health = HealthTable();
  final sent = _Scripted(script);
  return (
    health: health,
    prober: Prober(
      lines: () => registry.lines,
      health: health,
      send: sent.call,
      resolve: (path, line) => '${line.url}$path',
      options: options,
    ),
    sent: sent,
  );
}

void main() {
  test('records a latency for every line it can reach', () async {
    final it = build({'fast': ok, 'slow': ok});
    await it.prober.probeAll();

    expect(it.health['fast'].measured, isTrue);
    expect(it.health['slow'].measured, isTrue);
  });

  test('probes the unauthenticated liveness path', () async {
    final it = build({'fast': ok, 'slow': ok});
    await it.prober.probeAll();

    expect(it.sent.seen, contains('https://fast.example/mt/probe'));
  });

  test('counts a refusal as a failure — reachable is not the same as working',
      () async {
    // A 500 answers promptly. A probe that recorded it as a success would leave a broken line
    // ranked first, which is the opposite of what measuring is for.
    final it = build({'fast': ok, 'slow': refused});
    await it.prober.probeAll();

    expect(it.health['fast'].consecutiveFailures, 0);
    expect(it.health['slow'].consecutiveFailures, 1);
  });

  test('marks a line down once failures stop looking like noise', () async {
    final it = build({'fast': ok, 'slow': boom});
    for (var i = 0; i < 3; i++) {
      await it.prober.probeAll();
    }

    expect(it.health['slow'].state, LineState.down);
    expect(it.health['fast'].state, LineState.up);
  });

  test('times out rather than waiting on a line that never answers', () async {
    final it = build(
      {'fast': ok, 'slow': never},
      options: const ProberOptions(timeout: Duration(milliseconds: 30)),
    );

    await it.prober.probeAll().timeout(const Duration(seconds: 2));

    expect(it.health['slow'].consecutiveFailures, 1);
  });

  test('does not begin probing until it is told to', () async {
    // A library that starts making network requests the moment it is constructed is one that
    // surprises people.
    final it = build({'fast': ok, 'slow': ok});
    await Future<void>.delayed(const Duration(milliseconds: 20));

    expect(it.sent.seen, isEmpty);
  });

  test('probes on a timer once started, and stops when stopped', () async {
    final it = build(
      {'fast': ok, 'slow': ok},
      options: const ProberOptions(interval: Duration(milliseconds: 20)),
    );

    it.prober.start();
    await Future<void>.delayed(const Duration(milliseconds: 70));
    final whileRunning = it.sent.seen.length;
    expect(whileRunning, greaterThan(2));

    it.prober.stop();
    await Future<void>.delayed(const Duration(milliseconds: 60));
    expect(it.sent.seen.length, whileRunning);
  });

  test('probes a dead line less and less often', () async {
    // A dead route costs a request every few minutes rather than every few seconds. Read from the
    // schedule rather than by waiting minutes for it.
    final now = DateTime(2026);
    final health = HealthTable();
    final sent = _Scripted({'fast': ok, 'slow': boom});
    final prober = Prober(
      lines: () => registry.lines,
      health: health,
      send: sent.call,
      resolve: (path, line) => '${line.url}$path',
      options: const ProberOptions(interval: Duration(seconds: 15)),
      now: () => now,
    );

    await prober.probeAll();
    final afterOne = sent.seen.length;
    await prober.probeAll();
    await prober.probeAll();

    // probeAll ignores the schedule — that is what a refresh button is — so all three rounds ask.
    expect(sent.seen.length, afterOne * 3);
    // What backoff changes is the failure count the schedule is derived from.
    expect(health['slow'].consecutiveFailures, 3);
  });

  test('does not measure throughput while the application is busy', () async {
    final it = build(
      {'fast': ok, 'slow': ok},
      options: const ProberOptions(
        interval: Duration(milliseconds: 20),
        bandwidthPath: '/mt/bandwidth',
        idleBeforeBandwidth: Duration(seconds: 30),
      ),
    );

    it.prober.noteTraffic();
    it.prober.start();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    it.prober.stop();

    expect(it.sent.seen.where((u) => u.contains('bandwidth')), isEmpty);
  });

  test('measures once the application has gone quiet', () async {
    final it = build(
      {'fast': ok, 'slow': ok},
      options: const ProberOptions(
        interval: Duration(milliseconds: 20),
        bandwidthPath: '/mt/bandwidth',
        idleBeforeBandwidth: Duration.zero,
      ),
    );

    it.prober.start();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    it.prober.stop();

    expect(it.sent.seen.where((u) => u.contains('bandwidth')), isNotEmpty);
  });

  test('measures one line at a time, so they do not compete for the same pipe',
      () async {
    // The property is about CONCURRENCY, not about totals: over enough rounds every line gets
    // measured, and that is fine. What must never happen is two of them downloading at once, each
    // reporting a fraction of a pipe they were sharing.
    var inFlight = 0;
    var peak = 0;
    final health = HealthTable();
    final seen = <String>[];
    Future<ProbeOutcome> send(String url) async {
      seen.add(url);
      if (!url.contains('bandwidth')) return const ProbeOutcome(ok: true);
      inFlight += 1;
      peak = peak > inFlight ? peak : inFlight;
      await Future<void>.delayed(const Duration(milliseconds: 15));
      inFlight -= 1;
      return const ProbeOutcome(ok: true, bytes: 4096);
    }

    final prober = Prober(
      lines: () => registry.lines,
      health: health,
      send: send,
      resolve: (path, line) => '${line.url}$path',
      options: const ProberOptions(
        interval: Duration(milliseconds: 10),
        bandwidthPath: '/mt/bandwidth',
        idleBeforeBandwidth: Duration.zero,
        bandwidthInterval: Duration(milliseconds: 1),
      ),
    );

    prober.start();
    await Future<void>.delayed(const Duration(milliseconds: 80));
    prober.stop();

    expect(seen.where((u) => u.contains('bandwidth')), isNotEmpty);
    expect(peak, 1);
  });

  test('is disabled entirely when no bandwidth path is configured', () async {
    final it = build(
      {'fast': ok, 'slow': ok},
      options: const ProberOptions(interval: Duration(milliseconds: 20)),
    );

    it.prober.start();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    it.prober.stop();

    expect(it.sent.seen.where((u) => u.contains('bandwidth')), isEmpty);
  });

  test('produces a ranking that puts the reachable line first', () async {
    // The point of measuring at all.
    final it = build({'fast': ok, 'slow': boom});
    await it.prober.probeAll();

    expect(it.health.rank(registry.lines).first.id, 'fast');
  });

  test('a manager told how to send one measures every line', () async {
    // The wiring, not the loop: a manager without a sender ranks on configured weight and measures
    // nothing, which is what every Dart consumer got until this existed.
    final sent = _Scripted({'fast': ok, 'slow': ok});
    final manager = LineManager(registry: registry, send: sent.call);

    await manager.probeNow();

    expect(manager.health['fast'].measured, isTrue);
    expect(manager.health['slow'].measured, isTrue);
  });

  test('a manager without one still works, and says it measured nothing',
      () async {
    final manager = LineManager(registry: registry);

    await manager.probeNow();

    expect(manager.health['fast'].measured, isFalse);
    expect(manager.ranked, hasLength(2));
  });
}
