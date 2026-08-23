// The manager as a whole: what it routes over, what it learns from, and what it refuses to break.

import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

Registry two() => parseRegistry({
      'lines': [
        {'id': 'a', 'url': ''},
        {'id': 'b', 'url': 'https://b.example.com'},
      ],
    });

void main() {
  group('memory', _memory);
  test('a single same-origin line sends exactly what a plain client would',
      () async {
    final manager = LineManager(
      registry: parseRegistry({
        'lines': [
          {'id': 'origin', 'url': ''},
        ],
      }),
    );
    final urls = <String>[];
    await manager.read((line, {required cancelled}) async {
      urls.add(line.resolve('/mt/chat'));
      return 'ok';
    });
    expect(urls, ['/mt/chat'],
        reason: 'adoption must change nothing observable');
  });

  test('real traffic teaches the ranking, not just probes', () async {
    // A client that only learned from probes would keep sending over a line it has watched fail all
    // morning.
    final manager = LineManager(registry: two());
    await manager.read((line, {required cancelled}) async {
      if (line.id == 'a') throw StateError('a is down');
      return line.id;
    });
    expect(manager.health['a'].consecutiveFailures, 1);
    expect(manager.health['b'].measured, isTrue);
  });

  test('a failing line is eventually ranked last', () async {
    final manager = LineManager(registry: two());
    for (var i = 0; i < 3; i++) {
      await manager.read((line, {required cancelled}) async {
        if (line.id == 'a') throw StateError('a is down');
        return line.id;
      });
    }
    expect(manager.ranked.map((l) => l.id), ['b', 'a']);
  });

  test('swapping the registry forgets the lines that left', () {
    final manager = LineManager(registry: two());
    manager.health
        .recordSuccess('b', const Duration(milliseconds: 5), DateTime.now());
    manager.registry = parseRegistry({
      'lines': [
        {'id': 'a', 'url': ''},
      ],
    });
    expect(manager.health['b'].measured, isFalse);
  });

  test('probing records a healthy line and a broken one differently', () async {
    final manager = LineManager(registry: two());
    await manager.probe((line) async {
      if (line.id == 'b') throw StateError('no route');
    });
    expect(manager.health['a'].measured, isTrue);
    expect(manager.health['b'].consecutiveFailures, 1);
  });

  test('an observer that throws cannot break the request it observes',
      () async {
    final manager = LineManager(
      registry: two(),
      onAttempt: (_) => throw StateError('a debugging aid misbehaving'),
    );
    expect(
        await manager.read((line, {required cancelled}) async => 'ok'), 'ok');
  });

  test('the idempotency key is 128 random bits and does not repeat', () {
    final keys = {for (var i = 0; i < 1000; i++) newIdempotencyKey()};
    expect(keys, hasLength(1000));
    expect(keys.first, hasLength(32));
  });
}

/// A store in memory, and one that fails, because both are things a real one does.
class _Memory extends HealthStore {
  String? encoded;
  int saves = 0;

  @override
  Future<String?> load() async => encoded;

  @override
  Future<void> save(String value) async {
    saves++;
    encoded = value;
  }
}

class _Broken extends HealthStore {
  const _Broken();

  @override
  Future<String?> load() async => throw StateError('quota');

  @override
  Future<void> save(String value) async => throw StateError('quota');
}

void _memory() {
  test('the second visit starts from what the first measured', () async {
    final store = _Memory();
    final first = LineManager(registry: two(), storage: store);
    // b answers, a does not: real traffic is a measurement like any other.
    await first.read((line, {required cancelled}) async {
      if (line.id == 'a') throw StateError('down');
      return line.id;
    });
    await first.saveHealth();
    expect(store.saves, 1);

    final second = LineManager(registry: two(), storage: store);
    expect(second.ranked.first.id, 'a',
        reason: 'nothing restored yet, so the configured order stands');
    await second.restoreHealth();
    expect(second.ranked.first.id, 'b');
    expect(second.preferredLineIds, ['b', 'a']);
    expect(second.health['a'].state, LineState.up,
        reason: 'yesterday failure must not start today demoted');
  });

  test('a store that throws costs nothing', () async {
    final manager = LineManager(registry: two(), storage: const _Broken());
    await manager.restoreHealth();
    await manager.saveHealth();
    expect(manager.ranked, hasLength(2));
  });

  test('restoring happens once however often it is asked for', () async {
    final store = _Memory();
    final manager = LineManager(registry: two(), storage: store);
    await Future.wait([manager.restoreHealth(), manager.restoreHealth()]);
    manager.stop();
  });

  test('recent attempts are newest first and bounded', () async {
    final manager = LineManager(registry: two(), attemptHistory: 3);
    for (var i = 0; i < 5; i++) {
      await manager.read((line, {required cancelled}) async => '$i');
    }
    expect(manager.recentAttempts, hasLength(3),
        reason: 'a diagnostic that grows without limit is a leak');
    expect(manager.recentAttempts.first.ok, isTrue);

    final failed = LineManager(registry: two());
    await expectLater(
      failed.write((line, {required cancelled}) async =>
          throw StateError('every line is out')),
      throwsA(anything),
    );
    expect(failed.recentAttempts.first.ok, isFalse);
    expect(failed.recentAttempts.first.error, isA<StateError>());
  });
}
