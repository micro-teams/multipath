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
