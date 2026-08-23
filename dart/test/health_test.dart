// The judgement calls, each one asserted.
//
// Mirrors ts/test/health.test.ts and go/health_test.go. Every test here corresponds to a sentence
// in health.dart's header comment; if one of those sentences stops being true, a test says so.

import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

final _at = DateTime.utc(2026, 1, 1);
Duration ms(int n) => Duration(milliseconds: n);

void main() {
  group('persistence', _persistence);
  test('a line nobody has measured is up, not down', () {
    final table = HealthTable();
    expect(table['never-seen'].state, LineState.up);
    expect(table['never-seen'].measured, isFalse);
  });

  test('one failure is noise; a run of them is a fact', () {
    final table = HealthTable();
    table.recordFailure('a', 'boom', _at);
    expect(table['a'].state, LineState.up, reason: 'one dropped packet');
    table.recordFailure('a', 'boom', _at);
    expect(table['a'].state, LineState.up);
    table.recordFailure('a', 'boom', _at);
    expect(table['a'].state, LineState.down,
        reason: 'three in a row is a fact');
  });

  test('a success clears the failure run', () {
    final table = HealthTable();
    table
      ..recordFailure('a', 'boom', _at)
      ..recordFailure('a', 'boom', _at)
      ..recordSuccess('a', ms(10), _at);
    expect(table['a'].consecutiveFailures, 0);
    expect(table['a'].state, LineState.up);
    expect(table['a'].lastError, isNull);
  });

  test('the first sample seeds the average rather than being blended into zero',
      () {
    // Averaging up from zero would make a new line look impossibly fast and win a ranking it has
    // not earned.
    final table = HealthTable();
    table.recordSuccess('a', ms(100), _at);
    expect(table['a'].latency, ms(100));
  });

  test(
      'later samples are smoothed, so one unlucky probe does not reorder anything',
      () {
    final table = HealthTable();
    table
      ..recordSuccess('a', ms(100), _at)
      ..recordSuccess('a', ms(200), _at);
    // 100 * 0.7 + 200 * 0.3
    expect(table['a'].latency, ms(130));
  });

  group('ranking', () {
    const a = Line(id: 'a');
    const b = Line(id: 'b', url: 'https://b.example.com');
    const c = Line(id: 'c', url: 'https://c.example.com');

    test('puts the faster measured line first', () {
      final table = HealthTable();
      table
        ..recordSuccess('a', ms(200), _at)
        ..recordSuccess('b', ms(50), _at);
      expect(table.rank([a, b]).map((l) => l.id), ['b', 'a']);
    });

    test('a measured line outranks one that has never answered', () {
      final table = HealthTable();
      table.recordSuccess('b', ms(500), _at);
      expect(table.rank([a, b]).map((l) => l.id), ['b', 'a']);
    });

    test('a down line goes last but is never dropped', () {
      final table = HealthTable();
      for (var i = 0; i < 3; i++) {
        table.recordFailure('a', 'boom', _at);
      }
      final ranked = table.rank([a, b]);
      expect(ranked.map((l) => l.id), ['b', 'a']);
      expect(ranked, hasLength(2), reason: 'somewhere still beats nowhere');
    });

    test('weight decides only when nothing has been measured', () {
      final table = HealthTable();
      const low = Line(id: 'low', weight: 1);
      const high = Line(id: 'high', url: 'https://h.example.com', weight: 99);
      expect(table.rank([low, high]).map((l) => l.id), ['high', 'low']);

      // ...and stops deciding the moment a measurement exists.
      table.recordSuccess('low', ms(5), _at);
      expect(table.rank([low, high]).map((l) => l.id), ['low', 'high']);
    });

    test('throughput breaks a tie between equally quick lines', () {
      final table = HealthTable();
      table
        ..recordSuccess('a', ms(20), _at)
        ..recordSuccess('b', ms(20), _at)
        ..recordThroughput('b', 1000000);
      expect(table.rank([a, b]).map((l) => l.id), ['b', 'a']);
    });

    test('is stable: nothing distinguishes these, so the order does not move',
        () {
      // List.sort is introsort and is not stable. Without the merge sort in health.dart this
      // assertion fails, and in production it shows up as traffic moving for no reason.
      final table = HealthTable();
      final input = [a, b, c];
      expect(table.rank(input).map((l) => l.id), ['a', 'b', 'c']);
      expect(table.rank(input).map((l) => l.id), ['a', 'b', 'c']);
    });
  });

  test('degraded is a label for answering far slower than the best', () {
    final table = HealthTable();
    table
      ..recordSuccess('fast', ms(10), _at)
      ..recordSuccess('slow', ms(100), _at)
      ..reconcileDegraded();
    expect(table['fast'].state, LineState.up);
    expect(table['slow'].state, LineState.degraded);
  });

  test('a line back within the factor stops being degraded', () {
    final table = HealthTable();
    table
      ..recordSuccess('fast', ms(10), _at)
      ..recordSuccess('slow', ms(100), _at)
      ..reconcileDegraded();
    expect(table['slow'].state, LineState.degraded);

    // Repeated samples pull the average back down under 4x.
    for (var i = 0; i < 20; i++) {
      table.recordSuccess('slow', ms(12), _at);
    }
    table.reconcileDegraded();
    expect(table['slow'].state, LineState.up);
  });

  test('a line that leaves the registry loses its reputation', () {
    // Or a re-added id would inherit whatever used to be called that.
    final table = HealthTable();
    table.recordSuccess('gone', ms(10), _at);
    table.retain(['stays']);
    expect(table['gone'].measured, isFalse);
  });
}

// Persistence: what survives a visit, and what deliberately does not.
void _persistence() {
  test('carries measurements to the next visit, but never a verdict', () {
    final at = DateTime.utc(2026, 8, 23, 12);
    final table = HealthTable();
    table.recordSuccess('a', const Duration(milliseconds: 40), at);
    table.recordThroughput('a', 500000);
    for (var i = 0; i < 3; i++) {
      table.recordFailure('b', StateError('train tunnel'), at);
    }
    expect(table['b'].state, LineState.down);

    final next = HealthTable();
    next.import(
      PersistedHealth.decode(PersistedHealth.encode(table.export())),
      const Duration(days: 7),
      at.add(const Duration(hours: 1)),
    );

    expect(next['a'].latency, const Duration(milliseconds: 40));
    expect(next['a'].throughputBps, 500000);
    expect(next['b'].state, LineState.up,
        reason: 'a line unreachable on a train yesterday starts today level');
  });

  test('ignores measurements old enough to be about a different network', () {
    final table = HealthTable();
    table.recordSuccess(
        'a', const Duration(milliseconds: 5), DateTime.utc(2026, 7));
    final next = HealthTable();
    next.import(
        table.export(), const Duration(days: 7), DateTime.utc(2026, 8, 23));
    expect(next['a'].measured, isFalse);
  });

  test('a corrupt or foreign store costs nothing', () {
    final table = HealthTable();
    expect(PersistedHealth.decode('not json'), isEmpty);
    expect(PersistedHealth.decode('{"lines":[]}'), isEmpty);
    expect(
        PersistedHealth.decode('[{"lineId":42},{"lineId":"a","latencyMs":7}]'),
        hasLength(1));
    table.import(PersistedHealth.decode('[{"lineId":"a","latencyMs":7}]'),
        const Duration(days: 7), DateTime.utc(2026, 8, 23));
    expect(table['a'].latency, const Duration(milliseconds: 7),
        reason: 'one bad entry must not cost the good ones');
  });

  test('a line never measured is not worth writing down', () {
    final table = HealthTable();
    table.recordFailure('a', StateError('no'), DateTime.utc(2026, 8, 23));
    expect(table.export(), isEmpty);
  });
}
