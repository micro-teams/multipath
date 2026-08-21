// Reads are raced, writes are not, and both of those are worth proving rather than reading.
//
// Mirrors ts/test/strategy.test.ts and the strategy half of go/client_test.go — including the two
// bugs both of those found the hard way: hedging that never fires on a healthy line, and a hedge
// that cancels the winner along with the losers.

import 'dart:async';

import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

const a = Line(id: 'a');
const b = Line(id: 'b', url: 'https://b.example.com');
const c = Line(id: 'c', url: 'https://c.example.com');

/// An attempt that answers after [delay], recording which lines were asked.
Attempt<String> answering(
  List<String> asked,
  Map<String, Duration> delays, {
  Set<String> failing = const {},
}) {
  return (Line line, {required Future<void> cancelled}) async {
    asked.add(line.id);
    await Future<void>.delayed(delays[line.id] ?? Duration.zero);
    if (failing.contains(line.id)) throw StateError('${line.id} is down');
    return line.id;
  };
}

void main() {
  group('hedged reads', () {
    test('a healthy first line means no second request is ever sent', () async {
      // This is the property that makes hedging affordable. Always fanning out would multiply every
      // request by the number of lines to buy an improvement that only exists on the slow tail.
      final asked = <String>[];
      expect(
        await hedgedRead(
          [a, b, c],
          answering(asked, {'a': const Duration(milliseconds: 5)}),
          options: fast,
        ),
        'a',
      );
      await settle();
      expect(asked, ['a']);
    });

    test('a slow first line brings the rest in, and the fastest answer wins',
        () async {
      final asked = <String>[];
      final result = await hedgedRead(
          [a, b, c],
          answering(asked, {
            'a': const Duration(seconds: 5),
            'b': const Duration(milliseconds: 5),
            'c': const Duration(seconds: 5),
          }),
          options: fast);
      expect(result, 'b');
      expect(asked, ['a', 'b', 'c']);
    });

    test('the losers are told they lost', () async {
      final cancelledLines = <String>[];
      final result = await hedgedRead([a, b], (
        Line line, {
        required Future<void> cancelled,
      }) async {
        unawaited(cancelled.then((_) => cancelledLines.add(line.id)));
        await Future<void>.delayed(
          line.id == 'b'
              ? const Duration(milliseconds: 5)
              : const Duration(seconds: 5),
        );
        return line.id;
      }, options: fast);
      expect(result, 'b');
      await settle();
      expect(cancelledLines, ['a']);
    });

    test('a line that fails at once does not wait out the hedge delay',
        () async {
      // Otherwise a dead first line costs every read the whole hedge budget in silence, waiting for
      // company that was already available.
      final asked = <String>[];
      final started = DateTime.now();
      final result = await hedgedRead(
        [a, b],
        answering(asked, const {}, failing: {'a'}),
        // The real default, so the assertion below means something.
        options: const StrategyOptions(),
      );
      expect(result, 'b');
      expect(asked, ['a', 'b']);
      expect(
        DateTime.now().difference(started),
        lessThan(const Duration(milliseconds: 150)),
        reason: 'it waited out the hedge delay instead of moving on',
      );
    });

    test('every line failing surfaces the error rather than hanging', () async {
      final asked = <String>[];
      await expectLater(
        hedgedRead(
          [a, b],
          answering(asked, const {}, failing: {'a', 'b'}),
          options: fast,
        ),
        throwsA(isA<StateError>()),
      );
    });

    test('no lines is an error, not an empty answer', () {
      expect(
        () => hedgedRead<String>([], answering([], const {}), options: fast),
        throwsA(isA<NoLineAvailable>()),
      );
    });
  });

  group('writes', () {
    test('go to exactly one line when it works', () async {
      final asked = <String>[];
      expect(
          await writeWithFailover([a, b, c], answering(asked, const {})), 'a');
      expect(asked, ['a'], reason: 'two writes are two writes');
    });

    test(
        'are never raced: the second line is only tried after the first throws',
        () async {
      final asked = <String>[];
      final result = await writeWithFailover(
        [a, b],
        answering(asked, const {}, failing: {'a'}),
      );
      expect(result, 'b');
      expect(asked, ['a', 'b']);
    });

    test('stop after maxWriteAttempts rather than hammering every line',
        () async {
      final asked = <String>[];
      await expectLater(
        writeWithFailover(
          [a, b, c],
          answering(asked, const {}, failing: {'a', 'b', 'c'}),
          options: const StrategyOptions(maxWriteAttempts: 2),
        ),
        throwsA(isA<StateError>()),
      );
      expect(asked, ['a', 'b']);
    });
  });
}

/// A short hedge budget, so a test that wants the fan-out does not wait 150ms for it.
const fast = StrategyOptions(hedgeAfter: Duration(milliseconds: 10));

/// Lets abandoned attempts finish their bookkeeping before an assertion reads it.
Future<void> settle() => Future<void>.delayed(const Duration(milliseconds: 30));
