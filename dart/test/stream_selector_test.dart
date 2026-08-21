// A stream cannot be raced, so all that is left is choosing, and choosing again.
//
// Mirrors ts/test/socket.test.ts and go/socket_test.go. The interesting cases are all about the one
// idea that separates this from ordinary ranking: holding a connection is a different ability from
// answering a request quickly, and it is remembered separately.

import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

const a = Line(id: 'a');
const b = Line(id: 'b', url: 'https://b.example.com');

void main() {
  var now = DateTime.utc(2026, 1, 1);
  DateTime clock() => now;

  setUp(() => now = DateTime.utc(2026, 1, 1));

  test('with nothing known, the best-ranked line is dialled', () {
    final selector = StreamSelector(lines: () => [a, b], now: clock);
    expect(selector.next()?.id, 'a');
  });

  test('no lines at all is null rather than a guess', () {
    final selector = StreamSelector(lines: () => [], now: clock);
    expect(selector.next(), isNull);
  });

  test('a line that could not HOLD a connection is skipped next time', () {
    // The proxy that serves requests perfectly and refuses the Upgrade.
    final selector = StreamSelector(lines: () => [a, b], now: clock);
    selector.closed(a, Duration.zero);
    expect(selector.next()?.id, 'b');
  });

  test('an ordinary disconnection after a long session is not held against it',
      () {
    // Or every line would slowly be penalised for the network being a network.
    final selector = StreamSelector(lines: () => [a, b], now: clock);
    selector.closed(a, const Duration(minutes: 30));
    expect(selector.next()?.id, 'a');
    expect(selector.penalties, isEmpty);
  });

  test('the penalty expires, so a line having a bad minute comes back', () {
    final selector = StreamSelector(lines: () => [a, b], now: clock);
    selector.closed(a, Duration.zero);
    expect(selector.next()?.id, 'b');
    now = now.add(const Duration(seconds: 61));
    expect(selector.next()?.id, 'a');
  });

  test(
      'when every line is penalised, the least-recently-penalised is used anyway',
      () {
    // A client with no connection is worse than one on a flaky connection, and the penalties may
    // all be stale.
    final selector = StreamSelector(lines: () => [a, b], now: clock);
    selector.closed(a, Duration.zero);
    now = now.add(const Duration(seconds: 1));
    selector.closed(b, Duration.zero);
    expect(selector.next()?.id, 'a', reason: 'a was penalised first');
  });

  test('the current line is reported while it is up, and not between attempts',
      () {
    final selector = StreamSelector(lines: () => [a, b], now: clock);
    expect(selector.current, isNull);
    selector.opened(a);
    expect(selector.current?.id, 'a');
    selector.closed(a, const Duration(minutes: 5));
    expect(selector.current, isNull);
  });
}
