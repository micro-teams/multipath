/*
 *  Description: The Dart line manager, end to end, against the running testbed.
 *
 *               The unit suite in dart/test proves the decisions in isolation with canned
 *               attempts. This proves the thing those cannot: that the decisions still hold when
 *               the attempts are real sockets to four real proxies in front of one real origin —
 *               including a line that accepts a connection and never answers, which is the case
 *               hedging and failover exist for and the one no unit test can honestly stage.
 *
 *               It is a plain program rather than `dart test`, because it asserts against a
 *               deployment that has to be up: a test file that silently passes when nothing is
 *               listening is worse than no test.
 *
 *  Author(s):
 *      Nictheboy Li    <nictheboy@outlook.com>
 */

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:multipath/multipath.dart';

final String serverUrl =
    Platform.environment['TESTBED_SERVER_URL'] ?? 'http://localhost:8080';

/// Every attempt gets its own deadline, and that is the CONSUMER's job.
///
/// This package does not own the transport, so it cannot impose one — which matters most for
/// writes, where nothing else stops a black-holed line from holding the whole write open forever.
/// Stated here because the testbed is where it would otherwise be discovered.
const Duration attemptTimeout = Duration(seconds: 2);

final HttpClient _http = HttpClient()
  ..connectionTimeout = const Duration(seconds: 2);

int _failures = 0;

void check(String what, bool ok, [String? detail]) {
  if (ok) {
    print('  ok    $what');
  } else {
    _failures++;
    print('  FAIL  $what${detail == null ? '' : '  ($detail)'}');
  }
}

Future<void> main() async {
  print('\n== the Dart line manager, against the live testbed ==\n');

  final registry = await _fetchRegistry();
  print(
    '  registry: ${registry.lines.map((l) => '${l.id}=${l.url}').join(', ')}\n',
  );

  await _readsAreHedged(registry);
  await _aHealthyLineIsAskedOnce(registry);
  await _aBlackHoledLineDoesNotStallARead(registry);
  await _oneWriteOverTwoLinesExecutesOnce(registry);
  await _aWriteFailsOverAndStillExecutesOnce(registry);
  await _probingFindsTheDeadLine(registry);

  _http.close(force: true);
  print('');
  if (_failures > 0) {
    print('$_failures assertion(s) failed');
    exit(1);
  }
  print('all Dart end-to-end assertions passed');
}

// --- the assertions ---------------------------------------------------------

/// The registry the origin actually serves is the one this client can read.
///
/// A parser that only ever sees its own test fixtures is a parser that agrees with itself.
Future<Registry> _fetchRegistry() async {
  final body = await _get('$serverUrl/mt/lines');
  final registry = parseRegistry(body);
  check('the origin\'s own registry parses', registry.lines.length >= 3);
  return registry;
}

/// A slow first line does not make the read slow: the rest are asked and the fastest answer wins.
Future<void> _readsAreHedged(Registry registry) async {
  final manager = LineManager(
    registry: Registry([_line(registry, 'slow'), _line(registry, 'fast')]),
  );
  final servedBy = <String>[];
  final started = DateTime.now();
  await manager.read((line, {required cancelled}) async {
    final answer = await _sendOver(line, 'GET', '/mt/probe');
    servedBy.add(line.id);
    return answer;
  });
  final took = DateTime.now().difference(started);

  // The slow line adds 400ms at the proxy; the hedge fires at 150ms and the fast line answers.
  check(
    'a 400ms line does not cost the read 400ms',
    took < const Duration(milliseconds: 350),
    'took ${took.inMilliseconds}ms',
  );
  check('the fast line is the one that answered', servedBy.contains('fast'));
}

/// On a healthy line, no second request is ever sent. This is what makes hedging affordable.
Future<void> _aHealthyLineIsAskedOnce(Registry registry) async {
  var attempts = 0;
  final manager = LineManager(
    registry: Registry([_line(registry, 'fast'), _line(registry, 'slow')]),
    onAttempt: (_) => attempts++,
  );
  await manager.read(
    (line, {required cancelled}) => _sendOver(line, 'GET', '/mt/probe'),
  );
  check('a healthy line means exactly one request', attempts == 1, '$attempts');
}

/// A line that accepts the connection and never answers must not black-hole a read.
Future<void> _aBlackHoledLineDoesNotStallARead(Registry registry) async {
  final manager = LineManager(
    registry: Registry([_line(registry, 'stalled'), _line(registry, 'fast')]),
  );
  final started = DateTime.now();
  final answer = await manager.read(
    (line, {required cancelled}) => _sendOver(line, 'GET', '/mt/probe'),
  );
  final took = DateTime.now().difference(started);
  check(
    'a black-holed first line does not stall the read',
    answer.isNotEmpty && took < const Duration(milliseconds: 800),
    'took ${took.inMilliseconds}ms',
  );
}

/// The write property, and a cross-package contract at the same time.
///
/// It passes only if the Dart package and the JVM filter agree on the header name — which is
/// exactly the sort of thing that is written down in two places and drifts.
Future<void> _oneWriteOverTwoLinesExecutesOnce(Registry registry) async {
  await _post('$serverUrl/mt/reset', '{}');
  const op = 'dart-e2e-once';
  final key = newIdempotencyKey();

  final body = jsonEncode({'op': op});
  await Future.wait([
    for (final id in ['fast', 'slow'])
      _sendOver(
        _line(registry, id),
        'POST',
        '/mt/echo',
        body: body,
        headers: {idempotencyHeader: key},
      ),
  ]);

  check(
    'the same key over two lines executes once',
    await _countOf(op) == 1,
    'count=${await _countOf(op)}',
  );
}

/// A write whose first line is black-holed still lands, and still lands once.
///
/// The per-attempt timeout is what makes this terminate at all — see [attemptTimeout].
Future<void> _aWriteFailsOverAndStillExecutesOnce(Registry registry) async {
  await _post('$serverUrl/mt/reset', '{}');
  const op = 'dart-e2e-failover';
  final key = newIdempotencyKey();

  final manager = LineManager(
    registry: Registry([_line(registry, 'stalled'), _line(registry, 'fast')]),
  );
  final tried = <String>[];
  await manager.write((line, {required cancelled}) async {
    tried.add(line.id);
    return _sendOver(
      line,
      'POST',
      '/mt/echo',
      body: jsonEncode({'op': op}),
      headers: {idempotencyHeader: key},
    );
  });

  check(
    'a write moves to the next line when the first never answers',
    tried.length == 2 && tried.first == 'stalled',
    tried.join(' -> '),
  );
  check(
    'and it executed exactly once',
    await _countOf(op) == 1,
    'count=${await _countOf(op)}',
  );
}

/// Probing demotes what cannot answer, and the ranking follows.
Future<void> _probingFindsTheDeadLine(Registry registry) async {
  final manager = LineManager(
    registry: Registry([_line(registry, 'stalled'), _line(registry, 'fast')]),
  );
  // Three rounds, because one failure is noise by design and a run of them is a fact.
  for (var i = 0; i < 3; i++) {
    await manager.probe((line) => _sendOver(line, 'GET', '/mt/probe'));
  }
  check(
    'a line that never answers is marked down',
    manager.health['stalled'].state == LineState.down,
    '${manager.health['stalled'].state}',
  );
  check(
    'and is ranked last rather than dropped',
    manager.ranked.map((l) => l.id).toList().toString() == '[fast, stalled]',
    manager.ranked.map((l) => l.id).join(', '),
  );
}

// --- plumbing ---------------------------------------------------------------

Line _line(Registry registry, String id) =>
    registry.lines.firstWhere((l) => l.id == id);

/// One attempt over one line, with its own deadline.
Future<String> _sendOver(
  Line line,
  String method,
  String path, {
  String? body,
  Map<String, String> headers = const {},
}) async {
  final url = Uri.parse(line.resolve(path));
  final request = await _http.openUrl(method, url);
  headers.forEach(request.headers.set);
  if (body != null) {
    request.headers.contentType = ContentType.json;
    request.write(body);
  }
  final response = await request.close().timeout(attemptTimeout);
  final text = await response.transform(utf8.decoder).join();
  // An error STATUS is an answer, not a routing failure — so it is returned, not thrown. Throwing
  // here would have a 404 retried across every line, and a write with it.
  return text;
}

Future<String> _get(String url) async {
  final response = await (await _http.getUrl(Uri.parse(url))).close();
  return response.transform(utf8.decoder).join();
}

Future<String> _post(String url, String body) async {
  final request = await _http.postUrl(Uri.parse(url));
  request.headers.contentType = ContentType.json;
  request.write(body);
  final response = await request.close();
  return response.transform(utf8.decoder).join();
}

Future<int> _countOf(String op) async {
  final body = await _get('$serverUrl/mt/count?op=$op');
  return (jsonDecode(body) as Map)['count'] as int;
}
