import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

// Cross-language proof for the Dart client: the Client against a real JVM Origin over WebSocket
// links, driving a tunnel (echo) and a normal HTTP exchange (greeter). Skipped unless MP_JVM_CP set.
final cp = Platform.environment['MP_JVM_CP'];

Future<(int, Process)> startOrigin(int n) async {
  final proc = await Process.start(
      'java', ['-cp', cp!, 'app.microteams.multipath.OriginMain', '$n']);
  final port = Completer<int>();
  proc.stdout
      .transform(utf8.decoder)
      .transform(const LineSplitter())
      .listen((line) {
    final m = RegExp(r'^LISTENING (\d+)').firstMatch(line);
    if (m != null && !port.isCompleted) port.complete(int.parse(m.group(1)!));
  });
  proc.stderr.transform(utf8.decoder).listen(stderr.write);
  return (await port.future.timeout(const Duration(seconds: 30)), proc);
}

void main() {
  test('tunnels and round-trips HTTP through a JVM origin over WebSocket links',
      () async {
    final n = 3;
    final (port, proc) = await startOrigin(n);
    try {
      final lines = List.generate(n, (_) => 'ws://127.0.0.1:$port');
      final linkEvents = <LinkState>[];
      final client = await Client.dial(
        lines,
        pingInterval: const Duration(milliseconds: 20),
        deadAfter: const Duration(seconds: 2),
        ackInterval: const Duration(milliseconds: 8),
        reconnectDelay: const Duration(milliseconds: 20),
        maxDelay: const Duration(milliseconds: 200),
        onLinkState: linkEvents.add,
      );

      // Tunnel: write, half-close, read the echo back to EOF.
      final st = client.openTunnel('echo:0', ticket: utf8.encode('ticket'));
      final msg = Uint8List.fromList(
          List.generate(20 * 1024, (i) => (i * 7 + 3) & 0xff));
      await st.write(msg);
      st.closeWrite();
      final echoed = BytesBuilder();
      while (true) {
        final chunk = await st.read();
        if (chunk == null) break;
        echoed.add(chunk);
      }
      expect(echoed.takeBytes(), msg);

      // Normal: an HTTP round trip via the client's roundTrip helper — the library serializes the
      // request and parses the response (status/headers/body), so the caller writes no HTTP by hand.
      final resp = await client
          .roundTrip(MultipathRequest('GET', Uri.parse('http://origin/xlang')));
      expect(resp.statusCode, 200);
      expect(utf8.decode(resp.body), endsWith('hello /xlang'));

      // The redundant transport exposes per-line health, and up transitions were reported.
      final stats = client.stats();
      expect(stats.length, n);
      expect(stats.every((s) => s.state == 'up'), isTrue);
      expect(linkEvents.any((e) => e.up), isTrue);

      client.close();
    } finally {
      proc.kill();
    }
  },
      timeout: const Timeout(Duration(seconds: 40)),
      skip: cp == null ? 'set MP_JVM_CP' : false);
}
