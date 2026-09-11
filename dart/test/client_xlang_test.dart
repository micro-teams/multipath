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
      final client = await Client.dial(
        lines,
        pingInterval: const Duration(milliseconds: 20),
        deadAfter: const Duration(seconds: 2),
        ackInterval: const Duration(milliseconds: 8),
        reconnectDelay: const Duration(milliseconds: 20),
        maxDelay: const Duration(milliseconds: 200),
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

      // Normal: a raw HTTP round trip to the origin's own greeter.
      final http = client.openNormal();
      await http.write(utf8.encode(
          'GET /xlang HTTP/1.1\r\nHost: origin\r\nConnection: close\r\n\r\n'));
      final resp = BytesBuilder();
      while (true) {
        final chunk = await http.read();
        if (chunk == null) break;
        resp.add(chunk);
      }
      final text = utf8.decode(resp.takeBytes());
      expect(text, contains('200'));
      expect(text, endsWith('hello /xlang'));

      client.close();
    } finally {
      proc.kill();
    }
  },
      timeout: const Timeout(Duration(seconds: 40)),
      skip: cp == null ? 'set MP_JVM_CP' : false);
}
