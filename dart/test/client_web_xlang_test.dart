import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

// Cross-language proof for the Dart client compiled for the WEB target: run under a real browser
// (`dart test -p chrome`), so this is what actually proves link_web.dart — the package:web L2 link —
// works against a real WebSocket server, not just that it compiles. No dart:io here: a browser page
// can't spawn the JVM origin itself, so testbed/run.sh starts one ahead of time on this fixed port
// (`dart test` has no supported way to pass a compile-time define or runtime arg through to a
// browser suite, so a shared constant is simpler than fighting that). This test is only meaningful
// run via testbed/run.sh; run standalone (no origin listening on this port), it fails outright
// rather than skipping — there's no cheap way to detect "no origin" from inside a browser page
// without first attempting and timing out the very connection this test exists to prove works.
const _originPort = 47653;

void main() {
  test(
      'web: tunnels and round-trips HTTP+WebSocket through a JVM origin over browser-native links',
      () async {
    final n = 3;
    final lines = List.generate(n, (_) => 'ws://127.0.0.1:$_originPort');
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
    final st =
        client.open('echo', ticket: Uint8List.fromList(utf8.encode('ticket')));
    final msg =
        Uint8List.fromList(List.generate(20 * 1024, (i) => (i * 7 + 3) & 0xff));
    await st.write(msg);
    st.closeWrite();
    final echoed = BytesBuilder();
    while (true) {
      final chunk = await st.read();
      if (chunk == null) break;
      echoed.add(chunk);
    }
    expect(echoed.takeBytes(), msg);

    // Normal: an HTTP round trip via the client's roundTrip helper.
    final resp = await client.roundTrip(
        'greeter', MultipathRequest('GET', Uri.parse('http://origin/xlang')));
    expect(resp.statusCode, 200);
    expect(utf8.decode(resp.body), endsWith('hello /xlang'));

    final stats = client.stats();
    expect(stats.length, n);
    expect(stats.every((s) => s.state == 'up'), isTrue);
    expect(linkEvents.any((e) => e.up), isTrue);

    // WebSocket: a real RFC 6455 handshake and message round trip through the substrate, over a
    // link that is itself a browser-native WebSocket (link_web.dart), to the origin's ws-echo
    // backend — proves the app-level WebSocket rides a real browser L2 link end to end.
    final ws = await client.openWebSocket('ws-echo', '/echo');
    final got = <WSMessage>[];
    ws.onMessage = got.add;
    ws.sendBinary(Uint8List.fromList(utf8.encode('hello ws')));
    final wsDeadline = DateTime.now().add(const Duration(seconds: 5));
    while (got.isEmpty && DateTime.now().isBefore(wsDeadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    expect(got.length, 1);
    expect(utf8.decode(got[0].data), 'hello ws');
    ws.close();

    client.close();
  },
      timeout: const Timeout(Duration(seconds: 40)));
}
