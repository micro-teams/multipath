import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'package:multipath/multipath.dart';
import 'package:multipath/src/appws.dart' show computeSha1ForTesting;
import 'package:test/test.dart';

/// A fake MuxStream: write() records every call; read() serves a pre-queued list of responses (a
/// chunk, or null for EOF), one per call, resolving a pending completer if the queue runs dry so a
/// test can feed a response after the client has already sent its request. `implements MuxStream`
/// (rather than extending it) so it satisfies MultipathWebSocket's MuxStream-typed parameter without
/// needing a real MuxSession to construct one.
class FakeStream implements MuxStream {
  final List<Uint8List> writes = [];
  final List<Uint8List?> _queue = [];
  final List<Completer<Uint8List?>> _waiters = [];

  @override
  Future<void> write(Uint8List bytes) async {
    writes.add(bytes);
  }

  @override
  Future<Uint8List?> read() {
    if (_queue.isNotEmpty) return Future.value(_queue.removeAt(0));
    final c = Completer<Uint8List?>();
    _waiters.add(c);
    return c.future;
  }

  void push(Uint8List? chunk) {
    if (_waiters.isNotEmpty) {
      _waiters.removeAt(0).complete(chunk);
    } else {
      _queue.add(chunk);
    }
  }

  @override
  void reset() {}

  @override
  void closeWrite() {}

  @override
  int get id => 0;
}

Future<String> _keyFromRequest(FakeStream fake) async {
  await Future<void>.delayed(Duration.zero);
  await Future<void>.delayed(Duration.zero);
  final req = ascii.decode(fake.writes[0], allowInvalid: true);
  final m = RegExp(r'Sec-WebSocket-Key: (\S+)').firstMatch(req);
  if (m == null) throw StateError('no Sec-WebSocket-Key in request: $req');
  return m.group(1)!;
}

Future<String> _accept(String key) async {
  final digest = await computeSha1ForTesting(
      utf8.encode('$key' + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'));
  return base64.encode(digest);
}

Uint8List _handshakeResponse(String accept) => Uint8List.fromList(ascii.encode(
    'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
    'Sec-WebSocket-Accept: $accept\r\n\r\n'));

void main() {
  group('SHA-1', () {
    test('matches the RFC 6455 §1.3 worked example', () async {
      // The spec's own example: key "dGhlIHNhbXBsZSBub25jZQ==" must accept to
      // "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".
      const key = 'dGhlIHNhbXBsZSBub25jZQ==';
      const guid = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
      final digest = await computeSha1ForTesting(utf8.encode(key + guid));
      expect(base64.encode(digest), 's3pPLMBiTxaQ9kYGzzhZRbK+xOo=');
    });

    test('matches the empty-string vector', () async {
      final digest = await computeSha1ForTesting(<int>[]);
      final hex = digest.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
      expect(hex, 'da39a3ee5e6b4b0d3255bfef95601890afd80709');
    });
  });

  group('WSFrameReader', () {
    test('parses a small unmasked frame in one push', () {
      final r = WSFrameReader();
      r.push(Uint8List.fromList(
          [0x82, 0x02, 0x68, 0x69])); // FIN|binary, len 2, "hi"
      final f = r.next()!;
      expect(f.fin, true);
      expect(f.opcode, 0x2);
      expect(f.data, [0x68, 0x69]);
      expect(r.next(), isNull);
    });

    test('unmasks a masked frame', () {
      final mask = [0x01, 0x02, 0x03, 0x04];
      final payload = [0x68, 0x69, 0x21];
      final masked = List.generate(3, (i) => payload[i] ^ mask[i % 4]);
      final r = WSFrameReader();
      r.push(Uint8List.fromList([0x82, 0x80 | 3, ...mask, ...masked]));
      expect(r.next()!.data, payload);
    });

    test('returns null until enough bytes have been pushed, byte by byte', () {
      final bytes = [0x82, 0x05, 1, 2, 3, 4, 5];
      final r = WSFrameReader();
      for (var i = 0; i < bytes.length - 1; i++) {
        r.push(Uint8List.fromList([bytes[i]]));
        expect(r.next(), isNull);
      }
      r.push(Uint8List.fromList([bytes.last]));
      expect(r.next()!.data, [1, 2, 3, 4, 5]);
    });

    test('decodes the 16-bit extended length', () {
      final payload = Uint8List(300)..fillRange(0, 300, 7);
      final r = WSFrameReader();
      r.push(Uint8List.fromList([0x82, 126, 300 >> 8, 300 & 0xff]));
      r.push(payload);
      final f = r.next()!;
      expect(f.data.length, 300);
      expect(f.data.every((b) => b == 7), isTrue);
    });
  });

  group('MultipathWebSocket', () {
    test(
        'handshakes, delivers a message, and answers a ping without surfacing it',
        () async {
      final fake = FakeStream();
      final openFuture = MultipathWebSocket.open(fake, '/chat');

      final key = await _keyFromRequest(fake);
      fake.push(_handshakeResponse(await _accept(key)));
      final ws = await openFuture;

      WSMessage? received;
      ws.onMessage = (m) => received = m;

      fake.push(Uint8List.fromList(
          [0x89, 0x04, 0x70, 0x69, 0x6e, 0x67])); // ping "ping"
      fake.push(Uint8List.fromList(
          [0x81, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f])); // text "hello"

      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(received, isNotNull);
      expect(received!.isText, isTrue);
      expect(received!.text, 'hello');

      final pong = fake.writes[1];
      expect(pong[0], 0x8a); // FIN|pong
      final mask = pong.sublist(2, 6);
      final maskedPayload = pong.sublist(6, 10);
      final unmasked = List.generate(4, (i) => maskedPayload[i] ^ mask[i]);
      expect(ascii.decode(unmasked), 'ping');
    });

    test('sendText masks and frames one complete message', () async {
      final fake = FakeStream();
      final openFuture = MultipathWebSocket.open(fake, '/chat');
      final key = await _keyFromRequest(fake);
      fake.push(_handshakeResponse(await _accept(key)));
      final ws = await openFuture;

      ws.sendText('hi');
      await Future<void>.delayed(const Duration(milliseconds: 20));

      final frame = fake.writes[1];
      expect(frame[0], 0x81); // FIN|text
      expect(frame[1] & 0x80, 0x80); // masked
      expect(frame[1] & 0x7f, 2);
      final mask = frame.sublist(2, 6);
      final masked = frame.sublist(6, 8);
      final unmasked = List.generate(2, (i) => masked[i] ^ mask[i]);
      expect(ascii.decode(unmasked), 'hi');
    });

    test('onDone fires on stream EOF', () async {
      final fake = FakeStream();
      final openFuture = MultipathWebSocket.open(fake, '/chat');
      final key = await _keyFromRequest(fake);
      fake.push(_handshakeResponse(await _accept(key)));
      final ws = await openFuture;

      var doneCalled = false;
      ws.onDone = () => doneCalled = true;

      fake.push(null); // EOF
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(doneCalled, isTrue);
      expect(ws.isClosed, isTrue);
    });
  });
}
