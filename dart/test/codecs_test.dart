import 'dart:convert';
import 'dart:typed_data';
import 'package:multipath/multipath.dart';
import 'package:test/test.dart';

List<Frame> drainByteByByte(Uint8List bytes) {
  final r = FrameReader();
  final frames = <Frame>[];
  for (final b in bytes) {
    r.push(Uint8List.fromList([b]));
    Frame? f;
    while ((f = r.next()) != null) {
      frames.add(f!);
    }
  }
  return frames;
}

void main() {
  test('redundant frames round-trip, reassembled byte by byte', () {
    final connId = Uint8List.fromList(List.generate(16, (i) => i + 1));
    final payload = Uint8List.fromList(List.generate(300, (i) => i & 0xff));
    final b = BytesBuilder();
    b.add(encodeHello(connId, 7));
    b.add(encodeData(1234567890123, payload));
    b.add(encodeAck(999));
    b.add(encodeNonce(framePing, 42));
    b.add(encodeNonce(framePong, 43));
    final frames = drainByteByByte(b.takeBytes());
    expect(frames.length, 5);
    expect(frames[0].type, frameHello);
    expect(frames[0].linkIdx, 7);
    expect(frames[0].connId, connId);
    expect(frames[1].type, frameData);
    expect(frames[1].offset, 1234567890123);
    expect(frames[1].payload, payload);
    expect(frames[2].offset, 999);
    expect(frames[3].nonce, 42);
    expect(frames[4].nonce, 43);
  });

  test('detects a corrupt DATA payload', () {
    final wire = encodeData(0, Uint8List.fromList([1, 2, 3, 4]));
    wire[wire.length - 1] ^= 0xff;
    final r = FrameReader()..push(wire);
    expect(r.next, throwsA(isA<StateError>()));
  });

  test('computes IEEE CRC32 (pinned to the peers)', () {
    final wire = encodeData(0, utf8.encode('123456789'));
    final crc = ByteData.view(wire.buffer).getUint32(11);
    expect(crc, 0xcbf43926);
  });

  test('L5 header round-trips and stops at the boundary', () {
    for (final h in [
      Header(kindNormal),
      Header(kindTunnel, target: 'api.anthropic.com:443'),
      Header(kindTunnel,
          target: '10.0.0.1:8080', ticket: Uint8List.fromList([9, 8, 7])),
    ]) {
      final b = BytesBuilder()
        ..add(encodeHeader(h))
        ..addByte(0x58);
      final out = readHeader(b.takeBytes())!;
      expect(out.header.kind, h.kind);
      expect(out.header.target, h.target);
    }
  });
}
