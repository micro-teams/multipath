// Wire framing for the redundant stream, byte-identical to the Go/JVM/TS peers.
//
//   HELLO 0x05 | connID[16] | linkIndex:u16
//   DATA  0x01 | offset:u64  | len:u16 | crc32:u32 | payload[len]
//   ACK   0x02 | cumulative:u64
//   PING  0x03 | nonce:u64
//   PONG  0x04 | nonce:u64

import 'dart:typed_data';

const int frameData = 0x01;
const int frameAck = 0x02;
const int framePing = 0x03;
const int framePong = 0x04;
const int frameHello = 0x05;

const int maxSegment = 32 * 1024;
const int _dataHdrLen = 1 + 8 + 2 + 4;

class Frame {
  final int type;
  final int offset; // DATA offset, or ACK cumulative
  final Uint8List? payload; // DATA
  final int nonce; // PING / PONG
  final Uint8List? connId; // HELLO
  final int linkIdx; // HELLO
  Frame(this.type,
      {this.offset = 0,
      this.payload,
      this.nonce = 0,
      this.connId,
      this.linkIdx = 0});
}

Uint8List encodeHello(Uint8List connId, int linkIdx) {
  final b = Uint8List(1 + 16 + 2);
  b[0] = frameHello;
  b.setRange(1, 17, connId);
  ByteData.view(b.buffer).setUint16(17, linkIdx);
  return b;
}

Uint8List encodeData(int offset, Uint8List payload) {
  final b = Uint8List(_dataHdrLen + payload.length);
  final v = ByteData.view(b.buffer);
  b[0] = frameData;
  v.setUint64(1, offset);
  v.setUint16(9, payload.length);
  v.setUint32(11, _crc32(payload));
  b.setRange(_dataHdrLen, _dataHdrLen + payload.length, payload);
  return b;
}

Uint8List encodeAck(int cumulative) {
  final b = Uint8List(9);
  b[0] = frameAck;
  ByteData.view(b.buffer).setUint64(1, cumulative);
  return b;
}

Uint8List encodeNonce(int type, int nonce) {
  final b = Uint8List(9);
  b[0] = type;
  ByteData.view(b.buffer).setUint64(1, nonce);
  return b;
}

// FrameReader reassembles frames off a link that delivers bytes in arbitrary chunk boundaries.
class FrameReader {
  final BytesBuilder _acc = BytesBuilder(copy: false);
  Uint8List _buf = Uint8List(0);

  void push(Uint8List chunk) {
    if (_buf.isNotEmpty) {
      _acc.add(_buf);
    }
    _acc.add(chunk);
    _buf = _acc.takeBytes();
  }

  // next returns the next complete frame, or null if more bytes are needed. Throws on corruption.
  Frame? next() {
    if (_buf.isEmpty) return null;
    final v = ByteData.view(_buf.buffer, _buf.offsetInBytes, _buf.length);
    switch (_buf[0]) {
      case frameHello:
        if (_buf.length < 1 + 18) return null;
        final connId = Uint8List.fromList(_buf.sublist(1, 17));
        final linkIdx = v.getUint16(17);
        _consume(1 + 18);
        return Frame(frameHello, connId: connId, linkIdx: linkIdx);
      case frameData:
        if (_buf.length < _dataHdrLen) return null;
        final offset = v.getUint64(1);
        final n = v.getUint16(9);
        final want = v.getUint32(11);
        if (n > maxSegment)
          throw StateError('multipath: corrupt frame (oversize DATA)');
        if (_buf.length < _dataHdrLen + n) return null;
        final payload =
            Uint8List.fromList(_buf.sublist(_dataHdrLen, _dataHdrLen + n));
        if (_crc32(payload) != want)
          throw StateError('multipath: corrupt frame (CRC)');
        _consume(_dataHdrLen + n);
        return Frame(frameData, offset: offset, payload: payload);
      case frameAck:
        if (_buf.length < 9) return null;
        final cum = v.getUint64(1);
        _consume(9);
        return Frame(frameAck, offset: cum);
      case framePing:
      case framePong:
        if (_buf.length < 9) return null;
        final type = _buf[0];
        final nonce = v.getUint64(1);
        _consume(9);
        return Frame(type, nonce: nonce);
      default:
        throw StateError('multipath: corrupt frame (unknown tag ${_buf[0]})');
    }
  }

  void _consume(int n) {
    _buf = Uint8List.sublistView(_buf, n);
  }
}

// crc32 (IEEE), lazily tabulated, so DATA frames verify byte-for-byte against the peers.
Uint32List? _crcTable;
int _crc32(Uint8List data) {
  var table = _crcTable;
  if (table == null) {
    table = Uint32List(256);
    for (var n = 0; n < 256; n++) {
      var c = n;
      for (var k = 0; k < 8; k++) {
        c = (c & 1) != 0 ? 0xedb88320 ^ (c >> 1) : c >> 1;
      }
      table[n] = c;
    }
    _crcTable = table;
  }
  var crc = 0xffffffff;
  for (var i = 0; i < data.length; i++) {
    crc = table[(crc ^ data[i]) & 0xff] ^ (crc >> 8);
  }
  return (crc ^ 0xffffffff) & 0xffffffff;
}
