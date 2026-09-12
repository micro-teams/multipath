/// Application-level WebSocket over the substrate: a real RFC 6455 client, written from scratch and
/// portable to both native and web Dart. dart:io's WebSocket only works on a real Socket (native) or
/// by dialling a real URL (`WebSocket.connect`) — neither can be pointed at an arbitrary byte stream
/// like a mux stream, and dart:io is a stub on web anyway. So an app that wants its WebSocket calls
/// to ride the redundant substrate has no platform primitive to lean on in either environment;
/// multipath provides one, the same way [Client.roundTrip] already provides one for HTTP.
///
/// [MultipathWebSocket] exposes callbacks (onMessage/onError/onDone) plus send/close, in the spirit
/// of this package's existing event style (see [RedundantStream.onLinkState]) rather than
/// implementing dart:io's `Stream`/`StreamSink` WebSocket interface — that interface is itself
/// dart:io-specific and unavailable on web, so mirroring it here would not actually be portable.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'mux.dart';

const int _opContinuation = 0x0;
const int _opText = 0x1;
const int _opBinary = 0x2;
const int _opClose = 0x8;
const int _opPing = 0x9;
const int _opPong = 0xa;
const String _wsGuid = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

/// One decoded WebSocket message: [isText] distinguishes a text frame (decode [data] as UTF-8 to get
/// the string) from binary.
class WSMessage {
  final bool isText;
  final Uint8List data;
  WSMessage(this.isText, this.data);
  String get text => utf8.decode(data);
}

class _WSFrame {
  final bool fin;
  final int opcode;
  final Uint8List data;
  _WSFrame(this.fin, this.opcode, this.data);
}

/// Pure, synchronous RFC 6455 frame parser: push bytes as they arrive off the stream, pull whole
/// frames as they become available. Split out from [MultipathWebSocket] so the wire format is
/// testable without any I/O, the same way [FrameReader] is tested independently of a real link.
class WSFrameReader {
  Uint8List _buf = Uint8List(0);

  void push(Uint8List chunk) {
    final next = Uint8List(_buf.length + chunk.length);
    next.setRange(0, _buf.length, _buf);
    next.setRange(_buf.length, next.length, chunk);
    _buf = next;
  }

  /// Returns the next complete frame, or null if more bytes are needed. Unmasks the payload if the
  /// frame was masked (a compliant server never masks, but this tolerates it either way).
  _WSFrame? next() {
    if (_buf.length < 2) return null;
    final v = ByteData.view(_buf.buffer, _buf.offsetInBytes, _buf.length);
    final fin = (_buf[0] & 0x80) != 0;
    final opcode = _buf[0] & 0x0f;
    final masked = (_buf[1] & 0x80) != 0;
    var length = _buf[1] & 0x7f;
    var headerLen = 2;
    if (length == 126) {
      if (_buf.length < 4) return null;
      length = v.getUint16(2);
      headerLen = 4;
    } else if (length == 127) {
      if (_buf.length < 10) return null;
      length = v.getUint64(2);
      headerLen = 10;
    }
    final maskLen = masked ? 4 : 0;
    final total = headerLen + maskLen + length;
    if (_buf.length < total) return null;
    final data = Uint8List.fromList(_buf.sublist(headerLen + maskLen, total));
    if (masked) {
      final mask = _buf.sublist(headerLen, headerLen + 4);
      for (var i = 0; i < data.length; i++) {
        data[i] ^= mask[i % 4];
      }
    }
    _buf = Uint8List.sublistView(_buf, total);
    return _WSFrame(fin, opcode, data);
  }
}

/// Frames one complete, unfragmented message. The client always masks (RFC 6455 requires it).
Uint8List _encodeFrame(int opcode, Uint8List payload) {
  final length = payload.length;
  final List<int> head;
  if (length < 126) {
    head = [0x80 | opcode, 0x80 | length];
  } else if (length < 1 << 16) {
    head = [0x80 | opcode, 0x80 | 126, (length >> 8) & 0xff, length & 0xff];
  } else {
    head = [
      0x80 | opcode,
      0x80 | 127,
      0,
      0,
      0,
      0,
      (length >> 24) & 0xff,
      (length >> 16) & 0xff,
      (length >> 8) & 0xff,
      length & 0xff,
    ];
  }
  final r = Random.secure();
  final mask = Uint8List.fromList(List.generate(4, (_) => r.nextInt(256)));
  final masked = Uint8List(length);
  for (var i = 0; i < length; i++) {
    masked[i] = payload[i] ^ mask[i % 4];
  }
  final out = Uint8List(head.length + 4 + length);
  out.setRange(0, head.length, head);
  out.setRange(head.length, head.length + 4, mask);
  out.setRange(head.length + 4, out.length, masked);
  return out;
}

Future<String> _wsAccept(String key) async {
  final bytes = utf8.encode(key + _wsGuid);
  final digest = await computeSha1ForTesting(bytes);
  return base64.encode(digest);
}

int _findDoubleCrlf(Uint8List buf) {
  for (var i = 0; i + 3 < buf.length; i++) {
    if (buf[i] == 13 &&
        buf[i + 1] == 10 &&
        buf[i + 2] == 13 &&
        buf[i + 3] == 10) {
      return i;
    }
  }
  return -1;
}

/// An application-level WebSocket carried over one multipath stream. Constructed via
/// [MultipathWebSocket.open]; the handshake must complete before use.
class MultipathWebSocket {
  final MuxStream _stream;
  final WSFrameReader _reader = WSFrameReader();
  Future<void> _writeChain = Future.value();
  int _fragType = 0;
  Uint8List _fragData = Uint8List(0);
  bool _closed = false;

  /// Called once per complete message.
  void Function(WSMessage) onMessage = (_) {};

  /// Called on a protocol or stream error; the connection is then torn down and [onDone] fires.
  void Function(Object) onError = (_) {};

  /// Called once, when the connection ends (peer close, EOF, or a local [close]).
  void Function() onDone = () {};

  bool get isClosed => _closed;

  MultipathWebSocket._(this._stream);

  /// Opens [stream] (already `Client.open`ed to a service) and performs the RFC 6455 client
  /// handshake at [path]; [headers] are sent verbatim (e.g. a sub-protocol the target expects). The
  /// far end must be a real WebSocket server. Starts the read loop before returning.
  static Future<MultipathWebSocket> open(
    MuxStream stream,
    String path, {
    Map<String, String> headers = const {},
  }) async {
    final ws = MultipathWebSocket._(stream);
    await ws._handshake(path, headers);
    unawaited(ws._readLoop());
    return ws;
  }

  Future<void> _handshake(String path, Map<String, String> headers) async {
    final r = Random.secure();
    final keyBytes =
        Uint8List.fromList(List.generate(16, (_) => r.nextInt(256)));
    final key = base64.encode(keyBytes);
    final buf = StringBuffer()
      ..write('GET $path HTTP/1.1\r\n')
      ..write('Host: multipath\r\n')
      ..write('Upgrade: websocket\r\n')
      ..write('Connection: Upgrade\r\n')
      ..write('Sec-WebSocket-Key: $key\r\n')
      ..write('Sec-WebSocket-Version: 13\r\n');
    headers.forEach((k, v) => buf.write('$k: $v\r\n'));
    buf.write('\r\n');
    await _stream.write(Uint8List.fromList(utf8.encode(buf.toString())));

    var head = Uint8List(0);
    var sep = -1;
    while (sep < 0) {
      final chunk = await _stream.read();
      if (chunk == null) {
        throw StateError('multipath: websocket handshake: stream closed');
      }
      final next = Uint8List(head.length + chunk.length);
      next.setRange(0, head.length, head);
      next.setRange(head.length, next.length, chunk);
      head = next;
      sep = _findDoubleCrlf(head);
    }
    final headText = ascii.decode(head.sublist(0, sep), allowInvalid: true);
    _reader.push(Uint8List.sublistView(head, sep + 4));

    final lines = headText.split('\r\n');
    final statusParts = lines[0].split(' ');
    if (statusParts.length < 2 || statusParts[1] != '101') {
      throw StateError('multipath: websocket handshake got ${lines[0]}');
    }
    final respHeaders = <String, String>{};
    for (final line in lines.skip(1)) {
      final idx = line.indexOf(':');
      if (idx > 0) {
        respHeaders[line.substring(0, idx).trim().toLowerCase()] =
            line.substring(idx + 1).trim();
      }
    }
    final accept = await _wsAccept(key);
    if ((respHeaders['upgrade'] ?? '').toLowerCase() != 'websocket' ||
        respHeaders['sec-websocket-accept'] != accept) {
      throw StateError(
          'multipath: websocket handshake response did not confirm the upgrade');
    }
  }

  Future<void> _readLoop() async {
    try {
      while (true) {
        var frame = _reader.next();
        while (frame == null) {
          final chunk = await _stream.read();
          if (chunk == null) {
            _finishClose();
            return;
          }
          _reader.push(chunk);
          frame = _reader.next();
        }
        if (!await _handleFrame(frame)) return;
      }
    } catch (e) {
      if (!_closed) onError(e);
      _finishClose();
    }
  }

  /// Returns false when the loop should stop (a close frame was seen or an error occurred).
  Future<bool> _handleFrame(_WSFrame frame) async {
    switch (frame.opcode) {
      case _opText:
      case _opBinary:
        _fragType = frame.opcode;
        _fragData = frame.data;
        break;
      case _opContinuation:
        final next = Uint8List(_fragData.length + frame.data.length);
        next.setRange(0, _fragData.length, _fragData);
        next.setRange(_fragData.length, next.length, frame.data);
        _fragData = next;
        break;
      case _opPing:
        await _rawSend(_opPong, frame.data);
        return true;
      case _opPong:
        return true;
      case _opClose:
        await _rawSend(_opClose, Uint8List(0));
        _finishClose();
        return false;
      default:
        onError(StateError(
            'multipath: websocket: unexpected opcode ${frame.opcode}'));
        _finishClose();
        return false;
    }
    if (frame.fin) {
      onMessage(WSMessage(_fragType == _opText, _fragData));
      _fragData = Uint8List(0);
    }
    return true;
  }

  /// Sends one complete text message.
  void sendText(String text) =>
      _send(_opText, Uint8List.fromList(utf8.encode(text)));

  /// Sends one complete binary message.
  void sendBinary(Uint8List data) => _send(_opBinary, data);

  void _send(int opcode, Uint8List payload) {
    unawaited(_rawSend(opcode, payload).catchError((Object e) => onError(e)));
  }

  Future<void> _rawSend(int opcode, Uint8List payload) {
    _writeChain =
        _writeChain.then((_) => _stream.write(_encodeFrame(opcode, payload)));
    return _writeChain;
  }

  /// Initiates a close: sends a close frame; [onDone] fires once the peer's close (or EOF) arrives.
  void close() {
    if (_closed) return;
    unawaited(_rawSend(_opClose, Uint8List(0)).catchError((Object _) {}));
  }

  void _finishClose() {
    if (_closed) return;
    _closed = true;
    _stream.reset();
    onDone();
  }
}

// A minimal, dependency-free SHA-1 (RFC 3174) so the handshake's Sec-WebSocket-Accept works without
// pulling in package:crypto — this package ships with zero dependencies, matching the go/jvm/ts
// peers' own from-scratch WebSocket framing (see websocket.go's comment on that choice). Not
// underscore-prefixed (so test/appws_test.dart can import it directly against the RFC 6455 §1.3
// worked example) but not re-exported from multipath.dart either — an internal implementation
// detail, not part of the package's public API.
Future<Uint8List> computeSha1ForTesting(List<int> message) async {
  final ml = message.length * 8;
  final padded = <int>[...message, 0x80];
  while (padded.length % 64 != 56) {
    padded.add(0);
  }
  final lenBytes = ByteData(8)..setUint64(0, ml, Endian.big);
  padded.addAll(lenBytes.buffer.asUint8List());

  var h0 = 0x67452301,
      h1 = 0xEFCDAB89,
      h2 = 0x98BADCFE,
      h3 = 0x10325476,
      h4 = 0xC3D2E1F0;
  const mask32 = 0xFFFFFFFF;
  int rotl(int x, int n) => ((x << n) | (x >> (32 - n))) & mask32;

  for (var chunkStart = 0; chunkStart < padded.length; chunkStart += 64) {
    final w = List<int>.filled(80, 0);
    for (var i = 0; i < 16; i++) {
      w[i] = (padded[chunkStart + i * 4] << 24) |
          (padded[chunkStart + i * 4 + 1] << 16) |
          (padded[chunkStart + i * 4 + 2] << 8) |
          padded[chunkStart + i * 4 + 3];
    }
    for (var i = 16; i < 80; i++) {
      w[i] = rotl(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }
    var a = h0, b = h1, c = h2, d = h3, e = h4;
    for (var i = 0; i < 80; i++) {
      int f, k;
      if (i < 20) {
        f = (b & c) | ((~b & mask32) & d);
        k = 0x5A827999;
      } else if (i < 40) {
        f = b ^ c ^ d;
        k = 0x6ED9EBA1;
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8F1BBCDC;
      } else {
        f = b ^ c ^ d;
        k = 0xCA62C1D6;
      }
      final temp = (rotl(a, 5) + f + e + k + w[i]) & mask32;
      e = d;
      d = c;
      c = rotl(b, 30);
      b = a;
      a = temp;
    }
    h0 = (h0 + a) & mask32;
    h1 = (h1 + b) & mask32;
    h2 = (h2 + c) & mask32;
    h3 = (h3 + d) & mask32;
    h4 = (h4 + e) & mask32;
  }
  final out = ByteData(20);
  out.setUint32(0, h0, Endian.big);
  out.setUint32(4, h1, Endian.big);
  out.setUint32(8, h2, Endian.big);
  out.setUint32(12, h3, Endian.big);
  out.setUint32(16, h4, Endian.big);
  return out.buffer.asUint8List();
}
