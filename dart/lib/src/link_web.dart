// L2, Dart web side: one duplex link per line over a browser-native WebSocket (package:web), the
// browser's only primitive for a raw duplex byte connection — there is no socket API on web, and
// dart:io's WebSocket is a compile-time-absent stub there (it is excluded from web SDK builds
// entirely, which is why this is its own file rather than a branch inside link_io.dart: importing
// dart:io at all would fail to compile for a web target). Mirrors link_io.dart's public shape
// (WsConnect / Link / openWebSocketLink) so link.dart's conditional export can swap between them
// without either side of the substrate (redundant.dart, client.dart) knowing which platform it's on.

import 'dart:async';
import 'dart:js_interop';
import 'dart:typed_data';

import 'package:web/web.dart' as web;

import 'frames.dart';
import 'link_handlers.dart';

export 'link_handlers.dart';

typedef WsConnect = Future<web.WebSocket> Function(String url);

class Link {
  final web.WebSocket _ws;
  bool _closed = false;
  Link._(this._ws);

  void send(Uint8List frame) {
    try {
      _ws.send(frame.toJS);
    } catch (_) {
      // dead link; the redundant stream notices through missing ACKs and reconnects
    }
  }

  void close() {
    _closed = true;
    try {
      _ws.close();
    } catch (_) {}
  }

  bool get closed => _closed;
}

Future<web.WebSocket> _defaultConnect(String url) {
  final ws = web.WebSocket(url)..binaryType = 'arraybuffer';
  final completer = Completer<web.WebSocket>();
  ws.onopen = (web.Event _) {
    if (!completer.isCompleted) completer.complete(ws);
  }.toJS;
  ws.onerror = (web.Event _) {
    if (!completer.isCompleted) {
      completer.completeError(
          StateError('multipath: websocket connect to $url failed'));
    }
  }.toJS;
  return completer.future;
}

// openWebSocketLink dials url, announces itself with HELLO, and drives handlers. Any error becomes
// onClose; the redundant stream above decides whether to reconnect. Identical contract to
// link_io.dart's version, just built on browser events instead of a dart:io Stream.
Future<Link> openWebSocketLink(
  String url,
  Uint8List connId,
  int linkIdx,
  LinkHandlers handlers, {
  WsConnect? connect,
}) async {
  final ws = await (connect ?? _defaultConnect)(url);
  final link = Link._(ws);
  final reader = FrameReader();
  var done = false;
  void fail() {
    if (done) return;
    done = true;
    handlers.onClose();
  }

  ws.onmessage = (web.MessageEvent e) {
    final data = e.data;
    if (data == null || !data.isA<JSArrayBuffer>()) {
      // binaryType is fixed to 'arraybuffer' above, so a non-buffer message is a protocol
      // violation from whatever is on the other end of the socket, not a case we can recover from.
      link.close();
      fail();
      return;
    }
    final bytes = (data as JSArrayBuffer).toDart.asUint8List();
    try {
      reader.push(bytes);
      Frame? frame;
      while ((frame = reader.next()) != null) {
        handlers.onFrame(frame!);
      }
    } catch (_) {
      link.close();
      fail();
    }
  }.toJS;
  ws.onerror = ((web.Event _) => fail()).toJS;
  ws.onclose = ((web.CloseEvent _) => fail()).toJS;

  ws.send(encodeHello(connId, linkIdx).toJS);
  handlers.onOpen();
  return link;
}
