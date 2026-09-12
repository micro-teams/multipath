// L2, Dart side: one duplex link per line over a WebSocket. dart:io's WebSocket is the default
// transport (Dart VM and Flutter mobile); a consumer on another platform can inject its own
// connector. The link sends HELLO on open and reassembles redundant frames across message
// boundaries, knowing nothing of offsets or windows.

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'frames.dart';

typedef WsConnect = Future<WebSocket> Function(String url);

class LinkHandlers {
  final void Function() onOpen;
  final void Function(Frame) onFrame;
  final void Function() onClose;
  LinkHandlers(
      {required this.onOpen, required this.onFrame, required this.onClose});
}

class Link {
  final WebSocket _ws;
  bool _closed = false;
  Link._(this._ws);

  void send(Uint8List frame) {
    try {
      _ws.add(frame);
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

// openWebSocketLink dials url, announces itself with HELLO, and drives handlers. Any error becomes
// onClose; the redundant stream above decides whether to reconnect.
Future<Link> openWebSocketLink(
  String url,
  Uint8List connId,
  int linkIdx,
  LinkHandlers handlers, {
  WsConnect? connect,
}) async {
  final ws = await (connect ?? WebSocket.connect)(url);
  final link = Link._(ws);
  final reader = FrameReader();
  var done = false;
  void fail() {
    if (done) return;
    done = true;
    handlers.onClose();
  }

  ws.add(encodeHello(connId, linkIdx));
  handlers.onOpen();
  ws.listen(
    (data) {
      final bytes = data is Uint8List
          ? data
          : Uint8List.fromList((data as List).cast<int>());
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
    },
    onError: (_) => fail(),
    onDone: fail,
    cancelOnError: true,
  );
  return link;
}
