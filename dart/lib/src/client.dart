// The client end of the substrate for Dart and Flutter: one redundant mux to the origin over every
// line, then each exchange a mux stream on top. A caller opens a tunnel to a target or a normal
// stream to the origin's own service; the redundancy is underneath, in the bytes.

import 'dart:typed_data';

import 'header.dart';
import 'link.dart';
import 'mux.dart';
import 'redundant.dart';

class Client {
  final MuxSession _sess;
  Client._(this._sess);

  // dial brings up the redundant transport over every line and resolves once at least one link is
  // up. Each line is a WebSocket origin (wss:// in production, ws:// for a testbed); linkPath is
  // joined to each.
  static Future<Client> dial(
    List<String> lines, {
    String linkPath = '/mt/link',
    int window = 4 << 20,
    Duration pingInterval = const Duration(seconds: 5),
    Duration deadAfter = const Duration(seconds: 15),
    Duration ackInterval = const Duration(milliseconds: 50),
    Duration reconnectDelay = const Duration(milliseconds: 200),
    Duration maxDelay = const Duration(seconds: 5),
    WsConnect? connect,
  }) async {
    final urls =
        lines.map((l) => l.replaceAll(RegExp(r'/$'), '') + linkPath).toList();
    final rs = RedundantStream(RedundantOptions(
      urls: urls,
      window: window,
      pingInterval: pingInterval,
      deadAfter: deadAfter,
      ackInterval: ackInterval,
      reconnectDelay: reconnectDelay,
      maxDelay: maxDelay,
      connect: connect,
    ));
    final sess = MuxSession.client(rs);
    await rs.dial();
    return Client._(sess);
  }

  /// Opens an opaque tunnel to [target], carrying [ticket] for the origin to authorise egress.
  MuxStream openTunnel(String target, {Uint8List? ticket}) {
    return _open(Header(kindTunnel, target: target, ticket: ticket));
  }

  /// Opens a stream bound for the origin's own service.
  MuxStream openNormal() => _open(Header(kindNormal));

  MuxStream _open(Header header) {
    final st = _sess.openStream();
    st.write(encodeHeader(header));
    return st;
  }

  void close() => _sess.close();
}
