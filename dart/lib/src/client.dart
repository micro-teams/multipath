// The client end of the substrate for Dart and Flutter: one redundant mux to the origin over every
// line, then each exchange a mux stream on top. A caller opens a tunnel to a target or a normal
// stream to the origin's own service; the redundancy is underneath, in the bytes.

import 'dart:convert';
import 'dart:typed_data';

import 'appws.dart';
import 'header.dart';
import 'link.dart';
import 'mux.dart';
import 'redundant.dart';

class Client {
  final MuxSession _sess;
  final RedundantStream _rs;
  Client._(this._sess, this._rs);

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
    void Function(LinkState)? onLinkState,
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
      onLinkState: onLinkState,
    ));
    final sess = MuxSession.client(rs);
    await rs.dial();
    return Client._(sess, rs);
  }

  /// Opens a stream to the named [service], carrying [ticket] for the origin's handler to authorise
  /// it. If the origin has not registered the name it resets the stream with a reason, which surfaces
  /// as an error on the first read.
  MuxStream open(String service, {Uint8List? ticket}) {
    final st = _sess.openStream();
    st.write(encodeHeader(Header(service, ticket: ticket)));
    return st;
  }

  /// Opens a stream to the named [service] and performs the RFC 6455 client handshake at [path],
  /// returning a callback-based WebSocket (onMessage/onError/onDone, sendText/sendBinary, close) —
  /// neither dart:io's WebSocket nor the web platform gives another way to run WebSocket traffic
  /// over the substrate (dart:io's is a stub on web; on native it only wraps a real Socket or dials
  /// a real URL, never an arbitrary byte stream). The far end must be a real WebSocket server;
  /// [headers] are sent verbatim in the handshake request (e.g. a sub-protocol the target expects).
  Future<MultipathWebSocket> openWebSocket(
    String service,
    String path, {
    Uint8List? ticket,
    Map<String, String> headers = const {},
  }) {
    final st = open(service, ticket: ticket);
    return MultipathWebSocket.open(st, path, headers: headers);
  }

  /// Carries one HTTP exchange over a normal stream: the request is serialized to HTTP/1.1, written
  /// to a fresh stream, and the response read back and parsed. This is the drop-in equivalent of the
  /// Go client's RoundTrip and the TS client's fetch — line redundancy happens underneath without the
  /// caller knowing more than one path exists, and the request-line / header / content-length /
  /// chunked framing is handled here once instead of by every caller. Content-Length and
  /// Transfer-Encoding: chunked responses are both decoded; otherwise the body is delimited by the
  /// stream's EOF (the origin closes per response).
  Future<MultipathResponse> roundTrip(String service, MultipathRequest request,
      {Uint8List? ticket}) async {
    final st = open(service, ticket: ticket);
    await st.write(_serializeRequest(request));
    final raw = await _readToEnd(st);
    return _parseResponse(raw);
  }

  /// A snapshot of every underlying line's health (up/connecting/down, last byte seen, reconnect
  /// count, last drop reason). A status view reads this; to react to changes as they happen, pass
  /// onLinkState to [dial]. Redundancy hides line failure from the data path on purpose — this is how
  /// a caller sees the failures it is surviving.
  List<LinkStat> stats() => _rs.stats();

  void close() => _sess.close();
}

/// One HTTP request to carry over the substrate. [headers] keys are case-insensitive on the wire;
/// Host, Content-Length and Connection are set automatically if absent.
class MultipathRequest {
  final String method;
  final Uri url;
  final Map<String, String> headers;
  final Uint8List body;
  MultipathRequest(
    this.method,
    this.url, {
    Map<String, String>? headers,
    Uint8List? body,
  })  : headers = headers ?? {},
        body = body ?? Uint8List(0);
}

/// The parsed HTTP response. [headers] are lower-cased keys.
class MultipathResponse {
  final int statusCode;
  final String reasonPhrase;
  final Map<String, String> headers;
  final Uint8List body;
  MultipathResponse(
      this.statusCode, this.reasonPhrase, this.headers, this.body);
}

const String _crlf = '\r\n';

Uint8List _serializeRequest(MultipathRequest req) {
  final path = req.url.path.isEmpty
      ? '/${req.url.hasQuery ? '?${req.url.query}' : ''}'
      : req.url.path + (req.url.hasQuery ? '?${req.url.query}' : '');
  // Header names are matched case-insensitively so caller-supplied ones aren't duplicated.
  final lower = {for (final k in req.headers.keys) k.toLowerCase(): k};
  final headers = Map<String, String>.from(req.headers);
  if (!lower.containsKey('host')) headers['Host'] = req.url.authority;
  headers[lower['content-length'] ?? 'Content-Length'] = '${req.body.length}';
  // The origin closes the connection per response, so EOF is the response boundary.
  headers[lower['connection'] ?? 'Connection'] = 'close';

  final sb = StringBuffer('${req.method} $path HTTP/1.1$_crlf');
  headers.forEach((k, v) => sb.write('$k: $v$_crlf'));
  sb.write(_crlf);
  final head = ascii.encode(sb.toString());
  final out = Uint8List(head.length + req.body.length);
  out.setRange(0, head.length, head);
  out.setRange(head.length, out.length, req.body);
  return out;
}

Future<Uint8List> _readToEnd(MuxStream st) async {
  final chunks = <Uint8List>[];
  var total = 0;
  for (;;) {
    final chunk = await st.read();
    if (chunk == null) break;
    chunks.add(chunk);
    total += chunk.length;
  }
  final out = Uint8List(total);
  var off = 0;
  for (final c in chunks) {
    out.setRange(off, off + c.length, c);
    off += c.length;
  }
  return out;
}

MultipathResponse _parseResponse(Uint8List bytes) {
  final sep = _findDoubleCrlf(bytes);
  if (sep < 0)
    throw const FormatException('multipath: malformed HTTP response');
  final headText = ascii.decode(bytes.sublist(0, sep), allowInvalid: true);
  final rawBody = Uint8List.sublistView(bytes, sep + 4);
  final lines = headText.split(_crlf);
  final statusParts = lines.isNotEmpty ? lines[0].split(' ') : <String>[];
  final status =
      statusParts.length >= 2 ? int.tryParse(statusParts[1]) ?? 0 : 0;
  final reason =
      statusParts.length >= 3 ? statusParts.sublist(2).join(' ') : '';
  final headers = <String, String>{};
  for (var i = 1; i < lines.length; i++) {
    final idx = lines[i].indexOf(':');
    if (idx > 0) {
      headers[lines[i].substring(0, idx).trim().toLowerCase()] =
          lines[i].substring(idx + 1).trim();
    }
  }

  Uint8List body;
  final te = headers['transfer-encoding'];
  final cl = headers['content-length'];
  if (te != null && te.toLowerCase().contains('chunked')) {
    body = _dechunk(rawBody);
  } else if (cl != null) {
    final n = int.tryParse(cl) ?? rawBody.length;
    body = Uint8List.sublistView(rawBody, 0, n.clamp(0, rawBody.length));
  } else {
    body = Uint8List.sublistView(rawBody);
  }
  return MultipathResponse(status, reason, headers, body);
}

// _dechunk decodes an HTTP/1.1 Transfer-Encoding: chunked body: repeated "<hexlen>CRLF<data>CRLF"
// until a zero-length chunk. Trailers, if any, are ignored.
Uint8List _dechunk(Uint8List data) {
  final out = BytesBuilder();
  var p = 0;
  while (p < data.length) {
    final lineEnd = _indexOfCrlf(data, p);
    if (lineEnd < 0) break;
    final sizeLine = ascii.decode(data.sublist(p, lineEnd), allowInvalid: true);
    // A chunk-size line may carry ";extension"; take only the hex size before it.
    final hex = sizeLine.split(';').first.trim();
    final size = int.tryParse(hex, radix: 16);
    if (size == null) break;
    p = lineEnd + 2;
    if (size == 0) break;
    if (p + size > data.length) break;
    out.add(data.sublist(p, p + size));
    p += size;
    // Skip the CRLF that terminates the chunk data.
    if (p + 2 <= data.length) p += 2;
  }
  return out.toBytes();
}

int _findDoubleCrlf(Uint8List b) {
  for (var i = 0; i + 3 < b.length; i++) {
    if (b[i] == 13 && b[i + 1] == 10 && b[i + 2] == 13 && b[i + 3] == 10) {
      return i;
    }
  }
  return -1;
}

int _indexOfCrlf(Uint8List b, int from) {
  for (var i = from; i + 1 < b.length; i++) {
    if (b[i] == 13 && b[i + 1] == 10) return i;
  }
  return -1;
}
