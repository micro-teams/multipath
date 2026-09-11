// L5 stream header, byte-identical to the peers: version:u8 | kind:u8 | targetLen:u16 | target |
// ticketLen:u16 | ticket. The library carries the ticket as opaque bytes; kind, target policy, and
// ticket meaning are the origin handler's concern.

import 'dart:convert';
import 'dart:typed_data';

const int kindNormal = 0;
const int kindTunnel = 1;

const int _headerVersion = 1;
const int _maxField = 4096;

class Header {
  final int kind;
  final String target;
  final Uint8List ticket;
  Header(this.kind, {this.target = '', Uint8List? ticket})
      : ticket = ticket ?? Uint8List(0);
}

Uint8List encodeHeader(Header h) {
  final target = utf8.encode(h.target);
  if (target.length > _maxField || h.ticket.length > _maxField) {
    throw ArgumentError(
        'multipath: stream header field exceeds $_maxField bytes');
  }
  final b = Uint8List(6 + target.length + h.ticket.length);
  final v = ByteData.view(b.buffer);
  b[0] = _headerVersion;
  b[1] = h.kind;
  v.setUint16(2, target.length);
  b.setRange(4, 4 + target.length, target);
  v.setUint16(4 + target.length, h.ticket.length);
  b.setRange(6 + target.length, 6 + target.length + h.ticket.length, h.ticket);
  return b;
}

class HeaderResult {
  final Header header;
  final int consumed;
  HeaderResult(this.header, this.consumed);
}

// readHeader returns the header and how many bytes it consumed, or null if more are needed.
HeaderResult? readHeader(Uint8List buf) {
  if (buf.length < 2) return null;
  if (buf[0] != _headerVersion) {
    throw StateError('multipath: unsupported stream header version ${buf[0]}');
  }
  final kind = buf[1];
  final v = ByteData.view(buf.buffer, buf.offsetInBytes, buf.length);

  final target = _readField(buf, v, 2);
  if (target == null) return null;
  final ticket = _readField(buf, v, target.next);
  if (ticket == null) return null;

  return HeaderResult(
      Header(kind, target: utf8.decode(target.body), ticket: ticket.body),
      ticket.next);
}

class _Field {
  final Uint8List body;
  final int next;
  _Field(this.body, this.next);
}

_Field? _readField(Uint8List buf, ByteData v, int off) {
  if (buf.length < off + 2) return null;
  final n = v.getUint16(off);
  if (n > _maxField)
    throw StateError(
        'multipath: stream header field length $n exceeds $_maxField');
  if (buf.length < off + 2 + n) return null;
  return _Field(
      Uint8List.fromList(buf.sublist(off + 2, off + 2 + n)), off + 2 + n);
}
