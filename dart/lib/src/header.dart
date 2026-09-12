// L5 stream header, byte-identical to the peers: version:u8 | serviceLen:u16 | service |
// ticketLen:u16 | ticket. Every stream names a service the origin registered — there is no "main
// service" and no client-chosen address; the library carries the ticket as opaque bytes and never
// reads it.

import 'dart:convert';
import 'dart:typed_data';

const int _headerVersion = 2;
const int _maxField = 4096;

class Header {
  final String service;
  final Uint8List ticket;
  Header(this.service, {Uint8List? ticket}) : ticket = ticket ?? Uint8List(0);
}

Uint8List encodeHeader(Header h) {
  final service = utf8.encode(h.service);
  if (service.length > _maxField || h.ticket.length > _maxField) {
    throw ArgumentError(
        'multipath: stream header field exceeds $_maxField bytes');
  }
  final b = Uint8List(5 + service.length + h.ticket.length);
  final v = ByteData.view(b.buffer);
  b[0] = _headerVersion;
  v.setUint16(1, service.length);
  b.setRange(3, 3 + service.length, service);
  v.setUint16(3 + service.length, h.ticket.length);
  b.setRange(
      5 + service.length, 5 + service.length + h.ticket.length, h.ticket);
  return b;
}

class HeaderResult {
  final Header header;
  final int consumed;
  HeaderResult(this.header, this.consumed);
}

// readHeader returns the header and how many bytes it consumed, or null if more are needed.
HeaderResult? readHeader(Uint8List buf) {
  if (buf.length < 1) return null;
  if (buf[0] != _headerVersion) {
    throw StateError('multipath: unsupported stream header version ${buf[0]}');
  }
  final v = ByteData.view(buf.buffer, buf.offsetInBytes, buf.length);

  final service = _readField(buf, v, 1);
  if (service == null) return null;
  final ticket = _readField(buf, v, service.next);
  if (ticket == null) return null;

  return HeaderResult(
      Header(utf8.decode(service.body), ticket: ticket.body), ticket.next);
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
