// L4 — many logical streams over the one redundant stream, the Dart mirror of the peers. Frame:
// type:u8 | streamID:u32 | length:u32 | payload. The opener uses odd stream IDs, a per-stream credit
// window bounds one stream, FIN half-closes, RST aborts, and a both-FIN stream is freed without an
// RST so a graceful close never discards the peer's buffered bytes.

import 'dart:async';
import 'dart:typed_data';

import 'redundant.dart';

const int _muxSyn = 0x01;
const int _muxData = 0x02;
const int _muxFin = 0x03;
const int _muxRst = 0x04;
const int _muxWindow = 0x05;
const int _muxHdr = 9;
const int _muxMaxChunk = 16 * 1024;
const int _streamWindow = 256 * 1024;

class MuxSession {
  final RedundantStream _transport;
  final Map<int, MuxStream> _streams = {};
  Uint8List _parseBuf = Uint8List(0);
  int _nextId;
  Future<void> _writeChain = Future.value();

  MuxSession._(this._transport, bool client) : _nextId = client ? 1 : 2 {
    _transport.onDeliver = _onBytes;
    // When the transport closes (e.g. every line rejected by the origin), fail the streams with its
    // reason so a pending read/write returns fast instead of hanging.
    _transport.onClose = _failAll;
  }

  void _failAll(Object? err) {
    final e = err ?? StateError('multipath: transport closed');
    for (final st in _streams.values) {
      st._fail(e);
    }
    _streams.clear();
  }

  factory MuxSession.client(RedundantStream transport) =>
      MuxSession._(transport, true);

  MuxStream openStream() {
    final id = _nextId;
    _nextId += 2;
    final st = MuxStream._(this, id);
    _streams[id] = st;
    send(_muxSyn, id, null);
    return st;
  }

  void close() {
    for (final st in _streams.values) {
      st._onReset();
    }
    _streams.clear();
    _transport.close();
  }

  Future<void> send(int type, int id, Uint8List? payload) {
    final frame = Uint8List(_muxHdr + (payload?.length ?? 0));
    final v = ByteData.view(frame.buffer);
    frame[0] = type;
    v.setUint32(1, id);
    v.setUint32(5, payload?.length ?? 0);
    if (payload != null) frame.setRange(_muxHdr, frame.length, payload);
    _writeChain =
        _writeChain.then((_) => _transport.write(frame)).catchError((_) {});
    return _writeChain;
  }

  void _remove(int id) => _streams.remove(id);

  void _onBytes(Uint8List chunk) {
    final next = Uint8List(_parseBuf.length + chunk.length);
    next.setRange(0, _parseBuf.length, _parseBuf);
    next.setRange(_parseBuf.length, next.length, chunk);
    _parseBuf = next;

    while (_parseBuf.length >= _muxHdr) {
      final v = ByteData.view(
          _parseBuf.buffer, _parseBuf.offsetInBytes, _parseBuf.length);
      final type = _parseBuf[0];
      final id = v.getUint32(1);
      final n = v.getUint32(5);
      if (_parseBuf.length < _muxHdr + n) break;
      final payload =
          Uint8List.fromList(_parseBuf.sublist(_muxHdr, _muxHdr + n));
      _parseBuf = Uint8List.sublistView(_parseBuf, _muxHdr + n);
      _dispatch(type, id, payload);
    }
  }

  void _dispatch(int type, int id, Uint8List payload) {
    final st = _streams[id];
    switch (type) {
      case _muxData:
        st?._onData(payload);
        break;
      case _muxFin:
        st?._remoteFin();
        break;
      case _muxRst:
        if (st != null) {
          st._onReset();
          _streams.remove(id);
        }
        break;
      case _muxWindow:
        if (st != null && payload.length == 4) {
          st._grantSend(ByteData.view(payload.buffer, payload.offsetInBytes)
              .getUint32(0));
        }
        break;
    }
  }
}

class MuxStream {
  final MuxSession _sess;
  final int id;
  final List<Uint8List> _inbox = [];
  bool _remoteEof = false;
  bool _localFin = false;
  Object? _err;
  int _sendWin = _streamWindow;
  final List<Completer<void>> _readWaiters = [];
  final List<Completer<void>> _writeWaiters = [];

  MuxStream._(this._sess, this.id);

  Future<Uint8List?> read() async {
    while (true) {
      if (_inbox.isNotEmpty) {
        final chunk = _inbox.removeAt(0);
        _sess.send(
            _muxWindow, id, _u32(chunk.length)); // replenish the peer's credit
        return chunk;
      }
      if (_err != null) throw _err!;
      if (_remoteEof) return null;
      final c = Completer<void>();
      _readWaiters.add(c);
      await c.future;
    }
  }

  Future<void> write(Uint8List bytes) async {
    var p = 0;
    while (p < bytes.length) {
      while (_sendWin == 0 && _err == null && !_localFin) {
        final c = Completer<void>();
        _writeWaiters.add(c);
        await c.future;
      }
      if (_err != null) throw _err!;
      if (_localFin) throw StateError('multipath: write after close');
      final n = [bytes.length - p, _sendWin, _muxMaxChunk]
          .reduce((a, b) => a < b ? a : b);
      _sendWin -= n;
      await _sess.send(_muxData, id, Uint8List.sublistView(bytes, p, p + n));
      p += n;
    }
  }

  void closeWrite() {
    if (_localFin) return;
    _localFin = true;
    _wakeWriters();
    _sess.send(_muxFin, id, null);
    _cleanupIfClosed();
  }

  void reset() {
    _err ??= StateError('multipath: stream reset');
    _wakeReaders();
    _wakeWriters();
    _sess._remove(id);
    _sess.send(_muxRst, id, null);
  }

  void _onData(Uint8List payload) {
    if (_err != null || _remoteEof) return;
    _inbox.add(payload);
    _wakeReaders();
  }

  void _remoteFin() {
    _remoteEof = true;
    _wakeReaders();
    _cleanupIfClosed();
  }

  void _onReset() {
    _err ??= StateError('multipath: stream reset');
    _wakeReaders();
    _wakeWriters();
  }

  void _fail(Object err) {
    _err ??= err;
    _wakeReaders();
    _wakeWriters();
  }

  void _grantSend(int n) {
    _sendWin += n;
    _wakeWriters();
  }

  void _cleanupIfClosed() {
    if (_localFin && _remoteEof) _sess._remove(id);
  }

  void _wakeReaders() {
    final w = List<Completer<void>>.from(_readWaiters);
    _readWaiters.clear();
    for (final c in w) {
      if (!c.isCompleted) c.complete();
    }
  }

  void _wakeWriters() {
    final w = List<Completer<void>>.from(_writeWaiters);
    _writeWaiters.clear();
    for (final c in w) {
      if (!c.isCompleted) c.complete();
    }
  }
}

Uint8List _u32(int n) {
  final b = Uint8List(4);
  ByteData.view(b.buffer).setUint32(0, n);
  return b;
}
