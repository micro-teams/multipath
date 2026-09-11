// L3 — aggregate several unreliable links into ONE reliable, ordered, never-interrupted duplex byte
// stream, the Dart mirror of the peers. Every byte is written to all live links, delivered once in
// order by absolute offset, acknowledged cumulatively, and a dropped link reconnects and replays the
// unacknowledged tail. write() completes when the send window has room.

import 'dart:async';
import 'dart:math';
import 'dart:typed_data';

import 'frames.dart';
import 'link.dart';

class RedundantOptions {
  final List<String> urls;
  final int window;
  final Duration pingInterval;
  final Duration deadAfter;
  final Duration ackInterval;
  final Duration reconnectDelay;
  final Duration maxDelay;
  final WsConnect? connect;
  RedundantOptions({
    required this.urls,
    this.window = 4 << 20,
    this.pingInterval = const Duration(seconds: 5),
    this.deadAfter = const Duration(seconds: 15),
    this.ackInterval = const Duration(milliseconds: 50),
    this.reconnectDelay = const Duration(milliseconds: 200),
    this.maxDelay = const Duration(seconds: 5),
    this.connect,
  });
}

class _Slot {
  Link? link;
  int lastSeen;
  int backoffMs;
  Timer? reconnectTimer;
  _Slot(this.lastSeen, this.backoffMs);
}

class RedundantStream {
  final RedundantOptions opt;
  final Uint8List connId;
  final List<_Slot> _slots;

  Uint8List _sendBuf =
      Uint8List(0); // unacknowledged bytes: [ackedOffset, sendOffset)
  int _sendOffset = 0;
  int _ackedOffset = 0;
  int _recvOffset = 0;
  bool _ackScheduled = false;
  final List<Completer<void>> _windowWaiters = [];
  Timer? _pingTimer;
  bool _closed = false;
  bool _opened = false;
  final Completer<void> _openCompleter = Completer<void>();

  void Function(Uint8List) onDeliver = (_) {};

  RedundantStream(this.opt)
      : connId = _randomConnId(),
        _slots = List.generate(
          opt.urls.length,
          (_) => _Slot(DateTime.now().millisecondsSinceEpoch,
              opt.reconnectDelay.inMilliseconds),
        );

  static Uint8List _randomConnId() {
    final r = Random.secure();
    return Uint8List.fromList(List.generate(16, (_) => r.nextInt(256)));
  }

  int get _nowMs => DateTime.now().millisecondsSinceEpoch;

  Future<void> dial() async {
    for (var i = 0; i < _slots.length; i++) {
      _connect(i);
    }
    _pingTimer = Timer.periodic(opt.pingInterval, (_) => _tick());
    await _openCompleter.future;
  }

  void _connect(int i) {
    if (_closed) return;
    final slot = _slots[i];
    openWebSocketLink(
      opt.urls[i],
      connId,
      i,
      LinkHandlers(
        onOpen: () {
          slot.backoffMs = opt.reconnectDelay.inMilliseconds;
          slot.lastSeen = _nowMs;
          _replay(slot);
          if (!_opened) {
            _opened = true;
            if (!_openCompleter.isCompleted) _openCompleter.complete();
          }
        },
        onFrame: (frame) {
          slot.lastSeen = _nowMs;
          _onFrame(frame, slot);
        },
        onClose: () {
          slot.link = null;
          _scheduleReconnect(i);
        },
      ),
      connect: opt.connect,
    ).then((link) {
      if (_closed) {
        link.close();
      } else {
        slot.link = link;
      }
    }).catchError((_) {
      _scheduleReconnect(i);
    });
  }

  void _scheduleReconnect(int i) {
    if (_closed) return;
    final slot = _slots[i];
    if (slot.reconnectTimer != null) return;
    slot.reconnectTimer = Timer(Duration(milliseconds: slot.backoffMs), () {
      slot.reconnectTimer = null;
      slot.backoffMs = min(slot.backoffMs * 2, opt.maxDelay.inMilliseconds);
      _connect(i);
    });
  }

  void _onFrame(Frame frame, _Slot slot) {
    switch (frame.type) {
      case frameData:
        final offset = frame.offset;
        final payload = frame.payload!;
        final end = offset + payload.length;
        if (end <= _recvOffset) return; // fully seen
        if (offset > _recvOffset) return; // a gap cannot happen on a live link
        final start = _recvOffset - offset;
        _recvOffset = end;
        onDeliver(Uint8List.sublistView(payload, start));
        _scheduleAck();
        break;
      case frameAck:
        final cum = frame.offset;
        if (cum > _ackedOffset) {
          final drop = min(cum - _ackedOffset, _sendBuf.length);
          _sendBuf = Uint8List.sublistView(_sendBuf, drop);
          _ackedOffset = cum;
          _wakeWriters();
        }
        break;
      case framePing:
        slot.link?.send(encodeNonce(framePong, frame.nonce));
        break;
      case framePong:
        break;
    }
  }

  Future<void> write(Uint8List bytes) async {
    if (_closed) throw StateError('multipath: redundant stream closed');
    var sent = 0;
    while (sent < bytes.length) {
      await _awaitWindow();
      if (_closed) throw StateError('multipath: redundant stream closed');
      final room = opt.window - _inFlight;
      final n = min(bytes.length - sent, room);
      final chunk = Uint8List.sublistView(bytes, sent, sent + n);
      _append(chunk);
      var off = _sendOffset - n;
      for (var p = 0; p < n; p += maxSegment) {
        final seg = Uint8List.sublistView(chunk, p, min(p + maxSegment, n));
        final framed = encodeData(off, seg);
        for (final slot in _slots) {
          slot.link?.send(framed);
        }
        off += seg.length;
      }
      sent += n;
    }
  }

  void _append(Uint8List chunk) {
    final next = Uint8List(_sendBuf.length + chunk.length);
    next.setRange(0, _sendBuf.length, _sendBuf);
    next.setRange(_sendBuf.length, next.length, chunk);
    _sendBuf = next;
    _sendOffset += chunk.length;
  }

  void _replay(_Slot slot) {
    if (_sendBuf.isEmpty) return;
    var off = _ackedOffset;
    for (var p = 0; p < _sendBuf.length; p += maxSegment) {
      final seg = Uint8List.sublistView(
          _sendBuf, p, min(p + maxSegment, _sendBuf.length));
      slot.link?.send(encodeData(off, seg));
      off += seg.length;
    }
  }

  int get _inFlight => _sendOffset - _ackedOffset;

  Future<void> _awaitWindow() {
    if (_inFlight < opt.window) return Future.value();
    final c = Completer<void>();
    _windowWaiters.add(c);
    return c.future;
  }

  void _wakeWriters() {
    final waiters = List<Completer<void>>.from(_windowWaiters);
    _windowWaiters.clear();
    for (final c in waiters) {
      if (!c.isCompleted) c.complete();
    }
  }

  void _scheduleAck() {
    if (_ackScheduled) return;
    _ackScheduled = true;
    Timer(opt.ackInterval, () {
      _ackScheduled = false;
      final ack = encodeAck(_recvOffset);
      for (final slot in _slots) {
        slot.link?.send(ack);
      }
    });
  }

  void _tick() {
    final now = _nowMs;
    for (var i = 0; i < _slots.length; i++) {
      final slot = _slots[i];
      final link = slot.link;
      if (link == null) continue;
      if (now - slot.lastSeen > opt.deadAfter.inMilliseconds) {
        slot.link = null;
        link.close();
        _scheduleReconnect(i);
        continue;
      }
      link.send(encodeNonce(framePing, i));
    }
  }

  void close() {
    if (_closed) return;
    _closed = true;
    _pingTimer?.cancel();
    for (final slot in _slots) {
      slot.reconnectTimer?.cancel();
      slot.link?.close();
    }
    _wakeWriters();
    if (!_openCompleter.isCompleted) _openCompleter.complete();
  }
}
