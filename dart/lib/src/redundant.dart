// L3 — aggregate several unreliable links into ONE reliable, ordered, never-interrupted duplex byte
// stream, the Dart mirror of the peers. Every byte is written to all live links, delivered once in
// order by absolute offset, acknowledged cumulatively, and a dropped link reconnects and replays the
// unacknowledged tail. write() completes when the send window has room.

import 'dart:async';
import 'dart:math';
import 'dart:typed_data';

import 'frames.dart';
import 'link.dart';

/// Reported to [RedundantOptions.onLinkState] the moment a line's up/down state changes — the edge a
/// log wants. [up] is true on (re)connect, false on drop; [reason] is the drop cause (empty on up);
/// [durationMs] is how long the line spent in the state it just left.
class LinkState {
  final int index;
  final bool up;
  final String reason;
  final int durationMs;
  LinkState(this.index, this.up, this.reason, this.durationMs);
}

/// A point-in-time snapshot of one line — the level a status view wants. [state] is "up",
/// "connecting" (never yet up) or "down" (was up, now reconnecting). [lastByteMs] is when a frame
/// last arrived (0 if never). [reconnects] counts recoveries after a drop; [reason] is the last drop
/// cause.
class LinkStat {
  final int index;
  final String state;
  final int lastByteMs;
  final int reconnects;
  final String reason;
  LinkStat(
      this.index, this.state, this.lastByteMs, this.reconnects, this.reason);
}

class RedundantOptions {
  final List<String> urls;
  final int window;
  final Duration pingInterval;
  final Duration deadAfter;
  final Duration ackInterval;
  final Duration reconnectDelay;
  final Duration maxDelay;
  final WsConnect? connect;

  /// Called once per line up/down transition (never on the hot path). Edge-triggered: a repeatedly
  /// failing reconnect does not re-fire it, but the latest reason shows up in [RedundantStream.stats].
  final void Function(LinkState)? onLinkState;
  RedundantOptions({
    required this.urls,
    this.window = 4 << 20,
    this.pingInterval = const Duration(seconds: 5),
    this.deadAfter = const Duration(seconds: 15),
    this.ackInterval = const Duration(milliseconds: 50),
    this.reconnectDelay = const Duration(milliseconds: 200),
    this.maxDelay = const Duration(seconds: 5),
    this.connect,
    this.onLinkState,
  });
}

class _Slot {
  Link? link;
  int lastSeen;
  int backoffMs;
  Timer? reconnectTimer;
  String state = 'connecting';
  int sinceMs;
  int reconnects = 0;
  String reason = '';
  String reapReason = '';
  bool rejected = false; // origin refused this link (REJECT) — never retry it
  _Slot(this.lastSeen, this.backoffMs) : sinceMs = lastSeen;
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

  /// Called when the stream closes; [err] is set when it closed with a cause (e.g. all links
  /// rejected). The mux above uses it to fail its streams so reads/writes don't hang.
  void Function(Object? err) onClose = (_) {};

  Object? _closeErr;

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

  // _markUp records line i as connected, firing onLinkState on a real transition (edge-triggered).
  void _markUp(int i) {
    final slot = _slots[i];
    if (slot.state == 'up') return;
    final was = slot.state;
    final dur = _nowMs - slot.sinceMs;
    slot.state = 'up';
    slot.sinceMs = _nowMs;
    if (was == 'down') slot.reconnects++;
    opt.onLinkState?.call(LinkState(i, true, '', dur));
  }

  // _markDown records line i as down with a reason. Always updates the last reason; fires
  // onLinkState only on a real transition, so repeated failed reconnects don't spam.
  void _markDown(int i, String reason) {
    final slot = _slots[i];
    slot.reason = reason;
    if (slot.state == 'down') return;
    final dur = _nowMs - slot.sinceMs;
    slot.state = 'down';
    slot.sinceMs = _nowMs;
    opt.onLinkState?.call(LinkState(i, false, reason, dur));
  }

  /// A snapshot of every line's current health. A status view reads this; a log uses onLinkState.
  List<LinkStat> stats() => List.generate(
        _slots.length,
        (i) => LinkStat(i, _slots[i].state, _slots[i].lastSeen,
            _slots[i].reconnects, _slots[i].reason),
      );

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
          slot.reapReason = '';
          _replay(slot);
          _markUp(i);
          if (!_opened) {
            _opened = true;
            if (!_openCompleter.isCompleted) _openCompleter.complete();
          }
        },
        onFrame: (frame) {
          slot.lastSeen = _nowMs;
          _onFrame(frame, slot, i);
        },
        onClose: () {
          slot.link = null;
          if (slot.rejected)
            return; // refused by origin; already down, never retry
          _markDown(
              i,
              slot.reapReason.isNotEmpty
                  ? slot.reapReason
                  : 'connection closed');
          slot.reapReason = '';
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
    }).catchError((Object e) {
      _markDown(i, 'connect failed: $e');
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

  void _onFrame(Frame frame, _Slot slot, int i) {
    switch (frame.type) {
      case frameReject:
        // The origin refused this link and said why. Mark it down, never retry it, and if every
        // link is refused, close the stream with the reason so reads/writes fail fast.
        slot.rejected = true;
        _markDown(i, 'origin rejected: ${frame.reason}');
        slot.link?.close();
        slot.link = null;
        if (_slots.every((s) => s.rejected)) {
          _closeWithError(StateError(
              'multipath: all links rejected by origin: ${frame.reason}'));
        }
        return;
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
    if (_closed)
      throw _closeErr ?? StateError('multipath: redundant stream closed');
    var sent = 0;
    while (sent < bytes.length) {
      await _awaitWindow();
      if (_closed)
        throw _closeErr ?? StateError('multipath: redundant stream closed');
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
        // close() drives the link's onClose, which marks it down; stamp the real cause first.
        slot.reapReason = 'no data for ${opt.deadAfter.inMilliseconds}ms';
        link.close();
        _scheduleReconnect(i);
        continue;
      }
      link.send(encodeNonce(framePing, i));
    }
  }

  // _closeWithError records a cause then closes, so the mux above (via onClose) fails its streams
  // with it and blocked/subsequent reads and writes surface the reason instead of hanging.
  void _closeWithError(Object err) {
    if (_closed) return;
    _closeErr = err;
    close();
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
    onClose(_closeErr);
  }
}
