/// Health tracking for the Dart client.
///
/// A deliberate mirror of `ts/src/health.ts` and `go/health.go`, down to the defaults and the
/// judgement calls, because every client has to mean the same thing by "a line" — an app that
/// ranked lines differently from the connector would make "which line is slow" a question with two
/// answers.
///
/// The judgements worth restating, since they are the ones a reader will want to argue with: an
/// unmeasured line is up rather than down, because a client that has only just started has to send
/// requests somewhere; one failure is noise and a run of them is a fact, because demoting on the
/// first would take a line out of service for a dropped packet; and a down line is ranked last but
/// never removed, because if everything is down, trying the least-bad option beats refusing to try.
library;

import 'dart:convert';

import 'registry.dart';

/// How usable a line looks right now.
enum LineState {
  /// Answering.
  up,

  /// Answers, but slowly or with recent errors: usable, ranked last among usable lines.
  degraded,

  /// Not answering, and skipped until a probe says otherwise.
  down,
}

int _stateRank(LineState state) => switch (state) {
      LineState.up => 0,
      LineState.degraded => 1,
      LineState.down => 2,
    };

/// Everything measured about one line.
class LineHealth {
  const LineHealth({
    required this.lineId,
    this.state = LineState.up,
    this.latency,
    this.throughputBps = 0,
    this.consecutiveFailures = 0,
    this.lastError,
    this.lastProbedAt,
  });

  final String lineId;
  final LineState state;

  /// The smoothed round-trip time. Null means nothing is known — which is different from zero, and
  /// the difference decides a ranking.
  final Duration? latency;

  bool get measured => latency != null;

  /// The smoothed delivery rate, zero when never measured.
  final double throughputBps;

  final int consecutiveFailures;
  final String? lastError;
  final DateTime? lastProbedAt;

  LineHealth copyWith({
    LineState? state,
    Duration? latency,
    double? throughputBps,
    int? consecutiveFailures,
    String? lastError,
    bool clearError = false,
    DateTime? lastProbedAt,
  }) =>
      LineHealth(
        lineId: lineId,
        state: state ?? this.state,
        latency: latency ?? this.latency,
        throughputBps: throughputBps ?? this.throughputBps,
        consecutiveFailures: consecutiveFailures ?? this.consecutiveFailures,
        lastError: clearError ? null : (lastError ?? this.lastError),
        lastProbedAt: lastProbedAt ?? this.lastProbedAt,
      );
}

/// What is worth remembering between visits.
///
/// Only the measurements, never the states: "down" is a fact about a moment, and a line that was
/// unreachable on a train yesterday must not start today demoted. Latency and throughput age more
/// gracefully — a line that was fast last week is a better guess than the registry's fixed order,
/// which is the alternative.
class PersistedHealth {
  const PersistedHealth({
    required this.lineId,
    required this.latency,
    required this.throughputBps,
    required this.at,
  });

  final String lineId;
  final Duration? latency;
  final double throughputBps;

  /// When the measurement was taken. Null when the line was never probed.
  final DateTime? at;

  /// Milliseconds on the wire, so the Dart and TypeScript clients can read each other's records —
  /// they share a browser, and one overwriting the other's entry with a shape it cannot parse
  /// would silently cost both of them their memory.
  Map<String, Object?> toJson() => {
        'lineId': lineId,
        'latencyMs': latency?.inMicroseconds == null
            ? null
            : latency!.inMicroseconds / 1000,
        'throughputBps': throughputBps == 0 ? null : throughputBps,
        'at': at?.millisecondsSinceEpoch ?? 0,
      };

  /// Null for anything that is not a record this wrote: a foreign or corrupt entry is skipped
  /// rather than failing the whole restore, since one bad line should not cost the others.
  static PersistedHealth? fromJson(Object? raw) {
    if (raw is! Map) return null;
    final id = raw['lineId'];
    if (id is! String || id.isEmpty) return null;
    final latencyMs = raw['latencyMs'];
    final throughput = raw['throughputBps'];
    final at = raw['at'];
    return PersistedHealth(
      lineId: id,
      latency: latencyMs is num
          ? Duration(microseconds: (latencyMs * 1000).round())
          : null,
      throughputBps: throughput is num ? throughput.toDouble() : 0,
      at: at is num && at > 0
          ? DateTime.fromMillisecondsSinceEpoch(at.toInt())
          : null,
    );
  }

  static String encode(List<PersistedHealth> entries) =>
      jsonEncode([for (final entry in entries) entry.toJson()]);

  /// An empty list for anything unreadable — see [fromJson].
  static List<PersistedHealth> decode(String? encoded) {
    if (encoded == null || encoded.isEmpty) return const [];
    Object? parsed;
    try {
      parsed = jsonDecode(encoded);
    } catch (_) {
      return const [];
    }
    if (parsed is! List) return const [];
    return [
      for (final raw in parsed)
        if (fromJson(raw) case final entry?) entry,
    ];
  }
}

/// Tuning for the table. The defaults are the other packages' defaults.
class HealthOptions {
  const HealthOptions({
    this.smoothing = 0.3,
    this.failuresBeforeDown = 3,
    this.degradedFactor = 4.0,
  });

  /// The weight of each new sample, between 0 and 1.
  ///
  /// 0.3 keeps roughly the last handful of probes in view: quick enough to notice a line going bad
  /// within seconds, slow enough that one unlucky sample does not reorder the ranking for long. A
  /// ranking that flaps is worse than a slightly stale one, because every flap moves traffic.
  final double smoothing;

  /// How many consecutive failures stop being noise and become a fact.
  final int failuresBeforeDown;

  /// A line answering this many times slower than the best is labelled degraded.
  final double degradedFactor;
}

/// What is known about each line.
class HealthTable {
  HealthTable([this.options = const HealthOptions()]);

  final HealthOptions options;
  final Map<String, LineHealth> _entries = {};

  /// What is known about a line. A line nobody has measured is UP, not down: optimism is the safe
  /// default, since a client with no measurements still has to send requests somewhere.
  LineHealth operator [](String lineId) =>
      _entries[lineId] ?? LineHealth(lineId: lineId);

  /// Every line the table has heard of, by id.
  List<LineHealth> get all {
    final out = _entries.values.toList();
    out.sort((a, b) => a.lineId.compareTo(b.lineId));
    return out;
  }

  /// Folds a round-trip time into the average and clears any failure run.
  void recordSuccess(String lineId, Duration latency, DateTime at) {
    final entry = this[lineId];
    _entries[lineId] = entry.copyWith(
      latency: _blend(entry.latency, latency),
      state: LineState.up,
      consecutiveFailures: 0,
      clearError: true,
      lastProbedAt: at,
    );
  }

  /// Counts a failure, demoting the line only once the run is long enough to be a fact.
  void recordFailure(String lineId, Object? error, DateTime at) {
    final entry = this[lineId];
    final failures = entry.consecutiveFailures + 1;
    _entries[lineId] = entry.copyWith(
      consecutiveFailures: failures,
      state:
          failures >= options.failuresBeforeDown ? LineState.down : entry.state,
      lastError: error?.toString(),
      lastProbedAt: at,
    );
  }

  /// Folds a delivery rate into the average.
  void recordThroughput(String lineId, double bytesPerSecond) {
    final entry = this[lineId];
    _entries[lineId] = entry.copyWith(
      throughputBps: entry.throughputBps == 0
          ? bytesPerSecond
          : entry.throughputBps * (1 - options.smoothing) +
              bytesPerSecond * options.smoothing,
    );
  }

  /// The measurements worth carrying to the next visit.
  List<PersistedHealth> export() => [
        for (final entry in all)
          if (entry.measured || entry.throughputBps > 0)
            PersistedHealth(
              lineId: entry.lineId,
              latency: entry.latency,
              throughputBps: entry.throughputBps,
              at: entry.lastProbedAt,
            ),
      ];

  /// Seeds from a previous visit.
  ///
  /// Seeded as measurements rather than as certainties: the next probe blends into them normally,
  /// so a line that has genuinely changed is corrected within a few samples rather than being
  /// trusted indefinitely. Nothing here can mark a line down — see [PersistedHealth].
  void import(
    Iterable<PersistedHealth> entries,
    Duration maxAge,
    DateTime now,
  ) {
    for (final entry in entries) {
      final at = entry.at;
      // Stale enough and it is worse than no information: the network the user was on last month
      // says nothing about the one they are on now.
      if (at != null && now.difference(at) > maxAge) continue;
      _entries[entry.lineId] = this[entry.lineId].copyWith(
        latency: entry.latency,
        throughputBps: entry.throughputBps,
        lastProbedAt: at,
      );
    }
  }

  /// Forgets lines that have left the registry, so a re-added id cannot inherit the reputation of
  /// whatever used to be called that.
  void retain(Iterable<String> lineIds) {
    final keep = lineIds.toSet();
    _entries.removeWhere((id, _) => !keep.contains(id));
  }

  /// Orders lines best-first.
  ///
  /// Down lines go last rather than being dropped: if every line is down the caller still has to
  /// send the request somewhere. Among usable lines, measured latency decides, throughput breaks
  /// ties because two lines that answer equally fast are distinguished by how much they can carry,
  /// and configured weight decides only when nothing has been measured at all.
  List<Line> rank(List<Line> lines) {
    final ranked = [...lines];
    // A stable sort, so lines that nothing distinguishes keep their configured order.
    mergeSortBy(ranked, (a, b) {
      final ha = this[a.id];
      final hb = this[b.id];

      final states = _stateRank(ha.state).compareTo(_stateRank(hb.state));
      if (states != 0) return states;

      if (ha.measured && hb.measured && ha.latency != hb.latency) {
        return ha.latency!.compareTo(hb.latency!);
      }
      // A line that has answered outranks one that never has: measured beats unknown.
      if (ha.measured != hb.measured) return ha.measured ? -1 : 1;

      if (ha.throughputBps != hb.throughputBps) {
        return hb.throughputBps.compareTo(ha.throughputBps);
      }
      return b.weight.compareTo(a.weight);
    });
    return ranked;
  }

  /// Labels lines answering far slower than the best. Purely a label, for diagnosis.
  void reconcileDegraded() {
    Duration? best;
    for (final entry in _entries.values) {
      if (entry.state == LineState.down || !entry.measured) continue;
      if (best == null || entry.latency! < best) best = entry.latency;
    }
    if (best == null) return;

    for (final id in _entries.keys.toList()) {
      final entry = _entries[id]!;
      if (entry.state == LineState.down || !entry.measured) continue;
      final slow = entry.latency!.inMicroseconds >
          best.inMicroseconds * options.degradedFactor;
      _entries[id] = entry.copyWith(
        state: slow ? LineState.degraded : LineState.up,
      );
    }
  }

  /// Seeds on the first sample rather than averaging up from zero, which would make a new line
  /// look impossibly fast and win a ranking it has not earned.
  Duration _blend(Duration? previous, Duration sample) {
    if (previous == null) return sample;
    return Duration(
      microseconds: (previous.inMicroseconds * (1 - options.smoothing) +
              sample.inMicroseconds * options.smoothing)
          .round(),
    );
  }
}

/// A stable sort.
///
/// `List.sort` is introsort and is NOT stable, so two lines that no measurement distinguishes would
/// swap places between calls — which shows up as traffic moving for no reason and a developer panel
/// whose order changes while you read it. The other packages get stability from their standard
/// libraries (`sort.SliceStable`, `Array.prototype.sort`); this is Dart's missing half.
void mergeSortBy<T>(List<T> items, int Function(T a, T b) compare) {
  if (items.length < 2) return;
  final buffer = List<T>.of(items);
  void merge(int low, int middle, int high) {
    var i = low;
    var j = middle;
    for (var k = low; k < high; k++) {
      if (i < middle && (j >= high || compare(buffer[i], buffer[j]) <= 0)) {
        items[k] = buffer[i++];
      } else {
        items[k] = buffer[j++];
      }
    }
  }

  void sort(int low, int high) {
    if (high - low < 2) return;
    final middle = low + (high - low) ~/ 2;
    sort(low, middle);
    sort(middle, high);
    buffer.setRange(low, high, items, low);
    merge(low, middle, high);
  }

  sort(0, items.length);
}
