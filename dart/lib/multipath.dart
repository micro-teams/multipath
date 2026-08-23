/// MultiPath — transport resilience across redundant network paths.
///
/// The Dart line manager: the registry, what is known about each line, and the two strategies that
/// follow from treating reads and writes oppositely. It has no dependencies and does not send
/// anything itself; a consumer supplies the attempt, so the same package serves a Flutter app on
/// Dio and a command-line tool on `package:http`.
///
/// See `README.md` for how to wire it, and what deliberately is NOT here.
library;

export 'src/cache.dart' show CacheStore, RequestCache;
export 'src/health.dart' show HealthOptions, HealthTable, LineHealth, LineState;
export 'src/idempotency.dart' show idempotencyHeader, newIdempotencyKey;
export 'src/line_manager.dart' show AttemptReport, LineManager;
export 'src/prober.dart' show ProbeOutcome, Prober, ProberOptions, SendProbe;
export 'src/registry.dart'
    show Line, Registry, RegistryFormatException, parseRegistry;
export 'src/strategy.dart'
    show
        Attempt,
        NoLineAvailable,
        StrategyOptions,
        hedgedRead,
        writeWithFailover;
export 'src/stream_selector.dart' show StreamSelector;
