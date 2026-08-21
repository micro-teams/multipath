/// The key that makes write failover safe.
///
/// Minted once per LOGICAL write, before any line is chosen, so that every attempt at that write
/// carries the same key and a second arrival is one attempt seen twice rather than two writes. A
/// key minted per attempt would be worse than no key at all: it would look like the mechanism was
/// in place while defeating it.
///
/// Uniqueness is all that is asked of it — the server only ever compares keys for equality — so 128
/// random bits, and no dependency for a UUID.
library;

import 'dart:math';

/// The header the server's interceptor reads. Must match the JVM package's.
const String idempotencyHeader = 'Idempotency-Key';

final Random _random = Random.secure();

String newIdempotencyKey() {
  final bytes = List<int>.generate(16, (_) => _random.nextInt(256));
  return [for (final b in bytes) b.toRadixString(16).padLeft(2, '0')].join();
}
