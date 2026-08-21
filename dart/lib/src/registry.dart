/// Registry parsing for the Dart client.
///
/// This is a deliberate duplicate of `ts/src/registry.ts` and `go/registry.go`, and the duplication
/// is the point: the registry JSON is the one vocabulary every package shares, so each has to be
/// able to read it on its own terms. What must not diverge is the *meaning* — an id that is unique,
/// a url that is a bare origin or the empty string, a document that is rejected at the boundary
/// rather than producing a puzzling URL somewhere much later.
///
/// Every rule below mirrors a rule in the other two parsers, and the tests are written against the
/// same cases, so that a change made to one and not the others shows up as a failure rather than as
/// a subtle disagreement in production.
library;

import 'dart:convert';

/// One network path to the origin.
class Line {
  const Line({
    required this.id,
    this.url = '',
    this.transport,
    this.weight = 0,
    this.foreignOrigin = false,
  });

  /// Stable, human-readable identifier — "cf", "ipv6-1", "frp-2".
  final String id;

  /// Absolute origin for this line, or `''` meaning "same origin as the caller's default".
  final String url;

  /// Free-form transport label, for diagnosis only.
  final String? transport;

  /// Static preference, higher is better. Only breaks ties between indistinguishable latencies;
  /// measurement outranks it, because a hand-set weight goes stale and an average does not.
  final int weight;

  /// True when the client does not see this line under our own domain — a free proxy that cannot
  /// be CNAME'd, and therefore a fallback with reduced capability.
  final bool foreignOrigin;

  /// The URL for [path] over this line.
  ///
  /// A same-origin line returns the path unchanged, so a single-line deployment issues exactly the
  /// requests it issued before MultiPath existed.
  String resolve(String path) {
    if (path.isEmpty || !path.startsWith('/')) {
      throw ArgumentError.value(path, 'path', 'must start with "/"');
    }
    return url.isEmpty ? path : '$url$path';
  }

  @override
  String toString() => 'Line($id, ${url.isEmpty ? "same-origin" : url})';
}

/// The document served by `GET /mt/lines`.
class Registry {
  const Registry(this.lines);

  final List<Line> lines;

  List<String> get ids => [for (final line in lines) line.id];
}

/// Thrown when a registry document cannot be trusted.
///
/// A named type rather than a bare exception because the caller's correct response is specific:
/// keep the line you already have, and say so once. See the note in the MicroTeams client about
/// why a malformed registry must not be fatal and must not be silent either.
class RegistryFormatException implements Exception {
  const RegistryFormatException(this.message);

  final String message;

  @override
  String toString() => 'invalid line registry: $message';
}

/// An absolute origin and nothing more: no path, no trailing slash.
///
/// A trailing slash silently produces `//mt/probe` once a path is appended, which is the kind of
/// fault that survives review and fails in production.
final RegExp _origin = RegExp(r'^https?://[^/]+$');

/// Validates an untrusted registry document.
///
/// It arrives over the network, so every assumption the client later makes is checked here — the
/// only place the mistake is still cheap to read.
Registry parseRegistry(Object? document) {
  if (document is String) return parseRegistry(jsonDecode(document));
  if (document is! Map) {
    throw const RegistryFormatException('not an object');
  }

  final raw = document['lines'];
  if (raw is! List || raw.isEmpty) {
    throw const RegistryFormatException('no lines');
  }

  final lines = <Line>[];
  final seen = <String>{};
  for (var i = 0; i < raw.length; i++) {
    final entry = raw[i];
    if (entry is! Map) {
      throw RegistryFormatException('line $i is not an object');
    }

    final id = entry['id'];
    if (id is! String || id.isEmpty) {
      throw RegistryFormatException('line $i has no id');
    }
    // A duplicate id makes the developer panel and every metric lie about which line served what,
    // which is worse than a hard failure because it is believed.
    if (!seen.add(id)) {
      throw RegistryFormatException('duplicate line id "$id"');
    }

    // A JSON null on an optional field is ABSENT, not invalid. An ordinary serializer emits nulls
    // for unset optionals, and rejecting the document whole for one of them is how a real registry
    // was silently ignored and a deployment believed it had two lines while using one. That bug is
    // recorded in the repo README; this is the line of code that prevents it here.
    final url = entry['url'] ?? '';
    if (url is! String) {
      throw RegistryFormatException('line "$id": url must be a string');
    }
    if (url.isNotEmpty && !_origin.hasMatch(url)) {
      throw RegistryFormatException(
        'line "$id": url must be an absolute origin with no path or trailing slash '
        '(or "" for same-origin), got "$url"',
      );
    }

    final transport = entry['transport'];
    if (transport != null && transport is! String) {
      throw RegistryFormatException('line "$id": transport must be a string');
    }

    final weight = entry['weight'] ?? 0;
    if (weight is! num) {
      throw RegistryFormatException('line "$id": weight must be a number');
    }

    final foreign = entry['foreignOrigin'] ?? false;
    if (foreign is! bool) {
      throw RegistryFormatException(
          'line "$id": foreignOrigin must be a boolean');
    }

    lines.add(
      Line(
        id: id,
        url: url,
        transport: transport as String?,
        weight: weight.toInt(),
        foreignOrigin: foreign,
      ),
    );
  }

  return Registry(lines);
}
