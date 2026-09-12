#!/usr/bin/env bash
#
# The end-to-end testbed: build the origin, then run every client against it over the shared wire,
# across a fault middlebox that cuts links underneath the substrate.
#
# There is one origin (the Kotlin app.microteams.multipath, run from OriginMain), one fault
# middlebox implementation (the per-link TCP cutter in the Go tests: black-hole, one-directional,
# hard disconnect), and one scenario each client drives — open a tunnel and echo bytes, open a
# normal stream and round-trip HTTP — so what is exercised is interoperation on the wire under an
# adverse network, not merely that each half compiles.
#
# Today the client is Go (the connector's language); the browser clients join the same origin and
# the same scenario when they land. The whole thing runs as ordinary processes, no containers.
#
# Usage: testbed/run.sh
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
jvm="$repo/jvm"
go_dir="$repo/go"

echo "==> building the origin (JVM classes + dependency classpath)"
(cd "$jvm" && ./mvnw -q -B -DskipTests compile)
cp_file="$(mktemp)"
(cd "$jvm" && ./mvnw -q -B dependency:build-classpath -Dmdep.outputFile="$cp_file")
export MP_JVM_CP="$jvm/target/classes:$(cat "$cp_file")"
rm -f "$cp_file"

echo "==> running the Go cross-language substrate e2e (Go client, JVM origin, fault middlebox)"
(cd "$go_dir" && go test -run TestXLang -count=1 -v -timeout 240s)

echo "==> running the TypeScript cross-language e2e (browser client over WebSocket links, JVM origin)"
ts_dir="$repo/ts"
(cd "$ts_dir" && npm ci --silent && npm run build --silent && npx vitest run xlang)

echo "==> running the Dart cross-language e2e (Dart client over WebSocket links, JVM origin)"
dart_dir="$repo/dart"
if command -v dart >/dev/null 2>&1; then
  (cd "$dart_dir" && dart pub get >/dev/null && dart test test/client_xlang_test.dart)
else
  echo "  no dart on PATH — skipping the Dart leg (CI installs one)"
fi
