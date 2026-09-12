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

echo "==> running the Dart WEB cross-language e2e (browser-native link_web.dart, real Chrome, JVM origin)"
# The native leg above proves link_io.dart; this proves link_web.dart the same way the TS leg proves
# the browser client — except this one runs in an actual browser (dart test -p chrome), not Node,
# because link_web.dart is package:web/dart:js_interop code a JS runtime alone can't exercise. A
# browser page can't spawn the JVM origin itself, so it's started here and the port handed in via
# --dart-define.
if command -v dart >/dev/null 2>&1 &&
  { command -v google-chrome-stable >/dev/null 2>&1 || command -v google-chrome >/dev/null 2>&1 ||
    command -v chromium-browser >/dev/null 2>&1 || command -v chromium >/dev/null 2>&1; }; then
  origin_log="$(mktemp)"
  java -cp "$MP_JVM_CP" app.microteams.multipath.OriginMain 3 >"$origin_log" 2>&1 &
  origin_pid=$!
  cleanup_web_origin() { kill "$origin_pid" >/dev/null 2>&1 || true; }
  trap cleanup_web_origin EXIT
  origin_port=""
  for _ in $(seq 1 100); do
    if grep -q '^LISTENING ' "$origin_log" 2>/dev/null; then
      origin_port="$(grep '^LISTENING ' "$origin_log" | head -1 | awk '{print $2}')"
      break
    fi
    sleep 0.1
  done
  if [ -z "$origin_port" ]; then
    echo "  origin never printed LISTENING; see $origin_log" >&2
    cat "$origin_log" >&2
    exit 1
  fi
  (cd "$dart_dir" && dart pub get >/dev/null &&
    dart test -p chrome --dart-define=MP_ORIGIN_PORT="$origin_port" test/client_web_xlang_test.dart)
  cleanup_web_origin
  trap - EXIT
else
  echo "  no dart+chrome on PATH — skipping the Dart web leg (CI installs both)"
fi
