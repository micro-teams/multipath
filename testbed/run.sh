#!/usr/bin/env bash
#
#  Description: Build and start the whole testbed: one origin, three lines, one page.
#
#               Everything runs as an ordinary process — no docker. A testbed that needs a
#               container runtime is a testbed people stop running locally, and one that only ever
#               runs in CI stops being trusted.
#
#               Ports and impairments live here rather than in the specs, so a spec reads as an
#               assertion about MultiPath and not as a deployment description.
#
#  Author(s):
#      agent4
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$HERE")"

SERVER_PORT="${TESTBED_SERVER_PORT:-8080}"
WEB_PORT="${WEB_PORT:-8000}"

# id:port:delay_ms:fail_every — the topology every spec is written against.
LINES=(
  "fast:9001:0:0"
  "slow:9002:400:0"
  "flaky:9003:0:3"
  # Accepts the connection and never answers — what a black-holed route looks like from a browser,
  # and the case hedging and failover exist for.
  "stalled:9004:0:0:stall"
)

RUN_E2E=0
[[ "${1:-}" == "--e2e" ]] && RUN_E2E=1

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

say "building the library"
npm --prefix "$ROOT/ts" ci --silent
npm --prefix "$ROOT/ts" run build --silent

say "installing the starter into the local maven repository"
# The testbed depends on the published artifact, not on source: what gets exercised here is
# byte-for-byte what a consumer would resolve.
(cd "$ROOT/jvm" && ./mvnw -q -B install -DskipTests)

say "building the testbed server"
(cd "$HERE/server" && ./mvnw -q -B package -DskipTests)

say "publishing the built package to the page"
rm -rf "$HERE/web/vendor"
mkdir -p "$HERE/web/vendor"
cp "$ROOT"/ts/dist/*.js "$HERE/web/vendor/"

# The registry the server hands out, describing the lines started below.
registry=""
registry_json="["
for spec in "${LINES[@]}"; do
  IFS=: read -r id port _ _ _ <<<"$spec"
  registry+="${registry:+,}${id}=http://localhost:${port}=test=100"
  [[ "$registry_json" != "[" ]] && registry_json+=","
  registry_json+="{\"id\":\"$id\",\"url\":\"http://localhost:$port\",\"weight\":100}"
done
registry_json+="]"

# The consumer's build step, standing in for whatever a real application would do.
say "generating the launcher and the service worker"
TESTBED_REGISTRY_JSON="$registry_json" TESTBED_BUILD_VERSION="$(date +%s 2>/dev/null || echo test)" \
  node "$HERE/web/build-launcher.mjs"

say "starting the origin (one instance) on :$SERVER_PORT"
TESTBED_SERVER_PORT="$SERVER_PORT" TESTBED_LINES="$registry" \
  java -jar "$HERE"/server/target/multipath-testbed-server-*.jar &
pids+=($!)

say "starting the lines"
for spec in "${LINES[@]}"; do
  IFS=: read -r id port delay fail stall <<<"$spec"
  LINE_NAME="$id" LINE_PORT="$port" LINE_TARGET_PORT="$SERVER_PORT" \
    LINE_DELAY_MS="$delay" LINE_FAIL_EVERY="$fail" \
    LINE_STALL="$([[ "$stall" == "stall" ]] && echo 1 || echo 0)" \
    node "$HERE/lines/line.js" &
  pids+=($!)
done

say "starting the page on :$WEB_PORT"
WEB_PORT="$WEB_PORT" node "$HERE/web/serve.js" &
pids+=($!)

say "waiting for everything to answer"
wait_for() {
  local url="$1" name="$2"
  for _ in $(seq 1 120); do
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then
      echo "  $name ready"
      return 0
    fi
    sleep 1
  done
  echo "  $name did NOT come up: $url" >&2
  return 1
}
wait_for "http://localhost:$SERVER_PORT/mt/probe" "origin"
for spec in "${LINES[@]}"; do
  IFS=: read -r id port _ _ _ <<<"$spec"
  # The flaky line fails one request in three by design, so retry rather than trust one probe.
  # The stalled line still answers its own control endpoint; only proxied traffic is black-holed.
  wait_for "http://localhost:$port/__line" "line $id"
done
wait_for "http://localhost:$WEB_PORT/" "page"

if [[ $RUN_E2E == 1 ]]; then
  say "running the end-to-end specs"
  TESTBED_WEB_URL="http://localhost:$WEB_PORT" \
    TESTBED_SERVER_URL="http://localhost:$SERVER_PORT" \
    npm --prefix "$HERE/e2e" test
  say "specs passed"
else
  say "testbed up — page at http://localhost:$WEB_PORT (ctrl-c to stop)"
  wait
fi
