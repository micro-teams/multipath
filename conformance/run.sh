#!/usr/bin/env bash
#
# Runs the conformance suite with production timeouts: builds multipath-server and multipath-client
# from ../go into conformance/bin and the JavaScript package in ../js, then runs
#   - every scenario and attack with the Go client,
#   - every client scenario again with the JavaScript client in Node (MP_CLIENT=js), and
#   - the Chromium leg (TestBrowserSmoke) when MP_BROWSER=1,
# and prints the MEASURE lines (recovery times, resource counts) as a summary at the end.
#
# Usage:
#   conformance/run.sh                      # Go and JavaScript clients
#   MP_BROWSER=1 conformance/run.sh         # and the Chromium leg (needs Playwright's Chromium)
#   MP_CLIENTS=js conformance/run.sh -run Restart   # one client; extra arguments go to `go test`
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export MP_BIN="$here/bin"
clients="${MP_CLIENTS:-go js}"
mkdir -p "$MP_BIN"
(cd "$here/../go" && go build -o "$MP_BIN/" ./cmd/multipath-server ./cmd/multipath-client)
if [[ "$clients" == *js* || "${MP_BROWSER:-}" == 1 ]]; then
  (cd "$here/../js" && npm run --silent build)
fi

log="$(mktemp)"
trap 'rm -f "$log"' EXIT
status=0
for client in $clients; do
  echo "=== the $client client ==="
  skip=()
  if [[ "$client" == js ]]; then
    # The attacks drive the server by hand and the browser leg has its own client: once is enough.
    skip=(-skip 'TestAttack|TestBrowserSmoke')
  fi
  (cd "$here" && MP_CLIENT="$client" go test -count=1 -timeout 20m -v "${skip[@]}" "$@" ./...) | tee -a "$log" || status=$?
done
echo
echo "=== measurements ==="
grep -o 'MEASURE .*' "$log" || true
exit "$status"
