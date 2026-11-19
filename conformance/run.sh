#!/usr/bin/env bash
#
# Runs the conformance suite: builds multipath-server and multipath-client from ../go into
# conformance/bin, then runs every scenario against those binaries with production timeouts, and
# prints the MEASURE lines (recovery times, resource counts) as a summary at the end.
#
# Usage:
#   conformance/run.sh                 # every scenario
#   conformance/run.sh -run Restart    # extra arguments go to `go test`
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export MP_BIN="$here/bin"
mkdir -p "$MP_BIN"
(cd "$here/../go" && go build -o "$MP_BIN/" ./cmd/multipath-server ./cmd/multipath-client)

log="$(mktemp)"
trap 'rm -f "$log"' EXIT
status=0
(cd "$here" && go test -count=1 -timeout 20m -v "$@" ./...) | tee "$log" || status=$?
echo
echo "=== measurements ==="
grep -o 'MEASURE .*' "$log" || true
exit "$status"
