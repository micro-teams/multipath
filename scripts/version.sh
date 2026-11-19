#!/usr/bin/env bash
#
# Reads or sets the MultiPath version, and describes how a release is cut.
#
# The repository-root VERSION file is the single source of truth. The Go implementation carries the
# same number in go/protocol/version.go, where it becomes the impl string sent in HELLO and WELCOME,
# and the CI release job refuses a tag that disagrees with either. The legacy ts/ package keeps the
# version it was last released with; it speaks protocol version 2 and is not released from here.
#
# Usage:
#   scripts/version.sh             # print the version and check that both places agree
#   scripts/version.sh <version>   # set it in both places
#
# Cutting a release:
#   1. scripts/version.sh <version>, for example 0.3.0, committed and merged to main through a pull
#      request, so the released commit is on main.
#   2. Tag the merged commit twice and push both tags:
#        git tag v<version>    && git push origin v<version>
#        git tag go/v<version> && git push origin go/v<version>
#      `v<version>` runs the release check in CI; `go/v<version>` is what Go resolves, because the
#      module lives in the go/ subdirectory.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="$ROOT/VERSION"
GO_FILE="$ROOT/go/protocol/version.go"
semver_re='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'

read_go() { sed -n 's/^const ImplVersion = "\(.*\)"$/\1/p' "$GO_FILE"; }

if [[ $# -eq 0 ]]; then
  file="$(tr -d '[:space:]' < "$VERSION_FILE")"
  impl="$(read_go)"
  echo "VERSION                 $file"
  echo "go/protocol/version.go  $impl"
  if [[ "$file" != "$impl" ]]; then
    echo "error: they disagree. Run: scripts/version.sh $file" >&2
    exit 1
  fi
  exit 0
fi

new="$1"
if [[ ! "$new" =~ $semver_re ]]; then
  echo "error: version must be X.Y.Z or X.Y.Z-prerelease, got: $new" >&2
  exit 1
fi
printf '%s\n' "$new" > "$VERSION_FILE"
sed -i "s/^const ImplVersion = \".*\"$/const ImplVersion = \"$new\"/" "$GO_FILE"
exec "$0"
