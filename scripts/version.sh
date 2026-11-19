#!/usr/bin/env bash
#
# Reads or sets the MultiPath version, and describes how a release is cut.
#
# The repository-root VERSION file is the single source of truth. The same number appears in four
# more places: go/protocol/version.go and js/src/version.ts, where it becomes the impl string sent in
# HELLO and WELCOME, and js/package.json and js/package-lock.json, which name the npm package. The
# CI release job runs this script and refuses a tag on which any of them disagrees.
#
# Usage:
#   scripts/version.sh             # print the versions and check that all places agree
#   scripts/version.sh <version>   # set it in every place
#
# Cutting a release:
#   1. scripts/version.sh <version>, for example 0.3.0, committed and merged to main through a pull
#      request, so the released commit is on main.
#   2. Tag the merged commit twice and push both tags:
#        git tag v<version>    && git push origin v<version>
#        git tag go/v<version> && git push origin go/v<version>
#      `v<version>` runs the release job in CI, which publishes @micro-teams/multipath to GitHub
#      Packages; `go/v<version>` is what Go resolves, because the module lives in the go/ subdirectory.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="$ROOT/VERSION"
GO_FILE="$ROOT/go/protocol/version.go"
JS_FILE="$ROOT/js/src/version.ts"
PKG="$ROOT/js/package.json"
LOCK="$ROOT/js/package-lock.json"
semver_re='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'

read_go() { sed -n 's/^const ImplVersion = "\(.*\)"$/\1/p' "$GO_FILE"; }
read_js() { sed -n "s/^export const IMPL_VERSION = '\(.*\)';$/\1/p" "$JS_FILE"; }
read_json() { node -p "const j = require('$1'); [j.version, ...(j.packages ? [j.packages[''].version] : [])].join(' ')"; }

if [[ $# -eq 0 ]]; then
  file="$(tr -d '[:space:]' < "$VERSION_FILE")"
  status=0
  printf '%-26s %s\n' VERSION "$file"
  for pair in "go/protocol/version.go:$(read_go)" "js/src/version.ts:$(read_js)" \
              "js/package.json:$(read_json "$PKG")" "js/package-lock.json:$(read_json "$LOCK")"; do
    printf '%-26s %s\n' "${pair%%:*}" "${pair#*:}"
    for v in ${pair#*:}; do [[ "$v" == "$file" ]] || status=1; done
  done
  if [[ $status -ne 0 ]]; then
    echo "error: they disagree. Run: scripts/version.sh $file" >&2
  fi
  exit "$status"
fi

new="$1"
if [[ ! "$new" =~ $semver_re ]]; then
  echo "error: version must be X.Y.Z or X.Y.Z-prerelease, got: $new" >&2
  exit 1
fi
printf '%s\n' "$new" > "$VERSION_FILE"
sed -i "s/^const ImplVersion = \".*\"$/const ImplVersion = \"$new\"/" "$GO_FILE"
sed -i "s/^export const IMPL_VERSION = '.*';$/export const IMPL_VERSION = '$new';/" "$JS_FILE"
(cd "$ROOT/js" && npm version "$new" --no-git-tag-version --allow-same-version > /dev/null)
exec "$0"
