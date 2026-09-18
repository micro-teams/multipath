#!/usr/bin/env bash
#
# One place to read or set the multipath version, and the only description of how a release is cut.
#
# The repo-root VERSION file is the single source of truth. Four components carry that number and
# they must all agree, because the publish workflow refuses a tag that disagrees with any of them —
# a tag is what ships, and an npm version cannot be replaced once taken.
#
#   jvm/pom.xml            the project <version>, never the parent or dependency ones
#   ts/package.json        and ts/package-lock.json, which npm keeps in step with it
#   dart/pubspec.yaml      `publish_to: none`, so this number is documentation — consumers pin the
#                          git TAG (micro-teams' app/pubspec.yaml does exactly that). Kept in step
#                          anyway, because a component that nothing enforces is a component that
#                          drifts: rc.4 and rc.5 were both cut by hand and both left this at 0.2.0.
#   go/                    has no version file at all. The COMMIT is the artifact, which is why the
#                          go/ tag below is not optional.
#
# Usage:
#   scripts/version.sh                 # print the current version + verify every file agrees
#   scripts/version.sh <version>       # set it everywhere
#   scripts/version.sh $(cat VERSION)  # re-propagate (e.g. after editing one file by hand)
#
# ---------------------------------------------------------------------------------------------
# Cutting a release
#
#   1. scripts/version.sh <version>     — e.g. 0.2.0-rc.6, or 0.2.0 for the real thing
#   2. commit it and merge to main through a PR, so the released commit is ON main and reachable
#      by history rather than only by the tag. Releases before rc.6 were cut the other way — the
#      bump lived on a dangling commit that the tag alone kept alive — which works, and leaves
#      `git log main` unable to tell you what any released version contained.
#   3. tag the MERGED commit, both tags, and push them:
#
#        git tag v<version>    && git push origin v<version>
#        git tag go/v<version> && git push origin go/v<version>
#
# Both tags, always. `v<version>` is what the publish workflow keys on; `go/v<version>` is what Go
# resolves, because the module lives in a subdirectory — without it `go get .../go@v<version>`
# reports "module found, but does not contain package", which reads like a broken module path
# rather than a missing tag.
#
# Nothing else needs doing: pushing the tag runs the checks and publishes the JVM and browser
# packages only once go, jvm, ts, dart and e2e are all green. Go needs no publishing — the commit
# is the artifact. Every push to any branch also republishes the JVM package as
# `<version>-<branch>-SNAPSHOT` for developing a consumer against unreleased work; that is not a
# release and the bump is never committed.
#
# Consuming the published packages: the JVM one needs a token with `read:packages` even though this
# repository is public — a GitHub Packages limitation, not a setting. Inside another repository's
# Actions the built-in GITHUB_TOKEN covers it. Go and Dart resolve straight from the tag.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VERSION_FILE="$ROOT/VERSION"
POM="$ROOT/jvm/pom.xml"
TS_PKG="$ROOT/ts/package.json"
TS_LOCK="$ROOT/ts/package-lock.json"
DART_PUBSPEC="$ROOT/dart/pubspec.yaml"

# Semver with an optional prerelease, because this project ships them: 0.2.0, 0.2.0-rc.5.
semver_re='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'

read_pom()    { perl -0777 -ne 'print "$1\n" if /<artifactId>multipath<\/artifactId>\s*<version>([^<]+)<\/version>/' "$POM"; }
read_ts()     { node -p "require('$TS_PKG').version"; }
read_lock()   { node -p "require('$TS_LOCK').version"; }
read_dart()   { perl -ne 'print "$1\n" if /^version:\s*(\S+)/' "$DART_PUBSPEC"; }

# ---- read/verify mode -------------------------------------------------------
if [[ $# -eq 0 ]]; then
  cur="$(tr -d '[:space:]' < "$VERSION_FILE")"
  echo "multipath version (VERSION): $cur"
  echo
  echo "as found in each component:"
  disagreed=0
  while read -r label found; do
    mark="ok"
    if [[ "$found" != "$cur" ]]; then mark="DISAGREES"; disagreed=1; fi
    printf '  %-22s %-16s %s\n' "$label" "$found" "$mark"
  done <<EOF
jvm/pom.xml $(read_pom)
ts/package.json $(read_ts)
ts/package-lock.json $(read_lock)
dart/pubspec.yaml $(read_dart)
EOF
  echo
  echo "  go/                    (no version file — the commit is the artifact)"
  echo
  if [[ $disagreed -eq 1 ]]; then
    echo "error: a component disagrees with VERSION. Run: scripts/version.sh \$(cat VERSION)" >&2
    exit 1
  fi
  echo "a release of $cur needs BOTH tags on the merged commit:"
  echo "  git tag v$cur    && git push origin v$cur"
  echo "  git tag go/v$cur && git push origin go/v$cur"
  echo "(the plain tag is what the publish workflow keys on; the go/ one is how Go finds a module"
  echo " in a subdirectory. See the header of this script for the whole procedure.)"
  exit 0
fi

# ---- set mode ---------------------------------------------------------------
new="$1"
if [[ ! "$new" =~ $semver_re ]]; then
  echo "error: version must be X.Y.Z or X.Y.Z-prerelease, got: $new" >&2
  exit 1
fi

echo "setting multipath version -> $new"

printf '%s\n' "$new" > "$VERSION_FILE"

# Only the project's own <version> — the one right after <artifactId>multipath</artifactId> — and
# never the kotlin/junit dependency versions below it.
perl -0777 -i -pe "s{(<artifactId>multipath</artifactId>\s*<version>)[^<]+(</version>)}{\${1}$new\${2}}" "$POM"

# The lockfile carries the same number twice, at the top and in the root package entry, and npm
# rewrites both. Editing the manifest alone leaves `npm ci` installing a package that disagrees
# with itself.
node -e "
const fs = require('fs');
for (const [file, apply] of [
  ['$TS_PKG', (j) => { j.version = '$new'; }],
  ['$TS_LOCK', (j) => { j.version = '$new'; if (j.packages && j.packages['']) j.packages[''].version = '$new'; }],
]) {
  const json = JSON.parse(fs.readFileSync(file, 'utf8'));
  apply(json);
  fs.writeFileSync(file, JSON.stringify(json, null, 2) + '\n');
}
"

# The package's own `version:` at column 0, not a dependency constraint.
perl -i -pe "s{^version:\s*\S+}{version: $new}" "$DART_PUBSPEC"

echo "done. verifying:"
exec "$0"
