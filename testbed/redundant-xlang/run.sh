#!/usr/bin/env bash
# Cross-language end-to-end for the redundant multi-link stream: a real JVM server (Kotlin
# RedundantServer via EchoServerMain) + a Go client, with a per-link fault middlebox between them
# (black-hole / one-directional / disconnect). Builds the JVM classpath, then runs the env-guarded
# Go test TestXLangJavaServerGoClient which spawns the server and drives the client.
#
# Usage: testbed/redundant-xlang/run.sh
set -euo pipefail

repo="$(cd "$(dirname "$0")/../.." && pwd)"
jvm="$repo/jvm"
go_dir="$repo/go"

echo "==> building JVM classes + dependency classpath"
( cd "$jvm" && ./mvnw -q -DskipTests compile )
cp_file="$(mktemp)"
( cd "$jvm" && ./mvnw -q dependency:build-classpath -Dmdep.outputFile="$cp_file" )
export MP_JVM_CP="$jvm/target/classes:$(cat "$cp_file")"
rm -f "$cp_file"

echo "==> running cross-language e2e (Java server + Go client + fault middlebox)"
( cd "$go_dir" && go test -run TestXLangJavaServerGoClient -count=1 -v -timeout 180s )
