# Releasing

Every push publishes; tags publish differently. One workflow (`.github/workflows/ci.yml`) does the
checking and the publishing, so nothing reaches a registry that the test suite has not already had
an opinion about — `publish-*` runs only after `go`, `jvm` and `e2e` are all green.

Two packages ship, and they disagree about mutability, so they publish differently:

| Package | Where | How consumed |
|---|---|---|
| `jvm/` | GitHub Packages (Maven) | `app.microteams:multipath` |
| `go/` | nowhere | `go get github.com/micro-teams/multipath/go@<ref>` — the commit *is* the artifact |

## Branch builds — a moving pointer

Every push to any branch republishes the JVM package after the checks pass, for developing a
consumer against unreleased work. It is not a release and carries no stability promise.

```xml
<!-- Maven: -SNAPSHOT is the pointer, and resolvers know to re-check it -->
<version>0.2.0-feat--branch-SNAPSHOT</version>
```

```sh
# Go needs no publishing at all — the commit is the artifact
go get github.com/micro-teams/multipath/go@feat/branch
```

Maven's `-SNAPSHOT` is simply overwritten, so no run number accumulates. The bump is never committed
— the version in git is always the release version.

## Releases

Publishing a release is tag-driven. The workflow checks the tag agrees with the committed
`jvm/pom.xml` version before it publishes.

The Go module lives in `go/`, so Go requires its tag to carry that prefix: **`go/vX.Y.Z`**, alongside
the plain `vX.Y.Z` the publish workflow keys on. Without it `go get …/go@vX.Y.Z` reports "module
found, but does not contain package", which reads like a broken module path rather than a missing
tag.

### Cutting a release

```sh
# 1. bump jvm/pom.xml  ->  <version>0.2.0</version>
# 2. commit, then the plain tag the publish workflow keys on
git tag v0.2.0 && git push origin v0.2.0
# 3. and the Go module's own tag, which Go resolves by subdirectory prefix
git tag go/v0.2.0 && git push origin go/v0.2.0
```

A tag that disagrees with the committed version fails the job rather than publishing a surprise.

## What a human has to set up

**For publishing: nothing.** The Maven registry authenticates with the `GITHUB_TOKEN` Actions mints
per run, scoped by `permissions: packages: write` in the workflow. There is no personal access token
to create, store, rotate, or leak.

**For consuming the JVM package there is one thing:** GitHub Packages requires authentication to
*read* Maven packages **even when the repository is public** — a GitHub limitation, not a setting we
can switch off. So a consumer outside the org needs a token with `read:packages` in its Maven
settings; inside another repository's Actions the built-in `GITHUB_TOKEN` reads packages from the
same organisation for free. Go needs none of this — it resolves straight from the commit.
