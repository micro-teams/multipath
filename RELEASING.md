# Releasing

Every push publishes; tags publish differently. One workflow (`.github/workflows/ci.yml`) does the
checking and the publishing, so nothing reaches a registry that the test suite has not already had
an opinion about — `publish-*` runs only after `ts`, `jvm`, `go` and `e2e` are all green.

## Branch builds — a moving pointer

Every push to any branch republishes that branch, after the checks pass. This is for developing a
consumer against unreleased work; it is not a release and carries no stability promise.

```sh
# TypeScript: the dist-tag is the pointer, and always resolves to the newest build of that branch
npm install @micro-teams/multipath@main
```

```xml
<!-- Maven: -SNAPSHOT is the pointer, and resolvers know to re-check it -->
<version>0.1.0-main-SNAPSHOT</version>
```

```sh
# Go needs no publishing at all — the commit is the artifact
go get github.com/micro-teams/multipath/go@main
```

The two ecosystems need different mechanisms because they disagree about mutability. npm refuses to
replace a version that already exists, so each build gets a unique prerelease number
(`0.1.0-main.42`) and the dist-tag moves; Maven has the idea built in, so `-SNAPSHOT` is simply
overwritten and no run number accumulates. Neither bump is ever committed — the version in git is
always the release version.

## Releases

Publishing a release is tag-driven. The same workflow checks that the tag agrees with both
committed versions, and publishes:

| Package | Where | How consumed |
|---|---|---|
| `ts/` | GitHub Packages (npm) | `@micro-teams/multipath` |
| `jvm/` | GitHub Packages (Maven) | `app.microteams.multipath:multipath-spring-boot-starter` |
| `go/` | nowhere | `go get github.com/micro-teams/multipath/go@vX.Y.Z` — the tag *is* the release |

The Go module lives in `go/`, so Go requires its tag to carry that prefix: **`go/vX.Y.Z`**, alongside
the plain `vX.Y.Z` the publish workflow keys on. Without it `go get …/go@vX.Y.Z` reports "module
found, but does not contain package", which reads like a broken module path rather than a missing
tag. Cutting 0.1.1 hit exactly this.

### Cutting a release

```sh
# 1. bump all three to the same number
#    ts/package.json          ->  "version": "0.2.0"
#    jvm/pom.xml              ->  <version>0.2.0</version>
#    testbed/server/pom.xml   ->  the multipath-spring-boot-starter dependency
# 2. commit, then
git tag v0.2.0 && git push origin v0.2.0
# 3. and the Go module's own tag, which Go resolves by subdirectory prefix
git tag go/v0.2.0 && git push origin go/v0.2.0
```

The testbed one is easy to forget and the reason it must not be: `run.sh` installs `jvm/` into the
local repository and the testbed server resolves it from there, so a stale number there means the
browser suite either tests the previous starter or, if that version was never installed, fails to
resolve. Cutting 0.1.1 tripped exactly this.

A tag that disagrees with either committed version fails the job rather than publishing a surprise:
npm versions cannot be replaced once taken, so the check has to come before the publish.

## What a human has to set up

**For publishing: nothing.** Both registries authenticate with the `GITHUB_TOKEN` that Actions
mints per run, scoped by `permissions: packages: write` in the workflow. There is no personal
access token to create, store, rotate, or leak.

**For consuming, there is one thing, and it is a real nuisance worth knowing before you plan
around it:** GitHub Packages requires authentication to *read* npm and Maven packages **even when
the repository and the package are public**. This is a GitHub limitation, not a setting we can
switch off. So every consumer — a developer's laptop, MicroTeams' CI — needs a token with
`read:packages`:

```ini
# ~/.npmrc (or the consuming repo's .npmrc, with the token from an env var)
@micro-teams:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_PACKAGES_READ_TOKEN}
```

In another repository's Actions this is free — the built-in `GITHUB_TOKEN` can read packages from
the same organisation. It is only laptops and anything outside the org that need a token issued by
hand.

If that friction ever outweighs the benefit, the escape hatch is publishing `ts/` to public npmjs
instead, which needs no auth to install. That is a decision to take deliberately, not a default to
drift into.
