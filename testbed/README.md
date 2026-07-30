# MultiPath testbed

A complete, self-contained deployment of MultiPath with **no business logic in it at all**, so that
the library can be proven end to end — real browser, real separate network paths, real single
backend — without depending on any consumer.

That independence is the point. Verifying MultiPath by installing it into MicroTeams would make
MicroTeams' test suite the judge of MultiPath's correctness, and would leave MultiPath unable to
test the situations that matter most: two lines delivering the same write *at the same instant*,
a line that answers slowly, a line that dies mid-request. Those are trivial to stage here and
nearly impossible to stage against a real deployment.

```
                    ┌── line-fast  :9001 ── delay   0ms ──┐
browser ── web :8000├── line-slow  :9002 ── delay 400ms ──┼──→ server :8080  (ONE instance)
                    └── line-flaky :9003 ── fails 1 in 2 ─┘
```

Every line is a separate origin as far as the browser is concerned, and every line forwards to the
**same single server process** — which is precisely the assumption the whole design rests on, so
the testbed would be lying if it did anything else.

## Parts

| | |
|---|---|
| `server/` | A tiny Spring Boot app using the `jvm/` starter. A probe, a line registry, a counting write, an adjustable-latency read. Nothing else. |
| `lines/` | A dependency-free Node proxy, one process per line, with configurable added latency, failure rate and hard stalls. This is what makes adverse timing reproducible. |
| `web/` | A static page that drives the built `ts/` package in a real browser. |
| `e2e/` | Playwright specs: the assertions that actually decide whether MultiPath works. |

## Run it locally

```sh
npm --prefix testbed/e2e install
testbed/run.sh          # builds everything, starts server + lines + web, waits for health
testbed/run.sh --e2e    # …and then runs the Playwright specs against it
```

## The counting write is the whole trick

`POST /mt/echo` takes an `op` and increments a counter for it. A test can then send the same `op`
down two lines simultaneously and assert the counter reads **1**. There is no cleverness in the
endpoint and no de-duplication in it either — any de-duplication observed is MultiPath's, which is
what makes the assertion mean something.
