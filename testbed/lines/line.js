#!/usr/bin/env node
/*
 *  Description: One network path, simulated.
 *
 *               A dependency-free HTTP proxy that forwards everything to the single origin and can
 *               be told to be slow, to fail, or to stall outright. Running one process per line
 *               gives the browser genuinely distinct origins over genuinely distinct sockets — the
 *               difference between testing MultiPath and testing a mock of it.
 *
 *               Every impairment is deterministic where it can be. "Fails one request in three" is
 *               a counter, not a coin flip: a test that passes only sometimes is not a test.
 *
 *  Author(s):
 *      agent4
 */

import http from "node:http";

const PORT = Number(process.env.LINE_PORT ?? 9001);
const TARGET_HOST = process.env.LINE_TARGET_HOST ?? "127.0.0.1";
const TARGET_PORT = Number(process.env.LINE_TARGET_PORT ?? 8080);
const NAME = process.env.LINE_NAME ?? `line-${PORT}`;

/** Milliseconds of latency added to every response — this line's handicap. */
const DELAY_MS = Number(process.env.LINE_DELAY_MS ?? 0);
/** Fail 1 request in every N (0 disables). Deterministic, not random. */
const FAIL_EVERY = Number(process.env.LINE_FAIL_EVERY ?? 0);
/** Accept the connection and then never answer — what a black-holed route looks like. */
const STALL = process.env.LINE_STALL === "1";

let seen = 0;

const server = http.createServer((req, res) => {
  seen += 1;

  // Control endpoint on the line itself, so a spec can change this line's behaviour mid-run
  // without restarting anything. Answered here and never forwarded.
  if (req.url === "/__line") {
    res.writeHead(200, { "content-type": "application/json", ...cors(req) });
    res.end(
      JSON.stringify({ name: NAME, port: PORT, delayMs: DELAY_MS, seen }),
    );
    return;
  }

  if (req.method === "OPTIONS") {
    res.writeHead(204, cors(req));
    res.end();
    return;
  }

  if (STALL) return; // hold the socket open forever, answer nothing

  if (FAIL_EVERY > 0 && seen % FAIL_EVERY === 0) {
    // 502, not a dropped connection: a client must handle the line that answers *badly*, which is
    // the more common failure and the easier one to get wrong.
    respondLater(req, res, 502, { error: "line unavailable", line: NAME });
    return;
  }

  const upstream = http.request(
    {
      host: TARGET_HOST,
      port: TARGET_PORT,
      method: req.method,
      path: req.url,
      // Tell the origin which line carried this request — that is what makes "which line served
      // it" observable from the browser.
      headers: {
        ...req.headers,
        host: `${TARGET_HOST}:${TARGET_PORT}`,
        "x-multipath-line": NAME,
      },
    },
    (upstreamRes) => {
      const chunks = [];
      upstreamRes.on("data", (c) => chunks.push(c));
      upstreamRes.on("end", () => {
        const body = Buffer.concat(chunks);
        // Buffer, then delay, then flush: the handicap must apply to the whole response, or a
        // streamed body would arrive at full speed and the line would not actually be slow.
        setTimeout(() => {
          res.writeHead(upstreamRes.statusCode ?? 502, {
            ...stripHopByHop(upstreamRes.headers),
            ...cors(req),
            "x-multipath-line": NAME,
          });
          res.end(body);
        }, DELAY_MS);
      });
    },
  );

  upstream.on("error", (err) =>
    respondLater(req, res, 502, { error: String(err), line: NAME }),
  );
  req.pipe(upstream);
});

function respondLater(req, res, status, payload) {
  setTimeout(() => {
    res.writeHead(status, {
      "content-type": "application/json",
      ...cors(req),
      "x-multipath-line": NAME,
    });
    res.end(JSON.stringify(payload));
  }, DELAY_MS);
}

/**
 * CORS headers for a credentialed cross-origin request.
 *
 * A wildcard is not an option here, and the reason is easy to get wrong: `Access-Control-Allow-Origin: *`
 * is rejected outright when the request carries credentials, and with credentials `*` in
 * Allow-Headers is taken literally rather than as "any". So the origin and the requested headers
 * are echoed back — which is also what a real deployment does (the microteams backend derives its
 * allowed origin from the forwarded headers), so the testbed models production rather than a
 * configuration nobody could actually run.
 */
function cors(req) {
  return {
    "access-control-allow-origin": req.headers.origin ?? "*",
    "access-control-allow-credentials": "true",
    "access-control-allow-headers":
      req.headers["access-control-request-headers"] ??
      "content-type,idempotency-key",
    "access-control-allow-methods": "GET,POST,PATCH,DELETE,OPTIONS",
    "access-control-expose-headers": "x-multipath-line",
    // The response now varies by who asked, so a shared cache must not reuse it across origins.
    vary: "Origin",
  };
}

/** Connection-scoped headers describe the hop we just finished, not the one we are starting. */
function stripHopByHop(headers) {
  const out = { ...headers };
  for (const h of ["connection", "keep-alive", "transfer-encoding", "upgrade"])
    delete out[h];
  return out;
}

server.listen(PORT, () => {
  const traits = [
    DELAY_MS ? `+${DELAY_MS}ms` : "no delay",
    FAIL_EVERY ? `fails 1/${FAIL_EVERY}` : "no failures",
    STALL ? "STALLS" : null,
  ].filter(Boolean);
  console.log(
    `${NAME} :${PORT} -> ${TARGET_HOST}:${TARGET_PORT} (${traits.join(", ")})`,
  );
});
