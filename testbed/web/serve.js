#!/usr/bin/env node
/*
 *  Description: Static file server for the testbed page. Dependency-free by design — the testbed
 *               must not need an install step to be startable, or CI grows a way to be broken that
 *               has nothing to do with MultiPath.
 *
 *  Author(s):
 *      agent4
 */

import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.WEB_PORT ?? 8000);

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".css": "text/css; charset=utf-8",
};

http
  .createServer(async (req, res) => {
    const requested = decodeURIComponent((req.url ?? "/").split("?")[0]);
    const relative = requested === "/" ? "index.html" : requested.replace(/^\/+/, "");
    const file = path.resolve(ROOT, relative);

    // Resolve first, then check containment: without this, "/../../etc/passwd" is served.
    if (!file.startsWith(ROOT + path.sep) && file !== path.join(ROOT, "index.html")) {
      res.writeHead(403).end("forbidden");
      return;
    }

    try {
      const body = await fs.readFile(file);
      res.writeHead(200, {
        "content-type": TYPES[path.extname(file)] ?? "application/octet-stream",
        // The page is rebuilt between runs; a cached copy would silently test the previous build.
        "cache-control": "no-store",
      });
      res.end(body);
    } catch {
      res.writeHead(404).end("not found");
    }
  })
  .listen(PORT, () => console.log(`web :${PORT} (${ROOT})`));
