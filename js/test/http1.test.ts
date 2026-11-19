// One HTTP/1.1 exchange (src/http1.ts) over a scripted stream: requests as bytes, responses framed by
// length, chunks or the end of the stream, interim answers skipped, gzip decoded, a buffered mode,
// and the stream released as soon as the body is complete or abandoned.
import { gzipSync } from 'node:zlib';
import { describe, expect, it } from 'vitest';
import { readResponse, serializeRequest } from '../src/http1.js';
import type { Stream } from '../src/mux.js';

const enc = new TextEncoder();

// A stream that hands out the given pieces, then its end (or hangs, with open: true).
function scripted(pieces: (string | Uint8Array)[], open = false) {
  const queue = pieces.map((p) => (typeof p === 'string' ? enc.encode(p) : p));
  const st = {
    resets: [] as string[],
    released: false,
    async read() {
      if (queue.length) return queue.shift()!;
      if (open) return new Promise<null>(() => {});
      st.released = true; // the peer's FIN; the request side is released by the exchange
      return null;
    },
    reset(why: string) {
      st.resets.push(why);
      st.released = true;
    },
  };
  return st;
}

const opt = { method: 'GET', gzip: false, buffered: false };
const read = (st: ReturnType<typeof scripted>, o = opt) => readResponse(st as unknown as Stream, o);

describe('HTTP/1.1 exchange', () => {
  it('streams a Content-Length body split anywhere, and releases the stream at its end', async () => {
    const st = scripted(
      [
        'HTTP/1.1 200 OK\r\nContent-Ty',
        'pe: application/json\r\nContent-Length: 7\r\n\r\n{"a"',
        ':1}',
      ],
      true,
    );
    const res = await read(st);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ a: 1 });
    expect(st.resets).toEqual(['response complete']);
  });

  it('decodes a chunked body with extensions and trailers', async () => {
    const st = scripted(
      [
        'HTTP/1.1 201 Created\r\nTransfer-Encoding: chunked\r\n\r\n4;x=y\r\n{"a"\r\n3\r',
        '\n:1}\r\n0\r\nX-T: 1\r\n\r\n',
      ],
      true,
    );
    const res = await read(st);
    expect(res.headers.get('transfer-encoding')).toBeNull();
    expect(await res.json()).toEqual({ a: 1 });
    expect(st.resets).toEqual(['response complete']);
  });

  it('reads to the end of the stream without a length, and fails on a short or malformed body', async () => {
    expect(await (await read(scripted(['HTTP/1.1 200 OK\r\n\r\nabc', 'def']))).text()).toBe(
      'abcdef',
    );
    await expect(
      (await read(scripted(['HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nabc']))).text(),
    ).rejects.toThrow(/6 bytes short/);
    await expect(
      (
        await read(
          scripted([
            'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nhello\r\n0\r\n\r\n',
          ]),
        )
      ).text(),
    ).rejects.toThrow(/bad chunk size/);
    await expect(read(scripted(['HTTP/9 200 OK\r\n\r\n']))).rejects.toThrow(
      /malformed status line/,
    );
  });

  it('skips interim answers and keeps 204 and HEAD bodiless', async () => {
    const res = await read(
      scripted(['HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok']),
    );
    expect(await res.text()).toBe('ok');
    expect((await read(scripted(['HTTP/1.1 204 No Content\r\n\r\n']))).body).toBeNull();
    const head = scripted(['HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n'], true);
    expect((await read(head, { ...opt, method: 'HEAD' })).body).toBeNull();
    expect(head.resets).toEqual(['response complete']);
  });

  it('decodes gzip it asked for, and refuses an encoding it did not ask for', async () => {
    const text = 'x'.repeat(10000);
    const gz = gzipSync(text);
    const st = scripted([
      `HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: ${gz.length}\r\n\r\n`,
      gz,
    ]);
    const res = await read(st, { ...opt, gzip: true });
    expect(res.headers.get('content-encoding')).toBeNull();
    expect(res.headers.get('content-length')).toBeNull();
    expect(await res.text()).toBe(text);
    await expect(
      read(scripted(['HTTP/1.1 200 OK\r\nContent-Encoding: br\r\n\r\n']), { ...opt, gzip: true }),
    ).rejects.toThrow(/encoded as br/);
  });

  it('resets the stream when the body is abandoned', async () => {
    const st = scripted(['HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nabc'], true);
    const res = await read(st);
    const reader = res.body!.getReader();
    await reader.read();
    await reader.cancel();
    expect(st.resets).toEqual(['response complete']);
  });

  it('in buffered mode hands a textual body over as text', async () => {
    const json = enc.encode('{"t":"\u00e9\u00df\u20ac"}');
    const st = scripted([
      `HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${json.length}\r\n\r\n`,
      json,
    ]);
    expect(await (await read(st, { ...opt, buffered: true })).json()).toEqual({
      t: '\u00e9\u00df\u20ac',
    });
  });

  it('writes a request with its body, Host, gzip and Connection: close', async () => {
    // React Native's Request has no `body` property; only reading it says what it carries.
    const payload = enc.encode('{"title":"t"}');
    const rn = {
      url: 'http://o.example/mt/chat?x=1',
      method: 'POST',
      headers: new Headers({ 'content-type': 'application/json' }),
      arrayBuffer: async () => payload.slice().buffer,
    } as unknown as Request;
    const text = new TextDecoder().decode(await serializeRequest(rn, true));
    expect(text).toMatch(/^POST \/mt\/chat\?x=1 HTTP\/1\.1\r\n/);
    expect(text).toMatch(/host: o\.example\r\n/);
    expect(text).toMatch(/accept-encoding: gzip\r\n/);
    expect(text).toMatch(/content-length: 13\r\n/);
    expect(text).toMatch(/connection: close\r\n/);
    expect(text.endsWith('\r\n\r\n{"title":"t"}')).toBe(true);
  });
});
