import { describe, expect, it } from 'vitest';
import { dechunk, parseResponse } from '../src/http1.js';

// The response half of Client.fetch, without a transport: bytes in, Response out. A server behind
// the origin (nginx, Spring) answers a JSON body of unknown length chunked, and a caller has to see
// the JSON, not the chunk framing around it.

const enc = new TextEncoder();
const bytes = (s: string) => enc.encode(s);

describe('HTTP/1.1 responses', () => {
  it('passes a Content-Length body through untouched', async () => {
    const res = parseResponse(
      bytes(
        'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 7\r\n\r\n{"a":1}',
      ),
    );
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ a: 1 });
  });

  it('de-chunks a chunked body and says how long it is', async () => {
    const res = parseResponse(
      bytes(
        'HTTP/1.1 201 Created\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n{"a"\r\n3\r\n:1}\r\n0\r\n\r\n',
      ),
    );
    expect(res.status).toBe(201);
    expect(res.headers.get('transfer-encoding')).toBeNull();
    expect(res.headers.get('content-length')).toBe('7');
    expect(await res.json()).toEqual({ a: 1 });
  });

  it('ignores chunk extensions and trailers', () => {
    expect(
      new TextDecoder().decode(dechunk(bytes('5;name=v\r\nhello\r\n0\r\nX-T: 1\r\n\r\n'))),
    ).toBe('hello');
  });

  it('refuses a truncated or malformed chunked body', () => {
    expect(() => dechunk(bytes('a\r\nshort\r\n'))).toThrow(/truncated/);
    expect(() => dechunk(bytes('zz\r\nhello\r\n0\r\n\r\n'))).toThrow(/bad chunk size/);
  });

  it('keeps a 204 bodiless', () => {
    expect(parseResponse(bytes('HTTP/1.1 204 No Content\r\n\r\n')).body).toBeNull();
  });
});
