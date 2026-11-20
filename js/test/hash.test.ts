// The digests of src/hash.ts, checked against Node's own across the block boundaries where padding
// goes wrong, and SHA-1 against RFC 6455's worked handshake example.
import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { hexDigest, md5, sha1 } from '../src/hash.js';

const hex = (b: Uint8Array) => Buffer.from(b).toString('hex');

describe('sha1', () => {
  it('matches Node for lengths around every padding boundary', () => {
    for (const n of [0, 1, 55, 56, 63, 64, 65, 119, 120, 1000]) {
      const input = new Uint8Array(n).map((_, i) => (i * 31 + 7) & 0xff);
      expect(hex(sha1(input))).toBe(createHash('sha1').update(input).digest('hex'));
    }
  });

  it('produces the Sec-WebSocket-Accept of RFC 6455 section 1.3', () => {
    const key = 'dGhlIHNhbXBsZSBub25jZQ==258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
    const digest = sha1(new TextEncoder().encode(key));
    expect(Buffer.from(digest).toString('base64')).toBe('s3pPLMBiTxaQ9kYGzzhZRbK+xOo=');
  });
});

describe('md5 and hexDigest', () => {
  it('matches Node for lengths around every padding boundary', () => {
    for (const n of [0, 1, 55, 56, 63, 64, 65, 119, 120, 1000, 100000]) {
      const input = new Uint8Array(n).map((_, i) => (i * 31 + 7) & 0xff);
      expect(hex(md5(input))).toBe(createHash('md5').update(input).digest('hex'));
    }
  });

  it('gives lowercase hex for every algorithm', async () => {
    const input = new TextEncoder().encode('abc');
    for (const [alg, node] of [
      ['MD5', 'md5'],
      ['SHA-1', 'sha1'],
      ['SHA-256', 'sha256'],
      ['SHA-512', 'sha512'],
    ] as const) {
      expect(await hexDigest(alg, input)).toBe(createHash(node).update(input).digest('hex'));
    }
  });
});
