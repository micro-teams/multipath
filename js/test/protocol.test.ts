// The shared vectors (spec/vectors/frames.json) against the codecs of src/protocol.ts, as the Go
// package runs them: every valid vector encodes from its fields to exactly its hex and survives a
// decode and re-encode; every invalid vector fails with the listed error. Also the line list rules
// of src/lines.ts.
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { checkLines, linkUrl, parseLines } from '../src/lines.js';
import {
  DATA_OVERHEAD,
  FrameType,
  MAX_DATA,
  Mode,
  MuxType,
  ProtocolError,
  decodeData,
  decodeHello,
  decodeReason,
  decodeReject,
  decodeU64,
  decodeWelcome,
  defaultLimits,
  encodeAck,
  encodeData,
  encodeGoodbye,
  encodeHello,
  encodeMux,
  encodePing,
  encodePong,
  encodeReject,
  encodeWelcome,
  parseFrame,
  parseMux,
} from '../src/protocol.js';

interface Vector {
  name: string;
  hex: string;
  type?: string;
  fields?: Record<string, unknown>;
  error?: string;
}
const file = JSON.parse(
  readFileSync(new URL('../../spec/vectors/frames.json', import.meta.url), 'utf8'),
) as {
  link: Vector[];
  mux: Vector[];
  invalid: Vector[];
};

const unhex = (s: string) => Uint8Array.from(s.match(/../g) ?? [], (h) => parseInt(h, 16));
const tohex = (b: Uint8Array) => Buffer.from(b).toString('hex');

function encodeLink(v: Vector): Uint8Array {
  const f = v.fields as Record<string, never>;
  switch (v.type) {
    case 'HELLO':
      return encodeHello({
        version: f.version,
        session: unhex(f.session),
        link: f.link,
        mode: f.mode === 'join' ? Mode.Join : Mode.New,
        impl: f.impl,
      });
    case 'WELCOME':
      return encodeWelcome({
        version: f.version,
        created: f.created,
        impl: f.impl,
        limits: { ...(f as object) } as never,
      });
    case 'REJECT':
      return encodeReject(f.code, f.reason);
    case 'DATA':
      return encodeData(BigInt(f.offset), unhex(f.data));
    case 'ACK':
      return encodeAck(BigInt(f.value));
    case 'PING':
      return encodePing(BigInt(f.value));
    case 'PONG':
      return encodePong(BigInt(f.value));
    case 'GOODBYE':
      return encodeGoodbye(f.reason);
    case 'UNKNOWN': {
      const p = unhex(f.payload);
      return Uint8Array.from([f.code, p.length >> 8, p.length & 0xff, ...p]);
    }
  }
  throw new Error(`unknown vector type ${v.type}`);
}

// reencode decodes one frame and encodes what it decoded.
function reencode(type: number, p: Uint8Array): Uint8Array {
  switch (type) {
    case FrameType.Hello:
      return encodeHello(decodeHello(p));
    case FrameType.Welcome:
      return encodeWelcome(decodeWelcome(p));
    case FrameType.Reject: {
      const r = decodeReject(p);
      return encodeReject(r.code, r.reason);
    }
    case FrameType.Data: {
      const d = decodeData(p);
      return encodeData(d.offset, d.data);
    }
    case FrameType.Ack:
      return encodeAck(decodeU64(p));
    case FrameType.Ping:
      return encodePing(decodeU64(p));
    case FrameType.Pong:
      return encodePong(decodeU64(p));
    case FrameType.Goodbye:
      return encodeGoodbye(decodeReason(p));
  }
  return Uint8Array.from([type, p.length >> 8, p.length & 0xff, ...p]);
}

const muxTypes: Record<string, number> = {
  SYN: MuxType.Syn,
  DATA: MuxType.Data,
  FIN: MuxType.Fin,
  RST: MuxType.Rst,
  WINDOW: MuxType.Window,
  UNKNOWN: 0x7f,
};

describe('shared vectors', () => {
  for (const v of file.link) {
    it(`link: ${v.name}`, () => {
      expect(tohex(encodeLink(v))).toBe(v.hex);
      const { type, payload } = parseFrame(unhex(v.hex), defaultLimits().maxPayload);
      expect(tohex(reencode(type, payload))).toBe(v.hex);
    });
  }
  for (const v of file.mux) {
    it(`mux: ${v.name}`, () => {
      const f = v.fields as { stream: number; body: string };
      expect(tohex(encodeMux(muxTypes[v.type!], f.stream, unhex(f.body)))).toBe(v.hex);
      const parsed = parseMux(unhex(v.hex), 0)!;
      expect(parsed.n).toBe(v.hex.length / 2);
      expect(tohex(encodeMux(parsed.frame.type, parsed.frame.stream, parsed.frame.body))).toBe(
        v.hex,
      );
    });
  }
  for (const v of file.invalid) {
    it(`invalid: ${v.name}`, () => {
      const b = unhex(v.hex);
      const decode = () => {
        if (v.type === 'mux') return parseMux(b, 0);
        const { type, payload } = parseFrame(b, DATA_OVERHEAD + MAX_DATA);
        return reencode(type, payload);
      };
      expect(decode).toThrow(ProtocolError);
      try {
        decode();
      } catch (err) {
        expect((err as ProtocolError).kind).toBe(v.error);
      }
    });
  }
});

describe('mux parsing', () => {
  it('waits for a whole frame and parses frames back to back', () => {
    const two = Uint8Array.from([
      ...encodeMux(MuxType.Syn, 1),
      ...encodeMux(MuxType.Data, 1, Uint8Array.of(9)),
    ]);
    expect(parseMux(two.subarray(0, 5), 0)).toBeNull();
    const first = parseMux(two, 0)!;
    const second = parseMux(two, first.n)!;
    expect([second.frame.type, second.frame.stream, [...second.frame.body]]).toEqual([
      MuxType.Data,
      1,
      [9],
    ]);
  });
});

describe('line list', () => {
  const doc = {
    lines: [
      { id: 'cf', url: 'https://cf.example.app', weight: 3 },
      { id: 'direct', url: 'https://example.app' },
    ],
  };

  it('accepts a valid list, ignoring other members, and names link URLs', () => {
    const lines = parseLines(doc);
    expect(lines).toEqual([
      { id: 'cf', url: 'https://cf.example.app' },
      { id: 'direct', url: 'https://example.app' },
    ]);
    expect(linkUrl(lines[0], '/multipath/v3/link')).toBe('wss://cf.example.app/multipath/v3/link');
    expect(linkUrl({ id: 'l', url: 'http://127.0.0.1:8080' }, '/l')).toBe('ws://127.0.0.1:8080/l');
  });

  it('refuses empty lists, bad or duplicate ids and URLs that are not bare origins', () => {
    expect(() => parseLines({ lines: [] })).toThrow(/1 to 255/);
    expect(() => parseLines({})).toThrow(/1 to 255/);
    expect(() => parseLines({ lines: [{ id: '', url: 'https://a.app' }] })).toThrow(/no id/);
    expect(() => parseLines({ lines: [doc.lines[0], doc.lines[0]] })).toThrow(/duplicate/);
    for (const url of [
      'https://a.app/',
      'https://a.app/x',
      'ftp://a.app',
      'https://u@a.app',
      'https://a.app?q',
    ]) {
      expect(() => parseLines({ lines: [{ id: 'a', url }] })).toThrow(/origin/);
    }
  });

  it('keeps an https entry on https lines, and lines within the site', () => {
    const lines = parseLines(doc);
    expect(() => checkLines(lines, 'https://example.app', 'example.app')).not.toThrow();
    expect(() =>
      checkLines([{ id: 'p', url: 'http://example.app' }], 'https://example.app', null),
    ).toThrow(/not https/);
    expect(() => checkLines(lines, 'https://example.app', 'cf.example.app')).toThrow(
      /outside the site/,
    );
    expect(() =>
      checkLines([{ id: 'x', url: 'https://evil.app' }], 'https://example.app', 'example.app'),
    ).toThrow(/outside the site/);
    expect(() =>
      checkLines(
        [{ id: 'x', url: 'https://notexample.app' }],
        'https://example.app',
        'example.app',
      ),
    ).toThrow(/outside the site/);
    expect(() =>
      checkLines([{ id: 'x', url: 'https://evil.app' }], 'https://example.app', null),
    ).not.toThrow();
  });
});
