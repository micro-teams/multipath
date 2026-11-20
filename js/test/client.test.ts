// The client's session lifecycle (src/client.ts) against in-memory links: which lines it dials and
// when, how a fetched list replaces the stored one, how a stored list carries a session while the
// entry is unreachable (and only for its own entry), how a list breaking the policy is refused, and
// that a server which lost the session (REJECT, or a second "created") makes the client start a new
// session. Data over a live session is proven by the conformance suite against the real server.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Client, type ClientEvent, type LineStore } from '../src/client.js';
import { Mode, RejectCode, defaultLimits, encodeReject, type HelloFrame } from '../src/protocol.js';
import { FakeWebSocket, welcomeFor } from './fakes.js';

const limits = defaultLimits();

function listFetch(...docs: unknown[]) {
  let i = 0;
  return vi.fn(async () => new Response(JSON.stringify(docs[Math.min(i++, docs.length - 1)])));
}

const lines = (...hosts: string[]) => ({
  lines: hosts.map((h, i) => ({ id: `l${i}`, url: `https://${h}` })),
});

function start(
  fetch: ReturnType<typeof listFetch>,
  extra: Partial<ConstructorParameters<typeof Client>[0]> = {},
) {
  const events: ClientEvent[] = [];
  const log: string[] = [];
  const client = new Client({
    entry: 'https://app.example',
    WebSocket: FakeWebSocket,
    fetch,
    compression: false,
    log: (m) => log.push(m),
    onEvent: (e) => events.push(e),
    ...extra,
  });
  return { client, events, log };
}

// server answers as the real server does: the first link of a session creates it, later ones join,
// and a join for a session it does not know is rejected. restart() forgets every session.
let known = new Set<string>();
const restart = () => (known = new Set());
function server(hello: HelloFrame): Uint8Array {
  const key = hello.session.join(',');
  if (known.has(key)) return welcomeFor(limits, false);
  if (hello.mode === Mode.Join) return encodeReject(RejectCode.UnknownSession, 'unknown session');
  known.add(key);
  return welcomeFor(limits, true);
}

let clients: Client[] = [];
beforeEach(() => {
  FakeWebSocket.all = [];
  FakeWebSocket.answer = server;
  restart();
});
afterEach(async () => {
  for (const c of clients) await c.close();
  clients = [];
  vi.restoreAllMocks();
});

const urls = () => FakeWebSocket.all.map((w) => w.url);

describe('client', () => {
  it('fetches the list, dials every line, and is live once a link is up', async () => {
    const { client, events } = start(listFetch(lines('a.app.example', 'b.app.example')));
    clients.push(client);
    expect(await client.live(1000)).toBe(true);
    expect(urls()).toEqual([
      'wss://a.app.example/multipath/v3/link',
      'wss://b.app.example/multipath/v3/link',
    ]);
    await vi.waitFor(() => expect(client.stats().lines.map((l) => l.state)).toEqual(['up', 'up']));
    expect(events.map((e) => e.type)).toEqual(['session-start', 'line', 'line']);
    const st = client.stats();
    expect(st.session).toMatchObject({ state: 'live', server: 'fake-server/1' });
    expect(st.sessionsStarted).toBe(1);
  });

  it('dials the stored lines at once, and the fetched list replaces them', async () => {
    const saved: unknown[] = [];
    const store: LineStore = {
      load: async () => ({ entry: 'https://app.example', lines: lines('old.app.example').lines }),
      save: async (l) => void saved.push(l),
    };
    let release: () => void = () => {};
    const gate = new Promise<void>((r) => (release = r));
    const fetch = vi.fn(async () => {
      await gate;
      return new Response(JSON.stringify(lines('new.app.example')));
    });
    const { client } = start(fetch as never, { store });
    clients.push(client);
    expect(await client.live(1000)).toBe(true);
    expect(urls()).toEqual(['wss://old.app.example/multipath/v3/link']);
    release();
    await vi.waitFor(() =>
      expect(client.stats().lines.map((l) => l.url)).toEqual(['https://new.app.example']),
    );
    expect(FakeWebSocket.all[0].closed).toBe(true);
    expect(saved).toEqual([
      { entry: 'https://app.example', lines: lines('new.app.example').lines },
    ]);
  });

  it('runs on the stored lines while the entry is unreachable, and asks it until it answers', async () => {
    const store: LineStore = {
      load: async () => ({ entry: 'https://app.example', lines: lines('old.app.example').lines }),
      save: async () => undefined,
    };
    let reachable = false;
    const fetch = vi.fn(async () => {
      if (!reachable) throw new TypeError('fetch failed: connect ECONNREFUSED');
      return new Response(JSON.stringify(lines('new.app.example')));
    });
    const { client, log } = start(fetch as never, { store });
    clients.push(client);
    expect(await client.live(1000)).toBe(true);
    expect(urls()).toEqual(['wss://old.app.example/multipath/v3/link']);
    await vi.waitFor(() => expect(log.join('\n')).toMatch(/ECONNREFUSED/));
    expect(log.join('\n')).toMatch(/stays on the kept line list, asking again in 1000 ms/);
    reachable = true;
    await vi.waitFor(
      () => expect(client.stats().lines.map((l) => l.url)).toEqual(['https://new.app.example']),
      { timeout: 3000 },
    );
    expect(client.stats().sessionsStarted).toBe(1);
  });

  it('ignores a stored list of another entry and waits for its own', async () => {
    const store: LineStore = {
      load: async () => ({ entry: 'https://other.example', lines: lines('a.app.example').lines }),
      save: async () => undefined,
    };
    const { client, log } = start(listFetch(lines('b.app.example')), { store });
    clients.push(client);
    expect(await client.live(1000)).toBe(true);
    expect(urls()).toEqual(['wss://b.app.example/multipath/v3/link']);
    expect(log.join('\n')).toMatch(/holds the list of entry https:\/\/other.example/);
  });

  it('refuses a list with a line outside the site, or plain http under an https entry', async () => {
    for (const doc of [
      lines('evil.example'),
      { lines: [{ id: 'p', url: 'http://app.example' }] },
    ]) {
      const { client, log } = start(listFetch(doc), { waitMs: 50 });
      clients.push(client);
      await expect(client.fetch('/x')).rejects.toThrow(/no live session within 50 ms/);
      expect(log.join('\n')).toMatch(/refusing the whole list/);
      expect(FakeWebSocket.all).toEqual([]);
    }
  });

  it('starts a new session when the server rejects a join as an unknown session', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0);
    const { client, events } = start(listFetch(lines('a.app.example', 'b.app.example')));
    clients.push(client);
    expect(await client.live(1000)).toBe(true);
    const first = client.stats().session!.id;
    // The server restarts: it knows no session, and one link breaks; its redial joins and is refused.
    restart();
    FakeWebSocket.all[0].drop();
    await vi.waitFor(() => expect(client.stats().sessionsStarted).toBe(2));
    expect(await client.live(1000)).toBe(true);
    expect(client.stats().session!.id).not.toBe(first);
    expect(client.stats().lastSessionEnd).toBe(
      'the server lost the session: rejected (code 2): unknown session',
    );
    expect(events.filter((e) => e.type === 'session-end')).toHaveLength(1);
  });

  it('starts a new session when a second link reports that it created the session', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0);
    const { client } = start(listFetch(lines('a.app.example', 'b.app.example')));
    clients.push(client);
    expect(await client.live(1000)).toBe(true);
    FakeWebSocket.answer = () => ((FakeWebSocket.answer = server), welcomeFor(limits, true));
    FakeWebSocket.all[0].drop(); // one link redials; the server answers as if the session were new
    await vi.waitFor(() => expect(client.stats().sessionsStarted).toBe(2));
    expect(client.stats().lastSessionEnd).toBe(
      'the server lost the session: it created the session a second time',
    );
  });

  it('refuses a platform it cannot serve as configured', () => {
    expect(
      () => new Client({ entry: 'https://app.example/path', WebSocket: FakeWebSocket }),
    ).toThrow(/origin/);
    const saved = globalThis.DecompressionStream;
    (globalThis as { DecompressionStream?: unknown }).DecompressionStream = undefined;
    try {
      expect(() => new Client({ entry: 'https://app.example', WebSocket: FakeWebSocket })).toThrow(
        /compression: false/,
      );
    } finally {
      globalThis.DecompressionStream = saved;
    }
  });
});
