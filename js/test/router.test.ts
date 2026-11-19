// The service worker's line router (src/router.ts) over a stand-in client: when a request goes to
// the network instead and that it says why, that only GET and HEAD are ever sent twice, the
// static-file integrity check, and the Cache Storage line store.
import { createHash } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import type { ClientEvent } from '../src/client.js';
import { cacheLineStore, createLineRouter, type RouterClient } from '../src/router.js';

function stand(
  opts: {
    live?: boolean;
    fails?: boolean;
    silent?: boolean;
    body?: (url: string) => BodyInit;
  } = {},
) {
  const client: RouterClient & { requests: Request[] } = {
    requests: [],
    live: async () => opts.live ?? true,
    fetch: vi.fn(async (r: Request) => {
      client.requests.push(r);
      if (opts.fails) throw new Error('stream reset by peer: target unreachable');
      if (opts.silent) return new Promise<Response>(() => {});
      return new Response(opts.body ? opts.body(r.url) : `routed ${new URL(r.url).pathname}`);
    }),
    stats: () => ({
      session: null,
      lines: [],
      streams: [],
      sessionsStarted: 0,
      lastSessionEnd: '',
    }),
    close: async () => undefined,
  };
  return client;
}

function router(client: RouterClient, extra: Record<string, unknown> = {}) {
  const events: ClientEvent[] = [];
  const network = vi.fn(async (r: Request) => new Response(`direct ${new URL(r.url).pathname}`));
  const r = createLineRouter({
    entry: 'https://app.example',
    client,
    fallback: network,
    log: () => undefined,
    onEvent: (e) => events.push(e),
    ...extra,
  });
  return { r, events, network };
}

const text = async (p: Promise<Response>) => (await p).text();

describe('line router', () => {
  it('routes over the client while a session is live', async () => {
    const { r, network } = router(stand());
    expect(await text(r.fetch(new Request('https://app.example/app/x')))).toBe('routed /app/x');
    expect(network).not.toHaveBeenCalled();
  });

  it('goes to the network, saying why, when no session is live', async () => {
    const { r, events } = router(stand({ live: false }), { fallbackAfterMs: 10 });
    expect(await text(r.fetch(new Request('https://app.example/x')))).toBe('direct /x');
    expect(events).toEqual([
      {
        type: 'fallback',
        url: 'https://app.example/x',
        reason: expect.stringMatching(/no live session/),
      },
    ]);
  });

  it('sends a GET to the network when the lines fail it or stay silent, resetting the silent one', async () => {
    const { r } = router(stand({ fails: true }));
    expect(await text(r.fetch(new Request('https://app.example/x')))).toBe('direct /x');
    const silent = stand({ silent: true });
    const { r: r2, events } = router(silent, { requestTimeoutMs: 20 });
    expect(await text(r2.fetch(new Request('https://app.example/y')))).toBe('direct /y');
    expect(silent.requests[0].signal.aborted).toBe(true);
    expect(events[0]).toMatchObject({
      type: 'fallback',
      reason: 'no answer over the lines within 20 ms',
    });
  });

  it('never sends a POST twice, but sends it directly when no session ever took it', async () => {
    const { r, network } = router(stand({ fails: true }));
    await expect(
      r.fetch(new Request('https://app.example/x', { method: 'POST', body: 'hi' })),
    ).rejects.toThrow(/target unreachable/);
    expect(network).not.toHaveBeenCalled();
    const { r: r2, network: n2 } = router(stand({ live: false }), { fallbackAfterMs: 0 });
    await r2.fetch(new Request('https://app.example/x', { method: 'POST', body: 'hi' }));
    expect(await (n2.mock.calls[0][0] as Request).text()).toBe('hi');
  });

  it('passes a file whose name carries the right hash, and fails one with the wrong hash', async () => {
    const content = 'console.log(1)\n'.repeat(5000);
    const md5 = createHash('md5').update(content).digest('hex');
    const sha = createHash('sha256').update(content).digest('hex').slice(0, 32);
    const integrity = [
      { pattern: /\/fonts\/[^/]+\.([0-9a-f]{32})\.\w+$/, algorithm: 'SHA-256' as const },
      { pattern: /[-.]([0-9a-f]{32})\.\w+$/, algorithm: 'MD5' as const },
    ];
    const client = stand({ body: () => content });
    const { r, events } = router(client, { integrity });
    expect(await text(r.fetch(new Request(`https://app.example/app/js/index-${md5}.js`)))).toBe(
      content,
    );
    expect(await text(r.fetch(new Request(`https://app.example/app/fonts/Mono.${sha}.ttf`)))).toBe(
      content,
    );
    expect(events).toEqual([]);

    const bad = `https://app.example/app/js/index-${'0'.repeat(32)}.js`;
    await expect(text(r.fetch(new Request(bad)))).rejects.toThrow(/integrity mismatch/);
    expect(events[0]).toMatchObject({
      type: 'fallback',
      url: bad,
      reason: expect.stringMatching(/^integrity mismatch: MD5/),
    });
    // From then on that file comes from the network.
    expect(await text(r.fetch(new Request(bad)))).toMatch(/^direct/);
    expect(client.requests.length).toBe(3);
  });
});

describe('cache line store', () => {
  it('round-trips the list, reads nothing as [], and refuses a corrupt list', async () => {
    const entries = new Map<string, Response>();
    const caches = {
      open: async () => ({
        match: async (k: string) => entries.get(k)?.clone(),
        put: async (k: string, r: Response) => void entries.set(k, r),
      }),
    };
    const store = cacheLineStore('c', 'https://x/__lines', caches);
    expect(await store.load()).toEqual([]);
    const lines = [{ id: 'a', url: 'https://a.example' }];
    await store.save(lines);
    expect(await store.load()).toEqual(lines);
    entries.set('https://x/__lines', new Response('[{"id":"a"}]'));
    await expect(store.load()).rejects.toThrow(/invalid line list/);
  });
});
