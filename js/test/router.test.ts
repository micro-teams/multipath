import { describe, expect, it, vi } from 'vitest';
import {
  cacheLineStore,
  createLineRouter,
  installFetchRouter,
  type FetchEventLike,
  type FetchScope,
  type LineStore,
  type RoutedClient,
} from '../src/router.js';
import type { Client } from '../src/client.js';

function fakeScope() {
  let handler: ((e: FetchEventLike) => void) | null = null;
  const scope: FetchScope = { addEventListener: (_t, l) => (handler = l) };
  const dispatch = (request: Request) => {
    let responded: Response | Promise<Response> | undefined;
    handler!({ request, respondWith: (r) => (responded = r) });
    return responded;
  };
  return { scope, dispatch };
}

describe('service worker fetch router', () => {
  it('routes a request through the substrate client', async () => {
    const client = { fetch: vi.fn(async () => new Response('routed')) } as unknown as Client;
    const dial = vi.fn(async () => client);
    const { scope, dispatch } = fakeScope();
    installFetchRouter(scope, ['ws://a/', 'ws://b/'], { dial });

    const responded = dispatch(new Request('http://app/x'));
    expect(await (responded as Promise<Response>).then((r) => r.text())).toBe('routed');
    expect(dial).toHaveBeenCalledOnce();

    dispatch(new Request('http://app/y')); // client is reused, not re-dialled
    expect(dial).toHaveBeenCalledOnce();
  });

  it('leaves an unmatched request to the network', () => {
    const dial = vi.fn();
    const { scope, dispatch } = fakeScope();
    installFetchRouter(scope, ['ws://a/'], { dial, shouldRoute: (r) => r.url.includes('/api/') });
    expect(dispatch(new Request('http://app/static.js'))).toBeUndefined();
    expect(dial).not.toHaveBeenCalled();
  });
});

function fakeClient(opts: { up?: boolean; fails?: boolean } = {}) {
  const added: string[][] = [];
  let release: () => void = () => {};
  const ready = opts.up === false ? new Promise<void>((r) => (release = r)) : Promise.resolve();
  const client: RoutedClient = {
    ready: () => ready,
    closed: false,
    addLines: (lines) => (added.push(lines), lines.length),
    fetch: vi.fn(async (_s: string, r: Request) => {
      if (opts.fails) throw new Error('substrate down');
      return new Response(`routed ${new URL(r.url).pathname}`);
    }),
  };
  return { client, added, release: () => release() };
}

function memoryStore(lines: string[] = []): LineStore & { saved: string[][] } {
  const saved: string[][] = [];
  return { saved, load: async () => lines, save: async (l) => void saved.push(l) };
}

describe('line router', () => {
  it('dials the seed and the stored lines, then adds what discovery names and keeps it', async () => {
    const { client, added } = fakeClient();
    const connect = vi.fn(() => client);
    const store = memoryStore(['wss://b']);
    let answer: (lines: string[]) => void = () => {};
    const router = createLineRouter({
      seed: ['wss://a'],
      store,
      discover: () => new Promise((resolve) => (answer = resolve)),
      connect,
    });
    const res = await router.fetch(new Request('http://app/x'));
    expect(await res.text()).toBe('routed /x');
    // Dialled without waiting for discovery, which answers only now.
    expect(connect).toHaveBeenCalledWith(['wss://a', 'wss://b'], expect.anything());
    answer(['wss://a', 'wss://c']);
    await vi.waitFor(() => expect(store.saved).toEqual([['wss://a', 'wss://b', 'wss://c']]));
    expect(added).toEqual([['wss://c']]);
    expect(router.lines()).toEqual(['wss://a', 'wss://b', 'wss://c']);
  });

  it('goes to the network when no line comes up in time', async () => {
    const { client } = fakeClient({ up: false });
    const fallback = vi.fn(async () => new Response('direct'));
    const router = createLineRouter({
      seed: ['wss://a'],
      connect: () => client,
      fallback,
      fallbackAfterMs: 10,
    });
    expect(await (await router.fetch(new Request('http://app/x'))).text()).toBe('direct');
  });

  it('waits for a dial once, not once per request', async () => {
    const { client } = fakeClient({ up: false });
    const fallback = vi.fn(async () => new Response('direct'));
    const router = createLineRouter({
      seed: ['wss://a'],
      connect: () => client,
      fallback,
      fallbackAfterMs: 50,
    });
    await router.fetch(new Request('http://app/x'));
    const started = Date.now();
    expect(await (await router.fetch(new Request('http://app/y'))).text()).toBe('direct');
    expect(Date.now() - started).toBeLessThan(40);
  });

  it('a GET goes to the network when no line is up, or the substrate does not answer in time', async () => {
    const fallback = vi.fn(async () => new Response('direct'));
    const down = { ...fakeClient().client, stats: () => [{ state: 'down' }] };
    const r1 = createLineRouter({ seed: ['wss://a'], connect: () => down, fallback });
    expect(await (await r1.fetch(new Request('http://app/x'))).text()).toBe('direct');
    const silent = {
      ...fakeClient().client,
      stats: () => [{ state: 'up' }],
      fetch: () => new Promise<Response>(() => {}),
    };
    const r2 = createLineRouter({
      seed: ['wss://a'],
      connect: () => silent,
      fallback,
      requestTimeoutMs: 20,
    });
    expect(await (await r2.fetch(new Request('http://app/x'))).text()).toBe('direct');
    expect(fallback).toHaveBeenCalledTimes(2);
  });

  it('retries a failed GET over the network, but never a POST', async () => {
    const { client } = fakeClient({ fails: true });
    const fallback = vi.fn(async () => new Response('direct'));
    const router = createLineRouter({ seed: ['wss://a'], connect: () => client, fallback });
    expect(await (await router.fetch(new Request('http://app/x'))).text()).toBe('direct');
    await expect(
      router.fetch(new Request('http://app/x', { method: 'POST', body: 'hi' })),
    ).rejects.toThrow('substrate down');
    expect(fallback).toHaveBeenCalledOnce();
  });

  it('a failed discovery keeps the lines already known', async () => {
    const { client } = fakeClient();
    const router = createLineRouter({
      seed: ['wss://a'],
      connect: () => client,
      discover: async () => {
        throw new Error('offline');
      },
    });
    await router.fetch(new Request('http://app/x'));
    await router.refresh();
    expect(router.lines()).toEqual(['wss://a']);
  });
});

describe('cache line store', () => {
  it('round-trips the list and reads nothing as []', async () => {
    const entries = new Map<string, Response>();
    const caches = {
      open: async () => ({
        match: async (k: string) => entries.get(k)?.clone(),
        put: async (k: string, r: Response) => void entries.set(k, r),
      }),
    };
    const store = cacheLineStore('c', 'http://x/__lines', caches);
    expect(await store.load()).toEqual([]);
    await store.save(['wss://a', 'wss://b']);
    expect(await store.load()).toEqual(['wss://a', 'wss://b']);
  });
});
