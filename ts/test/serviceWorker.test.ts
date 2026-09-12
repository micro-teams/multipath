import { describe, expect, it, vi } from 'vitest';
import { installFetchRouter, type FetchEventLike, type FetchScope } from '../src/serviceWorker.js';
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
