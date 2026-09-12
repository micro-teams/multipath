// The browser integration point: a service worker that intercepts fetch and routes it over the
// multipath substrate instead of the network. The application keeps calling fetch() as it always
// did; underneath, every request becomes a mux stream over one redundant transport across all lines,
// and nothing in the app learns that more than one path exists.
//
// The scope and event are typed by the minimum this needs, so the package compiles against the DOM
// lib alone without pulling in the WebWorker lib (whose globals collide with the DOM's).

import { Client, ClientOptions } from './client.js';

export interface FetchEventLike {
  readonly request: Request;
  respondWith(response: Response | Promise<Response>): void;
}

export interface FetchScope {
  addEventListener(type: 'fetch', listener: (event: FetchEventLike) => void): void;
}

export interface RouterOptions extends ClientOptions {
  // Decide per request whether to route it over multipath; return false to let it hit the network
  // untouched. Default: route everything.
  shouldRoute?: (request: Request) => boolean;
  // Injected in tests; defaults to Client.dial.
  dial?: (lines: string[], opts: ClientOptions) => Promise<Client>;
}

// installFetchRouter wires a service worker's fetch handler to the substrate. The client is dialled
// lazily on the first routed request and reused; a failed dial is retried on the next request rather
// than cached, so a transient startup failure does not wedge the worker.
export function installFetchRouter(
  scope: FetchScope,
  lines: string[],
  opts: RouterOptions = {},
): void {
  const dial = opts.dial ?? Client.dial;
  let pending: Promise<Client> | null = null;
  const client = (): Promise<Client> => {
    if (pending === null) {
      pending = dial(lines, opts).catch((err) => {
        pending = null; // let the next request try again
        throw err;
      });
    }
    return pending;
  };

  scope.addEventListener('fetch', (event) => {
    if (opts.shouldRoute && !opts.shouldRoute(event.request)) return;
    event.respondWith(client().then((c) => c.fetch(event.request)));
  });
}
