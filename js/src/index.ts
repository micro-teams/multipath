// The public surface of @micro-teams/multipath, the JavaScript client of MultiPath protocol version
// 3 (PROTOCOL.md): everything a page, a service worker, a React Native app or a Node program uses,
// and nothing of the layers underneath.
//
//   - Client, with ClientOptions, ClientEvent, ClientStats, LineStore and StoredLines (client.ts):
//     one session over every line of an entry; fetch(), openWebSocket(), live(), stats(), close().
//   - MultipathWebSocket (websocket.ts): what openWebSocket resolves with.
//   - createLineRouter and cacheLineStore, with LineRouter, LineRouterOptions, RouterClient and
//     IntegrityRule (router.ts): the service worker's router, with static-file integrity.
//   - Line (lines.ts), HashAlgorithm (hash.ts), WebSocketConstructor and WebSocketLike (link.ts):
//     the types those options name.
//
//     import { Client } from '@micro-teams/multipath';
//     const client = new Client({ entry: 'https://example.app' });
//     const res = await client.fetch('/index.html');
export {
  Client,
  type ClientEvent,
  type ClientOptions,
  type ClientStats,
  type LineStore,
  type StoredLines,
} from './client.js';
export type { HashAlgorithm } from './hash.js';
export type { Line } from './lines.js';
export type { WebSocketConstructor, WebSocketLike } from './link.js';
export {
  cacheLineStore,
  createLineRouter,
  type IntegrityRule,
  type LineRouter,
  type LineRouterOptions,
  type RouterClient,
} from './router.js';
export { MultipathWebSocket } from './websocket.js';
