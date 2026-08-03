/*
 *  Description: Public surface of the MultiPath client package.
 *
 *  Author(s):
 *      agent4
 */

export {
  InvalidRegistryError,
  SAME_ORIGIN_REGISTRY,
  parseRegistry,
  type Line,
  type Registry,
} from "./registry.js";

export { HealthTable, type HealthOptions, type LineHealth, type LineState } from "./health.js";

export { Prober, type ProberOptions } from "./prober.js";

export { buildLauncher, type LauncherOptions } from "./launcher.js";

export { createPrecache, type PrecacheOptions } from "./serviceWorker.js";

export { mountLinePanel, type PanelOptions, type PanelSource } from "./panel.js";

export { RequestCache, type RequestCacheOptions } from "./cache.js";

export {
  connectOverLines,
  type SocketConnection,
  type SocketLike,
  type SocketOptions,
} from "./socket.js";

export {
  STRATEGY_DEFAULTS,
  hedgedRead,
  writeWithFailover,
  type Attemptor,
  type StrategyOptions,
} from "./strategy.js";

export {
  LineManager,
  NoLineAvailableError,
  SENTINEL_ORIGIN,
  type Attempt,
  type CachedCall,
  type LineManagerOptions,
} from "./lineManager.js";
