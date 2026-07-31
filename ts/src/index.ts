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

export {
  LineManager,
  NoLineAvailableError,
  SENTINEL_ORIGIN,
  type Attempt,
  type LineManagerOptions,
} from "./lineManager.js";
