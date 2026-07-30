/*
 *  Description: The line registry — the single source of truth for "which paths exist".
 *
 *               Frontend, CLI and the developer panel all read this one shape, so adding or
 *               removing a line is a registry edit and nothing else. It is maintained by hand
 *               on purpose: which free reverse proxy is worth keeping is a judgement call, not
 *               something a health check should decide. (Transient "this line is not answering
 *               right now" is a different question, and belongs to the line manager's probing.)
 *
 *  Author(s):
 *      agent4
 */

/** One network path to the origin. */
export interface Line {
  /** Stable, human-readable identifier — `cf`, `ipv6-1`, `frp-2`. Shown in the developer panel. */
  readonly id: string;
  /**
   * Absolute origin for this line (`https://cf.mt.example.app`), or the empty string meaning
   * "same origin as the page" — which is what a single-line deployment uses to keep emitting the
   * relative URLs it emitted before MultiPath existed.
   */
  readonly url: string;
  /** Free-form transport label, for diagnosis only: `cloudflare`, `ipv6`, `frp`, `proxy`. */
  readonly transport?: string;
  /**
   * Static preference, higher is better. Only breaks ties between lines whose measured latency is
   * indistinguishable; measurement outranks it, because a hand-set weight goes stale and an EWMA
   * does not.
   */
  readonly weight?: number;
  /**
   * True when the browser does not see this line under our own registrable domain — a free proxy
   * that cannot be CNAME'd.
   *
   * Note what this does *not* control: whether credentials are sent. Every line with an absolute
   * url is cross-origin to the page, so all of them need `credentials: "include"`. This flag marks
   * the narrower case of a line outside our own domain, which additionally needs `SameSite=None`
   * cookies and a CORS allowance the origin cannot derive from its own forwarded headers — a
   * fallback with reduced capability rather than a peer of the others.
   */
  readonly foreignOrigin?: boolean;
}

/** The registry document, as served by `GET /mt/lines` or baked into the build. */
export interface Registry {
  readonly lines: readonly Line[];
}

/** A registry with exactly one same-origin line: MultiPath configured to change nothing. */
export const SAME_ORIGIN_REGISTRY: Registry = {
  lines: [{ id: "same-origin", url: "", transport: "same-origin", weight: 100 }],
};

export class InvalidRegistryError extends Error {
  constructor(message: string) {
    super(`invalid line registry: ${message}`);
    this.name = "InvalidRegistryError";
  }
}

/**
 * Validate an untrusted registry document (it arrives over the network) and return it typed.
 *
 * Every rule here is one that would otherwise fail confusingly much later: a duplicate id makes
 * the panel and the metrics lie about which line served what; a trailing slash silently produces
 * `//mt/chat`; a relative non-empty url is neither a line nor same-origin. Rejecting loudly at the
 * boundary is the only place the mistake is still cheap to read.
 */
export function parseRegistry(input: unknown): Registry {
  if (typeof input !== "object" || input === null) {
    throw new InvalidRegistryError("not an object");
  }
  const lines = (input as { lines?: unknown }).lines;
  if (!Array.isArray(lines)) throw new InvalidRegistryError("`lines` is not an array");
  if (lines.length === 0) throw new InvalidRegistryError("`lines` is empty");

  const seen = new Set<string>();
  const parsed = lines.map((raw, i) => {
    if (typeof raw !== "object" || raw === null) {
      throw new InvalidRegistryError(`line ${i} is not an object`);
    }
    const { id, url, transport, weight, foreignOrigin } = raw as Record<string, unknown>;
    if (typeof id !== "string" || id === "") {
      throw new InvalidRegistryError(`line ${i} has no id`);
    }
    if (seen.has(id)) throw new InvalidRegistryError(`duplicate line id ${JSON.stringify(id)}`);
    seen.add(id);
    if (typeof url !== "string") {
      throw new InvalidRegistryError(`line ${JSON.stringify(id)} has no url`);
    }
    if (url !== "") {
      if (!/^https?:\/\/[^/]+$/.test(url)) {
        throw new InvalidRegistryError(
          `line ${JSON.stringify(id)}: url must be an absolute origin with no path or trailing ` +
            `slash (or "" for same-origin), got ${JSON.stringify(url)}`,
        );
      }
    }
    if (transport !== undefined && typeof transport !== "string") {
      throw new InvalidRegistryError(`line ${JSON.stringify(id)}: transport must be a string`);
    }
    if (weight !== undefined && (typeof weight !== "number" || !Number.isFinite(weight))) {
      throw new InvalidRegistryError(`line ${JSON.stringify(id)}: weight must be a finite number`);
    }
    if (foreignOrigin !== undefined && typeof foreignOrigin !== "boolean") {
      throw new InvalidRegistryError(`line ${JSON.stringify(id)}: foreignOrigin must be a boolean`);
    }
    const line: Line = {
      id,
      url,
      ...(transport !== undefined ? { transport } : {}),
      ...(weight !== undefined ? { weight } : {}),
      ...(foreignOrigin !== undefined ? { foreignOrigin } : {}),
    };
    return line;
  });

  return { lines: parsed };
}
