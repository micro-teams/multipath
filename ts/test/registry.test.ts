import { describe, expect, it } from "vitest";
import { InvalidRegistryError, SAME_ORIGIN_REGISTRY, parseRegistry } from "../src/registry.js";

describe("parseRegistry", () => {
  it("accepts a well-formed registry and keeps line order", () => {
    const registry = parseRegistry({
      lines: [
        { id: "cf", url: "https://cf.mt.example.app", transport: "cloudflare", weight: 100 },
        { id: "ipv6-1", url: "https://ipv6-1.mt.example.app", transport: "ipv6", weight: 80 },
      ],
    });
    expect(registry.lines.map((l) => l.id)).toEqual(["cf", "ipv6-1"]);
    expect(registry.lines[0]?.weight).toBe(100);
  });

  it("accepts the empty url as same-origin", () => {
    expect(parseRegistry({ lines: [{ id: "self", url: "" }] }).lines[0]?.url).toBe("");
  });

  it("omits absent optional fields rather than filling them in", () => {
    const line = parseRegistry({ lines: [{ id: "self", url: "" }] }).lines[0]!;
    expect("weight" in line).toBe(false);
    expect("transport" in line).toBe(false);
  });

  it("round-trips the built-in same-origin registry", () => {
    expect(parseRegistry(JSON.parse(JSON.stringify(SAME_ORIGIN_REGISTRY)))).toEqual(
      SAME_ORIGIN_REGISTRY,
    );
  });

  // Each of these would otherwise surface much later as a confusing URL or a lying panel.
  it.each([
    ["not an object", 42],
    ["lines missing", {}],
    ["lines empty", { lines: [] }],
    ["line not an object", { lines: ["cf"] }],
    ["id missing", { lines: [{ url: "" }] }],
    ["id empty", { lines: [{ id: "", url: "" }] }],
    [
      "duplicate id",
      {
        lines: [
          { id: "cf", url: "" },
          { id: "cf", url: "" },
        ],
      },
    ],
    ["url missing", { lines: [{ id: "cf" }] }],
    ["url relative", { lines: [{ id: "cf", url: "/mt" }] }],
    ["url has a path", { lines: [{ id: "cf", url: "https://cf.example.app/mt" }] }],
    ["url has a trailing slash", { lines: [{ id: "cf", url: "https://cf.example.app/" }] }],
    ["url not http(s)", { lines: [{ id: "cf", url: "ws://cf.example.app" }] }],
    ["weight not a number", { lines: [{ id: "cf", url: "", weight: "high" }] }],
    ["weight not finite", { lines: [{ id: "cf", url: "", weight: Number.NaN }] }],
    ["foreignOrigin not a boolean", { lines: [{ id: "cf", url: "", foreignOrigin: "yes" }] }],
  ])("rejects: %s", (_name, input) => {
    expect(() => parseRegistry(input)).toThrow(InvalidRegistryError);
  });
});

// Reported from production: the backend serves `"transport": null` for a line whose label was not
// configured, the registry was rejected whole, and the client fell back to its single built-in
// line — with no error anywhere. It stayed invisible for as long as the deployment had one line,
// because the fallback and the truth were the same thing.
describe("a null on an optional field means absent", () => {
  it("accepts the document a JSON serializer actually produces", () => {
    const registry = parseRegistry({
      lines: [
        { id: "origin", url: "", transport: null, weight: null, foreignOrigin: null },
        {
          id: "direct",
          url: "https://direct.mt.example.app",
          transport: "direct",
          weight: 90,
          foreignOrigin: null,
        },
      ],
    });

    expect(registry.lines.map((line) => line.id)).toEqual(["origin", "direct"]);
    expect(registry.lines[0].transport).toBeUndefined();
    expect(registry.lines[0].weight).toBeUndefined();
    expect(registry.lines[1].transport).toBe("direct");
    expect(registry.lines[1].weight).toBe(90);
  });

  // The converse: a null where a value is required is a real fault, not an absence.
  it("still refuses a null id or url", () => {
    expect(() => parseRegistry({ lines: [{ id: null, url: "" }] })).toThrow(InvalidRegistryError);
    expect(() => parseRegistry({ lines: [{ id: "a", url: null }] })).toThrow(InvalidRegistryError);
  });

  it("still refuses a wrong type on an optional field", () => {
    expect(() => parseRegistry({ lines: [{ id: "a", url: "", weight: "heavy" }] })).toThrow(
      InvalidRegistryError,
    );
  });
});
