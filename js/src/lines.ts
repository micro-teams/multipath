// The line list (PROTOCOL.md section 2): what the entry serves at its lines path and what the client
// dials. It arrives over the network, or from a store that kept an earlier copy, so every assumption
// the client later makes about it is checked here, at the boundary.
//
//   - Line: one entry of the list, a unique id and the URL of a bare http(s) origin.
//
//   - parseLines(doc) validates a parsed JSON document ({"lines": [...]}) and returns its lines;
//     no lines or more than 255, an empty or duplicate id, or a URL that is not a bare http(s) origin
//     throws. Members other than id and url are ignored.
//
//   - checkLines(lines, entry, site) applies the client's policy and throws if the list breaks it: an
//     https entry accepts only https lines (a tampered list must not downgrade the links), and when
//     site is not null every line's host must be site itself or a subdomain of it. Browsers pass the
//     site the page belongs to, so a list can never send the page's traffic to another site.
//
//   - linkUrl(line, linkPath) is the ws:// or wss:// URL of a line's link endpoint.
//
//     const lines = parseLines(await (await fetch('https://example.app/multipath/v3/lines')).json());
//     checkLines(lines, 'https://example.app', 'example.app');
//     linkUrl(lines[0], '/multipath/v3/link'); // 'wss://cf.example.app/multipath/v3/link'

export interface Line {
  id: string;
  url: string;
}

// A bare origin: no path, no trailing slash, no credentials. A trailing slash would produce "//" once
// the link path is appended.
const ORIGIN = /^https?:\/\/[^/@?#]+$/;

export function parseLines(doc: unknown): Line[] {
  const list = (doc as { lines?: unknown } | null)?.lines;
  if (!Array.isArray(list) || list.length === 0 || list.length > 255) {
    throw new Error('invalid line list: want 1 to 255 lines');
  }
  const seen = new Set<string>();
  return list.map((entry: unknown, i) => {
    const { id, url } = (entry ?? {}) as { id?: unknown; url?: unknown };
    if (typeof id !== 'string' || id === '')
      throw new Error(`invalid line list: line ${i} has no id`);
    // A duplicate id makes every log and status view lie about which line is which.
    if (seen.has(id)) throw new Error(`invalid line list: duplicate line id "${id}"`);
    seen.add(id);
    if (typeof url !== 'string' || !ORIGIN.test(url)) {
      throw new Error(
        `invalid line list: line "${id}": url must be an http(s) origin with no path`,
      );
    }
    return { id, url };
  });
}

export function checkLines(lines: Line[], entry: string, site: string | null): void {
  for (const l of lines) {
    if (entry.startsWith('https:') && !l.url.startsWith('https:')) {
      throw new Error(`line "${l.id}" is not https while the entry is: refusing the whole list`);
    }
    const host = new URL(l.url).hostname;
    if (site !== null && host !== site && !host.endsWith(`.${site}`)) {
      throw new Error(
        `line "${l.id}" (${host}) is outside the site ${site}: refusing the whole list`,
      );
    }
  }
}

export function linkUrl(line: Line, linkPath: string): string {
  return 'ws' + line.url.slice('http'.length) + linkPath;
}
