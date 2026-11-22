// The line list (PROTOCOL.md section 2): what the entry serves at its lines path and what the client
// dials. It arrives over the network, or from a store that kept an earlier copy, so every assumption
// the client later makes about it is checked here, at the boundary.
//
//   - Line: one entry of the list, a unique id, the URL of a bare http(s) origin, and optionally the
//     address to connect to instead of the URL's host: an IP literal and a port. A link over an
//     address sends no TLS server name (there is none for an IP) and verifies the certificate
//     against the URL's host, so a network that filters TLS by server name sees none. A browser
//     cannot verify a certificate against another name than the one it connects to, so only a
//     client created with dialAddresses (client.ts) uses it.
//
//   - parseLines(doc) validates a parsed JSON document ({"lines": [...]}) and returns its lines;
//     no lines or more than 255, an empty or duplicate id, a URL that is not a bare http(s) origin,
//     or an address that is not an IP literal and a port throws. Members other
//     than id, url and address are ignored.
//
//   - checkLines(lines, entry, site) applies the client's policy and throws if the list breaks it: an
//     https entry accepts only https lines (a tampered list must not downgrade the links), and when
//     site is not null every line's host must be site itself or a subdomain of it. Browsers pass the
//     site the page belongs to, so a list can never send the page's traffic to another site.
//
//   - linkUrl(line, linkPath) is the ws:// or wss:// URL of a line's link endpoint; with address
//     true, the same endpoint at the line's address (for a line that has one).
//
//     const lines = parseLines(await (await fetch('https://example.app/multipath/v3/lines')).json());
//     checkLines(lines, 'https://example.app', 'example.app');
//     linkUrl(lines[0], '/multipath/v3/link'); // 'wss://cf.example.app/multipath/v3/link'
//     linkUrl({ id: 'r', url: 'https://r.example.app:8443', address: '203.0.113.7:8443' },
//       '/multipath/v3/link', true); // 'wss://203.0.113.7:8443/multipath/v3/link'

export interface Line {
  id: string;
  url: string;
  address?: string;
}

// A bare origin: no path, no trailing slash, no credentials. A trailing slash would produce "//" once
// the link path is appended.
const ORIGIN = /^https?:\/\/[^/@?#]+$/;

// An IP literal and a port, as "203.0.113.7:8443" or "[2001:db8::7]:8443". A host name is refused:
// it would be resolved and sent as the server name, which is what the address exists to avoid. The
// check is written out rather than left to URL, whose React Native version does not parse IPv6.
const ADDRESS = /^(?:(\d{1,3}(?:\.\d{1,3}){3})|\[([0-9a-f:]+)\]):([1-9]\d{0,4})$/;

function isAddress(address: string): boolean {
  const m = ADDRESS.exec(address);
  if (!m || Number(m[3]) > 65535) return false;
  if (m[1] !== undefined) return m[1].split('.').every((b) => /^(0|[1-9]\d*)$/.test(b) && +b < 256);
  const halves = m[2].split('::');
  const groups = halves.flatMap((h) => (h === '' ? [] : h.split(':')));
  if (halves.length > 2 || !groups.every((g) => /^(0|[1-9a-f][0-9a-f]{0,3})$/.test(g)))
    return false;
  return halves.length === 2 ? groups.length < 8 : groups.length === 8;
}

export function parseLines(doc: unknown): Line[] {
  const list = (doc as { lines?: unknown } | null)?.lines;
  if (!Array.isArray(list) || list.length === 0 || list.length > 255) {
    throw new Error('invalid line list: want 1 to 255 lines');
  }
  const seen = new Set<string>();
  return list.map((entry: unknown, i) => {
    const { id, url, address } = (entry ?? {}) as {
      id?: unknown;
      url?: unknown;
      address?: unknown;
    };
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
    if (address === undefined) return { id, url };
    if (typeof address !== 'string' || !isAddress(address)) {
      throw new Error(`invalid line list: line "${id}": address must be an IP literal and a port`);
    }
    return { id, url, address };
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

export function linkUrl(line: Line, linkPath: string, address = false): string {
  if (!address) return 'ws' + line.url.slice('http'.length) + linkPath;
  if (line.address === undefined) throw new Error(`line "${line.id}" has no address`);
  return (line.url.startsWith('https:') ? 'wss://' : 'ws://') + line.address + linkPath;
}
