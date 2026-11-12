/**
 * The client address every per-address budget is keyed by.
 *
 * Rust trusts `GONVEX_CLIENT_IP_HEADER` only from a configured proxy and
 * otherwise uses the socket peer (`client_ip`, public_functions.rs:178-212).
 * On Cloudflare the edge sets `CF-Connecting-IP` on every request and a
 * client cannot forge it, so it is the address (CONTRACT.md C.5 "Client IP",
 * replace). A missing, repeated or unparsable value falls back to one shared
 * bucket, `0.0.0.0`, as Rust's unknown peer does (`websocket_upgrade`,
 * lib.rs:1585-1589): such callers share one conservative budget.
 *
 * Buckets (`ip_bucket`, public_functions.rs:67-79): IPv4 addresses are their
 * own bucket; IPv4-mapped IPv6 addresses share the IPv4 bucket; other IPv6
 * addresses share their /64.
 */

export const UNKNOWN_ADDRESS = "0.0.0.0";

/** The address bucket of a request (the router's input to every limiter). */
export function clientAddress(headers: Headers): string {
  const value = headers.get("cf-connecting-ip");
  // `Headers.get` joins repeated fields with ", ": ambiguous, so it fails closed (Rust: one field only).
  if (value === null || value.includes(",")) return UNKNOWN_ADDRESS;
  return addressBucket(value.trim()) ?? UNKNOWN_ADDRESS;
}

/** The bucket of one textual IP address, or null when it is not one (Rust's `IpAddr` grammar: no port, brackets or zone). */
export function addressBucket(text: string): string | null {
  const v4 = parseIpv4(text);
  if (v4) return v4.join(".");
  const v6 = parseIpv6(text);
  if (!v6) return null;
  // ::ffff:a.b.c.d shares the IPv4 budget (`to_ipv4_mapped`).
  if (v6.slice(0, 5).every(w => w === 0) && v6[5] === 0xffff) return [v6[6]! >> 8, v6[6]! & 0xff, v6[7]! >> 8, v6[7]! & 0xff].join(".");
  // The /64: the first four 16-bit groups. A canonical key, not Rust's Display text.
  return `${v6.slice(0, 4).map(w => w.toString(16)).join(":")}::/64`;
}

function parseIpv4(text: string): number[] | null {
  const parts = text.split(".");
  if (parts.length !== 4) return null;
  const out: number[] = [];
  for (const part of parts) {
    // Rust's `Ipv4Addr` parser: 1-3 decimal digits, no leading zero, at most 255.
    if (!/^[0-9]{1,3}$/.test(part) || (part.length > 1 && part[0] === "0")) return null;
    const n = Number(part);
    if (n > 255) return null;
    out.push(n);
  }
  return out;
}

function parseIpv6(text: string): number[] | null {
  if (!/^[0-9A-Fa-f:.]+$/.test(text)) return null;
  const halves = text.split("::");
  if (halves.length > 2) return null;
  const words = (half: string): number[] | null => {
    if (half === "") return [];
    const out: number[] = [];
    const groups = half.split(":");
    for (let i = 0; i < groups.length; i++) {
      const g = groups[i]!;
      if (i === groups.length - 1 && g.includes(".")) {
        const v4 = parseIpv4(g);
        if (!v4) return null;
        out.push((v4[0]! << 8) | v4[1]!, (v4[2]! << 8) | v4[3]!);
      } else {
        if (!/^[0-9A-Fa-f]{1,4}$/.test(g)) return null;
        out.push(parseInt(g, 16));
      }
    }
    return out;
  };
  const head = words(halves[0]!);
  if (!head) return null;
  if (halves.length === 1) return head.length === 8 ? head : null;
  const tail = words(halves[1]!);
  if (!tail) return null;
  // An embedded IPv4 address is only allowed at the very end.
  if (halves[0]!.includes(".")) return null;
  const missing = 8 - head.length - tail.length;
  if (missing < 1) return null;
  return [...head, ...new Array<number>(missing).fill(0), ...tail];
}
