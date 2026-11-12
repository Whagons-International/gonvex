/**
 * Token buckets for the Ingress Worker (K18). The gateway's limits are
 * `<per second>/<burst>` token buckets keyed by `{project}\0{tenant}`
 * (whagons-api-gateway `ratelimit.rs`); these are the same, with a cap on how
 * many keys one limiter holds so a flood of distinct keys cannot grow it.
 */

export interface RateLimit {
  perSecond: number;
  burst: number;
}

/** whagons-api-gateway `GATEWAY_TENANT_RATE_LIMIT` default. */
export const WEBHOOK_RATE: RateLimit = { perSecond: 50, burst: 100 };
/** Share pages and images: link-preview fetchers ask a few times per link. */
export const SHARE_RATE: RateLimit = { perSecond: 20, burst: 40 };
/**
 * Tenant lookups that reach the control object (cache misses), per project
 * and isolate. Random selectors are misses; this bounds what they cost the
 * control object, which also serves sign-ins (0043 §5).
 */
export const LOOKUP_RATE: RateLimit = { perSecond: 20, burst: 40 };
/** Keys one limiter remembers; past this the least recently used is dropped. */
export const MAX_BUCKETS = 10_000;

/** `<per second>[/<burst>]`, as the gateway parses its limits. `0` disables. */
export function parseRate(value: string | undefined, fallback: RateLimit): RateLimit {
  if (!value || !value.trim()) return fallback;
  const [rate, burst] = value.split("/", 2).map((part) => Number(part.trim()));
  const perSecond = rate ?? Number.NaN;
  const size = burst === undefined ? Math.max(1, perSecond) : burst;
  if (!Number.isFinite(perSecond) || !Number.isFinite(size) || perSecond < 0 || size < 1) return fallback;
  return { perSecond, burst: size };
}

interface Bucket {
  tokens: number;
  at: number;
}

/** Named limits, each a token bucket per key (the key defaults to the name). */
export class Limiter {
  private readonly buckets = new Map<string, Bucket>();

  constructor(private readonly limits: Record<string, RateLimit>, private readonly maxBuckets = MAX_BUCKETS) {}

  take(name: string, nowMs: number, key = ""): boolean {
    const limit = this.limits[name];
    if (!limit || limit.perSecond === 0) return true;
    const id = `${name}\u0000${key}`;
    const bucket = this.buckets.get(id) ?? { tokens: limit.burst, at: nowMs };
    bucket.tokens = Math.min(limit.burst, bucket.tokens + ((nowMs - bucket.at) / 1000) * limit.perSecond);
    bucket.at = nowMs;
    // Re-insert so iteration order is least recently used first.
    this.buckets.delete(id);
    this.buckets.set(id, bucket);
    while (this.buckets.size > this.maxBuckets) this.buckets.delete(this.buckets.keys().next().value!);
    if (bucket.tokens < 1) return false;
    bucket.tokens -= 1;
    return true;
  }

  get size(): number {
    return this.buckets.size;
  }
}
