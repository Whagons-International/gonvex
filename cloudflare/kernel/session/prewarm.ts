/**
 * Tenant prewarm on the page's first socket (control-path breakdown, 2026-11-23).
 *
 * A cold load's sign-in waits for the requested tenant's admission inside `exchangeExternalToken`. When the tenant
 * object is cold, that admission waits for the object to wake: 0.3-2.2 s on dev (x18y Workers Logs: `control.auth`
 * `admitMs` 276-2201 against a 98-295 ms `lookupMs`), the largest variable part of click -> tenant auth. Rust's
 * tenants are in-process and never pay it.
 *
 * The router sees the tenant about a second earlier than the exchange: the page's first `/ws` upgrade (≈40 ms after
 * the sign-in click) carries `Origin: <scheme>://<tenant selector>.<base>` and `?project=<project id>`. Here it asks
 * Control, without waiting, to wake that tenant (`SessionControlObject.prewarmTenant`), so the wake overlaps the
 * Firebase sign-in instead of the exchange.
 *
 * The `Origin` is a hint, never authority: it selects which object to wake and nothing else. Control resolves the
 * selector exactly as `resolveTenant` does (id or domain of an active tenant of that project); an unknown one is a
 * no-op. The tenant only answers `ping()`: no read of tenant data, no write, no session. It starts only after the
 * upgrade is admitted (the K16 address limiter has run), is capped per isolate independent of the selector, and is
 * throttled per (project, selector) here and per tenant in Control. Residual cost: an anonymous client can keep a
 * known tenant resident with one admitted upgrade per 10 s (DO duration billing for a tenant that would otherwise be
 * evicted).
 */
import { rpc } from "./rpc";

export interface PrewarmEnv {
  CONTROL?: DurableObjectNamespace<any>;
  /** Kill switch: "0" disables the prewarm. */
  GONVEX_TENANT_PREWARM?: string;
  /** Dev/test only: with it set, `?prewarm=0` skips the prewarm for one upgrade (A/B measurement seam). */
  GONVEX_ASSEMBLY_FIXTURE_KEY?: string;
}

/** Upgrades of one (project, selector) in this isolate within this interval ask Control once. */
export const PREWARM_ISOLATE_INTERVAL_MS = 10_000;
export const PREWARM_ISOLATE_MAX = 1_024;
/**
 * #562 review: a cap per isolate that no client-chosen value can spread: at most this many `prewarmTenant` calls per
 * second, whatever the selectors. One call is one real page load of a tenant (each (project, selector) at most once
 * per 10 s here), and 20 page loads/s of distinct tenants through one router isolate is far above dev and staging
 * traffic. An upgrade over the cap is still served; it only skips the wake. At 20/s an isolate adds at most 20
 * one-row SQL reads per second to Control, the object every sign-in waits on.
 */
export const PREWARM_ISOLATE_PER_SECOND = 20;
const recent = new Map<string, number>();
let windowStart = 0, windowCalls = 0;
/** Test seams. */
export function clearPrewarmThrottle(): void { recent.clear(); windowStart = 0; windowCalls = 0; }
export function prewarmThrottleSize(): number { return recent.size; }

/** Insert `key` as the newest entry of an insertion-ordered map, dropping the oldest entries above `max`. */
export function rememberBounded<V>(map: Map<string, V>, key: string, value: V, max: number): void {
  map.delete(key);
  map.set(key, value);
  while (map.size > max) map.delete(map.keys().next().value!);
}

const LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const PROJECT = /^[A-Za-z0-9._:-]{1,128}$/;

/** The tenant selector a page origin names: the first DNS label of a host with at least two labels. */
export function tenantSelectorFromOrigin(origin: string | null): string | undefined {
  if (!origin || origin === "null") return undefined;
  let host: string;
  try { host = new URL(origin).hostname.toLowerCase(); } catch { return undefined; }
  const labels = host.split(".");
  if (labels.length < 2 || labels.every((label) => /^\d+$/.test(label))) return undefined; // no subdomain, or an IPv4 address
  const first = labels[0]!;
  if (!LABEL.test(first) || first === "www") return undefined;
  return first;
}

/** Starts the prewarm for this upgrade, if it names a tenant; never delays or fails the upgrade. */
export function prewarmTenantForUpgrade(request: Request, url: URL, env: PrewarmEnv, waitUntil: ((work: Promise<unknown>) => void) | undefined, now = Date.now()): boolean {
  if (!env.CONTROL || !waitUntil || env.GONVEX_TENANT_PREWARM === "0") return false;
  if (url.searchParams.get("public") === "1") return false;
  if (env.GONVEX_ASSEMBLY_FIXTURE_KEY && url.searchParams.get("prewarm") === "0") return false;
  const project = url.searchParams.get("project") ?? "";
  if (!PROJECT.test(project)) return false;
  const selector = tenantSelectorFromOrigin(request.headers.get("origin"));
  if (!selector) return false;
  const key = `${project}\u0000${selector}`;
  const last = recent.get(key);
  if (last !== undefined && now - last < PREWARM_ISOLATE_INTERVAL_MS) return false;
  if (now - windowStart >= 1_000) { windowStart = now; windowCalls = 0; }
  if (windowCalls >= PREWARM_ISOLATE_PER_SECOND) return false;
  windowCalls++;
  rememberBounded(recent, key, now, PREWARM_ISOLATE_MAX);
  try {
    const control = rpc<{ prewarmTenant(project: string, selector: string): Promise<unknown> }>(env.CONTROL, "control");
    waitUntil(Promise.resolve().then(() => control.prewarmTenant(project, selector)).catch(() => undefined));
  } catch { return false; }
  return true;
}
