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
 * no-op. The tenant only answers `ping()`: no read of tenant data, no write, no session. Throttled per isolate here
 * and per tenant in Control, so a page that reconnects, or a client that sends forged Origins, wakes a given tenant
 * at most once per interval per Control instance.
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
const PREWARM_ISOLATE_MAX = 1_024;
const recent = new Map<string, number>();
/** Test seam. */
export function clearPrewarmThrottle(): void { recent.clear(); }

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
  recent.delete(key);
  recent.set(key, now);
  if (recent.size > PREWARM_ISOLATE_MAX) recent.delete(recent.keys().next().value!);
  try {
    const control = rpc<{ prewarmTenant(project: string, selector: string): Promise<unknown> }>(env.CONTROL, "control");
    waitUntil(Promise.resolve().then(() => control.prewarmTenant(project, selector)).catch(() => undefined));
  } catch { return false; }
  return true;
}
