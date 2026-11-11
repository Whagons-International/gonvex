/** What every control-object code path is given. */
import type { Sql } from "./sql";
import type { TenantPort } from "./tenant-port";
import type { ServicePrincipalConfig, TenantRoute } from "./types";

export interface ControlConfig {
  /** `GONVEX_DASHBOARD_SESSION_SECRET`: provider secret encryption (C.3.3). */
  controlSecret?: string;
  /** `GONVEX_SERVICE_PRINCIPALS`, parsed. */
  servicePrincipals: ServicePrincipalConfig[];
  /** `GONVEX_AUTH_URL` or `GONVEX_PUBLIC_URL`: native OAuth callback base. */
  authPublicUrl?: string;
  /** `GONVEX_GOOGLE_CLIENT_ID` / `_SECRET`: fallback Google client. */
  googleClientId?: string;
  googleClientSecret?: string;
  /** `runtimeVersion` in `session.ready`. */
  runtimeVersion: string;
  /**
   * Firebase Auth Emulator decoder (docs/firebase-auth-emulator.md). Enabled
   * only when `GONVEX_TEST_AUTH_EMULATOR=true`, `GONVEX_ENV=test` and a
   * loopback `FIREBASE_AUTH_EMULATOR_HOST` are all set, which only test
   * configurations do. The reference gates the same decoder behind a debug
   * build feature; a Worker has no debug build, so the gate is configuration.
   */
  authEmulator?: { host: string };
  /** Override for Google endpoints (tests point them at a local stub). */
  googleTokenUrl?: string;
  identityToolkitUrl?: string;
}

export interface Ctx {
  sql: Sql;
  config: ControlConfig;
  /** The tenant object behind a resolved route. */
  tenant(route: TenantRoute): TenantPort;
  /** Fires `ControlChanged{project}` to every socket owner watching the project. */
  controlChanged(projectId: string): void;
  /** Network access for JWKS, Google and provider token endpoints. */
  fetch: typeof fetch;
  /** Counts tenant RPCs made for the current call. */
  tenantCalls: number;
}

/** Tenant RPC fan-out bound (0012 R15). */
export const FANOUT = 32;

/** Runs `fn` over `items` with at most `limit` in flight, preserving order. */
export async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const results = new Array<R>(items.length);
  let next = 0;
  const worker = async () => {
    for (;;) {
      const index = next++;
      if (index >= items.length) return;
      results[index] = await fn(items[index]!);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}
