/**
 * What the session layer uses of K16: the limits, the per-address budgets
 * (through the address's limiter object) and the admission re-check.
 *
 *  - The router reserves the pending and public slots before the upgrade
 *    (`admitUpgrade`, HTTP 429 as Rust).
 *  - The socket owner (a gateway) releases them: the pending slot when the
 *    socket first authenticates, every held slot when it closes.
 *  - Each public-transport frame is charged to the address window here and to
 *    the connection window in the socket's own record (`kernel/session`).
 */
import type { ControlPort } from "../session/ports";
import { ADMISSION_VALIDATE_TIMEOUT_MS, type PublicLimits, type PublicLimitsEnv, publicLimits } from "./config";
import { type AdmitResult, limiterFor, type SlotKind } from "./limiter";

export interface PublicGuardEnv extends PublicLimitsEnv {
  /** The address limiter objects (`PublicLimiterObject`). Without it, per-address budgets are off (test Workers only). */
  PUBLIC_LIMITER?: DurableObjectNamespace<any>;
}

export interface PublicGuard {
  readonly limits: PublicLimits;
  /** Charges `cost` calls to the address window; false when the window has no room. */
  allowAddress(address: string, cost: number): Promise<boolean>;
  /** Frees the slots a connection held. Idempotent. */
  release(address: string, connectionId: string, kinds: SlotKind[]): Promise<void>;
}

export function publicGuard(env: PublicGuardEnv): PublicGuard {
  const limits = publicLimits(env);
  const namespace = env.PUBLIC_LIMITER;
  if (!namespace) {
    return { limits, allowAddress: async () => true, release: async () => {} };
  }
  return {
    limits,
    async allowAddress(address, cost) {
      try {
        return await limiterFor(namespace, address).allow({ v: 1, cost, limit: limits.addressCalls });
      } catch (error) {
        // Fail-open by ruling (0045): the connection budget still applies, and refusing every public call while one
        // object is unreachable would turn a platform hiccup into an outage of every public page.
        console.error("public guard: address budget unavailable; allowing", error);
        return true;
      }
    },
    async release(address, connectionId, kinds) {
      if (!kinds.length) return;
      try {
        await limiterFor(namespace, address).release({ v: 1, connectionId, kinds });
      } catch (error) {
        // The limiter's audit frees it once the bucket is full (limiter.ts).
        console.error("public guard: release failed", error);
      }
    },
  };
}

/** The answer when the address limiter cannot be reached: the upgrade is refused (0045), not admitted unchecked. */
export const LIMITER_UNAVAILABLE = "connection limiter is unavailable";

/**
 * The router's reservation before the upgrade (`websocket_upgrade`,
 * lib.rs:1590-1617). Null when admitted; otherwise the 429 response Rust sends
 * (axum's `(StatusCode, &str)`: a text/plain body). When the limiter cannot be
 * reached the upgrade is refused with 503 (0045): admitting it would let a
 * limiter outage lift the socket caps. The SDK retries a refused upgrade as any
 * failed connect.
 */
export async function admitUpgrade(
  env: PublicGuardEnv,
  request: { address: string; connectionId: string; gateway: string; publicTransport: boolean },
): Promise<Response | null> {
  if (!env.PUBLIC_LIMITER) return null;
  const limits = publicLimits(env);
  let result: AdmitResult;
  try {
    result = await limiterFor(env.PUBLIC_LIMITER, request.address).admit({
      v: 1, connectionId: request.connectionId, gateway: request.gateway, publicTransport: request.publicTransport,
      pendingLimit: limits.pendingPerAddress, publicLimit: limits.connectionsPerAddress,
    });
  } catch (error) {
    console.error("public guard: connection limiter unavailable; refusing the upgrade", error);
    return new Response(LIMITER_UNAVAILABLE, { status: 503, headers: { "content-type": "text/plain; charset=utf-8" } });
  }
  if (result.ok) return null;
  return new Response(result.error, { status: 429, headers: { "content-type": "text/plain; charset=utf-8" } });
}

/** `validate_public_scope` (lib.rs:3871-3880): the tenant still resolves for public access, within 5 s. */
export async function validatePublicScope(control: Pick<ControlPort, "resolvePublicTenant">, scope: readonly [string, string]): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timeout = new Promise<false>((resolve) => { timer = setTimeout(() => resolve(false), ADMISSION_VALIDATE_TIMEOUT_MS); });
    const check = control.resolvePublicTenant(scope[0], scope[1]).then((r) => r.ok, () => false);
    return await Promise.race([check, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
