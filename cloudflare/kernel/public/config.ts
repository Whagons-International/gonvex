/**
 * Public-function limits (`PublicLimits`, rust/crates/runtime/src/public_functions.rs:8-35,
 * parsed in config.rs:482-516). Each variable is a positive integer; an empty
 * or absent value keeps Rust's default, and anything else is refused, as Rust
 * refuses to start (`ConfigError::Integer`).
 *
 * Not read here: `GONVEX_TRUSTED_PROXY_CIDRS`, `GONVEX_PUBLIC_TRUSTED_PROXY_IPS`
 * and `GONVEX_CLIENT_IP_HEADER`. On Cloudflare the client address is
 * `CF-Connecting-IP`, which the platform sets (CONTRACT.md C.5 "Client IP",
 * replace). `GONVEX_AUTH_TIMEOUT_SECONDS` stays K05's (`LIMITS.authTimeoutMs`).
 */

export interface PublicLimits {
  /** `GONVEX_PUBLIC_CALLS_PER_CONNECTION`: calls per connection per 60 s window. */
  connectionCalls: number;
  /** `GONVEX_PUBLIC_CALLS_PER_IP`: calls per client address per 60 s window. */
  addressCalls: number;
  /** `GONVEX_PUBLIC_MAX_PAYLOAD_BYTES`: public transport message cap, and the unscoped/anonymous cap (UTF-8 bytes). */
  payloadBytes: number;
  /** `GONVEX_PUBLIC_CONNECTIONS_PER_IP`: open public-transport sockets per address. */
  connectionsPerAddress: number;
  /** `GONVEX_PENDING_CONNECTIONS_PER_IP`: unauthenticated sockets per address, both transports. */
  pendingPerAddress: number;
  /** `GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS`, in ms: an anonymous Action's deadline. */
  actionTimeoutMs: number;
}

export const DEFAULT_PUBLIC_LIMITS: Readonly<PublicLimits> = Object.freeze({
  connectionCalls: 30,
  addressCalls: 120,
  payloadBytes: 64 * 1024,
  connectionsPerAddress: 20,
  pendingPerAddress: 100,
  actionTimeoutMs: 60_000,
});

/** The fixed call window (`Window::allow`, public_functions.rs:52-62). */
export const CALL_WINDOW_MS = 60_000;
/** The admission cache's maximum age and the monitor's period (lib.rs:2868-2870, 3888-3902; hard-coded in Rust). */
export const ADMISSION_MAX_AGE_MS = 30_000;
/** `validate_public_scope`'s timeout on the Control Plane read (lib.rs:3871-3880). */
export const ADMISSION_VALIDATE_TIMEOUT_MS = 5_000;
/** The normal Action deadline (CONTRACT.md B.6): a public Action's deadline never exceeds it. */
export const ACTION_TIMEOUT_MS = 15 * 60_000;

export interface PublicLimitsEnv {
  GONVEX_PUBLIC_CALLS_PER_CONNECTION?: string;
  GONVEX_PUBLIC_CALLS_PER_IP?: string;
  GONVEX_PUBLIC_MAX_PAYLOAD_BYTES?: string;
  GONVEX_PUBLIC_CONNECTIONS_PER_IP?: string;
  GONVEX_PENDING_CONNECTIONS_PER_IP?: string;
  GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS?: string;
}

export class PublicConfigError extends Error {}

/** config.rs `integer`: absent or blank keeps the default; otherwise a positive integer. */
function positive(name: keyof PublicLimitsEnv, value: string | undefined, fallback: number): number {
  const trimmed = value?.trim();
  if (!trimmed) return fallback;
  const n = /^[0-9]+$/.test(trimmed) ? Number(trimmed) : NaN;
  if (!Number.isSafeInteger(n) || n <= 0) throw new PublicConfigError(`${name} must be a positive integer, got ${JSON.stringify(value)}`);
  return n;
}

const cache = new WeakMap<object, PublicLimits>();

/** The limits of this Worker version, parsed once per env object. */
export function publicLimits(env: PublicLimitsEnv): PublicLimits {
  const hit = cache.get(env);
  if (hit) return hit;
  const limits: PublicLimits = {
    connectionCalls: positive("GONVEX_PUBLIC_CALLS_PER_CONNECTION", env.GONVEX_PUBLIC_CALLS_PER_CONNECTION, DEFAULT_PUBLIC_LIMITS.connectionCalls),
    addressCalls: positive("GONVEX_PUBLIC_CALLS_PER_IP", env.GONVEX_PUBLIC_CALLS_PER_IP, DEFAULT_PUBLIC_LIMITS.addressCalls),
    payloadBytes: positive("GONVEX_PUBLIC_MAX_PAYLOAD_BYTES", env.GONVEX_PUBLIC_MAX_PAYLOAD_BYTES, DEFAULT_PUBLIC_LIMITS.payloadBytes),
    connectionsPerAddress: positive("GONVEX_PUBLIC_CONNECTIONS_PER_IP", env.GONVEX_PUBLIC_CONNECTIONS_PER_IP, DEFAULT_PUBLIC_LIMITS.connectionsPerAddress),
    pendingPerAddress: positive("GONVEX_PENDING_CONNECTIONS_PER_IP", env.GONVEX_PENDING_CONNECTIONS_PER_IP, DEFAULT_PUBLIC_LIMITS.pendingPerAddress),
    actionTimeoutMs: positive("GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS", env.GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS, DEFAULT_PUBLIC_LIMITS.actionTimeoutMs / 1000) * 1000,
  };
  cache.set(env, limits);
  return limits;
}

/**
 * The deadline of an anonymous Action (execution.rs:904-915): the earlier of
 * the normal execution deadline and now + `GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS`.
 * An agent Action has no normal deadline, but an anonymous one still gets this
 * one. The Action runtime (K12) applies it; this is the one place it is computed.
 */
export function publicActionDeadline(now: number, executionDeadline: number | null, limits: Pick<PublicLimits, "actionTimeoutMs">): number {
  const limit = now + limits.actionTimeoutMs;
  return executionDeadline === null ? limit : Math.min(executionDeadline, limit);
}
