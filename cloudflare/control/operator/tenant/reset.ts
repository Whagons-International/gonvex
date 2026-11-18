// A tenant object reset (CPU limit, eviction, code update) as the operator routes see it (X10, run9).
//
// While a Reducer runs past the tenant CPU limit, every call queued behind it fails when the object
// resets, with texts like "Durable Object storage is no longer accessible." or "...exceeded its CPU time
// limit and was reset". The next call reaches a fresh instance. Before this, the operator routes
// answered such a call with HTTP 500. Now a read-only call is retried on a fresh stub until the new
// instance answers; a write, which may or may not have run, is never replayed and answers the
// documented transient error (503, `retryable: true`) instead. Overload is not a reset and passes through.

import { HttpError } from "../http";
import type { OperatorTenantPort } from "./port";

/** Calls that only read, so a fresh instance may answer them again. */
const READ_ONLY = new Set<string>(["operatorListTables", "operatorReadRows", "operatorActiveMembers", "operatorMigrationStatus", "operatorLedger", "operatorServedModule"]);

const RESET = /exceeded its CPU time limit and was reset|storage is no longer accessible|Durable Object reset because|Network connection lost|Durable Object was reset/i;

/** True for the platform's reset of the tenant object (not overload, not an application error). */
export function isTenantReset(cause: unknown): boolean {
  if (!(cause instanceof Error)) return false;
  const flags = cause as Error & { overloaded?: boolean };
  if (flags.overloaded === true) return false;
  return RESET.test(cause.message);
}

/** The documented answer to a write the reset interrupted: transient, retry the request. */
export function tenantResetError(tenantId: string, cause: Error): HttpError {
  return new HttpError(503, `tenant ${JSON.stringify(tenantId)} was reset while handling this request (${cause.message}); it was not retried, retry the request`, { class: "transient", retryable: true });
}

export interface ResetOptions {
  /** Read attempts in all, each on a fresh stub. */
  attempts?: number;
  /** Wait before each retry, in ms. */
  delays?: readonly number[];
  sleep?: (ms: number) => Promise<void>;
}

/**
 * The operator's tenant port with reset handling: `stub()` makes a fresh stub, so a retry after a reset
 * reaches the new instance.
 */
export function resetTolerantTenantPort(tenantId: string, stub: () => OperatorTenantPort, options: ResetOptions = {}): OperatorTenantPort {
  const attempts = options.attempts ?? 4;
  const delays = options.delays ?? [100, 500, 1500];
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  let current = stub();
  return new Proxy({} as OperatorTenantPort, {
    get(_target, key) {
      if (typeof key !== "string" || key === "then") return undefined;
      return async (...args: unknown[]) => {
        for (let attempt = 1; ; attempt += 1) {
          try {
            return await (current as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>)[key]!(...args);
          } catch (cause) {
            if (!isTenantReset(cause)) throw cause;
            current = stub();
            if (!READ_ONLY.has(key)) throw tenantResetError(tenantId, cause as Error);
            if (attempt >= attempts) throw tenantResetError(tenantId, cause as Error);
            await sleep(delays[Math.min(attempt - 1, delays.length - 1)] ?? 0);
          }
        }
      };
    },
  });
}
