// Capture around one K01 invocation (CONTRACT E.a.2 c4, c5).
//
// K01's `TenantStorage.invoke(kind, body)` runs `body` in one storage
// transaction and commits a Reducer when the body returns, no statement failure
// is outstanding and the deferred foreign keys hold. Capture finalizes as the
// last step of the body, so the revision, its change rows and its provenance
// commit or roll back with the Reducer, and the row report reaches K01's
// foreign key check, which runs after the body. The wake-up fires only once
// the transaction committed.
//
// Typed structurally against K01 (PR #339): this package does not import
// `kernel/storage/`.

import { CaptureState, finalize, type CommitNotice, type FinalizeStats } from "./finalize.ts";
import type { Provenance } from "./provenance.ts";
import { counters, type CaptureSql, type RowReporter } from "./sql.ts";
import type { ChangeWake } from "./wake.ts";

/** What capture needs from K01's `StorageSession`. */
export interface CapturedSession extends CaptureSql, RowReporter {
  readonly kind: "reducer" | "query";
  /** The first statement failure since the last ROLLBACK TO: the invocation will not commit. */
  readonly aborted: string | undefined;
  /** `discard()` was called: the invocation will roll back. */
  readonly discarded: boolean;
}

type Result<T> =
  | { readonly ok: true; readonly value: T; readonly committed: boolean }
  | { readonly ok: false; readonly error: unknown };

/** K01's `TenantStorage`, as far as capture uses it. */
export interface Invoker<S extends CapturedSession, R extends Result<unknown>> {
  invoke<T>(kind: "reducer" | "query", body: (session: S) => Promise<T>): Promise<R & Result<T>>;
}

export interface CaptureContext {
  readonly state: CaptureState;
  readonly wake: ChangeWake;
  /** Report written rows to K01 (`noteRows`) for the row-level FK check. */
  readonly reportRows?: boolean;
}

export type CapturedResult<R> = R & { readonly notice: CommitNotice | undefined; readonly capture: FinalizeStats };

/**
 * Runs `body` through K01 with change capture: a committed Reducer that
 * changed captured rows gets one revision with `provenance` and wakes delivery.
 */
export async function invokeCaptured<S extends CapturedSession, R extends Result<unknown>, T>(
  storage: Invoker<S, R>,
  kind: "reducer" | "query",
  provenance: Provenance,
  body: (session: S) => Promise<T>,
  context: CaptureContext,
): Promise<CapturedResult<R & Result<T>>> {
  let notice: CommitNotice | undefined;
  const stats = counters();
  const result = await storage.invoke(kind, async (session) => {
    const value = await body(session);
    // An aborted or discarded invocation will not commit; finalizing it would only replace K01's error.
    if (kind === "reducer" && session.aborted === undefined && !session.discarded) {
      notice = finalize(session, provenance, { state: context.state, stats, ...(context.reportRows ? { reporter: session } : {}) });
    }
    return value;
  });
  const committed = result.ok && result.committed ? notice : undefined;
  if (committed) context.wake.publish(committed);
  return { ...result, notice: committed, capture: stats } as CapturedResult<R & Result<T>>;
}

export { CaptureState };
