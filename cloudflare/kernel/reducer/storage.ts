// The Reducer's storage transaction: K01's `TenantStorage.invoke` with the
// steps Rust runs around the module (rust/crates/runtime/src/execution.rs
// 556-690, rust/crates/store/src/lib.rs 1759-1834):
//
//   open (claim the receipt) → module → store the receipt result →
//   reference flush → finalize (one revision, provenance) → deferred-FK check
//   → commit → publish.
//
// K04's `TenantModuleHost.run` calls `storage.invoke(kind, body)` itself, so
// the Reducer path hands it this subclass and arms it with the hooks of the
// next invocation (`arm`). Queries and unarmed invocations run as plain K01.
// Finalize goes through K03's `invokeCaptured`, which publishes the commit
// notice only after K01 reports the transaction committed.

import { invokeCaptured, type CaptureContext } from "../capture/invoke.ts";
import type { CommitNotice, FinalizeStats } from "../capture/finalize.ts";
import type { Provenance } from "../capture/provenance.ts";
import { TenantStorage, type InvocationKind, type InvocationResult, type StorageHandle, type StorageSession, type TenantStorageOptions } from "../storage/index";

/** What the Reducer path adds to one invocation's transaction. */
export interface InvocationHooks {
  readonly provenance: Provenance;
  /**
   * After the module returned a value, before finalize; still inside the
   * transaction. `value` is the result's JSON text. Throwing (a
   * `StorageError`, so K04 reports `module database capability failed: …`)
   * rolls the invocation back.
   */
  readonly afterSuccess?: (session: StorageSession, value: string) => void;
}

export interface CommitReport {
  /** The committed revision's notice (undefined: nothing committed, or no captured change). */
  readonly notice: CommitNotice | undefined;
  readonly capture: FinalizeStats;
  readonly committed: boolean;
}

/** K04's invocation body result (`TenantModuleHost.run`): the engine's JSON text on success. */
function moduleValue(value: unknown): string | undefined {
  if (value && typeof value === "object" && (value as { ok?: unknown }).ok === true) {
    const text = (value as { value?: unknown }).value;
    if (typeof text === "string") return text;
  }
  return undefined;
}

export class ReducerStorage extends TenantStorage {
  private armed: InvocationHooks | undefined;
  private report: CommitReport | undefined;

  constructor(storage: StorageHandle, readonly capture: CaptureContext, options: TenantStorageOptions = {}) {
    super(storage, options);
  }

  /** Hooks for the next `invoke` (the Reducer path is serialized, so "next" is exact). */
  arm(hooks: InvocationHooks): void {
    this.armed = hooks;
    this.report = undefined;
  }

  /** Drops hooks that no invocation took (K04 refused the call before opening storage). */
  disarm(): void {
    this.armed = undefined;
  }

  /** What the last armed invocation committed; cleared by `arm`. */
  takeReport(): CommitReport | undefined {
    const report = this.report;
    this.report = undefined;
    return report;
  }

  override async invoke<T>(kind: InvocationKind, body: (session: StorageSession) => Promise<T>): Promise<InvocationResult<T>> {
    const hooks = this.armed;
    this.armed = undefined;
    if (!hooks || kind !== "reducer") return super.invoke(kind, body);
    const invoker = { invoke: <U>(k: InvocationKind, b: (s: StorageSession) => Promise<U>) => super.invoke(k, b) };
    const result = await invokeCaptured(invoker, kind, hooks.provenance, async (session) => {
      const value = await body(session);
      const text = moduleValue(value);
      if (text !== undefined && session.aborted === undefined && !session.discarded) hooks.afterSuccess?.(session, text);
      return value;
    }, { ...this.capture, reportRows: this.writeTracking !== "statements" });
    this.report = { notice: result.notice, capture: result.capture, committed: result.ok && result.committed };
    return result;
  }
}
