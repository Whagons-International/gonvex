// What the Action runtime needs from other packages. Every port is plain
// data across Workers RPC, so each one can be a Durable Object stub, a service
// binding or an in-process object.
//
// Deploy skew (0016 G0-8, S4): a tenant object can keep the previous Worker
// version for 10-40 s after a deploy. Every tenant method here takes one
// object argument and answers one object, so a later version can add fields
// without breaking an older peer, and no method is ever renamed or removed in
// the version that stops using it. `rpc.ts` turns "RPC receiver does not
// implement the method" into a retryable error.
import type { Provenance } from "./provenance.js";
import type { ActionSession } from "./session.js";

/** A Query or Reducer run in the tenant object for an Action (tool or `ctx.functions`). */
export interface TenantCallRequest {
  v: 1;
  function: string;
  kind: "query" | "reducer";
  args: unknown;
  session: ActionSession;
  /** Its `ctx.invocation` and the attribution of anything it writes. */
  provenance: Provenance;
  /** Tool bindings of kind `query`/`internalReducer` may reach internal functions. */
  allowInternal: boolean;
}

/** The tenant's answer: the result JSON, or Rust's error text for the call. */
export type TenantCallReport =
  | { ok: true; value: string; committedRevision?: number | undefined }
  | { ok: false; error: string; transient: boolean };

/** Lease protocol on the tenant's outbox (`outbox.ts`), plus the calls Actions make. */
export interface TenantActionPort {
  call(request: TenantCallRequest): Promise<TenantCallReport>;
  claim(request: ClaimRequest): Promise<Claim>;
  renew(request: RenewRequest): Promise<Renewal>;
  fail(request: FailRequest): Promise<{ v: 1; state: "retry" | "dead" | "stale" }>;
  complete(request: CompleteRequest): Promise<Completion>;
}

export interface ClaimRequest { v: 1; id: string; owner: string }
export type Claim =
  | { v: 1; ok: true; id: string; origin: "outbox" | "call"; attempt: number; path: string; args: unknown; actorAccountId: string; provenance: unknown; session: ActionSession | null; leaseMs: number; renewMs: number }
  | { v: 1; ok: false; reason: "missing" | "done" | "dead" | "leased" | "notDue"; retryAfterMs?: number };

export interface RenewRequest { v: 1; id: string; attempt: number }
export interface Renewal { v: 1; ok: boolean; persisted: boolean }

export interface FailRequest { v: 1; id: string; attempt: number; error: string; terminal?: boolean }

export interface CompleteRequest {
  v: 1;
  id: string;
  attempt: number;
  /** The result JSON (direct calls deliver it; outbox Actions discard it as Rust does). */
  value?: string | undefined;
  error?: string | undefined;
}
export interface Completion { v: 1; applied: boolean; duplicate?: boolean; rejected?: boolean; appliedAttempt?: number }

/** K06: sessions and project secrets live in the control object. */
export interface ControlPort {
  /** `ControlPlane::tenant_session_for_account`; throws Rust's error text when the Member is not admitted. */
  tenantSessionForAccount(projectId: string, tenantId: string, accountId: string): Promise<ActionSession>;
  /** `gonvex_project_env` rows for these names. Missing names are absent from the answer. */
  projectSecrets(projectId: string, names: readonly string[]): Promise<Record<string, string>>;
}

/** K11: one-shot jobs from an Action (`enqueue_scheduled`, outside any transaction). */
export interface SchedulerPort {
  /** Inserts the job (or merges a coalesced wake) and answers the surviving job id. */
  enqueue(request: ScheduleRequest): Promise<string>;
}
export interface ScheduleRequest {
  v: 1;
  /** `job_<uuid v4>`; a coalesced wake may answer an older id instead. */
  id: string;
  function: string;
  args: unknown;
  runAtUnixMs: number;
  coalesce: boolean;
  session: ActionSession;
  provenance: Record<string, unknown>;
}

/** K13: `ctx.storage` from an Action (`storage.call(runtime, session, operation, payload)`). */
export interface StoragePort {
  call(operation: string, payload: unknown, session: ActionSession): Promise<unknown>;
}

/** K20: `ctx.sandbox` (a Container). */
export interface SandboxPort {
  call(operation: string, payload: unknown, session: ActionSession, duckdb: boolean): Promise<unknown>;
}
