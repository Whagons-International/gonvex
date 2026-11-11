/**
 * What the operator surface (K19) asks of a tenant object.
 *
 * The control object never opens tenant SQLite; each reference code path in
 * operator_data.rs, operations.rs and the `/dev/sync` rollout that touches a
 * tenant database maps to one call here. `TenantOperator` (`./operator.ts`)
 * implements the data and migration calls on a tenant object's storage with
 * K01, K02 and K03; the tenant object exposes them as RPC methods.
 *
 * Results are plain JSON. Failures come back as `{ ok:false, status, error }`
 * values, because a thrown error loses its text and status across RPC.
 */
import type { MigrationStatus } from "../../../kernel/migrate/migrator.ts";
import { sqliteFailure } from "../../../kernel/storage/errors.ts";

export type PortResult<T> = { ok: true; value: T } | { ok: false; status: number; error: string };

/** `operator_data.rs` `DataQuery` for row reads. */
export interface RowsQuery {
  limit: number;
  offset: number;
  search: string;
  sort: string;
  direction: string;
  filters: string;
  columns: string;
  count: string;
}

export interface TableSummary {
  name: string;
  columns: string[];
  rowCount: number;
  rowCountEstimated: boolean;
}

export interface RowsPage {
  table: string;
  columns: string[];
  rows: unknown[];
  total: number;
  offset: number;
  limit: number;
}

export interface ActiveMemberRow {
  id: string;
  accountId: string;
  displayName: string;
  role: string;
  permissions: unknown;
}

export interface ReplaceResult {
  textRows: number;
  jsonRows: number;
  columns: unknown[];
  dryRun: boolean;
}

/** The module a tenant object serves (the Worker version's baked release, 0016 G0-2). */
export interface ServedModule {
  projectId: string;
  hash: string;
  generation: number;
  functions: number;
  loadMs: number;
  error: string | null;
}

export interface OperatorTenantPort {
  // ------------------------------------------------ rollout (D.2.4, K02)
  /**
   * Starts or continues `planId` (K02 `TenantMigrator.applyRelease`); one
   * event's slice of work. `projectId` tells the object which project it
   * belongs to (it is named by tenant id).
   */
  operatorApplyRelease(planId: string, projectId?: string): Promise<MigrationStatus>;
  operatorMigrationStatus(): Promise<MigrationStatus>;
  /** `gonvex_migrations`, in name order. */
  operatorLedger(): Promise<Array<{ name: string; checksum: string; applied_at: number; duration_ms: number }>>;
  /** The release this object's Worker version carries for its project, if any. */
  operatorServedModule(projectId: string): Promise<ServedModule | null>;
  /** `RuntimeEvent::ModuleReloaded` after activation: sockets get `system.reload` or `client.updateRequired`. */
  operatorModuleReloaded(event: { projectId: string; hash: string; generation: number; clientContract: number | null }): Promise<void>;
  /** `DELETE /dev/tenants/{t}`, `DELETE /dev/projects/{p}`: the database file's removal. */
  operatorDeleteAll(): Promise<void>;

  // ------------------------------------------------ data browser (operator_data.rs)
  operatorListTables(): Promise<PortResult<TableSummary[]>>;
  operatorReadRows(table: string, query: RowsQuery): Promise<PortResult<RowsPage>>;
  operatorInsertRow(table: string, payload: Record<string, unknown>): Promise<PortResult<unknown>>;
  operatorUpdateRow(table: string, rowId: string, payload: Record<string, unknown>): Promise<PortResult<unknown>>;
  operatorDeleteRow(table: string, rowId: string): Promise<PortResult<unknown>>;
  operatorReplaceReferences(replacements: Record<string, string>, dryRun: boolean): Promise<PortResult<ReplaceResult>>;

  // ------------------------------------------------ memberships (operations.rs)
  /** `SELECT id,account_id,display_name,role,permissions FROM members WHERE status='active' ORDER BY lower(display_name),id`. */
  operatorActiveMembers(): Promise<PortResult<ActiveMemberRow[]>>;
  /** Revokes a member in a captured tenant transaction; `false` when no row matched. */
  operatorRevokeMember(memberId: string): Promise<PortResult<boolean>>;
}

/**
 * What a tenant object calls on the control object: after a commit that
 * changed rows (any writer, Reducers included), so dashboard data reads on
 * that tenant re-run (dashboard.rs subscribes to the tenant change feed).
 */
export interface OperatorDataSink {
  operatorDataChanged(projectId: string, tenantId: string): Promise<void>;
}

/**
 * Applies `planId` to one tenant object and waits for the outcome. K02's
 * `applyRelease` runs one event's slice and the object's alarm continues; it
 * restarts a plan that is already done (every release re-runs the prelude), so
 * it is called once and then the status is polled. It is called again only
 * when the object lost track of the plan (a reset before its first commit).
 */
export async function waitForRelease(port: OperatorTenantPort, planId: string, projectId: string, deadline: number, pollMs = 200): Promise<MigrationStatus | "timeout"> {
  let status = await port.operatorApplyRelease(planId, projectId);
  for (;;) {
    if (status.planId === planId && (status.state === "done" || status.state === "failed")) return status;
    if (Date.now() > deadline) return "timeout";
    await new Promise((resolve) => setTimeout(resolve, pollMs));
    status = await port.operatorMigrationStatus();
    if (status.planId !== planId && status.state !== "running") status = await port.operatorApplyRelease(planId, projectId);
  }
}

/**
 * The text Rust's `/dev/sync` answers for a failed tenant provision
 * (`DatabaseError` Display): K02 prefixes the unit (`migration 0001_x.sql: `)
 * and, in local workerd, says that a restore point could not be scheduled;
 * SQLite failures come in workerd's form. This undoes both and gives SQLite
 * failures sqlx's form, `error returned from database: (code: N) <message>`.
 */
export function rustTenantError(error: string | null): string {
  let text = (error ?? "tenant migration failed").replace(/^(?:prelude [^:]+|migration [^:]+|change-feed install): /, "");
  text = text.replace(/; the restore point could not be scheduled \([\s\S]*\), so the chunks already committed remain$/, "");
  const failure = sqliteFailure(new Error(text));
  return failure.code !== undefined ? failure.message : text;
}
