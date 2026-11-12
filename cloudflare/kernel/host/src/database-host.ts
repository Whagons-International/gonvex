// Database host calls for one Query or Reducer invocation in the tenant
// object (RT/host_calls.rs DatabaseHostCalls), on the K01 storage layer.
//
// Ruling 0022 §1: K01 owns every SQL semantic: admission, `$N`, read-only
// Queries, typing and decoding, catalogs and their cache, the aborted rule,
// savepoints, the deferred-FK check. This file owns the host-call surface:
// which op does what, the statements `db.insert/update/delete/deleteMany`,
// the outbox and the scheduler build (as Rust builds them), deterministic
// deferred ids, `permissionsHas`, `storage.getMetadata`, and Rust's error
// wrapping ("<capability> host call failed [host_call_failed]: <message>").
import {
  ABORTED,
  StorageError,
  columnParameter,
  decodeCell,
  kindFromDeclared,
  quoteIdentifier,
  scalarParameter,
  type Parameter,
  type RawResult,
  type StorageSession,
  type TableCatalog,
} from "../../storage/index";
import type { HostPayload, Identity, InvocationInfo } from "./context.js";
import { HostFailure, type ModuleHost } from "./engine.js";
import { classifySqliteCode, hostErrors } from "./errors.js";

export { ABORTED };
export const SCHEDULE_OUTBOX_PATH = "_gonvex.scheduler.enqueue";

/** In-memory permission bits of the tenant (adapt: B.2a `permissionsHas`; K09 builds them). */
export interface PermissionIndex {
  /** `true`/`false`, or `"rebuilding"` while the bits do not yet cover the transaction's revision. */
  has(memberId: string, permission: string, scope: string): boolean | "rebuilding";
}

/** `storage.getMetadata` inside a Reducer (K13: `_gonvex_files`, R2 head, IB1 owner check). */
export interface FileMetadataSource {
  /** Returns the metadata, or throws an Error with Rust's text ("file not found"). */
  getMetadata(fileId: string, session: StorageSession, identity: Identity): unknown;
}

export interface DatabaseHostOptions {
  mode: "query" | "reducer";
  session: StorageSession;
  identity: Identity;
  invocation: InvocationInfo;
  /** Registered Reducer or Action of the active generation (scheduler targets). */
  schedulable: (path: string) => boolean;
  permissions?: PermissionIndex | undefined;
  files?: FileMetadataSource | undefined;
  /** Primary key the module schema declares for a table (host_calls.rs declared_table_key). */
  declaredKey?: ((table: string) => string | undefined) | undefined;
  /** Server clock for `runAfter` (B.4: not `ctx.now`). */
  wallClock: () => number;
  randomUUID: () => string;
  subtle: SubtleCrypto;
}

export interface DatabaseHostStats {
  /** Host calls, in total and by op kind. */
  calls: number;
  byKind: Record<string, number>;
  /** A SQLite failure with a transient code happened (RT/error_class.rs TransientFault). */
  transientFault: boolean;
}

const hex = (bytes: Uint8Array) => Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");

/** Same seed and UUID layout as module-sdk reducerRowId / host_calls.rs intent_deferred_id. */
export async function intentDeferredId(subtle: SubtleCrypto, tenant: string, account: string, command: string, kind: string, ordinal: number): Promise<string> {
  const seed = JSON.stringify(["gonvex.reducer.ids.v1", tenant, account, command, `deferred:${kind}`, ordinal]);
  const bytes = new Uint8Array(await subtle.digest("SHA-256", new TextEncoder().encode(seed))).slice(0, 16);
  bytes[6] = ((bytes[6] as number) & 15) | 128;
  bytes[8] = ((bytes[8] as number) & 63) | 128;
  const h = hex(bytes);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

/** BTreeMap order, as Rust builds column lists. */
const byName = ([a]: [string, unknown], [b]: [string, unknown]) => (a < b ? -1 : a > b ? 1 : 0);

export class DatabaseHost implements ModuleHost {
  readonly stats: DatabaseHostStats = { calls: 0, byKind: {}, transientFault: false };
  private readonly ordinals = new Map<string, number>();

  constructor(private readonly options: DatabaseHostOptions) {}

  private get session() { return this.options.session; }
  private get reducer() { return this.options.mode === "reducer"; }

  /** Host-call entry point: Rust's failure text is "<capability> host call failed [host_call_failed]: <message>". */
  async call(payload: HostPayload, capability: string): Promise<unknown> {
    this.stats.calls++;
    this.stats.byKind[payload.kind] = (this.stats.byKind[payload.kind] ?? 0) + 1;
    try {
      return await this.dispatch(payload);
    } catch (error) {
      if (error instanceof StorageError && error.code !== undefined && classifySqliteCode(error.code) === "transient") this.stats.transientFault = true;
      throw new HostFailure(hostErrors.wrapped(capability, String((error as Error)?.message ?? error)));
    }
  }

  private requireOpen() { if (this.session.aborted !== undefined) throw new Error(ABORTED); }
  private requireWrite() { if (!this.reducer) throw new Error("a Query cannot modify application tables"); }

  private async dispatch(payload: HostPayload): Promise<unknown> {
    switch (payload.kind) {
      case "dbQuery": return this.session.query(String(payload["statement"] ?? ""), payload["parameters"]);
      case "dbInsert": this.requireWrite(); return this.insert(String(payload["table"]), payload["row"], payload["generatedId"]);
      case "dbUpdate": this.requireWrite(); return this.update(String(payload["table"]), String(payload["key"] ?? ""), payload["id"], payload["patch"]);
      case "dbDelete": this.requireWrite(); return this.delete(String(payload["table"]), String(payload["key"] ?? ""), payload["id"]);
      case "dbDeleteMany": this.requireWrite(); return this.deleteMany(String(payload["table"]), String(payload["key"] ?? ""), payload["ids"]);
      case "permissionsHas": return this.permissionsHas(String(payload["permission"]), String(payload["scope"] ?? ""));
      case "actionEnqueue": return this.enqueueAction(payload["function"], payload["args"]);
      case "scheduleAfter": {
        this.requireWrite();
        const delay = Number(payload["delayMs"]);
        if (!Number.isSafeInteger(delay) || delay < 0) throw new Error("scheduler delayMs is outside the supported range");
        return this.enqueueScheduled(payload["function"], payload["args"], this.options.wallClock() + delay, false);
      }
      case "scheduleAt": {
        this.requireWrite();
        const at = Number(payload["atUnixMs"]);
        if (!Number.isSafeInteger(at) || at < 0 || at > 8.64e15) throw new Error("scheduler atUnixMs is outside the supported range");
        return this.enqueueScheduled(payload["function"], payload["args"], at, payload["coalesce"] === true);
      }
      case "storage":
        if (payload["operation"] === "getMetadata") return this.getMetadata(payload["payload"]);
        break;
      default: break;
    }
    throw new Error(this.reducer ? "this Reducer host capability is not implemented in the Rust runtime yet" : "a Query may only use its read-only database capability");
  }

  // ---- db.insert / update / delete / deleteMany (host_calls.rs SqlCalls) -------

  private catalog(table: string): TableCatalog { return this.session.catalogOf(table); }

  private parameter(catalog: TableCatalog, column: string, value: unknown): Parameter {
    try { return columnParameter(value, catalog.columns.get(column)); } catch (error) { throw new Error(`column ${JSON.stringify(column)}: ${(error as Error).message}`); }
  }

  private object(value: unknown, name: string): [string, unknown][] {
    if (value === null || value === undefined) return [];
    if (typeof value !== "object" || Array.isArray(value)) throw new Error(`${name} must be an object`);
    return Object.entries(value as Record<string, unknown>);
  }

  private textKey(key: TableCatalog["key"]): boolean {
    return key.dataType !== undefined && key.dataType.trim() !== "" && kindFromDeclared(key.dataType) === "text";
  }

  /** returning_row: the first row decoded by the table's declared kinds (TEXT when undeclared). */
  private returningRow(result: RawResult, catalog: TableCatalog): Record<string, unknown> | null {
    const row = result.rows[0];
    if (!row) return null;
    return Object.fromEntries(result.columnNames.map((name, index) => [name, decodeCell(catalog.columns.get(name) ?? "text", row[index])]));
  }

  private returningRecordRow(table: string, sql: string, binds: readonly Parameter[], catalog: TableCatalog): Record<string, unknown> | null {
    const row = this.returningRow(this.session.runTyped(sql, binds), catalog);
    if (!catalog.recordView || row === null) return row;
    const key = row[catalog.key.column];
    if (key === undefined) throw new Error("record write did not return its key");
    const reread = this.session.runTyped(`SELECT * FROM ${quoteIdentifier(table)} WHERE ${quoteIdentifier(catalog.key.column)}=?1`, [scalarParameter(key)]);
    return this.returningRow(reread, catalog);
  }

  private insert(table: string, row: unknown, generatedId: unknown): unknown {
    this.requireOpen();
    const values = new Map(this.object(row, "row").sort(byName));
    const catalog = this.catalog(table);
    const key = catalog.key;
    if (!values.has(key.column) && typeof generatedId === "string" && this.textKey(key)) values.set(key.column, generatedId);
    if (!values.has(key.column) && !key.databaseGenerated && key.dataType !== undefined) {
      if (this.textKey(key)) values.set(key.column, this.options.randomUUID());
      else throw new Error(`insert into ${JSON.stringify(table)} requires primary key ${JSON.stringify(key.column)}; Gonvex can only allocate omitted text keys, but this column is ${key.dataType}`);
    }
    const ordered = [...values].sort(byName);
    const quotedTable = quoteIdentifier(table);
    const columns = ordered.map(([column]) => quoteIdentifier(column));
    const placeholders = ordered.map((_, index) => `?${index + 1}`);
    if (catalog.recordView) for (const [column, expression] of catalog.defaults) if (!values.has(column)) { columns.push(quoteIdentifier(column)); placeholders.push(expression); }
    const binds = ordered.map(([column, value]) => this.parameter(catalog, column, value));
    return this.returningRecordRow(table, `INSERT INTO ${quotedTable} (${columns.join(", ")}) VALUES (${placeholders.join(", ")}) RETURNING *`, binds, catalog);
  }

  /** host_calls.rs resolve_table_key: requested, else declared, else catalog. */
  private tableKey(table: string, requested: string): string {
    const wanted = requested.trim();
    const declared = this.options.declaredKey?.(table);
    if (wanted) {
      if (declared !== undefined && declared !== wanted) throw new Error(`table ${JSON.stringify(table)} declares primary key ${JSON.stringify(declared)}, not ${JSON.stringify(wanted)}`);
      return quoteIdentifier(wanted);
    }
    if (declared !== undefined) return quoteIdentifier(declared);
    return quoteIdentifier(this.catalog(table).key.column);
  }

  private requireRowId(id: unknown): void {
    if (typeof id !== "string" && typeof id !== "number") throw new Error("a row id must be a string or number");
  }

  private update(table: string, requestedKey: string, id: unknown, patch: unknown): unknown {
    this.requireOpen();
    const key = this.tableKey(table, requestedKey);
    this.requireRowId(id);
    const values = this.object(patch, "patch").sort(byName);
    if (!values.length) throw new Error("an update requires at least one column");
    const catalog = this.catalog(table);
    const quotedTable = quoteIdentifier(table);
    const assignments = values.map(([column], index) => `${quoteIdentifier(column)} = ?${index + 1}`);
    const binds = values.map(([column, value]) => this.parameter(catalog, column, value));
    binds.push(scalarParameter(id));
    return this.returningRecordRow(table, `UPDATE ${quotedTable} SET ${assignments.join(", ")} WHERE ${key} = ?${values.length + 1} RETURNING *`, binds, catalog);
  }

  private delete(table: string, requestedKey: string, id: unknown): unknown {
    this.requireOpen();
    const key = this.tableKey(table, requestedKey);
    const quotedTable = quoteIdentifier(table);
    this.requireRowId(id);
    const result = this.session.runTyped(`DELETE FROM ${quotedTable} WHERE ${key} = ?1 RETURNING 1`, [scalarParameter(id)]);
    return { deleted: result.rows.length };
  }

  private deleteMany(table: string, requestedKey: string, ids: unknown): unknown {
    this.requireOpen();
    if (!Array.isArray(ids)) throw new Error("deleteMany ids must be an array");
    if (ids.length > 10_000) throw new Error("deleteMany cannot delete more than 10000 rows per call");
    if (!ids.length) return { deleted: 0 };
    for (const id of ids) this.requireRowId(id);
    const key = this.tableKey(table, requestedKey);
    const quotedTable = quoteIdentifier(table);
    // One JSON parameter instead of up to 10,000 placeholders (and DO's 100-parameter cap).
    const result = this.session.runTyped(`DELETE FROM ${quotedTable} WHERE ${key} IN (SELECT value FROM json_each(?1)) RETURNING 1`, [{ kind: "text", value: JSON.stringify(ids) }]);
    return { deleted: result.rows.length };
  }

  // ---- permissions, files ------------------------------------------------------

  private permissionsHas(permission: string, scope: string): boolean {
    const memberId = this.options.identity.member?.id;
    if (!this.reducer || !this.options.permissions || !memberId) throw new Error("ctx.permissions is available to Reducers with a tenant session");
    const held = this.options.permissions.has(memberId, permission, scope);
    if (held === "rebuilding") throw new Error("permission bits are rebuilding; retry");
    return held;
  }

  private getMetadata(payload: unknown): unknown {
    if (!this.options.files) throw new Error("storage metadata is unavailable");
    const fileId = (payload as { fileId?: unknown } | null)?.fileId;
    if (typeof fileId !== "string" || !fileId.trim()) throw new Error("fileId is required");
    return this.options.files.getMetadata(fileId, this.session, this.options.identity);
  }

  // ---- outbox and scheduler -----------------------------------------------------

  private async deferredId(kind: "action" | "schedule"): Promise<string> {
    const commandId = this.options.invocation.commandId;
    if (!commandId) return this.options.randomUUID();
    const ordinal = this.ordinals.get(kind) ?? 0;
    this.ordinals.set(kind, ordinal + 1);
    return intentDeferredId(this.options.subtle, this.options.identity.tenant?.id ?? "", this.options.identity.account?.id ?? "", commandId, kind, ordinal);
  }

  private provenance(): Record<string, unknown> {
    const i = this.options.invocation;
    return {
      commandId: i.commandId ?? null, rootCommandId: i.rootCommandId ?? null, rootChannel: i.rootChannel ?? null, channel: i.channel ?? null,
      actorAccountId: i.actorAccountId ?? null, actorMemberId: i.actorMemberId ?? null, onBehalfOfMemberId: i.onBehalfOfMemberId ?? null, agentExecutionId: null,
      // K17: a delegated session's durable work keeps its delegation (`InvocationProvenance.delegation`, omitted when absent).
      ...(i.delegation !== undefined && i.delegation !== null ? { delegation: i.delegation } : {}),
    };
  }

  private async enqueueAction(path: unknown, args: unknown): Promise<string> {
    this.requireWrite();
    this.requireOpen();
    const name = String(path ?? "").trim();
    if (!name) throw new Error("actions.enqueue requires an Action path");
    if (name === SCHEDULE_OUTBOX_PATH) throw new Error("actions.enqueue cannot invoke a Gonvex system operation");
    const id = await this.deferredId("action");
    this.writeOutbox(id, name, args);
    return id;
  }

  private async enqueueScheduled(path: unknown, args: unknown, atUnixMs: number, coalesce: boolean): Promise<string> {
    this.requireOpen();
    const name = String(path ?? "").trim();
    if (!this.options.schedulable(name)) throw new Error(`scheduled function ${JSON.stringify(name)} is not a registered Reducer or Action`);
    const jobId = `job_${await this.deferredId("schedule")}`;
    // store::enqueue_action: the outbox row id itself is a random UUID.
    this.writeOutbox(this.options.randomUUID(), SCHEDULE_OUTBOX_PATH, { jobId, function: name, args: args ?? null, runAtUnixMs: atUnixMs, coalesce });
    return jobId;
  }

  private writeOutbox(id: string, path: string, args: unknown): void {
    const account = this.options.identity.account;
    this.session.run(
      "INSERT INTO _gonvex_action_outbox (id, action_path, args, actor_user_id, actor_email, provenance) VALUES (?1, ?2, ?3, NULLIF(?4, ''), NULLIF(?5, ''), ?6)",
      [id, path, JSON.stringify(args ?? null), account?.id ?? "", account?.email ?? "", JSON.stringify(this.provenance())],
    );
  }
}
