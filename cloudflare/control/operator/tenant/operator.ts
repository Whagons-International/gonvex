/**
 * The tenant-object half of the operator surface: the data browser of
 * rust/crates/runtime/src/operator_data.rs, the membership reads and writes of
 * operations.rs, and the rollout step of `/dev/sync`, on one tenant object's
 * storage.
 *
 * Reads use the object's SQLite directly (Rust uses a read-only pool). Writes
 * are tenant transactions, as Rust's `TenantTransaction`: one K01 invocation
 * (deferred-FK check before commit, 0016 G0-5) with K03 capture finalizing one
 * revision, so the change feed and replicas see operator edits like any
 * Reducer write. Writes wait while a migration holds the schema gate
 * (0028 §3.3).
 */
import { invokeCaptured, type CaptureState, type ChangeWake } from "../../../kernel/capture/index.ts";
import type { MigrationStatus, TenantMigrator } from "../../../kernel/migrate/migrator.ts";
import { sqliteFailure } from "../../../kernel/storage/errors.ts";
import type { StorageSession, TenantStorage } from "../../../kernel/storage/session.ts";
import type { ActiveMemberRow, PortResult, ReplaceResult, RowsPage, RowsQuery, TableSummary } from "./port.ts";
import {
  appendWhere,
  internalTable,
  jsonInputExpr,
  jsonObjectExpr,
  kindOf,
  parseFilters,
  quoteIdent,
  replaceJsonReferences,
  tableColumns,
  validIdent,
  type ColumnInfo,
  type JsonKind,
} from "./rows.ts";

export interface TenantOperatorOptions {
  storage: DurableObjectStorage;
  /** K01: one per tenant object. */
  tenantStorage: TenantStorage;
  /** K03 per-object capture state and wake-up. */
  capture: CaptureState;
  wake: ChangeWake;
  /** K02: the per-object migrator. */
  migrator: TenantMigrator;
}

const ok = <T>(value: T): PortResult<T> => ({ ok: true, value });
const fail = (status: number, error: string): PortResult<never> => ({ ok: false, status, error });
const message = (error: unknown): string => {
  const failure = sqliteFailure(error);
  return failure.code !== undefined ? failure.message : error instanceof Error ? error.message : String(error);
};

/** Tables the platform or the local runtime keeps in every object; never application data. */
const platformTable = (name: string) => name.startsWith("_cf_") || name === "__miniflare_do_name";

export class TenantOperator {
  constructor(private readonly options: TenantOperatorOptions) {}

  private get sql(): SqlStorage {
    return this.options.storage.sql;
  }

  // ------------------------------------------------------------ rollout

  applyRelease(planId: string): Promise<MigrationStatus> {
    return this.options.migrator.applyRelease(planId);
  }

  // ------------------------------------------------------------ reads

  /** `list_tables`. */
  listTables(): PortResult<TableSummary[]> {
    let rows: Array<{ name: string; columns: string }>;
    try {
      rows = this.sql
        .exec<{ name: string; columns: string }>(
          `SELECT m.name AS name,
                  (SELECT json_group_array(c.name) FROM
                     (SELECT name FROM pragma_table_info(m.name) ORDER BY cid) c) AS columns
           FROM sqlite_schema m
           WHERE m.type='table' AND m.name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
             AND m.name NOT LIKE '\\_cf\\_%' ESCAPE '\\'
           ORDER BY m.name`,
        )
        .toArray();
    } catch (error) {
      return fail(503, message(error));
    }
    const hasStats = this.sql.exec<{ found: number }>("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='sqlite_stat1') AS found").one().found === 1;
    const estimates = new Map<string, number>();
    if (hasStats) {
      for (const row of this.sql.exec<{ tbl: string; stat: string | null }>("SELECT tbl, stat FROM sqlite_stat1 WHERE stat IS NOT NULL")) {
        const count = Number.parseInt(String(row.stat).trim().split(/\s+/)[0] ?? "", 10);
        if (Number.isSafeInteger(count)) estimates.set(row.tbl, count);
      }
    }
    const tables: TableSummary[] = [];
    for (const row of rows) {
      if (internalTable(row.name) || platformTable(row.name)) continue;
      let columns: string[] = [];
      try {
        columns = JSON.parse(row.columns) as string[];
      } catch {
        columns = [];
      }
      let rowCount = 0;
      let rowCountEstimated = true;
      const estimate = estimates.get(row.name);
      if (estimate !== undefined) rowCount = estimate;
      else if (validIdent(row.name)) {
        try {
          rowCount = Number(this.sql.exec<{ n: number }>(`SELECT count(*) AS n FROM ${quoteIdent(row.name)}`).one().n);
        } catch {
          rowCount = 0;
        }
        rowCountEstimated = false;
      }
      tables.push({ name: row.name, columns, rowCount, rowCountEstimated });
    }
    return ok(tables);
  }

  /** `read_rows` after authorization and tenant resolution. */
  readRows(table: string, query: RowsQuery): PortResult<RowsPage> {
    if (internalTable(table) || platformTable(table)) return fail(404, "table not found");
    if (!validIdent(table)) return fail(400, "invalid table name");
    let infos: ColumnInfo[];
    try {
      infos = tableColumns(this.sql, table);
    } catch (error) {
      return fail(400, message(error));
    }
    if (infos.length === 0) return fail(400, "table does not exist");
    const all = infos.map((column) => column.name);
    const selected = requestedColumns(query.columns, all);
    if (typeof selected === "string") return fail(400, selected);
    let filters;
    try {
      filters = parseFilters(query.filters);
    } catch (error) {
      return fail(400, (error as Error).message);
    }
    const kinds = new Map<string, JsonKind>(infos.map((column) => [column.name, kindOf(column.declared)]));
    const quotedTable = quoteIdent(table);
    const exactTotal = query.count !== "false" && query.count !== "estimate";
    let total = 0;
    try {
      if (exactTotal) {
        const where = appendWhere(kinds, query.search, filters, 1);
        total = Number(this.sql.exec<{ n: number }>(`SELECT count(*) AS n FROM ${quotedTable}${where.sql}`, ...(where.binds as SqlStorageValue[])).one().n);
      }
    } catch (error) {
      return fail(400, error instanceof Error && !/SQLITE_/.test(error.message) ? error.message : message(error));
    }
    const limit = query.limit <= 0 ? 100 : Math.min(query.limit, 1000);
    const offset = Math.max(query.offset, 0);
    const allSet = new Set(all);
    const sort = query.sort.trim() === "" ? (allSet.has("created_at") ? "created_at" : allSet.has("id") ? "id" : all[0]!) : query.sort.trim();
    if (!allSet.has(sort) || !validIdent(sort)) return fail(400, "invalid sort column");
    const direction = query.direction.toLowerCase() === "desc" || (query.sort === "" && sort === "created_at") ? " DESC" : " ASC";
    let rows: unknown[];
    try {
      const where = appendWhere(kinds, query.search, filters, 1);
      const columns = selected.map((column) => [column, kinds.get(column)!] as const);
      // LIMIT/OFFSET are validated integers; inlined because the platform binds JS numbers as REAL.
      const statement = `SELECT ${jsonObjectExpr("t", columns)} AS row FROM ${quotedTable} t${where.sql} ORDER BY t.${quoteIdent(sort)}${direction} LIMIT ${limit} OFFSET ${offset}`;
      rows = this.sql
        .exec<{ row: string }>(statement, ...(where.binds as SqlStorageValue[]))
        .toArray()
        .flatMap((row) => {
          try {
            return [JSON.parse(row.row) as unknown];
          } catch {
            return [];
          }
        });
    } catch (error) {
      return fail(400, error instanceof Error && !/SQLITE_/.test(error.message) ? error.message : message(error));
    }
    const inferred = exactTotal ? total : offset + rows.length + (rows.length === limit ? limit : 0);
    return ok({ table, columns: selected, rows, total: inferred, offset, limit });
  }

  /** Active members, as `project_auth_memberships` reads them. */
  activeMembers(): PortResult<ActiveMemberRow[]> {
    try {
      const rows = this.sql
        .exec<{ id: string; account_id: string; display_name: string; role: string; permissions: string }>(
          "SELECT id,account_id,display_name,role,permissions FROM members WHERE status='active' ORDER BY lower(display_name),id",
        )
        .toArray();
      return ok(
        rows.map((row) => ({
          id: row.id,
          accountId: row.account_id,
          displayName: row.display_name,
          role: row.role,
          permissions: parseJson(row.permissions),
        })),
      );
    } catch (error) {
      return fail(503, message(error));
    }
  }

  // ------------------------------------------------------------ writes

  /** A tenant transaction: K01 invocation with K03 capture, default (empty) provenance. */
  private async write<T>(body: (session: StorageSession) => T): Promise<PortResult<T>> {
    if (this.options.migrator.gateClosed()) return fail(503, "tenant schema migration is in progress; retry when it completes");
    const result = await invokeCaptured(this.options.tenantStorage, "reducer", {}, async (session) => body(session), {
      state: this.options.capture,
      wake: this.options.wake,
    });
    if (!result.ok) {
      const error = (result as { error: unknown }).error;
      return fail(400, error instanceof HttpFailure ? error.message : message(error));
    }
    return ok(result.value as T);
  }

  /** `insert_row`. */
  async insertRow(table: string, payload: Record<string, unknown>): Promise<PortResult<unknown>> {
    if (internalTable(table) || platformTable(table)) return fail(404, "table not found");
    if (!validIdent(table)) return fail(400, "invalid table name");
    const values = Object.keys(payload);
    if (values.length === 0) return fail(400, "no values provided");
    return this.write((session) => {
      const all = tableColumns(this.sql, table);
      if (all.length === 0) throw new HttpFailure("table does not exist");
      const kinds = new Map(all.map((column) => [column.name, kindOf(column.declared)]));
      const names = values.filter((name) => kinds.has(name) && validIdent(name)).sort();
      if (names.length !== values.length || names.length === 0) throw new HttpFailure("invalid column name");
      const statement = `INSERT INTO ${quoteIdent(table)} (${names.map(quoteIdent).join(",")}) VALUES (${names
        .map((name) => jsonInputExpr(name, kinds.get(name)!))
        .join(",")}) RETURNING ${jsonObjectExpr("", all.map((c) => [c.name, kindOf(c.declared)] as const))} AS row`;
      const out = session.run(statement, [JSON.stringify(payload)]);
      const row = out.rows[0]?.[0];
      if (row === undefined) throw new HttpFailure("insert did not return a row");
      return parseJson(row);
    });
  }

  /** `update_row`. */
  async updateRow(table: string, rowId: string, payload: Record<string, unknown>): Promise<PortResult<unknown>> {
    if (internalTable(table) || platformTable(table)) return fail(404, "table not found");
    if (!validIdent(table)) return fail(400, "invalid table name");
    const values = Object.keys(payload);
    if (values.length === 0) return fail(400, "at least one value is required");
    if (values.some((column) => ["_id", "id", "tenantId", "tenant_id"].includes(column))) return fail(400, "identity and tenant columns cannot be changed");
    return this.write((session) => {
      const all = tableColumns(this.sql, table);
      if (all.length === 0) throw new HttpFailure("table does not exist");
      const kinds = new Map(all.map((column) => [column.name, kindOf(column.declared)]));
      const id = kinds.has("_id") ? "_id" : kinds.has("id") ? "id" : undefined;
      if (!id) throw new HttpFailure("table has no supported identity column");
      const names = values.filter((name) => kinds.has(name) && validIdent(name)).sort();
      if (names.length !== values.length) throw new HttpFailure("invalid column name");
      const statement = `UPDATE ${quoteIdent(table)} SET ${names
        .map((name) => `${quoteIdent(name)}=${jsonInputExpr(name, kinds.get(name)!)}`)
        .join(",")} WHERE CAST(${quoteIdent(id)} AS TEXT)=?2 RETURNING ${jsonObjectExpr("", all.map((c) => [c.name, kindOf(c.declared)] as const))} AS row`;
      const row = session.run(statement, [JSON.stringify(payload), rowId]).rows[0]?.[0];
      if (row === undefined) throw new HttpFailure("update did not find a row");
      return parseJson(row);
    });
  }

  /** `delete_row`. */
  async deleteRow(table: string, rowId: string): Promise<PortResult<unknown>> {
    if (internalTable(table) || platformTable(table)) return fail(404, "table not found");
    if (!validIdent(table)) return fail(400, "invalid table name");
    return this.write((session) => {
      const all = tableColumns(this.sql, table);
      const id = all.some((c) => c.name === "_id") ? "_id" : all.some((c) => c.name === "id") ? "id" : undefined;
      if (!id) throw new HttpFailure("table has no supported identity column");
      const statement = `DELETE FROM ${quoteIdent(table)} WHERE CAST(${quoteIdent(id)} AS TEXT)=?2 RETURNING ${jsonObjectExpr("", all.map((c) => [c.name, kindOf(c.declared)] as const))} AS row`;
      const row = session.run(statement, [null, rowId]).rows[0]?.[0];
      if (row === undefined) throw new HttpFailure("delete did not find a row");
      return parseJson(row);
    });
  }

  /** `delete_project_auth_membership` with a member id. */
  async revokeMember(memberId: string): Promise<PortResult<boolean>> {
    return this.write((session) => {
      const out = session.run(
        "UPDATE members SET status='revoked',membership_revision=membership_revision+1,updated_at=CAST(unixepoch('subsec')*1000 AS INTEGER) WHERE id=?1 RETURNING 1",
        [memberId],
      );
      return out.rows.length > 0;
    });
  }

  /**
   * `replace_references`. Rust stages the map in a TEMP table, which Durable
   * Object SQLite refuses (A01 T4); an ordinary table created and dropped in
   * the same transaction holds it instead.
   */
  async replaceReferences(replacements: Record<string, string>, dryRun: boolean): Promise<PortResult<ReplaceResult>> {
    const map = new Map(Object.entries(replacements).map(([from, to]) => [from.trim(), to.trim()]));
    const TABLE = '"_gonvex_st_reference_replacements"';
    return this.write((session) => {
      session.run(`DROP TABLE IF EXISTS ${TABLE}`);
      session.run(`CREATE TABLE ${TABLE} (source_id TEXT PRIMARY KEY, replacement_id TEXT NOT NULL)`);
      for (const [source, replacement] of map) session.run(`INSERT INTO ${TABLE}(source_id,replacement_id) VALUES(?1,?2)`, [source, replacement]);
      const columns = this.sql
        .exec<{ table_name: string; column_name: string; data_type: string }>(
          `SELECT m.name AS table_name, c.name AS column_name, lower(c.type) AS data_type
           FROM sqlite_schema m JOIN pragma_table_xinfo(m.name) c
           WHERE m.type='table' AND m.name<>'members'
             AND m.name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
             AND m.name NOT LIKE '\\_gonvex\\_%' ESCAPE '\\'
             AND m.name NOT LIKE 'gonvex\\_%' ESCAPE '\\'
             AND m.name NOT LIKE '\\_cf\\_%' ESCAPE '\\'
             AND c.hidden=0
           ORDER BY m.name, c.cid`,
        )
        .toArray();
      let textRows = 0;
      let jsonRows = 0;
      const changed: unknown[] = [];
      for (const { table_name: table, column_name: column, data_type: dataType } of columns) {
        const kind = kindOf(dataType);
        const isJson = kind === "json";
        const isText = !isJson && (dataType.includes("text") || dataType.includes("char") || dataType.includes("clob"));
        if (!(isJson || isText) || ["_id", "id", "tenantId", "tenant_id"].includes(column)) continue;
        if (!validIdent(table) || !validIdent(column)) continue;
        const t = quoteIdent(table);
        const c = quoteIdent(column);
        let affected: number;
        if (isJson) {
          const candidates = session.run(
            `SELECT rowid AS gonvex_rowid, ${c} AS value FROM ${t} WHERE ${c} IS NOT NULL AND EXISTS (SELECT 1 FROM ${TABLE} replacements WHERE instr(CAST(${c} AS TEXT), json_quote(replacements.source_id))>0)`,
          ).rows;
          if (!dryRun) {
            for (const [rowid, raw] of candidates) {
              let value: unknown;
              try {
                value = JSON.parse(String(raw));
              } catch {
                continue;
              }
              session.run(`UPDATE ${t} SET ${c}=?1 WHERE rowid=?2`, [JSON.stringify(replaceJsonReferences(value, map)), rowid]);
            }
          }
          affected = candidates.length;
          jsonRows += affected;
        } else if (dryRun) {
          affected = Number(session.run(`SELECT count(*) FROM ${t} target JOIN ${TABLE} replacements ON target.${c}=replacements.source_id`).rows[0]?.[0] ?? 0);
          textRows += affected;
        } else {
          affected = session.run(`UPDATE ${t} SET ${c}=replacements.replacement_id FROM ${TABLE} replacements WHERE ${t}.${c}=replacements.source_id RETURNING 1`).rows.length;
          textRows += affected;
        }
        if (affected > 0) changed.push({ table, column, dataType, rows: affected });
      }
      session.run(`DROP TABLE ${TABLE}`);
      return { textRows, jsonRows, columns: changed, dryRun };
    });
  }
}

/** A refusal raised inside a write: its text is the response, status 400. */
class HttpFailure extends Error {}

function parseJson(value: unknown): unknown {
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

/** operator_data.rs `requested_columns`: the selection, or an error text. */
export function requestedColumns(raw: string, all: readonly string[]): string[] | string {
  if (raw.trim() === "") return [...all];
  const allowed = new Set(all);
  const seen = new Set<string>();
  const selected: string[] = [];
  for (const column of raw.split(",").map((value) => value.trim()).filter((value) => value !== "")) {
    if (!allowed.has(column) || !validIdent(column)) return "invalid selected column";
    if (!seen.has(column)) {
      seen.add(column);
      selected.push(column);
    }
  }
  return selected.length === 0 ? [...all] : selected;
}
