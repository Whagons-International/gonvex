// One revision per commit (feed.rs `finalize_known`, docs/sqlite-storage.md
// "Change feed" step 2), run inside the committing storage transaction.
//
//  1. record flush: logical record images staged in `_gonvex_record_pending`
//     become change rows (A01 `recordFlushSql`, one statement per record name);
//  2. nothing staged: no revision;
//  3. delete no-op updates;
//  4. `_gonvex_sync_clock.revision + 1`;
//  5. stamp revision, transaction_id, ordinal (event order) and command_id, and
//     split `[old, new, changed]` (images.ts) in the same UPDATE;
//  6. the revision's tables;
//  7. the `_gonvex_sync_transactions` provenance row.
//
// The statements and their order are Rust's, with two differences that leave
// the same rows: the record flush is one INSERT per record name instead of one
// call of a custom function, and the split is folded into the stamping UPDATE
// (Rust runs it as its own UPDATE first), which saves one row write per change.

import { RECORD_PENDING_CLEANUP_SQL, recordFlushSql, recordView } from "../schema-transform/index.ts";
import { DELETE_NO_OP_SQL, ROW_IMAGE_SPLIT_ASSIGNMENTS } from "./images.ts";
import { changeCommandId, transactionRow, type Provenance } from "./provenance.ts";
import { counting, counters, integerLiteral, objects, quoteIdentifier, scalar, type CaptureSql, type RowCounters, type RowReporter } from "./sql.ts";

/** A committed revision, published after the transaction commits (feed.rs `CommitNotice`). */
export interface CommitNotice {
  readonly epoch: string;
  readonly revision: number;
  /** Tables (and logical records) changed, in BINARY order. */
  readonly tables: readonly string[];
  /** Change rows in the revision. */
  readonly changes: number;
}

/** What one finalize cost, for budgets. */
export interface FinalizeStats extends RowCounters {}

/** Per-object memory of what never goes away once created (feed.rs `feed_known`). */
export class CaptureState {
  feedKnown = false;
  pendingKnown = false;
  /** Entity table -> key column and whether it has a rowid, for row reports. */
  readonly keys = new Map<string, { key: string; rowid: boolean } | null>();

  /** Call after a schema change (K02 migrations, module DDL). */
  invalidate(): void {
    this.keys.clear();
  }
}

export interface FinalizeOptions {
  /** Per-object state; a fresh one re-probes the schema each time. */
  readonly state?: CaptureState;
  /** Receives the rows the revision changed in captured entity tables (K01 `noteRows`). */
  readonly reporter?: RowReporter;
  /** Adds the finalize statements' row counts here. */
  readonly stats?: FinalizeStats;
}

const exists = (sql: CaptureSql, query: string) => Number(scalar(sql.run(query)) ?? 0) === 1;

/**
 * Assigns one revision to the transaction's staged changes. Returns
 * `undefined` when the transaction changed no captured rows, or when the
 * database has no change feed.
 */
export function finalize(inner: CaptureSql, provenance: Provenance, options: FinalizeOptions = {}): CommitNotice | undefined {
  const state = options.state ?? new CaptureState();
  const sql = counting(inner, options.stats ?? counters());
  if (!state.feedKnown) {
    if (!exists(sql, "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_gonvex_sync_changes')")) return undefined;
    state.feedKnown = true;
  }
  flushRecords(sql, state);
  if (!exists(sql, "SELECT EXISTS (SELECT 1 FROM _gonvex_sync_changes WHERE revision IS NULL)")) return undefined;
  sql.run(DELETE_NO_OP_SQL);
  if (!exists(sql, "SELECT EXISTS (SELECT 1 FROM _gonvex_sync_changes WHERE revision IS NULL)")) return undefined;
  const clock = sql.run("UPDATE _gonvex_sync_clock SET revision = revision + 1 WHERE singleton = 1 RETURNING revision, epoch");
  const row = clock.rows[0];
  if (!row) throw new Error("_gonvex_sync_clock has no row");
  const revision = Number(row[0]);
  const epoch = String(row[1]);
  const r = integerLiteral(revision);
  sql.run(
    `UPDATE _gonvex_sync_changes
           SET revision = ${r}, transaction_id = ${r}, ordinal = ranked.row_ordinal, command_id = ?1,
               ${ROW_IMAGE_SPLIT_ASSIGNMENTS}
           FROM (SELECT event_id, row_number() OVER (ORDER BY event_id) AS row_ordinal
                 FROM _gonvex_sync_changes WHERE revision IS NULL) AS ranked
           WHERE _gonvex_sync_changes.event_id = ranked.event_id`,
    [changeCommandId(provenance)],
  );
  const tables = sql
    .run(`SELECT DISTINCT table_name FROM _gonvex_sync_changes INDEXED BY gonvex_sync_changes_revision WHERE revision = ${r} ORDER BY table_name`)
    .rows.map((t) => String(t[0]));
  const p = transactionRow(provenance);
  sql.run(
    `INSERT INTO _gonvex_sync_transactions
           (revision, transaction_id, root_command_id, origin_command_id, root_invocation_channel,
            invocation_channel, actor_account_id, actor_member_id, on_behalf_of_member_id, agent_execution_id)
           VALUES (${r}, ${r}, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
           ON CONFLICT (revision) DO NOTHING`,
    [p.root_command_id, p.origin_command_id, p.root_invocation_channel, p.invocation_channel, p.actor_account_id, p.actor_member_id, p.on_behalf_of_member_id, p.agent_execution_id],
  );
  // Rust takes the stamp UPDATE's rows_affected. rowsWritten also counts index rows, so count the revision.
  const changes = Number(scalar(sql.run(`SELECT count(*) FROM _gonvex_sync_changes INDEXED BY gonvex_sync_changes_revision WHERE revision = ${r}`)) ?? 0);
  if (options.reporter) reportRows(sql, state, options.reporter, revision, tables);
  return { epoch, revision, tables, changes };
}

/** record_views.rs `flush`: pending logical-record images become change rows. */
function flushRecords(sql: CaptureSql, state: CaptureState): void {
  if (!state.pendingKnown) {
    if (!exists(sql, "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_gonvex_record_pending' AND type='table')")) return;
    state.pendingKnown = true;
  }
  const views = objects(
    sql.run(
      "SELECT v.name, v.storage_table, v.key_column, v.schema, p.table_name AS pending FROM (SELECT DISTINCT table_name FROM _gonvex_record_pending) p LEFT JOIN _gonvex_record_views v ON v.name = p.table_name",
    ),
  );
  if (views.length === 0) return;
  // Rust's single statement emits rows in (table_name, row_id) key order: names in BINARY order.
  views.sort((a, b) => (String(a.pending) < String(b.pending) ? -1 : String(a.pending) > String(b.pending) ? 1 : 0));
  for (const view of views) {
    if (view.name === null) throw new Error("logical record view is not registered on this connection");
    sql.run(recordFlushSql(recordView(view as { name: string; storage_table: string; key_column: string; schema: string })));
  }
  sql.run(RECORD_PENDING_CLEANUP_SQL);
}

/**
 * Reports the rowids the revision inserted or updated in captured entity
 * tables, and deletes as table-level, to K01's row-level deferred foreign key
 * check (0022 §2c). Logical records and tables capture does not see (derived,
 * metadata, runtime tables) are not reported: K01 must still track those by
 * statement (see README "K01 integration").
 */
function reportRows(sql: CaptureSql, state: CaptureState, reporter: RowReporter, revision: number, tables: readonly string[]): void {
  const r = integerLiteral(revision);
  for (const table of tables) {
    let shape = state.keys.get(table);
    if (shape === undefined) {
      const info = objects(
        sql.run(
          "SELECT (SELECT name FROM pragma_table_info(?1) WHERE pk > 0 ORDER BY cid LIMIT 1) AS key, (SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1) AS ddl, EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'trigger' AND name = ?2 AND tbl_name = ?1 AND sql LIKE '%AFTER INSERT%') AS entity",
          [table, `_gonvex_sync_${table}_insert`],
        ),
      )[0];
      shape = info && info.key !== null && Number(info.entity) === 1 ? { key: String(info.key), rowid: !/\bWITHOUT\s+ROWID\b/i.test(String(info.ddl ?? "")) } : null;
      state.keys.set(table, shape);
    }
    if (!shape) continue;
    const ops = sql.run(`SELECT DISTINCT operation FROM _gonvex_sync_changes INDEXED BY gonvex_sync_changes_table WHERE table_name = ?1 AND revision = ${r}`, [table]).rows.map((o) => String(o[0]));
    if (ops.includes("delete")) reporter.noteRows(table, "delete", []);
    if (!shape.rowid) continue;
    const written = sql.run(
      `SELECT c.operation, t.rowid FROM _gonvex_sync_changes c INDEXED BY gonvex_sync_changes_table JOIN ${quoteIdentifier(table)} t ON t.${quoteIdentifier(shape.key)} = c.row_id WHERE c.table_name = ?1 AND c.revision = ${r} AND c.operation <> 'delete'`,
      [table],
    );
    const byOp = new Map<"insert" | "update", number[]>();
    for (const [operation, rowid] of written.rows) {
      const op = operation === "insert" ? "insert" : "update";
      byOp.set(op, [...(byOp.get(op) ?? []), Number(rowid)]);
    }
    for (const [op, rowids] of byOp) reporter.noteRows(table, op, rowids);
  }
}
