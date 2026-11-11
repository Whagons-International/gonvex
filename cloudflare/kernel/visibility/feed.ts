// The tenant's change log as the visibility indexes consume it: the clock and
// committed change rows of `_gonvex_sync_changes` (K03). Rust's indexes are
// fed by an applier task subscribed to the in-process feed; in the tenant
// object every index is brought to the snapshot's revision synchronously,
// either by the caller pushing the transactions it already read (delivery,
// K10) or by reading the log for the index's own tables (`catchUp`).

import { isObject, type JsonValue } from "./json.ts";
import type { VisibilitySql } from "./principal.ts";

/** `change_feed::LogChange`, as visibility reads it. Deleted rows have `newValue` null. */
export interface LogChange {
  readonly table: string;
  readonly rowId: string;
  readonly operation: string;
  readonly changedColumns: readonly string[];
  readonly oldValue: JsonValue;
  readonly newValue: JsonValue;
}

/** One committed transaction of the feed. */
export interface FeedTransaction {
  readonly epoch: string;
  readonly revision: number;
  readonly changes: readonly LogChange[];
}

/** `_gonvex_sync_clock`: epoch, revision, and the revision below which the log was pruned. */
export interface TenantClock {
  readonly epoch: string;
  readonly revision: number;
  readonly retained: number;
}

export function readClock(sql: VisibilitySql): TenantClock {
  const row = sql.run("SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock WHERE singleton = 1").rows[0];
  if (!row) throw new Error("no rows returned by a query that expected to return at least one row");
  return { epoch: String(row[0]), revision: Math.max(0, Number(row[1])), retained: Math.max(0, Number(row[2])) };
}

function parseJson(text: SqlStorageValue | undefined): JsonValue {
  if (text === null || text === undefined) return null;
  if (typeof text !== "string") return text as JsonValue;
  return JSON.parse(text) as JsonValue;
}

/** Rows read per catch-up statement. */
export const CHANGE_PAGE_ROWS = 1000;

/**
 * Committed transactions touching `tables` in `(after, through]`, in revision
 * order, read in pages of `CHANGE_PAGE_ROWS` change rows. Revisions with no
 * row of these tables are absent; callers advance to `through` themselves.
 */
export function readChanges(sql: VisibilitySql, epoch: string, tables: readonly string[], after: number, through: number): FeedTransaction[] {
  if (!tables.length || after >= through) return [];
  const transactions: Array<{ epoch: string; revision: number; changes: LogChange[] }> = [];
  // Keyset pages over (revision, ordinal); the first page starts after `after`.
  let cursor: [number, number] = [after, Number.MAX_SAFE_INTEGER];
  const tableList = tables.map((_, index) => `$${index + 4}`).join(", ");
  for (;;) {
    const page = sql.run(
      `SELECT revision, ordinal, table_name, row_id, operation, changed_columns, old_value, new_value FROM _gonvex_sync_changes
       WHERE revision IS NOT NULL AND table_name IN (${tableList}) AND revision <= $1 AND (revision, ordinal) > ($2, $3)
       ORDER BY revision, ordinal LIMIT ${CHANGE_PAGE_ROWS}`,
      [through, cursor[0], cursor[1], ...tables],
    ).rows;
    for (const row of page) {
      const revision = Number(row[0]);
      let last = transactions[transactions.length - 1];
      if (!last || last.revision !== revision) transactions.push((last = { epoch, revision, changes: [] }));
      const changed = parseJson(row[5]);
      last.changes.push({
        table: String(row[2]),
        rowId: String(row[3]),
        operation: String(row[4]),
        changedColumns: Array.isArray(changed) ? changed.map(String) : [],
        oldValue: parseJson(row[6]),
        newValue: parseJson(row[7]),
      });
      cursor = [revision, Number(row[1])];
    }
    if (page.length < CHANGE_PAGE_ROWS) break;
  }
  return transactions;
}

/** A change's new row image, when it is an object. */
export function newObject(change: LogChange): Record<string, JsonValue> | undefined {
  return isObject(change.newValue) ? change.newValue : undefined;
}
