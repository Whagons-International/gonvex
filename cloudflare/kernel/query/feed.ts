/**
 * The change-feed input of Live Queries: Rust's `change_feed::FeedEvent`.
 * Delivery (K10) owns the per-socket cursors and reads committed revisions
 * after a K03 wake; `readTransaction` is the read of one revision in the
 * shape the engine consumes (used by the tests, and by delivery until it has
 * its own reader).
 */
import type { LogChange } from "./visibility";

export type FeedEvent =
  | { readonly kind: "transaction"; readonly revision: number; readonly epoch: string; readonly changes: readonly LogChange[] }
  /** The feed restarted (`FeedEvent::Reset`): every live query reruns and answers with reason `reconnect`. */
  | { readonly kind: "reset" };

const parse = (value: SqlStorageValue): Record<string, unknown> | null =>
  value === null || value === undefined ? null : (JSON.parse(String(value)) as Record<string, unknown> | null);

/** The changes of one committed revision, in ordinal order. */
export function readTransaction(sql: SqlStorage, revision: number): FeedEvent {
  const epoch = String(sql.exec("SELECT epoch FROM _gonvex_sync_clock WHERE singleton = 1").one().epoch);
  const changes = [...sql.exec(
    "SELECT table_name, row_id, operation, changed_columns, old_value, new_value FROM _gonvex_sync_changes WHERE revision = ? ORDER BY ordinal",
    revision,
  ).raw()].map(([table, rowId, operation, changed, oldValue, newValue]): LogChange => ({
    table: String(table),
    rowId: String(rowId),
    operation: String(operation),
    changedColumns: changed === null ? [] : (JSON.parse(String(changed)) as string[]),
    oldValue: parse(oldValue ?? null),
    newValue: parse(newValue ?? null),
  }));
  return { kind: "transaction", revision, epoch, changes };
}
