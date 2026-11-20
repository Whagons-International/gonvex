// X17p (#555 R1, lead decision): REPLACE conflict resolution deletes rows without firing delete triggers
// (`recursive_triggers` is off, as in Rust), so the change log shows only the insert or update that replaced
// them. When storage runs a write that can resolve a conflict with REPLACE on a captured table (`REPLACE`,
// `INSERT OR REPLACE`, `UPDATE OR REPLACE`, a table declared `ON CONFLICT REPLACE`, or a trigger doing one of
// those), it logs one marker change row for that table in the same transaction: table_name
// `_gonvex_replaced:<table>`, operation `delete`, no row, no images. Finalize stamps it with the commit's
// revision like any change. Readers that patch state from row images (the principal cache's roll-forward,
// delivery's live masks, the parent ACL indexes, permission bits) treat a marker as "rows of <table> may have
// gone without a log row" and rebuild; every other reader filters the log by real table names and never sees
// it. A leaf module: storage, capture, visibility and delivery import it.

export const REPLACED_MARKER_PREFIX = "_gonvex_replaced:";

/** The marker's table_name for `table`. */
export function replacedMarker(table: string): string {
  return `${REPLACED_MARKER_PREFIX}${table}`;
}

/** The table a marker names, or undefined for an ordinary change. */
export function markedTable(tableName: string): string | undefined {
  return tableName.startsWith(REPLACED_MARKER_PREFIX) ? tableName.slice(REPLACED_MARKER_PREFIX.length) : undefined;
}

/** Logs the marker for `?1` (a `replacedMarker` name) once per transaction. */
export const REPLACED_MARKER_SQL = `INSERT INTO _gonvex_sync_changes (table_name, row_id, operation)
  SELECT ?1, '', 'delete' WHERE NOT EXISTS (SELECT 1 FROM _gonvex_sync_changes WHERE revision IS NULL AND table_name = ?1)`;
