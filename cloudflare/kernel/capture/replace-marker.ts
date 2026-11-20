// X17p (#555 R1, lead decisions rounds 1-3): REPLACE conflict resolution deletes rows without firing delete
// triggers (`recursive_triggers` is off, as in Rust), so the change log shows only the insert or update that
// replaced them. Modelling where a REPLACE can reach (triggers inheriting the outer conflict policy, foreign key
// actions, declared conflict clauses) proved open-ended, so the rule is conservative and sound by construction:
//
// - schema hazard H (per catalog generation): a table declares `ON CONFLICT REPLACE`, or a trigger body uses
//   REPLACE conflict resolution;
// - statement hazard S: the statement's own conflict resolution is REPLACE (`REPLACE INTO`, `INSERT OR REPLACE`,
//   `UPDATE OR REPLACE`);
// - on S, or on H for any write, storage logs one global marker change row for the transaction: table_name
//   `_gonvex_replaced:*`, operation `delete`, no row, no images. Finalize stamps it like any change.
//
// Every reader that patches state from row images (the principal cache, delivery masks and rows, live queries,
// parent ACL indexes, permission bits in visibility and in the Reducer host) treats the marker as a non-insert
// change to every table and rebuilds. Readers that filter the log by real table names never see it. Apps that
// never use REPLACE (Whagons today) never log it. A leaf module: storage, visibility, delivery, query and reducer
// import it.

export const REPLACED_MARKER = "_gonvex_replaced:*";

/** Whether a change row is the marker. */
export function isReplacedMarker(tableName: string): boolean {
  return tableName === REPLACED_MARKER;
}

/** Logs the marker once per transaction. */
export const REPLACED_MARKER_SQL = `INSERT INTO _gonvex_sync_changes (table_name, row_id, operation)
  SELECT '${REPLACED_MARKER}', '', 'delete' WHERE NOT EXISTS (SELECT 1 FROM _gonvex_sync_changes WHERE revision IS NULL AND table_name = '${REPLACED_MARKER}')`;
