// The row-image step, kept in one place.
//
// Rust's capture triggers stage `[old, new, changed]` from `gonvex_capture`
// (pre-update hook) and pre-images from `gonvex_record_value`. A Durable
// Object has neither, so A01's T5 rewrites those calls into plain-SQL
// `json_object` images (0010, 0016 G0-4). Finalize then splits the staged
// array into old_value / new_value / changed_columns.
//
// R05 (stage 1b) removes row images from the change log. It replaces this
// module and `ROW_IMAGE_SPLIT_SQL`; nothing else in capture builds or reads an
// image.

import { SchemaTransform, type RecordView, type TableShape } from "../schema-transform/index.ts";

export interface ImageCatalog {
  /** `_gonvex_record_views` rows. */
  records: ReadonlyArray<{ name: string; storage_table: string; key_column: string; schema: string } | RecordView>;
  /** Column shapes of tables whose triggers call gonvex_capture. */
  tables: readonly TableShape[];
}

/**
 * A Rust-form capture script as the statements a Durable Object accepts:
 * `gonvex_capture` and `gonvex_record_value` become plain-SQL images. Throws
 * A01's `TransformError` for anything it cannot rewrite or that breaks a limit.
 */
export function plainCaptureStatements(name: string, script: string, catalog: ImageCatalog, transform: SchemaTransform = new SchemaTransform()): string[] {
  return transform.runtime(name, script, { records: catalog.records, tables: catalog.tables }).statements;
}

/**
 * Rust's entity capture passes `NEW.rowid`/`OLD.rowid` to gonvex_capture. On a
 * WITHOUT ROWID table that column does not exist, so in Rust every write that
 * reaches the table fails with "no such column: NEW.rowid" (note 0026). T5
 * drops the argument; this keeps a reference to it so the kernel fails the
 * same writes with the same error. Applied to WITHOUT ROWID entity tables only.
 */
export function keepRowidReference(script: string): string {
  return script.replace(/gonvex_capture\('(?:[^']|'')*', (NEW|OLD)\.rowid, [012]\)/g, (call, row: string) => `nullif(${call}, ${row}.rowid)`);
}

/**
 * Finalize step 2 (feed.rs): rows captured by a trigger hold `[old, new,
 * changed]` in new_value; split them. Updates that changed nothing carry NULL
 * and are deleted first (`DELETE_NO_OP_SQL`).
 */
export const DELETE_NO_OP_SQL = "DELETE FROM _gonvex_sync_changes WHERE revision IS NULL AND operation = 'update' AND new_value IS NULL";

export const ROW_IMAGE_SPLIT_SQL = `UPDATE _gonvex_sync_changes
           SET old_value = nullif(new_value -> '$[0]', 'null'),
               changed_columns = coalesce(new_value -> '$[2]', '[]'),
               new_value = nullif(new_value -> '$[1]', 'null')
           WHERE revision IS NULL AND json_type(new_value) = 'array'`;

/**
 * `ROW_IMAGE_SPLIT_SQL` as SET assignments for finalize's stamping UPDATE, so
 * each change row is rewritten once instead of twice (Rust runs the split and
 * the stamp as two UPDATEs; the rows end the same). SQLite evaluates every SET
 * expression against the row before the UPDATE, as the split needs.
 */
export const ROW_IMAGE_SPLIT_ASSIGNMENTS = `old_value = CASE WHEN json_type(new_value) = 'array' THEN nullif(new_value -> '$[0]', 'null') ELSE old_value END,
               changed_columns = CASE WHEN json_type(new_value) = 'array' THEN coalesce(new_value -> '$[2]', '[]') ELSE changed_columns END,
               new_value = CASE WHEN json_type(new_value) = 'array' THEN nullif(new_value -> '$[1]', 'null') ELSE new_value END`;
