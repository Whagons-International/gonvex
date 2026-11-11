// K07: declared reference integrity (rust/crates/store/src/reference_integrity.rs).
//
// Every entry point takes K03's synchronous `CaptureSql`, so K01's
// `StorageSession` (inside an invocation) and `directSql(ctx.storage.sql)`
// (migrations, installation) both drive it. `declareReferences` and
// `afterMigration` take `atomic: fn => ctx.storage.transactionSync(fn)` for
// Rust's savepoint around one declaration.

import { splitStatements } from "../../schema-transform/index.ts";
import { REFERENCE_SCHEMA_SQL } from "../../migrate/runtime-sql.ts";
import type { CaptureSql } from "../../capture/sql.ts";

export {
  afterMigration,
  bindingId,
  checkCoverage,
  configuration,
  declareReferences,
  decode,
  flushReferences,
  hasStagedReferences,
  ReferenceIntegrityError,
  type Atomic,
  type DeclareOptions,
  type FlushStats,
} from "./integrity.ts";
export { JsonPath, PathError, type Vars } from "./jsonpath.ts";
export { equal, formatFloat, fromJs, JsonError, parseJson, stringify, textOf, toJs, type Json, type JsonObject } from "./json.ts";
export { stagingTriggerNames, stagingTriggers, type ColumnInfo } from "./schema.ts";

/**
 * reference_integrity.rs `install`: the ledger tables (idempotent). K02's
 * runtime prelude already runs the same SQL (`REFERENCE_SCHEMA_SQL`, generated
 * from reference_integrity/schema.sql); this is for callers without it.
 */
export function installReferences(sql: CaptureSql): void {
  for (const statement of splitStatements(REFERENCE_SCHEMA_SQL)) sql.run(statement);
}
