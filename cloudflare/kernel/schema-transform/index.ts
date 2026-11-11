// Public interface of the kernel schema transform (package A01, decision 0014).
// K02 (migrations and provisioning) and K03 (change capture) build on this.

export {
  ADDED_PREFIX,
  objectName,
  SchemaTransform,
  TRANSFORM_VERSION,
  transformMigrations,
  writeTarget,
  type AddedObject,
  type CatalogInput,
  type Rewrite,
  type Rule,
  type SchemaObject,
  type TransformedMigration,
  type TransformOptions,
} from "./transform.ts";
export { TransformError, type TransformFailure } from "./errors.ts";
export { checkStatement, LIMITS, type LimitName, type Limits, type Violation } from "./limits.ts";
export { chunkStatements, CHUNK_DEFAULTS, type ChunkOptions } from "./chunk.ts";
export {
  captureValueSql,
  jsonKind,
  recordFlushSql,
  recordImage,
  recordView,
  RECORD_PENDING_CLEANUP_SQL,
  RECORD_PENDING_NAMES_SQL,
  rowImage,
  type JsonKind,
  type RecordView,
  type TableShape,
} from "./capture.ts";
export { splitStatements } from "./lexer.ts";
