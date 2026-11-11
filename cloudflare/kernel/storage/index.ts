// Kernel storage layer (package K01): the tenant object's SQLite, with the
// Rust runtime's module SQL semantics on Durable Object SQLite. See README.md.

export { TenantStorage, StorageSession, chunkParameters, CLOSED } from "./session";
export type { StorageHandle, WriteTracking, InvocationKind, InvocationResult, InvocationStats, RowCounters, RawResult, TenantStorageOptions } from "./session";
export { StorageError, ABORTED, FOREIGN_KEY_FAILED, READ_ONLY_WRITE, databaseMessage, sqliteFailure } from "./errors";
export type { StorageErrorKind } from "./errors";
export { admitStatement, requireReadHead, writeTarget, AdmissionError } from "./admission";
export { MAX_BOUND_PARAMETERS } from "./parameters";
export { Catalog, DEFAULT_KEY, quoteIdentifier } from "./catalog";
export type { TableCatalog, TableKey } from "./catalog";
export { kindFromDeclared, decodeCell, decodeExpressionCell, scalarParameter, columnParameter, blobHex } from "./values";
export type { JsonKind, JsonValue, JsonObject, Parameter } from "./values";
export { regexpPredicate, RegexpUnsupportedError } from "./regexp";
export { projectionKinds } from "./projection";
