export{sqliteSchemaBootstrap}from'./sqlite-schema-options.js';
// Only for disposable schema authoring/inspection. The authoritative Rust
// installer owns the real request table, including its deferred commit guard.
// Do not use this instead of the installer on a writable tenant database.
export const sqliteReferenceInspectionBootstrap = `CREATE TABLE _gonvex_reference_requests (
  seq INTEGER PRIMARY KEY, declaration JSON NOT NULL CHECK(json_valid(declaration))
)`;
export type LocalColumn = { type: string; nullable: boolean; default?: string };
export type LocalForeignKey = { columns: readonly string[]; targetTable: string; targetColumns: readonly string[]; onDelete: string; onUpdate: string };
export type LocalTableSchema = { key: string; columns: Record<string, LocalColumn>; foreignKeys?: readonly LocalForeignKey[] };
export type LocalSchema = Record<string, LocalTableSchema>;
