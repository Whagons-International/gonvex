type SqlLike = { exec(sql: string, ...bindings: SqlStorageValue[]): { toArray(): unknown[] } };
export type SchemaObject = { type: string; name: string; tblName: string; sql: string | null; kind: string };
export function readPlans(sql: SqlLike): Array<{ name: string; withoutRowid: boolean; key: string[]; copyColumns: string[] }>;
export function readSchema(sql: SqlLike): SchemaObject[];
export function schemaPhases(schema: SchemaObject[]): { before: SchemaObject[]; after: SchemaObject[]; fts: string[] };
