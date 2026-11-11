// Declared column kinds and primary keys of tenant tables: a port of
// host_calls.rs `catalog_table` (pragma_table_info plus the logical record
// schemas in `_gonvex_record_views`).
//
// Rust reads a table's catalog once per invocation transaction. Here the
// cache lives on the tenant object across invocations (0016 G0-12): catalog
// lookups were 95-99 % of the rows a Reducer read in S2. Durable Object SQL
// does not expose `schema_version`, so the cache is invalidated explicitly:
// by module DDL that runs through the storage layer, and by whoever changes
// the schema otherwise (migrations, a new bundle generation) calling
// `invalidate()`.

import { kindFromDeclared, type JsonKind } from "./values";

export const DEFAULT_KEY = "id";

export interface TableKey {
  readonly column: string;
  readonly dataType: string | undefined;
  readonly databaseGenerated: boolean;
}

export interface TableCatalog {
  readonly key: TableKey;
  /** Declared kind of each column. */
  readonly columns: ReadonlyMap<string, JsonKind>;
  /** Column names in declaration order (pragma_table_info cid order). */
  readonly order: readonly string[];
  /** Record-view column defaults (SQL expressions). */
  readonly defaults: ReadonlyMap<string, string>;
  readonly recordView: boolean;
  /** False when pragma_table_info found no table or view of that name. */
  readonly exists: boolean;
}

export class CatalogError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CatalogError";
  }
}

/** host_calls.rs `valid_identifier`. */
function validIdentifier(name: string): boolean {
  if (!name || name.length > 63) return false;
  return Array.from(name).every((char, index) => /[A-Za-z_]/.test(char) || (index > 0 && /[0-9$]/.test(char)));
}

/** host_calls.rs `quote_identifier`: `name` or `schema.name`, plain identifiers only. */
export function quoteIdentifier(name: string): string {
  const trimmed = name.trim();
  const parts = trimmed.split(".");
  if (!trimmed || parts.length > 2 || parts.some((part) => !validIdentifier(part))) {
    throw new CatalogError(`identifier ${JSON.stringify(trimmed)} is not a table or column name`);
  }
  return parts.map((part) => `"${part}"`).join(".");
}

/** host_calls.rs `table_catalog_parts`. */
function catalogParts(table: string): [schema: string | undefined, table: string] {
  quoteIdentifier(table);
  const parts = table.trim().split(".");
  if (parts.length === 1) return [undefined, parts[0]!];
  return [parts[0], parts[1]!];
}

/** Statements the catalog runs; the session counts their rows. */
export interface CatalogReader {
  rows(sql: string, ...binds: (string | number | null)[]): Record<string, unknown>[];
}

interface RecordSchema {
  key?: unknown;
  columns?: Record<string, { type?: unknown; default?: unknown; nullable?: unknown }>;
}

export class Catalog {
  private readonly tables = new Map<string, TableCatalog>();
  private recordViews: boolean | undefined;
  private generationValue = 0;

  /** Bumped by every invalidation; caches derived from the schema compare it. */
  get generation(): number {
    return this.generationValue;
  }

  invalidate(): void {
    this.tables.clear();
    this.recordViews = undefined;
    this.generationValue++;
  }

  cached(table: string): TableCatalog | undefined {
    return this.tables.get(table);
  }

  /** The catalog of `table`, reading it on a miss. Throws CatalogError for a bad name or record schema. */
  lookup(table: string, reader: CatalogReader): TableCatalog {
    const cached = this.tables.get(table);
    if (cached) return cached;
    const [schema, name] = catalogParts(table);
    const info = reader.rows(
      "SELECT name, type, pk, dflt_value IS NOT NULL AS has_default FROM pragma_table_info(?1, COALESCE(?2, 'main')) ORDER BY cid",
      name,
      schema ?? null,
    );
    let record: RecordSchema | undefined;
    if (schema === undefined || schema.toLowerCase() === "main") {
      this.recordViews ??= Number(reader.rows("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_record_views') AS present")[0]?.present) === 1;
      if (this.recordViews) {
        const found = reader.rows("SELECT schema FROM _gonvex_record_views WHERE name=?1 COLLATE NOCASE", name)[0];
        if (found) {
          try {
            record = JSON.parse(String(found.schema)) as RecordSchema;
          } catch (error) {
            throw new CatalogError(String((error as Error).message));
          }
        }
      }
    }
    const order = info.map((row) => String(row.name));
    let catalog: TableCatalog;
    if (record) {
      if (typeof record.key !== "string") throw new CatalogError("record schema has no key");
      if (!record.columns || typeof record.columns !== "object") throw new CatalogError("record schema has no columns");
      const keyDefinition = record.columns[record.key];
      if (!keyDefinition) throw new CatalogError("record key is not a column");
      const columns = new Map<string, JsonKind>();
      const defaults = new Map<string, string>();
      for (const [column, definition] of Object.entries(record.columns)) {
        if (typeof definition?.type !== "string") throw new CatalogError("record column has no type");
        columns.set(column, kindFromDeclared(definition.type));
        if (typeof definition.default === "string") defaults.set(column, definition.default);
      }
      catalog = {
        key: { column: record.key, dataType: typeof keyDefinition.type === "string" ? keyDefinition.type : undefined, databaseGenerated: defaults.has(record.key) },
        columns, order, defaults, recordView: true, exists: true,
      };
    } else {
      const columns = new Map<string, JsonKind>();
      const keys: [number, string, string, boolean][] = [];
      for (const row of info) {
        const declared = String(row.type ?? "");
        columns.set(String(row.name), kindFromDeclared(declared));
        const position = Number(row.pk);
        if (position > 0) keys.push([position, String(row.name), declared, Number(row.has_default) !== 0]);
      }
      keys.sort((a, b) => a[0] - b[0] || (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0));
      if (keys.length > 1) throw new CatalogError(`table ${JSON.stringify(table)} has a composite primary key, which db.update/delete do not support`);
      const only = keys[0];
      const key: TableKey = only
        ? { column: only[1], dataType: only[2], databaseGenerated: only[3] || only[2].trim().toUpperCase() === "INTEGER" }
        : { column: DEFAULT_KEY, dataType: undefined, databaseGenerated: true };
      catalog = { key, columns, order, defaults: new Map(), recordView: false, exists: info.length > 0 };
    }
    this.tables.set(table, catalog);
    return catalog;
  }
}
