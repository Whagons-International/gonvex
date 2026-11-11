// Table catalogs for the database host calls (host_calls.rs catalog_table)
// and the object-wide cache in front of them (0016 G0-12, R04).
//
// Rust reads `pragma_table_info` and `_gonvex_record_views` once per
// invocation transaction. S2 measured that these reads are 95-99 % of the rows
// a Reducer reads in a Durable Object: the `_gonvex_record_views` presence
// check scans `sqlite_schema`, which has thousands of rows. Catalogs only
// change with the schema, so the tenant object keeps them in memory, keyed by
// a schema epoch:
//
// - `invalidate()` on deploy (a new generation) and after every migration
//   (K02) or any other kernel DDL;
// - an invocation that runs DDL, or writes `_gonvex_record_views`, stops
//   using the shared cache for the rest of its transaction (it may roll back)
//   and invalidates it when it ends, committed or not;
// - a missing table is never cached across invocations.
//
// The cache is not contract (CONTRACT.md "Caches … are not part of the contract").

export type JsonKind = "json" | "boolean" | "integer" | "real" | "text";

/** JsonKind::from_declared. */
export function kindOf(declared: unknown): JsonKind {
  const type = String(declared ?? "").toUpperCase();
  if (type.includes("JSON")) return "json";
  if (type.includes("BOOL")) return "boolean";
  if (type.includes("INT")) return "integer";
  if (/REAL|DOUB|FLOA|NUMERIC|DECIMAL/.test(type)) return "real";
  return "text";
}

export interface TableKey {
  column: string;
  dataType: string | undefined;
  databaseGenerated: boolean;
}

export interface TableCatalog {
  key: TableKey;
  columns: ReadonlyMap<string, JsonKind>;
  order: readonly string[];
  defaults: ReadonlyMap<string, string>;
  recordView: boolean;
  /** Zero columns: the table does not exist (Rust still builds a catalog; the statement then fails). */
  missing: boolean;
}

export interface CatalogStats {
  hits: number;
  misses: number;
  invalidations: number;
}

/** Object-wide catalog cache. One per tenant object. */
export class CatalogCache {
  private tables = new Map<string, TableCatalog>();
  private recordViewsPresent: boolean | undefined;
  private recordSchemas = new Map<string, string | null>();
  /** Every `_gonvex_record_views` row is in `recordSchemas` (a miss means "no record"). */
  private recordSchemasLoaded = false;
  /** Bumped by every invalidation; an entry read under an older epoch is dropped. */
  epoch = 0;
  readonly stats: CatalogStats = { hits: 0, misses: 0, invalidations: 0 };

  constructor(readonly enabled = true) {}

  invalidate(): void {
    this.tables = new Map();
    this.recordViewsPresent = undefined;
    this.recordSchemas = new Map();
    this.recordSchemasLoaded = false;
    this.epoch++;
    this.stats.invalidations++;
  }

  getTable(name: string): TableCatalog | undefined {
    if (!this.enabled) return undefined;
    const found = this.tables.get(name);
    if (found) this.stats.hits++; else this.stats.misses++;
    return found;
  }

  setTable(name: string, catalog: TableCatalog, epoch: number): void {
    if (this.enabled && epoch === this.epoch && !catalog.missing) this.tables.set(name, catalog);
  }

  getRecordViewsPresent(): boolean | undefined {
    return this.enabled ? this.recordViewsPresent : undefined;
  }

  setRecordViewsPresent(value: boolean, epoch: number): void {
    if (this.enabled && epoch === this.epoch) this.recordViewsPresent = value;
  }

  /** `key` is `nocase:<lower name>` (catalog lookup, COLLATE NOCASE) or `exact:<name>` (ctx.db.recordColumns). */
  getRecordSchema(key: string): string | null | undefined {
    if (!this.enabled) return undefined;
    const found = this.recordSchemas.get(key);
    return found !== undefined ? found : this.recordSchemasLoaded ? null : undefined;
  }

  get allRecordSchemasLoaded(): boolean { return this.enabled && this.recordSchemasLoaded; }

  /**
   * Loads every record schema at once. `name COLLATE NOCASE` cannot use the
   * table's BINARY primary key, so each per-table lookup scans the whole
   * table; one scan per epoch replaces all of them. For a NOCASE lookup the
   * first row in scan order wins, as SQLite's would.
   */
  loadRecordSchemas(rows: readonly (readonly [string, string])[], epoch: number): void {
    if (!this.enabled || epoch !== this.epoch) return;
    for (const [name, schema] of rows) {
      this.recordSchemas.set(`exact:${name}`, schema);
      if (!this.recordSchemas.has(`nocase:${name.toLowerCase()}`)) this.recordSchemas.set(`nocase:${name.toLowerCase()}`, schema);
    }
    this.recordSchemasLoaded = true;
  }

  setRecordSchema(key: string, schema: string | null, epoch: number): void {
    if (this.enabled && epoch === this.epoch) this.recordSchemas.set(key, schema);
  }
}
