// Database host calls for one Query or Reducer invocation in the tenant
// object: the semantics of rust/crates/runtime/src/host_calls.rs (SqlCalls +
// DatabaseHostCalls) at the reference commit, ported from the S2 spike
// (cloudflare/spikes/s2/src/host.js) onto the K01 storage interface.
//
// Every call is synchronous SQL on the invocation's storage transaction
// except module savepoints, which await a nested storage transaction (0006).
import { CatalogCache, kindOf, type JsonKind, type TableCatalog } from "./catalog.js";
import type { HostPayload, Identity, InvocationInfo } from "./context.js";
import { HostFailure, type ModuleHost } from "./engine.js";
import { classifySqliteCode, hostErrors } from "./errors.js";
import { admit, requireReadHead, savepointCommand, selectProjection, writeTarget, type Token } from "./sql.js";
import { SqlError, type ExecResult, type InvocationTransaction, type SqlValue } from "./storage.js";

export const ABORTED = "current transaction is aborted, commands ignored until end of transaction block";
export const SCHEDULE_OUTBOX_PATH = "_gonvex.scheduler.enqueue";
const DEFAULT_KEY = "id";
const RECORD_VIEWS = "_gonvex_record_views";

/** In-memory permission bits of the tenant (adapt: B.2a `permissionsHas`). */
export interface PermissionIndex {
  /** `true`/`false`, or `"rebuilding"` while the bits do not yet cover the transaction's revision. */
  has(memberId: string, permission: string, scope: string): boolean | "rebuilding";
}

/** `storage.getMetadata` inside a Reducer (K13 owns it: `_gonvex_files`, R2 head, IB1 owner check). */
export interface FileMetadataSource {
  /** Returns the metadata, or throws an Error with Rust's text ("file not found"). */
  getMetadata(fileId: string, tx: InvocationTransaction, identity: Identity): unknown;
}

export interface DatabaseHostOptions {
  mode: "query" | "reducer";
  tx: InvocationTransaction;
  cache: CatalogCache;
  identity: Identity;
  invocation: InvocationInfo;
  /** Registered Reducer or Action of the active generation (scheduler targets). */
  schedulable: (path: string) => boolean;
  permissions?: PermissionIndex | undefined;
  files?: FileMetadataSource | undefined;
  /** Primary key the module schema declares for a table (host_calls.rs declared_table_key). */
  declaredKey?: ((table: string) => string | undefined) | undefined;
  /** Server clock for `runAfter` (B.4: not `ctx.now`). */
  wallClock: () => number;
  randomUUID: () => string;
  subtle: SubtleCrypto;
}

export interface DatabaseHostStats {
  calls: number;
  byKind: Record<string, number>;
  statements: number;
  rowsRead: number;
  rowsWritten: number;
  catalogStatements: number;
  catalogRowsRead: number;
  catalogHits: number;
  savepoints: number;
  transientFault: boolean;
}

const hex = (buffer: ArrayBuffer | Uint8Array) => Array.from(buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer), (b) => b.toString(16).padStart(2, "0")).join("");
const jsonShaped = (text: string) => { const t = text.trim(); return t[0] === "{" || t[0] === "[" || t[0] === '"' || t === "true" || t === "false"; };

/** direct.rs write_json: one cell by its declared kind (undefined: expression cell). */
export function decodeCell(kind: JsonKind | undefined, value: SqlValue | undefined): unknown {
  if (value === null || value === undefined) return null;
  if (value instanceof ArrayBuffer) return hex(value);
  if (typeof value === "bigint") return Number(value);
  if (kind === "boolean" && typeof value === "number") return value !== 0;
  if (kind === "json" && typeof value === "string") { try { return JSON.parse(value); } catch { return value; } }
  if (kind === undefined && typeof value === "string" && jsonShaped(value)) { try { return JSON.parse(value); } catch { return value; } }
  return value;
}

/** sql_parameters.rs Parameter::scalar. */
function scalar(value: unknown): SqlValue {
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "string") return value;
  if (typeof value === "number") {
    if (Number.isInteger(value) && value >= 2 ** 63) throw new Error("integer is outside the SQLite integer range");
    return value;
  }
  return JSON.stringify(value);
}

/** Parameter::column: JSON columns always store serialized JSON, BOOLEAN stores 0/1. */
function columnParameter(kind: JsonKind | undefined, value: unknown): SqlValue {
  if (value === null || value === undefined) return null;
  if (kind === "json") return JSON.stringify(value);
  if (kind === "boolean" && typeof value === "boolean") return value ? 1 : 0;
  return scalar(value);
}

function validIdentifier(name: string): boolean {
  return name.length > 0 && name.length <= 63 && /^[A-Za-z_][A-Za-z0-9_$]*$/.test(name);
}

/** host_calls.rs quote_identifier. */
export function quoteIdentifier(name: string): string {
  const trimmed = name.trim();
  const parts = trimmed.split(".");
  if (!trimmed || parts.length > 2 || !parts.every(validIdentifier)) throw new Error(`identifier ${JSON.stringify(trimmed)} is not a table or column name`);
  return parts.map((part) => `"${part}"`).join(".");
}

function catalogParts(table: string): [string | undefined, string] {
  quoteIdentifier(table);
  const parts = table.trim().split(".");
  if (parts.length === 1) return [undefined, parts[0] as string];
  return [parts[0], parts[1] as string];
}

/** Same seed and UUID layout as module-sdk reducerRowId / host_calls.rs intent_deferred_id. */
export async function intentDeferredId(subtle: SubtleCrypto, tenant: string, account: string, command: string, kind: string, ordinal: number): Promise<string> {
  const seed = JSON.stringify(["gonvex.reducer.ids.v1", tenant, account, command, `deferred:${kind}`, ordinal]);
  const bytes = new Uint8Array(await subtle.digest("SHA-256", new TextEncoder().encode(seed))).slice(0, 16);
  bytes[6] = ((bytes[6] as number) & 15) | 128;
  bytes[8] = ((bytes[8] as number) & 63) | 128;
  const h = hex(bytes);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

const byName = ([a]: [string, unknown], [b]: [string, unknown]) => (a < b ? -1 : a > b ? 1 : 0);

export class DatabaseHost implements ModuleHost {
  readonly stats: DatabaseHostStats = { calls: 0, byKind: {}, statements: 0, rowsRead: 0, rowsWritten: 0, catalogStatements: 0, catalogRowsRead: 0, catalogHits: 0, savepoints: 0, transientFault: false };
  /** Rust's `aborted`: set by a failed statement, cleared by ROLLBACK TO. */
  aborted: string | undefined;
  /** The invocation changed the schema (DDL or `_gonvex_record_views`); the shared cache is bypassed and invalidated at the end. */
  schemaDirty = false;
  private readonly local = new Map<string, TableCatalog>();
  private readonly ordinals = new Map<string, number>();
  private readonly epoch: number;

  constructor(private readonly options: DatabaseHostOptions) {
    this.epoch = options.cache.epoch;
  }

  /** Host-call entry point: Rust's failure text is "<capability> host call failed [host_call_failed]: <message>". */
  async call(payload: HostPayload, capability: string): Promise<unknown> {
    this.stats.calls++;
    this.stats.byKind[payload.kind] = (this.stats.byKind[payload.kind] ?? 0) + 1;
    try {
      return await this.dispatch(payload);
    } catch (error) {
      throw new HostFailure(hostErrors.wrapped(capability, String((error as Error)?.message ?? error)));
    }
  }

  /** Called by the invocation runner when the transaction ends. */
  finish(): void {
    if (this.schemaDirty) this.options.cache.invalidate();
  }

  private get reducer() { return this.options.mode === "reducer"; }
  private requireOpen() { if (this.aborted !== undefined) throw new Error(ABORTED); }
  private requireWrite() { if (!this.reducer) throw new Error("a Query cannot modify application tables"); }

  private async dispatch(payload: HostPayload): Promise<unknown> {
    switch (payload.kind) {
      case "dbQuery": return this.query(payload["statement"], payload["parameters"], payload["catalog"]);
      case "dbInsert": this.requireWrite(); return this.insert(String(payload["table"]), payload["row"], payload["generatedId"]);
      case "dbUpdate": this.requireWrite(); return this.update(String(payload["table"]), String(payload["key"] ?? ""), payload["id"], payload["patch"]);
      case "dbDelete": this.requireWrite(); return this.delete(String(payload["table"]), String(payload["key"] ?? ""), payload["id"]);
      case "dbDeleteMany": this.requireWrite(); return this.deleteMany(String(payload["table"]), String(payload["key"] ?? ""), payload["ids"]);
      case "permissionsHas": return this.permissionsHas(String(payload["permission"]), String(payload["scope"] ?? ""));
      case "actionEnqueue": return this.enqueueAction(payload["function"], payload["args"]);
      case "scheduleAfter": {
        this.requireWrite();
        const delay = Number(payload["delayMs"]);
        if (!Number.isSafeInteger(delay) || delay < 0) throw new Error("scheduler delayMs is outside the supported range");
        return this.enqueueScheduled(payload["function"], payload["args"], this.options.wallClock() + delay, false);
      }
      case "scheduleAt": {
        this.requireWrite();
        const at = Number(payload["atUnixMs"]);
        if (!Number.isSafeInteger(at) || at < 0 || at > 8.64e15) throw new Error("scheduler atUnixMs is outside the supported range");
        return this.enqueueScheduled(payload["function"], payload["args"], at, payload["coalesce"] === true);
      }
      case "storage":
        if (payload["operation"] === "getMetadata") return this.getMetadata(payload["payload"]);
        break;
      default: break;
    }
    throw new Error(this.reducer ? "this Reducer host capability is not implemented in the Rust runtime yet" : "a Query may only use its read-only database capability");
  }

  // ---- SQL execution and accounting ---------------------------------------

  private exec(sql: string, binds: readonly SqlValue[], catalog = false): ExecResult {
    this.stats.statements++;
    if (catalog) this.stats.catalogStatements++;
    try {
      const result = this.options.tx.exec(sql, binds);
      this.stats.rowsRead += result.rowsRead;
      this.stats.rowsWritten += result.rowsWritten;
      if (catalog) this.stats.catalogRowsRead += result.rowsRead;
      return result;
    } catch (error) {
      const message = String((error as Error)?.message ?? error);
      if (error instanceof SqlError && error.code !== undefined && classifySqliteCode(error.code) === "transient") this.stats.transientFault = true;
      // A failed statement marks the transaction aborted (B.7); the store
      // refuses to commit it even if the module catches the error.
      this.aborted ??= message;
      throw new Error(message);
    }
  }

  private async query(statement: unknown, parameters: unknown, hint: unknown): Promise<unknown> {
    const text = String(statement ?? "");
    const admitted = admit(text);
    if (!this.reducer) requireReadHead(admitted.firstWord);
    const recovers = admitted.firstWord === "rollback";
    if (!recovers) this.requireOpen();
    let values: unknown[];
    if (parameters === null || parameters === undefined) values = [];
    else if (Array.isArray(parameters)) values = parameters;
    else throw new Error("query parameters must be an array");
    const binds = values.map((value, index) => {
      try { return scalar(value); } catch (error) { throw new Error(`parameter $${index + 1}: ${(error as Error).message}`); }
    });

    const savepoint = savepointCommand(admitted.tokens);
    if (savepoint) {
      // DO SQL refuses SAVEPOINT/RELEASE/ROLLBACK TO in exec; K01 maps them
      // onto nested storage transactions (0006, G0-3).
      this.stats.savepoints++;
      try {
        if (savepoint.command === "savepoint") await this.options.tx.savepoint(savepoint.name);
        else if (savepoint.command === "release") await this.options.tx.release(savepoint.name);
        else await this.options.tx.rollbackTo(savepoint.name);
      } catch (error) {
        const message = String((error as Error)?.message ?? error);
        this.aborted ??= message;
        throw new Error(message);
      }
      if (recovers) this.aborted = undefined;
      return [];
    }

    const cached = this.cachedCatalogQuery(hint, values);
    if (cached !== undefined) return cached;

    const target = writeTarget(admitted.tokens);
    let catalog: TableCatalog | undefined;
    if (target !== undefined && this.isIdentifier(target)) catalog = this.catalog(target);
    const projection = catalog ? undefined : this.projectionKinds(admitted.tokens);
    const head = admitted.firstWord;
    const changesSchema = head === "create" || head === "alter" || head === "drop";
    if (changesSchema || (target !== undefined && target.toLowerCase() === RECORD_VIEWS)) this.markSchemaDirty();
    const result = this.exec(text.trim(), binds);
    if (changesSchema) this.local.clear();
    const kinds = result.columnNames.map((name, index) => {
      const fromProjection = projection && projection.length === result.columnNames.length ? projection[index] : undefined;
      return fromProjection ?? catalog?.columns.get(name);
    });
    if (recovers) this.aborted = undefined;
    return result.rows.map((row) => Object.fromEntries(result.columnNames.map((name, index) => [name, decodeCell(kinds[index], row[index])])));
  }

  private isIdentifier(name: string): boolean {
    try { quoteIdentifier(name); return true; } catch { return false; }
  }

  private markSchemaDirty(): void {
    if (!this.schemaDirty) {
      this.schemaDirty = true;
      this.local.clear();
      // Entries cached before this point were read before the change.
      this.options.cache.invalidate();
    }
  }

  private shared(): boolean { return !this.schemaDirty && this.options.cache.enabled; }

  /**
   * The two statements of ctx.db.recordColumns (BS:575-585), answered from
   * the cache when it holds them. Same result and aborted rule as the SQL.
   */
  private cachedCatalogQuery(hint: unknown, values: unknown[]): unknown[] | undefined {
    if (!this.shared()) return undefined;
    const cache = this.options.cache;
    if (hint === "recordViewsPresent") {
      let present = cache.getRecordViewsPresent();
      if (present === undefined) {
        present = this.recordViewsPresent();
      } else this.stats.catalogHits++;
      return [{ present: present ? 1 : 0 }];
    }
    const name = values[0];
    if (hint === "recordViewSchema" && typeof name === "string") {
      if (!this.recordViewsPresent()) return undefined;
      const schema = this.recordSchema(`exact:${name}`, () => this.exec(`SELECT schema FROM ${RECORD_VIEWS} WHERE name=?1`, [name], true));
      return schema === null ? [] : [{ schema }];
    }
    return undefined;
  }

  private recordViewsPresent(): boolean {
    const cache = this.options.cache;
    const known = this.shared() ? cache.getRecordViewsPresent() : undefined;
    if (known !== undefined) { this.stats.catalogHits++; return known; }
    const present = this.exec(`SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='${RECORD_VIEWS}')`, [], true).rows[0]?.[0] === 1;
    if (this.shared()) cache.setRecordViewsPresent(present, this.epoch);
    return present;
  }

  /**
   * One record schema. With the shared cache, the first lookup of an epoch
   * loads every row of `_gonvex_record_views`; without it, Rust's single-row
   * statement runs (`lookup`).
   */
  private recordSchema(key: string, lookup: () => ExecResult): string | null {
    const cache = this.options.cache;
    if (this.shared()) {
      const cached = cache.getRecordSchema(key);
      if (cached !== undefined) { this.stats.catalogHits++; return cached; }
      if (!cache.allRecordSchemasLoaded) {
        const all = this.exec(`SELECT name, schema FROM ${RECORD_VIEWS}`, [], true);
        cache.loadRecordSchemas(all.rows.map((row) => [String(row[0]), String(row[1])] as const), this.epoch);
        const loaded = cache.getRecordSchema(key);
        if (loaded !== undefined) return loaded;
      }
    }
    const row = lookup().rows[0];
    return row ? String(row[0]) : null;
  }

  private projectionKinds(tokens: readonly Token[]): (JsonKind | undefined)[] | undefined {
    const columns = selectProjection(tokens);
    if (!columns) return undefined;
    const kinds: (JsonKind | undefined)[] = [];
    for (const column of columns) {
      if (column.star) {
        for (const table of column.tables) {
          const catalog = this.tryCatalog(table);
          if (!catalog || catalog.missing) return undefined;
          for (const name of catalog.order) kinds.push(catalog.columns.get(name));
        }
        continue;
      }
      if (!column.source) { kinds.push(undefined); continue; }
      const [table, name, candidates] = column.source;
      if (table) { kinds.push(this.tryCatalog(table)?.columns.get(name)); continue; }
      const owners = (candidates ?? []).map((t) => this.tryCatalog(t)).filter((c): c is TableCatalog => c !== undefined && c.columns.has(name));
      kinds.push(owners.length === 1 ? owners[0]?.columns.get(name) : undefined);
    }
    return kinds;
  }

  private tryCatalog(table: string): TableCatalog | undefined {
    if (!this.isIdentifier(table)) return undefined;
    try { return this.catalog(table); } catch { return undefined; }
  }

  /** host_calls.rs catalog_table, behind the per-invocation map and the object-wide cache. */
  catalog(table: string): TableCatalog {
    const local = this.local.get(table);
    if (local) return local;
    if (this.shared()) {
      const cached = this.options.cache.getTable(table);
      if (cached) { this.stats.catalogHits++; this.local.set(table, cached); return cached; }
    }
    const [schemaName, tableName] = catalogParts(table);
    this.requireOpen();
    const info = this.exec("SELECT name, type, pk, dflt_value IS NOT NULL AS has_default FROM pragma_table_info(?1, COALESCE(?2, 'main')) ORDER BY cid", [tableName, schemaName ?? null], true);
    const rows = info.rows.map((r) => [String(r[0]), String(r[1] ?? ""), Number(r[2]), Number(r[3]) !== 0] as const);
    const order = rows.map((r) => r[0]);
    let record: { key?: unknown; columns?: Record<string, { type?: unknown; default?: unknown }> } | undefined;
    if (schemaName === undefined || schemaName.toLowerCase() === "main") {
      if (this.recordViewsPresent()) {
        const schema = this.recordSchema(`nocase:${tableName.toLowerCase()}`, () => this.exec(`SELECT schema FROM ${RECORD_VIEWS} WHERE name=?1 COLLATE NOCASE`, [tableName], true));
        if (schema !== null) record = JSON.parse(schema) as typeof record;
      }
    }
    let catalog: TableCatalog;
    if (record) {
      if (typeof record.key !== "string") throw new Error("record schema has no key");
      const definitions = record.columns;
      if (!definitions || typeof definitions !== "object") throw new Error("record schema has no columns");
      const keyDefinition = definitions[record.key];
      if (!keyDefinition) throw new Error("record key is not a column");
      const columns = new Map<string, JsonKind>(), defaults = new Map<string, string>();
      for (const [name, definition] of Object.entries(definitions)) {
        if (typeof definition.type !== "string") throw new Error("record column has no type");
        columns.set(name, kindOf(definition.type));
        if (typeof definition.default === "string") defaults.set(name, definition.default);
      }
      catalog = {
        key: { column: record.key, dataType: typeof keyDefinition.type === "string" ? keyDefinition.type : undefined, databaseGenerated: defaults.has(record.key) },
        columns, order, defaults, recordView: true, missing: rows.length === 0,
      };
    } else {
      const columns = new Map<string, JsonKind>();
      const keys: [number, string, string, boolean][] = [];
      for (const [name, declared, pk, hasDefault] of rows) {
        columns.set(name, kindOf(declared));
        if (pk > 0) keys.push([pk, name, declared, hasDefault]);
      }
      keys.sort((a, b) => a[0] - b[0]);
      if (keys.length > 1) throw new Error(`table ${JSON.stringify(table)} has a composite primary key, which db.update/delete do not support`);
      const [only] = keys;
      catalog = {
        key: only === undefined
          ? { column: DEFAULT_KEY, dataType: undefined, databaseGenerated: true }
          : { column: only[1], dataType: only[2], databaseGenerated: only[3] || only[2].trim().toUpperCase() === "INTEGER" },
        columns, order, defaults: new Map(), recordView: false, missing: rows.length === 0,
      };
    }
    this.local.set(table, catalog);
    if (this.shared()) this.options.cache.setTable(table, catalog, this.epoch);
    return catalog;
  }

  private textKey(key: TableCatalog["key"]): boolean {
    return key.dataType !== undefined && key.dataType.trim() !== "" && kindOf(key.dataType) === "text";
  }

  private returningRow(sql: string, binds: readonly SqlValue[], catalog: TableCatalog): Record<string, unknown> | null {
    const result = this.exec(sql, binds);
    const row = result.rows[0];
    if (!row) return null;
    return Object.fromEntries(result.columnNames.map((name, index) => [name, decodeCell(catalog.columns.get(name) ?? "text", row[index])]));
  }

  private returningRecordRow(table: string, sql: string, binds: readonly SqlValue[], catalog: TableCatalog): Record<string, unknown> | null {
    const row = this.returningRow(sql, binds, catalog);
    if (!catalog.recordView || row === null) return row;
    const key = row[catalog.key.column];
    if (key === undefined) throw new Error("record write did not return its key");
    return this.returningRow(`SELECT * FROM ${quoteIdentifier(table)} WHERE ${quoteIdentifier(catalog.key.column)}=?1`, [scalar(key)], catalog);
  }

  private object(value: unknown, name: string): [string, unknown][] {
    if (value === null || value === undefined) return [];
    if (typeof value !== "object" || Array.isArray(value)) throw new Error(`${name} must be an object`);
    return Object.entries(value as Record<string, unknown>);
  }

  private parameter(catalog: TableCatalog, column: string, value: unknown): SqlValue {
    try { return columnParameter(catalog.columns.get(column), value); } catch (error) { throw new Error(`column ${JSON.stringify(column)}: ${(error as Error).message}`); }
  }

  private insert(table: string, row: unknown, generatedId: unknown): unknown {
    this.requireOpen();
    const values = new Map(this.object(row, "row").sort(byName));
    const catalog = this.catalog(table);
    const key = catalog.key;
    if (!values.has(key.column) && typeof generatedId === "string" && this.textKey(key)) values.set(key.column, generatedId);
    if (!values.has(key.column) && !key.databaseGenerated && key.dataType !== undefined) {
      if (this.textKey(key)) values.set(key.column, this.options.randomUUID());
      else throw new Error(`insert into ${JSON.stringify(table)} requires primary key ${JSON.stringify(key.column)}; Gonvex can only allocate omitted text keys, but this column is ${key.dataType}`);
    }
    const ordered = [...values].sort(byName);
    const quotedTable = quoteIdentifier(table);
    const columns = ordered.map(([column]) => quoteIdentifier(column));
    const placeholders = ordered.map((_, index) => `?${index + 1}`);
    if (catalog.recordView) for (const [column, expression] of catalog.defaults) if (!values.has(column)) { columns.push(quoteIdentifier(column)); placeholders.push(expression); }
    const binds = ordered.map(([column, value]) => this.parameter(catalog, column, value));
    return this.returningRecordRow(table, `INSERT INTO ${quotedTable} (${columns.join(", ")}) VALUES (${placeholders.join(", ")}) RETURNING *`, binds, catalog);
  }

  /** host_calls.rs resolve_table_key: requested, else declared, else catalog. */
  private tableKey(table: string, requested: string): string {
    const wanted = requested.trim();
    const declared = this.options.declaredKey?.(table);
    if (wanted) {
      if (declared !== undefined && declared !== wanted) throw new Error(`table ${JSON.stringify(table)} declares primary key ${JSON.stringify(declared)}, not ${JSON.stringify(wanted)}`);
      return quoteIdentifier(wanted);
    }
    if (declared !== undefined) return quoteIdentifier(declared);
    return quoteIdentifier(this.catalog(table).key.column);
  }

  private requireRowId(id: unknown): void {
    if (typeof id !== "string" && typeof id !== "number") throw new Error("a row id must be a string or number");
  }

  private update(table: string, requestedKey: string, id: unknown, patch: unknown): unknown {
    this.requireOpen();
    const key = this.tableKey(table, requestedKey);
    this.requireRowId(id);
    const values = this.object(patch, "patch").sort(byName);
    if (!values.length) throw new Error("an update requires at least one column");
    const catalog = this.catalog(table);
    const quotedTable = quoteIdentifier(table);
    const assignments = values.map(([column], index) => `${quoteIdentifier(column)} = ?${index + 1}`);
    const binds = values.map(([column, value]) => this.parameter(catalog, column, value));
    binds.push(scalar(id));
    return this.returningRecordRow(table, `UPDATE ${quotedTable} SET ${assignments.join(", ")} WHERE ${key} = ?${values.length + 1} RETURNING *`, binds, catalog);
  }

  private delete(table: string, requestedKey: string, id: unknown): unknown {
    this.requireOpen();
    const key = this.tableKey(table, requestedKey);
    const quotedTable = quoteIdentifier(table);
    this.requireRowId(id);
    const result = this.exec(`DELETE FROM ${quotedTable} WHERE ${key} = ?1 RETURNING 1`, [scalar(id)]);
    return { deleted: result.rows.length };
  }

  private deleteMany(table: string, requestedKey: string, ids: unknown): unknown {
    this.requireOpen();
    if (!Array.isArray(ids)) throw new Error("deleteMany ids must be an array");
    if (ids.length > 10_000) throw new Error("deleteMany cannot delete more than 10000 rows per call");
    if (!ids.length) return { deleted: 0 };
    for (const id of ids) this.requireRowId(id);
    const key = this.tableKey(table, requestedKey);
    const quotedTable = quoteIdentifier(table);
    // One JSON parameter instead of up to 10,000 placeholders (and DO's 100-parameter cap).
    const result = this.exec(`DELETE FROM ${quotedTable} WHERE ${key} IN (SELECT value FROM json_each(?1)) RETURNING 1`, [JSON.stringify(ids)]);
    return { deleted: result.rows.length };
  }

  private permissionsHas(permission: string, scope: string): boolean {
    const memberId = this.options.identity.member?.id;
    if (!this.reducer || !this.options.permissions || !memberId) throw new Error("ctx.permissions is available to Reducers with a tenant session");
    const held = this.options.permissions.has(memberId, permission, scope);
    if (held === "rebuilding") throw new Error("permission bits are rebuilding; retry");
    return held;
  }

  private getMetadata(payload: unknown): unknown {
    if (!this.options.files) throw new Error("storage metadata is unavailable");
    const fileId = (payload as { fileId?: unknown } | null)?.fileId;
    if (typeof fileId !== "string" || !fileId.trim()) throw new Error("fileId is required");
    return this.options.files.getMetadata(fileId, this.options.tx, this.options.identity);
  }

  private async deferredId(kind: "action" | "schedule"): Promise<string> {
    const commandId = this.options.invocation.commandId;
    if (!commandId) return this.options.randomUUID();
    const ordinal = this.ordinals.get(kind) ?? 0;
    this.ordinals.set(kind, ordinal + 1);
    return intentDeferredId(this.options.subtle, this.options.identity.tenant?.id ?? "", this.options.identity.account?.id ?? "", commandId, kind, ordinal);
  }

  private provenance(): Record<string, unknown> {
    const i = this.options.invocation;
    return {
      commandId: i.commandId ?? null, rootCommandId: i.rootCommandId ?? null, rootChannel: i.rootChannel ?? null, channel: i.channel ?? null,
      actorAccountId: i.actorAccountId ?? null, actorMemberId: i.actorMemberId ?? null, onBehalfOfMemberId: i.onBehalfOfMemberId ?? null, agentExecutionId: null,
    };
  }

  private async enqueueAction(path: unknown, args: unknown): Promise<string> {
    this.requireWrite();
    this.requireOpen();
    const name = String(path ?? "").trim();
    if (!name) throw new Error("actions.enqueue requires an Action path");
    if (name === SCHEDULE_OUTBOX_PATH) throw new Error("actions.enqueue cannot invoke a Gonvex system operation");
    const id = await this.deferredId("action");
    this.writeOutbox(id, name, args);
    return id;
  }

  private async enqueueScheduled(path: unknown, args: unknown, atUnixMs: number, coalesce: boolean): Promise<string> {
    this.requireOpen();
    const name = String(path ?? "").trim();
    if (!this.options.schedulable(name)) throw new Error(`scheduled function ${JSON.stringify(name)} is not a registered Reducer or Action`);
    const jobId = `job_${await this.deferredId("schedule")}`;
    // store::enqueue_action: the outbox row id itself is a random UUID.
    this.writeOutbox(this.options.randomUUID(), SCHEDULE_OUTBOX_PATH, { jobId, function: name, args: args ?? null, runAtUnixMs: atUnixMs, coalesce });
    return jobId;
  }

  private writeOutbox(id: string, path: string, args: unknown): void {
    const account = this.options.identity.account;
    this.exec(
      "INSERT INTO _gonvex_action_outbox (id, action_path, args, actor_user_id, actor_email, provenance) VALUES (?1, ?2, ?3, NULLIF(?4, ''), NULLIF(?5, ''), ?6)",
      [id, path, JSON.stringify(args ?? null), account?.id ?? "", account?.email ?? "", JSON.stringify(this.provenance())],
    );
  }
}
