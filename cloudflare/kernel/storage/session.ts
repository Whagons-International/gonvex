// One Query or Reducer invocation against the tenant's Durable Object SQLite.
//
// Transactions (contract-changes 0016 G0-3, proven by S2):
//   - one `ctx.storage.transaction(async …)` per invocation, around module
//     code that awaits every host call;
//   - module `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` map onto nested storage
//     transactions held open on a LIFO stack (Durable Object SQL refuses the
//     statements themselves);
//   - Rust's rules hold: after a failed statement every call fails with
//     "current transaction is aborted…" until `ROLLBACK TO`, and a Reducer
//     that caught a SQL error without recovering is not committed;
//   - a Query never writes (0011 §2) and never commits;
//   - before a Reducer's transaction ends, the deferred foreign keys its
//     writes could break are checked (0016 G0-5), so a violation is a clean
//     Reducer error and never resets the object.
//
// Statements (module SQL through `query`): single-statement admission and
// refusal of ATTACH/DETACH/PRAGMA/VACUUM (sql_admission.rs), `$N` binding by
// number (0011 §1), integers bound as INTEGER, REGEXP without a custom
// function, rows decoded by declared type, and at most 100 bound parameters
// per statement (wide FROM-less SELECTs, as existsDataRows builds, are split).
//
// Every statement's rows read and written are counted for budgets.

import {
  AdmissionError,
  admitStatement,
  readStatementWrites,
  requireReadHead,
  savepointCommand,
  SavepointSyntaxError,
  writeTarget,
  type AdmittedStatement,
  type WriteOperation,
} from "./admission";
import { Catalog, CatalogError, quoteIdentifier, type TableCatalog } from "./catalog";
import {
  ABORTED,
  databaseMessage,
  FOREIGN_KEY_FAILED,
  READ_ONLY_WRITE,
  sqliteFailure,
  StorageError,
} from "./errors";
import { checkDeferredForeignKeys, ForeignKeyModel, WrittenSet, type ForeignKeyCheckResult } from "./foreign-keys";
import { isSymbol, isWord, tokenize, type Token } from "./lexer";
import {
  assignSlots,
  BindError,
  MAX_BOUND_PARAMETERS,
  restoreColumnName,
  StatementWriter,
  type Rewrite,
  type SlotAssignment,
  type SqlBind,
} from "./parameters";
import { projectionKinds } from "./projection";
import { RegexpRewriteError, regexpRewrites, RegexpUnsupportedError } from "./regexp";
import {
  decodeCell,
  decodeExpressionCell,
  ParameterError,
  scalarParameter,
  type JsonKind,
  type JsonObject,
  type JsonValue,
  type Parameter,
} from "./values";

export const CLOSED = "the invocation transaction is closed";

/** What the session needs from `ctx.storage`. */
export interface StorageHandle {
  readonly sql: SqlStorage;
  transaction<T>(closure: (txn: DurableObjectTransaction) => Promise<T>): Promise<T>;
}

export type InvocationKind = "reducer" | "query";

export interface RowCounters {
  statements: number;
  rowsRead: number;
  rowsWritten: number;
}

export interface InvocationStats extends RowCounters {
  /** Catalog lookups (pragma_table_info, record views) among the totals above. */
  readonly catalog: RowCounters;
  /** Schema model reads for the foreign key check (once per schema generation). */
  readonly schema: RowCounters;
  /** Module savepoint commands (SAVEPOINT, RELEASE, ROLLBACK TO). */
  savepoints: number;
  /** The deferred foreign key check, when the Reducer wrote. */
  foreignKeyCheck: Omit<ForeignKeyCheckResult, "violation"> | undefined;
}

export type InvocationResult<T> =
  | { readonly ok: true; readonly value: T; readonly committed: boolean; readonly stats: InvocationStats }
  | { readonly ok: false; readonly error: unknown; readonly stats: InvocationStats };

/** A raw result: column names, cells as Durable Object SQL returns them, and row counts. */
export interface RawResult {
  readonly columnNames: readonly string[];
  readonly rows: readonly (readonly SqlStorageValue[])[];
  readonly rowsRead: number;
  readonly rowsWritten: number;
}

/**
 * How writes reach the deferred foreign key check: "statements" closes each
 * writing statement's table over the schema's triggers and FK actions and
 * checks per table; "rows" relies only on `noteRows` reports (change
 * capture, K03), which name the rows, trigger writes included; "mixed"
 * (0028 §2d) tracks statements, but keeps the tables `rowReported` names
 * row-level, because capture reports their rows before the check. Every
 * other table written (derived, metadata, record storage, `_gonvex_*`)
 * stays table-level.
 */
export type WriteTracking = "statements" | "rows" | "mixed";

export interface TenantStorageOptions {
  readonly writeTracking?: WriteTracking;
  /**
   * Mixed mode: whether capture reports the written rows of a table (a
   * captured entity table with rowids; K03 `finalize` with `reportRows`).
   * Called with the lowercase table name.
   */
  readonly rowReported?: (table: string) => boolean;
  /** Clock for the foreign key check's duration; performance.now() by default. */
  readonly now?: () => number;
}

/**
 * The storage layer of one tenant object. Holds what outlives an invocation:
 * the catalog cache and the foreign key model, both per schema generation.
 */
export class TenantStorage {
  readonly catalog = new Catalog();
  private model: { generation: number; model: ForeignKeyModel } | undefined;
  private active = false;
  readonly now: () => number;
  writeTracking: WriteTracking;
  rowReported: ((table: string) => boolean) | undefined;

  constructor(readonly storage: StorageHandle, options: TenantStorageOptions = {}) {
    this.writeTracking = options.writeTracking ?? "statements";
    this.rowReported = options.rowReported;
    this.now = options.now ?? (() => performance.now());
  }

  /** Call after anything outside module SQL changes the schema (migrations, a new bundle generation). */
  invalidateSchema(): void {
    this.catalog.invalidate();
    this.model = undefined;
  }

  /** The foreign key model for the current schema generation. */
  foreignKeyModel(reader: (sql: string, ...binds: SqlBind[]) => Record<string, unknown>[]): ForeignKeyModel {
    if (this.model?.generation !== this.catalog.generation) {
      this.model = { generation: this.catalog.generation, model: new ForeignKeyModel({ rows: reader }) };
    }
    return this.model.model;
  }

  /**
   * Runs `body` as one invocation in one storage transaction. A Reducer
   * commits when `body` returns, no statement failure is outstanding and the
   * deferred foreign keys hold; anything else rolls back. A Query always
   * rolls back. Errors from `body` are returned, not thrown.
   */
  async invoke<T>(kind: InvocationKind, body: (session: StorageSession) => Promise<T>): Promise<InvocationResult<T>> {
    if (this.active) throw new Error("an invocation is already running on this tenant object");
    this.active = true;
    const session = new StorageSession(this, kind);
    const rollback = Symbol("rollback");
    let result: InvocationResult<T> | undefined;
    try {
      await this.storage.transaction(async () => {
        let value: T | undefined;
        let error: unknown;
        let failed = false;
        try {
          value = await body(session);
        } catch (thrown) {
          failed = true;
          error = thrown;
        }
        const commit = !failed && kind === "reducer" && !session.discarded;
        await session.settleSavepoints(commit);
        if (commit) {
          if (session.aborted !== undefined) {
            failed = true;
            error = new StorageError(ABORTED, "aborted", undefined, session.aborted);
          } else {
            const violation = session.checkForeignKeys();
            if (violation) {
              failed = true;
              error = new StorageError(FOREIGN_KEY_FAILED, "foreign_key", 787,
                `${violation.table} rowid ${violation.rowid} -> ${violation.parent} (fkid ${violation.fkid})`);
            }
          }
        }
        session.close();
        // Schema changes that roll back must not survive in the caches.
        if (session.changedSchema && (!commit || failed)) this.invalidateSchema();
        result = failed
          ? { ok: false, error, stats: session.stats }
          : { ok: true, value: value as T, committed: commit, stats: session.stats };
        if (!commit || failed) throw rollback;
      });
    } catch (thrown) {
      if (thrown !== rollback) {
        session.close();
        if (session.changedSchema) this.invalidateSchema();
        throw thrown;
      }
    } finally {
      this.active = false;
    }
    return result!;
  }
}

interface PlannedStatement {
  readonly sql: string;
  readonly binds: readonly SqlBind[];
  readonly renames: ReadonlyMap<string, string>;
}

interface OpenSavepoint {
  readonly name: string;
  readonly settle: { resolve: () => void; reject: (reason: unknown) => void };
  readonly done: Promise<unknown>;
}

const emptyCounters = (): RowCounters => ({ statements: 0, rowsRead: 0, rowsWritten: 0 });

/** Statement heads that change the schema (host_calls.rs `changes_schema`). */
const SCHEMA_HEADS = new Set(["create", "alter", "drop"]);

export class StorageSession {
  /** The first statement failure since the last ROLLBACK TO (Rust's `aborted`). */
  aborted: string | undefined;
  readonly stats: InvocationStats = { ...emptyCounters(), catalog: emptyCounters(), schema: emptyCounters(), savepoints: 0, foreignKeyCheck: undefined };
  private readonly written = new WrittenSet();
  /** Set by `discard()`: roll back even on success. */
  discarded = false;
  /** Module DDL ran: caches must be dropped again if it rolls back. */
  changedSchema = false;
  private readonly savepoints: OpenSavepoint[] = [];
  private closed = false;

  constructor(private readonly tenant: TenantStorage, readonly kind: InvocationKind) {}

  // ---- module SQL (`db.query`) ----------------------------------------------

  /**
   * One module statement, with the Rust host's semantics
   * (host_calls.rs `SqlCalls::query`). Resolves to rows decoded by declared
   * type; rejects with a StorageError carrying Rust's text.
   */
  async query(statement: string, parameters: unknown = []): Promise<JsonObject[]> {
    this.requireNotClosed();
    let admitted: AdmittedStatement;
    try {
      admitted = admitStatement(statement);
      if (this.kind === "query") requireReadHead(admitted.firstWord);
    } catch (error) {
      if (error instanceof AdmissionError) throw new StorageError(error.message, "refused");
      throw error;
    }
    // Admission refuses a bare ROLLBACK, so this is ROLLBACK TO.
    const recovers = admitted.firstWord === "rollback";
    if (!recovers) this.requireOpen();
    const values = this.parameters(parameters);
    let savepoint;
    try {
      savepoint = savepointCommand(admitted.tokens);
    } catch (error) {
      if (error instanceof SavepointSyntaxError) throw this.failed(databaseMessage(1, error.message), 1);
      throw error;
    }
    if (savepoint) {
      this.stats.savepoints++;
      if (savepoint.command === "savepoint") await this.openSavepoint(savepoint.name);
      else if (savepoint.command === "release") await this.releaseSavepoint(savepoint.name);
      else {
        await this.rollbackToSavepoint(savepoint.name);
        if (this.changedSchema) this.tenant.invalidateSchema();
        this.aborted = undefined;
      }
      return [];
    }
    // RETURNING rows of a simple write decode by the target table's catalog.
    const targetName = writeTarget(admitted.tokens);
    let target: TableCatalog | undefined;
    if (targetName !== undefined && isPlainName(targetName)) target = this.catalogOf(targetName);
    let projection: (JsonKind | undefined)[] | undefined;
    if (!target) {
      projection = projectionKinds(admitted.sql, (table) => {
        const catalog = this.catalogOf(table);
        return catalog.order.filter((name) => catalog.columns.has(name)).map((name) => [name, catalog.columns.get(name)] as const);
      });
    }
    if (this.kind === "query" && readStatementWrites(admitted.tokens)) throw this.failed(READ_ONLY_WRITE, 8);
    const result = this.runModule(admitted, values);
    if (this.kind === "query" && result.rowsWritten > 0) throw this.failed(READ_ONLY_WRITE, 8);
    if (SCHEMA_HEADS.has(admitted.firstWord)) {
      this.changedSchema = true;
      this.tenant.invalidateSchema();
    }
    // A statement that changed no row cannot have broken a foreign key.
    else if (result.rowsWritten > 0) this.noteStatement(admitted.tokens);
    if (recovers) this.aborted = undefined;
    const kinds = result.columnNames.map((name, index) => {
      const fromProjection = projection && projection.length === result.columnNames.length ? projection[index] : undefined;
      return fromProjection ?? target?.columns.get(name);
    });
    return result.rows.map((row) => {
      const object: JsonObject = {};
      result.columnNames.forEach((name, index) => {
        const kind = kinds[index];
        setOwn(object, name, kind === undefined ? decodeExpressionCell(row[index]) : decodeCell(kind, row[index]));
      });
      return object;
    });
  }

  /** Rolls the invocation back when it ends, even if it succeeds (Rust's `finish(false)`). */
  discard(): void {
    this.discarded = true;
  }

  /**
   * `EXPLAIN` of a module statement as the layer would run it (parameters
   * bound, REGEXP rewritten). Raw opcode rows; nothing else is executed.
   */
  explain(statement: string, parameters: unknown = []): RawResult {
    this.requireNotClosed();
    let admitted: AdmittedStatement;
    try {
      admitted = admitStatement(statement);
    } catch (error) {
      if (error instanceof AdmissionError) throw new StorageError(error.message, "refused");
      throw error;
    }
    const results = this.plan(admitted, this.parameters(parameters)).map((statement) => this.execute(`EXPLAIN ${statement.sql}`, statement.binds));
    return {
      columnNames: results[0]!.columnNames,
      rows: results.flatMap((result) => result.rows),
      rowsRead: results.reduce((total, result) => total + result.rowsRead, 0),
      rowsWritten: 0,
    };
  }

  /** sql_parameters.rs `Parameter::scalar` over the module's parameter array. */
  private parameters(parameters: unknown): Parameter[] {
    if (parameters === null || parameters === undefined) return [];
    if (!Array.isArray(parameters)) throw new StorageError("query parameters must be an array", "refused");
    return parameters.map((value, index) => {
      try {
        return scalarParameter(value);
      } catch (error) {
        if (error instanceof ParameterError) throw new StorageError(`parameter $${index + 1}: ${error.message}`, "refused");
        throw error;
      }
    });
  }

  /**
   * Binds and rewrites one admitted module statement into the SQL to run: one
   * statement, or for a wide FROM-less SELECT over 100 parameters, one per
   * group of select items.
   */
  private plan(admitted: AdmittedStatement, values: readonly Parameter[]): PlannedStatement[] {
    let slots: SlotAssignment;
    let rewrites: Rewrite[];
    try {
      slots = assignSlots(admitted.tokens, values);
      rewrites = regexpRewrites(admitted.tokens, new StatementWriter(admitted.sql, admitted.tokens, slots));
    } catch (error) {
      if (error instanceof BindError || error instanceof RegexpRewriteError || error instanceof RegexpUnsupportedError) {
        throw this.failed(error.message);
      }
      throw error;
    }
    const writer = new StatementWriter(admitted.sql, admitted.tokens, slots);
    const sql = writer.emit(0, admitted.tokens.length - 1, rewrites);
    if (writer.binds.length <= MAX_BOUND_PARAMETERS) return [{ sql, binds: writer.binds, renames: writer.renames }];
    const groups = wideSelectGroups(admitted.tokens, slots, rewrites);
    if (!groups) {
      throw this.failed(`a database statement binds ${writer.binds.length} parameters; Durable Object SQLite allows at most ${MAX_BOUND_PARAMETERS} per statement`);
    }
    // existsDataRows: independent expressions with no FROM, run in groups and joined into one row.
    return groups.map((group) => {
      const groupWriter = new StatementWriter(admitted.sql, admitted.tokens, slots);
      const items = group.map(([from, to]) => groupWriter.emit(from, to, rewrites.filter((rewrite) => rewrite.from >= from && rewrite.to <= to)));
      return { sql: `SELECT ${items.join(", ")}`, binds: groupWriter.binds, renames: groupWriter.renames };
    });
  }

  /** Runs one admitted module statement. */
  private runModule(admitted: AdmittedStatement, values: readonly Parameter[]): RawResult {
    const planned = this.plan(admitted, values);
    const results = planned.map((statement) => {
      const result = this.execute(statement.sql, statement.binds);
      return { ...result, columnNames: result.columnNames.map((name) => restoreColumnName(name, statement.renames, admitted.sql)) };
    });
    if (results.length === 1) return results[0]!;
    return {
      columnNames: results.flatMap((result) => result.columnNames),
      rows: [results.flatMap((result) => [...(result.rows[0] ?? [])])],
      rowsRead: results.reduce((total, result) => total + result.rowsRead, 0),
      rowsWritten: results.reduce((total, result) => total + result.rowsWritten, 0),
    };
  }

  /** Runs SQL on the connection, counting rows; a failure marks the transaction aborted. */
  private execute(sql: string, binds: readonly SqlBind[], counters?: RowCounters): RawResult {
    let cursor: SqlStorageCursor<Record<string, SqlStorageValue>>;
    let rows: SqlStorageValue[][];
    try {
      cursor = this.tenant.storage.sql.exec(sql, ...binds);
      rows = Array.from(cursor.raw());
    } catch (error) {
      const failure = sqliteFailure(error);
      throw this.failed(failure.message, failure.code, failure.sqliteMessage);
    }
    const result = { columnNames: cursor.columnNames, rows, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
    for (const target of counters ? [this.stats, counters] : [this.stats]) {
      target.statements++;
      target.rowsRead += result.rowsRead;
      target.rowsWritten += result.rowsWritten;
    }
    return result;
  }

  /** Records a statement failure (Rust's `sql_failure`) and returns the error to throw. */
  private failed(message: string, code?: number, detail?: string): StorageError {
    this.aborted ??= message;
    return new StorageError(message, "sql", code, detail);
  }

  private requireNotClosed(): void {
    if (this.closed) throw new StorageError(CLOSED, "closed");
  }

  private requireOpen(): void {
    if (this.aborted !== undefined) throw new StorageError(ABORTED, "aborted");
  }

  // ---- kernel SQL ---------------------------------------------------------------

  /**
   * A statement the kernel builds itself (db.insert/update/delete, the
   * outbox, the scheduler). No admission, but the same `$N`/`?N` binding,
   * integer typing, row counting, abort-on-failure and write tracking as
   * module SQL. Parameters are JSON values (sql_parameters.rs `scalar`), or
   * prepared `Parameter`s for column-typed binds.
   */
  run(sql: string, parameters: readonly unknown[] = []): RawResult {
    return this.runTyped(sql, parameters.map((value) => scalarParameter(value)));
  }

  /** `run` with parameters already typed, e.g. by `columnParameter` for a known column. */
  runTyped(sql: string, values: readonly Parameter[]): RawResult {
    this.requireNotClosed();
    const tokens = tokenize(sql);
    let slots: SlotAssignment;
    try {
      slots = assignSlots(tokens, values);
    } catch (error) {
      if (error instanceof BindError) throw this.failed(error.message);
      throw error;
    }
    const writer = new StatementWriter(sql, tokens, slots);
    const text = tokens.length ? writer.emit(0, tokens.length - 1) : sql;
    if (writer.binds.length > MAX_BOUND_PARAMETERS) {
      throw new Error(`kernel statement binds ${writer.binds.length} parameters; chunk it under ${MAX_BOUND_PARAMETERS} (see chunkParameters)`);
    }
    const result = this.execute(text, writer.binds);
    if (result.rowsWritten > 0) this.noteStatement(tokens);
    return { ...result, columnNames: result.columnNames.map((name) => restoreColumnName(name, writer.renames, sql)) };
  }

  /**
   * Runs a kernel statement once per chunk of `values`, so no statement binds
   * more than 100 parameters (runtime `in`/`oneOf` lists). `build` receives
   * the placeholders for one chunk, numbered after `fixed`.
   */
  runInChunks(build: (placeholders: string) => string, values: readonly unknown[], fixed: readonly unknown[] = []): RawResult {
    const size = MAX_BOUND_PARAMETERS - fixed.length;
    if (size < 1) throw new Error("fixed parameters leave no room for a chunk");
    let columnNames: readonly string[] = [];
    const rows: (readonly SqlStorageValue[])[] = [];
    let rowsRead = 0, rowsWritten = 0;
    for (const chunk of chunkParameters(values, size)) {
      const placeholders = chunk.map((_, index) => `?${fixed.length + index + 1}`).join(", ");
      const result = this.run(build(placeholders), [...fixed, ...chunk]);
      columnNames = result.columnNames;
      rows.push(...result.rows);
      rowsRead += result.rowsRead;
      rowsWritten += result.rowsWritten;
    }
    return { columnNames, rows, rowsRead, rowsWritten };
  }

  /**
   * The catalog of a table (host_calls.rs `catalog_table`), from the tenant
   * object's cache. A malformed name or record schema is an error the module
   * sees without aborting; a failed catalog statement aborts like any SQL.
   */
  catalogOf(table: string): TableCatalog {
    const cached = this.tenant.catalog.cached(table);
    if (cached) return cached;
    this.requireOpen();
    try {
      return this.tenant.catalog.lookup(table, { rows: (sql, ...binds) => this.objects(this.execute(sql, binds, this.stats.catalog)) });
    } catch (error) {
      if (error instanceof CatalogError) throw new StorageError(error.message, "refused");
      throw error;
    }
  }

  private objects(result: RawResult): Record<string, unknown>[] {
    return result.rows.map((row) => Object.fromEntries(result.columnNames.map((name, index) => [name, row[index]])));
  }

  private noteStatement(tokens: readonly Token[]): void {
    const mode = this.tenant.writeTracking;
    if (mode === "statements") this.written.noteStatement(tokens, this.foreignKeyModel());
    else if (mode === "mixed") this.written.noteStatement(tokens, this.foreignKeyModel(), this.tenant.rowReported ?? (() => false));
  }

  /** Change capture (K03) or a writer reports the exact rows it wrote, for a row-level FK check. */
  noteRows(table: string, operation: WriteOperation, rowids: readonly number[]): void {
    this.written.noteRows(table, operation, rowids);
  }

  // ---- savepoints ---------------------------------------------------------------

  private async openSavepoint(name: string): Promise<void> {
    let settle!: OpenSavepoint["settle"];
    const gate = new Promise<void>((resolve, reject) => { settle = { resolve, reject }; });
    let started!: () => void;
    const opened = new Promise<void>((resolve) => { started = resolve; });
    const done = this.tenant.storage.transaction(async () => {
      started();
      await gate;
    });
    done.catch(() => {});
    this.savepoints.push({ name: name.toLowerCase(), settle, done });
    await opened;
  }

  private find(name: string): number {
    const lower = name.toLowerCase();
    for (let index = this.savepoints.length - 1; index >= 0; index--) if (this.savepoints[index]!.name === lower) return index;
    throw this.failed(databaseMessage(1, `no such savepoint: ${name}`), 1);
  }

  private async closeTop(commit: boolean): Promise<void> {
    const top = this.savepoints.pop()!;
    if (commit) top.settle.resolve();
    else top.settle.reject(new Error("ROLLBACK TO"));
    await top.done.catch(() => {});
  }

  /** RELEASE commits the named savepoint and every savepoint opened after it. */
  private async releaseSavepoint(name: string): Promise<void> {
    const index = this.find(name);
    while (this.savepoints.length > index) await this.closeTop(true);
  }

  /** ROLLBACK TO undoes everything since the savepoint and leaves it open, as SQLite does. */
  private async rollbackToSavepoint(name: string): Promise<void> {
    const index = this.find(name);
    const original = this.savepoints[index]!.name;
    while (this.savepoints.length > index) await this.closeTop(false);
    await this.openSavepoint(original);
  }

  /** At the end of the invocation: release every open savepoint on commit, roll them back otherwise. */
  async settleSavepoints(commit: boolean): Promise<void> {
    while (this.savepoints.length) await this.closeTop(commit);
  }

  // ---- commit ----------------------------------------------------------------------

  private foreignKeyModel(): ForeignKeyModel {
    return this.tenant.foreignKeyModel((sql, ...binds) => this.objects(this.execute(sql, binds, this.stats.schema)));
  }

  /** The deferred foreign key check; returns the first violation, if any. */
  checkForeignKeys(): ForeignKeyCheckResult["violation"] {
    if (!this.written.tables.size && !this.written.everything) return undefined;
    const model = this.foreignKeyModel();
    const check = checkDeferredForeignKeys(model, this.written, {
      rows: (sql, ...binds) => {
        const result = this.execute(sql, binds);
        return { rows: this.objects(result), rowsRead: result.rowsRead };
      },
    }, this.tenant.now);
    const { violation, ...stats } = check;
    this.stats.foreignKeyCheck = stats;
    return violation;
  }

  close(): void {
    this.closed = true;
  }
}

/** A table name write_target found that `quote_identifier` accepts. */
function isPlainName(name: string): boolean {
  try {
    quoteIdentifier(name);
    return true;
  } catch {
    return false;
  }
}

/** JSON.parse semantics for a key: `__proto__` becomes an own property. */
function setOwn(object: JsonObject, key: string, value: JsonValue): void {
  if (key === "__proto__") Object.defineProperty(object, key, { value, enumerable: true, writable: true, configurable: true });
  else object[key] = value;
}

/** Splits `values` into chunks of at most `size` (100 bound parameters per statement). */
export function chunkParameters<T>(values: readonly T[], size: number = MAX_BOUND_PARAMETERS): T[][] {
  const chunks: T[][] = [];
  for (let offset = 0; offset < values.length; offset += size) chunks.push(values.slice(offset, offset + size));
  return chunks;
}

/**
 * For `SELECT a, b, …` with no FROM or other clause, groups of select items
 * (as inclusive token ranges) whose own parameters fit 100 per statement.
 * Each item is evaluated on its own, so running them in groups inside the
 * same transaction gives the same row. Undefined for any other shape, or an
 * item that alone binds more than 100.
 */
function wideSelectGroups(tokens: readonly Token[], slots: SlotAssignment, rewrites: readonly Rewrite[]): [number, number][][] | undefined {
  if (!isWord(tokens[0], "select")) return undefined;
  const items: [number, number][] = [];
  let depth = 0;
  let start = 1;
  for (let index = 1; index <= tokens.length; index++) {
    const found = tokens[index];
    if (found === undefined || (depth === 0 && isSymbol(found, ","))) {
      if (index - 1 < start) return undefined;
      items.push([start, index - 1]);
      start = index + 1;
      continue;
    }
    if (isSymbol(found, "(")) depth++;
    else if (isSymbol(found, ")")) depth--;
    else if (depth === 0 && found.type === "word" && ["from", "where", "group", "having", "order", "limit", "window", "union", "intersect", "except", "distinct", "all"].includes(found.lower)) return undefined;
  }
  if (rewrites.some((rewrite) => !items.some(([from, to]) => rewrite.from >= from && rewrite.to <= to))) return undefined;
  const groups: [number, number][][] = [];
  let group: [number, number][] = [];
  let used = new Set<number>();
  for (const item of items) {
    const own = new Set<number>();
    for (let index = item[0]; index <= item[1]; index++) {
      const slot = slots.slotOfToken.get(index);
      if (slot !== undefined) own.add(slot);
    }
    if (own.size > MAX_BOUND_PARAMETERS) return undefined;
    const merged = new Set([...used, ...own]);
    if (merged.size > MAX_BOUND_PARAMETERS && group.length) {
      groups.push(group);
      group = [];
      used = new Set(own);
    } else used = merged;
    group.push(item);
  }
  if (group.length) groups.push(group);
  return groups;
}
