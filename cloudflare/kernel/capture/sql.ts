// The SQL surface capture needs, and row counting for budgets.
//
// Inside an invocation, capture runs its statements through K01's
// `StorageSession.run` (kernel SQL: no admission, the same binding, row
// counters and abort rule as module SQL). `StorageSession` satisfies
// `CaptureSql` structurally, so this package does not import K01. Outside an
// invocation (installation, pruning) `directSql` wraps `ctx.storage.sql`.
//
// Capture binds only TEXT and NULL. Revisions are integers the kernel read
// from `_gonvex_sync_clock` and are written into the SQL as literals, because
// Durable Object SQL binds every JS number as REAL.

export interface RawResult {
  readonly columnNames: readonly string[];
  readonly rows: readonly (readonly SqlStorageValue[])[];
  readonly rowsRead: number;
  readonly rowsWritten: number;
}

/** One kernel statement with `?N` parameters (K01 `StorageSession.run`). */
export interface CaptureSql {
  run(sql: string, parameters?: readonly unknown[]): RawResult;
}

/** Write kinds as K01 names them (`WriteOperation`). */
export type WriteOperation = "insert" | "update" | "delete";

/** K01's row-level write report for the deferred foreign key check (0022 §2c). */
export interface RowReporter {
  noteRows(table: string, operation: WriteOperation, rowids: readonly number[]): void;
}

export interface RowCounters {
  statements: number;
  rowsRead: number;
  rowsWritten: number;
}

export const counters = (): RowCounters => ({ statements: 0, rowsRead: 0, rowsWritten: 0 });

/** `CaptureSql` over `ctx.storage.sql`, counting into `into`. */
export function directSql(sql: SqlStorage, into: RowCounters = counters()): CaptureSql & { readonly counters: RowCounters } {
  return {
    counters: into,
    run(text, parameters = []) {
      const cursor = sql.exec(text, ...(parameters as SqlStorageValue[]));
      const rows = [...cursor.raw()];
      into.statements += 1;
      into.rowsRead += cursor.rowsRead;
      into.rowsWritten += cursor.rowsWritten;
      return { columnNames: cursor.columnNames, rows, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
    },
  };
}

/** Wraps a `CaptureSql` so every statement also adds to `into`. */
export function counting(inner: CaptureSql, into: RowCounters): CaptureSql {
  return {
    run(text, parameters) {
      const result = inner.run(text, parameters);
      into.statements += 1;
      into.rowsRead += result.rowsRead;
      into.rowsWritten += result.rowsWritten;
      return result;
    },
  };
}

export function objects(result: RawResult): Record<string, SqlStorageValue>[] {
  return result.rows.map((row) => Object.fromEntries(result.columnNames.map((name, index) => [name, row[index]!])));
}

export function scalar(result: RawResult): SqlStorageValue | undefined {
  return result.rows[0]?.[0];
}

/** rows.rs `quote_identifier`. */
export function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/** A SQL string literal. */
export function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/** A revision or other kernel-held integer, written into SQL as a literal. */
export function integerLiteral(value: number): string {
  if (!Number.isSafeInteger(value)) throw new Error(`not a safe integer: ${value}`);
  return String(value);
}
