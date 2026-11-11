/**
 * Thin layer over Durable Object SQLite for the control object.
 *
 * The reference SQL (rust/crates/runtime/src/control.rs) uses sqlx `$N`
 * placeholders. SQLite reads `$1` as a *named* parameter whose index is its
 * order of first appearance, so `$3 ... $1` would bind out of order. Every
 * `$N` is rewritten to the numbered form `?N`, which binds by index exactly as
 * sqlx does.
 *
 * Rows read and written are counted per call so the PR can report them and
 * tests can assert them (`SqlStorageCursor.rowsRead` / `rowsWritten`).
 */

export type SqlValue = string | number | null | ArrayBuffer | Uint8Array;
export type Row = Record<string, SqlStorageValue>;

/** The current Unix time in milliseconds, as the reference stores it. */
export const NOW_MS = "CAST(unixepoch('subsec')*1000 AS INTEGER)";

const cache = new Map<string, string>();
function rewrite(query: string): string {
  let out = cache.get(query);
  if (out === undefined) {
    out = query.replace(/\$(\d+)/g, "?$1");
    if (cache.size > 512) cache.clear();
    cache.set(query, out);
  }
  return out;
}

function bind(value: unknown): SqlValue {
  if (value === undefined || value === null) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "string" || typeof value === "number") return value;
  if (value instanceof Uint8Array || value instanceof ArrayBuffer) return value;
  // JSON columns hold JSON text, as sqlx's Json<Value> binds them.
  return JSON.stringify(value);
}

export class Sql {
  rowsRead = 0;
  rowsWritten = 0;
  constructor(private readonly storage: DurableObjectStorage) {}

  private cursor<T extends Row>(query: string, args: unknown[]): SqlStorageCursor<T> {
    return this.storage.sql.exec<T>(rewrite(query), ...args.map(bind));
  }

  private count(cursor: SqlStorageCursor<Row>): void {
    this.rowsRead += cursor.rowsRead;
    this.rowsWritten += cursor.rowsWritten;
  }

  /** Every row. */
  all<T extends Row = Row>(query: string, ...args: unknown[]): T[] {
    const cursor = this.cursor<T>(query, args);
    const rows = cursor.toArray();
    this.count(cursor);
    return rows;
  }

  /** The first row, or undefined. */
  get<T extends Row = Row>(query: string, ...args: unknown[]): T | undefined {
    return this.all<T>(query, ...args)[0];
  }

  /** The first column of the first row, or undefined. */
  value<T extends SqlStorageValue = SqlStorageValue>(query: string, ...args: unknown[]): T | undefined {
    const row = this.get(query, ...args);
    return row === undefined ? undefined : (Object.values(row)[0] as T);
  }

  /** Runs a statement and returns nothing. */
  run(query: string, ...args: unknown[]): void {
    const cursor = this.cursor(query, args);
    for (const _ of cursor) { /* drain */ }
    this.count(cursor);
  }

  /**
   * Runs an INSERT/UPDATE/DELETE and returns how many rows it changed, like
   * sqlx `rows_affected()`. `rowsWritten` also counts index writes, so the
   * statement is given `RETURNING 1` and its rows are counted instead.
   */
  affected(query: string, ...args: unknown[]): number {
    return this.all(`${query} RETURNING 1 AS affected`, ...args).length;
  }

  /** Runs a multi-statement script (no bindings). */
  script(text: string): void {
    const cursor = this.storage.sql.exec(text);
    for (const _ of cursor) { /* drain */ }
    this.count(cursor);
  }

  /**
   * One atomic transaction. `fn` must be synchronous: a Control Plane write in
   * the reference holds the single SQLite writer for its whole body, and a
   * synchronous storage transaction is the same guarantee here. A throw rolls
   * everything back.
   */
  tx<T>(fn: () => T): T {
    return this.storage.transactionSync(fn);
  }

  /** Counter snapshot, for per-call accounting. */
  mark(): { read: number; written: number } {
    return { read: this.rowsRead, written: this.rowsWritten };
  }
}

/** SQLite booleans are 0/1. */
export const bool = (value: SqlStorageValue | undefined): boolean => value === 1 || (value as unknown) === true;
export const str = (value: SqlStorageValue | undefined): string => (value === null || value === undefined ? "" : String(value));
export const num = (value: SqlStorageValue | undefined): number => Number(value ?? 0);
export const optNum = (value: SqlStorageValue | undefined): number | null => (value === null || value === undefined ? null : Number(value));
export function json(value: SqlStorageValue | undefined): unknown {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") return value;
  try { return JSON.parse(value); } catch { return value; }
}
export function blob(value: SqlStorageValue | undefined): Uint8Array | null {
  if (value === null || value === undefined) return null;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if ((value as unknown) instanceof Uint8Array) return value as unknown as Uint8Array;
  return null;
}
