// Reducer receipts: `_gonvex_reducer_idempotency` (CONTRACT E.receipts).
//
// Rust (rust/crates/store/src/lib.rs:1106-1199, 1600-1637):
//   ensure_reducer_idempotency: create the table and its created_at index on
//     the first keyed call, renaming the legacy `_gonvex_mutation_idempotency`;
//     both tables present is AmbiguousReducerReceipts.
//   claim_reducer: INSERT … ON CONFLICT DO NOTHING when the Reducer's
//     transaction opens; store_reducer_result: UPDATE … SET result before commit.
//   replay_reducer_result: the stored result, or IdempotencyPathMismatch.
//
// A committed receipt therefore always has a non-NULL result (claim and result
// commit together, a failed Reducer rolls both back). The kernel uses that:
// **a committed row whose result is NULL is a tombstone** (0009 rule 1). This
// keeps the table byte-identical to Rust's (no extra column, P03 round trips
// unchanged). See contract-change note 0030.
//
// Expiry (0009, IB3): when the app declares `offlineMaxAgeMs`, results older
// than `offlineMaxAgeMs + 24 h` are cleared (the row stays as a tombstone);
// tombstones go 365 days after that. A null window keeps receipts as Rust does.
// Pruning runs on the tenant alarm, in pages, never inside a Reducer.

import type { CaptureSql, RawResult } from "../capture/sql.ts";

export const RECEIPTS_TABLE = "_gonvex_reducer_idempotency";
export const LEGACY_RECEIPTS_TABLE = "_gonvex_mutation_idempotency";
export const RECEIPT_QUERY = "__gonvex.reducerReceipt";

/** Byte for byte Rust's script (lib.rs:1150-1165), so `sqlite_schema` matches a Rust tenant. */
export const RECEIPTS_DDL = [
  `CREATE TABLE IF NOT EXISTS _gonvex_reducer_idempotency (
                 subject TEXT NOT NULL DEFAULT '',
                 idempotency_key TEXT NOT NULL,
                 path TEXT NOT NULL,
                 result JSON,
                 created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
                 PRIMARY KEY (subject, idempotency_key)
               )`,
  `CREATE INDEX IF NOT EXISTS gonvex_reducer_idempotency_created_at
                 ON _gonvex_reducer_idempotency (created_at)`,
] as const;

const STATUS_SQL = `SELECT
            EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_reducer_idempotency'),
            EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_gonvex_mutation_idempotency'),
            EXISTS(SELECT 1 FROM sqlite_schema WHERE type='index'
                AND name='gonvex_reducer_idempotency_created_at' AND tbl_name='_gonvex_reducer_idempotency')`;

/** 0009 rule 1: results expire this long after the offline window. */
export const RECEIPT_GRACE_MS = 24 * 60 * 60 * 1000;
/** 0009 rule 2: tombstones are kept this long after their result expired. */
export const TOMBSTONE_RETENTION_MS = 365 * 24 * 60 * 60 * 1000;
export const RECEIPT_PRUNE_PAGE_ROWS = 500;
/** Receipt pruning runs at most this often per tenant (one alarm slot, CONTRACT E.timers). */
export const RECEIPT_PRUNE_INTERVAL_MS = 60 * 60 * 1000;

export class AmbiguousReceiptsError extends Error {
  constructor() {
    super("ambiguous current and legacy receipt history requires recovery");
  }
}

export interface ReceiptTables {
  readonly current: boolean;
  readonly legacy: boolean;
  readonly indexed: boolean;
}

export function receiptTables(sql: CaptureSql): ReceiptTables {
  const row = sql.run(STATUS_SQL).rows[0]!;
  return { current: Number(row[0]) === 1, legacy: Number(row[1]) === 1, indexed: Number(row[2]) === 1 };
}

/**
 * `ensure_reducer_idempotency`. Cheap when the table exists (one catalog read);
 * the caller caches a positive answer per schema generation. Throws
 * `AmbiguousReceiptsError` when both the current and the legacy table exist.
 */
export function ensureReceipts(sql: CaptureSql): void {
  const status = receiptTables(sql);
  if (status.current && status.legacy) throw new AmbiguousReceiptsError();
  if (status.current && status.indexed) return;
  if (status.legacy && !status.current) sql.run("ALTER TABLE _gonvex_mutation_idempotency RENAME TO _gonvex_reducer_idempotency");
  for (const statement of RECEIPTS_DDL) sql.run(statement);
}

export type Receipt =
  | { readonly state: "none" }
  /** `result` is the stored JSON text, replayed verbatim. */
  | { readonly state: "stored"; readonly path: string; readonly result: string }
  | { readonly state: "expired"; readonly path: string };

/** The receipt of `(subject, key)` in `_gonvex_reducer_idempotency`. */
export function readReceipt(sql: CaptureSql, subject: string, key: string): Receipt {
  const row = sql.run("SELECT path, result FROM _gonvex_reducer_idempotency WHERE subject = ?1 AND idempotency_key = ?2", [subject, key]).rows[0];
  if (!row) return { state: "none" };
  const path = String(row[0]);
  return row[1] === null ? { state: "expired", path } : { state: "stored", path, result: String(row[1]) };
}

/**
 * Rust's claim and stored result in one statement: the row commits with the
 * Reducer or not at all. `result` is the result's JSON text.
 */
export function storeReceipt(sql: CaptureSql, subject: string, key: string, path: string, result: string): RawResult {
  return sql.run(
    "INSERT INTO _gonvex_reducer_idempotency (subject, idempotency_key, path, result) VALUES (?1, ?2, ?3, ?4)",
    [subject, key, path, result],
  );
}

export interface ReceiptPruneStorage {
  readonly sql: SqlStorage;
  transactionSync<T>(closure: () => T): T;
}

export interface ReceiptPruneOptions {
  /** The app's `offlineMaxAgeMs`; null (or no client contract) keeps receipts forever (0009 rule 4). */
  readonly offlineMaxAgeMs: number | null;
  /** Object clock (ms). */
  readonly now: number;
  readonly pageRows?: number;
  /** Stop after this many pages; `done` is false and the caller re-arms. */
  readonly maxPages?: number;
  /** Awaited between pages (design rule: page and yield). */
  readonly yieldBetweenPages?: () => Promise<void>;
  /**
   * Runs one page. The tenant object queues each page behind its Reducers, so
   * a page never runs inside a Reducer's open storage transaction.
   */
  readonly runPage?: <T>(page: () => T) => Promise<T>;
}

export interface ReceiptPruneResult {
  /** Results cleared (rows that became tombstones). */
  readonly expired: number;
  /** Tombstones deleted. */
  readonly deleted: number;
  readonly pages: number;
  readonly rowsRead: number;
  readonly rowsWritten: number;
  readonly done: boolean;
}

const defaultYield = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

/** The cutoffs of 0009 for a window: results created before `expireBefore` expire, tombstones before `deleteBefore` go. */
export function receiptCutoffs(offlineMaxAgeMs: number, now: number): { expireBefore: number; deleteBefore: number } {
  const expireBefore = now - (offlineMaxAgeMs + RECEIPT_GRACE_MS);
  return { expireBefore, deleteBefore: expireBefore - TOMBSTONE_RETENTION_MS };
}

interface PageResult { expired: number; deleted: number; rowsRead: number; rowsWritten: number; more: boolean }

/**
 * One page, one storage transaction: old tombstones are deleted first, then
 * expired results are cleared (by the created_at index). `more` is true when
 * the page was full. Integers are SQL literals: Durable Object SQL binds every
 * JS number as REAL.
 */
export function pruneReceiptsPage(storage: ReceiptPruneStorage, cutoffs: { expireBefore: number; deleteBefore: number }, pageRows: number): PageResult {
  return storage.transactionSync(() => {
    let rowsRead = 0, rowsWritten = 0;
    const exec = (text: string) => {
      const cursor = storage.sql.exec(text);
      const n = [...cursor.raw()].length;
      rowsRead += cursor.rowsRead;
      rowsWritten += cursor.rowsWritten;
      return n;
    };
    const deleted = exec(`DELETE FROM _gonvex_reducer_idempotency WHERE rowid IN (
        SELECT rowid FROM _gonvex_reducer_idempotency INDEXED BY gonvex_reducer_idempotency_created_at
        WHERE created_at < ${Math.floor(cutoffs.deleteBefore)} AND result IS NULL LIMIT ${pageRows}) RETURNING 1`);
    if (deleted === pageRows) return { expired: 0, deleted, rowsRead, rowsWritten, more: true };
    const expired = exec(`UPDATE _gonvex_reducer_idempotency SET result = NULL WHERE rowid IN (
        SELECT rowid FROM _gonvex_reducer_idempotency INDEXED BY gonvex_reducer_idempotency_created_at
        WHERE created_at < ${Math.floor(cutoffs.expireBefore)} AND result IS NOT NULL LIMIT ${pageRows - deleted}) RETURNING 1`);
    return { expired, deleted, rowsRead, rowsWritten, more: deleted + expired === pageRows };
  });
}

/**
 * 0009 rules 1, 2 and 5: clears expired results and deletes old tombstones, in
 * pages (one storage transaction each), yielding between pages. Idempotent: a
 * stopped run is finished by the next.
 */
export async function pruneReceipts(storage: ReceiptPruneStorage, options: ReceiptPruneOptions): Promise<ReceiptPruneResult> {
  let expired = 0, deleted = 0, pages = 0, rowsRead = 0, rowsWritten = 0;
  const result = (done: boolean): ReceiptPruneResult => ({ expired, deleted, pages, rowsRead, rowsWritten, done });
  const window = options.offlineMaxAgeMs;
  if (window === null || !Number.isSafeInteger(window) || window <= 0) return result(true);
  const exists = storage.sql.exec("SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = 'gonvex_reducer_idempotency_created_at'").toArray().length > 0;
  if (!exists) return result(true);
  const pageRows = Math.max(1, Math.floor(options.pageRows ?? RECEIPT_PRUNE_PAGE_ROWS));
  const pause = options.yieldBetweenPages ?? defaultYield;
  const runPage = options.runPage ?? (<T>(page: () => T) => Promise.resolve(page()));
  const cutoffs = receiptCutoffs(window, Math.floor(options.now));
  for (;;) {
    if (options.maxPages !== undefined && pages >= options.maxPages) return result(false);
    if (pages > 0) await pause();
    const page = await runPage(() => pruneReceiptsPage(storage, cutoffs, pageRows));
    pages += 1;
    expired += page.expired;
    deleted += page.deleted;
    rowsRead += page.rowsRead;
    rowsWritten += page.rowsWritten;
    if (!page.more) return result(true);
  }
}
