// 30-day change-log retention (change_feed.rs `prune_retained`, CONTRACT
// E.a.2 c9: "adapt: DO alarm, paged deletes").
//
// Rust, hourly, in one transaction:
//   R = max(revision) of changes older than 30 days;
//   delete changes older than 30 days with revision <= R;
//   delete `_gonvex_sync_transactions` rows <= R that have no changes left;
//   retained_revision = max(retained_revision, R).
//
// Here the same end state is reached in pages, one storage transaction each,
// yielding between pages (design rule). `retained_revision` is raised in the
// first transaction, before anything is deleted, so a reader never sees a
// partly deleted revision: a cursor below R is already expired
// (`change feed cursor expired`), and revisions above R are untouched. If the
// object stops between pages, the next run finishes the job: the remaining
// rows still match, and R only grows.

export const RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
/** Rust prunes every hour per tenant; the tenant object's alarm multiplexer schedules it (CONTRACT E.d). */
export const PRUNE_INTERVAL_MS = 60 * 60 * 1000;
export const PRUNE_PAGE_ROWS = 1000;

export interface PruneStorage {
  readonly sql: SqlStorage;
  transactionSync<T>(closure: () => T): T;
}

export interface PruneOptions {
  /** Rows per page (per storage transaction). */
  readonly pageRows?: number;
  /** Stop after this many pages; `done` is false and the caller re-arms. */
  readonly maxPages?: number;
  /** Retention in ms (tests). */
  readonly retentionMs?: number;
  /** Awaited between pages. */
  readonly yieldBetweenPages?: () => Promise<void>;
}

export interface PruneResult {
  /** R, or null when nothing was old enough. */
  readonly revision: number | null;
  readonly deletedChanges: number;
  readonly deletedTransactions: number;
  readonly pages: number;
  readonly rowsRead: number;
  readonly rowsWritten: number;
  /** False when `maxPages` stopped it early. */
  readonly done: boolean;
}

const defaultYield = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

export function pruneDue(lastRunMs: number | undefined, nowMs: number): boolean {
  return lastRunMs === undefined || nowMs - lastRunMs >= PRUNE_INTERVAL_MS;
}

export async function pruneChangeLog(storage: PruneStorage, options: PruneOptions = {}): Promise<PruneResult> {
  const page = Math.max(1, Math.floor(options.pageRows ?? PRUNE_PAGE_ROWS));
  const retention = Math.floor(options.retentionMs ?? RETENTION_MS);
  const pause = options.yieldBetweenPages ?? defaultYield;
  let rowsRead = 0, rowsWritten = 0, pages = 0, deletedChanges = 0, deletedTransactions = 0;
  const exec = (text: string) => {
    const cursor = storage.sql.exec(text);
    const rows = [...cursor.raw()];
    rowsRead += cursor.rowsRead;
    rowsWritten += cursor.rowsWritten;
    return rows;
  };
  const start = storage.transactionSync(() => {
    const [row] = exec(
      `SELECT CAST(unixepoch('subsec') * 1000 AS INTEGER) - ${retention} AS cutoff,
              (SELECT max(revision) FROM _gonvex_sync_changes
                WHERE created_at < CAST(unixepoch('subsec') * 1000 AS INTEGER) - ${retention} AND revision IS NOT NULL) AS revision`,
    );
    const cutoff = Number(row![0]);
    const revision = row![1] === null ? null : Number(row![1]);
    if (revision !== null) exec(`UPDATE _gonvex_sync_clock SET retained_revision=max(retained_revision,${revision}) WHERE singleton=1`);
    return { cutoff, revision };
  });
  pages += 1;
  const { cutoff, revision } = start;
  if (revision === null) return { revision, deletedChanges, deletedTransactions, pages, rowsRead, rowsWritten, done: true };
  const within = () => options.maxPages === undefined || pages < options.maxPages;
  const stopped = () => ({ revision, deletedChanges, deletedTransactions, pages, rowsRead, rowsWritten, done: false });
  for (;;) {
    if (!within()) return stopped();
    await pause();
    const n = storage.transactionSync(
      () =>
        exec(
          `DELETE FROM _gonvex_sync_changes WHERE event_id IN (
             SELECT event_id FROM _gonvex_sync_changes INDEXED BY gonvex_sync_changes_created_at
             WHERE created_at < ${cutoff} AND revision IS NOT NULL AND revision <= ${revision} LIMIT ${page})
           RETURNING event_id`,
        ).length,
    );
    pages += 1;
    deletedChanges += n;
    if (n < page) break;
  }
  for (;;) {
    if (!within()) return stopped();
    await pause();
    const n = storage.transactionSync(
      () =>
        exec(
          `DELETE FROM _gonvex_sync_transactions WHERE revision IN (
             SELECT t.revision FROM _gonvex_sync_transactions t
             WHERE t.revision <= ${revision}
               AND NOT EXISTS (SELECT 1 FROM _gonvex_sync_changes changes WHERE changes.revision = t.revision)
             LIMIT ${page})
           RETURNING revision`,
        ).length,
    );
    pages += 1;
    deletedTransactions += n;
    if (n < page) break;
  }
  return { revision, deletedChanges, deletedTransactions, pages, rowsRead, rowsWritten, done: true };
}
