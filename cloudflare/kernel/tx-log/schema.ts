// Triggerless payload-free change-log schema for Serverless DOD Tenant DOs.
// Eliminates per-table INSERT/UPDATE/DELETE SQLite triggers and JSON old/new row duplication.
// Only dense integer coordinates `(seq, tbl, rid, op, scope_hint)` are written per mutation.

export const TX_OP_UPSERT = 1;
export const TX_OP_DELETE = 2;

export type TxOp = typeof TX_OP_UPSERT | typeof TX_OP_DELETE;

/** Installs the triggerless sequence and compact change-log tables (idempotent). */
export function ensureTxLogSchema(sql: SqlStorage): void {
  sql.exec(`
    CREATE TABLE IF NOT EXISTS _gonvex_tx_seq (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      seq INTEGER NOT NULL
    )
  `);
  sql.exec("INSERT OR IGNORE INTO _gonvex_tx_seq (singleton, seq) VALUES (1, 0)");
  sql.exec(`
    CREATE TABLE IF NOT EXISTS _gonvex_tx_changes (
      seq INTEGER NOT NULL,
      tbl INTEGER NOT NULL,
      rid INTEGER NOT NULL,
      op INTEGER NOT NULL,
      scope_hint INTEGER NOT NULL,
      PRIMARY KEY (seq, tbl, rid)
    ) WITHOUT ROWID
  `);
  sql.exec("CREATE INDEX IF NOT EXISTS _gonvex_tx_changes_tbl_seq ON _gonvex_tx_changes (tbl, seq)");
}

/** Reads the current committed sequence number (1 row read). */
export function readTxSeq(sql: SqlStorage): number {
  const iter = sql.exec("SELECT seq FROM _gonvex_tx_seq WHERE singleton = 1").raw<[number]>();
  const first = iter.next();
  return first.done ? 0 : first.value[0];
}
