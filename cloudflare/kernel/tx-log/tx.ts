// Triggerless single-writer transaction (`Tx`) over Durable Object SqlStorage.
// Records compact `(tbl, rid, op, scope_hint)` entries in memory during the write transaction
// and flushes them in batched <=100-bind INSERT statements at commit (zero triggers, zero JSON).

import { TX_OP_DELETE, TX_OP_UPSERT, readTxSeq, type TxOp } from "./schema";

/** One compact recorded mutation. */
export interface TxChange {
  readonly seq: number;
  readonly tbl: number;
  readonly rid: number;
  readonly op: TxOp;
  readonly scopeHint: number;
}

export interface TxCommitResult<T> {
  readonly value: T;
  readonly seq: number;
  readonly changes: readonly TxChange[];
  readonly rowsWritten: number;
}

const MAX_BINDS = 100;
const BINDS_PER_CHANGE = 5;
const CHANGES_PER_INSERT = Math.floor(MAX_BINDS / BINDS_PER_CHANGE); // 20

/** Single-writer handle passed to transaction callbacks. */
export class TxWriter {
  private readonly tbls: number[] = [];
  private readonly rids: number[] = [];
  private readonly ops: TxOp[] = [];
  private readonly hints: number[] = [];
  private readonly indexByKey = new Map<number, number>();
  private written = 0;

  constructor(private readonly sql: SqlStorage) {}

  /** Executes a write statement and tracks its `rowsWritten`. */
  exec(query: string, ...bindings: SqlStorageValue[]): SqlStorageCursor<Record<string, SqlStorageValue>> {
    const cur = this.sql.exec(query, ...bindings);
    this.written += cur.rowsWritten;
    return cur;
  }

  /**
   * Records a row change (last-op-wins per `(tbl, rid)` within the transaction).
   * Key packs `tbl * 4294967296 + (rid >>> 0)` into a safe JS integer (zero string allocation).
   */
  record(tbl: number, rid: number, op: TxOp, scopeHint = 0): void {
    const key = tbl * 4294967296 + (rid >>> 0);
    const existing = this.indexByKey.get(key);
    if (existing !== undefined) {
      this.ops[existing] = op;
      this.hints[existing] = scopeHint;
      return;
    }
    const idx = this.tbls.length;
    this.indexByKey.set(key, idx);
    this.tbls.push(tbl);
    this.rids.push(rid);
    this.ops.push(op);
    this.hints.push(scopeHint);
  }

  recordUpsert(tbl: number, rid: number, scopeHint = 0): void {
    this.record(tbl, rid, TX_OP_UPSERT, scopeHint);
  }

  recordDelete(tbl: number, rid: number, scopeHint = 0): void {
    this.record(tbl, rid, TX_OP_DELETE, scopeHint);
  }

  get changeCount(): number {
    return this.tbls.length;
  }

  get rowsWritten(): number {
    return this.written;
  }

  /** Flushes all buffered changes into `_gonvex_tx_changes` at `seq` in <=100-bind batches. */
  flush(seq: number): TxChange[] {
    const n = this.tbls.length;
    const out = new Array<TxChange>(n);
    if (n === 0) return out;

    const seqCur = this.sql.exec("UPDATE _gonvex_tx_seq SET seq = ? WHERE singleton = 1", seq);
    this.written += seqCur.rowsWritten;

    let offset = 0;
    while (offset < n) {
      const batchLen = n - offset < CHANGES_PER_INSERT ? n - offset : CHANGES_PER_INSERT;
      let placeholders = "";
      const binds = new Array<SqlStorageValue>(batchLen * BINDS_PER_CHANGE);
      for (let i = 0; i < batchLen; i++) {
        if (i > 0) placeholders += ",";
        placeholders += "(?,?,?,?,?)";
        const idx = offset + i;
        const tbl = this.tbls[idx]!;
        const rid = this.rids[idx]!;
        const op = this.ops[idx]!;
        const scopeHint = this.hints[idx]!;
        const b = i * BINDS_PER_CHANGE;
        binds[b] = seq;
        binds[b + 1] = tbl;
        binds[b + 2] = rid;
        binds[b + 3] = op;
        binds[b + 4] = scopeHint;
        out[idx] = { seq, tbl, rid, op, scopeHint };
      }
      const cur = this.sql.exec(
        `INSERT OR REPLACE INTO _gonvex_tx_changes (seq, tbl, rid, op, scope_hint) VALUES ${placeholders}`,
        ...binds,
      );
      this.written += cur.rowsWritten;
      offset += batchLen;
    }
    return out;
  }
}

/**
 * Runs a single-writer synchronous DO transaction, allocating a monotonic `seq` only if
 * at least one change was recorded.
 */
export function runTx<T>(storage: Pick<DurableObjectStorage, "sql" | "transactionSync">, body: (tx: TxWriter) => T): TxCommitResult<T> {
  return storage.transactionSync(() => {
    const tx = new TxWriter(storage.sql);
    const value = body(tx);
    if (tx.changeCount === 0) {
      return { value, seq: readTxSeq(storage.sql), changes: [], rowsWritten: tx.rowsWritten };
    }
    const nextSeq = readTxSeq(storage.sql) + 1;
    const changes = tx.flush(nextSeq);
    return { value, seq: nextSeq, changes, rowsWritten: tx.rowsWritten };
  });
}

/** Reads compact change entries strictly after `afterSeq` up to `throughSeq` via `.raw()`. */
export function readChangesSince(sql: SqlStorage, afterSeq: number, throughSeq: number): TxChange[] {
  const cur = sql.exec(
    "SELECT seq, tbl, rid, op, scope_hint FROM _gonvex_tx_changes WHERE seq > ? AND seq <= ? ORDER BY seq, tbl, rid",
    afterSeq,
    throughSeq,
  );
  const out: TxChange[] = [];
  const iter = cur.raw<[number, number, number, number, number]>();
  for (;;) {
    const next = iter.next();
    if (next.done) break;
    const row = next.value;
    out.push({
      seq: row[0],
      tbl: row[1],
      rid: row[2],
      op: row[3] as TxOp,
      scopeHint: row[4],
    });
  }
  return out;
}

/** Prunes committed change-log rows with `seq <= throughSeq`. */
export function pruneTxLog(sql: SqlStorage, throughSeq: number): number {
  return sql.exec("DELETE FROM _gonvex_tx_changes WHERE seq <= ?", throughSeq).rowsWritten;
}
