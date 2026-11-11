// The storage-layer interface K04 needs from K01 (contract-change note 0020),
// and `DurableObjectInvocationStorage`, a stand-in built from the S2 spike
// (cloudflare/spikes/s2/src/engines.js) that K01's implementation replaces.
//
// K04 owns what happens *inside* an invocation (host calls, catalogs,
// admission, typing, the aborted-transaction rule). K01 owns the transaction
// mechanics around it: one storage transaction per invocation (0016 G0-3),
// module savepoints as nested storage transactions (0006), `$N` -> `?N`
// (0011.1), read-only Query enforcement by `rowsWritten` (0011.2), parameter
// limits (0011.3) and the deferred-FK check before commit (0016 G0-5).
import { rustSqlMessage } from "./errors.js";
import { numberedParameters } from "./sql.js";

export type SqlValue = null | number | string | ArrayBuffer | bigint;

export interface ExecResult {
  columnNames: string[];
  rows: SqlValue[][];
  rowsRead: number;
  rowsWritten: number;
}

/** A failed statement. `message` is Rust's sqlx text; `code` the extended SQLite code when known. */
export class SqlError extends Error {
  readonly code: number | undefined;
  constructor(message: string, code: number | undefined) {
    super(message);
    this.name = "SqlError";
    this.code = code;
  }
}

/** The SQL surface of one open invocation transaction. */
export interface InvocationTransaction {
  /**
   * Runs one statement. `$N` and `?N` placeholders bind by number. Throws
   * `SqlError` with Rust's text. In a Query transaction a statement that
   * writes fails with SQLite's read-only error and rolls back (0011.2).
   */
  exec(sql: string, binds: readonly SqlValue[]): ExecResult;
  /** Module `SAVEPOINT name` (0006): nested storage transaction, LIFO. */
  savepoint(name: string): Promise<void>;
  /** Module `RELEASE [SAVEPOINT] name`: commits it and every later savepoint. */
  release(name: string): Promise<void>;
  /** Module `ROLLBACK TO name`: rolls back to it and keeps it open, as SQLite does. */
  rollbackTo(name: string): Promise<void>;
}

export type InvocationMode = "query" | "reducer";

export interface InvocationOutcome<T> {
  result: T;
  committed: boolean;
  /**
   * Set when the commit was refused before it reached the platform: a
   * deferred foreign key would fail (G0-5). Rust's text, e.g.
   * "error returned from database: (code: 787) FOREIGN KEY constraint failed".
   */
  commitError?: SqlError;
  /** Rows the commit check itself read (not module work). */
  commitCheckRowsRead: number;
}

export interface TenantStorage {
  /**
   * One invocation in one atomic storage transaction. `body` runs with the
   * transaction open; `shouldCommit(result)` decides. A Query never commits.
   * Savepoints still open when `body` settles are released on commit and
   * rolled back otherwise.
   */
  invocation<T>(mode: InvocationMode, body: (tx: InvocationTransaction) => Promise<T>, shouldCommit: (result: T) => boolean): Promise<InvocationOutcome<T>>;
}

interface Gate { name: string; resolve: () => void; reject: (error: Error) => void; done: Promise<void> }

/**
 * Stand-in for K01, from the S2 spike's DurableObjectEngine + invoke. Two
 * shortcuts K01 must replace: the deferred-FK check scans
 * `pragma_foreign_key_check` (55 ms per 1M child rows; K01 checks only the
 * rows written), and parameter-count errors are left to the platform.
 */
export class DurableObjectInvocationStorage implements TenantStorage {
  constructor(private readonly storage: DurableObjectStorage) {}

  async invocation<T>(mode: InvocationMode, body: (tx: InvocationTransaction) => Promise<T>, shouldCommit: (result: T) => boolean): Promise<InvocationOutcome<T>> {
    return this.storage.transaction(async (txn) => {
      const tx = new SpikeTransaction(this.storage, mode);
      let result: T;
      try {
        result = await body(tx);
      } catch (error) {
        await tx.settle(false);
        txn.rollback();
        throw error;
      }
      let commit = mode === "reducer" && shouldCommit(result);
      await tx.settle(commit);
      let commitError: SqlError | undefined;
      let commitCheckRowsRead = 0;
      if (commit) {
        const check = tx.deferredForeignKeyViolation();
        commitCheckRowsRead = check.rowsRead;
        if (check.violation) { commitError = check.violation; commit = false; }
      }
      if (!commit) txn.rollback();
      const outcome: InvocationOutcome<T> = { result, committed: commit, commitCheckRowsRead };
      if (commitError) outcome.commitError = commitError;
      return outcome;
    });
  }
}

class SpikeTransaction implements InvocationTransaction {
  private readonly stack: Gate[] = [];
  constructor(private readonly storage: DurableObjectStorage, private readonly mode: InvocationMode) {}

  exec(sql: string, binds: readonly SqlValue[]): ExecResult {
    const numbered = numberedParameters(sql);
    let cursor: SqlStorageCursor<Record<string, SqlStorageValue>>;
    let rows: SqlValue[][];
    try {
      cursor = this.storage.sql.exec(numbered.sql, ...binds.slice(0, numbered.count));
      rows = Array.from(cursor.raw()) as SqlValue[][];
    } catch (error) {
      const failure = rustSqlMessage(error);
      throw new SqlError(failure.message, failure.code);
    }
    if (this.mode === "query" && cursor.rowsWritten > 0) {
      // 0011.2: Rust runs Queries on a read-only connection.
      throw new SqlError("error returned from database: (code: 8) attempt to write a readonly database", 8);
    }
    return { columnNames: cursor.columnNames, rows, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
  }

  private async open(name: string): Promise<void> {
    let opened!: () => void;
    const started = new Promise<void>((resolve) => { opened = resolve; });
    let gate!: Pick<Gate, "resolve" | "reject">;
    const held = new Promise<void>((resolve, reject) => { gate = { resolve, reject }; });
    const done = this.storage.transaction(async () => { opened(); await held; });
    const settled = done.catch(() => undefined);
    this.stack.push({ name: name.toLowerCase(), ...gate, done: settled });
    await started;
  }

  private find(name: string): number {
    const lower = name.toLowerCase();
    for (let index = this.stack.length - 1; index >= 0; index--) if (this.stack[index]?.name === lower) return index;
    throw new SqlError(`error returned from database: (code: 1) no such savepoint: ${name}`, 1);
  }

  private async close(commit: boolean): Promise<void> {
    const top = this.stack.pop();
    if (!top) return;
    if (commit) top.resolve(); else top.reject(new Error("ROLLBACK TO"));
    await top.done;
  }

  savepoint(name: string): Promise<void> { return this.open(name); }

  async release(name: string): Promise<void> {
    const index = this.find(name);
    while (this.stack.length > index) await this.close(true);
  }

  async rollbackTo(name: string): Promise<void> {
    const index = this.find(name);
    const original = (this.stack[index] as Gate).name;
    while (this.stack.length > index) await this.close(false);
    await this.open(original);
  }

  async settle(commit: boolean): Promise<void> {
    while (this.stack.length) await this.close(commit);
  }

  /** S2's measure, not K01's design: a full `pragma_foreign_key_check`. */
  deferredForeignKeyViolation(): { violation: SqlError | undefined; rowsRead: number } {
    const cursor = this.storage.sql.exec('SELECT "table", parent FROM pragma_foreign_key_check LIMIT 1');
    const row = cursor.toArray()[0];
    return {
      violation: row ? new SqlError("error returned from database: (code: 787) FOREIGN KEY constraint failed", 787) : undefined,
      rowsRead: cursor.rowsRead,
    };
  }
}
