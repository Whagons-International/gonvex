// SQL engines behind SqlHost.
//
// DurableObjectEngine: ctx.storage.sql.exec for statements. DO SQL refuses
// BEGIN/SAVEPOINT/RELEASE/ROLLBACK TO, so a module savepoint becomes a nested
// ctx.storage.transaction() whose callback stays open until the module
// releases (resolve -> RELEASE) or rolls back (reject -> ROLLBACK TO + RELEASE)
// that savepoint. Nested storage transactions are SQLite savepoints, so LIFO
// order and partial rollback follow SQLite's own rules.
//
// NodeEngine: node:sqlite with real BEGIN/SAVEPOINT, for the reference run.

import { installed } from "./runtime-globals.js";

// SQLite result codes for the messages modules see. The Rust host reports
// sqlx's "error returned from database: (code: N) <sqlite message>".
const codes = {
  SQLITE_ERROR: 1, SQLITE_ABORT: 4, SQLITE_BUSY: 5, SQLITE_NOMEM: 7, SQLITE_INTERRUPT: 9, SQLITE_TOOBIG: 18, SQLITE_CONSTRAINT: 19, SQLITE_MISMATCH: 20, SQLITE_RANGE: 25,
  SQLITE_CONSTRAINT_CHECK: 275, SQLITE_CONSTRAINT_FOREIGNKEY: 787, SQLITE_CONSTRAINT_NOTNULL: 1299, SQLITE_CONSTRAINT_PRIMARYKEY: 1555, SQLITE_CONSTRAINT_TRIGGER: 1811, SQLITE_CONSTRAINT_UNIQUE: 2067, SQLITE_CONSTRAINT_ROWID: 2579,
};
/** workerd: "<message>: SQLITE_X (extended: SQLITE_X_Y)"; node:sqlite: errcode. */
export function rustSqlMessage(error) {
  const text = String(error?.message ?? error);
  if (typeof error?.errcode === "number") return `error returned from database: (code: ${error.errcode}) ${text}`;
  const match = /^([\s\S]*): (SQLITE_[A-Z_]+)(?: \(extended: (SQLITE_[A-Z_]+)\))?$/.exec(text);
  if (!match) return text;
  const code = codes[match[3]] ?? codes[match[2]];
  return code === undefined ? match[1] : `error returned from database: (code: ${code}) ${match[1]}`;
}

export class DurableObjectEngine {
  constructor(storage) {
    this.storage = storage;
    this.sql = storage.sql;
    this.stack = [];
  }

  exec(sql, binds) {
    let cursor, rows;
    try {
      cursor = this.sql.exec(sql, ...binds);
      rows = cursor.raw().toArray();
    } catch (error) { throw new Error(rustSqlMessage(error)); }
    return { columnNames: cursor.columnNames, rows, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
  }

  async open(name) {
    let settle;
    const gate = new Promise((resolve, reject) => { settle = { resolve, reject }; });
    let opened;
    const started = new Promise((resolve) => { opened = resolve; });
    const done = this.storage.transaction(async () => { opened(); await gate; });
    done.catch(() => {});
    this.stack.push({ name: String(name).toLowerCase(), settle, done });
    await started;
  }

  find(name) {
    const lower = String(name).toLowerCase();
    for (let index = this.stack.length - 1; index >= 0; index--) if (this.stack[index].name === lower) return index;
    throw new Error(`no such savepoint: ${name}`);
  }

  async close(commit) {
    const top = this.stack.pop();
    if (commit) top.settle.resolve(); else top.settle.reject(new Error("ROLLBACK TO"));
    await top.done.catch(() => {});
  }

  savepoint(name) { return this.open(name); }

  async release(name) {
    const index = this.find(name);
    // RELEASE commits the named savepoint and every savepoint opened after it.
    while (this.stack.length > index) await this.close(true);
  }

  async rollbackTo(name) {
    const index = this.find(name);
    const original = this.stack[index].name;
    while (this.stack.length > index) await this.close(false);
    // SQLite keeps the savepoint open after ROLLBACK TO.
    await this.open(original);
  }

  /** Releases (commit) or rolls back every savepoint still open at the end of the invocation. */
  async settle(commit) {
    while (this.stack.length) await this.close(commit);
  }

  /**
   * DO SQL checks deferred foreign keys only at its implicit commit, after
   * the event; a violation there fails the request and resets the object.
   * Check inside the transaction instead (O(rows with foreign keys): the
   * spike's measure, not a design).
   */
  deferredViolation() {
    const started = installed.Date.now();
    const cursor = this.sql.exec('SELECT "table", rowid, parent, fkid FROM pragma_foreign_key_check LIMIT 1');
    const row = cursor.toArray()[0];
    return { violation: row ? `FOREIGN KEY constraint failed (${row.table} -> ${row.parent})` : undefined, rowsRead: cursor.rowsRead, ms: installed.Date.now() - started };
  }
}

export class NodeEngine {
  constructor(db) {
    this.db = db;
    this.prepared = new Map();
  }

  statement(sql) {
    let statement = this.prepared.get(sql);
    if (!statement) {
      statement = this.db.prepare(sql);
      statement.setReturnArrays(true);
      this.prepared.set(sql, statement);
    }
    return statement;
  }

  exec(sql, binds) {
    try { return this.run(sql, binds); } catch (error) { throw new Error(rustSqlMessage(error)); }
  }

  run(sql, binds) {
    const statement = this.statement(sql);
    const columns = statement.columns();
    if (!columns.length) {
      const result = statement.run(...binds);
      return { columnNames: [], rows: [], rowsRead: 0, rowsWritten: Number(result.changes) };
    }
    const rows = statement.all(...binds);
    return { columnNames: columns.map((column) => column.name), rows, rowsRead: 0, rowsWritten: 0 };
  }

  decltypes(sql) { return this.statement(sql).columns().map((column) => column.type); }

  savepoint(name) { this.db.exec(`SAVEPOINT "${String(name).replaceAll('"', '""')}"`); this.depth = (this.depth ?? 0) + 1; return Promise.resolve(); }
  release(name) { this.db.exec(`RELEASE "${String(name).replaceAll('"', '""')}"`); return Promise.resolve(); }
  rollbackTo(name) { this.db.exec(`ROLLBACK TO "${String(name).replaceAll('"', '""')}"`); return Promise.resolve(); }
  settle() { return Promise.resolve(); }
  deferredViolation() { return { violation: undefined, rowsRead: 0 }; }

  /** One atomic transaction; COMMIT releases any open savepoints, ROLLBACK discards them. */
  async transaction(run) {
    this.db.exec("BEGIN IMMEDIATE");
    let rolledBack = false;
    let result;
    try {
      result = await run(() => { rolledBack = true; });
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
    if (rolledBack) { this.db.exec("ROLLBACK"); return result; }
    try {
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      // Like the Rust store: a failed COMMIT (deferred foreign keys) is an
      // invocation error and nothing is kept.
      this.db.exec("ROLLBACK");
      return { ...result, committed: false, envelope: { status: "error", kind: "commit", message: String(error.message) } };
    }
  }
}
