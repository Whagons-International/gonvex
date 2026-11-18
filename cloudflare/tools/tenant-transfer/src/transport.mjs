// P03 tenant transfer: a Durable Object per tenant that accepts a SQLite
// file's schema and rows in bounded chunks, and gives them back.
//
// Routes (POST, JSON body, `authorization: Bearer $TRANSFER_TOKEN`):
//   /t/<tenant>/status      object size, table and schema object counts
//   /t/<tenant>/schema      run a batch of DDL in one transaction; on failure,
//                           one transaction per statement, refusals by name
//   /t/<tenant>/insert      rows of one or more tables, one transaction
//   /t/<tenant>/sequence    set sqlite_sequence values
//   /t/<tenant>/catalog     sqlite_schema (minus platform tables), table plans,
//                           sqlite_sequence
//   /t/<tenant>/read        keyset-paged rows of several tables, size-capped
//   /t/<tenant>/checksum    row count + content sha256 per table
//   /t/<tenant>/check       quick_check per table, foreign_key_check, FTS5
//                           integrity-check
//   /t/<tenant>/pragma      foreign_keys / defer_foreign_keys on or off
//   /t/<tenant>/exec        read-only diagnostic query
//   /t/<tenant>/wipe        delete the object's storage
//
// Platform limits honored here: at most 100 bound parameters per statement,
// statement text far below 100 KB (values are always bound), one transaction
// per request, request sizes chosen by the caller and capped below.
// `ms` in responses is wall time inside the object; on the platform the clock
// does not advance during synchronous work, so it reads 0 there.
//
// CPU per request (X10): the object's CPU limit applies to each request, so
// the operations whose work grows with the tenant (`schema`, `checksum`,
// `check`) stop at `budgetMs` and answer with `next` (a cursor; `consumed` for
// `schema`). The caller sends the request again with that cursor. Time is read
// after a macrotask yield, when it includes the CPU spent before it.

import { createHash } from 'node:crypto';
import { RowHasher, fromWire, keyIndexes, quoteIdent, readTypedRows, toWire } from './codec.mjs';
import { Sha256, createResumableHash } from './sha256.mjs';
import { buildInserts, readPlans, readSchema, readSequences } from './sql.mjs';

const MAX_READ_BYTES = 8 * 1024 * 1024;
/** Rows hashed between clock reads. */
const CHECKSUM_PAGE_ROWS = 2_000;
/** Default CPU budget of one request, measured after yields. */
export const REQUEST_BUDGET_MS = 1_000;

/** A macrotask turn: on the platform, `Date.now()` afterwards includes the CPU spent before it. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

export class TenantTransfer {
  constructor(ctx, { budgetMs = REQUEST_BUDGET_MS } = {}) {
    this.ctx = ctx;
    this.budgetMs = budgetMs;
  }
  get sql() {
    return this.ctx.storage.sql;
  }

  // Table plans are derived from the schema; cached until the next DDL.
  plans() {
    this.cachedPlans ??= readPlans(this.sql);
    return this.cachedPlans;
  }

  async fetch(request) {
    const op = new URL(request.url).pathname.split('/').at(-1);
    const body = request.method === 'POST' ? await request.json() : {};
    const started = Date.now();
    try {
      const result = await this[`op_${op}`]?.(body);
      if (result === undefined) return Response.json({ error: `unknown op ${op}` }, { status: 404 });
      return Response.json({ ...result, ms: Date.now() - started });
    } catch (error) {
      return Response.json({ error: String(error?.message ?? error), op }, { status: 500 });
    }
  }

  op_status() {
    const tables = this.sql.exec(
      "SELECT count(*) AS n FROM sqlite_schema WHERE type='table' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\'",
    ).one().n;
    const objects = this.sql.exec("SELECT count(*) AS n FROM sqlite_schema WHERE name NOT LIKE '\\_cf\\_%' ESCAPE '\\'").one().n;
    return { databaseSize: this.sql.databaseSize, tables, objects };
  }

  // DDL. Statements run in transactions of a few statements each (a DDL
  // statement costs milliseconds on a schema of thousands of objects); if a
  // transaction fails it is rolled back and replayed one statement per
  // transaction, so a statement the platform refuses is reported by name and
  // the rest still apply. Stops at the budget: `consumed` statements were
  // handled (applied or failed); the caller sends the rest again.
  async op_schema({ statements }) {
    this.cachedPlans = null;
    const started = Date.now();
    const failed = [];
    let applied = 0;
    let consumed = 0;
    let size = 8;
    while (consumed < statements.length) {
      const batch = statements.slice(consumed, consumed + size);
      const t0 = Date.now();
      try {
        this.ctx.storage.transactionSync(() => {
          for (const { sql } of batch) this.sql.exec(sql);
        });
        applied += batch.length;
      } catch {
        for (const { name, sql } of batch) {
          try {
            this.ctx.storage.transactionSync(() => this.sql.exec(sql));
            applied++;
          } catch (error) {
            failed.push({ name, bytes: sql.length, error: String(error?.message ?? error) });
          }
        }
      }
      consumed += batch.length;
      await settle();
      const ms = Date.now() - t0;
      size = Math.max(1, Math.min(64, size * 2, ms > 0 ? Math.floor((batch.length * this.budgetMs) / (4 * ms)) : size * 2));
      if (Date.now() - started >= this.budgetMs) break;
    }
    return { applied, failed, consumed };
  }

  // Rows for one or more tables, in one transaction: all or nothing.
  // `parts` is [{table, columns, rows}].
  op_insert({ parts }) {
    // Rows arrive table by table, not in foreign-key order; like
    // gonvex-admin export-sqlite, the copy runs with enforcement off and the
    // check op runs PRAGMA foreign_key_check at the end. The setting is per
    // connection, so it is applied on every request (an evicted object
    // reopens with enforcement on). SQLite ignores it inside a transaction.
    this.sql.exec('PRAGMA foreign_keys = OFF');
    let written = 0;
    let statementCount = 0;
    let rowCount = 0;
    this.ctx.storage.transactionSync(() => {
      for (const { table, columns, rows } of parts) {
        for (const { sql, params } of buildInserts(table, columns, rows)) {
          written += this.sql.exec(sql, ...params).rowsWritten;
          statementCount++;
        }
        rowCount += rows.length;
      }
    });
    return { rows: rowCount, statements: statementCount, rowsWritten: written };
  }

  op_sequence({ sequences }) {
    this.ctx.storage.transactionSync(() => {
      for (const [name, seq] of Object.entries(sequences)) {
        const { sql, param } = fromWire(seq);
        const params = param === undefined ? [] : [param];
        this.sql.exec('DELETE FROM sqlite_sequence WHERE name = ?', name);
        this.sql.exec(`INSERT INTO sqlite_sequence(name, seq) VALUES (?, ${sql})`, name, ...params);
      }
    });
    return { sequences: Object.keys(sequences).length };
  }

  op_catalog() {
    const read = (label, fn) => { try { return fn(); } catch (error) { throw new Error(`catalog ${label}: ${error.message}`); } };
    return { schema: read('schema', () => readSchema(this.sql)), plans: read('plans', () => this.plans()), sequences: read('sequences', () => readSequences(this.sql)) };
  }

  // Keyset-paged rows of several tables in key order, up to maxBytes of
  // JSON. `tables` is [{table, after}]; a table whose rows did not all fit
  // comes back with `next`, and the remaining tables are left for the next
  // request.
  op_read({ tables, maxBytes = 4 * 1024 * 1024 }) {
    const cap = Math.min(maxBytes, MAX_READ_BYTES);
    const pages = [];
    let bytes = 0;
    for (const { table, after = null } of tables) {
      if (bytes >= cap) break;
      const plan = this.plans().find((p) => p.name === table);
      if (!plan) throw new Error(`no table ${table}`);
      const keys = keyIndexes(plan);
      const rows = [];
      let full = false;
      for (const raw of readTypedRows(this.sql, plan, { after })) {
        const row = [];
        for (let i = 0; i < raw.length; i += 2) row.push(toWire(raw[i], raw[i + 1]));
        rows.push(row);
        bytes += JSON.stringify(row).length;
        if (bytes >= cap) {
          full = true;
          break;
        }
      }
      pages.push({ table, columns: plan.copyColumns, rows, next: full ? keys.map((i) => rows.at(-1)[i]) : null });
    }
    return { pages };
  }

  // Row count + content sha256 per table, in key order. A table may take
  // several requests: `next` carries the key of the last row hashed and the
  // running hash state; send it back as `cursor` to continue.
  async op_checksum({ tables = null, cursor = null }) {
    const plans = this.plans().filter((p) => !tables || tables.includes(p.name));
    const started = Date.now();
    const out = {};
    let index = 0;
    let resume = null;
    if (cursor) {
      index = plans.findIndex((p) => p.name === cursor.table);
      if (index < 0) throw new Error(`checksum cursor names no requested table ${cursor.table}`);
      resume = cursor;
    }
    for (; index < plans.length; index++) {
      const plan = plans[index];
      const keys = keyIndexes(plan);
      const hasher = resume?.state ? RowHasher.resume(resume.state, (h) => Sha256.load(h)) : new RowHasher(createResumableHash);
      let after = resume?.after ?? null;
      resume = null;
      for (;;) {
        let last = null;
        let n = 0;
        for (const raw of readTypedRows(this.sql, plan, { after, limit: CHECKSUM_PAGE_ROWS })) {
          hasher.addTyped(raw);
          last = raw;
          n++;
        }
        if (n < CHECKSUM_PAGE_ROWS) break;
        after = keys.map((i) => toWire(last[2 * i], last[2 * i + 1]));
        await settle();
        if (Date.now() - started >= this.budgetMs) return { checksums: out, next: { table: plan.name, after, state: hasher.save() } };
      }
      out[plan.name] = hasher.digest();
      await settle();
      if (index + 1 < plans.length && Date.now() - started >= this.budgetMs) {
        return { checksums: out, next: { table: plans[index + 1].name, after: null, state: null } };
      }
    }
    return { checksums: out };
  }

  // A whole-database PRAGMA quick_check over ~9,500 schema objects returns
  // SQLITE_NOMEM in local workerd (an opcode cap per program; some platform
  // hosts allow it) and integrity_check is not authorized there.
  // quick_check(table) checks each table with its indexes and works on both.
  // foreign_key_check runs per table too (the sum equals the whole-database
  // count), and the FTS5 integrity-checks one index at a time. Stops at the
  // budget with `next`; send it back as `cursor` to continue.
  async op_check({ fts = [], cursor = null }) {
    const started = Date.now();
    const tables = this.sql
      .exec("SELECT name FROM sqlite_schema WHERE type = 'table' AND sql NOT LIKE 'CREATE VIRTUAL%' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY rowid")
      .toArray()
      .map(({ name }) => name);
    const at = cursor ?? { step: 0, quick: [], foreignKeys: 0, fts: {} };
    if (!Number.isSafeInteger(at.step) || at.step < 0 || !Array.isArray(at.quick) || !Number.isSafeInteger(at.foreignKeys)) throw new Error('invalid check cursor');
    const steps = tables.length * 2 + fts.length;
    while (at.step < steps) {
      const step = at.step;
      if (step < tables.length) {
        const name = tables[step];
        for (const row of this.sql.exec(`PRAGMA quick_check(${quoteIdent(name)})`).toArray()) {
          const value = Object.values(row)[0];
          if (value !== 'ok') at.quick.push(`${name}: ${value}`);
        }
      } else if (step < tables.length * 2) {
        at.foreignKeys += this.sql.exec('SELECT count(*) AS n FROM pragma_foreign_key_check(?)', tables[step - tables.length]).one().n;
      } else {
        const name = fts[step - tables.length * 2];
        try {
          this.sql.exec(`INSERT INTO ${quoteIdent(name)}(${quoteIdent(name)}) VALUES ('integrity-check')`);
          at.fts[name] = 'ok';
        } catch (error) {
          at.fts[name] = String(error?.message ?? error);
        }
      }
      at.step++;
      await settle();
      if (at.step < steps && Date.now() - started >= this.budgetMs) return { next: at };
    }
    return { quickCheck: at.quick.length ? at.quick.join('; ') : 'ok', foreignKeyViolations: at.foreignKeys, fts: at.fts };
  }

  // Connection settings (foreign_keys); SQLite ignores them inside a
  // transaction, so they run outside transactionSync.
  op_pragma({ sql }) {
    if (!/^\s*PRAGMA\s+(foreign_keys|defer_foreign_keys)\s*=\s*(ON|OFF|0|1)\s*;?\s*$/i.test(sql)) throw new Error('pragma not allowed');
    this.sql.exec(sql);
    return { value: this.sql.exec(sql.replace(/=.*$/, '')).toArray() };
  }

  op_exec({ sql, params = [] }) {
    if (!/^\s*(SELECT|WITH)\b/i.test(sql) && !/^\s*PRAGMA\s+[\w.]+(\s*\([^)]*\))?\s*;?\s*$/i.test(sql)) {
      throw new Error('exec is read-only (SELECT, WITH, or PRAGMA without assignment)');
    }
    const cursor = this.sql.exec(sql, ...params);
    return { rows: cursor.toArray().slice(0, 1000) };
  }

  async op_wipe() {
    await this.ctx.storage.deleteAll();
    this.cachedPlans = null;
    return { wiped: true };
  }
}
