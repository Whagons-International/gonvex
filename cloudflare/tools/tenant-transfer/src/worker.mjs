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

import { DurableObject } from 'cloudflare:workers';
import { createHash } from 'node:crypto';
import { RowHasher, fromWire, keyIndexes, quoteIdent, selectSql, toWire } from './codec.mjs';
import { buildInserts, readPlans, readSchema, readSequences } from './sql.mjs';

const MAX_READ_BYTES = 8 * 1024 * 1024;

export class TenantStore extends DurableObject {
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

  // DDL. A batch runs in one transaction; if any statement fails, the batch
  // is rolled back and replayed one statement per transaction, so a statement
  // the platform refuses is reported by name and the rest still apply.
  op_schema({ statements }) {
    this.cachedPlans = null;
    try {
      this.ctx.storage.transactionSync(() => {
        for (const { sql } of statements) this.sql.exec(sql);
      });
      return { applied: statements.length, failed: [] };
    } catch {
      // fall through to one transaction per statement
    }
    const failed = [];
    let applied = 0;
    for (const { name, sql } of statements) {
      try {
        this.ctx.storage.transactionSync(() => this.sql.exec(sql));
        applied++;
      } catch (error) {
        failed.push({ name, bytes: sql.length, error: String(error?.message ?? error) });
      }
    }
    return { applied, failed };
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
    return { schema: readSchema(this.sql), plans: this.plans(), sequences: readSequences(this.sql) };
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
      const { sql, params } = selectSql(plan, { after });
      const keys = keyIndexes(plan);
      const rows = [];
      let full = false;
      for (const raw of this.sql.exec(sql, ...params).raw()) {
        const row = [];
        for (let i = 0; i < raw.length; i += 2) row.push(toWire(raw[i], raw[i + 1]));
        rows.push(row);
        bytes += JSON.stringify(row).length;
        if (bytes >= cap) {
          full = true;
          break;
        }
      }
      // A cursor stays open only until the next exec; stopping early is fine.
      pages.push({ table, columns: plan.copyColumns, rows, next: full ? keys.map((i) => rows.at(-1)[i]) : null });
    }
    return { pages };
  }

  op_checksum({ tables = null }) {
    const plans = this.plans().filter((p) => !tables || tables.includes(p.name));
    const out = {};
    for (const plan of plans) {
      const hasher = new RowHasher(createHash);
      for (const raw of this.sql.exec(selectSql(plan).sql).raw()) hasher.addTyped(raw);
      out[plan.name] = hasher.digest();
    }
    return { checksums: out };
  }

  op_check({ fts = [] }) {
    // A whole-database PRAGMA quick_check over ~9,500 schema objects runs out
    // of memory in the object (SQLITE_NOMEM) and integrity_check is not
    // authorized; quick_check(table) checks each table with its indexes.
    const quick = [];
    const tables = this.sql
      .exec("SELECT name FROM sqlite_schema WHERE type = 'table' AND sql NOT LIKE 'CREATE VIRTUAL%' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\'")
      .toArray();
    for (const { name } of tables) {
      for (const row of this.sql.exec(`PRAGMA quick_check(${quoteIdent(name)})`).toArray()) {
        const value = Object.values(row)[0];
        if (value !== 'ok') quick.push(`${name}: ${value}`);
      }
    }
    if (!quick.length) quick.push('ok');
    const foreignKeys = this.sql.exec('PRAGMA foreign_key_check').toArray().length;
    const ftsResults = {};
    for (const name of fts) {
      try {
        this.sql.exec(`INSERT INTO ${quoteIdent(name)}(${quoteIdent(name)}) VALUES ('integrity-check')`);
        ftsResults[name] = 'ok';
      } catch (error) {
        ftsResults[name] = String(error?.message ?? error);
      }
    }
    return { quickCheck: quick.join('; '), foreignKeyViolations: foreignKeys, fts: ftsResults };
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

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/t\/([A-Za-z0-9_.-]{1,128})\/([a-z]+)$/);
    if (!match) return new Response('not found', { status: 404 });
    const expected = env.TRANSFER_TOKEN;
    if (!expected || request.headers.get('authorization') !== `Bearer ${expected}`) {
      return new Response('unauthorized', { status: 401 });
    }
    const stub = env.TENANT_STORE.get(env.TENANT_STORE.idFromName(match[1]));
    return stub.fetch(request);
  },
};
