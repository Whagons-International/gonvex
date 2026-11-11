// Statement building and catalog reads shared by the Durable Object and tests.
// `sql` arguments are Durable Object SqlStorage-shaped: exec(sql, ...params)
// returning a cursor with toArray(), one() and raw().

import { fromWire, quoteIdent, tablePlan, toWire } from './codec.mjs';

export const MAX_PARAMS = 100;
export const MAX_STATEMENT_BYTES = 100_000;

/**
 * Multi-row INSERTs for one chunk: each statement binds at most MAX_PARAMS
 * values and stays under MAX_STATEMENT_BYTES of SQL text. Large values travel
 * as bound parameters, never inline, so the text stays small.
 */
export function buildInserts(table, columns, rows) {
  const head = `INSERT INTO ${quoteIdent(table)} (${columns.map((c) => (c === 'rowid' ? 'rowid' : quoteIdent(c))).join(', ')}) VALUES `;
  if (columns.length > MAX_PARAMS) throw new Error(`${table}: ${columns.length} columns exceed ${MAX_PARAMS} bound parameters per row`);
  const statements = [];
  let tuples = [];
  let params = [];
  let textBytes = head.length;
  const flush = () => {
    if (tuples.length) statements.push({ sql: head + tuples.join(', '), params });
    tuples = [];
    params = [];
    textBytes = head.length;
  };
  for (const row of rows) {
    if (row.length !== columns.length) throw new Error(`${table}: row has ${row.length} values, expected ${columns.length}`);
    const parts = [];
    const rowParams = [];
    for (const value of row) {
      const { sql, param } = fromWire(value);
      parts.push(sql);
      if (param !== undefined) rowParams.push(param);
    }
    const tuple = `(${parts.join(', ')})`;
    if (tuples.length && (params.length + rowParams.length > MAX_PARAMS || textBytes + tuple.length + 2 > MAX_STATEMENT_BYTES)) flush();
    tuples.push(tuple);
    params.push(...rowParams);
    textBytes += tuple.length + 2;
  }
  flush();
  return statements;
}

/** Platform or tool tables that are never part of a tenant. */
export function isPlatformTable(name) {
  return name.startsWith('_cf_') || name.startsWith('__miniflare');
}

// FTS5 creates these shadow tables for each virtual table.
const FTS5_SHADOW = ['data', 'idx', 'content', 'docsize', 'config'];

/**
 * sqlite_schema in creation order, without platform tables. `kind` is
 * table / virtual / shadow / view / index / trigger. It is derived from
 * sqlite_schema alone: pragma_table_list resolves every view's columns, which
 * costs ~14 s of CPU in a Durable Object holding the tenant schema (474 views).
 */
export function readSchema(sql) {
  const rows = sql
    .exec('SELECT type, name, tbl_name AS tblName, sql FROM sqlite_schema ORDER BY rowid')
    .toArray()
    .filter((o) => !isPlatformTable(o.name));
  const virtual = new Set(rows.filter((o) => o.type === 'table' && /^\s*CREATE\s+VIRTUAL\s+TABLE/i.test(o.sql ?? '')).map((o) => o.name));
  const shadow = new Set();
  for (const name of virtual) for (const suffix of FTS5_SHADOW) shadow.add(`${name}_${suffix}`);
  return rows.map((o) => ({
    ...o,
    kind: o.type !== 'table' ? o.type : virtual.has(o.name) ? 'virtual' : shadow.has(o.name) ? 'shadow' : 'table',
  }));
}

/**
 * Splits a schema into what is created before the rows (tables, virtual
 * tables, indexes, views) and after them (triggers, so copied rows fire
 * nothing), plus the external-content FTS5 indexes to rebuild from their
 * copied content. SQLite's own objects (sqlite_sequence, sqlite_stat*,
 * autoindexes) and FTS shadow tables are created by SQLite itself.
 */
export function schemaPhases(schema) {
  const own = schema.filter((o) => o.sql && !o.name.startsWith('sqlite_') && o.kind !== 'shadow');
  const before = own.filter((o) => o.type !== 'trigger');
  const after = own.filter((o) => o.type === 'trigger');
  const fts = own
    .filter((o) => o.kind === 'virtual')
    .map((o) => {
      const lowered = o.sql.toLowerCase();
      if (!/using\s+fts5/.test(lowered) || !/content\s*=/.test(lowered) || /content\s*=\s*''/.test(lowered)) {
        throw new Error(`${o.name}: only external-content FTS5 virtual tables are supported`);
      }
      return o.name;
    });
  return { before, after, fts };
}

/** (type, name, sql) of a schema for comparison, sorted. */
export function schemaFingerprint(schema) {
  return schema
    .filter((o) => !o.name.startsWith('sqlite_stat'))
    .map((o) => `${o.type}\u0000${o.name}\u0000${o.sql ?? ''}`)
    .sort();
}

/** Copy plans of every ordinary table (no virtual, shadow or sqlite_* tables). */
export function readPlans(sql, schema = readSchema(sql)) {
  const tables = schema.filter((o) => o.kind === 'table' && !o.name.startsWith('sqlite_'));
  // One statement for every table's columns.
  const columns = new Map(tables.map((t) => [t.name, []]));
  for (const c of sql
    .exec(
      `SELECT m.name AS tbl, c.name AS name, c.type AS type, c.pk AS pk, c.hidden AS hidden
       FROM sqlite_schema AS m, pragma_table_xinfo(m.name) AS c WHERE m.type = 'table' ORDER BY m.rowid, c.cid`,
    )
    .toArray()) {
    columns.get(c.tbl)?.push(c);
  }
  return tables.map(({ name, sql: ddl }) => {
    const cols = columns.get(name);
    const primaryKey = cols.filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk).map((c) => c.name);
    return tablePlan({ name, sql: ddl, columns: cols, primaryKey });
  });
}

export function readSequences(sql) {
  const exists = sql.exec("SELECT count(*) AS n FROM sqlite_schema WHERE name = 'sqlite_sequence'").one().n;
  if (!exists) return {};
  const out = {};
  for (const [name, type, value] of sql.exec("SELECT name, typeof(seq), CAST(seq AS TEXT) FROM sqlite_sequence ORDER BY name").raw()) {
    out[name] = toWire(type, value);
  }
  return out;
}

