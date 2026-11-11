// Push a SQLite file into a tenant Durable Object, pull it back out, and
// compare content checksums. Pure transport: every table moves verbatim; the
// "Moving existing data" rules run in gonvex-tenant-stage before and after.

import { createHash } from 'node:crypto';
import { RowHasher, selectSql, toWire, wireSize } from './codec.mjs';
import { openFile } from './file.mjs';
import { buildInserts, readPlans, readSchema, readSequences, schemaFingerprint, schemaPhases } from './sql.mjs';

/** Chunking. One request = one transaction in the object. */
export const LIMITS = {
  chunkBytes: 1_500_000, // approximate JSON bytes of rows per insert request
  chunkRows: 4_000,
  schemaBytes: 1_500_000, // DDL text per schema request
  schemaStatements: 400,
  readBytes: 4_000_000, // rows per read response
  readTables: 200, // tables per read request
  checksumTables: 60, // tables per checksum request
};

export class ObjectClient {
  constructor({ url, token, tenant, log = () => {} }) {
    this.base = `${url.replace(/\/$/, '')}/t/${encodeURIComponent(tenant)}`;
    this.token = token;
    this.log = log;
    this.requests = 0;
    this.bytesSent = 0;
    this.bytesReceived = 0;
  }

  async call(op, body = {}) {
    const payload = JSON.stringify(body);
    for (let attempt = 1; ; attempt++) {
      let response;
      try {
        response = await fetch(`${this.base}/${op}`, {
          method: 'POST',
          headers: { authorization: `Bearer ${this.token}`, 'content-type': 'application/json' },
          body: payload,
          // An object reset can leave a request without a response.
          signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
        });
      } catch (error) {
        // Network failure before a response: only reads are safe to retry.
        if (attempt < 4 && RETRYABLE.has(op)) {
          await sleep(500 * attempt);
          continue;
        }
        throw error;
      }
      const text = await response.text();
      this.requests++;
      this.bytesSent += payload.length;
      this.bytesReceived += text.length;
      let json;
      try {
        json = JSON.parse(text);
      } catch {
        json = { error: text.slice(0, 500) };
      }
      if (!response.ok || json.error) {
        if (attempt < 4 && RETRYABLE.has(op) && response.status >= 500) {
          await sleep(500 * attempt);
          continue;
        }
        throw new Error(`${op}: HTTP ${response.status}: ${json.error ?? text.slice(0, 500)}`);
      }
      return json;
    }
  }
}

const REQUEST_TIMEOUT_MS = 180_000;
const RETRYABLE = new Set(['status', 'catalog', 'read', 'checksum', 'check', 'exec', 'wipe']);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function batches(items, { maxItems, maxBytes, size }) {
  const out = [];
  let current = [];
  let bytes = 0;
  for (const item of items) {
    const n = size(item);
    if (current.length && (current.length >= maxItems || bytes + n > maxBytes)) {
      out.push(current);
      current = [];
      bytes = 0;
    }
    current.push(item);
    bytes += n;
  }
  if (current.length) out.push(current);
  return out;
}

/** Content checksums of every ordinary table of a local file. */
export function fileChecksums(path, tables = null) {
  const file = openFile(path, { readOnly: true });
  try {
    const out = {};
    for (const plan of readPlans(file.sql)) {
      if (tables && !tables.includes(plan.name)) continue;
      const hasher = new RowHasher(createHash);
      for (const raw of file.sql.exec(selectSql(plan).sql).raw()) hasher.addTyped(raw);
      out[plan.name] = hasher.digest();
    }
    return out;
  } finally {
    file.close();
  }
}

export function fileSchema(path) {
  const file = openFile(path, { readOnly: true });
  try {
    return readSchema(file.sql);
  } finally {
    file.close();
  }
}

export async function objectChecksums(client, tables) {
  const out = {};
  for (let i = 0; i < tables.length; i += LIMITS.checksumTables) {
    const { checksums } = await client.call('checksum', { tables: tables.slice(i, i + LIMITS.checksumTables) });
    Object.assign(out, checksums);
  }
  return out;
}

/** Table-by-table comparison; `tables` defaults to the union of both sides. */
export function compareChecksums(left, right, tables = null) {
  const names = tables ?? [...new Set([...Object.keys(left), ...Object.keys(right)])].sort();
  const mismatches = [];
  let rows = 0;
  for (const name of names) {
    const a = left[name];
    const b = right[name];
    if (!a || !b || a.rows !== b.rows || a.sha256 !== b.sha256) {
      mismatches.push({ table: name, left: a ?? null, right: b ?? null });
    } else {
      rows += a.rows;
    }
  }
  return { tables: names.length, rows, mismatches };
}

/**
 * File -> object. The object must be empty (wipe it first). Statements the
 * platform refuses are returned in `schemaFailures`; nothing is rewritten.
 */
export async function pushFile(client, path, { log = () => {} } = {}) {
  const file = openFile(path, { readOnly: true });
  const timings = {};
  const t0 = Date.now();
  try {
    const status = await client.call('status');
    if (status.objects > 0) throw new Error(`object is not empty (${status.objects} schema objects); wipe it first`);

    const schema = readSchema(file.sql);
    const { before, after, fts } = schemaPhases(schema);
    const schemaFailures = [];
    let t = Date.now();
    for (const batch of batches(before, { maxItems: LIMITS.schemaStatements, maxBytes: LIMITS.schemaBytes, size: (o) => o.sql.length })) {
      const result = await client.call('schema', { statements: batch.map(({ name, sql }) => ({ name, sql })) });
      schemaFailures.push(...result.failed);
    }
    timings.schemaMs = Date.now() - t;
    log(`schema: ${before.length} objects, ${schemaFailures.length} refused`);

    t = Date.now();
    const plans = readPlans(file.sql);
    let rows = 0;
    let chunks = 0;
    let rowsWritten = 0;
    // One request carries rows of as many tables as fit in a chunk.
    let parts = [];
    let part = null;
    let bytes = 0;
    let count = 0;
    const flush = async () => {
      if (!count) return;
      const result = await client.call('insert', { parts });
      rowsWritten += result.rowsWritten;
      chunks++;
      parts = [];
      part = null;
      bytes = 0;
      count = 0;
    };
    for (const plan of plans) {
      for (const raw of file.sql.exec(selectSql(plan).sql).raw()) {
        const row = [];
        let size = 0;
        for (let i = 0; i < raw.length; i += 2) {
          const value = toWire(raw[i], raw[i + 1]);
          size += wireSize(value);
          row.push(value);
        }
        if (count && (bytes + size > LIMITS.chunkBytes || count >= LIMITS.chunkRows)) await flush();
        if (!part || part.table !== plan.name) {
          part = { table: plan.name, columns: plan.copyColumns, rows: [] };
          parts.push(part);
        }
        part.rows.push(row);
        bytes += size;
        count++;
        rows++;
      }
    }
    await flush();
    timings.rowsMs = Date.now() - t;
    log(`rows: ${rows} in ${chunks} chunks`);

    t = Date.now();
    const sequences = readSequences(file.sql);
    if (Object.keys(sequences).length) await client.call('sequence', { sequences });
    for (const batch of batches(after, { maxItems: LIMITS.schemaStatements, maxBytes: LIMITS.schemaBytes, size: (o) => o.sql.length })) {
      const result = await client.call('schema', { statements: batch.map(({ name, sql }) => ({ name, sql })) });
      schemaFailures.push(...result.failed);
    }
    for (const name of fts) {
      const q = `"${name.replaceAll('"', '""')}"`;
      const result = await client.call('schema', { statements: [{ name: `${name} rebuild`, sql: `INSERT INTO ${q}(${q}) VALUES ('rebuild')` }] });
      schemaFailures.push(...result.failed);
    }
    await client.call('pragma', { sql: 'PRAGMA foreign_keys = ON' });
    timings.triggersMs = Date.now() - t;
    log(`triggers: ${after.length}, fts rebuilt: ${fts.length}`);
    timings.totalMs = Date.now() - t0;
    return { schemaObjects: before.length + after.length, schemaFailures, tables: plans.length, rows, chunks, rowsWritten, fts, timings };
  } finally {
    file.close();
  }
}

/** Object -> new file (verbatim). */
export async function pullFile(client, path, { log = () => {} } = {}) {
  const timings = {};
  const t0 = Date.now();
  const catalog = await client.call('catalog');
  const { before, after, fts } = schemaPhases(catalog.schema);
  const file = openFile(path);
  try {
    file.db.exec('PRAGMA foreign_keys = OFF; PRAGMA synchronous = OFF; PRAGMA journal_mode = OFF; BEGIN');
    let t = Date.now();
    for (const o of before) file.db.exec(o.sql);
    timings.schemaMs = Date.now() - t;

    t = Date.now();
    let rows = 0;
    let pages = 0;
    const plansByName = new Map(catalog.plans.map((p) => [p.name, p]));
    let queue = catalog.plans.map((p) => ({ table: p.name, after: null }));
    while (queue.length) {
      const request = queue.slice(0, LIMITS.readTables);
      const response = await client.call('read', { tables: request, maxBytes: LIMITS.readBytes });
      pages++;
      for (const page of response.pages) {
        if (!plansByName.has(page.table)) throw new Error(`unexpected table ${page.table}`);
        for (const { sql, params } of buildInserts(page.table, page.columns, page.rows)) file.sql.run(sql, ...params);
        rows += page.rows.length;
      }
      if (!response.pages.length) throw new Error('read returned no pages');
      const last = response.pages.at(-1);
      queue = queue.slice(response.pages.length);
      if (last.next) queue.unshift({ table: last.table, after: last.next });
    }
    timings.rowsMs = Date.now() - t;
    log(`rows: ${rows} in ${pages} pages`);

    t = Date.now();
    for (const [name, seq] of Object.entries(catalog.sequences)) {
      file.sql.run('DELETE FROM sqlite_sequence WHERE name = ?', name);
      const { sql, params } = buildInserts('sqlite_sequence', ['name', 'seq'], [[name, seq]])[0];
      file.sql.run(sql, ...params);
    }
    for (const o of after) file.db.exec(o.sql);
    for (const name of fts) {
      const q = `"${name.replaceAll('"', '""')}"`;
      file.db.exec(`INSERT INTO ${q}(${q}) VALUES ('rebuild')`);
    }
    file.db.exec('COMMIT');
    file.db.exec('PRAGMA journal_mode = WAL');
    timings.triggersMs = Date.now() - t;
    timings.totalMs = Date.now() - t0;
    return { schemaObjects: before.length + after.length, tables: catalog.plans.length, rows, pages, fts, timings };
  } finally {
    file.close();
  }
}

/** Schema of the object vs a file: objects missing on either side. */
export function compareSchemas(objectSchema, fileSchemaRows) {
  const a = new Set(schemaFingerprint(objectSchema.filter((o) => o.kind !== 'shadow')));
  const b = new Set(schemaFingerprint(fileSchemaRows.filter((o) => o.kind !== 'shadow')));
  const name = (f) => f.split('\u0000').slice(0, 2).join(' ');
  return {
    missingInObject: [...b].filter((f) => !a.has(f)).map(name),
    extraInObject: [...a].filter((f) => !b.has(f)).map(name),
  };
}
