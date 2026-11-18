// X10: a P03 import at the size of the largest dev copy (devcopy-stg1117-12, about 115k rows) through
// the assembled `/__tenant-transfer` path. Every request must stay inside the transport's CPU budget,
// continuing with cursors, and the paged checksums must equal the one-pass node:crypto checksums.
import { env, runInDurableObject } from 'cloudflare:test';
import { expect, it } from 'vitest';
import { createHash } from 'node:crypto';
import { handleTenantTransfer } from '../tenant-transfer';
import { readTypedRows, RowHasher } from '../../../tools/tenant-transfer/src/codec.mjs';
import { readPlans } from '../../../tools/tenant-transfer/src/sql.mjs';
import { REQUEST_BUDGET_MS, TenantTransfer } from '../../../tools/tenant-transfer/src/transport.mjs';

const tenant = 'devcopy-budget';
const config = { GONVEX_DEV_TRANSFER_TARGET: 'gonvex-cf-dev-test', GONVEX_DEV_TRANSFER_TOKEN: 'test-only', GONVEX_DEV_TRANSFER_TENANTS: tenant };
const TABLES = 300, TRIGGERS = 14, BIG_ROWS = 85_000, ROWS = 100;
const name = (i: number) => `t${String(i).padStart(3, '0')}`;
const COLUMNS = ['id', 'parent_id', 'title', 'body', 'rank', 'updated_at'];

/** Per request: operation, wall ms (local workerd runs one request's JavaScript and SQLite on one thread). */
const timings: Array<{ op: string; ms: number }> = [];
async function call<T = Record<string, unknown>>(op: string, body: unknown = {}): Promise<T> {
  const request = new Request(`https://dev/__tenant-transfer/t/${tenant}/${op}`, { method: 'POST', headers: { authorization: 'Bearer test-only' }, body: JSON.stringify(body) });
  const t0 = Date.now();
  const response = (await handleTenantTransfer(request, { ...config, TENANT: env.TENANT }))!;
  const text = await response.text();
  timings.push({ op, ms: Date.now() - t0 });
  if (!response.ok) throw new Error(`${op}: ${response.status} ${text}`);
  return JSON.parse(text) as T;
}
async function schema(statements: Array<{ name: string; sql: string }>) {
  let rest = statements;
  while (rest.length) {
    const result = await call<{ failed: unknown[]; consumed: number }>('schema', { statements: rest });
    expect(result.failed).toEqual([]);
    expect(result.consumed).toBeGreaterThan(0);
    rest = rest.slice(result.consumed);
  }
}
const rowsOf = (table: number, n: number) => Array.from({ length: n }, (_, r) => [
  `r${String(r + 1).padStart(6, '0')}`, table === 0 ? null : `r${String(1 + ((r + 1) * 37) % BIG_ROWS).padStart(6, '0')}`,
  `row ${r + 1}`, `${'body '.repeat(8)}${table}:${r}`, (r + 1) % (table === 0 ? 1000 : 50), 1_700_000_000_000 + r]);

it('imports, checks and verifies 115k rows with every request inside the CPU budget', { timeout: 900_000 }, async () => {
  await call('wipe');
  const before: Array<{ name: string; sql: string }> = [], after: Array<{ name: string; sql: string }> = [];
  for (let i = 0; i < TABLES; i += 1) {
    const t = name(i);
    before.push({ name: t, sql: `CREATE TABLE ${t} (id TEXT PRIMARY KEY, parent_id TEXT${i ? ` REFERENCES ${name(0)}(id)` : ''}, title TEXT NOT NULL, body TEXT, rank INTEGER NOT NULL DEFAULT 0, updated_at INTEGER)` });
    before.push({ name: `${t}_parent`, sql: `CREATE INDEX ${t}_parent ON ${t}(parent_id)` }, { name: `${t}_rank`, sql: `CREATE INDEX ${t}_rank ON ${t}(rank, updated_at)` });
    before.push({ name: `${t}_open`, sql: `CREATE VIEW ${t}_open AS SELECT id, title, rank FROM ${t} WHERE rank >= 0` });
    for (let k = 0; k < TRIGGERS; k += 1) after.push({ name: `${t}_guard_${k}`, sql: `CREATE TRIGGER ${t}_guard_${k} BEFORE UPDATE OF title ON ${t} WHEN NEW.title = '__never_${k}__' BEGIN SELECT RAISE(ABORT, 'guard ${k}'); END` });
  }
  before.push({ name: 'docs', sql: 'CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)' });
  before.push({ name: 'docs_fts', sql: "CREATE VIRTUAL TABLE docs_fts USING fts5(body, content='docs', content_rowid='id')" });
  await schema(before);
  // Insert pages as P03 sends them (at most 4,000 rows / 1.5 MB per request, one transaction each).
  const expected: Record<string, { rows: number; sha256: string }> = {};
  for (let i = 0; i < TABLES; i += 1) {
    const rows = rowsOf(i, i === 0 ? BIG_ROWS : ROWS);
    for (let r = 0; r < rows.length; r += 4000) await call('insert', { parts: [{ table: name(i), columns: COLUMNS, rows: rows.slice(r, r + 4000) }] });
  }
  for (let r = 0; r < 20_000; r += 4000) await call('insert', { parts: [{ table: 'docs', columns: ['id', 'body'], rows: Array.from({ length: 4000 }, (_, k) => [r + k + 1, `document ${r + k} alpha beta gamma ${k % 97}`]) }] });
  await schema(after);
  await schema([{ name: 'docs_fts rebuild', sql: "INSERT INTO docs_fts(docs_fts) VALUES ('rebuild')" }]);

  // Reference: node:crypto in one pass inside the object, as the P03 file side computes it.
  await runInDurableObject(env.TENANT.getByName(tenant), async (_instance, state) => {
    expect(state.storage.sql.exec("SELECT count(*) AS n FROM sqlite_schema").one().n).toBeGreaterThan(5_000);
    // The P03 table plans (a TEXT primary key carries the rowid too), as both sides of P03 use them.
    const plans = readPlans(state.storage.sql);
    for (const plan of plans.filter((p) => /^t\d{3}$|^docs$/.test(p.name))) {
      const hasher = new RowHasher(createHash);
      for (const row of readTypedRows(state.storage.sql, plan)) hasher.addTyped(row);
      expected[plan.name] = hasher.digest();
    }
  });
  expect(Object.values(expected).reduce((n, v) => n + v.rows, 0)).toBe(BIG_ROWS + (TABLES - 1) * ROWS + 20_000);

  // The object's checksums, continued with its cursor.
  const tables = Object.keys(expected);
  const paged: Record<string, { rows: number; sha256: string }> = {};
  let continued = 0;
  for (let i = 0; i < tables.length; i += 60) {
    let cursor: unknown = null;
    do {
      const result: { checksums: typeof paged; next?: unknown } = await call('checksum', { tables: tables.slice(i, i + 60), cursor });
      Object.assign(paged, result.checksums);
      cursor = result.next ?? null;
      if (cursor) continued += 1;
    } while (cursor);
  }
  expect(paged).toEqual(expected);
  // Whether a page continued depends on the machine's speed; the cursor itself is tested below at budget 0.
  void continued;

  // Integrity checks: the cursor stays in the object; the import is complete only after the last page.
  let checks: { next?: unknown; quickCheck?: string; foreignKeyViolations?: number; fts?: Record<string, string> };
  let pages = 0;
  do { checks = await call('check', { fts: ['docs_fts'], cursor: { step: 1e9 } }); pages += 1; } while (checks.next);
  // A client cursor that claims every step done is ignored: the FTS index was still checked.
  expect(checks).toMatchObject({ quickCheck: 'ok', foreignKeyViolations: 0, fts: { docs_fts: 'ok' } });
  expect(pages).toBeGreaterThanOrEqual(1);
  // The transport's own cursor, one step per request (budget 0): the same result as one pass.
  await runInDurableObject(env.TENANT.getByName(tenant), async (_instance, state) => {
    const one = await (await new TenantTransfer(state, { budgetMs: 1e9 }).fetch(new Request('http://t/check', { method: 'POST', body: JSON.stringify({ fts: ['docs_fts'] }) }))).json() as Record<string, unknown>;
    const stepwise = new TenantTransfer(state, { budgetMs: 0 });
    let cursor: unknown = null, result: Record<string, unknown>, steps = 0;
    do {
      result = await (await stepwise.fetch(new Request('http://t/check', { method: 'POST', body: JSON.stringify({ fts: ['docs_fts'], cursor }) }))).json() as Record<string, unknown>;
      cursor = result.next ?? null; steps += 1;
    } while (cursor);
    expect(steps).toBeGreaterThan(2 * TABLES);
    const { ms: _a, ...a } = one, { ms: _b, ...b } = result;
    expect(b).toEqual(a);
  });
  // The server-side verify, a page of 60 tables at a time, each answered `pending` until done.
  for (let i = 0; i < tables.length; i += 60) {
    const checksums = Object.fromEntries(tables.slice(i, i + 60).map((t) => [t, expected[t]!]));
    while ((await call<{ pending?: boolean }>('verify', { checksums })).pending);
  }
  expect(await call('complete')).toEqual({ ready: true });

  const byOp = new Map<string, number[]>();
  for (const { op, ms } of timings) byOp.set(op, [...(byOp.get(op) ?? []), ms]);
  const summary = Object.fromEntries([...byOp].map(([op, ms]) => [op, { requests: ms.length, maxMs: Math.max(...ms) }]));
  console.log(`X10 transfer requests: ${JSON.stringify(summary)}`);
  // Budgeted operations stop at the budget plus at most one page or table; inserts are bounded by the page size.
  for (const op of ['schema', 'checksum', 'check', 'verify', 'insert']) expect(summary[op]!.maxMs, op).toBeLessThan(2 * REQUEST_BUDGET_MS);
}); 

it('a catalog read while a schema request yields cannot leave plans of the old schema cached', async () => {
  await runInDurableObject(env.TENANT.getByName('devcopy-plans-cache'), async (_instance, state) => {
    const transfer = new TenantTransfer(state, { budgetMs: 1e9 });
    const post = (op: string, body: unknown) => transfer.fetch(new Request(`http://t/${op}`, { method: 'POST', body: JSON.stringify(body) })).then((r) => r.json() as Promise<Record<string, any>>);
    const statements = Array.from({ length: 300 }, (_, i) => ({ name: `s${i}`, sql: `CREATE TABLE s${i} (id INTEGER PRIMARY KEY)` }));
    let done = false;
    const schema = post('schema', { statements }).then((r) => { done = true; return r; });
    let interleaved = 0;
    // Readers run in the schema request's yields, between its transactions.
    while (!done) {
      const { plans } = await post('catalog', {});
      if (!done && plans.length < 300) interleaved += 1;
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect((await schema).applied).toBe(300);
    expect(interleaved).toBeGreaterThan(0);
    const { plans } = await post('catalog', {});
    expect(plans.filter((p: { name: string }) => p.name.startsWith('s'))).toHaveLength(300);
  });
});

it('a resumable checksum is exact over a quiescent tenant and refuses to continue after any write', async () => {
  await runInDurableObject(env.TENANT.getByName('devcopy-quiescent'), async (_instance, state) => {
    const call = (t: InstanceType<typeof TenantTransfer>, op: string, body: unknown) => t.fetch(new Request(`http://t/${op}`, { method: 'POST', body: JSON.stringify(body) }));
    const json = async (r: Response) => ({ status: r.status, body: await r.json() as Record<string, any> });
    const once = new TenantTransfer(state, { budgetMs: 1e9 }), stepwise = new TenantTransfer(state, { budgetMs: 0 });
    expect((await call(once, 'schema', { statements: [{ name: 'q', sql: 'CREATE TABLE q (id INTEGER PRIMARY KEY, v TEXT)' }] })).status).toBe(200);
    const rows = (from: number, n: number) => Array.from({ length: n }, (_, i) => [from + i, `v${from + i}`]);
    expect((await call(once, 'insert', { parts: [{ table: 'q', columns: ['id', 'v'], rows: rows(10, 9_000) }] })).status).toBe(200);
    const reference = (await json(await call(once, 'checksum', { tables: ['q'] }))).body.checksums.q;
    // At budget 0 every page continues; the digest equals the one-pass digest.
    let cursor: unknown = null, pages = 0, result: Record<string, any>;
    do { result = (await json(await call(stepwise, 'checksum', { tables: ['q'], cursor }))).body; cursor = result.next ?? null; pages += 1; } while (cursor);
    expect(pages).toBeGreaterThan(3);
    expect(result.checksums.q).toEqual(reference);
    // A write between two pages: the cursor is refused (a low key behind it would otherwise be missed).
    const first = (await json(await call(stepwise, 'checksum', { tables: ['q'], cursor: null }))).body;
    expect(first.next).toBeTruthy();
    expect((await call(once, 'insert', { parts: [{ table: 'q', columns: ['id', 'v'], rows: rows(1, 1) }] })).status).toBe(200);
    expect(await json(await call(stepwise, 'checksum', { tables: ['q'], cursor: first.next }))).toMatchObject({ status: 409, body: { stale: true } });
    // A write while a checksum request yields between pages: the request is refused, not answered.
    const running = call(new TenantTransfer(state, { budgetMs: 1e9 }), 'checksum', { tables: ['q'] });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect((await call(once, 'insert', { parts: [{ table: 'q', columns: ['id', 'v'], rows: rows(2, 1) }] })).status).toBe(200);
    expect(await json(await running)).toMatchObject({ status: 409, body: { stale: true } });
    // The same for a check cursor.
    const check = (await json(await call(stepwise, 'check', { fts: [] }))).body;
    expect(check.next).toBeTruthy();
    await call(once, 'insert', { parts: [{ table: 'q', columns: ['id', 'v'], rows: rows(3, 1) }] });
    expect(await json(await call(stepwise, 'check', { fts: [], cursor: check.next }))).toMatchObject({ status: 409, body: { stale: true } });
  });
});

it('the assembled path runs checksum, check and verify only while the import is in progress', async () => {
  const tenant = 'devcopy-budget';
  const post = async (op: string, body: unknown = {}) => {
    const request = new Request(`https://dev/__tenant-transfer/t/${tenant}/${op}`, { method: 'POST', headers: { authorization: 'Bearer test-only' }, body: JSON.stringify(body) });
    const response = (await handleTenantTransfer(request, { ...config, TENANT: env.TENANT }))!;
    return { status: response.status, body: await response.json() as Record<string, unknown> };
  };
  // devcopy-budget completed its import in the first test.
  for (const op of ['checksum', 'check', 'verify']) expect(await post(op, op === 'verify' ? { checksums: {} } : { tables: [name(0)] })).toMatchObject({ status: 409, body: { error: `${op} runs only while the import is in progress` } });
});
