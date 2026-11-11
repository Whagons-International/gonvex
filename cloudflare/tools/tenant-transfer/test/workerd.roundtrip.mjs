// Round trip of a generated fixture through a real Durable Object in local
// workerd (wrangler dev): file -> object -> file, checksums and schema equal
// in both directions, object checks clean, FTS still answers, and the
// platform's SQLite limits are the ones P03 reports.
//
//   node --test test/workerd.roundtrip.mjs      (pnpm test:workerd)

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { after, before, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { openFile } from '../src/file.mjs';
import { ObjectClient, compareChecksums, compareSchemas, fileChecksums, fileSchema, objectChecksums, pullFile, pushFile } from '../src/transfer.mjs';
import { writeFixture } from './fixture.mjs';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const dir = mkdtempSync(join(tmpdir(), 'p03-workerd-'));
const port = 20000 + Math.floor(Math.random() * 20000);
const token = randomBytes(16).toString('hex');
let server;

before(async () => {
  server = spawn(
    join(root, 'node_modules', '.bin', 'wrangler'),
    ['dev', '--local', '--port', String(port), '--inspector-port', String(port + 1), '--persist-to', join(dir, 'state'), '--var', `TRANSFER_TOKEN:${token}`],
    { cwd: root, stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, WRANGLER_SEND_METRICS: 'false' } },
  );
  let output = '';
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`wrangler dev did not start:\n${output}`)), 60_000);
    const onData = (chunk) => {
      output += chunk;
      if (/Ready on/.test(output)) {
        clearTimeout(timer);
        resolve();
      }
    };
    server.stdout.on('data', onData);
    server.stderr.on('data', onData);
    server.on('exit', (code) => reject(new Error(`wrangler dev exited ${code}:\n${output}`)));
  });
});

after(() => {
  server?.kill('SIGTERM');
  rmSync(dir, { recursive: true, force: true });
});

const client = (tenant) => new ObjectClient({ url: `http://127.0.0.1:${port}`, token, tenant });

test('fixture round trip is lossless', async () => {
  const source = join(dir, 'fixture.db');
  const back = join(dir, 'back.db');
  writeFixture(source);
  const c = client('fixture');
  await c.call('wipe');

  const push = await pushFile(c, source);
  assert.deepEqual(push.schemaFailures, []);
  const fileSums = fileChecksums(source);
  const objectSums = await objectChecksums(c, Object.keys(fileSums));
  assert.deepEqual(compareChecksums(fileSums, objectSums).mismatches, []);
  const catalog = await c.call('catalog');
  assert.deepEqual(compareSchemas(catalog.schema, fileSchema(source)), { missingInObject: [], extraInObject: [] });
  assert.deepEqual(catalog.sequences, { seq: 1000 });
  const checks = await c.call('check', { fts: ['docs_fts'] });
  assert.equal(checks.quickCheck, 'ok');
  assert.equal(checks.foreignKeyViolations, 0);
  assert.equal(checks.fts.docs_fts, 'ok');
  const hits = await c.call('exec', { sql: "SELECT count(*) AS n FROM docs_fts WHERE docs_fts MATCH 'number 12'" });
  const file = openFile(source, { readOnly: true });
  const expected = file.sql.exec("SELECT count(*) AS n FROM docs_fts WHERE docs_fts MATCH 'number 12'").one().n;
  file.close();
  assert.equal(hits.rows[0].n, expected);
  // Copied rows fired no trigger: the projection equals the source exactly.
  assert.equal(objectSums.parent_counts.sha256, fileSums.parent_counts.sha256);
  // Enforcement is back on after the import.
  const fk = await c.call('exec', { sql: 'PRAGMA foreign_keys' });
  assert.equal(fk.rows[0].foreign_keys, 1);

  await pullFile(c, back);
  assert.deepEqual(compareChecksums(objectSums, fileChecksums(back)).mismatches, []);
  assert.deepEqual(compareSchemas(fileSchema(back), fileSchema(source)), { missingInObject: [], extraInObject: [] });
  await c.call('wipe');
  assert.equal((await c.call('status')).objects, 0);
});

test('push refuses a non-empty object', async () => {
  const source = join(dir, 'small.db');
  writeFixture(source, { rows: 5 });
  const c = client('nonempty');
  await c.call('wipe');
  await pushFile(c, source);
  await assert.rejects(pushFile(c, source), /not empty/);
  await c.call('wipe');
});

test('the object refuses the statements P03 reports, and nothing else', async () => {
  const c = client('limits');
  await c.call('wipe');
  const union = (n) => Array.from({ length: n }, (_, i) => `SELECT ${i} AS x`).join(' UNION ALL ');
  const nest = (n) => `${'abs('.repeat(n)}1${')'.repeat(n)}`;
  const values = (bytes) => {
    let sql = 'CREATE VIEW long AS SELECT * FROM (VALUES (0)';
    for (let i = 1; sql.length + 12 < bytes; i++) sql += `,(${String(i).padStart(7, '0')})`;
    return `${sql})`.padEnd(bytes, ' ');
  };
  const result = await c.call('schema', {
    statements: [
      { name: 'compound5', sql: `CREATE VIEW compound5 AS ${union(5)}` },
      { name: 'compound6', sql: `CREATE VIEW compound6 AS ${union(6)}` },
      { name: 'depth99', sql: `CREATE VIEW depth99 AS SELECT ${nest(99)} AS x` },
      { name: 'depth100', sql: `CREATE VIEW depth100 AS SELECT ${nest(100)} AS x` },
      { name: 'len99000', sql: values(99_000) },
      { name: 'len100000', sql: values(100_000).replace('VIEW long', 'VIEW long2') },
    ],
  });
  assert.deepEqual(result.failed.map((f) => f.name), ['compound6', 'depth100', 'len100000']);
  assert.match(result.failed[0].error, /too many terms in compound SELECT/);
  assert.match(result.failed[1].error, /Expression tree is too large/);
  assert.match(result.failed[2].error, /TOOBIG/);
  await c.call('wipe');
});
