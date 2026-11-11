#!/usr/bin/env node
// P03 tenant transfer CLI. See ../README.md.
//
//   tenant-transfer run      --migrations DIR --work DIR [--url URL] SOURCE.db...
//   tenant-transfer push     --tenant ID FILE          (file -> empty object)
//   tenant-transfer pull     --tenant ID FILE          (object -> new file)
//   tenant-transfer verify   --tenant ID FILE          (checksums + schema, object vs file)
//   tenant-transfer checksum FILE                      (per-table checksums of a file)
//   tenant-transfer wipe     --tenant ID
//
// The object endpoint is --url (default http://localhost:8799, wrangler dev).
// The bearer token comes from $TRANSFER_TOKEN or --token-file (a .dev.vars
// style file with TRANSFER_TOKEN=...); it is never printed.

import { execFile } from 'node:child_process';
import { existsSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs, promisify } from 'node:util';
import {
  ObjectClient,
  compareChecksums,
  compareSchemas,
  fileChecksums,
  fileSchema,
  objectChecksums,
  pullFile,
  pushFile,
} from '../src/transfer.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const run = promisify(execFile);

const { values: opts, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    url: { type: 'string', default: 'http://localhost:8799' },
    'token-file': { type: 'string', default: join(here, '..', '.dev.vars') },
    tenant: { type: 'string' },
    migrations: { type: 'string' },
    work: { type: 'string' },
    'stage-bin': { type: 'string', default: process.env.GONVEX_TENANT_STAGE ?? 'gonvex-tenant-stage' },
    'keep-epoch': { type: 'boolean', default: false },
    'keep-object': { type: 'boolean', default: false },
    'keep-files': { type: 'boolean', default: false },
    prefix: { type: 'string', default: 'p03-' },
    report: { type: 'string' },
    quiet: { type: 'boolean', default: false },
  },
});

const log = opts.quiet ? () => {} : (msg) => process.stderr.write(`${msg}\n`);

function token() {
  if (process.env.TRANSFER_TOKEN) return process.env.TRANSFER_TOKEN;
  const file = opts['token-file'];
  if (existsSync(file)) {
    const match = readFileSync(file, 'utf8').match(/^TRANSFER_TOKEN=(.+)$/m);
    if (match) return match[1].trim();
  }
  throw new Error('no TRANSFER_TOKEN (set it in the environment or --token-file)');
}

const client = (tenant) => new ObjectClient({ url: opts.url, token: token(), tenant });
const size = (path) => (existsSync(path) ? statSync(path).size : 0);
const removeDb = (path) => ['', '-wal', '-shm', '-journal'].forEach((s) => rmSync(path + s, { force: true }));

async function stage(source, out, { keepEpoch }) {
  const args = ['stage', '--source', source, '--migrations', resolve(opts.migrations), '--out', out];
  if (opts.work) args.push('--template', join(resolve(opts.work), 'template.db'));
  if (keepEpoch) args.push('--keep-epoch');
  const started = Date.now();
  let stdout;
  try {
    ({ stdout } = await run(opts['stage-bin'], args, { maxBuffer: 64 * 1024 * 1024 }));
  } catch (error) {
    if (error.code === 2 && error.stdout) stdout = error.stdout; // findings: report below
    else throw new Error(`stage ${basename(source)}: ${error.stderr || error.message}`);
  }
  const report = JSON.parse(stdout);
  return { report, ms: Date.now() - started };
}

/** Object vs file: every table's checksum, the schema, and the object's own checks. */
async function verifyObject(c, path, { fts = [] } = {}) {
  const fileSums = fileChecksums(path);
  const objectSums = await objectChecksums(c, Object.keys(fileSums));
  const catalog = await c.call('catalog');
  const schema = compareSchemas(catalog.schema, fileSchema(path));
  const extraTables = catalog.plans.map((p) => p.name).filter((n) => !(n in fileSums));
  const checks = await c.call('check', { fts });
  return { checksums: compareChecksums(fileSums, objectSums), schema, extraTables, checks, fileSums, objectSums };
}

async function runOne(source) {
  const work = resolve(opts.work);
  const name = basename(source).replace(/\.db$/, '');
  const tenant = `${opts.prefix}${name}`;
  const files = { staged: join(work, `${name}.in.db`), raw: join(work, `${name}.raw.db`), out: join(work, `${name}.out.db`) };
  Object.values(files).forEach(removeDb);
  const c = client(tenant);
  const result = { tenant: name, object: tenant, sourceBytes: size(source), findings: [] };
  log(`== ${name} (${(result.sourceBytes / 1e6).toFixed(1)} MB)`);

  // Import: source -> runtime-provisioned file (doc rules) -> object.
  const stageIn = await stage(resolve(source), files.staged, { keepEpoch: opts['keep-epoch'] });
  result.stageIn = { ms: stageIn.ms, provisionMs: stageIn.report.provisionMs, copyMs: stageIn.report.copyMs, clock: stageIn.report.clock };
  result.findings.push(...stageIn.report.findings.map((f) => `stage in: ${f}`));
  const copied = Object.keys(stageIn.report.copiedTables);
  result.stagedBytes = size(files.staged);

  const sourceSums = fileChecksums(source, copied);
  const stagedSums = fileChecksums(files.staged);
  const sourceVsStaged = compareChecksums(sourceSums, stagedSums, copied);

  await c.call('wipe');
  const push = await pushFile(c, files.staged, { log });
  result.push = push;
  const objectStatus = await c.call('status');
  result.objectBytes = objectStatus.databaseSize;
  const inCheck = await verifyObject(c, files.staged, { fts: push.fts });
  result.importVerify = {
    sourceVsStaged: { tables: sourceVsStaged.tables, rows: sourceVsStaged.rows, mismatches: sourceVsStaged.mismatches },
    stagedVsObject: { tables: inCheck.checksums.tables, rows: inCheck.checksums.rows, mismatches: inCheck.checksums.mismatches },
    schema: inCheck.schema,
    extraTables: inCheck.extraTables,
    checks: inCheck.checks,
  };
  // Copied tables, source vs object directly.
  const sourceVsObject = compareChecksums(sourceSums, inCheck.objectSums, copied);
  result.importVerify.sourceVsObject = { tables: sourceVsObject.tables, rows: sourceVsObject.rows, mismatches: sourceVsObject.mismatches };

  // Export: object -> verbatim file -> runtime-provisioned file (doc rules), accepted by the installer.
  const pullStarted = Date.now();
  const pull = await pullFile(c, files.raw, { log });
  result.pull = pull;
  const rawSums = fileChecksums(files.raw);
  const objectVsRaw = compareChecksums(inCheck.objectSums, rawSums);
  const stageOut = await stage(files.raw, files.out, { keepEpoch: opts['keep-epoch'] });
  result.stageOut = { ms: stageOut.ms, provisionMs: stageOut.report.provisionMs, copyMs: stageOut.report.copyMs, clock: stageOut.report.clock, accept: stageOut.report.accept };
  result.findings.push(...stageOut.report.findings.map((f) => `stage out: ${f}`));
  result.outBytes = size(files.out);
  const outSums = fileChecksums(files.out);
  const exportCopied = Object.keys(stageOut.report.copiedTables);
  const rawVsOut = compareChecksums(rawSums, outSums, exportCopied);
  const roundTrip = compareChecksums(sourceSums, outSums, copied);
  result.exportVerify = {
    objectVsRaw: { tables: objectVsRaw.tables, rows: objectVsRaw.rows, mismatches: objectVsRaw.mismatches },
    rawVsOut: { tables: rawVsOut.tables, rows: rawVsOut.rows, mismatches: rawVsOut.mismatches },
    sourceVsOut: { tables: roundTrip.tables, rows: roundTrip.rows, mismatches: roundTrip.mismatches },
  };
  result.importMs = stageIn.ms + push.timings.totalMs;
  result.exportMs = Date.now() - pullStarted;

  // Change-feed semantics across the round trip.
  const srcClock = stageIn.report.clock;
  const outClock = stageOut.report.clock;
  if (outClock.revision !== srcClock.sourceRevision) result.findings.push(`clock revision ${srcClock.sourceRevision} -> ${outClock.revision}`);
  if (outClock.retainedRevision !== outClock.revision) result.findings.push('retained_revision != revision after export');
  if (!opts['keep-epoch'] && (outClock.epoch === srcClock.sourceEpoch || srcClock.epoch === srcClock.sourceEpoch)) result.findings.push('epoch was not renewed');

  for (const [label, cmp] of Object.entries({ ...result.importVerify, ...result.exportVerify })) {
    if (cmp?.mismatches?.length) result.findings.push(`${label}: ${cmp.mismatches.length} table(s) differ (${cmp.mismatches.slice(0, 5).map((m) => m.table).join(', ')})`);
  }
  if (inCheck.extraTables.length) result.findings.push(`object has tables the file lacks: ${inCheck.extraTables.join(', ')}`);
  if (inCheck.checks.quickCheck !== 'ok') result.findings.push(`object quick_check: ${inCheck.checks.quickCheck}`);
  if (inCheck.checks.foreignKeyViolations) result.findings.push(`object foreign_key_check: ${inCheck.checks.foreignKeyViolations}`);
  for (const [fts, status] of Object.entries(inCheck.checks.fts)) if (status !== 'ok') result.findings.push(`object ${fts}: ${status}`);
  // Statements the platform refused are a platform-limit finding, reported, not worked around.
  result.platformRefused = push.schemaFailures.map((f) => ({ name: f.name, bytes: f.bytes, error: f.error }));
  const refused = new Set(push.schemaFailures.map((f) => f.name));
  const unexplained = inCheck.schema.missingInObject.filter((n) => !refused.has(n.split(' ').slice(1).join(' ')));
  if (unexplained.length || inCheck.schema.extraInObject.length) {
    result.findings.push(`schema differs beyond refused statements: missing ${unexplained.join(', ')}; extra ${inCheck.schema.extraInObject.join(', ')}`);
  }

  if (!opts['keep-object']) await c.call('wipe');
  if (!opts['keep-files']) Object.values(files).forEach(removeDb);
  result.requests = c.requests;
  result.bytesSent = c.bytesSent;
  result.bytesReceived = c.bytesReceived;
  result.ok = result.findings.length === 0;
  log(`   import ${(result.importMs / 1000).toFixed(1)} s, export ${(result.exportMs / 1000).toFixed(1)} s, rows ${push.rows}, ok=${result.ok}${result.platformRefused.length ? `, refused ${result.platformRefused.length}` : ''}`);
  return result;
}

async function main() {
  const [command, ...rest] = positionals;
  switch (command) {
    case 'run': {
      if (!opts.migrations || !opts.work || !rest.length) throw new Error('run needs --migrations, --work and source files');
      const results = [];
      for (const source of rest) {
        try {
          results.push(await runOne(source));
        } catch (error) {
          results.push({ tenant: basename(source), ok: false, error: String(error?.stack ?? error) });
          log(`   FAILED: ${error?.message ?? error}`);
        }
        if (opts.report) writeFileSync(opts.report, JSON.stringify({ url: opts.url, results }, null, 2));
      }
      console.log(JSON.stringify(results.map(summary), null, 2));
      if (results.some((r) => !r.ok)) process.exitCode = 2;
      return;
    }
    case 'push':
      console.log(JSON.stringify(await pushFile(client(opts.tenant), resolve(rest[0]), { log }), null, 2));
      return;
    case 'pull':
      console.log(JSON.stringify(await pullFile(client(opts.tenant), resolve(rest[0]), { log }), null, 2));
      return;
    case 'verify': {
      const v = await verifyObject(client(opts.tenant), resolve(rest[0]));
      delete v.fileSums;
      delete v.objectSums;
      console.log(JSON.stringify(v, null, 2));
      if (v.checksums.mismatches.length) process.exitCode = 2;
      return;
    }
    case 'checksum':
      console.log(JSON.stringify(fileChecksums(resolve(rest[0])), null, 2));
      return;
    case 'wipe':
      console.log(JSON.stringify(await client(opts.tenant).call('wipe')));
      return;
    default:
      throw new Error('usage: tenant-transfer run|push|pull|verify|checksum|wipe (see bin/tenant-transfer.mjs)');
  }
}

function summary(r) {
  if (r.error) return { tenant: r.tenant, ok: false, error: r.error.split('\n')[0] };
  return {
    tenant: r.tenant,
    ok: r.ok,
    sourceMB: +(r.sourceBytes / 1e6).toFixed(1),
    tables: r.push.tables,
    rows: r.push.rows,
    importS: +(r.importMs / 1000).toFixed(1),
    objectMB: +(r.objectBytes / 1e6).toFixed(1),
    exportS: +(r.exportMs / 1000).toFixed(1),
    outMB: +(r.outBytes / 1e6).toFixed(1),
    refused: r.platformRefused.map((f) => f.name),
    findings: r.findings,
  };
}

main().catch((error) => {
  console.error(error?.stack ?? String(error));
  process.exit(1);
});
