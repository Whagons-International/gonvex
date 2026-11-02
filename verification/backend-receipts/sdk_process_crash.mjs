import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { ExpoSQLiteLocalReplicaStorage, ExpoSQLiteOutboxStore } from '../../packages/expo-sqlite/dist/index.js';

const script = fileURLToPath(import.meta.url);
const deadlineMs = 20_000; // Same progress bound as the ordinary SDK fixture.
async function until(check, label, child) {
  const deadline = Date.now() + deadlineMs;
  while (!await check()) {
    assert(!child || (child.exitCode === null && child.signalCode === null), `SDK process exited before ${label}`);
    assert(Date.now() < deadline, `SDK process crash fixture did not reach ${label}`);
    await delay(10);
  }
}

function startChild(url, output, phase, boundary) {
  const child = spawn(process.execPath, [script, '--url', url, '--output', output, '--phase', phase, '--boundary', boundary],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '', stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  const exited = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => resolve({ code, signal }));
  });
  return { child, exited, logs: () => ({ stdout, stderr }) };
}

/** Kill only a child created here, after independent readers witness the exact
 * journal/replica boundary. Neither the SDK nor SQLite gets graceful cleanup. */
export async function runSDKProcessCrashes(url, output) {
  await mkdir(output, { recursive: false });
  const cases = [];
  for (const boundary of ['after-acceptance-commit', 'before-acceptance-commit']) {
    const directory = join(output, boundary);
    await mkdir(directory, { recursive: false });
    const first = startChild(url, directory, 'crash', boundary);
    try {
      let witness;
      await until(async () => {
        try { witness = JSON.parse(await readFile(join(directory, 'boundary.json'), 'utf8')); return true; }
        catch (error) { if (error.code === 'ENOENT' || error instanceof SyntaxError) return false; throw error; }
      }, boundary, first.child);
      assert.equal(witness.pid, first.child.pid, 'boundary did not identify the owned SDK process');
      assert.equal(witness.boundary, boundary);
      assert.equal(witness.replicaRow, null, 'crash boundary already contained the business row');
      assert.equal(witness.durableState, boundary === 'after-acceptance-commit' ? 'committed' : 'inflight');
      assert.equal(witness.outcomePending, true, 'SDK published success before crash boundary');
      assert(witness.committedRevision > witness.replicaCursor.revision);
      assert.equal(first.child.exitCode, null);
      assert.equal(first.child.signalCode, null);
      assert(first.child.kill('SIGKILL'), 'owned SDK process could not be killed');
      const exit = await first.exited;
      assert.equal(exit.signal, 'SIGKILL', 'SDK did not die from the required process kill');
      await writeFile(join(directory, 'crashed-process.log'), JSON.stringify(first.logs()) + '\n');
      const recovered = startChild(url, directory, 'resume', boundary);
      try {
        const result = await recovered.exited;
        await writeFile(join(directory, 'recovery-process.log'), JSON.stringify(recovered.logs()) + '\n');
        assert.equal(result.code, 0, 'SDK process recovery failed:\n' + recovered.logs().stderr);
        const recovery = JSON.parse(await readFile(join(directory, 'recovery.json'), 'utf8'));
        assert.equal(recovery.reducerCalls.length, 0, 'process recovery resent an already accepted intent');
        assert(recovery.receiptQueries.length > 0 && recovery.retirements.length > 0);
        assert(recovery.receiptQueries.every(query => query.args.idempotencyKey === witness.intent.idempotencyKey
          && query.args.receiptPath === witness.intent.path), 'process recovery changed receipt identity');
        cases.push({ boundary, killedPid: first.child.pid, killSignal: exit.signal, witness, recovery });
      } finally {
        if (recovered.child.exitCode === null && recovered.child.signalCode === null) recovered.child.kill('SIGKILL');
        await recovered.exited;
      }
    } catch (error) {
      await writeFile(join(directory, 'failed-process.log'), JSON.stringify(first.logs()) + '\n');
      throw new Error(error.message + '\n' + first.logs().stderr, { cause: error });
    } finally {
      if (first.child.exitCode === null && first.child.signalCode === null) first.child.kill('SIGKILL');
      await first.exited;
    }
  }
  await writeFile(join(output, 'receipt.json'), JSON.stringify({ cases,
    boundary: 'Owned Node SDK processes killed with SIGKILL before/after SQLite acceptance COMMIT. Real authenticated backend and fresh-process SQLite recovery. No native phone or machine power-loss claim.' }, null, 2) + '\n');
  return cases;
}

async function childMain(args) {
  const url = args.get('--url'), output = args.get('--output'), phase = args.get('--phase'), boundary = args.get('--boundary');
  assert(url?.startsWith('ws://127.0.0.1:') && output && ['crash', 'resume'].includes(phase));
  assert(['after-acceptance-commit', 'before-acceptance-commit'].includes(boundary));
  const { GonvexClient } = await import(process.env.GONVEX_SDK_RECEIPT_CLIENT_MODULE
    ? pathToFileURL(process.env.GONVEX_SDK_RECEIPT_CLIENT_MODULE).href
    : new URL('../../packages/client/dist/index.js', import.meta.url).href);
  const rowId = 'sdk-process-' + boundary;
  const db = new DatabaseSync(join(output, 'replica.sqlite'));
  const journal = new DatabaseSync(join(output, 'journal.sqlite'));
  for (const connection of [db, journal]) connection.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
  const replicaReader = new DatabaseSync(join(output, 'replica.sqlite'), { readOnly: true });
  const journalReader = new DatabaseSync(join(output, 'journal.sqlite'), { readOnly: true });
  let client, scope, committedRevision = 0, held = false, outcomePending = true, damageDone = false, retirementError;
  const reducerCalls = [], receiptQueries = [], retirements = [];
  const NativeSocket = globalThis.WebSocket;
  class Socket extends NativeSocket {
    send(data) {
      const message = JSON.parse(data);
      reducerCalls.push(...(message.type === 'reducer.callMany' ? message.calls : message.type === 'reducer.call' ? [message] : []));
      if (message.type === 'query.call' && message.path === '__gonvex.reducerReceipt') receiptQueries.push(message);
      super.send(data);
    }
    addEventListener(type, listener, options) {
      if (type !== 'message') return super.addEventListener(type, listener, options);
      return super.addEventListener(type, event => {
        const message = JSON.parse(event.data);
        if (message.type === 'auth.result') scope = message.result.replica.visibilityScope;
        if (message.type === 'reducer.result') committedRevision = message.committedRevision;
        // The real accepted result reaches normal SDK dispatch, but its replica
        // frames stay undelivered until this process is killed.
        if (phase === 'crash' && reducerCalls.length > 0 && message.type.startsWith('replica.')) return;
        if (typeof listener === 'function') listener.call(this, event); else listener.handleEvent(event);
      }, options);
    }
  }
  globalThis.WebSocket = Socket;
  async function holdBoundary() {
    held = true;
    const intent = JSON.parse(journal.prepare('SELECT value FROM _gonvex_outbox').get().value);
    const durable = JSON.parse(journalReader.prepare('SELECT value FROM _gonvex_outbox').get().value);
    const cursor = JSON.parse(replicaReader.prepare("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='cursor'").get(scope).value);
    const row = replicaReader.prepare('SELECT value FROM _gonvex_replica_entities WHERE scope=? AND entity=? AND id=?').get(scope, 'receipt_records', rowId);
    await writeFile(join(output, 'boundary.json'), JSON.stringify({ pid: process.pid, boundary,
      committedRevision, intent, durableState: durable.state, replicaCursor: cursor,
      replicaRow: row ?? null, outcomePending }, null, 2) + '\n');
    await new Promise(() => {});
  }
  const adapter = connection => ({
    async execAsync(sql) { connection.exec(sql); },
    async runAsync(sql, ...params) { return connection.prepare(sql).run(...params); },
    async getAllAsync(sql, ...params) { return connection.prepare(sql).all(...params); },
    async getFirstAsync(sql, ...params) { return connection.prepare(sql).get(...params) ?? null; },
    async withTransactionAsync(run) {
      connection.exec('BEGIN');
      let committed = false;
      try {
        await run();
        const acceptance = phase === 'crash' && connection === journal && !held && committedRevision > 0
          && connection.prepare("SELECT 1 FROM _gonvex_outbox WHERE state='committed'").get();
        if (acceptance && boundary === 'before-acceptance-commit') await holdBoundary();
        connection.exec('COMMIT');
        committed = true;
        if (acceptance && boundary === 'after-acceptance-commit') await holdBoundary();
      } catch (error) { if (!committed) connection.exec('ROLLBACK'); throw error; }
    },
  });
  const storage = new ExpoSQLiteLocalReplicaStorage(adapter(db));
  const store = new ExpoSQLiteOutboxStore(adapter(journal));
  const remove = store.delete.bind(store), removeMany = store.deleteMany.bind(store);
  const witness = phase === 'resume' ? JSON.parse(await readFile(join(output, 'boundary.json'), 'utf8')) : undefined;
  function verifyRetirement(ids) {
    if (process.env.GONVEX_SDK_PROCESS_DAMAGE === 'missing-row' && !damageDone) {
      damageDone = true;
      db.prepare('DELETE FROM _gonvex_replica_entities WHERE scope=? AND entity=? AND id=?').run(scope, 'receipt_records', rowId);
    }
    replicaReader.exec('BEGIN');
    try {
      const row = replicaReader.prepare('SELECT value FROM _gonvex_replica_entities WHERE scope=? AND entity=? AND id=?').get(scope, 'receipt_records', rowId);
      assert.deepEqual(row && JSON.parse(row.value), { id: rowId, value: 7 }, 'process recovery retirement passed durable replica data');
      const cursor = JSON.parse(replicaReader.prepare("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='cursor'").get(scope).value);
      assert(cursor.revision >= witness.committedRevision, 'process recovery retirement passed durable cursor');
      const windows = replicaReader.prepare('SELECT value FROM _gonvex_replica_queries WHERE scope=?').all(scope).map(row => JSON.parse(row.value));
      assert(windows.some(window => window.entity === 'receipt_records' && window.ids.includes(rowId)), 'process recovery retirement passed durable membership');
      retirements.push({ ids, cursor, row: JSON.parse(row.value) });
    } catch (error) {
      // SDK recovery correctly retains the journal if deletion fails. Keep
      // the oracle's actual assertion so the control cannot pass by timeout.
      retirementError = error;
      throw error;
    } finally { replicaReader.exec('COMMIT'); }
  }
  store.delete = async id => { verifyRetirement([id]); await remove(id); };
  store.deleteMany = async ids => { if (ids.length) verifyRetirement(ids); await removeMany(ids); };
  try {
    client = new GonvexClient(url, { project: 'project', tenant: 'tenant', token: 'gvx_session_receipt_sdk_fixture',
      identity: { iss: 'receipt-fixture', sub: 'account' }, errorReporting: false, replicaFieldPatches: false,
      localReplica: { storage }, outbox: { store } });
    const watch = client.watchReplica({ kind: 'query', path: 'all', delivery: 'replica', replica: {
      table: 'receipt_records', key: 'id', columns: ['id', 'value'], maxRows: 100,
    } }, {});
    client.connect();
    if (phase === 'crash') {
      await until(() => watch.status().isUpToDate && scope, 'authenticated crash child');
      void client.reducer({ kind: 'reducer', path: 'write', offline: { mode: 'allowed' } }, { id: rowId, value: 7 }, { offline: 'queue' })
        .finally(() => { outcomePending = false; }).catch(() => undefined);
      await new Promise(() => {});
    } else {
      await until(async () => {
        if (retirementError) throw retirementError;
        return watch.status().isUpToDate && (await store.load()).length === 0;
      }, 'fresh-process recovery');
      assert.equal(db.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
      assert.equal(journal.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
      await writeFile(join(output, 'recovery.json'), JSON.stringify({ reducerCalls, receiptQueries, retirements,
        cursor: (await storage.load(scope)).cursor, sqlite: db.prepare('SELECT sqlite_version() AS version').get().version }, null, 2) + '\n');
    }
  } finally {
    client?.close(); if (client) await client.replicaFrames;
    replicaReader.close(); journalReader.close(); journal.close(); db.close(); globalThis.WebSocket = NativeSocket;
  }
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  const args = new Map();
  for (let index = 2; index < process.argv.length; index += 2) args.set(process.argv[index], process.argv[index + 1]);
  await childMain(args);
}
