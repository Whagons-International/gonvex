import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, readFile, writeFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { ExpoSQLiteLocalReplicaStorage, ExpoSQLiteOutboxStore } from '../../packages/expo-sqlite/dist/index.js';

async function until(check, label) {
  const deadline = Date.now() + 20_000;
  while (!await check()) { assert(Date.now() < deadline, 'Concurrent SDK fixture did not reach ' + label); await delay(10); }
}
async function jsonWhenReady(path, label) {
  let value;
  await until(async () => {
    try { value = JSON.parse(await readFile(path, 'utf8')); return true; }
    catch (error) { if (error.code === 'ENOENT' || error instanceof SyntaxError) return false; throw error; }
  }, label); return value;
}
export async function runSDKConcurrency(url, output) {
  await mkdir(output, { recursive: false });
  const { GonvexClient } = await import(process.env.GONVEX_SDK_RECEIPT_CLIENT_MODULE
    ? pathToFileURL(process.env.GONVEX_SDK_RECEIPT_CLIENT_MODULE).href
    : new URL('../../packages/client/dist/index.js', import.meta.url).href);
  const cases = [];
  for (const mode of ['commit', 'rollback']) {
    const directory = join(output, mode); await mkdir(directory, { recursive: false });
    const rowId = 'sdk-concurrent-' + mode, clients = [], connections = [], states = [];
    const NativeSocket = globalThis.WebSocket;
    let constructing;
    class Socket extends NativeSocket {
      constructor(...args) { super(...args); this.label = constructing; }
      send(data) {
        const message = JSON.parse(data);
        const calls = message.type === 'reducer.callMany' ? message.calls : message.type === 'reducer.call' ? [message] : [];
        states[this.label].calls.push(...calls); super.send(data);
      }
      addEventListener(type, listener, options) {
        if (type !== 'message') return super.addEventListener(type, listener, options);
        return super.addEventListener(type, event => {
          const message = JSON.parse(event.data);
          if (message.type === 'auth.result') states[this.label].scope = message.result.replica.visibilityScope;
          if (['reducer.result', 'reducer.error'].includes(message.type)) states[this.label].results.push(message);
          if (typeof listener === 'function') listener.call(this, event); else listener.handleEvent(event);
        }, options);
      }
    }
    globalThis.WebSocket = Socket;
    const open = label => {
      const db = new DatabaseSync(join(directory, `replica-${label}.sqlite`));
      const journal = new DatabaseSync(join(directory, `journal-${label}.sqlite`));
      for (const connection of [db, journal]) connection.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
      connections.push(db, journal);
      const adapter = connection => ({
        async execAsync(sql) { connection.exec(sql); },
        async runAsync(sql, ...params) { return connection.prepare(sql).run(...params); },
        async getAllAsync(sql, ...params) { return connection.prepare(sql).all(...params); },
        async getFirstAsync(sql, ...params) { return connection.prepare(sql).get(...params) ?? null; },
        async withTransactionAsync(run) { connection.exec('BEGIN'); try { await run(); connection.exec('COMMIT'); } catch (error) { connection.exec('ROLLBACK'); throw error; } },
      });
      const state = { calls: [], results: [], scope: undefined, store: new ExpoSQLiteOutboxStore(adapter(journal)),
        storage: new ExpoSQLiteLocalReplicaStorage(adapter(db)), db, journal };
      states.push(state); return state;
    };
    const create = state => {
      constructing = clients.length;
      const client = new GonvexClient(url, { project: 'project', tenant: 'tenant', token: 'gvx_session_receipt_sdk_fixture',
        identity: { iss: 'receipt-fixture', sub: 'account' }, errorReporting: false, replicaFieldPatches: false,
        localReplica: { storage: state.storage }, outbox: { store: state.store } });
      clients.push(client); return client;
    };
    try {
      const a = open(0), first = create(a);
      const queued = await first.reducer({ kind: 'reducer', path: 'write', offline: { mode: 'allowed' } },
        { id: rowId, value: 7 }, { offline: 'queue' });
      assert.equal(queued.status, 'queued');
      const [original] = await a.store.load();
      const b = open(1); await b.store.put(structuredClone(original));
      const second = create(b);
      await writeFile(join(directory, 'request.tmp'), JSON.stringify({ mode, key: original.idempotencyKey, rowId }) + '\n');
      await rename(join(directory, 'request.tmp'), join(directory, 'request.json'));
      await jsonWhenReady(join(directory, 'prepared.json'), 'backend gate');
      const watches = clients.map((client, label) => {
        constructing = label;
        return client.watchReplica({ kind: 'query', path: 'all', delivery: 'replica', replica: {
          table: 'receipt_records', key: 'id', columns: ['id', 'value'], maxRows: 100,
        } }, {});
      });
      const witness = await jsonWhenReady(join(directory, 'witness.json'), 'queued claims behind the runtime writer');
      // The backend held the runtime's single tenant writer in a real
      // transaction while both SDK reducers were admitted and queued behind it
      // (sustained, not sampled once); reads kept working and saw nothing of
      // the intent. A second writer would have let a claimant commit.
      assert(witness.overlap === true && witness.gate === 'runtime-writer' && witness.writerOwned === true
        && witness.queuedWriters >= 2 && witness.readDuringWrite === true && witness.rowsWhileHeld === 0,
        'missing witnessed receipt overlap');
      if (mode === 'rollback') assert.equal(witness.ownerRolledBack, true, 'first owner did not roll back its claim');
      await until(async () => watches.every(watch => watch.status().isUpToDate)
        && (await Promise.all(states.map(state => state.store.load()))).every(entries => entries.length === 0), 'both durable retirements');
      for (const state of states) {
        assert.equal(state.calls.length, 1, 'SDK repeated accepted concurrent work');
        assert.equal(state.calls[0].idempotencyKey, original.idempotencyKey);
        assert.deepEqual(state.calls[0].args, original.args);
        const saved = await state.storage.load(state.scope);
        assert.deepEqual(saved.entities.receipt_records[rowId], { id: rowId, value: 7 });
        assert(Object.values(saved.liveQueries).some(window => window.ids.includes(rowId)), 'concurrent retirement lost durable membership');
        assert.equal(state.db.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
      }
      // Neither claimant was dropped: one executed, the other replayed its receipt
      // (after a rolled-back owner, the executing claimant took over).
      assert(states.every(state => state.results.some(result => result.type === 'reducer.result')), 'a queued claimant got no result');
      assert(!states.some(state => state.results.some(result => result.type === 'reducer.error')), 'a queued claimant failed');
      const revisions = new Set(states.map(state => state.results.find(result => result.type === 'reducer.result').committedRevision));
      assert.equal(revisions.size, 1, 'competing claims committed the intent more than once');
      const result = { mode, original, witness, clients: states.map(state => ({ calls: state.calls, results: state.results })) };
      cases.push(result); await writeFile(join(directory, 'receipt.json'), JSON.stringify(result, null, 2) + '\n');
    } catch (error) {
      await writeFile(join(directory, 'failure.json'), JSON.stringify({ error: error.message,
        clients: states.map(state => ({ calls: state.calls, results: state.results,
          journal: state.journal.prepare('SELECT value FROM _gonvex_outbox').all().map(row => JSON.parse(row.value)) })) }, null, 2) + '\n');
      throw error;
    } finally {
      for (const client of clients) client.close();
      await Promise.all(clients.map(client => client.replicaFrames));
      for (const connection of connections) connection.close(); globalThis.WebSocket = NativeSocket;
    }
  }
  await writeFile(join(output, 'receipt.json'), JSON.stringify({ cases,
    boundary: 'Two real SDKs, independent SQLite journals/replicas and one immutable logical intent. The runtime\'s own single tenant writer is held in a real transaction while both reducers are admitted and queue behind it; reads continue and see nothing uncommitted. Commit: one claimant executes, the other replays. Rollback: a first owner that claimed the receipt and wrote the row rolls back, and a claimant takes over. No arbitrary shared-store coordination claim.' }, null, 2) + '\n');
}
