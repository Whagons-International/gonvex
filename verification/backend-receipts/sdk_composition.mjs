import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { ExpoSQLiteLocalReplicaStorage, ExpoSQLiteOutboxStore } from '../../packages/expo-sqlite/dist/index.js';
import { runSDKProcessCrashes } from './sdk_process_crash.mjs';
import { runSDKBrowserUpgrade } from './sdk_browser_upgrade.mjs';
import { runSDKOwnerSwitch } from './sdk_owner_switch.mjs';
import { runSDKConcurrency } from './sdk_concurrent.mjs';

const { GonvexClient } = await import(process.env.GONVEX_SDK_RECEIPT_CLIENT_MODULE
  ? pathToFileURL(process.env.GONVEX_SDK_RECEIPT_CLIENT_MODULE).href
  : new URL('../../packages/client/dist/index.js', import.meta.url).href);

const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
const url = args.get('--url'), output = args.get('--output');
assert(url?.startsWith('ws://127.0.0.1:') && output, 'owned runtime URL and fresh evidence directory required');
await mkdir(output, { recursive: false });
const NativeWebSocket = globalThis.WebSocket;
assert(NativeWebSocket, 'Node with a native WebSocket implementation required');
const collection = { kind: 'query', path: 'all', delivery: 'replica', replica: {
  table: 'receipt_records', key: 'id', columns: ['id', 'value'], maxRows: 100,
} };
const reducer = { kind: 'reducer', path: 'write', offline: { mode: 'allowed' } };

async function until(check, label) {
  const end = Date.now() + 20_000;
  while (!await check()) {
    assert(Date.now() < end, `SDK fixture did not reach ${label}`);
    await delay(10);
  }
}

for (const mode of ['success', 'lost_reply', 'write_failure', 'receipt_write_failure']) {
  const rowId = `sdk-${mode}`, file = join(output, `${mode}.sqlite`);
  const events = [], calls = [], receiptQueries = [], retirements = [], retirementAttempts = [], cursorWrites = [];
  let client, db, journal, reader, store, storage, scope, committedRevision;
  let dropped = false, failedWrite = false, failedAcceptanceWrite = false, blockIncoming = false;
  let closingForFault = false;
  let damaged = false;
  let commitHeld = false, releaseCommit, heldWitness, postgresCrash;

  class ObservedSocket extends NativeWebSocket {
    send(data) {
      const message = JSON.parse(data);
      const batch = message.type === 'reducer.callMany' ? message.calls : message.type === 'reducer.call' ? [message] : [];
      for (const call of batch) calls.push({ id: call.id, key: call.idempotencyKey, path: call.path, args: call.args });
      if (message.type === 'query.call' && message.path === '__gonvex.reducerReceipt') receiptQueries.push(message);
      super.send(data);
    }
    addEventListener(type, listener, options) {
      if (type !== 'message') return super.addEventListener(type, listener, options);
      return super.addEventListener(type, event => {
        if (blockIncoming) return;
        if (typeof event.data === 'string') {
          const message = JSON.parse(event.data);
          events.push({ type: message.type, revision: message.committedRevision ?? message.cursor?.revision ?? message.revision,
            ...(message.error ? { error: message.error } : {}) });
          if (message.type === 'auth.result') {
            assert.equal(message.result.accountId, 'account');
            assert.equal(message.result.tenantId, 'tenant');
            scope = message.result.replica.visibilityScope;
          }
          if (message.type === 'reducer.result') {
            committedRevision = Math.max(committedRevision ?? 0, message.committedRevision ?? 0);
            if (mode === 'lost_reply' && !dropped) {
              dropped = true;
              blockIncoming = true;
              closingForFault = true;
              client.close();
              return;
            }
          }
        }
        if (typeof listener === 'function') listener.call(this, event);
        else listener.handleEvent(event);
      }, options);
    }
  }
  globalThis.WebSocket = ObservedSocket;

  function openDatabase() {
    db = new DatabaseSync(file);
    db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
    journal = new DatabaseSync(join(output, `${mode}.outbox.sqlite`));
    journal.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
    reader = new DatabaseSync(file, { readOnly: true });
    const adapter = connection => ({
      async execAsync(sql) { connection.exec(sql); },
      async runAsync(sql, ...params) {
        const result = connection.prepare(sql).run(...params);
        if (connection === db && sql.includes("'cursor'")) cursorWrites.push({ value: params.at(-1), changes: result.changes });
        return result;
      },
      async getAllAsync(sql, ...params) { return connection.prepare(sql).all(...params); },
      async getFirstAsync(sql, ...params) { return connection.prepare(sql).get(...params) ?? null; },
      async withTransactionAsync(run) {
        connection.exec('BEGIN');
        try {
          await run();
          if (connection === db && mode === 'success' && process.env.GONVEX_SDK_RECEIPT_HOLD_REPLICA === '1'
            && !commitHeld && typeof scope === 'string' && connection.prepare('SELECT id FROM _gonvex_replica_entities WHERE scope=? AND entity=? AND id=?')
              .get(scope, 'receipt_records', rowId)) {
            commitHeld = true;
            await new Promise(resolve => { releaseCommit = resolve; });
          }
          connection.exec('COMMIT');
        }
        catch (error) {
          connection.exec('ROLLBACK');
          if (String(error).includes('injected SQLite replica write failure')) {
            failedWrite = true;
            closingForFault = true;
            // Let the real storage rejection reach the SDK before shutting it
            // down. A fresh SDK and database handle perform the recovery.
            setImmediate(() => client.close());
          }
          if (String(error).includes('injected SQLite acceptance write failure')) {
            failedAcceptanceWrite = true;
            closingForFault = true;
            blockIncoming = true;
            client.close();
          }
          throw error;
        }
      },
    });
    storage = new ExpoSQLiteLocalReplicaStorage(adapter(db));
    store = new ExpoSQLiteOutboxStore(adapter(journal));
    const remove = store.delete.bind(store), removeMany = store.deleteMany.bind(store);
    const inspectRetirement = async ids => {
      if (process.env.GONVEX_SDK_RECEIPT_DAMAGE === 'remove-replica-row' && !damaged) {
        damaged = true;
        db.prepare('DELETE FROM _gonvex_replica_entities WHERE scope=? AND entity=? AND id=?')
          .run(scope, 'receipt_records', rowId);
      }
      // Read another connection synchronously. Awaiting storage.load() here
      // would add a serialization barrier and could conceal early retirement.
      // The journal is a different file, so SQLite cannot impose that barrier.
      reader.exec('BEGIN');
      try {
        const row = reader.prepare('SELECT value FROM _gonvex_replica_entities WHERE scope=? AND entity=? AND id=?')
          .get(scope, 'receipt_records', rowId);
        const cursor = reader.prepare("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='cursor'").get(scope);
        const windows = reader.prepare('SELECT value FROM _gonvex_replica_queries WHERE scope=?').all(scope)
          .map(record => JSON.parse(record.value));
        retirementAttempts.push({ ids, rowPresent: !!row, cursor: cursor && JSON.parse(cursor.value), committedRevision });
        assert.deepEqual(row && JSON.parse(row.value), { id: rowId, value: 7 }, 'intent retirement passed durable replica data');
        assert(committedRevision > 0, 'intent retired without an observed backend commit barrier');
        const savedCursor = cursor && JSON.parse(cursor.value);
        assert(savedCursor?.revision >= committedRevision, 'intent retirement passed durable replica cursor');
        assert(windows.some(window => window.entity === 'receipt_records' && window.ids.includes(rowId)),
          'intent retirement passed durable replica membership');
        retirements.push({ ids, cursor: savedCursor, committedRevision, row: JSON.parse(row.value), windows });
      } finally { reader.exec('COMMIT'); }
    };
    store.delete = async id => { await inspectRetirement([id]); await remove(id); };
    store.deleteMany = async ids => { if (ids.length) await inspectRetirement(ids); await removeMany(ids); };
  }

  function closeDatabases() {
    reader?.close(); reader = undefined;
    journal?.close(); journal = undefined;
    db?.close(); db = undefined;
  }

  function openClient() {
    client = new GonvexClient(url, {
      project: 'project', tenant: 'tenant', token: 'gvx_session_receipt_sdk_fixture',
      identity: { iss: 'receipt-fixture', sub: 'account' },
      errorReporting: false, replicaFieldPatches: false,
      localReplica: { storage }, outbox: { store },
    });
    const watch = client.watchReplica(collection, {});
    client.connect();
    return watch;
  }

  try {
    openDatabase();
    let watch = openClient();
    await until(() => watch.status().isUpToDate && scope, 'authenticated initial replica');
    const before = await storage.load(scope);
    if (mode === 'write_failure') {
      // This is an actual SQLite transaction failure, not a memory adapter
      // rejection. It rejects insertion of this business row only.
      db.exec(`CREATE TRIGGER fail_replica_write BEFORE INSERT ON _gonvex_replica_entities
        WHEN NEW.id='${rowId}' BEGIN SELECT RAISE(ABORT,'injected SQLite replica write failure'); END;`);
    }
    if (mode === 'receipt_write_failure') {
      journal.exec(`CREATE TRIGGER fail_acceptance_write BEFORE INSERT ON _gonvex_outbox
        WHEN NEW.state='committed' BEGIN SELECT RAISE(ABORT,'injected SQLite acceptance write failure'); END;`);
    }
    let outcomeComplete = false;
    const outcome = client.reducer(reducer, { id: rowId, value: 7 }, { offline: 'queue' })
      .then(value => { outcomeComplete = true; return { value }; }, error => {
        outcomeComplete = true; return { error: { code: error.code, message: error.message } };
      });
    if (mode === 'success' && process.env.GONVEX_SDK_RECEIPT_HOLD_REPLICA === '1') {
      try {
        await until(() => commitHeld, 'staged uncommitted replica write');
        await until(() => committedRevision > 0, 'server result while SQLite commit is held');
        await delay(0);
        assert.equal(retirementAttempts.length, 0, 'intent retirement passed uncommitted replica data');
        assert.equal(outcomeComplete, false, 'SDK completion passed uncommitted replica data');
        reader.exec('BEGIN');
        try {
          assert.equal(reader.prepare('SELECT value FROM _gonvex_replica_entities WHERE scope=? AND entity=? AND id=?')
            .get(scope, 'receipt_records', rowId), undefined, 'independent reader observed uncommitted replica data');
        } finally { reader.exec('COMMIT'); }
        const retained = await store.load();
        assert.equal(retained.length, 1, 'held replica commit lost its durable intent');
        heldWitness = { resultObserved: true, outcomePending: !outcomeComplete, journalEntries: retained.length,
          retirementAttempts: retirementAttempts.length, independentReaderSawRow: false };
      } finally { releaseCommit?.(); }
    }
    if (mode === 'success') {
      assert.deepEqual(await outcome, { value: { id: rowId, value: 7 } });
    } else {
      await until(() => mode === 'lost_reply' ? dropped : mode === 'receipt_write_failure' ? failedAcceptanceWrite : failedWrite, 'injected failure boundary');
      await until(() => closingForFault && !client.connectionState().isWebSocketConnected, 'closed old SDK');
      await client.replicaFrames;
      const result = await outcome;
      assert(result.error || result.value?.status === 'queued', 'failed delivery published reducer success');
      const retained = await store.load();
      assert.equal(retained.length, 1, 'failed delivery discarded its durable intent');
      assert.equal(retained[0].args.id, rowId);
      assert.equal(retained[0].idempotencyKey, calls[0].key);
      if (mode === 'write_failure') {
        assert.equal(retained[0].state, 'committed', 'received acceptance was not durably recorded');
        assert.equal(retained[0].awaitingReplica, true, 'accepted intent lost its pending replica barrier');
        const failed = await storage.load(scope);
        assert.equal(failed?.entities.receipt_records?.[rowId], undefined);
        assert.deepEqual(failed?.cursor, before.cursor, 'failed SQLite transaction advanced the durable cursor');
        db.exec('DROP TRIGGER fail_replica_write');
      }
      if (mode === 'receipt_write_failure') {
        assert.equal(retained[0].state, 'inflight', 'acceptance failure did not preserve the preceding journal checkpoint');
        journal.exec('DROP TRIGGER fail_acceptance_write');
      }
      if (mode === 'lost_reply' && process.env.GONVEX_SDK_PG_CRASH_COORDINATION) {
        const coordination = process.env.GONVEX_SDK_PG_CRASH_COORDINATION;
        await writeFile(`${coordination}.ready.tmp`, JSON.stringify({ mode, key: calls[0].key, rowId,
          committedRevision, retained, receivedResultDropped: dropped, oldSDKClosed: !client.connectionState().isWebSocketConnected }) + '\n');
        await rename(`${coordination}.ready.tmp`, `${coordination}.ready`);
        // A separate coordinator owns and kills PostgreSQL. The runtime and its
        // existing pools remain alive; fresh SDK handles must recover through
        // those pools after actual server crash recovery.
        const deadline = Date.now() + 60_000;
        for (;;) {
          try { postgresCrash = JSON.parse(await readFile(`${coordination}.restarted`, 'utf8')); break; }
          catch (error) { if (error.code !== 'ENOENT') throw error; }
          assert(Date.now() < deadline, 'PostgreSQL crash coordinator did not resume recovery');
          await delay(10);
        }
        assert.equal(postgresCrash.killSignal, 'SIGKILL');
        assert.equal(postgresCrash.exitCode, 137);
        assert.equal(postgresCrash.before.effects, 1);
        assert.deepEqual(postgresCrash.after, postgresCrash.before);
      }
      closeDatabases();
      blockIncoming = false;
      closingForFault = false;
      openDatabase();
      watch = openClient();
      await until(async () => watch.status().isUpToDate && (await store.load()).length === 0, 'reopened SQLite replay and retirement');
      assert.equal(calls.length, 1, 'accepted intent became sendable again');
      assert(receiptQueries.length > 0, 'accepted intent did not use read-only receipt reconciliation');
      assert(receiptQueries.every(query => query.args.idempotencyKey === calls[0].key && query.args.receiptPath === 'write'),
        'receipt reconciliation changed the original intent identity');
      assert.equal(new Set(calls.map(call => call.key)).size, 1, 'retry changed intent identity');
      assert(calls.every(call => call.args.id === rowId && call.args.value === 7), 'retry changed command arguments');
    }
    await until(async () => (await store.load()).length === 0, 'durable intent retirement');
    const saved = await storage.load(scope);
    assert.deepEqual(saved.entities.receipt_records[rowId], { id: rowId, value: 7 });
    assert.equal(client.localReplica.entity('receipt_records', rowId)?.value, 7);
    assert(retirements.length > 0, 'retirement witness missing');
    assert(events.some(event => event.type === 'auth.result'), 'real authenticated response missing');
    const sqlite = db.prepare('SELECT sqlite_version() AS version').get().version;
    await writeFile(join(output, `${mode}.json`), JSON.stringify({ mode, node: process.version, sqlite,
      dropped, failedWrite, failedAcceptanceWrite, commitHeld, heldWitness, postgresCrash, calls, receiptQueries, retirementAttempts, retirements, cursorWrites, events, finalCursor: saved.cursor,
      boundary: 'Real authenticated SDK WebSocket/runtime/V8/PostgreSQL and Node SQLite through the Expo adapter. Journal and replica use independent files; the retirement oracle uses another replica connection. Restart reopens SDK and all database handles. Does not prove native Expo/Hermes, browser engines or power-loss durability.' }, null, 2) + '\n');
  } catch (error) {
    console.error(JSON.stringify({ mode, scope, committedRevision, dropped, failedWrite, events, calls,
      retirementAttempts, retirementCount: retirements.length, cursorWrites, memoryCursor: client?.localReplica.cursor(),
      windows: client?.localReplica.listWindows(), processedWatermark: client?.processedReplicaWatermarkRevision }));
    throw error;
  } finally {
    releaseCommit?.();
    client?.close();
    if (client) await client.replicaFrames;
    closeDatabases();
    globalThis.WebSocket = NativeWebSocket;
  }
}
await runSDKProcessCrashes(url, join(output, 'process-crashes'));
await runSDKBrowserUpgrade(url, join(output, 'browser-upgrade'));
const ownershipOrder = process.env.GONVEX_SDK_OWNER_ORDER === 'tenant-first' ? ['tenant', 'account'] : ['account', 'tenant'];
for (const dimension of ownershipOrder) {
  await runSDKOwnerSwitch(url, join(output, dimension === 'tenant' ? 'tenant-switch' : 'owner-switch'), dimension);
}
await runSDKConcurrency(url, join(output, 'concurrency'));
console.log('Authenticated SDK/backend/SQLite failures and both SDK process-kill boundaries recovered');
