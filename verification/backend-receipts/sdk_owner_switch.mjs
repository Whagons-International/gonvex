import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { ExpoSQLiteLocalReplicaStorage, ExpoSQLiteOutboxStore } from '../../packages/expo-sqlite/dist/index.js';

const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
async function until(check, label) {
  const deadline = Date.now() + 20_000;
  while (!await check()) { assert(Date.now() < deadline, 'SDK owner fixture did not reach ' + label); await delay(10); }
}
export async function runSDKOwnerSwitch(url, output, dimension = 'account') {
  assert(['account', 'tenant'].includes(dimension));
  const tenantSwitch = dimension === 'tenant';
  const firstOwner = tenantSwitch ? 'tenant' : 'account', secondOwner = tenantSwitch ? 'tenant-b' : 'bob';
  const firstRow = tenantSwitch ? 'sdk-tenant-alice' : 'sdk-owner-alice';
  const secondRow = tenantSwitch ? 'sdk-tenant-bob' : 'sdk-owner-bob';
  await mkdir(output, { recursive: false });
  const { GonvexClient } = await import(process.env.GONVEX_SDK_RECEIPT_CLIENT_MODULE
    ? pathToFileURL(process.env.GONVEX_SDK_RECEIPT_CLIENT_MODULE).href
    : new URL('../../packages/client/dist/index.js', import.meta.url).href);
  const db = new DatabaseSync(join(output, 'replica.sqlite')), journal = new DatabaseSync(join(output, 'journal.sqlite'));
  for (const connection of [db, journal]) connection.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
  const adapter = connection => ({
    async execAsync(sql) { connection.exec(sql); },
    async runAsync(sql, ...params) { return connection.prepare(sql).run(...params); },
    async getAllAsync(sql, ...params) { return connection.prepare(sql).all(...params); },
    async getFirstAsync(sql, ...params) { return connection.prepare(sql).get(...params) ?? null; },
    async withTransactionAsync(run) { connection.exec('BEGIN'); try { await run(); connection.exec('COMMIT'); } catch (error) { connection.exec('ROLLBACK'); throw error; } },
  });
  const storage = new ExpoSQLiteLocalReplicaStorage(adapter(db)), store = new ExpoSQLiteOutboxStore(adapter(journal));
  // The fixture inserts Bob's parked record through the storage interface
  // while the SDK is live. Opt into fresh shared-store reads for that peer write.
  // There is one SDK and the insert finishes before rotation; no cross-process
  // coordination guarantee is inferred from this flag.
  store.shared = true;
  const release = deferred(), acceptanceReturned = deferred();
  const update = store.update.bind(store);
  let original, owner, visibility, committedRevision = 0, client;
  const calls = [], watermarks = [], auth = [], events = [];
  store.update = async (id, change) => {
    const entry = await update(id, change);
    if (entry?.awaitingReplica && !original) {
      original = structuredClone(entry);
      await release.promise;
      acceptanceReturned.resolve();
    }
    return entry;
  };
  const NativeSocket = globalThis.WebSocket;
  class Socket extends NativeSocket {
    send(data) {
      const message = JSON.parse(data);
      const batch = message.type === 'reducer.callMany' ? message.calls : message.type === 'reducer.call' ? [message] : [];
      for (const call of batch) calls.push({ owner, ...call });
      super.send(data);
    }
    addEventListener(type, listener, options) {
      if (type !== 'message') return super.addEventListener(type, listener, options);
      return super.addEventListener(type, event => {
        const message = JSON.parse(event.data);
        events.push({ type: message.type, ...(message.error ? { error: message.error } : {}) });
        if (message.type === 'auth.result') {
          owner = tenantSwitch ? message.result.tenantId : message.result.accountId; visibility = message.result.replica.visibilityScope;
          auth.push({ owner, visibility, account: message.result.accountId, tenant: message.result.tenantId });
        }
        if (message.type === 'reducer.result' && owner === firstOwner) committedRevision = message.committedRevision;
        if (message.type === 'replica.watermark') watermarks.push({ owner, revision: message.revision });
        if (typeof listener === 'function') listener.call(this, event); else listener.handleEvent(event);
      }, options);
    }
  }
  globalThis.WebSocket = Socket;
  const credentials = selected => ({ project: 'project', tenant: tenantSwitch ? selected : 'tenant',
    identity: { iss: 'receipt-fixture', sub: tenantSwitch ? 'account' : selected },
    token: tenantSwitch || selected === 'account' ? 'gvx_session_receipt_sdk_fixture' : 'gvx_session_receipt_sdk_bob' });
  try {
    client = new GonvexClient(url, { ...credentials(firstOwner), errorReporting: false, replicaFieldPatches: false,
      localReplica: { storage }, outbox: { store } });
    const watch = client.watchReplica({ kind: 'query', path: 'all', delivery: 'replica', replica: {
      table: 'receipt_records', key: 'id', columns: ['id', 'value'], maxRows: 100,
    } }, {});
    client.connect();
    await until(() => owner === firstOwner && watch.status().isUpToDate, 'first owner admission');
    let outcome = 'pending';
    const result = client.reducer({ kind: 'reducer', path: 'write', offline: { mode: 'allowed' } },
      { id: firstRow, value: 7 }, { offline: 'queue' }).then(value => { outcome = 'value'; return { value }; },
      error => { outcome = 'error'; return { error: { code: error.code, message: error.message } }; });
    await until(() => original && committedRevision > 0 && watermarks.some(mark => mark.owner === firstOwner && mark.revision >= committedRevision), 'accepted first-owner data barrier');
    await client.replicaFrames;
    assert.equal(outcome, 'pending', 'acceptance promise was not held');
    assert.equal(client.localReplica.entity('receipt_records', firstRow)?.value, 7);
    const bobScope = ['identity', url, 'project', tenantSwitch ? secondOwner : 'tenant', 'receipt-fixture', tenantSwitch ? 'account' : secondOwner].join('\u0000');
    const bob = { ...original, id: original.id + 1, scope: bobScope, args: { id: secondRow, value: 7 },
      state: 'failed', awaitingReplica: undefined, attempts: 1, nextAttemptAt: 0, knownUnsent: true,
      lastError: 'Fixture parked intent requires explicit retry', lastErrorClass: 'transient',
      patches: [{ entity: 'receipt_records', rowId: secondRow, op: 'upsert', fields: { id: secondRow, value: 7 } }] };
    await store.put(bob);
    client.setAuth(credentials(secondOwner));
    await until(async () => owner === secondOwner && watch.status().isUpToDate
      && (await client.listIntents()).some(intent => intent.entryId === bob.id), 'second owner restoration');
    assert.equal(client.localReplica.entity('receipt_records', secondRow)?.value, 7);
    if (tenantSwitch) {
      assert.equal(client.localReplica.entity('receipt_records', firstRow), undefined, 'old tenant data appeared in replacement tenant');
      assert.equal(client.localReplica.entity('receipt_records', 'tenant-b-private')?.value, 73, 'new tenant did not receive its actual database data');
      assert(client.localReplica.cursor().revision >= committedRevision,
        'tenant ownership control was masked by a smaller replacement revision');
    }
    assert.deepEqual(await client.query({ kind: 'query', path: '__gonvex.reducerReceipt' },
      { idempotencyKey: original.idempotencyKey, receiptPath: 'write' }), { found: false }, 'replacement owner saw original owner receipt');
    release.resolve(); await acceptanceReturned.promise;
    // Reach the next task after all promise continuations from the released
    // acceptance write, rather than guessing how long the stale callback takes.
    await new Promise(resolve => setImmediate(resolve));
    const oldOutcome = await result;
    assert.equal(oldOutcome.error?.code, 'superseded', 'old owner result published into replacement owner');
    assert.equal(client.localReplica.entity('receipt_records', secondRow)?.value, 7,
      'old accepted callback removed replacement owner prediction');
    assert.deepEqual((await client.listIntents()).map(intent => intent.entryId), [bob.id]);
    const retained = await store.load();
    assert(retained.some(entry => entry.id === original.id && entry.awaitingReplica), 'old unresolved intent was retired in new owner');
    assert(retained.some(entry => entry.id === bob.id && entry.state === 'failed'), 'old callback changed new owner journal');
    assert.equal(await client.retryIntent(bob.idempotencyKey), true);
    await until(async () => (await store.load()).every(entry => entry.id !== bob.id), 'second owner commit');
    assert.equal(client.localReplica.entity('receipt_records', secondRow)?.value, 7);
    const bobSaved = await storage.load(visibility);
    assert.deepEqual(bobSaved.entities.receipt_records[secondRow], { id: secondRow, value: 7 });
    client.setAuth(credentials(firstOwner));
    await until(async () => owner === firstOwner && watch.status().isUpToDate && (await store.load()).length === 0, 'original owner receipt reconciliation');
    if (tenantSwitch) {
      assert.equal(client.localReplica.entity('receipt_records', secondRow), undefined, 'replacement tenant row leaked on return');
      assert.equal(client.localReplica.entity('receipt_records', 'tenant-b-private'), undefined, 'replacement tenant private data leaked on return');
      assert(auth.every(event => event.account === 'account'), 'tenant schedule changed accounts');
    }
    assert.equal(calls.filter(call => call.owner === firstOwner).length, 1, 'original owner resent accepted work');
    assert.equal(calls.filter(call => call.owner === secondOwner).length, 1);
    assert(calls.every(call => call.idempotencyKey === original.idempotencyKey), 'owner switch changed original logical keys');
    await writeFile(join(output, 'receipt.json'), JSON.stringify({ dimension, auth, calls, oldOutcome, original, bob, retained,
      boundary: (tenantSwitch ? 'Actual authenticated tenant A->B->A across separately routed tenant SQLite files; account remains fixed. ' : '') + 'Actual authenticated owner A->B->A on one SDK/socket and real SQLite. Same logical key in distinct owner namespaces, delayed durable acceptance completion, parked prediction, explicit retry and original-owner reconciliation.' }, null, 2) + '\n');
  } catch (error) {
    await writeFile(join(output, 'failure.json'), JSON.stringify({ error: error.message, owner, visibility, auth, calls, watermarks, events,
      scope: client?.outboxScope, cursor: client?.localReplica.cursor(),
      journal: journal.prepare('SELECT value FROM _gonvex_outbox').all().map(row => JSON.parse(row.value)) }, null, 2) + '\n');
    throw error;
  } finally { release.resolve(); client?.close(); if (client) await client.replicaFrames; journal.close(); db.close(); globalThis.WebSocket = NativeSocket; }
}
