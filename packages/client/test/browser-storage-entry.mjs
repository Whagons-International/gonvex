import { GonvexClient, browserUpgradeStorage } from '../src/index.ts';
import { IndexedDBLocalReplicaStorage } from '../src/indexeddb-replica.ts';
import { IndexedDBEngineReplicaStorage } from '../src/indexeddb-engine.ts';
import { DexieReducerOutbox } from '../src/outbox.ts';
import { replicaRowsHashes, replicaHashesDigest } from '../src/replica-integrity.ts';
import { Dexie } from 'dexie';
import { LocalReplica, MemoryLocalReplicaStorage } from '../src/local-replica.ts';
import { entityRecord } from '../src/indexeddb-read-view.ts';

const storageFor = (kind, name) => kind === 'legacy'
  ? new IndexedDBLocalReplicaStorage(name) : new IndexedDBEngineReplicaStorage(name);
const cursor = revision => ({ epoch: 'browser-epoch', revision });
const initial = { cursor: cursor(7), entities: { tasks: {
  a: { id: 'a', name: 'Original' }, b: { id: 'b', name: 'Keep' },
} }, liveQueries: {} };
const transaction = { cursor: cursor(8), changes: [
  { entity: 'tasks', id: 'a', operation: 'update', newValue: { id: 'a', name: 'Changed' } },
  { entity: 'tasks', id: 'b', operation: 'delete' },
  { entity: 'tasks', id: 'c', operation: 'insert', newValue: { id: 'c', name: 'Inserted' } },
] };

// The witness fires only after an actual native IndexedDB row request succeeds
// inside the still-uncommitted transaction. It is not a pre-write rejection.
function interruptEntityWrite(name, crash = false) {
  const original = IDBObjectStore.prototype.put;
  let witnessed = 0, aborted = 0;
  IDBObjectStore.prototype.put = function (...args) {
    const request = original.apply(this, args);
    if (!witnessed && this.name === 'entities' && this.transaction.db.name.startsWith(name)) {
      witnessed = -1;
      request.addEventListener('success', () => {
        witnessed = 1;
        if (!crash) {
          this.transaction.addEventListener('abort', () => { aborted++; }, { once: true });
          this.transaction.abort();
        }
        else {
          // Keep this transaction alive until the test kills its browser.
          const pump = () => { this.get(request.result).onsuccess = pump; };
          pump();
          void window.transactionWriteWitness();
        }
      });
    }
    return request;
  };
  return { count: () => witnessed, aborted: () => aborted, restore: () => { IDBObjectStore.prototype.put = original; } };
}

async function seed(kind, name) {
  const storage = storageFor(kind, name);
  try {
    await storage.replaceSnapshot(structuredClone(initial), 'account-a');
    await storage.replaceSnapshot({ ...structuredClone(initial), cursor: cursor(3) }, 'account-b');
  } finally { storage.close(); }
}

async function read(kind, name, scope = 'account-a') {
  const storage = storageFor(kind, name);
  try { return await storage.load(scope); } finally { storage.close(); }
}

async function abortTransaction(kind, name) {
  const storage = storageFor(kind, name), fault = interruptEntityWrite(name);
  let rejected = false;
  try { await storage.applyTransaction(transaction, initial, 'account-a'); }
  catch (error) {
    if (!/abort/i.test(String(error))) throw error;
    rejected = true;
  } finally { fault.restore(); storage.close(); }
  return { witnessed: fault.count(), rejected, saved: await read(kind, name), other: await read(kind, name, 'account-b') };
}

async function commitTransaction(kind, name) {
  const storage = storageFor(kind, name);
  try {
    await storage.applyTransaction(transaction, initial, 'account-a');
    // A delayed old transaction cannot restore deleted rows or older fields.
    await storage.applyTransaction({ cursor: cursor(7), changes: [
      { entity: 'tasks', id: 'a', operation: 'update', newValue: { id: 'a', name: 'Stale' } },
      { entity: 'tasks', id: 'b', operation: 'insert', newValue: { id: 'b', name: 'Stale resurrection' } },
    ] }, initial, 'account-a');
  } finally { storage.close(); }
  return read(kind, name);
}

async function crashTransaction(kind, name) {
  const storage = storageFor(kind, name);
  interruptEntityWrite(name, true);
  await storage.applyTransaction(transaction, initial, 'account-a');
  throw Error('transaction unexpectedly completed before browser crash');
}

class Socket {
  static CONNECTING = 0;
  static OPEN = 1;
  static instances = [];
  readyState = 0;
  sent = [];
  listeners = new Map();
  constructor() { Socket.instances.push(this); }
  addEventListener(type, listener) { this.listeners.set(type, [...this.listeners.get(type) ?? [], listener]); }
  send(text) { this.sent.push(JSON.parse(text)); }
  open() { this.readyState = 1; for (const listener of this.listeners.get('open') ?? []) listener(); }
  close() { this.readyState = 3; for (const listener of this.listeners.get('close') ?? []) listener(); }
  receive(message) { for (const listener of this.listeners.get('message') ?? []) listener({ data: JSON.stringify(message) }); }
}
async function until(check) {
  const end = Date.now() + 10000;
  while (!await check()) {
    if (Date.now() > end) throw Error('browser fixture condition timed out');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}
// Read-only joining of frames injected through the public wire.
const join = client => client.replicaFrames;

async function snapshotBarrier(kind, name, fail = false) {
  const originalSocket = window.WebSocket;
  window.WebSocket = Socket;
  const storage = storageFor(kind, name);
  const client = new GonvexClient('ws://fixture.invalid/ws', { errorReporting: false,
    localReplica: { storage }, outbox: { databaseName: name + '-queue' } });
  let fault;
  try {
    const ref = { kind: 'query', path: 'tasks.all', delivery: 'replica',
      replica: { table: 'tasks', key: 'id', columns: ['id', 'name'], maxRows: 100 } };
    client.watchReplica(ref, {});
    const socket = Socket.instances.at(-1);
    socket.open();
    const directive = { protocolVersion: 1, scope: 'tenant', visibilityScope: 'visible', epoch: 'browser-epoch' };
    socket.receive({ type: 'session.ready', capabilities: { replicaWatermark: 1, replicaSnapshotPages: 1 }, replica: directive });
    const auth = socket.sent.find(m => m.type === 'auth');
    if (auth) socket.receive({ type: 'auth.result', id: auth.id, result: { replica: directive } });
    await until(() => socket.sent.some(m => m.type === 'replica.open'));
    const open = socket.sent.find(m => m.type === 'replica.open');
    const rows = [{ id: 'a', name: 'Original' }];
    const hashes = await replicaRowsHashes(rows, 'id'), digest = await replicaHashesDigest(hashes);
    socket.receive({ type: 'replica.snapshot', id: open.id, path: ref.path, key: 'id',
      result: rows, cursor: cursor(7), maxRows: 100, hashes, digest, truncated: false });
    socket.receive({ type: 'replica.ready', id: open.id, path: ref.path, cursor: cursor(7), digest, truncated: false });
    await join(client);
    let outcome = 'pending';
    const result = client.reducer({ kind: 'reducer', path: 'tasks.create', offline: { mode: 'allowed' } },
      { id: 'c' }, { offline: 'queue' }).then(value => { outcome = value === 'accepted' ? 'success' : 'queued'; },
      () => { outcome = 'error'; });
    await until(() => socket.sent.some(m => m.type === 'reducer.call'));
    const call = socket.sent.find(m => m.type === 'reducer.call');
    client.watchReplica({ ...ref, path: 'tasks.paged' }, {});
    await until(() => socket.sent.some(m => m.type === 'replica.open' && m.path === 'tasks.paged'));
    const paged = socket.sent.find(m => m.type === 'replica.open' && m.path === 'tasks.paged');
    const all = [...rows, { id: 'c', name: 'New snapshot row' }];
    const finalDigest = await replicaHashesDigest(await replicaRowsHashes(all, 'id'));
    const page = { type: 'replica.snapshotPage', id: paged.id, path: paged.path,
      snapshotId: name, scope: 'tenant', visibilityScope: 'visible', key: 'id', cursor: cursor(8),
      mode: 'eager', maxRows: 100, sequence: 0, last: false, result: all, truncated: false };
    socket.receive(page);
    await join(client);
    socket.receive({ type: 'reducer.result', id: call.id, path: call.path,
      result: 'accepted', originCommandId: call.id, committedRevision: 8 });
    socket.receive({ type: 'replica.watermark', revision: 8 });
    await join(client);
    await new Promise(resolve => setTimeout(resolve, 0));
    const before = { outcome, cursor: client.localReplica.cursor(), intents: await client.listIntents(),
      saved: await storage.load('visible') };
    // Return the specific violated observations to the negative-control runner.
    if (before.outcome !== 'pending' || before.intents.length !== 1) return { before };
    if (fail) fault = interruptEntityWrite(name);
    socket.receive({ ...page, sequence: 1, last: true, result: [], digest: finalDigest });
    await join(client);
    if (!fail) socket.receive({ type: 'replica.ready', id: paged.id, path: paged.path, cursor: cursor(8), digest: finalDigest, truncated: false });
    await join(client);
    if (!fail) await until(() => outcome === 'success');
    else await until(() => socket.readyState === 3);
    const after = { outcome, cursor: client.localReplica.cursor(), intents: await client.listIntents(),
      saved: await storage.load('visible'), witnessed: fault?.count() };
    void result;
    return { before, after };
  } finally {
    fault?.restore(); client.close(); await join(client); storage.close(); window.WebSocket = originalSocket;
  }
}

async function recoveredQueue(name) {
  const queue = new DexieReducerOutbox({ databaseName: name + '-queue' });
  const scopes = await queue.listScopes();
  // This fixture's browser context owns and closes the database connection.
  return (await Promise.all(scopes.map(scope => queue.loadAll(scope.scope)))).flat();
}

async function acceptedQueue(name, seed = false) {
  const queue = new DexieReducerOutbox({ databaseName: name + '-queue' });
  const scope = 'accepted-account';
  try {
    if (seed) {
      const entry = await queue.enqueue({ scope, path: 'tasks.save', args: { value: 1 }, idempotencyKey: 'accepted-intent' });
      await queue.markInflight(entry.id);
      await queue.markCommitted(entry.id);
      return await queue.list(scope);
    }
    const before = await queue.loadAll(scope);
    if (before.length !== 1) throw Error('accepted queue record missing after crash');
    await queue.reject(before[0].id, 'late rejection');
    const retried = await queue.retry(before[0].id);
    await queue.fail(before[0].id, 'late timeout', { maxAttempts: 1 });
    return { before, after: await queue.list(scope), retried: retried ?? null,
      ready: await queue.nextReady(scope, Number.MAX_SAFE_INTEGER) ?? null };
  } finally { queue.dispose?.(); }
}

async function seedHistoricalReplica(version, name, ambiguous = false) {
  const database = new Dexie(name);
  database.version(version).stores({ snapshots: version === 1 ? '&key' : '&scope' });
  const snapshot = { ...structuredClone(initial), liveQueries: { 'tasks:all': {
    signature: 'tasks:all', kind: 'replica', entity: 'tasks', key: 'id', ids: ['a', 'b'],
    cursor: cursor(7), completeness: 'complete', source: 'server',
  } } };
  const rows = version === 1 ? [{ key: 'replica', snapshot }] : [
    { scope: 'account-a', snapshot },
    { scope: 'account-b', snapshot: { cursor: { epoch: 'other-owner', revision: 19 },
      entities: { tasks: { private: { id: 'private', name: 'Other owner' } } }, liveQueries: {} } },
  ];
  if (ambiguous) rows.push({ key: 'another-checkpoint', snapshot: { ...structuredClone(snapshot), cursor: cursor(99) } });
  try { await database.table('snapshots').bulkPut(rows); } finally { database.close(); }
  return rows;
}

async function inspectHistoricalReplica(name) {
  const database = new Dexie(name);
  try {
    await database.open();
    return { version: database.verno, tables: database.tables.map(table => table.name).sort(), rows: await database.table('snapshots').toArray() };
  } finally { database.close(); }
}

async function abortHistoricalReplicaUpgrade(name) {
  const storage = new IndexedDBLocalReplicaStorage(name), fault = interruptEntityWrite(name);
  let rejected = false, failure;
  try { await storage.load(); }
  catch (error) {
    if (fault.count() !== 1) throw error;
    rejected = true;
    failure = `${error.name}: ${error.message}`;
  } finally { fault.restore(); storage.close(); }
  const raw = await inspectHistoricalReplica(name);
  return { witnessed: fault.count(), aborted: fault.aborted(), rejected, failure, raw };
}

async function crashHistoricalReplicaUpgrade(name) {
  const storage = new IndexedDBLocalReplicaStorage(name);
  interruptEntityWrite(name, true);
  await storage.load();
  throw Error('historical upgrade unexpectedly completed before browser crash');
}

async function readHistoricalReplica(name) {
  const storage = new IndexedDBLocalReplicaStorage(name);
  try {
    return { default: await storage.load('default'), a: await storage.load('account-a'), b: await storage.load('account-b') };
  } finally { storage.close(); }
}

async function seedNormalizedHistoricalReplica(version, name) {
  const database = new Dexie(name);
  database.version(version).stores({
    entities: '[scope+entity+id], scope, [scope+entity]' + (version === 4 ? ', *lookupKeys' : ''),
    windows: '[scope+signature], scope', meta: '[scope+key], scope', snapshots: '&scope',
  });
  const a = { ...structuredClone(initial), storageSequence: 0, liveQueries: { 'tasks:all': {
    signature: 'tasks:all', kind: 'replica', entity: 'tasks', key: 'id', ids: ['a', 'b'],
    cursor: cursor(7), completeness: 'complete', source: 'server',
  } } };
  const b = { storageSequence: 0, cursor: { epoch: 'other-owner', revision: 19 },
    entities: { tasks: { private: { id: 'private', name: 'Other owner' } } }, liveQueries: {} };
  try {
    for (const [scope, snapshot] of [['account-a', a], ['account-b', b]]) {
      for (const [entity, rows] of Object.entries(snapshot.entities)) for (const [id, value] of Object.entries(rows)) {
        const record = version === 4 ? entityRecord(scope, entity, id, value) : { scope, entity, id, value: JSON.stringify(value) };
        await database.table('entities').put(record);
      }
      for (const [signature, value] of Object.entries(snapshot.liveQueries))
        await database.table('windows').put({ scope, signature, value: JSON.stringify(value) });
      await database.table('meta').put({ scope, key: 'cursor', value: JSON.stringify(snapshot.cursor) });
    }
  } finally { database.close(); }
  return { expected: { default: undefined, a, b }, raw: await inspectNamedDatabase(name) };
}

function interruptSchemaIndex(name, crash) {
  const original = IDBObjectStore.prototype.createIndex;
  let witnessed = 0, aborted = 0;
  IDBObjectStore.prototype.createIndex = function (...args) {
    const index = original.apply(this, args);
    if (witnessed === 0 && this.transaction.db.name === name) {
      witnessed = -1;
      // This native request can succeed only after IndexedDB created the new
      // index. Hold or abort its still-uncommitted version transaction there.
      const request = index.getAllKeys();
      request.onsuccess = () => {
        witnessed = 1;
        if (!crash) {
          this.transaction.addEventListener('abort', () => { aborted++; }, { once: true });
          this.transaction.abort();
        } else {
          const pump = () => { this.count().onsuccess = pump; };
          pump(); void window.transactionWriteWitness();
        }
      };
    }
    return index;
  };
  return { count: () => witnessed, aborted: () => aborted, restore: () => { IDBObjectStore.prototype.createIndex = original; } };
}

async function interruptNormalizedHistoricalUpgrade(name, crash) {
  const storage = new IndexedDBLocalReplicaStorage(name), fault = interruptSchemaIndex(name, crash);
  let error;
  try { await storage.load('account-a'); }
  catch (failure) { error = `${failure.name}: ${failure.message}`; }
  finally { fault.restore(); storage.close(); }
  if (crash) throw Error('normalized historical upgrade ended before browser crash: ' + error);
  const raw = await inspectNamedDatabase(name);
  return { error, witnessed: fault.count(), aborted: fault.aborted(), raw };
}

async function readNormalizedHistoricalReplica(name) {
  const storage = new IndexedDBLocalReplicaStorage(name);
  try {
    return { snapshots: { default: await storage.load('default'), a: await storage.load('account-a'), b: await storage.load('account-b') },
      predicate: await storage.withReadView('account-a', { tasks: { key: 'id', complete: true } },
        view => view.select({ table: 'tasks', where: { column: 'name', op: 'eq', value: 'Original' } })) };
  } finally { storage.close(); }
}

async function inspectReplicaDatabase(kind, name) {
  return inspectNamedDatabase(kind === 'engine' ? `${name}:engine-members-v1` : name);
}

async function inspectNamedDatabase(name) {
  const database = new Dexie(name);
  try {
    await database.open();
    const tables = {};
    for (const table of database.tables) tables[table.name] = {
      primaryKey: table.schema.primKey.src,
      indexes: table.schema.indexes.map(index => index.src).sort(), rows: await table.toArray(),
    };
    return { version: database.backendDB().version, tables };
  } finally { database.close(); }
}

async function seedFutureReplica(kind, name) {
  await seed(kind, name);
  const original = await inspectReplicaDatabase(kind, name);
  await new Promise((resolve, reject) => {
    const request = indexedDB.open(kind === 'engine' ? `${name}:engine-members-v1` : name, original.version + 10);
    request.onupgradeneeded = () => {
      request.result.createObjectStore('future-contract', { keyPath: 'id' }).put({ id: 'accepted-history', accepted: true });
      // A future format can change indexes. An old reader must not "repair"
      // those indexes before discovering that it cannot interpret that format.
      request.transaction.objectStore('entities').deleteIndex('lookupKeys');
    };
    request.onsuccess = () => { request.result.close(); resolve(); };
    request.onerror = () => reject(request.error);
  });
  return inspectReplicaDatabase(kind, name);
}

async function readFutureReplica(kind, name) {
  const storage = storageFor(kind, name);
  let loaded, error;
  try { loaded = await storage.load('account-a'); }
  catch (failure) { error = `${failure.name}: ${failure.message}`; }
  finally { storage.close(); }
  return { loaded, error, raw: await inspectReplicaDatabase(kind, name) };
}

async function replicaCounterIntegrity(kind, name, value, mode = 'encoded') {
  if (mode === 'window') {
    const storage = storageFor(kind, name);
    try {
      await storage.replaceSnapshot({ cursor: cursor(7), entities: {}, liveQueries: { empty: {
        signature: 'empty', kind: 'replica', entity: 'tasks', key: 'id', ids: [],
        cursor: cursor(7), completeness: 'complete', source: 'server',
      } } }, 'account-a');
      await storage.replaceSnapshot(structuredClone(initial), 'account-b');
    } finally { storage.close(); }
  } else await seed(kind, name);
  const raw = new Dexie(kind === 'engine' ? `${name}:engine-members-v1` : name);
  const storage = storageFor(kind, name), reads = {};
  try {
    await raw.open();
    const priorSequence = Number((await raw.table('meta').get(['account-a', 'sequence'])).value);
    if (mode === 'missing') await raw.table('meta').delete(['account-a', 'sequence']);
    else await raw.table('meta').put({ scope: 'account-a', key: mode === 'reset' ? 'resetSequence' : 'sequence', value });
    if (mode === 'other-owner-ahead') {
      await raw.table('meta').put({ scope: 'account-b', key: 'sequence', value: '1000' });
      await storage.applyTransaction(transaction, initial, 'account-b');
    }
    const before = await inspectReplicaDatabase(kind, name);
    for (const [label, run] of [
      ['load', () => storage.load('account-a')],
      ['workingSet', () => storage.loadWorkingSet('account-a', { maxRows: 100, maxBytes: 1_000_000 })],
      ['changes', () => storage.readChanges('account-a', 0)],
      ['entityRows', () => storage.loadEntityRows('account-a', 'tasks', ['a'])],
      ['windowRows', () => storage.loadWindowRows('account-a', 'empty')],
      ['readView', () => storage.withReadView('account-a', { tasks: { key: 'id', complete: true } },
        view => view.select({ table: 'tasks', where: { column: 'id', op: 'eq', value: 'a' } }))],
    ]) {
      try { reads[label] = { value: await run() }; }
      catch (error) { reads[label] = { error: `${error.name}: ${error.message}` }; }
    }
    let commit, writeError, peer;
    try { commit = await storage.applyTransaction(transaction, initial, 'account-a'); }
    catch (error) { writeError = `${error.name}: ${error.message}`; }
    try { peer = { value: await storage.readChanges('account-a', priorSequence) }; }
    catch (error) { peer = { error: `${error.name}: ${error.message}` }; }
    return { before, after: await inspectReplicaDatabase(kind, name), reads, commit, writeError, peer,
      priorSequence, other: await storage.load('account-b') };
  } finally { raw.close(); storage.close(); }
}

const upgradeOptions = name => ({ replicaName: name, outboxName: name + '-queue',
  initialVersion: 1, contract: { version: 1, offlineMaxAgeMs: null }, migrations: [] });

async function seedFutureJournal(kind, name) {
  const stores = browserUpgradeStorage(upgradeOptions(name));
  try {
    await stores.ready;
    await stores.store.put({ id: 1, scope: 'account-a', path: 'write', receiptPath: 'historical.write',
      args: { value: 7 }, idempotencyKey: 'accepted-before-upgrade', entityKeys: ['a'],
      createdAt: 1, attempts: 1, nextAttemptAt: 0, state: 'committed', awaitingReplica: true });
  } finally { stores.close(); }
  const target = kind === 'upgrade-meta' ? name + '-upgrades' : name + '-queue';
  const before = await inspectNamedDatabase(target);
  await new Promise((resolve, reject) => {
    const request = indexedDB.open(target, before.version + 10);
    request.onupgradeneeded = () => {
      request.result.createObjectStore('future-contract', { keyPath: 'id' }).put({ id: 'accepted-history', accepted: true });
      if (kind !== 'upgrade-meta') request.transaction.objectStore('entries').deleteIndex('[scope+state]');
    };
    request.onsuccess = () => { request.result.close(); resolve(); };
    request.onerror = () => reject(request.error);
  });
  return inspectNamedDatabase(target);
}

async function readFutureJournal(kind, name) {
  let loaded, error;
  if (kind === 'direct-outbox') {
    const queue = new DexieReducerOutbox({ databaseName: name + '-queue' });
    try { loaded = await queue.loadAll('account-a'); }
    catch (failure) { error = `${failure.name}: ${failure.message}: ${failure.cause?.name ?? ''}`; }
    finally { queue.database?.close(); }
  } else {
    const stores = browserUpgradeStorage(upgradeOptions(name));
    try { await stores.ready; loaded = await stores.store.load(); }
    catch (failure) { error = `${failure.name}: ${failure.message}`; }
    finally { stores.close(); }
  }
  return { loaded, error, raw: await inspectNamedDatabase(kind === 'upgrade-meta' ? name + '-upgrades' : name + '-queue') };
}

async function replicaSequenceGuard(kind, name, sequence, repeat = false) {
  await seed(kind, name);
  const database = new Dexie(kind === 'engine' ? `${name}:engine-members-v1` : name);
  const storage = storageFor(kind, name);
  try {
    await database.open();
    await database.table('meta').put({ scope: 'account-a', key: 'sequence', value: String(sequence) });
    const rawBefore = await inspectReplicaDatabase(kind, name);
    let before, beforeError;
    try { before = await storage.load('account-a'); }
    catch (failure) { beforeError = `${failure.name}: ${failure.message}`; }
    let result, error;
    try { result = await storage.applyTransaction(transaction, initial, 'account-a'); }
    catch (failure) { error = `${failure.name}: ${failure.message}`; }
    let second, peerChanges;
    if (repeat) {
      if (!result || error) throw Error('source control did not reach the first committed sequence');
      const update = { cursor: cursor(9), changes: [{ entity: 'tasks', id: 'a', operation: 'update', newValue: { id: 'a', name: 'Second' } }] };
      second = await storage.applyTransaction(update, initial, 'account-a');
      peerChanges = await storage.readChanges('account-a', result.sequence);
    }
    let after, afterError;
    try { after = await storage.load('account-a'); }
    catch (failure) { afterError = `${failure.name}: ${failure.message}`; }
    return { result, error, second, peerChanges, before, beforeError, after, afterError,
      rawBefore, rawAfter: await inspectReplicaDatabase(kind, name), counter: await database.table('meta').get(['account-a', 'sequence']),
      other: await storage.load('account-b') };
  } finally { storage.close(); database.close(); }
}

async function replicaStartupSequence(kind, name, sequence) {
  await seed(kind, name);
  const database = new Dexie(kind === 'engine' ? `${name}:engine-members-v1` : name);
  const storage = storageFor(kind, name), replica = new LocalReplica(storage), publications = [];
  const stop = replica.subscribe(() => publications.push({ row: replica.entity('tasks', 'a') ?? null, cursor: replica.cursor() ?? null }));
  try {
    await database.open();
    const original = await database.table('meta').get(['account-a', 'sequence']);
    await database.table('meta').put({ ...original, value: String(sequence) });
    const raw = async () => Object.fromEntries(await Promise.all(database.tables.map(async table => [table.name, await table.toArray()])));
    const before = await raw();
    let error;
    try { await replica.activateScope('account-a'); }
    catch (failure) { error = `${failure.name}: ${failure.message}`; }
    await new Promise(resolve => setTimeout(resolve, 0));
    const result = { error, row: replica.entity('tasks', 'a') ?? null, cursor: replica.cursor() ?? null,
      publications: [...publications], before, after: await raw() };
    // Remove only the controlled fault and prove that the same replica can
    // recover without deleting or manufacturing a checkpoint.
    await database.table('meta').put(original);
    result.restoredCounter = await database.table('meta').get(['account-a', 'sequence']);
    try {
      await replica.activateScope('account-a', true);
      result.recovered = { row: replica.entity('tasks', 'a'), cursor: replica.cursor() };
    } catch (failure) { result.recoveryError = `${failure.name}: ${failure.message}`; }
    return result;
  } finally { stop(); replica.dispose(); storage.close(); database.close(); }
}

async function activationOwnerRace() {
  const storage = new MemoryLocalReplicaStorage(), replica = new LocalReplica(storage), calls = [];
  const snapshot = owner => ({ cursor: { epoch: owner, revision: 1 }, entities: { tasks: { one: { id: 'one', owner } } }, liveQueries: {} });
  let releaseAlice, releaseBob, aliceStarted, bobStarted;
  const aliceRead = new Promise(resolve => { releaseAlice = resolve; });
  const bobRead = new Promise(resolve => { releaseBob = resolve; });
  const aliceReady = new Promise(resolve => { aliceStarted = resolve; });
  const bobReady = new Promise(resolve => { bobStarted = resolve; });
  storage.load = scope => {
    calls.push(scope);
    if (scope === 'alice') { aliceStarted(); return aliceRead; }
    bobStarted(); return bobRead;
  };
  const alice = replica.activateScope('alice');
  let bob, repeated;
  try {
    await aliceReady;
    bob = replica.activateScope('bob');
    releaseAlice(snapshot('alice'));
    await alice; await bobReady;
    repeated = replica.activateScope('bob');
    const sameInFlight = repeated === bob;
    releaseBob(snapshot('bob'));
    await Promise.all([bob, repeated]);
    return { sameInFlight, calls, row: replica.entity('tasks', 'one'), cursor: replica.cursor() };
  } finally {
    releaseAlice(snapshot('alice')); releaseBob(snapshot('bob'));
    await Promise.allSettled([alice, bob, repeated].filter(Boolean));
    replica.dispose();
  }
}

async function activationIdentityDomain() {
  const storage = new MemoryLocalReplicaStorage(), replica = new LocalReplica(storage), loads = [], publications = [];
  replica.scopeActivationGeneration = Number.MAX_SAFE_INTEGER - 1;
  const snapshot = owner => ({ cursor: { epoch: owner, revision: 1 }, entities: { tasks: { one: { id: 'one', owner } } }, liveQueries: {} });
  let releaseAlice, started;
  const read = new Promise(resolve => { releaseAlice = resolve; });
  const ready = new Promise(resolve => { started = resolve; });
  storage.load = scope => {
    loads.push(scope);
    if (scope === 'alice') { started(); return read; }
    return Promise.resolve(snapshot(scope));
  };
  const stop = replica.subscribe(() => publications.push(replica.entity('tasks', 'one')?.owner ?? null));
  const observe = promise => promise.then(() => ({ error: null }), error => ({ error: String(error) }));
  const alice = replica.activateScope('alice');
  let bob, carol;
  try {
    await ready;
    bob = observe(replica.activateScope('bob'));
    carol = observe(replica.activateScope('carol'));
    releaseAlice(snapshot('alice'));
    await alice;
    const outcomes = await Promise.all([bob, carol]);
    return { loads, publications, outcomes, row: replica.entity('tasks', 'one') ?? null };
  } finally {
    releaseAlice(snapshot('alice'));
    await Promise.allSettled([alice, bob, carol].filter(Boolean));
    stop(); replica.dispose();
  }
}

async function activationListenerRace() {
  const storage = new MemoryLocalReplicaStorage(), replica = new LocalReplica(storage), loads = [], publications = [];
  const snapshot = owner => ({ cursor: { epoch: owner, revision: 1 }, entities: { tasks: { one: { id: 'one', owner } } }, liveQueries: {} });
  for (const owner of ['initial', 'alice', 'bob']) await storage.replaceSnapshot(snapshot(owner), owner);
  await replica.activateScope('initial');
  let releaseBob, started;
  const gate = new Promise(resolve => { releaseBob = resolve; });
  const ready = new Promise(resolve => { started = resolve; });
  const original = storage.load.bind(storage);
  storage.load = async scope => {
    loads.push(scope);
    if (scope === 'bob') { started(); await gate; }
    return original(scope);
  };
  let switched = false, bob;
  const stop = replica.subscribe(() => {
    publications.push(replica.entity('tasks', 'one')?.owner ?? null);
    if (!switched) { switched = true; bob = replica.activateScope('bob'); }
  });
  const alice = replica.activateScope('alice');
  try {
    await ready; releaseBob();
    await Promise.all([alice, bob]);
    return { requestedOwner: 'bob', row: replica.entity('tasks', 'one'), loads, publications };
  } finally {
    releaseBob(); await Promise.allSettled([alice, bob].filter(Boolean));
    stop(); replica.dispose();
  }
}

async function activationReturnRace() {
  const storage = new MemoryLocalReplicaStorage(), replica = new LocalReplica(storage), publications = [], loads = [];
  const snapshot = owner => ({ cursor: { epoch: owner, revision: 1 }, entities: { tasks: { one: { id: 'one', owner } } }, liveQueries: {} });
  for (const owner of ['alice', 'bob']) await storage.replaceSnapshot(snapshot(owner), owner);
  const original = storage.load.bind(storage);
  storage.load = scope => { loads.push(scope); return original(scope); };
  let phase = 0, bob, again;
  const stop = replica.subscribe(() => {
    const owner = replica.entity('tasks', 'one')?.owner ?? null;
    publications.push(owner);
    if (phase === 0 && owner === 'alice') { phase = 1; bob = replica.activateScope('bob'); }
    else if (phase === 1 && owner === null) { phase = 2; again = replica.activateScope('alice'); }
  });
  const alice = replica.activateScope('alice');
  try {
    await alice; await Promise.all([bob, again]);
    return { phase, publications, loads, reusedOldPromise: again === alice, row: replica.entity('tasks', 'one'), requestedOwner: 'alice' };
  } finally {
    await Promise.allSettled([alice, bob, again].filter(Boolean));
    stop(); replica.dispose();
  }
}

window.browserGuarantees = { seed, read, abortTransaction, commitTransaction,
  seedHistoricalReplica, readHistoricalReplica, inspectHistoricalReplica, abortHistoricalReplicaUpgrade, crashHistoricalReplicaUpgrade,
  seedFutureReplica, readFutureReplica,
  replicaCounterIntegrity,
  seedFutureJournal, readFutureJournal,
  seedNormalizedHistoricalReplica, interruptNormalizedHistoricalUpgrade, readNormalizedHistoricalReplica,
  inspectNamedDatabase,
  replicaSequenceGuard,
  replicaStartupSequence,
  activationOwnerRace,
  activationIdentityDomain, activationListenerRace,
  activationReturnRace,
  crashTransaction, snapshotBarrier, recoveredQueue, acceptedQueue };
