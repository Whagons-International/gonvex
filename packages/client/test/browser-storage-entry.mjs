import { GonvexClient } from '../src/index.ts';
import { IndexedDBLocalReplicaStorage } from '../src/indexeddb-replica.ts';
import { IndexedDBEngineReplicaStorage } from '../src/indexeddb-engine.ts';
import { DexieReducerOutbox } from '../src/outbox.ts';
import { replicaRowsHashes, replicaHashesDigest } from '../src/replica-integrity.ts';

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
  let witnessed = 0;
  IDBObjectStore.prototype.put = function (...args) {
    const request = original.apply(this, args);
    if (!witnessed && this.name === 'entities' && this.transaction.db.name.startsWith(name)) {
      witnessed = -1;
      request.addEventListener('success', () => {
        witnessed = 1;
        if (!crash) this.transaction.abort();
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
  return { count: () => witnessed, restore: () => { IDBObjectStore.prototype.put = original; } };
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

window.browserGuarantees = { seed, read, abortTransaction, commitTransaction,
  crashTransaction, snapshotBarrier, recoveredQueue, acceptedQueue };
