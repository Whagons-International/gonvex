import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { IDBFactory, IDBKeyRange, IDBObjectStore, IDBIndex } from 'fake-indexeddb';
import { Dexie } from 'dexie';
import { IndexedDBEngineReplicaStorage, indexedDBEngineReplica } from './indexeddb-engine';
import { IndexedDBLocalReplicaStorage } from './indexeddb-replica';
import { LocalReplica, type ReplicaWindow } from './local-replica';
import { GonvexClient } from './index';
import { applyReplicaHashDelta, replicaAggregateDigest, replicaRowsHashes } from './replica-integrity';

const stores: Array<IndexedDBEngineReplicaStorage | IndexedDBLocalReplicaStorage> = [];
const replicas: LocalReplica[] = [];
const clients: GonvexClient[] = [];
let oldIDB: typeof globalThis.indexedDB, oldRange: typeof globalThis.IDBKeyRange;
beforeEach(() => {
  oldIDB = globalThis.indexedDB; oldRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB; Dexie.dependencies.IDBKeyRange = IDBKeyRange;
});
afterEach(() => {
  clients.splice(0).forEach(client => client.close());
  replicas.splice(0).forEach(replica => replica.dispose());
  stores.splice(0).forEach(storage => storage.close());
  vi.restoreAllMocks(); vi.unstubAllGlobals();
  Object.assign(globalThis, { indexedDB: oldIDB, IDBKeyRange: oldRange });
  Dexie.dependencies.indexedDB = oldIDB; Dexie.dependencies.IDBKeyRange = oldRange;
});
function store(name = `engine-${Math.random()}`) { const value = new IndexedDBEngineReplicaStorage(name); stores.push(value); return value; }
function replica(storage: IndexedDBEngineReplicaStorage, maxResidentRows?: number) {
  const value = new LocalReplica(storage, { maxResidentRows }); replicas.push(value); return value;
}
function window(ids = ['a', 'b'], signature = 'tasks', revision = 1): ReplicaWindow {
  return { signature, kind: 'replica', entity: 'tasks', key: 'id', ids,
    completeness: 'complete', source: 'server', cursor: { epoch: 'e', revision } };
}
const empty = { entities: {}, liveQueries: {} };

it('persists identical replicas for legacy and compact transaction streams', async () => {
  const legacyStore = store(), compactStore = store();
  const legacy = replica(legacyStore), compact = replica(compactStore);
  await legacy.activateScope('tenant-a'); await compact.activateScope('tenant-a');
  let random = 0x13579bdf;
  let delayed: Parameters<LocalReplica['applyTransaction']>[0] | undefined;
  for (let revision = 1; revision <= 64; revision++) {
    random ^= random << 13; random ^= random >>> 17; random ^= random << 5;
    const id = `row-${(random >>> 0) % 8}`;
    const remove = (random >>> 4) % 4 === 0;
    const change = { entity: 'tasks', id, operation: remove ? 'delete' as const : 'update' as const,
      oldValue: legacy.entity('tasks', id),
      ...(remove ? {} : { newValue: { id, value: random & 1 ? null : random, nested: { revision } } }) };
    const transaction = { cursor: { epoch: 'e', revision }, changes: [change] };
    const { oldValue, ...withoutOld } = change;
    await legacy.applyTransaction(transaction, 'tenant-a');
    await compact.applyTransaction({ ...transaction, changes: [withoutOld] }, 'tenant-a');
    if (delayed && revision % 3 === 0) {
      await legacy.applyTransaction(delayed, 'tenant-a');
      await compact.applyTransaction(delayed, 'tenant-a');
    }
    delayed = { ...transaction, changes: [withoutOld] };
    expect(compact.entityRows('tasks')).toEqual(legacy.entityRows('tasks'));
  }
  expect(await compactStore.load('tenant-a')).toEqual(await legacyStore.load('tenant-a'));
  const restored = replica(compactStore);
  await restored.activateScope('tenant-a');
  const byId = (value: LocalReplica) => Object.fromEntries(value.entityRows('tasks').map(row => [String(row.id), row]));
  expect(byId(restored)).toEqual(byId(compact));
  await restored.activateScope('tenant-b');
  await restored.applyTransaction(delayed!, 'tenant-a');
  expect(restored.entityRows('tasks')).toEqual([]);
});

async function seed(storage: IndexedDBEngineReplicaStorage, signature = 'tasks', revision = 1) {
  const value = window(['a', 'b'], signature, revision);
  await storage.replaceWindow(value, { ...empty, cursor: value.cursor }, 'default', [{ id: 'a', title: 'A', status: 'new' }, { id: 'b', title: 'B' }]);
}

it('batches stationary membership hashes and startup patches without changing order or scope', async () => {
  const storage = store(), db = (storage as any).database;
  const before = Array.from({ length: 300 }, (_, i) => ({ id: String(i), status: 'open' }));
  const after = before.map(row => ({ ...row, status: 'done' }));
  const first = { ...window(before.map(row => row.id)), hashes: await replicaRowsHashes(before, 'id') };
  await storage.replaceWindow(first, empty, 'default', before);
  const next = { ...first, cursor: { epoch: 'e', revision: 2 }, hashes: await replicaRowsHashes(after, 'id') };
  const reads = vi.spyOn(db.members, 'get'), writes = vi.spyOn(db.members, 'put');
  const bulkReads = vi.spyOn(db.members, 'bulkGet'), bulkWrites = vi.spyOn(db.members, 'bulkPut');
  await storage.applyWindowDelta(next, { upserts: after, deleted: [] }, { ...empty, cursor: next.cursor });
  expect(reads).not.toHaveBeenCalled(); expect(writes).not.toHaveBeenCalled();
  expect(bulkReads).toHaveBeenCalledTimes(3); expect(bulkWrites).toHaveBeenCalledTimes(3);
  const loaded = await storage.load();
  expect(loaded!.liveQueries.tasks.ids).toEqual(first.ids);
  expect(loaded!.liveQueries.tasks.hashes).toEqual(next.hashes);
  expect(Object.values(loaded!.entities.tasks).every((row: any) => row.status === 'done')).toBe(true);
  const startup = await storage.loadWorkingSet('default', { maxRows: 1000, maxBytes: Infinity });
  expect(startup!.liveQueries.tasks.ids).toEqual(first.ids);
  expect(startup!.liveQueries.tasks.hashes).toEqual(next.hashes);
  expect(await storage.load('other-tenant')).toBeUndefined();
});

it('rolls back batched members, row authority and cursor if a startup patch fails', async () => {
  const storage = store(), db = (storage as any).database;
  await seed(storage);
  const before = await storage.load();
  const failure = vi.spyOn(db.meta, 'bulkPut').mockRejectedValueOnce(new Error('startup patch failed'));
  const next = window(['a', 'b'], 'tasks', 2);
  await expect(storage.applyWindowDelta(next, { upserts: [{ id: 'a', title: 'Changed' }], deleted: [] },
    { ...empty, cursor: next.cursor })).rejects.toThrow('startup patch failed');
  failure.mockRestore();
  expect(await storage.load()).toEqual(before);
  const startup = await storage.loadWorkingSet('default', { maxRows: 100, maxBytes: Infinity });
  expect(startup!.entities.tasks.a).toEqual(before!.entities.tasks.a);
  expect(startup!.cursor).toEqual(before!.cursor);
});

it('isolates the engine namespace from an open legacy database and exports a client storage option', async () => {
  const name = 'shared-name';
  const legacy = new IndexedDBLocalReplicaStorage(name); stores.push(legacy);
  await legacy.replaceWindow(window(), { entities: { tasks: { a: { id: 'a', title: 'legacy' } } }, liveQueries: {} });
  const engine = indexedDBEngineReplica(name) as IndexedDBEngineReplicaStorage; stores.push(engine);
  expect(await engine.load()).toBeUndefined();
  await seed(engine);
  expect((await legacy.load())?.entities.tasks.a.title).toBe('legacy');
  const client = new GonvexClient('ws://runtime.test/ws', { errorReporting: false, localReplica: { storage: engine } }); clients.push(client);
  expect((client as any).localStorage).toBe(engine);
  const defaultClient = new GonvexClient('ws://runtime.test/ws', { errorReporting: false }); clients.push(defaultClient);
  expect((defaultClient as any).localStorage).toBeUndefined();
});

it('writes one member and metadata only, and never rereads its adjacent row commits', async () => {
  const storage = store(), local = replica(storage);
  const rows = Array.from({ length: 1_000 }, (_, index) => ({ id: `id-${index}`, title: 'old' }));
  await local.replaceWindow({ ...window(), rows });
  const db = (storage as any).database;
  const memberPut = vi.spyOn(db.members, 'put'), fullMembers = vi.spyOn(db.members, 'bulkPut');
  const metadataPut = vi.spyOn(db.windows, 'put'), reads = vi.spyOn(storage, 'readChanges');
  await local.applyWindowDelta({ signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id',
    upserts: [{ id: 'id-500', title: 'new' }], deleted: [], cursor: { epoch: 'e', revision: 2 } });
  expect(reads).not.toHaveBeenCalled(); expect(memberPut).not.toHaveBeenCalled();
  expect(fullMembers.mock.calls.flatMap(call => call[0])).toHaveLength(1);
  expect(JSON.parse(metadataPut.mock.calls[0][0].value)).not.toHaveProperty('ids');
  expect(JSON.parse(metadataPut.mock.calls[0][0].value)).not.toHaveProperty('hashes');
  await local.applyTransaction({ cursor: { epoch: 'e', revision: 3 }, changes: [{ entity: 'tasks', id: 'id-500', operation: 'update', newValue: { title: 'transaction' } }] });
  expect(reads).not.toHaveBeenCalled();
  expect(local.entity('tasks', 'id-500')?.title).toBe('transaction');
}, 30_000);

it('rolls back rows, members, sequence and checkpoint when a write fails between them', async () => {
  const storage = store(); await seed(storage);
  const before = await storage.load();
  const db = (storage as any).database;
  const failure = vi.spyOn(db.windows, 'put').mockRejectedValueOnce(new Error('checkpoint failed'));
  await expect(storage.applyWindowDelta(window(['a', 'c'], 'tasks', 2),
    { upserts: [{ id: 'c', title: 'C' }], deleted: ['b'], memberships: [{ id: 'c', moved: true, after: 'a' }] },
    { ...empty, cursor: { epoch: 'e', revision: 2 } })).rejects.toThrow('checkpoint failed');
  failure.mockRestore();
  expect(await storage.load()).toEqual(before);
  const receipt = await storage.applyWindowDelta(window(['a', 'c'], 'tasks', 2),
    { upserts: [{ id: 'c', title: 'C' }], deleted: ['b'], memberships: [{ id: 'c', moved: true, after: 'a' }] },
    { ...empty, cursor: { epoch: 'e', revision: 2 } });
  expect(receipt).toMatchObject({ previousSequence: 1, sequence: 2, changes: { entities: { tasks: { b: null, c: { id: 'c', title: 'C' } } } } });
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['a', 'c']);
});

it('preserves per-field authority and tombstones in receipts and after reopening', async () => {
  const name = 'authority'; let storage = store(name); await seed(storage, 'tasks', 10);
  const local = replica(storage); await local.hydrate();
  // The disk window is newer than the connection-wide floor, as with overlapping projections.
  (local as any).cursorValue = { epoch: 'e', revision: 1 };
  const reads = vi.spyOn(storage, 'readChanges');
  await local.applyTransaction({ cursor: { epoch: 'e', revision: 9 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'update', newValue: { title: 'stale', description: 'new field' } },
    { entity: 'tasks', id: 'b', operation: 'delete' },
  ] });
  expect(reads).not.toHaveBeenCalled();
  expect(local.entity('tasks', 'a')).toEqual({ id: 'a', title: 'A', status: 'new', description: 'new field' });
  expect(local.entity('tasks', 'b')).toEqual({ id: 'b', title: 'B' });
  expect(local.getWindow('tasks')?.ids).toEqual(['a', 'b']);
  await storage.applyTransaction({ cursor: { epoch: 'e', revision: 11 }, changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] }, empty);
  const receipt = await storage.applyTransaction({ cursor: { epoch: 'e', revision: 10 }, changes: [{ entity: 'tasks', id: 'a', operation: 'update', newValue: { title: 'resurrect' } }] }, empty);
  expect(receipt.changes.entities.tasks.a).toBeNull();
  local.dispose(); storage.close(); storage = store(name);
  expect((await storage.load())?.entities.tasks.a).toBeUndefined();
  expect((await storage.load())?.cursor?.revision).toBe(11);
});

it('uses reverse ownership for exclusion and deletes from every active owning window', async () => {
  const storage = store(); await seed(storage); await seed(storage, 'other');
  await storage.applyWindowDelta(window(['b'], 'tasks', 2), { upserts: [], deleted: ['a'], memberships: [] }, empty);
  expect((await storage.load())?.entities.tasks.a).toBeDefined();
  const db = (storage as any).database;
  const allWindows = vi.spyOn(db.windows, 'where');
  await storage.applyTransaction({ cursor: { epoch: 'e', revision: 3 }, changes: [{ entity: 'tasks', id: 'b', operation: 'delete' }] }, empty);
  expect(allWindows).not.toHaveBeenCalled();
  const after = await storage.load();
  expect(after?.liveQueries.tasks.ids).toEqual([]); expect(after?.liveQueries.other.ids).toEqual(['a']);
  expect(after?.entities.tasks.b).toBeUndefined();
});

it.each(['delta', 'replacement'] as const)('does not prune rows for a rejected stale window %s', async method => {
  const name = `stale-window-${method}`; let storage = store(name);
  await seed(storage);
  const current = window(['a', 'b'], 'tasks', 10);
  // A watermark advances membership authority without rewriting the row fields.
  await storage.advanceWatermark([current], current.cursor);
  const before = (await storage.load())!;
  const stale = window(['b'], 'tasks', 5);
  const snapshot = { ...empty, cursor: stale.cursor };
  if (method === 'delta') {
    const receipt = await storage.applyWindowDelta(stale, { upserts: [], deleted: ['a'] }, snapshot);
    // No row is rewritten, but RAM still needs its unchanged committed value.
    expect(receipt.changes.entities).toEqual({ tasks: { a: before.entities.tasks.a } });
    expect(receipt.changes.windows.tasks).toEqual(current);
  } else {
    await storage.replaceWindow(stale, snapshot);
  }
  expect((await storage.readChanges('default', before.storageSequence!)).entities).toEqual({});
  storage.close(); storage = store(name);
  expect(await storage.load()).toMatchObject({
    cursor: before.cursor, entities: before.entities, liveQueries: before.liveQueries,
  });
});

it('resets rows and membership on epoch change and refuses a retired epoch', async () => {
  const storage = store(); await seed(storage);
  const next = { ...window(['z']), cursor: { epoch: 'next', revision: 1 } };
  await storage.replaceWindow(next, { ...empty, cursor: next.cursor }, 'default', [{ id: 'z' }]);
  expect((await storage.readChanges('default', 1)).reset).toBe(true);
  expect((await storage.load())?.entities.tasks).toEqual({ z: { id: 'z' } });
  await expect(storage.applyWindowDelta(window(), { upserts: [], deleted: [] }, empty)).rejects.toThrow('obsolete replica epoch');
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['z']);
});

it('preserves server order, duplicate IDs and result skeleton across generation replacement and ordered moves', async () => {
  const storage = store();
  const value = { ...window(['a', 'b', 'a']), kind: 'live' as const, resultSkeleton: [{ id: 'a' }, { id: 'b' }, { id: 'a' }] };
  await storage.replaceWindow(value, empty, 'default', [{ id: 'a' }, { id: 'b' }]);
  expect((await storage.load())?.liveQueries.tasks).toMatchObject({ ids: value.ids, resultSkeleton: value.resultSkeleton });
  await seed(storage);
  const rebalances = vi.spyOn((storage as any).database.members, 'bulkPut');
  for (let index = 0; index < 240; index++) {
    const id = `insert-${index}`;
    const previous = (await storage.load())!.liveQueries.tasks;
    const ids = ['a', id, ...previous.ids.slice(1)];
    await storage.applyWindowDelta({ ...previous, ids, cursor: { epoch: 'e', revision: index + 2 } },
      { upserts: [{ id }], deleted: [], memberships: [{ id, moved: true, after: 'a', before: previous.ids[1] }] }, empty);
    expect((await storage.load())?.liveQueries.tasks.ids).toEqual(ids);
  }
  expect(rebalances).toHaveBeenCalled();
  expect(rebalances.mock.calls.every(([records]) => (records as any[]).length <= 33)).toBe(true);
  const local = replica(storage); await local.hydrate();
  await local.replaceWindow({ ...window(), rows: [{ id: 'a', rank: 1 }, { id: 'b', rank: 2 }], orderBy: 'rank', orderDirection: 'asc', cursor: { epoch: 'e', revision: 1000 } });
  await local.applyWindowDelta({ signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id', upserts: [{ id: 'a', rank: 3 }], deleted: [], cursor: { epoch: 'e', revision: 1001 } });
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['b', 'a']);
  for (let index = 0; index < 10; index++) await storage.cleanupRetiredMemberships('default', 20);
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['b', 'a']);
}, 30_000);

it('hydrates a bounded working set without turning cold or partial rows into proven absence', async () => {
  const storage = store(); await seed(storage);
  const snapshot = await storage.loadWorkingSet('default', { maxRows: 1, maxBytes: 4096 });
  expect(Object.keys(snapshot!.entities.tasks)).toHaveLength(1);
  expect(snapshot?.residentPartialTables).toEqual(['tasks']);
  expect(snapshot?.liveQueries.tasks.completeness).toBe('complete');
  expect((await storage.loadEntityRows('default', 'tasks', ['b']))[0].row).toEqual({ id: 'b', title: 'B' });
  const local = replica(storage, 1); await local.hydrate();
  expect(local.entity('tasks', 'b')).toBeUndefined();
  expect((await storage.loadWindowRows('default', 'tasks'))?.rows).toHaveLength(2);
});

it('round-trips binary projected hashes and still verifies cursor+digest resume after a delta and watermark', async () => {
  const storage = store(); const local = replica(storage);
  const rows = [{ id: 'a', value: 1 }, { id: 'b', value: 2 }];
  const hashes = await replicaRowsHashes(rows, 'id');
  await local.replaceWindow({ ...window(), rows, hashes });
  const upserts = [{ id: 'a', value: 3 }];
  const next = await applyReplicaHashDelta(hashes, upserts, [], 'id');
  await local.applyWindowDelta({ signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id', upserts, deleted: [], hashes: next, cursor: { epoch: 'e', revision: 2 } });
  const members = await (storage as any).database.members.toArray();
  expect(members.every((member: any) => member.hash instanceof Uint8Array && member.hash.length === 32)).toBe(true);
  const puts = vi.spyOn((storage as any).database.members, 'put');
  await local.advanceWatermark(3, ['tasks']); expect(puts).not.toHaveBeenCalled();
  const restored = replica(storage); await restored.hydrate();
  expect(restored.windowResumeState('tasks')).toEqual({ cursor: { epoch: 'e', revision: 3 }, digest: replicaAggregateDigest(next) });
  expect(restored.getWindow('tasks')?.hashes).toEqual(next);
  const client = new GonvexClient('ws://runtime.test/ws', { errorReporting: false, localReplica: { storage } }); clients.push(client);
  const state = client as any; await state.replica.hydrate(); state.serverCapabilities = { replicaResume: 1 };
  expect(state.replicaOpenRequest({ key: 'tasks', id: 'r', path: 'tasks.list', args: {} })).toEqual({ id: 'r', path: 'tasks.list', args: {}, cursor: { epoch: 'e', revision: 3 }, digest: replicaAggregateDigest(next) });
});

it('notifies other tabs only after commit and catches a peer sequence gap before accepting an own receipt', async () => {
  const name = 'peers'; const writer = store(name), follower = store(name);
  await seed(writer);
  const local = replica(follower); await local.hydrate();
  const notification = vi.fn(); follower.subscribePeer(notification);
  await writer.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [{ entity: 'tasks', id: 'a', operation: 'update', newValue: { title: 'peer' } }] }, empty);
  await vi.waitFor(() => expect(notification).toHaveBeenCalledWith('default'));
  await local.synchronizeStorage(); expect(local.entity('tasks', 'a')?.title).toBe('peer');
  // Force a peer write between hydration and our own commit.
  const isolated = replica(writer); await isolated.hydrate();
  await follower.applyTransaction({ cursor: { epoch: 'e', revision: 3 }, changes: [{ entity: 'tasks', id: 'b', operation: 'update', newValue: { title: 'gap' } }] }, empty);
  const reads = vi.spyOn(writer, 'readChanges');
  await isolated.applyTransaction({ cursor: { epoch: 'e', revision: 4 }, changes: [{ entity: 'tasks', id: 'a', operation: 'update', newValue: { title: 'own' } }] });
  expect(isolated.entity('tasks', 'b')?.title).toBe('gap');
  expect(reads).toHaveBeenCalled();
});

it('publishes no cross-entity partial state or peer notification if checkpoint persistence fails', async () => {
  const storage = store(), peer = store((storage as any).database.name.replace(':engine-members-v1', ''));
  await seed(storage); const local = replica(storage); await local.hydrate();
  const before = await storage.load(); const publish = vi.fn(), notifyPeer = vi.fn();
  local.subscribe(publish); peer.subscribePeer(notifyPeer);
  // Let the seed notification settle before injecting a later commit failure.
  await new Promise(resolve => setTimeout(resolve, 20)); notifyPeer.mockClear();
  const table = (storage as any).database.meta, put = table.put.bind(table);
  const fail = vi.spyOn(table, 'put').mockImplementation((record: any) => {
    if (record.key === 'cursor') throw new Error('crash at checkpoint');
    return put(record);
  });
  await expect(local.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'update', newValue: { title: 'partial' } },
    { entity: 'statuses', id: 'new', operation: 'insert', newValue: { id: 'new', name: 'partial' } },
  ] })).rejects.toThrow('crash at checkpoint');
  fail.mockRestore();
  expect(await storage.load()).toEqual(before);
  expect(local.entity('tasks', 'a')?.title).toBe('A'); expect(local.entity('statuses', 'new')).toBeUndefined();
  expect(publish).not.toHaveBeenCalled();
  await new Promise(resolve => setTimeout(resolve, 20)); expect(notifyPeer).not.toHaveBeenCalled();
});

it('updates field authority even for equal visible values and restores hashes on an unchanged snapshot', async () => {
  const storage = store(), local = replica(storage);
  const rows = [{ id: 'a', value: 1 }];
  await local.replaceWindow({ ...window(['a']), rows });
  await local.applyWindowDelta({ signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id', upserts: rows, deleted: [], cursor: { epoch: 'e', revision: 10 } });
  const receipt = await storage.applyTransaction({ cursor: { epoch: 'e', revision: 9 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'update', newValue: { value: 2 } },
  ] }, empty);
  expect(receipt.changes.entities.tasks.a?.value).toBe(1);
  await local.synchronizeStorage();
  const hashes = await replicaRowsHashes(rows, 'id');
  await local.replaceWindow({ ...window(['a'], 'tasks', 11), rows, hashes });
  expect((await storage.load())?.liveQueries.tasks.hashes).toEqual(hashes);
});

it('moves several ordered members in final sequence without rewriting other memberships', async () => {
  const storage = store(), local = replica(storage);
  await local.replaceWindow({ ...window(['a', 'b', 'c', 'd']), rows: [
    { id: 'a', rank: 1 }, { id: 'b', rank: 2 }, { id: 'c', rank: 3 }, { id: 'd', rank: 4 },
  ], orderBy: 'rank', orderDirection: 'asc' });
  const bulk = vi.spyOn((storage as any).database.members, 'bulkPut');
  await local.applyWindowDelta({ signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id',
    orderBy: 'rank', orderDirection: 'asc', cursor: { epoch: 'e', revision: 2 },
    upserts: [{ id: 'a', rank: 6 }, { id: 'b', rank: 5 }], deleted: [] });
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['c', 'd', 'b', 'a']);
  expect(bulk).not.toHaveBeenCalled();
});

it('preserves ordered deltas through the original storage call shape', async () => {
  const storage = store(); await seed(storage);
  await storage.applyWindowDelta({ ...window(['b', 'c', 'a'], 'tasks', 2), orderBy: 'rank' },
    { upserts: [{ id: 'a', rank: 3 }, { id: 'c', rank: 2 }], deleted: [] }, empty);
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['b', 'c', 'a']);
});

it('activates only the final replacement when a transaction repeats a window signature', async () => {
  const storage = store(); await seed(storage);
  await storage.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [],
    memberships: [window(['a'], 'tasks', 2), window(['b'], 'tasks', 2)] }, empty);
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['b']);
  await storage.cleanupRetiredMemberships();
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['b']);
});

it('keeps rejected stale-write corrections when a peer commit creates a sequence gap', async () => {
  const name = 'stale-peer-gap'; const storage = store(name), peer = store(name);
  await seed(storage, 'tasks', 10);
  await storage.replaceWindow({ ...window(['s'], 'statuses', 1), entity: 'statuses' }, empty, 'default', [{ id: 's', title: 'before' }]);
  const local = replica(storage); await local.hydrate();
  await peer.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [
    { entity: 'statuses', id: 's', operation: 'update', newValue: { title: 'peer' } },
  ] }, empty);
  const reads = vi.spyOn(storage, 'readChanges');
  await local.applyTransaction({ cursor: { epoch: 'e', revision: 9 }, changes: [
    { entity: 'tasks', id: 'b', operation: 'delete' },
  ] });
  expect(reads).toHaveBeenCalled();
  expect(local.entity('tasks', 'b')).toEqual({ id: 'b', title: 'B' });
  expect(local.getWindow('tasks')?.ids).toEqual(['a', 'b']);
  expect(local.entity('statuses', 's')?.title).toBe('peer');
});

it('preserves tombstones and newer fields when a same-epoch full snapshot is replayed', async () => {
  const storage = store(); await seed(storage, 'tasks', 10);
  await storage.applyTransaction({ cursor: { epoch: 'e', revision: 11 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'delete' },
  ] }, empty);
  const old = window(['a', 'b'], 'tasks', 9);
  await storage.replaceSnapshot({ cursor: old.cursor, liveQueries: { tasks: old }, entities: { tasks: {
    a: { id: 'a', title: 'resurrect' }, b: { id: 'b', title: 'stale', description: 'new projection' },
  } } });
  const restored = await storage.load();
  expect(restored?.cursor?.revision).toBe(11);
  expect(restored?.entities.tasks.a).toBeUndefined();
  expect(restored?.entities.tasks.b).toEqual({ id: 'b', title: 'B', description: 'new projection' });
  expect(restored?.liveQueries.tasks.ids).toEqual(['b']);
  await storage.replaceSnapshot({ cursor: { epoch: 'e', revision: 12 }, liveQueries: {}, entities: {} });
  expect((await storage.load())?.entities).toEqual({});
  expect(Object.values((await storage.load())!.liveQueries).every(window => window.ids.length === 0)).toBe(true);
  await storage.replaceWindow(window(['a'], 'tasks', 10), empty, 'default', [{ id: 'a', title: 'late again' }]);
  expect((await storage.load())?.entities.tasks?.a).toBeUndefined();
});

it('retires a removed window without scanning or deleting its memberships on the interactive path', async () => {
  const storage = store(); await seed(storage);
  const db = (storage as any).database, members = vi.spyOn(db.members, 'where');
  await storage.removeWindow('tasks', empty);
  expect(members).not.toHaveBeenCalled();
  expect((await storage.readChanges('default', 1)).windows.tasks).toBeNull();
  expect((await storage.load())?.liveQueries).toEqual({});
  await storage.cleanupRetiredMemberships();
  expect(await db.members.count()).toBe(0);
  expect((await db.meta.toArray()).some((record: any) => record.key.startsWith('startup:'))).toBe(false);
  expect((await storage.load())?.entities.tasks.a).toBeDefined();
});

it('continues idle retirement cleanup past an empty generation', async () => {
  const storage = store();
  await storage.replaceWindow(window([], 'empty'), empty);
  await storage.replaceWindow(window([], 'empty', 2), empty);
  await seed(storage, 'z-real'); await seed(storage, 'z-real', 2);
  const table = (storage as any).database.members;
  await vi.waitFor(async () => expect(await table.count()).toBe(2));
  expect((await storage.load())?.liveQueries['z-real'].ids).toEqual(['a', 'b']);
});

it('retains the exact collection certificate after a normalized delete and verifies the resumed delta', async () => {
  const storage = store(), local = replica(storage);
  const rows = [{ id: 'a', value: 1 }, { id: 'b', value: 2 }];
  const hashes = await replicaRowsHashes(rows, 'id');
  await local.replaceWindow({ ...window(), rows, hashes });
  await local.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'delete' },
  ] });
  const client = new GonvexClient('ws://runtime.test/ws', { errorReporting: false, localReplica: { storage } }); clients.push(client);
  const state = client as any; await state.replica.hydrate(); state.serverCapabilities = { replicaResume: 1 }; state.replicaScope = 'default';
  const subscription = { key: 'tasks', id: 'r', path: 'tasks.list', args: {}, entity: 'tasks',
    columns: ['id', 'value'], retiredEpochs: new Set(), verificationGeneration: 1, forceFullIntegrity: false };
  state.emitReplicaMessage = vi.fn();
  expect(state.replica.getWindow('tasks')).toMatchObject({ ids: ['b'], hashes });
  expect(state.replicaOpenRequest(subscription)).toEqual({ id: 'r', path: 'tasks.list', args: {},
    cursor: { epoch: 'e', revision: 1 }, digest: replicaAggregateDigest(hashes) });
  await state.handleReplicaMessage(subscription, { type: 'replica.delta', id: 'r', cursor: { epoch: 'e', revision: 2 }, upserts: [], deleted: ['a'] });
  await state.handleReplicaMessage(subscription, { type: 'replica.ready', id: 'r', cursor: { epoch: 'e', revision: 2 }, digest: replicaAggregateDigest({ b: hashes.b }) });
  expect(subscription).toMatchObject({ isUpToDate: true, forceFullIntegrity: false });
  expect((await storage.load())?.liveQueries.tasks.hashes).toEqual({ b: hashes.b });
  expect(await (storage as any).database.members.where('[scope+entity+id]').equals(['default', 'tasks', 'a']).count()).toBe(0);
});

it('reconciles checkpoint-only hashes when an unchanged full snapshot advances the collection', async () => {
  const storage = store(), local = replica(storage);
  const rows = [{ id: 'a', value: 1 }, { id: 'b', value: 2 }];
  const hashes = await replicaRowsHashes(rows, 'id');
  await local.replaceWindow({ ...window(), rows, hashes });
  await local.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'delete' },
  ] });
  expect((await storage.load())?.liveQueries.tasks.hashes).toEqual(hashes);
  const full = vi.spyOn(storage, 'replaceWindow');
  await local.replaceWindow({ ...window(['b'], 'tasks', 2), rows: [rows[1]], hashes: { b: hashes.b } });
  expect(full).not.toHaveBeenCalled();
  expect((await storage.load())?.liveQueries.tasks.hashes).toEqual({ b: hashes.b });
  expect(await (storage as any).database.members.where('[scope+entity+id]').equals(['default', 'tasks', 'a']).count()).toBe(0);
});

it('preserves checkpoint-only hashes through full snapshot replacement and reopening', async () => {
  const name = 'snapshot-certificate'; let storage = store(name);
  const local = replica(storage);
  const rows = [{ id: 'a', value: 1 }, { id: 'b', value: 2 }];
  const hashes = await replicaRowsHashes(rows, 'id');
  await local.replaceWindow({ ...window(), rows, hashes });
  await local.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'delete' },
  ] });
  const snapshot = (await storage.load())!;
  expect(snapshot.liveQueries.tasks).toMatchObject({ ids: ['b'], hashes });
  await storage.replaceSnapshot(snapshot);
  local.dispose(); storage.close(); storage = store(name);
  const restored = replica(storage); await restored.hydrate();
  expect((await storage.load())?.liveQueries.tasks).toEqual(snapshot.liveQueries.tasks);
  expect(restored.windowResumeState('tasks')).toEqual({
    cursor: snapshot.liveQueries.tasks.cursor, digest: replicaAggregateDigest(hashes),
  });
  // A checkpoint hash must not keep a row alive as an owning membership.
  await storage.replaceWindow(window(['a'], 'other', 3), empty, 'default', [rows[0]]);
  await storage.applyWindowDelta(window([], 'other', 4), { upserts: [], deleted: ['a'] }, empty);
  expect((await storage.load())?.entities.tasks.a).toBeUndefined();
  expect((await storage.load())?.liveQueries.tasks.hashes).toEqual(hashes);
});

it('pages startup rows across tombstones and scopes while respecting row and byte budgets', async () => {
  const storage = store(); await seed(storage);
  const db = (storage as any).database;
  const template = await db.entities.get(['default', 'tasks', 'a']);
  await db.entities.bulkPut(Array.from({ length: 2050 }, (_, index) => ({ ...template,
    id: `page-${String(index).padStart(4, '0')}`, deleted: index < 2048,
    value: JSON.stringify({ id: `page-${String(index).padStart(4, '0')}` }),
  })));
  await db.entities.put({ ...template, scope: 'other', id: 'foreign' });
  const getAll = vi.spyOn(IDBObjectStore.prototype, 'getAll');
  const parse = JSON.parse;
  let readsBeforeFirstDecode = 0;
  vi.spyOn(JSON, 'parse').mockImplementation((value: string, reviver?: any) => {
    if (value === template.value && !readsBeforeFirstDecode) readsBeforeFirstDecode = getAll.mock.instances.filter(table => table?.name === 'entities').length;
    return parse(value, reviver);
  });
  const loaded = await storage.loadWorkingSet('default', { maxRows: 5000, maxBytes: 10000000 });
  expect(readsBeforeFirstDecode).toBe(2);
  expect(getAll.mock.instances.filter(table => table?.name === 'entities')).toHaveLength(9);
  expect(Object.keys(loaded!.entities.tasks).sort()).toEqual(['a', 'b', 'page-2048', 'page-2049']);
  expect(loaded?.liveQueries.tasks.ids).toEqual(['a', 'b']);
  const bounded = await storage.loadWorkingSet('default', { maxRows: 1, maxBytes: 10000 });
  expect(Object.keys(bounded!.entities.tasks)).toEqual(['a']);
  expect(bounded?.residentPartialTables).toEqual(['tasks']);
  const noBytes = await storage.loadWorkingSet('default', { maxRows: 10, maxBytes: 0 });
  expect(noBytes?.entities).toEqual({});
  expect(noBytes?.residentPartialTables).toEqual(['tasks']);
}, 30_000);

it('preserves duplicates and special IDs while decoding all 256 hash byte values', async () => {
  const storage = store();
  const unique = ['a', 'b', '__proto__', 'c', 'd', 'e', 'f', 'g'];
  const ids = [...unique, 'a'];
  const hashes = Object.fromEntries(unique.map((id, index) => [id,
    Array.from({ length: 32 }, (_, byte) => ((byte + index * 32) % 256).toString(16).padStart(2, '0')).join('')]));
  await storage.replaceWindow({ ...window(ids), hashes }, empty, 'default', ids.map(id => ({ id })));
  const loaded = await storage.loadWorkingSet('default', { maxRows: 10, maxBytes: 10000 });
  expect(loaded?.liveQueries.tasks.ids).toEqual(ids);
  expect(loaded?.liveQueries.tasks.hashes).toEqual(hashes);
  expect(Object.hasOwn(loaded!.liveQueries.tasks.hashes!, '__proto__')).toBe(true);
});

it('loads the transactional startup index without reading members and falls back for missing or stale indexes', async () => {
  const storage = store(); await seed(storage);
  const db = (storage as any).database;
  const before = await storage.load();
  const members = vi.spyOn(db.members, 'where');
  expect((await storage.loadWorkingSet('default', { maxRows: 10, maxBytes: 10000 }))?.liveQueries).toEqual(before?.liveQueries);
  expect(members).not.toHaveBeenCalled();
  const record = await db.windows.get(['default', 'tasks']);
  const key = `startup:${JSON.stringify(['tasks', record.generation])}:`;
  const cached = await db.meta.get(['default', key]);
  await db.meta.delete(['default', key]);
  expect((await storage.load())?.liveQueries).toEqual(before?.liveQueries);
  expect(members).toHaveBeenCalled(); members.mockClear();
  await db.meta.put({ ...cached, value: JSON.stringify({ sequence: -1, ids: ['wrong'] }) });
  expect((await storage.load())?.liveQueries).toEqual(before?.liveQueries);
  expect(members).toHaveBeenCalled();
});

it('keeps startup edits atomic with membership writes and does not revive an index invalidated by an old writer', async () => {
  const storage = store(); await seed(storage);
  const db = (storage as any).database;
  const before = await storage.load();
  const cached = await db.meta.toArray();
  const write = db.meta.put.bind(db.meta);
  const put = vi.spyOn(db.meta, 'put').mockImplementation((record: any) => {
    if (record.key.startsWith('startup:')) throw new Error('cache write failed');
    return write(record);
  });
  await expect(storage.applyWindowDelta(window(['a', 'c'], 'tasks', 2),
    { upserts: [{ id: 'c' }], deleted: ['b'], memberships: [{ id: 'c', moved: true, after: 'a' }] }, empty)).rejects.toThrow('cache write failed');
  put.mockRestore();
  expect(await storage.load()).toEqual(before);
  expect(await db.meta.toArray()).toEqual(cached);
  await storage.applyWindowDelta(window(['a', 'c'], 'tasks', 2),
    { upserts: [{ id: 'c' }], deleted: ['b'], memberships: [{ id: 'c', moved: true, after: 'a' }] }, empty);
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['a', 'c']);
  // Older SDKs copy an old startup marker on normalized deletes, or omit it
  // on window metadata writes. Neither may make the cached IDs authoritative.
  const record = await db.windows.get(['default', 'tasks']);
  await db.transaction('rw', db.members, db.windows, async () => {
    await db.members.delete(['default', 'tasks', record.generation, 'c']);
    await db.windows.put({ ...record, sequence: record.sequence + 1 });
  });
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['a']);
  await storage.advanceWatermark([window(['a'], 'tasks', 3)], { epoch: 'e', revision: 3 });
  expect((await db.windows.get(['default', 'tasks'])).startupSequence).toBeUndefined();
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['a']);
});

it('rolls back an inactive generation when its startup snapshot cannot be persisted', async () => {
  const storage = store(); await seed(storage);
  const db = (storage as any).database;
  const before = await storage.load(), members = await db.members.toArray(), metadata = await db.meta.toArray();
  const write = db.meta.put.bind(db.meta);
  const put = vi.spyOn(db.meta, 'put').mockImplementation((record: any) => {
    if (record.key.startsWith('startup:') && record.key.endsWith(':')) throw new Error('startup snapshot failed');
    return write(record);
  });
  await expect(storage.replaceWindow(window(['z'], 'tasks', 2), empty, 'default', [{ id: 'z' }])).rejects.toThrow('startup snapshot failed');
  put.mockRestore();
  expect(await storage.load()).toEqual(before);
  expect(await db.members.toArray()).toEqual(members);
  expect(await db.meta.toArray()).toEqual(metadata);
});

it('propagates a prefetched native read failure instead of returning a partial startup snapshot', async () => {
  const storage = store(); await seed(storage);
  const db = (storage as any).database;
  const template = await db.entities.get(['default', 'tasks', 'a']);
  await db.entities.bulkPut(Array.from({ length: 300 }, (_, index) => ({ ...template,
    id: `read-${index}`, value: JSON.stringify({ id: `read-${index}` }),
  })));
  const read = IDBObjectStore.prototype.getAll;
  let requests = 0;
  vi.spyOn(IDBObjectStore.prototype, 'getAll').mockImplementation(function (this: IDBObjectStore, ...args: any[]) {
    if (this.name === 'entities' && ++requests === 2) throw new Error('prefetched read failed');
    return (read as any).apply(this, args);
  });
  await expect(storage.loadWorkingSet('default', { maxRows: 1000, maxBytes: 1000000 })).rejects.toThrow('prefetched read failed');
  expect(requests).toBe(2);
});


it('discovers retired scopes with bounded primary keys when secondary key cursors are unavailable', async () => {
  const cursor = IDBIndex.prototype.openKeyCursor;
  vi.spyOn(IDBIndex.prototype, 'openKeyCursor').mockImplementation(function (this: IDBIndex, ...args: any[]) {
    if (this.objectStore.name === 'meta' && this.name === 'scope') throw new Error('Unable to open cursor');
    return (cursor as any).apply(this, args);
  });
  vi.spyOn(IndexedDBEngineReplicaStorage.prototype as any, 'scheduleCleanup').mockImplementation(() => {});
  const name = 'startup-scope-keys'; let storage = store(name); await seed(storage);
  await storage.replaceWindow(window(['b'], 'tasks', 2), empty, 'default', [{ id: 'b' }]);
  await storage.replaceWindow(window(['x'], 'tasks', 1), empty, 'tenant', [{ id: 'x' }]);
  await storage.replaceWindow(window(['y'], 'tasks', 2), empty, 'tenant', [{ id: 'y' }]);
  storage.close(); storage = store(name);
  expect((await storage.load())?.liveQueries.tasks.ids).toEqual(['b']);
  expect([...(storage as any).pendingCleanup].sort()).toEqual(['default', 'tenant']);
});

it('keeps rows whose ids shadow Object.prototype members in receipts and snapshots', async () => {
  const storage = store();
  const ids = ['__proto__', 'constructor', 'toString'];
  const value = window(ids);
  const receipt = await storage.replaceWindow(value, { ...empty, cursor: value.cursor }, 'default', ids.map((id) => ({ id, title: id })));
  const changed = (receipt as any)?.changes?.entities?.tasks;
  if (changed) expect(Object.keys(changed).sort()).toEqual([...ids].sort());
  const snapshot = await storage.load();
  expect(Object.keys(snapshot!.entities.tasks).sort()).toEqual([...ids].sort());
  const read = await storage.readChanges('default', 0);
  expect(Object.keys(read.entities.tasks ?? {}).sort()).toEqual([...ids].sort());
  expect(JSON.parse(JSON.stringify(snapshot!.entities.tasks))['__proto__']).toEqual({ id: '__proto__', title: '__proto__' });
});

it.each([false, true])('batches many-owner deletes and rolls back startup patches and checkpoints, hashes=%s', async hashed => {
  const storage = store(), db = (storage as any).database;
  const rows = [{ id: 'a', title: 'A' }, { id: 'b', title: 'B' }];
  const hashes = hashed ? await replicaRowsHashes(rows, 'id') : undefined;
  for (let index = 0; index < 100; index++) {
    await storage.replaceWindow({ ...window(['a', 'b'], `owner-${index}`), hashes }, empty, 'default', rows);
  }
  const before = await storage.load();
  const transaction = { cursor: { epoch: 'e', revision: 2 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'delete' as const },
    { entity: 'tasks', id: 'b', operation: 'delete' as const },
  ] };
  const failure = vi.spyOn(db.windows, 'bulkPut').mockRejectedValueOnce(new Error('checkpoint failed'));
  await expect(storage.applyTransaction(transaction, empty)).rejects.toThrow('checkpoint failed');
  failure.mockRestore();
  expect(await storage.load()).toEqual(before);
  expect(await storage.loadWorkingSet('default', { maxRows: 100, maxBytes: Infinity })).toMatchObject(before!);
  const reads = vi.spyOn(db.windows, 'get'), bulkReads = vi.spyOn(db.windows, 'bulkGet'), deletes = vi.spyOn(db.members, 'delete');
  const windowPages = vi.spyOn(db.windows, 'where'), checkpoints = vi.spyOn(db.windows, 'bulkPut'), sequence = vi.spyOn(db.meta, 'put');
  const receipt = await storage.applyTransaction(transaction, empty);
  expect(reads).not.toHaveBeenCalled(); expect(bulkReads).not.toHaveBeenCalled(); expect(windowPages).toHaveBeenCalledOnce(); expect(deletes).not.toHaveBeenCalled();
  expect(checkpoints.mock.calls.flatMap(call => call[0])).toHaveLength(100);
  expect(sequence.mock.calls.filter(call => call[0].key === 'sequence')).toHaveLength(1);
  expect(receipt.changes.removedMemberships).toHaveLength(200);
  const loaded = await storage.loadWorkingSet('default', { maxRows: 100, maxBytes: Infinity });
  expect(Object.values(loaded!.liveQueries).every(value => value.ids.length === 0)).toBe(true);
  const members = await db.members.toArray();
  expect(members).toHaveLength(hashed ? 200 : 0);
  expect(members.every((member: any) => member.hashOnly === 1)).toBe(true);
  if (hashed) for (const value of Object.values(loaded!.liveQueries)) expect(value.hashes).toEqual(hashes);
  expect(receipt.sequence).toBe(receipt.previousSequence + 1);
}, 30_000);

it('returns fresh working-set rows that remain safe to freeze and update', async () => {
  const storage = store(); await seed(storage);
  const budget = { maxRows: 100, maxBytes: Infinity };
  const first = await storage.loadWorkingSet('default', budget);
  const local = replica(storage);
  const load = vi.spyOn(storage, 'loadWorkingSet').mockResolvedValueOnce(first);
  await local.hydrate();
  expect(local.entity('tasks', 'a')).toBe(first!.entities.tasks.a);
  expect(Object.isFrozen(first!.entities.tasks.a)).toBe(true);
  load.mockRestore();
  const second = await storage.loadWorkingSet('default', budget);
  expect(second!.entities.tasks.a).not.toBe(first!.entities.tasks.a);
  second!.entities.tasks.a.title = 'caller';
  expect((await storage.loadWorkingSet('default', budget))!.entities.tasks.a.title).toBe('A');
  await storage.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'update', newValue: { title: 'updated' } },
  ] }, empty);
  expect((await storage.loadWorkingSet('default', budget))!.entities.tasks.a.title).toBe('updated');
  expect(first!.entities.tasks.a.title).toBe('A');
});

it('falls back to exact window keys when many owners lie beyond the bounded metadata page', async () => {
  const storage = store(), db = (storage as any).database;
  for (let index = 0; index < 140; index++) await storage.replaceWindow(window([], `aa-unrelated-${index}`), empty);
  for (let index = 0; index < 40; index++) {
    await seed(storage, `zz-owner-${index}`);
    await storage.replaceWindow(window(['c'], `zz-other-${index}`), empty, 'default', [{ id: 'c' }]);
  }
  const page = vi.spyOn(db.windows, 'where'), exact = vi.spyOn(db.windows, 'bulkGet');
  await storage.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [
    { entity: 'tasks', id: 'a', operation: 'delete' },
    { entity: 'tasks', id: 'c', operation: 'delete' },
  ] }, empty);
  expect(page).toHaveBeenCalledOnce(); expect(exact).toHaveBeenCalledTimes(2);
  expect(exact.mock.calls[0][0]).toHaveLength(40);
  const loaded = await storage.load();
  for (let index = 0; index < 40; index++) {
    expect(loaded!.liveQueries[`zz-owner-${index}`].ids).toEqual(['b']);
    expect(loaded!.liveQueries[`zz-other-${index}`].ids).toEqual([]);
  }
  expect(loaded!.liveQueries['aa-unrelated-0'].ids).toEqual([]);
}, 30_000);
