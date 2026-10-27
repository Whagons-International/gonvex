import {afterEach, beforeEach, expect, it} from 'vitest';
import {IDBFactory, IDBKeyRange} from 'fake-indexeddb';
import {Dexie} from 'dexie';
import {IndexedDBEngineReplicaStorage} from './indexeddb-engine.js';
import {IndexedDBLocalReplicaStorage} from './indexeddb-replica.js';
import {mergeReplicaRecord} from './replica-record.js';

const stores: Array<IndexedDBEngineReplicaStorage | IndexedDBLocalReplicaStorage> = [];
let priorIDB: typeof globalThis.indexedDB, priorRange: typeof globalThis.IDBKeyRange;
beforeEach(() => {
  priorIDB = globalThis.indexedDB; priorRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, {indexedDB: new IDBFactory(), IDBKeyRange});
  Dexie.dependencies.indexedDB = globalThis.indexedDB; Dexie.dependencies.IDBKeyRange = IDBKeyRange;
});
afterEach(() => {
  stores.splice(0).forEach(storage => storage.close());
  Object.assign(globalThis, {indexedDB: priorIDB, IDBKeyRange: priorRange});
  Dexie.dependencies.indexedDB = priorIDB; Dexie.dependencies.IDBKeyRange = priorRange;
});

it('preserves valid-update recovery from legacy NaN field and tombstone authority', () => {
  const prior = {row: {value: 'old'}, authority: {epoch: 'e', fields: {value: NaN}}};
  const cursor = {epoch: 'e', revision: 2};
  const updated = mergeReplicaRecord(prior, {value: 'fresh'}, cursor);
  expect(updated.row?.value).toBe('fresh');
  expect(updated.authority.fields.value).toBe(2);
  expect(mergeReplicaRecord(prior, null, cursor).row).toBeNull();
  const tombstone = {row: null, authority: {epoch: 'e', fields: {}, deleted: NaN}};
  expect(mergeReplicaRecord(tombstone, {value: 'fresh'}, cursor).row?.value).toBe('fresh');
});

it.each([
  ['engine', IndexedDBEngineReplicaStorage],
  ['legacy', IndexedDBLocalReplicaStorage],
] as const)('recovers legacy NaN authority after reopening the %s IndexedDB adapter', async (label, Adapter) => {
  const name = `revision-upgrade-${label}`;
  const initial = new Adapter(name); stores.push(initial);
  await initial.load();
  const db = (initial as any).database;
  // Authentic legacy storage representation: authority is a structured clone,
  // while row values are JSON. IndexedDB preserves NaN in the former.
  await db.entities.bulkPut([
    {scope: 'default', entity: 'tasks', id: 'a', value: JSON.stringify({id: 'a', value: 'old'}),
      lookupKeys: [], authority: {epoch: 'e', fields: {id: 1, value: NaN}}, sequence: 1},
    {scope: 'default', entity: 'tasks', id: 'b', value: JSON.stringify({id: 'b', value: 'old'}),
      lookupKeys: [], authority: {epoch: 'e', fields: {id: 1, value: NaN}}, sequence: 1},
    {scope: 'default', entity: 'tasks', id: 'c', value: 'null', deleted: true,
      lookupKeys: [], authority: {epoch: 'e', fields: {}, deleted: NaN}, sequence: 1},
  ]);
  initial.close();
  const reopened = new Adapter(name); stores.push(reopened);
  const reloaded = (reopened as any).database;
  expect(Number.isNaN((await reloaded.entities.get(['default', 'tasks', 'a'])).authority.fields.value)).toBe(true);
  await reopened.applyTransaction({cursor: {epoch: 'e', revision: 2}, changes: [
    {entity: 'tasks', id: 'a', operation: 'update', newValue: {id: 'a', value: 'fresh'}},
    {entity: 'tasks', id: 'b', operation: 'delete'},
    {entity: 'tasks', id: 'c', operation: 'insert', newValue: {id: 'c', value: 'returned'}},
  ]}, {entities: {}, liveQueries: {}});
  const snapshot = await reopened.load();
  expect(snapshot?.entities.tasks.a.value).toBe('fresh');
  expect(snapshot?.entities.tasks.b).toBeUndefined();
  expect(snapshot?.entities.tasks.c.value).toBe('returned');
  expect((await reloaded.entities.get(['default', 'tasks', 'a'])).authority.fields.value).toBe(2);
  expect((await reloaded.entities.get(['default', 'tasks', 'b'])).authority.deleted).toBe(2);
  expect((await reloaded.entities.get(['default', 'tasks', 'c'])).authority.deleted).toBeUndefined();
});
