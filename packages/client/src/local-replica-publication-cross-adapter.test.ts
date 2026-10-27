import { afterEach, beforeEach, expect, it } from 'vitest';
import { Dexie } from 'dexie';
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb';
import { IndexedDBLocalReplicaStorage } from './indexeddb-replica.js';
import { IndexedDBEngineReplicaStorage } from './indexeddb-engine.js';
import { publicationContract, type PublicationFactory } from '../test/local-replica-publication-contract.js';
import { publicationWireContract } from '../test/local-replica-publication-wire.js';
import { LocalReplica, type LocalReplicaStorage } from './local-replica.js';

let previous: { indexedDB: typeof globalThis.indexedDB; range: typeof globalThis.IDBKeyRange;
  dexieIDB: typeof Dexie.dependencies.indexedDB; dexieRange: typeof Dexie.dependencies.IDBKeyRange };
beforeEach(() => {
  previous = { indexedDB: globalThis.indexedDB, range: globalThis.IDBKeyRange,
    dexieIDB: Dexie.dependencies.indexedDB, dexieRange: Dexie.dependencies.IDBKeyRange };
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = IDBKeyRange;
});
afterEach(() => {
  Object.assign(globalThis, { indexedDB: previous.indexedDB, IDBKeyRange: previous.range });
  Dexie.dependencies.indexedDB = previous.dexieIDB;
  Dexie.dependencies.IDBKeyRange = previous.dexieRange;
});

for (const Storage of [IndexedDBLocalReplicaStorage, IndexedDBEngineReplicaStorage]) {
  const create: PublicationFactory = () => {
    const namespace = `publication-${crypto.randomUUID()}`;
    const instances: Array<InstanceType<typeof Storage>> = [];
    const fresh = () => { const storage = new Storage(namespace); instances.push(storage); return storage; };
    return { storage: fresh(), fresh, close() { for (const storage of instances.reverse()) storage.close(); } };
  };
  publicationContract(Storage.name, create);
  publicationWireContract(Storage.name, create);
  it(`${Storage.name}: reconciles a later peer resurrection at a full resident budget`, async () => {
    const owner = create(); const peer = owner.fresh();
    const scope = 'publication-budget'; const cursor = (revision: number) => ({ epoch: 'e', revision });
    const window = (signature: string, revision: number, ids: string[]) => ({ signature, kind: 'replica' as const, entity: 'tasks', key: '_id', ids, cursor: cursor(revision), completeness: 'complete' as const, source: 'server' as const });
    const base = new LocalReplica(owner.storage);
    let follower: LocalReplica | undefined;
    try {
      await base.activateScope(scope);
      await base.replaceWindow({ ...window('anchor', 1, []), entity: 'anchor', rows: [] });
      await base.replaceWindow({ ...window('tasks', 1, ['a']), rows: [{ _id: 'a', status: 'original' }] });
      let resurrect = false;
      const gate = new Proxy(owner.storage, { get(target, key) {
        if (key === 'subscribePeer') return undefined;
        if (key === 'readChanges') return async (...args: Parameters<NonNullable<LocalReplicaStorage['readChanges']>>) => {
          if (resurrect) {
            resurrect = false;
            await peer.applyTransaction({ cursor: cursor(4), changes: [{ entity: 'tasks', id: 'a', operation: 'insert', newValue: { _id: 'a', status: 'peer resurrection' } }], memberships: [window('tasks', 4, ['a'])] }, { entities: {}, liveQueries: {} }, scope);
          }
          return target.readChanges!(...args);
        };
        const value = Reflect.get(target, key); return typeof value === 'function' ? value.bind(target) : value;
      } });
      follower = new LocalReplica(gate, { maxResidentRows: 1, maxResidentBytes: 4096 });
      await follower.activateScope(scope);
      expect(follower.entity('tasks', 'a')).toBeDefined();
      // Create a real durable sequence gap. No new discretionary row fits.
      await peer.applyTransaction({ cursor: cursor(2), changes: [{ entity: 'other', id: 'b', operation: 'insert', newValue: { _id: 'b' } }] }, { entities: {}, liveQueries: {} }, scope);
      resurrect = true;
      await follower.applyTransaction({ cursor: cursor(3), changes: [{ entity: 'tasks', id: 'a', operation: 'delete' }] }, scope);
      expect(follower.entity('tasks', 'a')).toEqual({ _id: 'a', status: 'peer resurrection' });
      expect(follower.getWindow('tasks')!.ids).toEqual(['a']);
      expect(follower.cursor()).toEqual(cursor(3));
    } finally { follower?.dispose(); base.dispose(); owner.close(); }
  });
}
