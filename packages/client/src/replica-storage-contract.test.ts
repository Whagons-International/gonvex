import { afterEach, beforeEach, vi } from 'vitest';
import { Dexie } from 'dexie';
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb';
import { replicaStorageContract, type StorageFixture } from '../test/replica-storage-contract.js';
import { MemoryLocalReplicaStorage } from './local-replica.js';
import { IndexedDBLocalReplicaStorage } from './indexeddb-replica.js';
import { IndexedDBEngineReplicaStorage } from './indexeddb-engine.js';

let oldIDB: typeof globalThis.indexedDB;
let oldRange: typeof globalThis.IDBKeyRange;
beforeEach(() => {
  oldIDB = globalThis.indexedDB; oldRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = IDBKeyRange;
});
afterEach(() => {
  vi.restoreAllMocks();
  Object.assign(globalThis, { indexedDB: oldIDB, IDBKeyRange: oldRange });
  Dexie.dependencies.indexedDB = oldIDB; Dexie.dependencies.IDBKeyRange = oldRange;
});

replicaStorageContract('memory storage', () => ({ storage: new MemoryLocalReplicaStorage(), close() {} }));
for (const Storage of [IndexedDBLocalReplicaStorage, IndexedDBEngineReplicaStorage]) {
  replicaStorageContract(Storage.name, (): StorageFixture => {
    const namespace = `contract-${crypto.randomUUID()}`;
    let storage = new Storage(namespace);
    return {
      storage,
      close() { storage.close(); },
      reopen() { storage.close(); storage = new Storage(namespace); return storage; },
      interruptNextCommit() {
        const entities = (storage as any).database.entities;
        const original = entities.bulkPut.bind(entities);
        let writes = 0;
        vi.spyOn(entities, 'bulkPut').mockImplementation((...args: unknown[]) => {
          if (++writes === 2) throw new Error('contract interrupted commit');
          return original(...args);
        });
      },
    };
  }, { reopen: true, rollback: true, versionedRows: true });
}
