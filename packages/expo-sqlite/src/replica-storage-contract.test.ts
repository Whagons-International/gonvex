import { DatabaseSync, type SQLInputValue } from 'node:sqlite';
import { replicaStorageContract, type StorageFixture } from '../../client/test/replica-storage-contract.js';
import { ExpoSQLiteLocalReplicaStorage, type ExpoSQLiteDatabase } from './index.js';

replicaStorageContract('SQLite storage', (): StorageFixture => {
  const db = new DatabaseSync(':memory:');
  let writesUntilFailure: number | undefined;
  const adapter: ExpoSQLiteDatabase = {
    async execAsync(sql) { db.exec(sql); },
    async runAsync(sql, ...params) {
      if (writesUntilFailure !== undefined && /INSERT INTO _gonvex_replica_entities/i.test(sql)) {
        if (--writesUntilFailure === 0) {
          writesUntilFailure = undefined;
          throw new Error('contract interrupted commit');
        }
      }
      return db.prepare(sql).run(...params as SQLInputValue[]);
    },
    async getAllAsync<T>(sql, ...params) { return db.prepare(sql).all(...params as SQLInputValue[]) as T[]; },
    async getFirstAsync<T>(sql, ...params) { return db.prepare(sql).get(...params as SQLInputValue[]) as T ?? null; },
    async withTransactionAsync(run) {
      db.exec('BEGIN');
      try { await run(); db.exec('COMMIT'); }
      catch (error) { db.exec('ROLLBACK'); throw error; }
    },
  };
  return {
    storage: new ExpoSQLiteLocalReplicaStorage(adapter),
    close() { db.close(); },
    reopen() { return new ExpoSQLiteLocalReplicaStorage(adapter); },
    interruptNextCommit() { writesUntilFailure = 2; },
  };
}, { reopen: true, rollback: true, versionedRows: true });
