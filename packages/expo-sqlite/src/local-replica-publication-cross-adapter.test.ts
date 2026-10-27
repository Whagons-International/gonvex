import { DatabaseSync, type SQLInputValue } from 'node:sqlite';
import { ExpoSQLiteLocalReplicaStorage, type ExpoSQLiteDatabase } from './index.js';
import { publicationContract, type PublicationFactory } from '../../client/test/local-replica-publication-contract.js';
import { publicationWireContract } from '../../client/test/local-replica-publication-wire.js';

// Real Node SQLite through the Expo interface, not a native Expo-driver test.
const create: PublicationFactory = () => {
  const db = new DatabaseSync(':memory:');
  const database: ExpoSQLiteDatabase = {
    async execAsync(sql) { db.exec(sql); },
    async runAsync(sql, ...params) { return db.prepare(sql).run(...params as SQLInputValue[]); },
    async getAllAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).all(...params as SQLInputValue[]) as T[]; },
    async getFirstAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).get(...params as SQLInputValue[]) as T ?? null; },
    async withTransactionAsync(run) {
      db.exec('BEGIN');
      try { await run(); db.exec('COMMIT'); }
      catch (error) { db.exec('ROLLBACK'); throw error; }
    },
  };
  return { storage: new ExpoSQLiteLocalReplicaStorage(database),
    fresh: () => new ExpoSQLiteLocalReplicaStorage(database), close: () => db.close() };
};
publicationContract('Node SQLite via Expo contract', create);
publicationWireContract('Node SQLite via Expo contract', create);
