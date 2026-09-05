import type {
  ReplicaWindow,
  LocalReplicaStorage,
  ReplicaScope,
  ReplicaRow,
  ReplicaSnapshot,
  ReplicaTransaction,
} from "@gonvex/client";

export interface ExpoSQLiteDatabase {
  execAsync(sql: string): Promise<void>;
  runAsync(sql: string, ...params: unknown[]): Promise<unknown>;
  getFirstAsync<T>(sql: string, ...params: unknown[]): Promise<T | null>;
  getAllAsync<T>(sql: string, ...params: unknown[]): Promise<T[]>;
  withTransactionAsync(task: () => Promise<void>): Promise<void>;
}

type EntityRecord = { scope: ReplicaScope; entity: string; id: string; value: string };
type QueryRecord = { scope: ReplicaScope; signature: string; value: string };
type MetaRecord = { scope: ReplicaScope; key: string; value: string };
type TableInfo = { name: string };
const defaultReplicaScope: ReplicaScope = "default";
const replicaSchemaVersion = 2;

/**
 * Transactional, normalized SQLite persistence for Expo. Every server
 * transaction updates entities, Live Query memberships, and the replica
 * cursor inside one SQLite transaction.
 */
export class ExpoSQLiteLocalReplicaStorage implements LocalReplicaStorage {
  private initialized?: Promise<void>;

  constructor(private readonly database: ExpoSQLiteDatabase) {}

  private initialize() {
    return this.initialized ??= this.migrateSchema();
  }

  private async migrateSchema() {
    const [entities, queries, meta] = await Promise.all([
      this.database.getAllAsync<TableInfo>(`PRAGMA table_info(_gonvex_replica_entities)`),
      this.database.getAllAsync<TableInfo>(`PRAGMA table_info(_gonvex_replica_queries)`),
      this.database.getAllAsync<TableInfo>(`PRAGMA table_info(_gonvex_replica_meta)`),
    ]);
    const legacyEntities = entities.length > 0 && !entities.some((column) => column.name === "scope");
    const legacyQueries = queries.length > 0 && !queries.some((column) => column.name === "scope");
    const legacyMeta = meta.length > 0 && !meta.some((column) => column.name === "scope");
    const createTables = `
      CREATE TABLE IF NOT EXISTS _gonvex_replica_entities (
        scope TEXT NOT NULL, entity TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL,
        PRIMARY KEY (scope, entity, id)
      );
      CREATE TABLE IF NOT EXISTS _gonvex_replica_queries (
        scope TEXT NOT NULL, signature TEXT NOT NULL, value TEXT NOT NULL,
        PRIMARY KEY (scope, signature)
      );
      CREATE TABLE IF NOT EXISTS _gonvex_replica_meta (
        scope TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
        PRIMARY KEY (scope, key)
      );
    `;
    if (!legacyEntities && !legacyQueries && !legacyMeta) {
      await this.database.execAsync(`PRAGMA user_version = ${replicaSchemaVersion};${createTables}`);
      return;
    }
    // The v1 tables had no namespace at all. Keep their contents available to
    // direct/default LocalReplica users, but never leave those rows in the
    // canonical tables where an authenticated scope could read them.
    await this.database.withTransactionAsync(async () => {
      if (legacyEntities) await this.database.execAsync(`ALTER TABLE _gonvex_replica_entities RENAME TO _gonvex_replica_entities_legacy_v1`);
      if (legacyQueries) await this.database.execAsync(`ALTER TABLE _gonvex_replica_queries RENAME TO _gonvex_replica_queries_legacy_v1`);
      if (legacyMeta) await this.database.execAsync(`ALTER TABLE _gonvex_replica_meta RENAME TO _gonvex_replica_meta_legacy_v1`);
      await this.database.execAsync(createTables);
      if (legacyEntities) {
        await this.database.execAsync(
          `INSERT INTO _gonvex_replica_entities (scope, entity, id, value)
           SELECT '${defaultReplicaScope}', entity, id, value FROM _gonvex_replica_entities_legacy_v1`,
        );
      }
      if (legacyQueries) {
        await this.database.execAsync(
          `INSERT INTO _gonvex_replica_queries (scope, signature, value)
           SELECT '${defaultReplicaScope}', signature, value FROM _gonvex_replica_queries_legacy_v1`,
        );
      }
      if (legacyMeta) {
        await this.database.execAsync(
          `INSERT INTO _gonvex_replica_meta (scope, key, value)
           SELECT '${defaultReplicaScope}', key, value FROM _gonvex_replica_meta_legacy_v1`,
        );
      }
      // The copy and cleanup share the same SQLite transaction. A failed
      // migration rolls back both, so legacy rows cannot be lost before the
      // scoped normalized tables are complete.
      if (legacyEntities) await this.database.execAsync(`DROP TABLE _gonvex_replica_entities_legacy_v1`);
      if (legacyQueries) await this.database.execAsync(`DROP TABLE _gonvex_replica_queries_legacy_v1`);
      if (legacyMeta) await this.database.execAsync(`DROP TABLE _gonvex_replica_meta_legacy_v1`);
      await this.database.execAsync(`PRAGMA user_version = ${replicaSchemaVersion}`);
    });
  }

  async load(scope: ReplicaScope = defaultReplicaScope): Promise<ReplicaSnapshot | undefined> {
    await this.initialize();
    const cursor = await this.database.getFirstAsync<MetaRecord>(
      `SELECT scope, key, value FROM _gonvex_replica_meta WHERE scope = ? AND key = 'cursor'`, scope,
    );
    const entities: Record<string, Record<string, ReplicaRow>> = {};
    const entityRecords = await this.database.getAllAsync<EntityRecord>(
      `SELECT scope, entity, id, value FROM _gonvex_replica_entities WHERE scope = ? ORDER BY entity, id`, scope,
    );
    for (const row of entityRecords) {
      (entities[row.entity] ??= {})[row.id] = JSON.parse(row.value) as ReplicaRow;
    }
    const liveQueries: Record<string, ReplicaWindow> = {};
    const queryRecords = await this.database.getAllAsync<QueryRecord>(
      `SELECT scope, signature, value FROM _gonvex_replica_queries WHERE scope = ? ORDER BY signature`,
      scope,
    );
    for (const row of queryRecords) {
      liveQueries[row.signature] = normalizeWindow(JSON.parse(row.value) as ReplicaWindow);
    }
    if (!cursor && entityRecords.length === 0 && queryRecords.length === 0) return undefined;
    return { cursor: cursor ? JSON.parse(cursor.value) : undefined, entities, liveQueries };
  }

  async loadSession(scope: string): Promise<import("@gonvex/client").LocalReplicaSession | undefined> {
    await this.initialize();
    const row = await this.database.getFirstAsync<MetaRecord>(
      `SELECT scope, key, value FROM _gonvex_replica_meta WHERE scope = ? AND key = 'session'`, scope,
    );
    return row ? JSON.parse(row.value) : undefined;
  }

  async saveSession(scope: string, session: import("@gonvex/client").LocalReplicaSession | undefined): Promise<void> {
    await this.initialize();
    if (session) await this.database.runAsync(
      `INSERT OR REPLACE INTO _gonvex_replica_meta (scope,key,value) VALUES (?, 'session', ?)`, scope, JSON.stringify(session),
    );
    else await this.database.runAsync(`DELETE FROM _gonvex_replica_meta WHERE scope = ? AND key = 'session'`, scope);
  }

  async applyTransaction(transaction: ReplicaTransaction, _snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      const previous = await this.database.getFirstAsync<MetaRecord>(
        `SELECT scope, key, value FROM _gonvex_replica_meta WHERE scope = ? AND key = 'cursor'`, scope,
      );
      if (previous && JSON.parse(previous.value).epoch !== transaction.cursor.epoch) {
        await this.database.runAsync(`DELETE FROM _gonvex_replica_entities WHERE scope = ?`, scope);
        await this.database.runAsync(`DELETE FROM _gonvex_replica_queries WHERE scope = ?`, scope);
      }
      for (const change of transaction.changes) {
        if (change.operation === "delete") {
          await this.database.runAsync(
            `DELETE FROM _gonvex_replica_entities WHERE scope = ? AND entity = ? AND id = ?`,
            scope, change.entity, change.id,
          );
        } else {
          await this.database.runAsync(
            `INSERT INTO _gonvex_replica_entities (scope, entity, id, value) VALUES (?, ?, ?, ?)
             ON CONFLICT(scope, entity, id) DO UPDATE SET value = excluded.value`,
            scope, change.entity, change.id, JSON.stringify(change.newValue),
          );
        }
      }
      for (const membership of transaction.memberships ?? []) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_queries (scope, signature, value) VALUES (?, ?, ?)
           ON CONFLICT(scope, signature) DO UPDATE SET value = excluded.value`,
          scope, membership.signature, JSON.stringify(membership),
        );
      }
      await this.database.runAsync(
        `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
         ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value`,
        scope, JSON.stringify(transaction.cursor),
      );
    });
  }

  async advanceWatermark(
    windows: readonly ReplicaWindow[],
    cursor: ReplicaSnapshot["cursor"],
    scope: ReplicaScope = defaultReplicaScope,
  ): Promise<void> {
    await this.initialize();
    // Watermarks carry no row changes. Keep this transaction metadata-only so
    // advancing many retained windows does not rewrite the normalized corpus.
    await this.database.withTransactionAsync(async () => {
      for (const window of windows) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_queries (scope, signature, value) VALUES (?, ?, ?)
           ON CONFLICT(scope, signature) DO UPDATE SET value = excluded.value`,
          scope, window.signature, JSON.stringify(normalizeWindow(window)),
        );
      }
      if (cursor) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
           ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value`,
          scope, JSON.stringify(cursor),
        );
      }
    });
  }

  async replaceSnapshot(snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      await this.database.runAsync(`DELETE FROM _gonvex_replica_entities WHERE scope = ?`, scope);
      await this.database.runAsync(`DELETE FROM _gonvex_replica_queries WHERE scope = ?`, scope);
      await this.database.runAsync(`DELETE FROM _gonvex_replica_meta WHERE scope = ? AND key = 'cursor'`, scope);
      for (const [entity, rows] of Object.entries(snapshot.entities)) {
        for (const [id, value] of Object.entries(rows)) {
          await this.database.runAsync(
            `INSERT INTO _gonvex_replica_entities (scope, entity, id, value) VALUES (?, ?, ?, ?)`,
            scope, entity, id, JSON.stringify(value),
          );
        }
      }
      for (const [signature, membership] of Object.entries(snapshot.liveQueries)) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_queries (scope, signature, value) VALUES (?, ?, ?)`,
          scope, signature, JSON.stringify(membership),
        );
      }
      if (snapshot.cursor) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
           ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value`,
          scope, JSON.stringify(snapshot.cursor),
        );
      }
    });
  }

  async replaceWindow(window: ReplicaWindow, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      for (const [entity, rows] of Object.entries(snapshot.entities)) {
        for (const [id, value] of Object.entries(rows)) {
          await this.database.runAsync(
            `INSERT INTO _gonvex_replica_entities (scope, entity, id, value) VALUES (?, ?, ?, ?)
             ON CONFLICT(scope, entity, id) DO UPDATE SET value = excluded.value`,
            scope, entity, id, JSON.stringify(value),
          );
        }
      }
      await this.database.runAsync(
        `INSERT INTO _gonvex_replica_queries (scope, signature, value) VALUES (?, ?, ?)
         ON CONFLICT(scope, signature) DO UPDATE SET value = excluded.value`,
        scope, window.signature, JSON.stringify(normalizeWindow(window)),
      );
      if (snapshot.cursor) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
           ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value`,
          scope, JSON.stringify(snapshot.cursor),
        );
      }
    });
  }

  async applyWindowDelta(window: ReplicaWindow, _delta: { upserts: ReplicaRow[]; deleted: string[] }, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.replaceWindow(window, snapshot, scope);
  }

  async removeWindow(signature: string, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_queries WHERE scope = ? AND signature = ?`,
        scope, signature,
      );
      if (snapshot.cursor) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
           ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value`,
          scope, JSON.stringify(snapshot.cursor),
        );
      }
    });
  }

  async clear(scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      await this.database.runAsync(`DELETE FROM _gonvex_replica_entities WHERE scope = ?`, scope);
      await this.database.runAsync(`DELETE FROM _gonvex_replica_queries WHERE scope = ?`, scope);
      await this.database.runAsync(`DELETE FROM _gonvex_replica_meta WHERE scope = ?`, scope);
    });
  }
}

export function expoSQLite(database: ExpoSQLiteDatabase): LocalReplicaStorage {
  return new ExpoSQLiteLocalReplicaStorage(database);
}

function normalizeWindow(value: ReplicaWindow): ReplicaWindow {
  return {
    ...value,
    kind: value.kind ?? "live",
    key: value.key ?? "id",
    ids: [...value.ids],
    resultPath: value.resultPath ? [...value.resultPath] : undefined,
    hashes: value.hashes ? { ...value.hashes } : undefined,
  };
}
