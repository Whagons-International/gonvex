import type {
  ReplicaWindow,
  LocalReplicaStorage,
  ReplicaScope,
  ReplicaRow,
  ReplicaSnapshot,
  ReplicaTransaction,
  ReplicaStoragePostImages,
  ReplicaWindowWriteContext,
} from "@gonvex/client";
import {
  pagedReplicaReadView,
  mergeReplicaRecord,
  type ReplicaRecordVersion,
  type ReplicaReadCoverage,
  type ReplicaReducerReadView,
} from "@gonvex/client";
import { withSQLiteOperation } from './database-operations.js';

export interface ExpoSQLiteDatabase {
  execAsync(sql: string): Promise<void>;
  runAsync(sql: string, ...params: unknown[]): Promise<unknown>;
  getFirstAsync<T>(sql: string, ...params: unknown[]): Promise<T | null>;
  getAllAsync<T>(sql: string, ...params: unknown[]): Promise<T[]>;
  withTransactionAsync(task: () => Promise<void>): Promise<void>;
}

type EntityRecord = {
  scope: ReplicaScope;
  entity: string;
  id: string;
  value: string;
};
type QueryRecord = { scope: ReplicaScope; signature: string; value: string };
type MetaRecord = { scope: ReplicaScope; key: string; value: string };
type TableInfo = { name: string };
const defaultReplicaScope: ReplicaScope = "default";
const replicaSchemaVersion = 3;

/**
 * Transactional, normalized SQLite persistence for Expo. Every server
 * transaction updates entities, Live Query memberships, and the replica
 * cursor inside one SQLite transaction.
 */
export class ExpoSQLiteLocalReplicaStorage implements LocalReplicaStorage {
  private readonly core: SQLiteReplicaStorageCore;

  constructor(private readonly database: ExpoSQLiteDatabase) {
    // Core-to-core calls stay inside one admission; every public entry gates
    // reads and initialization too, so uncommitted rows cannot escape.
    this.core = new SQLiteReplicaStorageCore(database);
  }

  load(...args: Parameters<SQLiteReplicaStorageCore['load']>) {
    return withSQLiteOperation(this.database, () => this.core.load(...args));
  }

  loadWorkingSet(...args: Parameters<SQLiteReplicaStorageCore['loadWorkingSet']>) {
    return withSQLiteOperation(this.database, () => this.core.loadWorkingSet(...args));
  }

  loadEntityRows(...args: Parameters<SQLiteReplicaStorageCore['loadEntityRows']>) {
    return withSQLiteOperation(this.database, () => this.core.loadEntityRows(...args));
  }

  loadWindowRows(...args: Parameters<SQLiteReplicaStorageCore['loadWindowRows']>) {
    return withSQLiteOperation(this.database, () => this.core.loadWindowRows(...args));
  }

  loadSession(...args: Parameters<SQLiteReplicaStorageCore['loadSession']>) {
    return withSQLiteOperation(this.database, () => this.core.loadSession(...args));
  }

  saveSession(...args: Parameters<SQLiteReplicaStorageCore['saveSession']>) {
    return withSQLiteOperation(this.database, () => this.core.saveSession(...args));
  }

  applyTransaction(...args: Parameters<SQLiteReplicaStorageCore['applyTransaction']>) {
    return withSQLiteOperation(this.database, () => this.core.applyTransaction(...args));
  }

  advanceWatermark(...args: Parameters<SQLiteReplicaStorageCore['advanceWatermark']>) {
    return withSQLiteOperation(this.database, () => this.core.advanceWatermark(...args));
  }

  replaceSnapshot(...args: Parameters<SQLiteReplicaStorageCore['replaceSnapshot']>) {
    return withSQLiteOperation(this.database, () => this.core.replaceSnapshot(...args));
  }

  replaceWindow(...args: Parameters<SQLiteReplicaStorageCore['replaceWindow']>) {
    return withSQLiteOperation(this.database, () => this.core.replaceWindow(...args));
  }

  applyWindowDelta(...args: Parameters<SQLiteReplicaStorageCore['applyWindowDelta']>) {
    return withSQLiteOperation(this.database, () => this.core.applyWindowDelta(...args));
  }

  removeWindow(...args: Parameters<SQLiteReplicaStorageCore['removeWindow']>) {
    return withSQLiteOperation(this.database, () => this.core.removeWindow(...args));
  }

  clear(...args: Parameters<SQLiteReplicaStorageCore['clear']>) {
    return withSQLiteOperation(this.database, () => this.core.clear(...args));
  }

  withReadView<T>(scope: string, coverage: ReplicaReadCoverage, run: (view: ReplicaReducerReadView) => Promise<T>): Promise<T> {
    return withSQLiteOperation(this.database, () => this.core.withReadView(scope, coverage, run));
  }
}

class SQLiteReplicaStorageCore implements LocalReplicaStorage {
  private initialized?: Promise<void>;

  constructor(private readonly database: ExpoSQLiteDatabase) {}

  private initialize() {
    return (this.initialized ??= this.migrateSchema());
  }

  private async migrateSchema() {
    const storedVersion = await this.database.getFirstAsync<{
      user_version: number;
    }>("PRAGMA user_version");
    const [entities, queries, meta] = await Promise.all([
      this.database.getAllAsync<TableInfo>(
        `PRAGMA table_info(_gonvex_replica_entities)`,
      ),
      this.database.getAllAsync<TableInfo>(
        `PRAGMA table_info(_gonvex_replica_queries)`,
      ),
      this.database.getAllAsync<TableInfo>(
        `PRAGMA table_info(_gonvex_replica_meta)`,
      ),
    ]);
    const legacyEntities =
      entities.length > 0 &&
      !entities.some((column) => column.name === "scope");
    const legacyQueries =
      queries.length > 0 && !queries.some((column) => column.name === "scope");
    const legacyMeta =
      meta.length > 0 && !meta.some((column) => column.name === "scope");
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
      CREATE TABLE IF NOT EXISTS _gonvex_replica_lookup (
        scope TEXT NOT NULL, entity TEXT NOT NULL, column_name TEXT NOT NULL,
        kind TEXT NOT NULL, scalar BLOB NOT NULL, id TEXT NOT NULL,
        PRIMARY KEY(scope,entity,column_name,kind,scalar,id)
      );
      CREATE TRIGGER IF NOT EXISTS _gonvex_replica_lookup_insert AFTER INSERT ON _gonvex_replica_entities BEGIN
        INSERT INTO _gonvex_replica_lookup SELECT NEW.scope,NEW.entity,j.key,
          CASE WHEN j.type IN ('integer','real') THEN 'number' WHEN j.type IN ('true','false') THEN 'boolean' ELSE 'string' END,j.atom,NEW.id
          FROM json_each(NEW.value) j WHERE j.type IN ('integer','real','true','false') OR (j.type='text' AND length(j.atom)<=256);
      END;
      CREATE TRIGGER IF NOT EXISTS _gonvex_replica_lookup_update AFTER UPDATE ON _gonvex_replica_entities BEGIN
        DELETE FROM _gonvex_replica_lookup WHERE scope=OLD.scope AND entity=OLD.entity AND id=OLD.id;
        INSERT INTO _gonvex_replica_lookup SELECT NEW.scope,NEW.entity,j.key,
          CASE WHEN j.type IN ('integer','real') THEN 'number' WHEN j.type IN ('true','false') THEN 'boolean' ELSE 'string' END,j.atom,NEW.id
          FROM json_each(NEW.value) j WHERE j.type IN ('integer','real','true','false') OR (j.type='text' AND length(j.atom)<=256);
      END;
      CREATE TRIGGER IF NOT EXISTS _gonvex_replica_lookup_delete AFTER DELETE ON _gonvex_replica_entities BEGIN
        DELETE FROM _gonvex_replica_lookup WHERE scope=OLD.scope AND entity=OLD.entity AND id=OLD.id;
      END;
      CREATE INDEX IF NOT EXISTS _gonvex_replica_lookup_row ON _gonvex_replica_lookup(scope,entity,id);
    `;
    if (!legacyEntities && !legacyQueries && !legacyMeta) {
      await this.database.withTransactionAsync(async () => {
        await this.database.execAsync(createTables);
        if ((storedVersion?.user_version ?? 0) < 3) await this.rebuildLookup();
        await this.database.execAsync(
          `PRAGMA user_version = ${replicaSchemaVersion}`,
        );
      });
      return;
    }
    // The v1 tables had no namespace at all. Keep their contents available to
    // direct/default LocalReplica users, but never leave those rows in the
    // canonical tables where an authenticated scope could read them.
    await this.database.withTransactionAsync(async () => {
      if (legacyEntities)
        await this.database.execAsync(
          `ALTER TABLE _gonvex_replica_entities RENAME TO _gonvex_replica_entities_legacy_v1`,
        );
      if (legacyQueries)
        await this.database.execAsync(
          `ALTER TABLE _gonvex_replica_queries RENAME TO _gonvex_replica_queries_legacy_v1`,
        );
      if (legacyMeta)
        await this.database.execAsync(
          `ALTER TABLE _gonvex_replica_meta RENAME TO _gonvex_replica_meta_legacy_v1`,
        );
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
      if (legacyEntities)
        await this.database.execAsync(
          `DROP TABLE _gonvex_replica_entities_legacy_v1`,
        );
      if (legacyQueries)
        await this.database.execAsync(
          `DROP TABLE _gonvex_replica_queries_legacy_v1`,
        );
      if (legacyMeta)
        await this.database.execAsync(
          `DROP TABLE _gonvex_replica_meta_legacy_v1`,
        );
      await this.rebuildLookup();
      await this.database.execAsync(
        `PRAGMA user_version = ${replicaSchemaVersion}`,
      );
    });
  }

  private async rebuildLookup() {
    await this.database.execAsync(`INSERT OR REPLACE INTO _gonvex_replica_lookup
      SELECT e.scope,e.entity,j.key,CASE WHEN j.type IN ('integer','real') THEN 'number' WHEN j.type IN ('true','false') THEN 'boolean' ELSE 'string' END,j.atom,e.id
      FROM _gonvex_replica_entities e,json_each(e.value) j WHERE j.type IN ('integer','real','true','false') OR (j.type='text' AND length(j.atom)<=256)`);
  }

  async load(
    scope: ReplicaScope = defaultReplicaScope,
  ): Promise<ReplicaSnapshot | undefined> {
    await this.initialize();
    const cursor = await this.database.getFirstAsync<MetaRecord>(
      `SELECT scope, key, value FROM _gonvex_replica_meta WHERE scope = ? AND key = 'cursor'`,
      scope,
    );
    const entities: Record<string, Record<string, ReplicaRow>> = {};
    const entityRecords = await this.database.getAllAsync<EntityRecord>(
      `SELECT scope, entity, id, value FROM _gonvex_replica_entities WHERE scope = ? ORDER BY entity, id`,
      scope,
    );
    for (const row of entityRecords) {
      if (row.value !== "null")
        (entities[row.entity] ??= {})[row.id] = JSON.parse(
          row.value,
        ) as ReplicaRow;
    }
    const liveQueries: Record<string, ReplicaWindow> = {};
    const queryRecords = await this.database.getAllAsync<QueryRecord>(
      `SELECT scope, signature, value FROM _gonvex_replica_queries WHERE scope = ? ORDER BY signature`,
      scope,
    );
    for (const row of queryRecords) {
      liveQueries[row.signature] = normalizeWindow(
        JSON.parse(row.value) as ReplicaWindow,
      );
    }
    if (!cursor && entityRecords.length === 0 && queryRecords.length === 0)
      return undefined;
    return {
      cursor: cursor ? JSON.parse(cursor.value) : undefined,
      entities,
      liveQueries,
    };
  }

  async loadWorkingSet(
    scope: string,
    budget: { maxRows: number; maxBytes: number },
  ): Promise<ReplicaSnapshot | undefined> {
    await this.initialize();
    let snapshot: ReplicaSnapshot | undefined;
    await this.database.withTransactionAsync(async () => {
      const cursor = await this.database.getFirstAsync<MetaRecord>(
        `SELECT scope, key, value FROM _gonvex_replica_meta WHERE scope = ? AND key = 'cursor'`,
        scope,
      );
      const queries = await this.database.getAllAsync<QueryRecord>(
        `SELECT scope, signature, value FROM _gonvex_replica_queries WHERE scope = ? ORDER BY signature`,
        scope,
      );
      // Count live rows for every table in this read transaction. Looking
      // only at the loaded page misses tables entirely beyond either budget.
      const liveCounts = await this.database.getAllAsync<{
        entity: string;
        count: number;
      }>(
        `SELECT entity, COUNT(*) AS count FROM _gonvex_replica_entities WHERE scope = ? AND value <> 'null' GROUP BY entity ORDER BY entity`,
        scope,
      );
      const entities: ReplicaSnapshot["entities"] = {};
      let count = 0,
        bytes = 0,
        afterEntity = "",
        afterId = "",
        finished = false;
      while (count < budget.maxRows && !finished) {
        const records = await this.database.getAllAsync<EntityRecord>(
          `SELECT scope, entity, id, value FROM _gonvex_replica_entities WHERE scope = ? AND (entity > ? OR (entity = ? AND id > ?)) ORDER BY entity, id LIMIT ?`,
          scope,
          afterEntity,
          afterEntity,
          afterId,
          Math.min(256, budget.maxRows - count),
        );
        if (!records.length) break;
        for (const record of records) {
          if (record.value === "null") continue;
          const size = record.value.length * 2 + 256;
          if (bytes + size > budget.maxBytes) {
            finished = true;
            break;
          }
          (entities[record.entity] ??= {})[record.id] = JSON.parse(
            record.value,
          );
          bytes += size;
          count++;
        }
        const last = records.at(-1)!;
        afterEntity = last.entity;
        afterId = last.id;
      }
      if (cursor || queries.length || liveCounts.length)
        snapshot = {
          cursor: cursor ? JSON.parse(cursor.value) : undefined,
          entities,
          residentPartialTables: liveCounts
            .filter(({ entity, count }) =>
              count > Object.keys(entities[entity] ?? {}).length,
            )
            .map(({ entity }) => entity),
          liveQueries: Object.fromEntries(
            queries.map((row) => [
              row.signature,
              normalizeWindow(JSON.parse(row.value)),
            ]),
          ),
        };
    });
    return snapshot;
  }

  async loadEntityRows(scope: string, entity: string, ids: readonly string[]) {
    await this.initialize();
    const result: Array<{ id: string; row: ReplicaRow }> = [];
    for (let offset = 0; offset < ids.length; offset += 256) {
      const batch = ids.slice(offset, offset + 256);
      const rows = await this.database.getAllAsync<EntityRecord>(
        `SELECT scope, entity, id, value FROM _gonvex_replica_entities WHERE scope = ? AND entity = ? AND id IN (${batch.map(() => "?").join(",")})`,
        scope,
        entity,
        ...batch,
      );
      const byId = new Map(rows.map((row) => [row.id, row]));
      for (const id of batch) {
        const row = byId.get(id);
        if (row && row.value !== "null")
          result.push({ id, row: JSON.parse(row.value) });
      }
    }
    return result;
  }

  async loadWindowRows(scope: string, signature: string) {
    await this.initialize();
    let result: { window: ReplicaWindow; rows: ReplicaRow[] } | undefined;
    await this.database.withTransactionAsync(async () => {
      const record = await this.database.getFirstAsync<QueryRecord>(
        `SELECT scope, signature, value FROM _gonvex_replica_queries WHERE scope = ? AND signature = ?`,
        scope,
        signature,
      );
      if (!record) return;
      const window = normalizeWindow(JSON.parse(record.value));
      result = {
        window,
        rows: (await this.loadEntityRows(scope, window.entity, window.ids)).map(
          (entry) => entry.row,
        ),
      };
    });
    return result;
  }

  async withReadView<T>(
    scope: string,
    coverage: ReplicaReadCoverage,
    run: (view: ReplicaReducerReadView) => Promise<T>,
  ): Promise<T> {
    await this.initialize();
    let result!: T;
    await this.database.withTransactionAsync(async () => {
      result = await run(
        pagedReplicaReadView(coverage, async (read, after) => {
          const key = coverage[read.table]?.key ?? "_id";
          const predicate = read.where;
          const exact =
            predicate &&
            "column" in predicate &&
            !predicate.transform &&
            predicate.column === key &&
            predicate.op === "eq" &&
            typeof predicate.value === "string"
              ? predicate.value
              : undefined;
          const restriction = (function equality(
            value: typeof predicate,
          ): { column: string; values: unknown[] } | undefined {
            if (!value) return;
            if ("and" in value) {
              const candidates = value.and
                .map(equality)
                .filter((candidate) => candidate !== undefined);
              return (
                candidates.find((candidate) => candidate.column === key) ??
                candidates[0]
              );
            }
            if ("or" in value || value.transform) return;
            const values =
              value.op === "eq"
                ? [value.value]
                : value.op === "in"
                  ? [...value.values]
                  : undefined;
            // Null is deliberately absent from the scalar index. Fall back to a
            // bounded scan when it can match, including mixed IN predicates.
            if (
              values?.every(
                (value) =>
                  value !== null &&
                  (typeof value === "number" ||
                    typeof value === "boolean" ||
                    (typeof value === "string" && value.length <= 256)),
              )
            )
              return { column: value.column, values };
          })(predicate);
          let records: EntityRecord[];
          if (exact === undefined && restriction) {
            const values = restriction.values.map((value) => [
              typeof value,
              typeof value === "boolean" ? Number(value) : value,
            ]);
            records = values.length
              ? await this.database.getAllAsync<EntityRecord>(
                  `SELECT e.scope,e.entity,e.id,e.value FROM _gonvex_replica_entities e WHERE e.scope=? AND e.entity=? AND e.id>? AND e.value<>'null' AND e.id IN (SELECT l.id FROM json_each(?) j JOIN _gonvex_replica_lookup l ON l.scope=? AND l.entity=? AND l.column_name=? AND l.kind=json_extract(j.value,'$[0]') AND l.scalar=json_extract(j.value,'$[1]')) ORDER BY e.id LIMIT 256`,
                  scope,
                  read.table,
                  after ?? "",
                  JSON.stringify(values),
                  scope,
                  read.table,
                  restriction.column,
                )
              : [];
          } else
            records =
              exact !== undefined
                ? after
                  ? []
                  : await this.database.getAllAsync<EntityRecord>(
                      `SELECT scope, entity, id, value FROM _gonvex_replica_entities WHERE scope = ? AND entity = ? AND id = ?`,
                      scope,
                      read.table,
                      exact,
                    )
                : await this.database.getAllAsync<EntityRecord>(
                    `SELECT scope, entity, id, value FROM _gonvex_replica_entities WHERE scope = ? AND entity = ? AND id > ? AND value <> 'null' ORDER BY id LIMIT 256`,
                    scope,
                    read.table,
                    after ?? "",
                  );
          return records
            .filter((record) => record.value !== "null")
            .map((record) => ({
              id: record.id,
              row: JSON.parse(record.value),
            }));
        }),
      );
    });
    return result;
  }

  async loadSession(
    scope: string,
  ): Promise<import("@gonvex/client").LocalReplicaSession | undefined> {
    await this.initialize();
    const row = await this.database.getFirstAsync<MetaRecord>(
      `SELECT scope, key, value FROM _gonvex_replica_meta WHERE scope = ? AND key = 'session'`,
      scope,
    );
    return row ? JSON.parse(row.value) : undefined;
  }

  async saveSession(
    scope: string,
    session: import("@gonvex/client").LocalReplicaSession | undefined,
  ): Promise<void> {
    await this.initialize();
    if (session)
      await this.database.runAsync(
        `INSERT OR REPLACE INTO _gonvex_replica_meta (scope,key,value) VALUES (?, 'session', ?)`,
        scope,
        JSON.stringify(session),
      );
    else
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_meta WHERE scope = ? AND key = 'session'`,
        scope,
      );
  }

  // The caller must hold the SQLite write transaction. Reset epoch-bound
  // state before reading an old window or merging projected row fields.
  private async resetForEpoch(
    scope: ReplicaScope,
    cursor: ReplicaSnapshot["cursor"],
  ): Promise<void> {
    if (!cursor) return;
    const previous = await this.database.getFirstAsync<MetaRecord>(
      `SELECT scope, key, value FROM _gonvex_replica_meta WHERE scope = ? AND key = 'cursor'`,
      scope,
    );
    if (previous && JSON.parse(previous.value).epoch !== cursor.epoch) {
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_meta WHERE scope = ? AND key LIKE 'row:%'`,
        scope,
      );
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_entities WHERE scope = ?`,
        scope,
      );
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_queries WHERE scope = ?`,
        scope,
      );
    }
  }

  async applyTransaction(
    transaction: ReplicaTransaction,
    _snapshot: ReplicaSnapshot,
    scope: ReplicaScope = defaultReplicaScope,
  ): Promise<ReplicaStoragePostImages> {
    await this.initialize();
    const postImages: ReplicaStoragePostImages['postImages'] = { entities: Object.create(null), windows: Object.create(null) };
    await this.database.withTransactionAsync(async () => {
      await this.resetForEpoch(scope, transaction.cursor);
      const grouped = new Map<
        string,
        Array<{ id: string; row: ReplicaRow | null }>
      >();
      for (const change of transaction.changes) {
        const rows = grouped.get(change.entity) ?? [];
        rows.push({
          id: change.id,
          row: change.operation === "delete" ? null : change.newValue!,
        });
        grouped.set(change.entity, rows);
      }
      const attemptedDeletes = new Map<string, Set<string>>();
      for (const [entity, rows] of grouped) {
        postImages.entities[entity] = await this.writeRecords(scope, entity, rows, transaction.cursor);
        for (const change of rows) if (change.row === null) {
          let ids = attemptedDeletes.get(entity);
          if (!ids) { ids = new Set(); attemptedDeletes.set(entity, ids); }
          ids.add(change.id);
        }
      }
      for (const membership of transaction.memberships ?? []) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_queries (scope, signature, value) VALUES (?, ?, ?)
           ON CONFLICT(scope, signature) DO UPDATE SET value = excluded.value`,
          scope,
          membership.signature,
          JSON.stringify(membership),
        );
      }
      // Persist accepted entity-deletion membership changes. A newer window
      // certificate, a rejected row delete, or an explicit membership override
      // keeps its own final membership, which must also correct candidate RAM.
      if (attemptedDeletes.size) {
        const explicit = new Set((transaction.memberships ?? []).map(window => window.signature));
        const records = await this.database.getAllAsync<QueryRecord>(
          `SELECT scope,signature,value FROM _gonvex_replica_queries WHERE scope=?`, scope);
        for (const record of records) {
          let window: ReplicaWindow = JSON.parse(record.value);
          const attempted = attemptedDeletes.get(window.entity);
          if (!attempted || !window.ids.some(id => attempted.has(id))) continue;
          if (!explicit.has(window.signature)
            && !(window.cursor?.epoch === transaction.cursor.epoch && window.cursor.revision > transaction.cursor.revision)) {
            const ids = window.ids.filter(id => !attempted.has(id) || postImages.entities[window.entity]?.[id] !== null);
            if (ids.length !== window.ids.length) {
              window = { ...window, ids };
              await this.database.runAsync(
                `UPDATE _gonvex_replica_queries SET value=? WHERE scope=? AND signature=?`,
                JSON.stringify(window), scope, window.signature);
            }
          }
          postImages.windows[window.signature] = window;
        }
      }
      await this.database.runAsync(
        `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
         ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value
           WHERE json_extract(_gonvex_replica_meta.value, '$.epoch') <> json_extract(excluded.value, '$.epoch')
              OR json_extract(_gonvex_replica_meta.value, '$.revision') <= json_extract(excluded.value, '$.revision')`,
        scope,
        JSON.stringify(transaction.cursor),
      );
    });
    return { scope, postImages };
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
          scope,
          window.signature,
          JSON.stringify(normalizeWindow(window)),
        );
      }
      if (cursor) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
           ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value
           WHERE json_extract(_gonvex_replica_meta.value, '$.epoch') <> json_extract(excluded.value, '$.epoch')
              OR json_extract(_gonvex_replica_meta.value, '$.revision') <= json_extract(excluded.value, '$.revision')`,
          scope,
          JSON.stringify(cursor),
        );
      }
    });
  }

  async replaceSnapshot(
    snapshot: ReplicaSnapshot,
    scope: ReplicaScope = defaultReplicaScope,
  ): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_entities WHERE scope = ?`,
        scope,
      );
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_queries WHERE scope = ?`,
        scope,
      );
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_meta WHERE scope = ? AND (key = 'cursor' OR key LIKE 'row:%')`,
        scope,
      );
      for (const [entity, rows] of Object.entries(snapshot.entities)) {
        for (const [id, value] of Object.entries(rows)) {
          await this.database.runAsync(
            `INSERT INTO _gonvex_replica_entities (scope, entity, id, value) VALUES (?, ?, ?, ?)`,
            scope,
            entity,
            id,
            JSON.stringify(value),
          );
        }
      }
      for (const [signature, membership] of Object.entries(
        snapshot.liveQueries,
      )) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_queries (scope, signature, value) VALUES (?, ?, ?)`,
          scope,
          signature,
          JSON.stringify(membership),
        );
      }
      if (snapshot.cursor) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
           ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value
           WHERE json_extract(_gonvex_replica_meta.value, '$.epoch') <> json_extract(excluded.value, '$.epoch')
              OR json_extract(_gonvex_replica_meta.value, '$.revision') <= json_extract(excluded.value, '$.revision')`,
          scope,
          JSON.stringify(snapshot.cursor),
        );
      }
    });
  }

  private async writeRecords(
    scope: string,
    entity: string,
    rows: readonly { id: string; row: ReplicaRow | null }[],
    cursor?: ReplicaSnapshot["cursor"],
  ): Promise<Record<string, ReplicaRow | null>> {
    const resolved: Record<string, ReplicaRow | null> = Object.create(null);
    const previous = new Map(
      (
        await this.loadEntityRows(
          scope,
          entity,
          rows.map((row) => row.id),
        )
      ).map((entry) => [entry.id, entry.row]),
    );
    for (let offset = 0; offset < rows.length; offset += 100) {
      const batch = rows.slice(offset, offset + 100);
      const authorityKey = (id: string) =>
        `row:${JSON.stringify([entity, id])}`;
      const records = cursor
        ? await this.database.getAllAsync<MetaRecord>(
            `SELECT scope,key,value FROM _gonvex_replica_meta WHERE scope=? AND key IN (${batch.map(() => "?").join(",")})`,
            scope,
            ...batch.map((entry) => authorityKey(entry.id)),
          )
        : [];
      const authorities = new Map(
        records.map((record) => [
          record.key,
          JSON.parse(record.value) as ReplicaRecordVersion,
        ]),
      );
      const versions = batch.map(({ id, row }) => {
        const old = previous.get(id) ?? null;
        if (!cursor)
          return {
            id,
            row: row === null ? null : { ...old, ...row },
            authority: undefined,
          };
        const prior = authorities.get(authorityKey(id)) ?? {
          epoch: cursor.epoch,
          fields: Object.fromEntries(
            Object.keys(old ?? {}).map((key) => [key, 0]),
          ),
        };
        return {
          id,
          ...mergeReplicaRecord({ row: old, authority: prior }, row, cursor),
        };
      });
      for (const { id, row } of versions) resolved[id] = row;
      const params = versions.flatMap(({ id, row }) => [
        scope,
        entity,
        id,
        JSON.stringify(row),
      ]);
      await this.database.runAsync(
        `INSERT INTO _gonvex_replica_entities (scope,entity,id,value) VALUES ${batch.map(() => "(?,?,?,?)").join(",")} ON CONFLICT(scope,entity,id) DO UPDATE SET value=excluded.value`,
        ...params,
      );
      if (cursor)
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta(scope,key,value) VALUES ${batch.map(() => "(?,?,?)").join(",")} ON CONFLICT(scope,key) DO UPDATE SET value=excluded.value`,
          ...versions.flatMap(({ id, authority }) => [
            scope,
            authorityKey(id),
            JSON.stringify(authority),
          ]),
        );
    }
    return resolved;
  }

  async replaceWindow(
    window: ReplicaWindow,
    snapshot: ReplicaSnapshot,
    scope: ReplicaScope = defaultReplicaScope,
    projection?: readonly ReplicaRow[],
    context?: ReplicaWindowWriteContext,
  ): Promise<ReplicaStoragePostImages> {
    await this.initialize();
    const postImages: ReplicaStoragePostImages['postImages'] = { entities: Object.create(null), windows: Object.create(null) };
    await this.database.withTransactionAsync(async () => {
      // A collection cursor proves its epoch, not a global feed floor. When
      // no snapshot floor is supplied, checkpoint only that epoch at zero.
      // The monotone upsert below preserves an existing same-epoch floor.
      const checkpoint = window.cursor && snapshot.cursor?.epoch !== window.cursor.epoch
        ? { epoch: window.cursor.epoch, revision: 0 } : snapshot.cursor;
      await this.resetForEpoch(scope, checkpoint);
      const oldRecord = await this.database.getFirstAsync<QueryRecord>(
        `SELECT scope,signature,value FROM _gonvex_replica_queries WHERE scope=? AND signature=?`,
        scope,
        window.signature,
      );
      const oldWindow: ReplicaWindow | undefined = oldRecord
        ? JSON.parse(oldRecord.value)
        : undefined;
      if (
        oldWindow?.cursor &&
        window.cursor &&
        oldWindow.cursor.epoch === window.cursor.epoch &&
        oldWindow.cursor.revision > window.cursor.revision
      ) {
        postImages.windows[window.signature] = oldWindow;
        const ids = new Set([...window.ids, ...(context?.removedIDs ?? [])]);
        const values = Object.fromEntries([...ids].map(id => [id, null as ReplicaRow | null]));
        for (const { id, row } of await this.loadEntityRows(scope, window.entity, [...ids])) values[id] = row;
        postImages.entities[window.entity] = values;
        return;
      }
      const rows =
        projection ??
        window.ids
          .map((id) => snapshot.entities[window.entity]?.[id])
          .filter((row): row is ReplicaRow => row !== undefined);
      postImages.entities[window.entity] = await this.writeRecords(
        scope,
        window.entity,
        rows.map((row) => ({ id: String(row[window.key]), row })),
        window.cursor,
      );
      await this.database.runAsync(
        `INSERT INTO _gonvex_replica_queries (scope, signature, value) VALUES (?, ?, ?)
         ON CONFLICT(scope, signature) DO UPDATE SET value = excluded.value`,
        scope,
        window.signature,
        JSON.stringify(normalizeWindow(window)),
      );
      postImages.windows[window.signature] = normalizeWindow(window);
      const removed =
        oldWindow?.ids.filter((id) => !window.ids.includes(id)) ?? [];
      if (removed.length) {
        const windows = await this.database.getAllAsync<QueryRecord>(
          `SELECT scope,signature,value FROM _gonvex_replica_queries WHERE scope=?`,
          scope,
        );
        const retained = new Set(
          windows.flatMap((record) => {
            const other: ReplicaWindow = JSON.parse(record.value);
            return other.entity === window.entity ? other.ids : [];
          }),
        );
        const pruned = await this.writeRecords(
          scope,
          window.entity,
          removed
            .filter((id) => !retained.has(id))
            .map((id) => ({ id, row: null })),
          window.cursor,
        );
        Object.assign(postImages.entities[window.entity], pruned);
      }
      // RAM may remove an affected ID outside the previous membership. Such
      // rows were not pruned here, so publish their final stored values too.
      const affected = [...new Set(context?.removedIDs ?? [])].filter(id =>
        !Object.hasOwn(postImages.entities[window.entity]!, id));
      for (const id of affected) postImages.entities[window.entity]![id] = null;
      for (const { id, row } of await this.loadEntityRows(scope, window.entity, affected)) {
        postImages.entities[window.entity]![id] = row;
      }
      if (checkpoint) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
           ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value
           WHERE json_extract(_gonvex_replica_meta.value, '$.epoch') <> json_extract(excluded.value, '$.epoch')
              OR json_extract(_gonvex_replica_meta.value, '$.revision') <= json_extract(excluded.value, '$.revision')`,
          scope,
          JSON.stringify(checkpoint),
        );
      }
    });
    return { scope, postImages };
  }

  async applyWindowDelta(
    window: ReplicaWindow,
    delta: { upserts: ReplicaRow[]; deleted: string[] },
    snapshot: ReplicaSnapshot,
    scope: ReplicaScope = defaultReplicaScope,
  ): Promise<ReplicaStoragePostImages> {
    return this.replaceWindow(window, snapshot, scope, delta.upserts, { removedIDs: delta.deleted });
  }

  async removeWindow(
    signature: string,
    snapshot: ReplicaSnapshot,
    scope: ReplicaScope = defaultReplicaScope,
  ): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_queries WHERE scope = ? AND signature = ?`,
        scope,
        signature,
      );
      if (snapshot.cursor) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta (scope, key, value) VALUES (?, 'cursor', ?)
           ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value
           WHERE json_extract(_gonvex_replica_meta.value, '$.epoch') <> json_extract(excluded.value, '$.epoch')
              OR json_extract(_gonvex_replica_meta.value, '$.revision') <= json_extract(excluded.value, '$.revision')`,
          scope,
          JSON.stringify(snapshot.cursor),
        );
      }
    });
  }

  async clear(scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_entities WHERE scope = ?`,
        scope,
      );
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_queries WHERE scope = ?`,
        scope,
      );
      await this.database.runAsync(
        `DELETE FROM _gonvex_replica_meta WHERE scope = ?`,
        scope,
      );
    });
  }
}

export * from "./outbox.js";

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
