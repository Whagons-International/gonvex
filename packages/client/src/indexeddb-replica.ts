import { Dexie, type Table } from "dexie";
import type {
  LocalReplicaStorage,
  ReplicaScope,
  ReplicaSnapshot,
  ReplicaTransaction,
  ReplicaWindow,
  ReplicaRow,
} from "./local-replica.js";

type EntityRecord = { scope: ReplicaScope; entity: string; id: string; value: string };
type WindowRecord = { scope: ReplicaScope; signature: string; value: string };
type MetaRecord = { scope: ReplicaScope; key: string; value: string };
type LegacySnapshotRecord = { scope?: ReplicaScope; key?: string; snapshot: ReplicaSnapshot };
type ReplicaDatabase = Dexie & {
  entities: Table<EntityRecord, [string, string, string]>;
  windows: Table<WindowRecord, [string, string]>;
  meta: Table<MetaRecord, [string, string]>;
  snapshots: Table<LegacySnapshotRecord, string>;
};

const defaultReplicaScope: ReplicaScope = "default";
const replicaSchemaVersion = 3;

/** Atomic normalized web persistence for the Gonvex Local Replica. */
export class IndexedDBLocalReplicaStorage implements LocalReplicaStorage {
  private readonly database: ReplicaDatabase;
  private initialized?: Promise<void>;

  constructor(name = "gonvex-local-replica") {
    this.database = new Dexie(name) as ReplicaDatabase;
    this.database.version(1).stores({ snapshots: "&key" });
    this.database.version(2).stores({ snapshots: "&scope" }).upgrade(async (transaction) => {
      await transaction.table("snapshots").toCollection().modify((record: LegacySnapshotRecord & { key?: string }) => {
        record.scope = defaultReplicaScope;
        delete record.key;
      });
    });
    this.database.version(replicaSchemaVersion).stores({
      entities: "[scope+entity+id], scope, [scope+entity]",
      windows: "[scope+signature], scope",
      meta: "[scope+key], scope",
      // Retain the old table only as a one-time upgrade source. It is never
      // read after initialization and is removed from the active schema.
      snapshots: "&scope",
    }).upgrade(async (transaction) => {
      const snapshots = await transaction.table("snapshots").toArray() as LegacySnapshotRecord[];
      const entities = transaction.table("entities");
      const windows = transaction.table("windows");
      const meta = transaction.table("meta");
      for (const record of snapshots) {
        const scope = record.scope ?? defaultReplicaScope;
        const snapshot = record.snapshot;
        for (const [entity, rows] of Object.entries(snapshot.entities ?? {})) {
          for (const [id, value] of Object.entries(rows)) {
            await entities.put({ scope, entity, id, value: JSON.stringify(value) });
          }
        }
        for (const [signature, window] of Object.entries(snapshot.liveQueries ?? {})) {
          await windows.put({ scope, signature, value: JSON.stringify(window) });
        }
        if (snapshot.cursor) {
          await meta.put({ scope, key: "cursor", value: JSON.stringify(snapshot.cursor) });
        }
      }
      // The upgrade transaction is atomic: only remove the legacy full
      // snapshots after every entity/window/cursor has been copied into the
      // normalized stores. Keeping the source rows would retain a second,
      // row-bearing server-state store indefinitely.
      await transaction.table("snapshots").clear();
    });
  }

  private initialize() {
    return this.initialized ??= this.database.open().then(() => undefined);
  }

  async load(scope: ReplicaScope = defaultReplicaScope): Promise<ReplicaSnapshot | undefined> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    const [entityRecords, windowRecords, cursor] = await Promise.all([
      this.database.entities.where("scope").equals(normalizedScope).toArray(),
      this.database.windows.where("scope").equals(normalizedScope).toArray(),
      this.database.meta.get([normalizedScope, "cursor"]),
    ]);
    if (entityRecords.length === 0 && windowRecords.length === 0 && !cursor) return undefined;
    const entities: ReplicaSnapshot["entities"] = {};
    for (const record of entityRecords) (entities[record.entity] ??= {})[record.id] = JSON.parse(record.value) as ReplicaRow;
    const liveQueries: ReplicaSnapshot["liveQueries"] = {};
    for (const record of windowRecords) liveQueries[record.signature] = JSON.parse(record.value) as ReplicaWindow;
    return {
      cursor: cursor ? JSON.parse(cursor.value) : undefined,
      entities,
      liveQueries,
    };
  }

  async applyTransaction(transaction: ReplicaTransaction, _snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      const previous = await this.database.meta.get([normalizedScope, "cursor"]);
      if (previous && JSON.parse(previous.value).epoch !== transaction.cursor.epoch) {
        await this.database.entities.where("scope").equals(normalizedScope).delete();
        await this.database.windows.where("scope").equals(normalizedScope).delete();
      }
      for (const change of transaction.changes) {
        if (change.operation === "delete") {
          await this.database.entities.delete([normalizedScope, change.entity, change.id]);
          const windows = await this.database.windows.where("scope").equals(normalizedScope).toArray();
          for (const record of windows) {
            const window = JSON.parse(record.value) as ReplicaWindow;
            if (!window.ids.includes(change.id)) continue;
            window.ids = window.ids.filter((id) => id !== change.id);
            await this.database.windows.put({ ...record, value: JSON.stringify(window) });
          }
        } else if (change.newValue) {
          await this.database.entities.put({
            scope: normalizedScope,
            entity: change.entity,
            id: change.id,
            value: JSON.stringify(change.newValue),
          });
        }
      }
      for (const membership of transaction.memberships ?? []) {
        await this.database.windows.put({
          scope: normalizedScope,
          signature: membership.signature,
          value: JSON.stringify(normalizeWindow(membership)),
        });
      }
      await this.database.meta.put({ scope: normalizedScope, key: "cursor", value: JSON.stringify(transaction.cursor) });
    });
  }

  async advanceWatermark(
    windows: readonly ReplicaWindow[],
    cursor: ReplicaSnapshot["cursor"],
    scope: ReplicaScope = defaultReplicaScope,
  ): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    // A watermark contains no entity changes. Keep this transaction limited to
    // window metadata and the shared cursor so large normalized replicas are
    // never rewritten once per retained collection.
    await this.database.transaction("rw", this.database.windows, this.database.meta, async () => {
      for (const window of windows) {
        await this.database.windows.put({
          scope: normalizedScope,
          signature: window.signature,
          value: JSON.stringify(normalizeWindow(window)),
        });
      }
      if (cursor) {
        await this.database.meta.put({ scope: normalizedScope, key: "cursor", value: JSON.stringify(cursor) });
      }
    });
  }

  async replaceWindow(window: ReplicaWindow, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      // A window replacement owns only its projected entity rows. Rewriting the
      // complete normalized replica for every snapshot made startup quadratic:
      // each newly opened collection persisted every entity loaded by all prior
      // collections, and `replica.ready` repeated the same work. Persist this
      // window atomically while leaving unrelated entity tables untouched.
      const rows = snapshot.entities[window.entity] ?? {};
      const previousRecord = await this.database.windows.get([normalizedScope, window.signature]);
      if (previousRecord) {
        const previous = JSON.parse(previousRecord.value) as ReplicaWindow;
        const nextIDs = new Set(window.ids);
        for (const id of previous.ids) {
          // The in-memory snapshot already accounts for every other window and
          // transaction owner. Absence here means the entity is no longer
          // authorized/referenced and must not survive restart hydration.
          if (!nextIDs.has(id) && rows[id] === undefined) {
            await this.database.entities.delete([normalizedScope, window.entity, id]);
          }
        }
      }
      for (const id of window.ids) {
        const value = rows[id];
        if (value !== undefined) {
          await this.database.entities.put({
            scope: normalizedScope,
            entity: window.entity,
            id,
            value: JSON.stringify(value),
          });
        }
      }
      await this.database.windows.put({ scope: normalizedScope, signature: window.signature, value: JSON.stringify(normalizeWindow(window)) });
      if (snapshot.cursor) await this.database.meta.put({ scope: normalizedScope, key: "cursor", value: JSON.stringify(snapshot.cursor) });
    });
  }

  async applyWindowDelta(window: ReplicaWindow, _delta: { upserts: ReplicaRow[]; deleted: string[] }, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    // The in-memory replica has already computed the normalized result. The
    // storage transaction receives that result and persists it atomically;
    // retaining unrelated entities is deliberate and conservative.
    await this.replaceWindow(window, snapshot, scope);
  }

  async removeWindow(signature: string, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.windows, this.database.meta, async () => {
      await this.database.windows.delete([normalizedScope, signature]);
      if (snapshot.cursor) await this.database.meta.put({ scope: normalizedScope, key: "cursor", value: JSON.stringify(snapshot.cursor) });
    });
  }

  async replaceSnapshot(snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      await this.database.entities.where("scope").equals(normalizedScope).delete();
      await this.database.windows.where("scope").equals(normalizedScope).delete();
      await this.database.meta.delete([normalizedScope, "cursor"]);
      for (const [entity, rows] of Object.entries(snapshot.entities)) {
        for (const [id, value] of Object.entries(rows)) {
          await this.database.entities.put({ scope: normalizedScope, entity, id, value: JSON.stringify(value) });
        }
      }
      for (const [signature, window] of Object.entries(snapshot.liveQueries)) {
        await this.database.windows.put({ scope: normalizedScope, signature, value: JSON.stringify(normalizeWindow(window)) });
      }
      if (snapshot.cursor) await this.database.meta.put({ scope: normalizedScope, key: "cursor", value: JSON.stringify(snapshot.cursor) });
    });
  }

  async clear(scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      await this.database.entities.where("scope").equals(normalizedScope).delete();
      await this.database.windows.where("scope").equals(normalizedScope).delete();
      await this.database.meta.where("scope").equals(normalizedScope).delete();
    });
  }

  close() {
    this.database.close();
  }
}

export function indexedDBLocalReplica(name?: string): LocalReplicaStorage {
  return new IndexedDBLocalReplicaStorage(name);
}

function normalizeScope(scope: ReplicaScope): ReplicaScope {
  return typeof scope === "string" && scope.trim() ? scope : defaultReplicaScope;
}

function normalizeWindow(value: ReplicaWindow | (Omit<ReplicaWindow, "kind"> & { kind?: ReplicaWindow["kind"] })): ReplicaWindow {
  return {
    ...value,
    kind: value.kind ?? "live",
    key: value.key ?? "id",
    ids: [...value.ids],
    resultPath: value.resultPath ? [...value.resultPath] : undefined,
    hashes: value.hashes ? { ...value.hashes } : undefined,
  };
}
