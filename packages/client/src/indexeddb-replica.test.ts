import { describe, expect, it } from "vitest";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { Dexie } from "dexie";
import { IndexedDBLocalReplicaStorage } from "./indexeddb-replica";

describe("IndexedDBLocalReplicaStorage", () => {
  it("stores normalized entities and window metadata in one scope", async () => {
    // Dexie resolves globals lazily; the adapter accepts the browser globals in
    // production, while this test installs the fake implementation explicitly.
    const originalIndexedDB = globalThis.indexedDB;
    const originalKeyRange = globalThis.IDBKeyRange;
    Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
    Dexie.dependencies.indexedDB = globalThis.indexedDB;
    Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
    const storage = new IndexedDBLocalReplicaStorage(`gonvex-replica-test-${Math.random().toString(36).slice(2)}`);
    try {
      await storage.replaceWindow({
        signature: "tasks:grid",
        kind: "live",
        entity: "tasks",
        key: "id",
        ids: ["task-1"],
        completeness: "partial",
        source: "cache",
        cursor: { epoch: "tenant-a", revision: 3 },
      }, {
        cursor: { epoch: "tenant-a", revision: 3 },
        entities: { tasks: { "task-1": { id: "task-1", title: "Cached" } } },
        liveQueries: {},
      });
      const snapshot = await storage.load();
      expect(snapshot?.entities.tasks?.["task-1"]).toEqual({ id: "task-1", title: "Cached" });
      expect(snapshot?.liveQueries["tasks:grid"]).toMatchObject({ cursor: { revision: 3 }, kind: "live" });
    } finally {
      storage.close();
      Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
    }
  });

  it("replaces one window without rewriting unrelated normalized entities", async () => {
    const originalIndexedDB = globalThis.indexedDB;
    const originalKeyRange = globalThis.IDBKeyRange;
    Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
    Dexie.dependencies.indexedDB = globalThis.indexedDB;
    Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
    const storage = new IndexedDBLocalReplicaStorage(`gonvex-replica-window-test-${Math.random().toString(36).slice(2)}`);
    try {
      await storage.replaceWindow({
        signature: "tasks:list",
        kind: "replica",
        entity: "tasks",
        key: "id",
        ids: ["task-1"],
        completeness: "complete",
        source: "server",
      }, {
        entities: { tasks: { "task-1": { id: "task-1", title: "Original" } } },
        liveQueries: {},
      });
      await storage.replaceWindow({
        signature: "statuses:list",
        kind: "replica",
        entity: "statuses",
        key: "id",
        ids: ["status-1"],
        completeness: "complete",
        source: "server",
      }, {
        entities: {
          tasks: { "task-1": { id: "task-1", title: "Unrelated stale copy" } },
          statuses: { "status-1": { id: "status-1", name: "Open" } },
        },
        liveQueries: {},
      });

      const snapshot = await storage.load();
      expect(snapshot?.entities.tasks?.["task-1"]).toEqual({ id: "task-1", title: "Original" });
      expect(snapshot?.entities.statuses?.["status-1"]).toEqual({ id: "status-1", name: "Open" });
      expect(snapshot?.liveQueries["tasks:list"]?.ids).toEqual(["task-1"]);
      expect(snapshot?.liveQueries["statuses:list"]?.ids).toEqual(["status-1"]);

      await storage.replaceWindow({
        signature: "tasks:list",
        kind: "replica",
        entity: "tasks",
        key: "id",
        ids: [],
        completeness: "complete",
        source: "server",
      }, {
        entities: { statuses: { "status-1": { id: "status-1", name: "Open" } } },
        liveQueries: {},
      });
      const afterRemoval = await storage.load();
      expect(afterRemoval?.entities.tasks).toBeUndefined();
      expect(afterRemoval?.entities.statuses?.["status-1"]).toEqual({ id: "status-1", name: "Open" });
    } finally {
      storage.close();
      Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
    }
  });

  it("migrates legacy full snapshots into normalized stores and removes the source rows", async () => {
    const originalIndexedDB = globalThis.indexedDB;
    const originalKeyRange = globalThis.IDBKeyRange;
    Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
    Dexie.dependencies.indexedDB = globalThis.indexedDB;
    Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
    const name = `gonvex-replica-legacy-test-${Math.random().toString(36).slice(2)}`;
    const legacy = new Dexie(name);
    // Seed the post-v1 legacy schema directly. fake-indexeddb cannot emulate
    // Dexie's v1 -> v2 primary-key rewrite, while v3 is the migration that
    // copies the snapshot into normalized stores and removes the source.
    legacy.version(2).stores({ snapshots: "&scope" });
    await legacy.open();
    await legacy.table("snapshots").put({
      scope: "default",
      snapshot: {
        cursor: { epoch: "tenant-a", revision: 4 },
        entities: { tasks: { "task-1": { id: "task-1", title: "Cached" } } },
        liveQueries: {
          "tasks:list": {
            signature: "tasks:list",
            kind: "live",
            entity: "tasks",
            key: "id",
            ids: ["task-1"],
            completeness: "complete",
            source: "cache",
          },
        },
      },
    });
    legacy.close();

    const storage = new IndexedDBLocalReplicaStorage(name);
    try {
      const snapshot = await storage.load();
      expect(snapshot?.entities.tasks?.["task-1"]).toEqual({ id: "task-1", title: "Cached" });
      expect(snapshot?.liveQueries["tasks:list"]).toMatchObject({ ids: ["task-1"], source: "cache" });
      expect(snapshot?.cursor).toEqual({ epoch: "tenant-a", revision: 4 });
    } finally {
      storage.close();
    }

    const current = new Dexie(name);
    current.version(2).stores({ snapshots: "&scope" });
    current.version(3).stores({
      entities: "[scope+entity+id], scope, [scope+entity]",
      windows: "[scope+signature], scope",
      meta: "[scope+key], scope",
      snapshots: "&scope",
    });
    await current.open();
    try {
      expect(await current.table("snapshots").count()).toBe(0);
      expect(await current.table("entities").count()).toBe(1);
      expect(await current.table("windows").count()).toBe(1);
      expect(await current.table("meta").count()).toBe(1);
    } finally {
      current.close();
      Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
    }
  });
});
