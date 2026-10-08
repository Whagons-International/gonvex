import { describe, expect, it, vi } from "vitest";
import { IDBFactory, IDBKeyRange, IDBIndex } from "fake-indexeddb";
import { Dexie } from "dexie";
import { IndexedDBLocalReplicaStorage } from "./indexeddb-replica";
import { LocalReplica } from "./local-replica";

it('does not reread its own metadata commits, but catches every intervening peer row and membership', async () => {
  const oldIDB = globalThis.indexedDB;
  const oldRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
  const name = `metadata-receipt-${Math.random()}`;
  const storage = new IndexedDBLocalReplicaStorage(name);
  const peer = new IndexedDBLocalReplicaStorage(name);
  const replica = new LocalReplica(storage);
  const scope = 'tenant-a';
  const window = { signature: 'tasks', kind: 'replica' as const, entity: 'tasks', key: 'id',
    rows: [{ id: 'one', title: 'Original' }], completeness: 'complete' as const, source: 'server' as const,
    cursor: { epoch: 'test', revision: 1 }, hashes: { one: 'verified' } };
  try {
    await replica.activateScope(scope);
    await replica.replaceWindow(window);
    const read = vi.spyOn(storage, 'readChanges');
    for (let revision = 2; revision <= 10; revision += 2) {
      await replica.applyWindowDelta({ ...window, upserts: [], deleted: [], cursor: { epoch: 'test', revision } });
      await replica.advanceWatermark(revision + 1, ['tasks'], scope);
    }
    expect(read, 'own metadata is already published; do not reload every retained window').not.toHaveBeenCalled();
    expect(replica.entity('tasks', 'one')).toEqual({ id: 'one', title: 'Original' });
    const prior = await peer.load(scope);
    await peer.applyTransaction({ cursor: { epoch: 'test', revision: 20 }, changes: [
      { entity: 'tasks', id: 'peer-row', operation: 'insert', newValue: { id: 'peer-row', title: 'Peer only' } },
    ], memberships: [{ ...prior!.liveQueries.tasks!, signature: 'peer-window', ids: ['peer-row'], cursor: { epoch: 'test', revision: 20 } }] }, prior!, scope);
    await replica.applyWindowDelta({ ...window, upserts: [], deleted: [], cursor: { epoch: 'test', revision: 21 } });
    expect(read).toHaveBeenCalledTimes(1);
    expect(replica.entity('tasks', 'peer-row')).toEqual({ id: 'peer-row', title: 'Peer only' });
    expect(replica.getWindow('peer-window')?.ids).toEqual(['peer-row']);
    await peer.applyTransaction({ cursor: { epoch: 'test', revision: 22 }, changes: [
      { entity: 'tasks', id: 'peer-row', operation: 'update', newValue: { id: 'peer-row', title: 'Peer changed' } },
    ] }, (await peer.load(scope))!, scope);
    // A watermark receipt also must not hide a peer commit before it.
    await replica.advanceWatermark(23, ['tasks'], scope);
    await replica.applyWindowDelta({ ...window, upserts: [], deleted: [], cursor: { epoch: 'test', revision: 24 } });
    expect(read).toHaveBeenCalledTimes(2);
    expect(replica.entity('tasks', 'peer-row')).toEqual({ id: 'peer-row', title: 'Peer changed' });
    const stored = await storage.load(scope);
    expect(stored?.liveQueries.tasks?.cursor?.revision).toBe(24);
    expect(stored?.liveQueries['peer-window']?.ids).toEqual(['peer-row']);
    expect(stored?.entities.tasks?.['peer-row']).toEqual({ id: 'peer-row', title: 'Peer changed' });
    expect((await storage.load('tenant-b'))).toBeUndefined();
  } finally {
    vi.restoreAllMocks(); storage.close(); peer.close(); replica.dispose();
    Object.assign(globalThis, { indexedDB: oldIDB, IDBKeyRange: oldRange });
    Dexie.dependencies.indexedDB = oldIDB;
    Dexie.dependencies.IDBKeyRange = oldRange;
  }
}, 30_000);

it('issues metadata receipts only for exact, non-resetting, committed windows', async () => {
  const oldIDB = globalThis.indexedDB;
  const oldRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
  const storage = new IndexedDBLocalReplicaStorage(`metadata-guards-${Math.random()}`);
  const scope = 'tenant';
  const window = { signature: 'tasks', kind: 'replica' as const, entity: 'tasks', key: 'id', ids: ['one'],
    completeness: 'complete' as const, source: 'server' as const, cursor: { epoch: 'first', revision: 1 } };
  const snapshot = { cursor: window.cursor, entities: { tasks: { one: { id: 'one', title: 'Original' } } }, liveQueries: { tasks: window } };
  try {
    await storage.replaceSnapshot(snapshot, scope);
    const advanced = { ...window, cursor: { epoch: 'first', revision: 2 } };
    expect(await storage.applyWindowDelta(advanced, { upserts: [], deleted: [] }, { ...snapshot, cursor: advanced.cursor }, scope))
      .toEqual({ scope, previousSequence: 1, sequence: 2 });
    const changed = { ...window, cursor: { epoch: 'first', revision: 3 } };
    expect(await storage.applyWindowDelta(changed, { upserts: [{ id: 'one', title: 'Changed' }], deleted: [] }, { ...snapshot, cursor: changed.cursor }, scope)).toBeUndefined();
    expect(await storage.applyWindowDelta(advanced, { upserts: [], deleted: [] }, snapshot, scope)).toBeUndefined();
    expect((await storage.load(scope))?.liveQueries.tasks?.cursor).toEqual(changed.cursor);
    expect(await storage.advanceWatermark([advanced], advanced.cursor, scope)).toBeUndefined();
    const table = (storage as any).database.windows;
    const failure = vi.spyOn(table, 'bulkPut').mockRejectedValueOnce(new Error('quota'));
    const before = await storage.load(scope);
    await expect(storage.advanceWatermark([{ ...changed, cursor: { epoch: 'first', revision: 4 } }], { epoch: 'first', revision: 4 }, scope)).rejects.toThrow('quota');
    failure.mockRestore();
    expect(await storage.load(scope)).toEqual(before);
    const reset = { ...window, ids: [], cursor: { epoch: 'second', revision: 1 } };
    expect(await storage.applyWindowDelta(reset, { upserts: [], deleted: [] }, { cursor: reset.cursor, entities: {}, liveQueries: { tasks: reset } }, scope)).toBeUndefined();
    expect((await storage.load(scope))?.entities).toEqual({});
  } finally {
    vi.restoreAllMocks(); storage.close();
    Object.assign(globalThis, { indexedDB: oldIDB, IDBKeyRange: oldRange });
    Dexie.dependencies.indexedDB = oldIDB;
    Dexie.dependencies.IDBKeyRange = oldRange;
  }
}, 30_000);

it('checkpoints small windows together while bounding UTF-8 memberships and preserving an oversized window', async () => {
  const originalIndexedDB = globalThis.indexedDB;
  const originalKeyRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
  const storage = new IndexedDBLocalReplicaStorage(`metadata-budget-${Math.random()}`);
  const cursor = { epoch: 'test', revision: 1 };
  const small = Array.from({ length: 135 }, (_, index) => ({ signature: `small-${index}`, entity: 'tasks', key: '_id',
    kind: 'replica' as const, ids: [], completeness: 'complete' as const, source: 'server' as const, cursor }));
  const wide = Array.from({ length: 3 }, (_, index) => ({ ...small[0]!, signature: `wide-${index}`,
    ids: Array.from({ length: 400 }, (_, id) => `任务-${id}-${'界'.repeat(6)}`) }));
  const oversized = { ...small[0]!, signature: 'oversized', ids: Array.from({ length: 1000 }, (_, id) => `task-${id}-${'x'.repeat(40)}`) };
  const windows = [...small, ...wide, oversized];
  try {
    await storage.replaceSnapshot({ cursor, entities: {}, liveQueries: Object.fromEntries(windows.map(window => [window.signature, window])) }, 'tenant');
    const table = (storage as any).database.windows;
    const original = table.bulkPut.bind(table);
    const batches: any[][] = [];
    vi.spyOn(table, 'bulkPut').mockImplementation((records: any[]) => { batches.push(records); return original(records); });
    const nextCursor = { epoch: 'test', revision: 2 };
    const next = windows.map(window => ({ ...window, cursor: nextCursor }));
    await storage.advanceWatermark(next, nextCursor, 'tenant');
    // The 135 tiny memberships must not require the previous 17 round trips.
    expect(batches.filter(batch => batch.every(record => record.signature.startsWith('small-'))).length).toBeLessThanOrEqual(3);
    for (const batch of batches) {
      expect(batch.length).toBeLessThanOrEqual(64);
      const bytes = batch.reduce((sum, record) => sum + new TextEncoder().encode(record.value).byteLength, 0);
      if (bytes > 16 * 1024) expect(batch).toHaveLength(1);
    }
    const after = await storage.load('tenant');
    expect(after?.cursor).toEqual(nextCursor);
    for (const window of next) expect(after?.liveQueries[window.signature]).toEqual(window);
    expect(Object.keys(after?.liveQueries ?? {})).toHaveLength(windows.length);
  } finally {
    vi.restoreAllMocks(); storage.close();
    Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
    Dexie.dependencies.indexedDB = originalIndexedDB;
    Dexie.dependencies.IDBKeyRange = originalKeyRange;
  }
}, 30_000);

it('rolls back a late multi-window checkpoint failure without losing memberships or newer cursors', async () => {
  const originalIndexedDB = globalThis.indexedDB;
  const originalKeyRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
  const storage = new IndexedDBLocalReplicaStorage(`window-checkpoint-${Math.random()}`);
  const ids = Array.from({ length: 128 }, (_, i) => `task-${i}`);
  const cursor = { epoch: 'test', revision: 1 };
  const windows = Array.from({ length: 40 }, (_, i) => ({ signature: `window-${i}`, entity: 'tasks', key: '_id',
    kind: 'live' as const, ids, completeness: 'partial' as const, source: 'server' as const, cursor,
    hashes: Object.fromEntries(ids.map(id => [id, '0123456789abcdef'.repeat(4)])) }));
  try {
    await storage.replaceSnapshot({ cursor, entities: { tasks: Object.fromEntries(ids.map(_id => [_id, { _id }])) },
      liveQueries: Object.fromEntries(windows.map(window => [window.signature, window])) }, 'tenant');
    await expect(storage.advanceWatermark([], cursor, 'tenant')).resolves.toBeUndefined();
    const table = (storage as any).database.windows;
    const original = table.bulkPut.bind(table);
    const write = vi.spyOn(table, 'bulkPut').mockImplementation((records: any[]) => {
      if (records.some(record => record.signature === 'window-33')) throw new Error('Late checkpoint failed');
      return original(records);
    });
    const nextCursor = { epoch: 'test', revision: 3 };
    const next = windows.map(window => ({ ...window, cursor: nextCursor }));
    await expect(storage.advanceWatermark(next, nextCursor, 'tenant')).rejects.toThrow('Late checkpoint failed');
    const rolledBack = await storage.load('tenant');
    expect(rolledBack?.cursor).toEqual(cursor);
    for (const window of windows) expect(rolledBack?.liveQueries[window.signature]).toEqual(window);
    write.mockRestore();
    const newer = { epoch: 'test', revision: 7 };
    await storage.advanceWatermark([{ ...windows[39]!, cursor: newer }], newer, 'tenant');
    await storage.advanceWatermark(next, nextCursor, 'tenant');
    const after = await storage.load('tenant');
    expect(after?.cursor).toEqual(newer);
    for (const window of next) expect(after?.liveQueries[window.signature]).toEqual({ ...window, cursor: window.signature === 'window-39' ? newer : nextCursor });
    expect(new Set(Object.keys(after?.entities.tasks ?? {}))).toEqual(new Set(ids));
  } finally {
    vi.restoreAllMocks(); storage.close();
    Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
    Dexie.dependencies.indexedDB = originalIndexedDB;
    Dexie.dependencies.IDBKeyRange = originalKeyRange;
  }
}, 30_000);

it('keeps a wide multi-batch transaction atomic and preserves newer projected fields after retry', async () => {
  const originalIndexedDB = globalThis.indexedDB;
  const originalKeyRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
  const name = `wide-transaction-${Math.random()}`;
  let storage = new IndexedDBLocalReplicaStorage(name);
  const rows = Array.from({ length: 96 }, (_, index) => ({
    _id: `task-${index}`, ...Object.fromEntries(Array.from({ length: 48 }, (_, column) => [`field${column}`, `before-${index}-${column}`])),
  }));
  const window = { signature: 'tasks', entity: 'tasks', key: '_id', kind: 'replica' as const,
    ids: rows.map(row => row._id), completeness: 'complete' as const, source: 'server' as const,
    cursor: { epoch: 'test', revision: 1 } };
  const snapshot = { entities: {}, liveQueries: {}, cursor: window.cursor };
  const changes = rows.map(row => ({ entity: 'tasks', id: row._id, operation: 'update' as const, newValue: { _id: row._id, field0: 'newer' } }));
  try {
    await storage.replaceWindow(window, snapshot, 'tenant', rows);
    await expect(storage.applyTransaction({ cursor: window.cursor, changes: [
      { entity: 'tasks', id: 'ignored', operation: 'update' },
    ] }, snapshot, 'tenant')).resolves.toBeUndefined();
    await storage.replaceWindow({ ...window, signature: 'other', ids: ['unrelated'] }, snapshot, 'other', [{ _id: 'unrelated', field0: 'untouched' }]);
    const table = (storage as any).database.entities;
    const original = table.bulkPut.bind(table);
    const write = vi.spyOn(table, 'bulkPut').mockImplementation((records: any[]) => {
      if (records.some(record => record.id === 'task-80')) throw new Error('Late wide write failed');
      return original(records);
    });
    const transaction = { cursor: { epoch: 'test', revision: 3 }, changes };
    await expect(storage.applyTransaction(transaction, snapshot, 'tenant')).rejects.toThrow('Late wide write failed');
    const rolledBack = await storage.load('tenant');
    expect(rolledBack?.cursor?.revision).toBe(1);
    for (const row of rows) expect(rolledBack?.entities.tasks?.[row._id]).toEqual(row);
    write.mockRestore();
    await storage.applyTransaction(transaction, snapshot, 'tenant');
    // An older reconnect projection cannot erase fields learned from the newer
    // transaction, or unrelated columns not included in either projection.
    await storage.replaceWindow({ ...window, cursor: { epoch: 'test', revision: 2 } }, snapshot, 'tenant', rows.map(row => ({ _id: row._id, field0: 'stale', field1: 'projected' })));
    storage.close(); storage = new IndexedDBLocalReplicaStorage(name);
    const after = await storage.load('tenant');
    expect(after?.cursor?.revision).toBe(3);
    for (const row of rows) expect(after?.entities.tasks?.[row._id]).toEqual({ ...row, field0: 'newer', field1: 'projected' });
    expect((await storage.load('other'))?.entities.tasks?.unrelated).toEqual({ _id: 'unrelated', field0: 'untouched' });
  } finally {
    vi.restoreAllMocks(); storage.close();
    Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
    Dexie.dependencies.indexedDB = originalIndexedDB;
    Dexie.dependencies.IDBKeyRange = originalKeyRange;
  }
}, 30_000);

it('rolls back a late native write failure in a medium snapshot and can retry it intact', async () => {
  const originalIndexedDB = globalThis.indexedDB;
  const originalKeyRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
  const storage = new IndexedDBLocalReplicaStorage(`snapshot-retry-${Math.random()}`);
  const rows = Array.from({ length: 1056 }, (_, index) => ({
    _id: `context-${index}`, taskId: `task-${index}`, workspaceId: 'workspace', kind: 'approval',
  }));
  const window = { signature: 'contexts', entity: 'taskWorkspaceContexts', key: '_id',
    kind: 'replica' as const, ids: rows.map(row => row._id), completeness: 'complete' as const,
    source: 'server' as const, cursor: { epoch: 'test', revision: 2 } };
  const snapshot = { entities: {}, liveQueries: {}, cursor: window.cursor };
  try {
    await storage.replaceWindow({ ...window, ids: ['before'], cursor: { epoch: 'test', revision: 1 } },
      { ...snapshot, cursor: { epoch: 'test', revision: 1 } }, 'tenant', [{ _id: 'before', taskId: 'original' }]);
    const table = (storage as any).database.entities;
    const original = table.bulkPut.bind(table);
    const write = vi.spyOn(table, 'bulkPut').mockImplementation((records: any[]) => {
      if (records.some(row => row.id === 'context-1000')) throw new Error('Native write failed');
      return original(records);
    });
    await expect(storage.replaceWindow(window, snapshot, 'tenant', rows)).rejects.toThrow('Native write failed');
    const before = await storage.load('tenant');
    expect(before?.liveQueries.contexts.ids).toEqual(['before']);
    expect(before?.cursor?.revision).toBe(1);
    expect(Object.keys(before?.entities.taskWorkspaceContexts ?? {})).toEqual(['before']);
    write.mockRestore();
    await storage.replaceWindow(window, snapshot, 'tenant', rows);
    const after = await storage.load('tenant');
    expect(after?.liveQueries.contexts.ids).toEqual(rows.map(row => row._id));
    expect(after?.cursor?.revision).toBe(2);
    for (const row of rows) expect(after?.entities.taskWorkspaceContexts?.[row._id]).toEqual(row);
  } finally {
    vi.restoreAllMocks(); storage.close();
    Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
    Dexie.dependencies.indexedDB = originalIndexedDB;
    Dexie.dependencies.IDBKeyRange = originalKeyRange;
  }
}, 30_000);

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


it("persists only changed delta rows while retaining normalized projected fields", async () => {
  const originalIndexedDB = globalThis.indexedDB;
  const originalKeyRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
  const storage = new IndexedDBLocalReplicaStorage(`delta-${Math.random()}`);
  try {
    const replica = new LocalReplica(storage);
    await replica.replaceWindow({ signature: "tasks", kind: "replica", entity: "tasks", key: "id", rows: [{ id: "a", name: "A", status: "new" }, { id: "b", name: "B" }], completeness: "complete", source: "server" });
    const replace = vi.spyOn(storage, "replaceWindow");
    const apply = storage.applyWindowDelta.bind(storage);
    const delta = vi.spyOn(storage, "applyWindowDelta").mockImplementation(async (window, change, snapshot, scope) => {
      // Unchanged rows must not be materialized or serialized for this write.
      Object.defineProperty(snapshot.entities.tasks, "b", { get() { throw new Error("copied unchanged row"); } });
      return apply(window, change, snapshot, scope);
    });
    await replica.applyWindowDelta({ signature: "tasks", kind: "replica", entity: "tasks", key: "id", upserts: [{ id: "a", status: "working" }], deleted: [] });
    expect(delta).toHaveBeenCalledOnce();
    expect(replace).not.toHaveBeenCalled();
    expect((await storage.load())?.entities.tasks).toEqual({ a: { id: "a", name: "A", status: "working" }, b: { id: "b", name: "B" } });
  } finally {
    storage.close();
    Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
  }
});


it.each([3, 128, 257])("reads %i checkpoint rows without losing equal-sequence records", async count => {
  const originalIndexedDB = globalThis.indexedDB;
  const originalKeyRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
  const storage = new IndexedDBLocalReplicaStorage(`checkpoint-${Math.random()}`);
  try {
    const replica = new LocalReplica(storage);
    const rows = Array.from({ length: count }, (_, index) => ({ id: `task-${index}`, status: "new" }));
    await replica.replaceWindow({ signature: "tasks", kind: "replica", entity: "tasks", key: "id", rows, completeness: "complete", source: "server" });
    const cursor = vi.spyOn(IDBIndex.prototype, "openCursor");
    try {
      const changes = await storage.readChanges("default", 0);
      expect(Object.keys(changes.entities.tasks)).toHaveLength(count);
      for (const row of rows) expect(changes.entities.tasks[row.id]).toEqual(row);
      if (count < 128) expect(cursor).not.toHaveBeenCalled();
      const next = await storage.readChanges("default", changes.sequence);
      expect(next.entities).toEqual({});
      expect(next.windows).toEqual({});
    } finally { cursor.mockRestore(); }
  } finally {
    storage.close();
    Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
  }
});


it('persists metadata and explicit projections without materializing snapshot entities', async () => {
  const originalIndexedDB = globalThis.indexedDB;
  const originalKeyRange = globalThis.IDBKeyRange;
  Object.assign(globalThis, { indexedDB: new IDBFactory(), IDBKeyRange });
  Dexie.dependencies.indexedDB = globalThis.indexedDB;
  Dexie.dependencies.IDBKeyRange = globalThis.IDBKeyRange;
  const storage = new IndexedDBLocalReplicaStorage(`lazy-snapshot-${Math.random()}`);
  try {
    const window = {signature:'tasks',kind:'replica' as const,entity:'tasks',key:'id',ids:['a'],completeness:'complete' as const,source:'server' as const};
    const snapshot = {entities:{tasks:{a:{id:'a',status:'new'}}},liveQueries:{tasks:window}};
    await storage.replaceWindow(window,snapshot);
    Object.defineProperty(snapshot,'entities',{get(){throw new Error('materialized unused table');}});
    await storage.applyWindowDelta(window,{upserts:[],deleted:[]},snapshot);
    await storage.replaceWindow(window,snapshot,'default',[{id:'a',status:'working'}]);
    expect((await storage.load())?.entities.tasks.a).toEqual({id:'a',status:'working'});
  } finally {
    storage.close();
    Object.assign(globalThis, { indexedDB: originalIndexedDB, IDBKeyRange: originalKeyRange });
  }
});
