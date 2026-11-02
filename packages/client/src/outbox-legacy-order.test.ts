import { DatabaseSync } from "node:sqlite";
import { Dexie } from "dexie";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { DexieReducerOutbox, StoreReducerOutbox, type ReducerOutbox, type ReducerOutboxEntry } from "./outbox.js";
import { createKvOutboxStore, createMemoryGonvexKv, kvReducerOutboxTable } from "./kv-stores.js";
import { ExpoSQLiteOutboxStore } from "../../expo-sqlite/src/outbox.js";
import type { ExpoSQLiteDatabase } from "../../expo-sqlite/src/index.js";

const scope = "legacy-owner";
it.each((["kv", "indexeddb", "sqlite"] as const).flatMap(adapter => ["missing-keys", "empty-unknown-keys", "explicit-empty-dependencies"].map(mode => ({ adapter, mode }))))
("$adapter preserves recovery order with $mode", async ({ adapter, mode }) => {
  const earlier = { id: 1, scope, path: "tasks.first", args: { n: 1 }, idempotencyKey: "first", state: "pending",
    createdAt: 1, attempts: 0, nextAttemptAt: 1000, ...(mode === "missing-keys" ? {} : { entityKeys: [] }),
    ...(mode === "explicit-empty-dependencies" ? { dependencies: { reads: [], writes: [], scans: [] } } : {}) } as ReducerOutboxEntry;
  const later: ReducerOutboxEntry = { id: 2, scope, path: "tasks.second", args: { n: 2 }, idempotencyKey: "second", state: "pending",
    createdAt: 2, attempts: 0, nextAttemptAt: 0, entityKeys: ["tasks:a"], dependencies: { reads: [], writes: [{ entity: "tasks", rowId: "a" }], scans: [] } };
  let queue: ReducerOutbox; let cleanup = async () => {};
  if (adapter === "kv") {
    const kv = createMemoryGonvexKv();
    for (const entry of [earlier, later]) await kv.set(kvReducerOutboxTable, String(entry.id), JSON.stringify(entry));
    queue = new StoreReducerOutbox(createKvOutboxStore(kv));
  } else if (adapter === "indexeddb") {
    const indexedDB = new IDBFactory(); const name = `legacy-order-${crypto.randomUUID()}`;
    const database = new Dexie(name, { indexedDB, IDBKeyRange });
    database.version(2).stores({ entries: "++id, scope, state, nextAttemptAt, [scope+state], [scope+nextAttemptAt]" });
    await database.table("entries").bulkPut([earlier, later]); database.close();
    const owned = new DexieReducerOutbox({ databaseName: name, indexedDB, IDBKeyRange }); queue = owned;
    cleanup = async () => { (owned as unknown as { database?: Dexie }).database?.close(); await new Dexie(name, { indexedDB, IDBKeyRange }).delete(); };
  } else {
    const database = new DatabaseSync(":memory:");
    const driver: ExpoSQLiteDatabase = {
      async execAsync(sql) { database.exec(sql); },
      async runAsync(sql, ...args) { return database.prepare(sql).run(...args as any[]); },
      async getAllAsync<T>(sql: string, ...args: unknown[]) { return database.prepare(sql).all(...args as any[]) as T[]; },
      async getFirstAsync<T>(sql: string, ...args: unknown[]) { return database.prepare(sql).get(...args as any[]) as T ?? null; },
      async withTransactionAsync(run) { database.exec("BEGIN"); try { await run(); database.exec("COMMIT"); } catch (error) { database.exec("ROLLBACK"); throw error; } },
    };
    const store = new ExpoSQLiteOutboxStore(driver);
    await store.put(earlier); await store.put(later); queue = new StoreReducerOutbox(store);
    cleanup = async () => { database.close(); };
  }
  try {
    expect((await queue.loadAll(scope)).map(entry => [entry.id, entry.idempotencyKey, entry.args])).toEqual([[1, "first", { n: 1 }], [2, "second", { n: 2 }]]);
    if (mode === "explicit-empty-dependencies") {
      expect(await queue.nextReady(scope, 500)).toMatchObject({ id: 2 });
    } else {
      expect(await queue.nextReady(scope, 500)).toBeUndefined();
      expect(await queue.nextReady(scope, 1000)).toMatchObject({ id: 1 });
      await queue.markCommitted(1);
      expect(await queue.nextReady(scope, 1000)).toMatchObject({ id: 2 });
    }
  } finally { await cleanup(); }
});
