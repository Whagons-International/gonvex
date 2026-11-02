import { DatabaseSync } from "node:sqlite";
import { Dexie } from "dexie";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { DexieReducerOutbox, StoreReducerOutbox, type ReducerOutboxEntry } from "./outbox.js";
import { ExpoSQLiteOutboxStore } from "../../expo-sqlite/src/outbox.js";
import type { ExpoSQLiteDatabase } from "../../expo-sqlite/src/index.js";

const scope = "owner";
const dependencies = { reads: [], writes: [{ entity: "tasks", rowId: "a" }], scans: [] };
const draft = (key: string, value: number) => ({ scope, path: "tasks.set", args: { value }, idempotencyKey: key,
  entityKeys: ["tasks:a"], coalesceKey: '["a"]', dependencies });

it.each((["indexeddb", "sqlite"] as const).flatMap(adapter => ["legacy-pending", "legacy-inflight", "inflight-stale-provenance", "fresh-unsent"].map(mode => ({ adapter, mode }))))
("$adapter preserves recovered command identity before coalescing, $mode", async ({ adapter, mode }) => {
  const prior = { ...draft("original", 1), id: 1, state: mode === "legacy-pending" ? "pending" : "inflight",
    attempts: 0, createdAt: 1, nextAttemptAt: 0, ...(mode === "inflight-stale-provenance" ? { knownUnsent: true } : {}) } as ReducerOutboxEntry;
  let queue: DexieReducerOutbox | StoreReducerOutbox;
  let cleanup = async () => {};
  if (adapter === "indexeddb") {
    const indexedDB = new IDBFactory(); const name = `coalescing-${crypto.randomUUID()}`;
    const raw = new Dexie(name, { indexedDB, IDBKeyRange });
    raw.version(2).stores({ entries: "++id, scope, state, nextAttemptAt, [scope+state], [scope+nextAttemptAt]" });
    await raw.open(); if (mode !== "fresh-unsent") await raw.table("entries").put(prior); raw.close();
    const owned = new DexieReducerOutbox({ databaseName: name, indexedDB, IDBKeyRange }); queue = owned;
    cleanup = async () => { (owned as unknown as { database?: Dexie }).database?.close(); await new Dexie(name, { indexedDB, IDBKeyRange }).delete(); };
  } else {
    const db = new DatabaseSync(":memory:");
    const driver: ExpoSQLiteDatabase = {
      async execAsync(sql) { db.exec(sql); },
      async runAsync(sql, ...args) { return db.prepare(sql).run(...args as any[]); },
      async getAllAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).all(...args as any[]) as T[]; },
      async getFirstAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).get(...args as any[]) as T ?? null; },
      async withTransactionAsync(run) { db.exec("BEGIN"); try { await run(); db.exec("COMMIT"); } catch (error) { db.exec("ROLLBACK"); throw error; } },
    };
    const store = new ExpoSQLiteOutboxStore(driver);
    if (mode !== "fresh-unsent") await store.put(prior);
    queue = new StoreReducerOutbox(store);
    cleanup = async () => { db.close(); };
  }
  try {
    if (mode === "fresh-unsent") await queue.enqueue(draft("original", 1));
    await queue.loadAll(scope);
    const next = await queue.enqueue(draft("new-command", 2));
    const entries = await queue.list(scope);
    if (mode === "fresh-unsent") {
      expect(next.id).toBe(1); expect(entries).toHaveLength(1);
      expect(entries[0]).toMatchObject({ idempotencyKey: "new-command", args: { value: 2 } });
    } else {
      expect(next.id).toBe(2); expect(entries).toHaveLength(2);
      expect(entries[0]).toMatchObject({ id: 1, idempotencyKey: "original", args: { value: 1 }, state: "pending" });
      expect(entries[1]).toMatchObject({ id: 2, idempotencyKey: "new-command", args: { value: 2 } });
    }
  } finally { await cleanup(); }
});
