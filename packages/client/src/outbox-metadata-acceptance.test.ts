import { DatabaseSync } from "node:sqlite";
import { expect, it } from "vitest";
import { createReducerOutbox, type OutboxStore } from "./outbox.js";
import { createKvOutboxStore, createMemoryGonvexKv } from "./kv-stores.js";
import { ExpoSQLiteOutboxStore } from "../../expo-sqlite/src/outbox.js";
import type { ExpoSQLiteDatabase } from "../../expo-sqlite/src/index.js";

function backingStore(adapter: "kv" | "sqlite") {
  let base: OutboxStore; let close = () => {};
  if (adapter === "kv") base = createKvOutboxStore(createMemoryGonvexKv());
  else {
    const db = new DatabaseSync(":memory:");
    const driver: ExpoSQLiteDatabase = {
      async execAsync(sql) { db.exec(sql); },
      async runAsync(sql, ...args) { return db.prepare(sql).run(...args as any[]); },
      async getAllAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).all(...args as any[]) as T[]; },
      async getFirstAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).get(...args as any[]) as T ?? null; },
      async withTransactionAsync(run) { db.exec("BEGIN"); try { await run(); db.exec("COMMIT"); } catch (error) { db.exec("ROLLBACK"); throw error; } },
    };
    base = new ExpoSQLiteOutboxStore(driver); close = () => db.close();
  }
  return { base, close };
}

it.each(["kv", "sqlite"] as const)("%s metadata refresh cannot undo acceptance while its write is deferred", async adapter => {
  const { base, close } = backingStore(adapter);
  let defer = false, release!: () => void, entered!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  const started = new Promise<void>(resolve => { entered = resolve; });
  async function beforeWrite() { if (defer) { defer = false; entered(); await held; } }
  const store: OutboxStore = {
    ...base,
    load: scope => base.load(scope),
    put: async entry => { await beforeWrite(); await base.put(entry); },
    delete: id => base.delete(id), clear: scope => base.clear(scope),
    ...(base.allocateId ? { allocateId: () => base.allocateId!() } : {}),
    ...(base.update ? { update: async (id, change) => { await beforeWrite(); return base.update!(id, change); } } : {}),
  };
  try {
    const queue = createReducerOutbox({ store });
    const entry = await queue.enqueue({ scope: "owner", path: "tasks.set", args: { value: 1 }, idempotencyKey: "original" });
    await queue.markInflight(entry.id);
    defer = true;
    const execution = { artifactHash: "artifact", changes: [], result: null } as any;
    const metadata = queue.updateLocal(entry.id, [], execution);
    await started;
    await queue.markCommitted(entry.id);
    expect(await base.load()).toMatchObject([{ id: entry.id, state: "committed" }]);
    release(); await metadata;
    expect(await base.load()).toMatchObject([{ id: entry.id, state: "committed", idempotencyKey: "original", localExecution: execution }]);
    expect(await queue.markInflight(entry.id)).toBe(false);
  } finally { release(); close(); }
});


it.each(["kv", "sqlite"] as const)("%s recovery cannot replace a peer's acceptance from an older inflight cache", async adapter => {
  const { base, close } = backingStore(adapter);
  try {
    const older = createReducerOutbox({ store: base });
    const original = await older.enqueue({ scope: "owner", path: "tasks.set", args: { value: 1 }, idempotencyKey: "original" });
    await older.markInflight(original.id);
    const peer = createReducerOutbox({ store: base });
    await peer.markCommitted(original.id);
    expect(await base.load()).toMatchObject([{ id: original.id, state: "committed" }]);
    expect(await older.loadAll("owner")).toMatchObject([{ id: original.id, state: "committed", idempotencyKey: "original" }]);
    expect(await base.load()).toMatchObject([{ id: original.id, state: "committed" }]);
    expect(await older.markInflight(original.id)).toBe(false);
  } finally { close(); }
});
