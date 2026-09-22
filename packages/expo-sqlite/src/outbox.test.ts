import { afterEach, expect, it } from "vitest";
import { DatabaseSync } from "node:sqlite";
import { createReducerOutbox, type ReducerOutboxEntry } from "@gonvex/client";
import { ExpoSQLiteOutboxStore, migrateLegacyOutbox, type ExpoSQLiteDatabase } from "./index";

const databases: DatabaseSync[] = [];
afterEach(() => {
  for (const db of databases.splice(0)) db.close();
});

function sqlite() {
  const db = new DatabaseSync(":memory:");
  databases.push(db);
  const statements: string[] = [];
  const adapter: ExpoSQLiteDatabase = {
    async execAsync(sql) { db.exec(sql); },
    async runAsync(sql, ...params) { statements.push(sql); return db.prepare(sql).run(...(params as any[])); },
    async getAllAsync<T>(sql: string, ...params: unknown[]) { return db.prepare(sql).all(...(params as any[])) as T[]; },
    async getFirstAsync<T>(sql: string, ...params: unknown[]) { return (db.prepare(sql).get(...(params as any[])) as T) ?? null; },
    async withTransactionAsync(task) {
      db.exec("BEGIN");
      try {
        await task();
        db.exec("COMMIT");
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    },
  };
  return { db, adapter, statements };
}

const scope = ["identity", "ws://runtime", "project", "tenant", "issuer", "account"].join("\u0000");

function legacyEntry(overrides: Partial<ReducerOutboxEntry>): ReducerOutboxEntry {
  return {
    id: 1, scope, path: "tasks.update", args: { id: "t1", title: "Offline" }, idempotencyKey: "command-1",
    entityKeys: ["tasks:t1"], createdAt: 10, attempts: 0, nextAttemptAt: 10, state: "pending",
    patches: [{ entity: "tasks", rowId: "t1", op: "patch", fields: { title: "Offline" } }],
    ...overrides,
  };
}

it("persists one row per intent and survives a restart through the SDK queue", async () => {
  const { adapter, db } = sqlite();
  const outbox = createReducerOutbox({ store: new ExpoSQLiteOutboxStore(adapter) });
  const first = await outbox.enqueue({ scope, path: "tasks.update", args: { n: 1 }, idempotencyKey: "a", entityKeys: ["tasks:t1"] });
  const second = await outbox.enqueue({ scope, path: "tasks.update", args: { n: 2 }, idempotencyKey: "b", entityKeys: ["tasks:t2"] });
  await outbox.markInflight(first.id);
  await outbox.fail(first.id, "pool timed out", { errorClass: "transient", maxAttempts: 1 });
  await outbox.reject(second.id, "denied");
  expect((db.prepare("SELECT id, state FROM _gonvex_outbox ORDER BY id").all() as any[]).map((row) => [row.id, row.state]))
    .toEqual([[first.id, "failed"], [second.id, "rejected"]]);

  const restarted = createReducerOutbox({ store: new ExpoSQLiteOutboxStore(adapter) });
  const entries = await restarted.loadAll(scope);
  expect(entries).toMatchObject([
    { id: first.id, idempotencyKey: "a", state: "failed", lastError: "pool timed out", args: { n: 1 } },
    { id: second.id, idempotencyKey: "b", state: "rejected", lastError: "denied" },
  ]);
  await restarted.retry(first.id);
  await restarted.ack(first.id);
  await restarted.discard(second.id, ["rejected"]);
  expect(db.prepare("SELECT COUNT(*) AS count FROM _gonvex_outbox").get()).toEqual({ count: 0 });
  // Deleted ids are never reused.
  const third = await restarted.enqueue({ scope, path: "tasks.update", args: {} });
  expect(third.id).toBeGreaterThan(second.id);
});

it("updates a single row transactionally instead of rewriting the queue", async () => {
  const { adapter, statements } = sqlite();
  const store = new ExpoSQLiteOutboxStore(adapter);
  for (let id = 1; id <= 50; id += 1) await store.put(legacyEntry({ id, idempotencyKey: `command-${id}` }));
  statements.length = 0;
  const updated = await store.update(25, (entry) => ({ ...entry, state: "inflight" }));
  expect(updated?.state).toBe("inflight");
  expect(statements.filter((sql) => /INSERT OR REPLACE/.test(sql))).toHaveLength(1);
  await expect(store.update(25, (entry) => ({ ...entry, idempotencyKey: "other" }))).rejects.toThrow("identity");
  expect((await store.load(scope)).find((entry) => entry.id === 25)?.idempotencyKey).toBe("command-25");
  expect(await store.update(999, (entry) => entry)).toBeUndefined();
});

it("keeps local-execution admission atomic through append", async () => {
  const { adapter } = sqlite();
  const store = new ExpoSQLiteOutboxStore(adapter);
  const [a, b] = await Promise.all([
    store.append({ ...legacyEntry({}), idempotencyKey: "x" } as Omit<ReducerOutboxEntry, "id">),
    store.append({ ...legacyEntry({}), idempotencyKey: "y" } as Omit<ReducerOutboxEntry, "id">),
  ]);
  expect(new Set([a.id, b.id]).size).toBe(2);
  expect((await store.load()).map((entry) => entry.idempotencyKey).sort()).toEqual(["x", "y"]);
});

it("imports a legacy AsyncStorage outbox once, preserving order and idempotency keys", async () => {
  const { adapter } = sqlite();
  const store = new ExpoSQLiteOutboxStore(adapter);
  const legacyKey = "wh_gonvex_v2_reducer_outbox";
  const blob = JSON.stringify([
    legacyEntry({ id: 7, idempotencyKey: "second", state: "inflight", attempts: 3, lastError: "timeout" }),
    legacyEntry({ id: 3, idempotencyKey: "first" }),
    { not: "an entry" },
  ]);
  const values = new Map<string, string>([[legacyKey, blob]]);
  const storage = {
    getItem: async (key: string) => values.get(key) ?? null,
    removeItem: async (key: string) => { values.delete(key); },
  };
  expect(await migrateLegacyOutbox({ store, storage, key: legacyKey })).toEqual({ imported: 2, skipped: 0, invalid: 1 });
  expect(values.has(legacyKey)).toBe(false);
  const imported = await store.load(scope);
  expect(imported.map((entry) => [entry.id, entry.idempotencyKey, entry.state, entry.attempts])).toEqual([
    [3, "first", "pending", 0],
    // A row that was mid-send when the app died resumes as pending, same key.
    [7, "second", "pending", 3],
  ]);
  expect(imported[0]?.patches).toEqual([{ entity: "tasks", rowId: "t1", op: "patch", fields: { title: "Offline" } }]);

  // Re-running (e.g. a crash before removeItem) never duplicates an intent.
  expect(await store.importLegacy(blob)).toEqual({ imported: 0, skipped: 2, invalid: 1 });
  expect(await migrateLegacyOutbox({ store, storage, key: legacyKey })).toEqual({ imported: 0, skipped: 0, invalid: 0 });

  // The SDK sequence continues after the imported ids.
  const outbox = createReducerOutbox({ store });
  const next = await outbox.enqueue({ scope, path: "tasks.update", args: {} });
  expect(next.id).toBe(8);
  expect((await outbox.loadAll(scope)).map((entry) => entry.idempotencyKey)).toEqual(["first", "second", next.idempotencyKey]);
});

it("assigns a fresh id when a legacy id is already taken by a different intent", async () => {
  const { adapter } = sqlite();
  const store = new ExpoSQLiteOutboxStore(adapter);
  await store.put(legacyEntry({ id: 1, idempotencyKey: "existing" }));
  expect(await store.importLegacy([legacyEntry({ id: 1, idempotencyKey: "legacy" })])).toMatchObject({ imported: 1 });
  expect((await store.load()).map((entry) => [entry.id, entry.idempotencyKey])).toEqual([[1, "existing"], [2, "legacy"]]);
});

it("leaves an unreadable legacy blob in place", async () => {
  const { adapter } = sqlite();
  const store = new ExpoSQLiteOutboxStore(adapter);
  const values = new Map([["legacy", "{not json"]]);
  const storage = { getItem: async (key: string) => values.get(key) ?? null, removeItem: async (key: string) => { values.delete(key); } };
  await expect(migrateLegacyOutbox({ store, storage, key: "legacy" })).rejects.toThrow();
  expect(values.get("legacy")).toBe("{not json");
  values.set("legacy", JSON.stringify({ entries: [] }));
  await expect(migrateLegacyOutbox({ store, storage, key: "legacy" })).rejects.toThrow("JSON array");
  expect(values.has("legacy")).toBe(true);
});

it("clears one scope and lists scopes through the SDK", async () => {
  const { adapter } = sqlite();
  const store = new ExpoSQLiteOutboxStore(adapter);
  const outbox = createReducerOutbox({ store });
  const other = `${scope}-other`;
  await outbox.enqueue({ scope, path: "a", args: {} });
  await outbox.enqueue({ scope: other, path: "b", args: {} });
  expect((await outbox.listScopes()).map((summary) => summary.scope).sort()).toEqual([scope, other].sort());
  expect(await outbox.purgeScope(other)).toBe(1);
  expect((await store.load()).map((entry) => entry.scope)).toEqual([scope]);
});
