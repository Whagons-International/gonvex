import { describe, expect, it } from "vitest";
import { indexedDB, IDBKeyRange } from "fake-indexeddb";
import { DexieReducerOutbox, StoreReducerOutbox, type OutboxStore, type ReducerOutboxEntry, type ReducerOutbox } from "./outbox.js";
import { createKvOutboxStore, createMemoryGonvexKv } from "./kv-stores.js";

const scope = "test-account";
const draft = { scope, path: "tasks.save", args: {} };
function store(atomic: boolean, batch: boolean): OutboxStore {
  const rows = new Map<number, ReducerOutboxEntry>();
  return {
    load: async () => [...rows.values()].map(entry => structuredClone(entry)),
    put: async entry => { rows.set(entry.id, structuredClone(entry)); },
    delete: async id => { rows.delete(id); },
    clear: async () => { rows.clear(); },
    ...(atomic ? { update: async (id: number, change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry) => {
      const entry = rows.get(id); if (!entry) return;
      const next = change(structuredClone(entry)); rows.set(id, next); return next;
    } } : {}),
    ...(batch ? { updateMany: async (ids: number[], change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry) => {
      const result: ReducerOutboxEntry[] = [];
      for (const id of ids) { const entry = rows.get(id); if (entry) { const next = change(structuredClone(entry)); rows.set(id, next); result.push(next); } }
      return result;
    } } : {}),
  };
}
const factories: Array<[string, () => ReducerOutbox]> = [
  ["IndexedDB", () => new DexieReducerOutbox({ databaseName: `admission-${crypto.randomUUID()}`, indexedDB, IDBKeyRange })],
  ["memory", () => new StoreReducerOutbox(store(true, true), { enabled: false })],
  ["KV", () => new StoreReducerOutbox(createKvOutboxStore(createMemoryGonvexKv()))],
  ["atomic store", () => new StoreReducerOutbox(store(true, false))],
  ["batch store", () => new StoreReducerOutbox(store(true, true))],
];
async function populate(queue: ReducerOutbox) {
  const entries = await Promise.all(Array.from({ length: 5 }, () => queue.enqueue(draft)));
  await queue.markInflight(entries[1]!.id);
  await queue.markCommitted(entries[2]!.id);
  await queue.fail(entries[3]!.id, "failed", { maxAttempts: 1 });
  await queue.reject(entries[4]!.id, "rejected");
  return entries;
}
describe.each(factories)("%s delivery state", (_, create) => {
  it("does not start committed or parked work, while inflight start stays idempotent", async () => {
    const queue = create(); const entries = await populate(queue);
    const ids = entries.map(entry => entry.id);
    expect(await queue.markInflight(ids[2]!)).toBe(false);
    expect(await queue.markInflight(ids[3]!)).toBe(false);
    expect(await queue.markInflight(ids[4]!)).toBe(false);
    expect(await queue.markInflight(ids[0]!)).toBe(true);
    expect(await queue.markInflight(ids[1]!)).toBe(true);
    expect((await queue.list(scope)).map(entry => entry.state)).toEqual(["inflight", "inflight", "committed", "failed", "rejected"]);
  });
  it("recovery makes only inflight work pending and preserves record identity", async () => {
    const queue = create(); await populate(queue);
    const before = await queue.list(scope);
    for (const entry of before) await queue.markPending(entry.id);
    const after = await queue.list(scope);
    expect(after.slice(2)).toEqual(before.slice(2));
    expect(after.map(entry => entry.state)).toEqual(["pending", "pending", "committed", "failed", "rejected"]);
    expect(after.map(entry => [entry.id, entry.idempotencyKey, entry.args])).toEqual(before.map(entry => [entry.id, entry.idempotencyKey, entry.args]));
  });
});
describe.each(factories.filter(([name]) => name !== "IndexedDB"))("%s batch admission", (_, create) => {
  it("admits only newly pending records, once per ID", async () => {
    const queue = create(); const entries = await populate(queue);
    const ids = entries.map(entry => entry.id);
    expect((await queue.markInflightMany!([...ids, ids[0]!])).map(entry => entry.id)).toEqual([ids[0]]);
    expect((await queue.list(scope)).map(entry => entry.state)).toEqual(["inflight", "inflight", "committed", "failed", "rejected"]);
    expect(await queue.markInflightMany!(ids)).toEqual([]);
  });
});
