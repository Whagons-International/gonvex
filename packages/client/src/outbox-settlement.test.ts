import { describe, expect, it } from "vitest";
import { indexedDB, IDBKeyRange } from "fake-indexeddb";
import { DexieReducerOutbox, StoreReducerOutbox, type OutboxStore, type ReducerOutboxEntry, type ReducerOutbox } from "./outbox.js";
import { createKvOutboxStore, createMemoryGonvexKv } from "./kv-stores.js";

const scope = "settlement-account";
const draft = { scope, path: "tasks.save", args: { value: 1 }, idempotencyKey: "same-intent" };
function store(batch: boolean): OutboxStore {
  const rows = new Map<number, ReducerOutboxEntry>();
  const update = async (id: number, change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry) => {
    const entry = rows.get(id);
    if (!entry) return;
    const next = change(structuredClone(entry));
    rows.set(id, structuredClone(next));
    return structuredClone(next);
  };
  return {
    load: async () => [...rows.values()].map(entry => structuredClone(entry)),
    put: async entry => { rows.set(entry.id, structuredClone(entry)); },
    delete: async id => { rows.delete(id); },
    clear: async () => { rows.clear(); },
    update,
    ...(batch ? { updateMany: async (ids: number[], change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry) => {
      const result: ReducerOutboxEntry[] = [];
      for (const id of ids) { const next = await update(id, change); if (next) result.push(next); }
      return result;
    } } : {}),
  };
}
const factories: Array<[string, () => ReducerOutbox]> = [
  ["IndexedDB", () => new DexieReducerOutbox({ databaseName: `settlement-${crypto.randomUUID()}`, indexedDB, IDBKeyRange })],
  ["memory", () => new StoreReducerOutbox(store(true), { enabled: false })],
  ["KV", () => new StoreReducerOutbox(createKvOutboxStore(createMemoryGonvexKv()))],
  ["atomic store", () => new StoreReducerOutbox(store(false))],
  ["batch store", () => new StoreReducerOutbox(store(true))],
];

describe.each(factories)("%s settlement", (_, create) => {
  it("keeps an accepted intent and its metadata unchanged after late rejection and retry", async () => {
    const queue = create();
    try {
      const entry = await queue.enqueue(draft);
      await queue.markInflight(entry.id);
      await queue.markCommitted(entry.id);
      const accepted = await queue.list(scope);
      expect(await queue.reject(entry.id, "late rejected response")).toBeUndefined();
      expect(await queue.list(scope)).toEqual(accepted);
      expect(await queue.retry(entry.id)).toBeUndefined();
      await queue.markPending(entry.id);
      expect(await queue.markInflight(entry.id)).toBe(false);
      expect(await queue.fail(entry.id, "late timeout", { maxAttempts: 1 })).toBeUndefined();
      expect(await queue.loadAll(scope)).toEqual(accepted);
      expect(await queue.nextReady(scope, Number.MAX_SAFE_INTEGER)).toBeUndefined();
    } finally { queue.dispose?.(); }
  });

  it("records positive acceptance identically for single and batch settlement", async () => {
    const queue = create();
    try {
      const entries = [];
      for (let index = 0; index < 5; index++) entries.push(await queue.enqueue({ ...draft, idempotencyKey: `intent-${index}` }));
      await queue.markInflight(entries[1]!.id);
      await queue.markCommitted(entries[2]!.id);
      await queue.fail(entries[3]!.id, "timed out", { maxAttempts: 1 });
      await queue.reject(entries[4]!.id, "earlier rejection");
      const before = await queue.list(scope);
      if (queue.markCommittedMany) await queue.markCommittedMany(entries.map(entry => entry.id));
      else for (const entry of entries) await queue.markCommitted(entry.id);
      expect(await queue.list(scope)).toEqual(before.map(entry => ({ ...entry, state: "committed" })));
    } finally { queue.dispose?.(); }
  });

  it("never recreates an acknowledged record through a late settlement callback", async () => {
    const queue = create();
    try {
      const entry = await queue.enqueue(draft);
      await queue.markInflight(entry.id);
      await queue.markCommitted(entry.id);
      await queue.ack(entry.id);
      await queue.reject(entry.id, "late rejection");
      await queue.fail(entry.id, "late timeout");
      await queue.markCommitted(entry.id);
      await queue.markPending(entry.id);
      expect(await queue.markInflight(entry.id)).toBe(false);
      expect(await queue.retry(entry.id)).toBeUndefined();
      expect(await queue.loadAll(scope)).toEqual([]);
    } finally { queue.dispose?.(); }
  });
});
