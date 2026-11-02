import { Dexie } from "dexie";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { DexieReducerOutbox, createReducerOutbox } from "./outbox.js";
import { createKvOutboxStore, createMemoryGonvexKv, kvReducerOutboxTable } from "./kv-stores.js";

it.each(["pending", "committed"] as const)("preserves an unowned v1 %s intent and refuses silent recovery", async state => {
  const indexedDB = new IDBFactory();
  const name = `unowned-${state}-${crypto.randomUUID()}`;
  const options = { indexedDB, IDBKeyRange };
  const raw = new Dexie(name, options);
  raw.version(1).stores({ entries: "++id, state, nextAttemptAt" });
  const original = { id: 1, path: "tasks.save", args: { title: "preserve" }, idempotencyKey: "accepted-key", state, attempts: 0, createdAt: 1, nextAttemptAt: 0 };
  await raw.table("entries").put(original); raw.close();
  const outbox = new DexieReducerOutbox({ databaseName: name, ...options });
  try {
    await expect(outbox.loadAll("new-owner")).rejects.toThrow(/owner|scope|recovery|corrupt/i);
    // The failed upgrade must roll back, leaving the original bytes available
    // for an explicit owner-resolution workflow rather than deleting the row.
    const inspect = new Dexie(name, options);
    await inspect.open();
    expect(await inspect.table("entries").toArray()).toEqual([original]);
    inspect.close();
  } finally {
    (outbox as unknown as { database?: Dexie }).database?.close();
    await new Dexie(name, options).delete();
  }
});

it.each(["{", "null", '{"id":1}', '{"id":1,"state":"future"}'])("refuses a corrupt KV journal row without losing its bytes: %s", async value => {
  const kv = createMemoryGonvexKv();
  await kv.set(kvReducerOutboxTable, "1", value);
  const store = createKvOutboxStore(kv);
  await expect(store.load()).rejects.toThrow(/outbox|journal|intent/i);
  const outbox = createReducerOutbox({ store });
  await expect(outbox.loadAll("owner")).rejects.toThrow(/outbox|journal|intent/i);
  await expect(outbox.enqueue({ scope: "owner", path: "tasks.save", args: {} })).rejects.toThrow(/outbox|journal|intent/i);
  expect(await kv.get(kvReducerOutboxTable, "1")).toBe(value);
});
