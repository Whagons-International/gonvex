import { expect, it } from "vitest";
import { createKvOutboxStore, createMemoryGonvexKv } from "./kv-stores.js";
import { kvReducerOutboxMetaTable } from "./kv-sequence.js";
import { createReducerOutbox } from "./outbox.js";
const draft = { scope: "alice", path: "tasks.save", args: { value: 1 }, idempotencyKey: "accepted-original" };

it("does not reuse a retired KV journal position while an old owner can still retire it", async () => {
  const kv = createMemoryGonvexKv();
  const oldOwner = createReducerOutbox({ store: createKvOutboxStore(kv) });
  const original = await oldOwner.enqueue(draft); await oldOwner.markCommitted(original.id);
  const cleanupOwner = createReducerOutbox({ store: createKvOutboxStore(kv) });
  await cleanupOwner.loadAll(draft.scope); await cleanupOwner.ack(original.id);
  const newOwner = createReducerOutbox({ store: createKvOutboxStore(kv) });
  const current = await newOwner.enqueue({ ...draft, scope: "bob", idempotencyKey: "new-owner", args: { value: 2 } });
  await oldOwner.ack(original.id);
  expect(await createKvOutboxStore(kv).load()).toMatchObject([{ id: current.id, scope: "bob", idempotencyKey: "new-owner", args: { value: 2 } }]);
  expect(current.id).toBeGreaterThan(original.id);
});

it("allocates distinct durable KV positions for overlapping queue instances using the same host", async () => {
  const kv = createMemoryGonvexKv();
  const first = createReducerOutbox({ store: createKvOutboxStore(kv) });
  const second = createReducerOutbox({ store: createKvOutboxStore(kv) });
  const entries = await Promise.all([first.enqueue(draft), second.enqueue({ ...draft, idempotencyKey: "other", args: { value: 2 } })]);
  expect(new Set(entries.map(entry => entry.id)).size).toBe(2);
  expect((await createKvOutboxStore(kv).load()).map(entry => entry.idempotencyKey)).toEqual(["accepted-original", "other"]);
});

it("keeps a reservation burned after clear, failed append and reopen", async () => {
  const kv = createMemoryGonvexKv(); const store = createKvOutboxStore(kv);
  const reserved = await store.allocateId!(); await store.clear();
  const reopened = createKvOutboxStore(kv);
  const next = await reopened.allocateId!(); expect(next).toBeGreaterThan(reserved);
  let fail = true;
  const host = { ...kv, async set(...args: Parameters<typeof kv.set>) {
    if (fail && args[0] !== kvReducerOutboxMetaTable) throw new Error("append failed after reservation");
    await kv.set(...args);
  } };
  const queue = createReducerOutbox({ store: createKvOutboxStore(host) });
  await expect(queue.enqueue(draft)).rejects.toThrow("append failed after reservation");
  fail = false;
  const current = await createReducerOutbox({ store: createKvOutboxStore(host) }).enqueue(draft);
  expect(current.id).toBe(next + 2);
});

it.each(["{", "null", '{"version":2,"allocated":0}', '{"version":1,"allocated":1.5}', '{"version":1,"allocated":9007199254740991}'])(
  "refuses invalid or exhausted KV allocation metadata without overwriting it: %s", async raw => {
    const kv = createMemoryGonvexKv(); await kv.set(kvReducerOutboxMetaTable, "sequence", raw);
    const queue = createReducerOutbox({ store: createKvOutboxStore(kv) });
    await expect(queue.enqueue(draft)).rejects.toThrow(/sequence|recovery/i);
    expect(await kv.get(kvReducerOutboxMetaTable, "sequence")).toBe(raw);
    expect(await createKvOutboxStore(kv).load()).toEqual([]);
  });
