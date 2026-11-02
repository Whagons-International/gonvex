import { expect, it } from "vitest";
import { createKvOutboxStore, createMemoryGonvexKv, kvReducerOutboxTable } from "./kv-stores.js";
import { createReducerOutbox } from "./outbox.js";

const draft = { scope: "owner", path: "tasks.set", args: { value: 1 }, idempotencyKey: "original" };
it("blocks KV recovery and admission when durable history cannot be read", async () => {
  const kv = createMemoryGonvexKv();
  const first = await createReducerOutbox({ store: createKvOutboxStore(kv) }).enqueue(draft);
  const original = await kv.get(kvReducerOutboxTable, String(first.id));
  const queue = createReducerOutbox({ store: createKvOutboxStore({ ...kv, async list() { throw new Error("injected KV read failure"); } }) });
  await expect(queue.loadAll(draft.scope)).rejects.toThrow("injected KV read failure");
  await expect(queue.enqueue({ ...draft, idempotencyKey: "later" })).rejects.toThrow("injected KV read failure");
  expect(await kv.get(kvReducerOutboxTable, String(first.id))).toBe(original);
});

it("does not report a failed KV append as successful session-only admission", async () => {
  const kv = createMemoryGonvexKv(); let fail = false;
  const store = createKvOutboxStore({ ...kv, async set(...args) { if (fail) throw new Error("injected KV write failure"); await kv.set(...args); } });
  const queue = createReducerOutbox({ store }); const first = await queue.enqueue(draft);
  fail = true;
  await expect(queue.enqueue({ ...draft, idempotencyKey: "later" })).rejects.toThrow("injected KV write failure");
  expect((await queue.list(draft.scope)).map(entry => entry.idempotencyKey)).toEqual(["original"]);
  expect(await store.load()).toMatchObject([{ id: first.id, idempotencyKey: "original" }]);
});

it.each(["markInflight", "markCommitted"] as const)("retains its prior cache and durable state after failed KV %s", async operation => {
  const kv = createMemoryGonvexKv(); let fail = false;
  const store = createKvOutboxStore({ ...kv, async set(...args) { if (fail) throw new Error("injected KV write failure"); await kv.set(...args); } });
  const queue = createReducerOutbox({ store }); const first = await queue.enqueue(draft);
  if (operation === "markCommitted") await queue.markInflight(first.id);
  const before = await queue.list(draft.scope); fail = true;
  await expect(queue[operation](first.id)).rejects.toThrow("injected KV write failure");
  expect(await queue.list(draft.scope)).toEqual(before);
  expect(await store.load()).toMatchObject(before.map(({ patches: _patches, ...entry }) => entry));
  fail = false; await queue[operation](first.id);
  expect((await queue.list(draft.scope))[0]?.state).toBe(operation === "markInflight" ? "inflight" : "committed");
});

it("keeps accepted history available after a failed KV retirement, then permits retry", async () => {
  const kv = createMemoryGonvexKv(); let fail = false;
  const store = createKvOutboxStore({ ...kv, async delete(...args) { if (fail) throw new Error("injected KV delete failure"); await kv.delete(...args); } });
  const queue = createReducerOutbox({ store }); const first = await queue.enqueue(draft);
  await queue.markInflight(first.id); await queue.markCommitted(first.id);
  const before = await queue.list(draft.scope); fail = true;
  await expect(queue.ack(first.id)).rejects.toThrow("injected KV delete failure");
  expect(await queue.list(draft.scope)).toEqual(before);
  expect(await queue.nextReady(draft.scope, Number.MAX_SAFE_INTEGER)).toBeUndefined();
  expect(await store.load()).toMatchObject([{ id: first.id, state: "committed", idempotencyKey: "original" }]);
  fail = false; await queue.ack(first.id);
  expect(await store.load()).toEqual([]); expect(await queue.list(draft.scope)).toEqual([]);
});

it("does not rearm a durable acceptance when its write commits before the promise rejects", async () => {
  const kv = createMemoryGonvexKv(); let rejectAfterWrite = false;
  const host = { ...kv, async set(...args: Parameters<typeof kv.set>) {
    await kv.set(...args);
    if (rejectAfterWrite) throw new Error("lost completion after committed KV write");
  } };
  const store = createKvOutboxStore(host); const queue = createReducerOutbox({ store });
  const first = await queue.enqueue(draft); await queue.markInflight(first.id);
  rejectAfterWrite = true;
  await expect(queue.markCommitted(first.id)).rejects.toThrow("lost completion");
  expect(await store.load()).toMatchObject([{ id: first.id, state: "committed" }]);
  rejectAfterWrite = false;
  await queue.fail(first.id, "late timeout");
  await queue.markPending(first.id);
  expect(await queue.markInflight(first.id)).toBe(false);
  expect(await store.load()).toMatchObject([{ id: first.id, state: "committed", idempotencyKey: "original" }]);
});
