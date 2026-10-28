import { expect, it, vi } from "vitest";
import { StoreReducerOutbox, type OutboxStore, type ReducerOutboxEntry } from "./outbox.js";

const draft = { scope: "tenant", path: "tasks.save", args: {}, coalesceKey: "task", dependencies: { reads: [], writes: [{ entity: "tasks", rowId: "a" }], scans: [] } };

function storeFixture() {
  const rows = new Map<number, ReducerOutboxEntry>();
  let sequence = 40;
  const store = {
    load: vi.fn(async () => [...rows.values()]),
    allocateId: vi.fn(async () => ++sequence),
    put: vi.fn(async (entry: ReducerOutboxEntry) => { rows.set(entry.id, structuredClone(entry)); }),
    update: vi.fn(async (id: number, change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry) => {
      const entry = rows.get(id);
      if (!entry) return undefined;
      const updated = change(entry); rows.set(id, updated); return updated;
    }),
    replacePending: vi.fn(async () => undefined as ReducerOutboxEntry | undefined),
    delete: vi.fn(async (id: number) => { rows.delete(id); }),
    clear: vi.fn(async () => { rows.clear(); }),
    close: vi.fn(),
  } satisfies OutboxStore;
  return { store, rows };
}

async function deliverInMemory(queue: StoreReducerOutbox, id: number) {
  await expect(queue.markInflight(id)).resolves.toBe(true);
  await queue.markPending(id);
  expect(await queue.nextReady(draft.scope, Date.now())).toMatchObject({ id, state: "pending" });
  await queue.markInflight(id);
  await queue.markCommitted(id);
  expect(await queue.list(draft.scope)).toContainEqual(expect.objectContaining({ id, state: "committed" }));
  await queue.ack(id);
}

it("never uses store methods when persistence is disabled", async () => {
  const { store } = storeFixture();
  const queue = new StoreReducerOutbox(store, { enabled: false });
  const first = await queue.enqueue(draft);
  const second = await queue.enqueue(draft);
  expect(second.id).toBeGreaterThan(first.id);
  await deliverInMemory(queue, first.id);
  await deliverInMemory(queue, second.id);
  expect(await queue.list(draft.scope)).toEqual([]);
  for (const method of Object.values(store)) expect(method).not.toHaveBeenCalled();
});

it("keeps enqueue and delivery in memory after hydration fails", async () => {
  const { store } = storeFixture();
  store.load.mockRejectedValue(new Error("disk unavailable"));
  store.allocateId.mockRejectedValue(new Error("closed database"));
  const queue = new StoreReducerOutbox(store);
  const first = await queue.enqueue(draft);
  const second = await queue.enqueue(draft);
  expect(second.id).toBeGreaterThan(first.id);
  await deliverInMemory(queue, first.id);
  await deliverInMemory(queue, second.id);
  expect(store.close).toHaveBeenCalledOnce();
  for (const method of [store.allocateId, store.put, store.update, store.replacePending, store.delete]) expect(method).not.toHaveBeenCalled();
});

it("keeps allocated IDs ordered and queued entries intact after a failed write", async () => {
  const { store } = storeFixture();
  const queue = new StoreReducerOutbox(store);
  const first = await queue.enqueue({ ...draft, coalesceKey: undefined });
  store.put.mockRejectedValue(new Error("disk full"));
  const second = await queue.enqueue(draft);
  expect(store.close).toHaveBeenCalledOnce();
  for (const method of [store.allocateId, store.put, store.update, store.replacePending, store.delete]) method.mockClear();
  const third = await queue.enqueue(draft);
  expect(third.id).toBeGreaterThan(second.id);
  expect((await queue.list(draft.scope)).map(entry => entry.id)).toEqual([first.id, second.id, third.id]);
  await deliverInMemory(queue, first.id);
  await deliverInMemory(queue, second.id);
  await deliverInMemory(queue, third.id);
  for (const method of [store.allocateId, store.put, store.update, store.replacePending, store.delete]) expect(method).not.toHaveBeenCalled();
});

it("still rejects hydration failure when durable persistence is required", async () => {
  const { store } = storeFixture();
  store.load.mockRejectedValue(new Error("disk unavailable"));
  const queue = new StoreReducerOutbox({ ...store, strictPersistence: true });
  await expect(queue.enqueue(draft)).rejects.toThrow("disk unavailable");
  expect(store.close).not.toHaveBeenCalled();
});

it("never reuses IDs loaded from shared storage after a failed delete", async () => {
  const { store, rows } = storeFixture();
  const seed = await new StoreReducerOutbox(store).enqueue(draft);
  rows.clear();
  rows.set(1, { ...seed, id: 1 });
  rows.set(40, { ...seed, id: 40, idempotencyKey: "other" });
  const queue = new StoreReducerOutbox({ ...store, shared: true });
  expect(await queue.list(draft.scope)).toHaveLength(2);
  store.delete.mockRejectedValue(new Error("disk unavailable"));
  await queue.ack(40);
  store.allocateId.mockClear();
  const next = await queue.enqueue(draft);
  expect(next.id).toBeGreaterThan(40);
  expect((await queue.list(draft.scope)).map(entry => entry.id)).toEqual([1, next.id]);
  expect(store.allocateId).not.toHaveBeenCalled();
});
