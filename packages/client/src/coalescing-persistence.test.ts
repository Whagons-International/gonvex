import { Dexie } from "dexie";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { DexieReducerOutbox } from "./outbox.js";

it.each(["markInflight", "markCommitted"] as const)("refuses %s when its durable transition fails", async operation => {
  const indexedDB = new IDBFactory();
  const name = `coalescing-failure-${crypto.randomUUID()}`;
  const queue = new DexieReducerOutbox({ databaseName: name, indexedDB, IDBKeyRange });
  const draft = { scope: "owner", path: "tasks.set", args: { value: 1 }, idempotencyKey: "original", entityKeys: ["tasks:a"],
    coalesceKey: "a", dependencies: { reads: [], writes: [{ entity: "tasks", rowId: "a" }], scans: [] } };
  let database: Dexie | undefined;
  const fail = () => { throw new Error("injected durable transition failure"); };
  try {
    const first = await queue.enqueue(draft);
    database = (queue as unknown as { database?: Dexie }).database!;
    database.table("entries").hook("updating", fail);
    await expect(queue[operation](first.id)).rejects.toThrow(/recovery|durable|transition/i);
    database.table("entries").hook("updating").unsubscribe(fail);
    // A failed transaction cannot publish a volatile delivery or acceptance.
    expect(await queue.list(draft.scope)).toMatchObject([{ id: first.id, state: "pending", idempotencyKey: "original", knownUnsent: true }]);
    await expect(queue.markInflight(first.id)).resolves.toBe(true);
    await queue.markPending(first.id);
    const later = await queue.enqueue({ ...draft, args: { value: 2 }, idempotencyKey: "later" });
    expect(later.id).not.toBe(first.id);
    expect(await queue.list(draft.scope)).toMatchObject([
      { id: first.id, idempotencyKey: "original", args: { value: 1 }, knownUnsent: false },
      { id: later.id, idempotencyKey: "later", args: { value: 2 } },
    ]);
  } finally {
    database?.table("entries").hook("updating").unsubscribe(fail);
    database?.close();
    await new Dexie(name, { indexedDB, IDBKeyRange }).delete();
  }
});
