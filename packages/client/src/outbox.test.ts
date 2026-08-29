import { describe, expect, it, vi } from "vitest";
import { IDBKeyRange, indexedDB } from "fake-indexeddb";
import { DexieReducerOutbox, createReducerOutbox } from "./outbox";

const scope = "project-a\u0000tenant-a\u0000user-a";

function createOutbox(testName: string) {
  return new DexieReducerOutbox({
    databaseName: `gonvex-outbox-test-${testName}-${crypto.randomUUID()}`,
    indexedDB,
    IDBKeyRange,
  });
}

describe("DexieReducerOutbox", () => {
  it("enqueues and loads entries in id order", async () => {
    const outbox = createOutbox("ordering");
    const first = await outbox.enqueue({
      scope,
      path: "tasks.create",
      args: { title: "First" },
      idempotencyKey: "reducer-first",
      entityKeys: ["task:first"],
      patches: [{
        collection: "tasks.list",
        rowId: "first",
        op: "patch",
        fields: { title: "Optimistic" },
      }],
    });
    const second = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { title: "Second" },
      entityKeys: ["task:second"],
    });

    expect(first.id).toBeLessThan(second.id);
    await expect(outbox.loadAll(scope)).resolves.toMatchObject([
      {
        id: first.id,
        path: "tasks.create",
        idempotencyKey: "reducer-first",
        patches: [{ rowId: "first", fields: { title: "Optimistic" } }],
      },
      { id: second.id, path: "tasks.update", state: "pending" },
    ]);
    expect(second.idempotencyKey).toEqual(expect.any(String));
  });

  it("recovers inflight entries as pending when loaded", async () => {
    const outbox = createOutbox("recovery");
    const entry = await outbox.enqueue({ scope, path: "tasks.update", args: {} });
    await outbox.markInflight(entry.id);

    await expect(outbox.loadAll(scope)).resolves.toMatchObject([
      { id: entry.id, state: "pending" },
    ]);
    await expect(outbox.loadAll(scope)).resolves.toMatchObject([
      { id: entry.id, state: "pending" },
    ]);
  });

  it("blocks later writes to the same entity but allows independent writes", async () => {
    const outbox = createOutbox("causal-ordering");
    const first = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 1 },
      entityKeys: ["task:a"],
    });
    const blocked = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 2 },
      entityKeys: ["task:a"],
    });
    const independent = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 3 },
      entityKeys: ["task:b"],
    });

    await outbox.markInflight(first.id);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: independent.id });
    await outbox.ack(first.id);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: blocked.id });
  });

  it("does not let an accepted committed row block a newer write", async () => {
    const outbox = createOutbox("committed-ordering");
    const committed = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 1 },
      entityKeys: ["task:a"],
    });
    const pending = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 2 },
      entityKeys: ["task:a"],
    });
    await outbox.markCommitted(committed.id);

    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: pending.id });
  });

  it("removes acknowledged entries", async () => {
    const outbox = createOutbox("ack");
    const first = await outbox.enqueue({ scope, path: "tasks.create", args: { id: "a" } });
    await outbox.enqueue({ scope, path: "tasks.create", args: { id: "b" } });

    await outbox.ack(first.id);

    await expect(outbox.count(scope)).resolves.toBe(1);
    await expect(outbox.loadAll(scope)).resolves.not.toContainEqual(expect.objectContaining({ id: first.id }));
  });

  it("never restores or drains another authenticated identity's reducers", async () => {
    const outbox = createOutbox("scope-isolation");
    const otherScope = "project-a\u0000tenant-a\u0000user-b";
    const mine = await outbox.enqueue({ scope, path: "tasks.update", args: { priority: 1 } });
    const theirs = await outbox.enqueue({
      scope: otherScope,
      path: "tasks.update",
      args: { priority: 2 },
    });

    await expect(outbox.loadAll(scope)).resolves.toMatchObject([{ id: mine.id }]);
    await expect(outbox.loadAll(otherScope)).resolves.toMatchObject([{ id: theirs.id }]);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: mine.id });
    await expect(outbox.count(scope)).resolves.toBe(1);
    await expect(outbox.count(otherScope)).resolves.toBe(1);
  });

  it("keeps other scopes in the memory fallback mirror after a scoped load", async () => {
    const outbox = createOutbox("scope-memory-mirror");
    const otherScope = "project-a\u0000tenant-a\u0000user-b";
    await outbox.enqueue({ scope, path: "tasks.update", args: { priority: 1 } });
    await outbox.enqueue({ scope: otherScope, path: "tasks.update", args: { priority: 2 } });

    await outbox.loadAll(scope);
    await outbox.loadAll(otherScope);

    expect((outbox as unknown as { memoryEntries: Map<number, unknown> }).memoryEntries.size).toBe(2);
  });

  it("clears only the requested scope", async () => {
    const outbox = createOutbox("scope-clear");
    const otherScope = "project-a\u0000tenant-a\u0000user-b";
    await outbox.enqueue({ scope, path: "tasks.update", args: {} });
    await outbox.enqueue({ scope: otherScope, path: "tasks.update", args: {} });

    await outbox.clear(scope);

    await expect(outbox.count(scope)).resolves.toBe(0);
    await expect(outbox.count(otherScope)).resolves.toBe(1);
  });

  it("never resurrects an acknowledged row racing markCommitted", async () => {
    const outbox = createOutbox("commit-ack-race");
    const entry = await outbox.enqueue({ scope, path: "tasks.update", args: {} });

    await Promise.all([outbox.markCommitted(entry.id), outbox.ack(entry.id)]);

    await expect(outbox.count(scope)).resolves.toBe(0);
  });

  it("returns an admitted inflight row to pending without backoff", async () => {
    const outbox = createOutbox("admitted-pending");
    const entry = await outbox.enqueue({ scope: "scope", path: "tasks.update", args: {}, state: "inflight" });
    await outbox.markPending(entry.id);

    const pending = await outbox.nextReady("scope", Date.now());
    expect(pending).toMatchObject({
      id: entry.id,
      state: "pending",
      attempts: 0,
      lastError: undefined,
    });
    expect(pending?.nextAttemptAt).toBeLessThanOrEqual(Date.now());
  });

  it("does not delete an entry enqueued after scoped clearing starts", async () => {
    const outbox = createOutbox("clear-enqueue-race");
    const oldEntry = await outbox.enqueue({ scope, path: "tasks.update", args: { value: 1 } });

    const clearing = outbox.clear(scope);
    const nextEntry = await outbox.enqueue({ scope, path: "tasks.update", args: { value: 2 } });
    await clearing;

    await expect(outbox.loadAll(scope)).resolves.toMatchObject([{ id: nextEntry.id }]);
    await expect(outbox.loadAll(scope)).resolves.not.toContainEqual(expect.objectContaining({ id: oldEntry.id }));
  });

  it("backs off failures exponentially and does not return them before they are ready", async () => {
    const now = vi.spyOn(Date, "now").mockReturnValue(10_000);
    const outbox = createOutbox("backoff");
    const entry = await outbox.enqueue({ scope, path: "tasks.update", args: {} });

    await outbox.fail(entry.id, "offline");
    await expect(outbox.nextReady(scope, 11_999)).resolves.toBeUndefined();
    await expect(outbox.nextReady(scope, 12_000)).resolves.toMatchObject({
      id: entry.id,
      attempts: 1,
      nextAttemptAt: 12_000,
      lastError: "offline",
      state: "pending",
    });

    now.mockReturnValue(12_000);
    await outbox.fail(entry.id, "still offline");
    await expect(outbox.nextReady(scope, 15_999)).resolves.toBeUndefined();
    await expect(outbox.nextReady(scope, 16_000)).resolves.toMatchObject({
      attempts: 2,
      nextAttemptAt: 16_000,
      lastError: "still offline",
    });
    now.mockRestore();
  });

  it("notifies subscribers for each reducer and supports unsubscribe", async () => {
    const outbox = createOutbox("subscribe");
    const listener = vi.fn();
    const unsubscribe = outbox.subscribe(listener);
    const entry = await outbox.enqueue({ scope, path: "tasks.update", args: {} });
    await outbox.markInflight(entry.id);
    await outbox.fail(entry.id, "retry");
    await outbox.ack(entry.id);

    expect(listener).toHaveBeenCalledTimes(4);
    unsubscribe();
    await outbox.enqueue({ scope, path: "tasks.update", args: {} });
    expect(listener).toHaveBeenCalledTimes(4);
  });

  it("keeps session-only queue semantics when persistence is disabled", async () => {
    const outbox = createReducerOutbox({ enabled: false });
    const first = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 1 },
      idempotencyKey: "memory-first",
      entityKeys: ["task:a"],
    });
    const second = await outbox.enqueue({
      scope,
      path: "tasks.update",
      args: { value: 2 },
      entityKeys: ["task:a"],
    });

    await expect(outbox.count(scope)).resolves.toBe(2);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: first.id });
    await outbox.markInflight(first.id);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toBeUndefined();
    await outbox.ack(first.id);
    await expect(outbox.nextReady(scope, Date.now())).resolves.toMatchObject({ id: second.id });
    await expect(outbox.loadAll(scope)).resolves.toHaveLength(1);
  });
});
