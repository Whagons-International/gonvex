import { describe, expect, it, vi } from "vitest";
import { LocalReplica, MemoryLocalReplicaStorage, type LocalReplicaStorage, type ReplicaSnapshot, type ReplicaTransaction } from "./local-replica";

describe("LocalReplica", () => {
  it("publishes a multi-entity server transaction once", async () => {
    const replica = new LocalReplica();
    const listener = vi.fn();
    replica.subscribe(listener);
    await replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 1 },
      changes: [
        { entity: "tasks", id: "task-1", operation: "insert", newValue: { id: "task-1", status: "started" } },
        { entity: "taskMembers", id: "link-1", operation: "insert", newValue: { id: "link-1", taskId: "task-1", memberId: "gabriel" } },
      ],
    });
    expect(listener).toHaveBeenCalledTimes(1);
    expect(replica.entity("tasks", "task-1")).toMatchObject({ status: "started" });
    expect(replica.entity("taskMembers", "link-1")).toMatchObject({ memberId: "gabriel" });
  });

  it("updates one batched entity selector for inserts, patches, and deletes", async () => {
    const replica = new LocalReplica();
    await replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 1 },
      changes: [{ entity: "tasks", id: "a", operation: "insert", newValue: { id: "a", title: "A" } }],
    });
    expect(replica.entityBatch("tasks", ["a", "b"])).toEqual([{ id: "a", title: "A" }, undefined]);

    await replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 2 },
      changes: [
        { entity: "tasks", id: "a", operation: "update", newValue: { id: "a", title: "A2" } },
        { entity: "tasks", id: "b", operation: "insert", newValue: { id: "b", title: "B" } },
      ],
    });
    expect(replica.entityBatch("tasks", ["a", "b"])).toEqual([{ id: "a", title: "A2" }, { id: "b", title: "B" }]);

    await replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 3 },
      changes: [{ entity: "tasks", id: "a", operation: "delete" }],
    });
    expect(replica.entityBatch("tasks", ["a", "b"])).toEqual([undefined, { id: "b", title: "B" }]);
  });

  it("does not expose query membership changes before local persistence commits", async () => {
    let release!: () => void;
    const persisted = new Promise<void>((resolve) => { release = resolve; });
    const storage: LocalReplicaStorage = {
      load: async () => undefined,
      replaceSnapshot: async () => undefined,
      applyTransaction: async () => persisted,
    };
    const replica = new LocalReplica(storage);
    await replica.replaceWindow({
      signature: "tasks:grid", entity: "tasks", key: "id",
      rows: [{ id: "a" }, { id: "b" }], completeness: "complete", source: "server",
      cursor: { epoch: "tenant-a", revision: 1 },
    });
    const listener = vi.fn();
    replica.subscribe(listener);
    const application = replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 2 },
      changes: [{ entity: "tasks", id: "a", operation: "delete" }],
    });

    await Promise.resolve();
    expect(replica.liveQuery("tasks:grid").ids).toEqual(["a", "b"]);
    expect(listener).not.toHaveBeenCalled();

    release();
    await application;
    expect(replica.liveQuery("tasks:grid").ids).toEqual(["b"]);
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it("keeps optimistic state until its committed revision is applied", async () => {
    const replica = new LocalReplica();
    await replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 1 },
      changes: [{ entity: "tasks", id: "task-1", operation: "insert", newValue: { id: "task-1", status: "ready" } }],
    });
    replica.applyOptimistic("command-a", [{ entity: "tasks", rowId: "task-1", op: "patch", fields: { status: "started" } }]);
    replica.acknowledgeCommand("command-a", 2);
    expect(replica.entity("tasks", "task-1")).toMatchObject({ status: "started" });

    await replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 2 },
      originCommandId: "command-a",
      changes: [{ entity: "tasks", id: "task-1", operation: "update", newValue: { id: "task-1", status: "started" } }],
    });
    expect(replica.entity("tasks", "task-1")).toMatchObject({ status: "started" });
  });

  it("stores Live Query membership as IDs over normalized entities", async () => {
    const replica = new LocalReplica();
    await replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 7 },
      changes: [
        { entity: "tasks", id: "a", operation: "insert", newValue: { id: "a", title: "A" } },
        { entity: "tasks", id: "b", operation: "insert", newValue: { id: "b", title: "B" } },
      ],
      memberships: [{ kind: "live", signature: "tasks:recent", entity: "tasks", key: "id", ids: ["b", "a"], completeness: "partial", source: "server" }],
    });
    expect(replica.liveQuery("tasks:recent")).toMatchObject({
      rows: [{ id: "b", title: "B" }, { id: "a", title: "A" }],
      completeness: "partial",
      source: "server",
      freshness: "current",
    });
  });

  it("materializes cached query windows into the same normalized entities", async () => {
    const storage = new MemoryLocalReplicaStorage();
    const replica = new LocalReplica(storage);
    await replica.materializeWindow({
      signature: "tasks.grid:{}",
      entity: "tasks",
      key: "id",
      rows: [{ id: "b", title: "B" }, { id: "a", title: "A" }],
      completeness: "complete",
      source: "cache",
      kind: "live",
    });
    expect(replica.entity("tasks", "a")).toEqual({ id: "a", title: "A" });
    expect(replica.liveQuery("tasks.grid:{}")).toMatchObject({
      rows: [{ id: "b", title: "B" }, { id: "a", title: "A" }],
      source: "cache",
      completeness: "complete",
      freshness: "verifying",
    });
    const hydrated = new LocalReplica(storage);
    await hydrated.hydrate();
    expect(hydrated.liveQuery("tasks.grid:{}").rows).toHaveLength(2);
  });

  it("persists window metadata without duplicating row payloads", async () => {
    const storage = new MemoryLocalReplicaStorage();
    const replica = new LocalReplica(storage);
    await replica.replaceWindow({
      signature: "tasks:grid",
      kind: "replica",
      entity: "tasks",
      key: "id",
      rows: [{ id: "a", title: "A" }],
      completeness: "partial",
      source: "cache",
      cursor: { epoch: "tenant-a", revision: 4 },
      resultSkeleton: { page: [] },
      resultPath: ["page"],
      maxRows: 100,
    });
    expect(replica.getWindow("tasks:grid")).toMatchObject({
      kind: "replica",
      cursor: { epoch: "tenant-a", revision: 4 },
      resultSkeleton: { page: [] },
      resultPath: ["page"],
    });
    expect(replica.listWindows()).toHaveLength(1);
    expect(replica.snapshot().liveQueries["tasks:grid"]).toMatchObject({ kind: "replica" });
    expect(JSON.stringify(replica.snapshot().liveQueries["tasks:grid"])).not.toContain("title");
    expect(replica.entity("tasks", "a")).toEqual({ id: "a", title: "A" });
  });

  it("hydrates authoritative collection completeness and batched normalized entities", async () => {
    const storage = new MemoryLocalReplicaStorage();
    const first = new LocalReplica(storage);
    await first.replaceWindow({
      signature: "tasks:recent",
      kind: "replica",
      entity: "tasks",
      key: "id",
      rows: [{ id: "a", title: "A" }, { id: "b", title: "B" }],
      completeness: "partial",
      source: "server",
      truncated: true,
      cursor: { epoch: "tenant-a", revision: 42 },
    });

    const hydrated = new LocalReplica(storage);
    await hydrated.hydrate();

    expect(hydrated.collectionState("tasks:recent")).toMatchObject({
      rows: [{ id: "a", title: "A" }, { id: "b", title: "B" }],
      source: "cache",
      completeness: "partial",
      freshness: "verifying",
      truncated: true,
      computedRevision: 42,
    });
    expect(hydrated.entityBatch("tasks", ["b", "missing", "a"])).toEqual([
      { id: "b", title: "B" },
      undefined,
      { id: "a", title: "A" },
    ]);
  });

  it("applies a window delta and deletes revoked IDs from membership", async () => {
    const replica = new LocalReplica();
    await replica.replaceWindow({
      signature: "tasks:grid",
      kind: "live",
      entity: "tasks",
      key: "id",
      rows: [{ id: "a" }, { id: "b" }],
      completeness: "complete",
      source: "server",
      cursor: { epoch: "tenant-a", revision: 1 },
    });
    const listener = vi.fn();
    replica.subscribe(listener);
    await replica.applyWindowDelta({
      signature: "tasks:grid",
      entity: "tasks",
      key: "id",
      upserts: [{ id: "c", title: "C" }],
      deleted: ["a"],
      source: "server",
      cursor: { epoch: "tenant-a", revision: 2 },
    });
    expect(listener).toHaveBeenCalledTimes(1);
    expect(replica.windowRows("tasks:grid").map((row) => row.id)).toEqual(["b", "c"]);
    expect(replica.entity("tasks", "a")).toBeUndefined();
  });

  it("removes a window atomically while retaining shared normalized rows", async () => {
    const storage = new MemoryLocalReplicaStorage();
    const replica = new LocalReplica(storage);
    await replica.replaceWindow({
      signature: "tasks:a",
      entity: "tasks",
      key: "id",
      rows: [{ id: "shared" }],
      completeness: "complete",
      source: "server",
    });
    await replica.replaceWindow({
      signature: "tasks:b",
      entity: "tasks",
      key: "id",
      rows: [{ id: "shared" }],
      completeness: "complete",
      source: "server",
    });
    await replica.removeWindow("tasks:a");
    expect(replica.getWindow("tasks:a")).toBeUndefined();
    expect(replica.entity("tasks", "shared")).toEqual({ id: "shared" });
    expect((await storage.load())?.liveQueries["tasks:b"]).toBeDefined();
  });

  it("persists and hydrates the same authoritative snapshot", async () => {
    const storage = new MemoryLocalReplicaStorage();
    const first = new LocalReplica(storage);
    await first.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 3 },
      changes: [{ entity: "tasks", id: "task-1", operation: "insert", newValue: { id: "task-1", title: "Persisted" } }],
    });
    const second = new LocalReplica(storage);
    await second.hydrate();
    expect(second.entity("tasks", "task-1")).toMatchObject({ title: "Persisted" });
    expect(second.freshness()).toBe("verifying");
  });

  it("orders an arriving transaction after asynchronous hydration", async () => {
    let release!: (snapshot: ReplicaSnapshot) => void;
    const loaded = new Promise<ReplicaSnapshot>((resolve) => { release = resolve; });
    const storage: LocalReplicaStorage = {
      load: () => loaded,
      applyTransaction: async () => undefined,
    };
    const replica = new LocalReplica(storage);
    const hydration = replica.hydrate();
    const transaction = replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 2 },
      changes: [{ entity: "tasks", id: "task-1", operation: "update", newValue: { id: "task-1", title: "Current" } }],
    });
    release({
      cursor: { epoch: "tenant-a", revision: 1 },
      entities: { tasks: { "task-1": { id: "task-1", title: "Cached" } } },
      liveQueries: {},
    });
    await Promise.all([hydration, transaction]);
    expect(replica.entity("tasks", "task-1")).toMatchObject({ title: "Current" });
    expect(replica.cursor()?.revision).toBe(2);
  });

  it("clears stale entities when a materialized window changes epoch", async () => {
    const replica = new LocalReplica();
    await replica.applyTransaction({
      cursor: { epoch: "old", revision: 3 },
      changes: [{ entity: "tasks", id: "old-task", operation: "insert", newValue: { id: "old-task" } }],
    });
    await replica.materializeWindow({
      signature: "tasks:grid",
      entity: "tasks",
      key: "id",
      rows: [{ id: "new-task" }],
      completeness: "partial",
      source: "server",
      cursor: { epoch: "new", revision: 1 },
    });
    expect(replica.entity("tasks", "old-task")).toBeUndefined();
    expect(replica.entity("tasks", "new-task")).toEqual({ id: "new-task" });
  });

  it("allows persistence to recover after one failed transaction", async () => {
    let attempts = 0;
    const storage: LocalReplicaStorage = {
      load: async () => undefined,
      applyTransaction: async (_transaction: ReplicaTransaction) => {
        attempts += 1;
        if (attempts === 1) throw new Error("temporary storage failure");
      },
    };
    const replica = new LocalReplica(storage);
    await expect(replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 1 },
      changes: [{ entity: "tasks", id: "a", operation: "insert", newValue: { id: "a" } }],
    })).rejects.toThrow("temporary storage failure");
    await replica.applyTransaction({
      cursor: { epoch: "tenant-a", revision: 2 },
      changes: [{ entity: "tasks", id: "b", operation: "insert", newValue: { id: "b" } }],
    });
    expect(replica.entity("tasks", "b")).toEqual({ id: "b" });
  });

  it("exposes one normalized cached corpus and only claims completeness for a full Replica Collection", async () => {
    const replica = new LocalReplica();
    await replica.materializeWindow({
      signature: "tasks:grid",
      kind: "live",
      entity: "tasks",
      key: "id",
      rows: [{ id: "a", title: "Grid" }],
      completeness: "complete",
      source: "server",
    });
    expect(replica.entityRows("tasks")).toEqual([{ id: "a", title: "Grid" }]);
    expect(replica.entityCompleteness("tasks")).toBe("partial");

    await replica.materializeWindow({
      signature: "tasks:recent",
      kind: "replica",
      entity: "tasks",
      key: "id",
      rows: [{ id: "a", title: "Current" }, { id: "b", title: "Replica" }],
      completeness: "complete",
      source: "server",
      truncated: false,
    });
    expect(replica.entityRows("tasks")).toEqual([{ id: "a", title: "Current" }, { id: "b", title: "Replica" }]);
    expect(replica.entityCompleteness("tasks")).toBe("complete");
  });
});
