import { expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, createKvOutboxStore, createMemoryGonvexKv } from "./index.js";
import type { ReducerOutboxEntry } from "./outbox.js";
import type { LocalTransactionResult } from "@gonvex/local-runtime";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
const url = "ws://local-ownership.test/ws";
const owner = ["identity", url, "project", "tenant", "issuer", "alice"].join("\u0000");
const identity = { auth: { account: { id: "alice" } }, tenant: { id: "tenant" }, member: { id: "alice", accountId: "alice", permissions: {} } };
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible-alice", epoch: "e" };

it.each((["execute", "execute-error", "persist"] as const).flatMap(phase => [false, true].map(close => ({ phase, close }))))
("rebase cannot publish after close at $phase completion, close=$close", async ({ phase, close }) => {
  const storage = new MemoryLocalReplicaStorage();
  await storage.saveSession(owner, { directive, identity, artifactHash: "artifact" });
  const base = createKvOutboxStore(createMemoryGonvexKv());
  const entry: ReducerOutboxEntry = {
    id: 1, scope: owner, path: "tasks.save", args: {}, idempotencyKey: "alice-intent",
    entityKeys: ["tasks:private"], state: "rejected", createdAt: 1, attempts: 1, nextAttemptAt: 0,
    patches: [{ entity: "tasks", rowId: "private", op: "upsert", fields: { id: "private", title: "old prediction" } }],
    localExecution: { scope: "visible-alice", commandId: "alice-intent", now: 1, artifactHash: "artifact", identity },
  };
  await base.put(entry);
  const entered = deferred(), release = deferred();
  const transaction: LocalTransactionResult = {
    result: "replayed", deferred: [], readTables: [], readRows: [], scanTables: [],
    patches: [{ entity: "tasks", rowId: "private", op: "insert", fields: { id: "private", title: "replayed" } }],
  };
  const executeRead = vi.fn(async () => {
    if (phase !== "persist") { entered.resolve(); await release.promise; }
    if (phase === "execute-error") throw new Error("replay validation failed");
    return structuredClone(transaction);
  });
  const fallback = vi.fn();
  const client = new GonvexClient(url, {
    project: "project", tenant: "tenant", identity: { sub: "alice", iss: "issuer" }, errorReporting: false,
    localReplica: { storage }, onLocalExecutionFallback: fallback,
    localRuntime: { mode: "portable", artifactHash: "artifact", tables: ["tasks"], create: () => ({
      ready: Promise.resolve(), execute: vi.fn(), executeRead, replay: vi.fn(), close: vi.fn(),
    }) },
    outbox: { store: { ...base, strictPersistence: true, put: async updated => {
      await base.put(updated);
      if (phase === "persist" && updated.dependencies) { entered.resolve(); await release.promise; }
    }, update: async (id, change) => {
      const updated = await base.update!(id, change);
      if (phase === "persist" && updated?.dependencies) { entered.resolve(); await release.promise; }
      return updated;
    } } },
  });
  let outcome: Promise<boolean> | undefined;
  try {
    await client.localSessionReady;
    expect(client.localReplica.entity("tasks", "private")).toBeUndefined();
    outcome = client.retryIntent(entry.idempotencyKey);
    await entered.promise;
    if (close) client.close();
    release.resolve();
    expect(await outcome).toBe(!close);
    expect(client.localReplica.entity("tasks", "private")).toEqual(close || phase === "execute-error" ? undefined : { id: "private", title: "replayed" });
    expect(executeRead).toHaveBeenCalledTimes(1);
    expect(fallback).toHaveBeenCalledTimes(!close && phase === "execute-error" ? 1 : 0);
    const [saved] = await base.load();
    expect(saved).toMatchObject({ id: entry.id, scope: owner, idempotencyKey: entry.idempotencyKey, state: "pending" });
    // A write admitted before close may finish in Alice's journal. A completed
    // execution after close must not admit a new prediction write at all.
    expect(saved?.patches?.[0]?.fields?.title).toBe(close && phase !== "persist" ? "old prediction" : phase === "execute-error" ? undefined : "replayed");
  } finally { release.resolve(); if (outcome) await outcome; client.close(); }
});

it.each([false, true])("rebase bookkeeping survives a synchronous account-change listener, rotate=%s", async rotate => {
  const storage = new MemoryLocalReplicaStorage();
  await storage.saveSession(owner, { directive, identity, artifactHash: "artifact" });
  const bobOwner = ["identity", url, "project", "tenant", "issuer", "bob"].join("\u0000");
  await storage.saveSession(bobOwner, {
    directive: { ...directive, visibilityScope: "visible-bob" },
    identity: { ...identity, auth: { account: { id: "bob" } }, member: { ...identity.member, id: "bob", accountId: "bob" } },
    artifactHash: "artifact",
  });
  const base = createKvOutboxStore(createMemoryGonvexKv());
  await base.put({
    id: 1, scope: owner, path: "tasks.save", args: {}, idempotencyKey: "alice-intent",
    entityKeys: ["tasks:private"], state: "rejected", createdAt: 1, attempts: 1, nextAttemptAt: 0,
    patches: [], localExecution: { scope: "visible-alice", commandId: "alice-intent", now: 1, artifactHash: "artifact", identity },
  });
  const executeRead = vi.fn(async (): Promise<LocalTransactionResult> => ({
    result: "replayed", deferred: [], readTables: [], readRows: [], scanTables: [],
    patches: [{ entity: "tasks", rowId: "private", op: "insert", fields: { id: "private", title: "replayed" } }],
  }));
  const client = new GonvexClient(url, {
    project: "project", tenant: "tenant", identity: { sub: "alice", iss: "issuer" }, errorReporting: false,
    localReplica: { storage }, outbox: { store: { ...base, strictPersistence: true } },
    localRuntime: { mode: "portable", artifactHash: "artifact", tables: ["tasks"], create: () => ({
      ready: Promise.resolve(), execute: vi.fn(), executeRead, replay: vi.fn(), close: vi.fn(),
    }) },
  });
  let observed = false;
  let afterNotification: Array<[string, number]> | undefined;
  let unsubscribe = () => {};
  try {
    await client.localSessionReady;
    unsubscribe = client.localReplica.subscribe(() => {
      if (observed || client.localReplica.entity("tasks", "private")?.title !== "replayed") return;
      observed = true;
      if (rotate) client.setAuth({ identity: { sub: "bob", iss: "issuer" } });
      queueMicrotask(() => {
        afterNotification = [...(client as unknown as { optimisticOutboxEntryIds: Map<string, number> }).optimisticOutboxEntryIds];
      });
    });
    expect(await client.retryIntent("alice-intent")).toBe(!rotate);
    await client.localSessionReady;
    expect(observed).toBe(true);
    expect(afterNotification).toEqual(rotate ? [] : [["alice-intent", 1]]);
    expect(client.localReplica.hasPendingCommand("alice-intent")).toBe(!rotate);
    expect(await client.listIntents()).toHaveLength(rotate ? 0 : 1);
    // Inspect the live coordination state too: a leaked Alice mapping can be
    // used by later delivery even though Bob's journal projection is empty.
    const bookkeeping = client as unknown as {
      optimisticOutboxEntryIds: Map<string, number>; optimisticReducerIds: Set<string>;
    };
    expect([...bookkeeping.optimisticOutboxEntryIds]).toEqual(rotate ? [] : [["alice-intent", 1]]);
    expect([...bookkeeping.optimisticReducerIds]).toEqual(rotate ? [] : ["alice-intent"]);
    expect(await base.load()).toMatchObject([{ id: 1, scope: owner, state: "pending" }]);
  } finally { unsubscribe(); client.close(); }
});
