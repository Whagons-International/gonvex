import { expect, it } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, createKvOutboxStore, createMemoryGonvexKv } from "./index.js";
import type { ReducerOutboxEntry } from "./outbox.js";

const url = "ws://ownership.test/ws";
const owner = ["identity", url, "project", "tenant", "issuer", "alice"].join("\u0000");
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

it.each(["account", "tenant", "account-return", "close", "unchanged"] as const)("fences retry publication across %s while its durable write is pending", async change => {
  const base = createKvOutboxStore(createMemoryGonvexKv());
  const entry: ReducerOutboxEntry = {
    id: 1, scope: owner, path: "tasks.save", args: { secret: "Alice's draft" },
    idempotencyKey: "alice-intent", entityKeys: ["tasks:private"],
    patches: [{ entity: "tasks", rowId: "private", op: "upsert", fields: { id: "private", title: "Alice's draft" } }],
    state: "rejected", createdAt: 1, attempts: 1, nextAttemptAt: 0, lastError: "denied",
  };
  await base.put(entry);
  const entered = deferred(), release = deferred();
  const client = new GonvexClient(url, {
    project: "project", tenant: "tenant", identity: { sub: "alice", iss: "issuer" },
    errorReporting: false,
    outbox: { store: { ...base, put: async updated => {
      await base.put(updated);
      if (updated.id === entry.id && updated.state === "pending") {
        entered.resolve();
        await release.promise;
      }
    }, update: async (id, change) => {
      const updated = await base.update!(id, change);
      if (updated?.id === entry.id && updated.state === "pending") {
        entered.resolve();
        await release.promise;
      }
      return updated;
    } } },
  });
  try {
    expect(await client.listIntents()).toEqual([expect.objectContaining({ id: entry.idempotencyKey, state: "rejected" })]);
    const retry = client.retryIntent(entry.idempotencyKey);
    // Observe rejection immediately too, so close cannot create an unhandled promise.
    const outcome = retry.then(value => ({ value }), error => ({ error }));
    await entered.promise;
    if (change === "account") client.setAuth({ identity: { sub: "bob", iss: "issuer" } });
    else if (change === "account-return") {
      client.setAuth({ identity: { sub: "bob", iss: "issuer" } });
      client.setAuth({ identity: { sub: "alice", iss: "issuer" } });
    }
    else if (change === "tenant") client.setAuth({ tenant: "other-tenant" });
    else if (change === "close") client.close();
    if (change !== "close") {
      await client.listIntents();
      expect(client.localReplica.entity("tasks", "private")).toBeUndefined();
    }
    release.resolve();
    const result = await outcome;
    if (change !== "close") {
      if (change === "unchanged") {
        expect(client.localReplica.entity("tasks", "private")).toEqual({ id: "private", title: "Alice's draft" });
      } else {
        expect(client.localReplica.entity("tasks", "private")).toBeUndefined();
      }
      if (change === "account" || change === "tenant") expect(await client.listIntents()).toEqual([]);
    }
    expect(result).toEqual({ value: change === "unchanged" });
    // Completion of Alice's write stays in Alice's journal, with its original identity.
    expect(await base.load()).toEqual([expect.objectContaining({
      id: entry.id, scope: owner, idempotencyKey: entry.idempotencyKey, args: entry.args, state: "pending",
    })]);
  } finally { release.resolve(); client.close(); }
});

it.each((["local", "remote"] as const).flatMap(kind => [false, true].map(rotate => ({ kind, rotate }))))("restore fences publication after $kind committed-entry cleanup, rotate=$rotate", async ({ kind, rotate }) => {
  const base = createKvOutboxStore(createMemoryGonvexKv());
  const storage = new MemoryLocalReplicaStorage();
  const identities = Object.fromEntries(["alice", "bob"].map(name => [name, {
    auth: { account: { id: name } }, tenant: { id: "tenant" }, member: { id: name, accountId: name, permissions: {} },
  }]));
  for (const [index, name] of ["alice", "bob"].entries()) {
    const scope = ["identity", url, "project", "tenant", "issuer", name].join("\u0000");
    await storage.saveSession(scope, {
      directive: { protocolVersion: 1, scope: "tenant", visibilityScope: `restore-${name}`, epoch: "e" },
      identity: identities[name]!, artifactHash: "artifact",
    });
    await base.put({
      id: index + 2, scope, path: "tasks.save", args: { name }, idempotencyKey: "restored-intent",
      entityKeys: ["tasks:restored"], state: "pending", createdAt: 1, attempts: 0, nextAttemptAt: 0,
      patches: [{ entity: "tasks", rowId: "restored", op: "upsert", fields: { id: "restored", title: name } }],
    });
  }
  await base.put({
    id: 1, scope: owner, path: "tasks.save", args: {}, idempotencyKey: "already-accepted",
    entityKeys: [], state: "committed", patches: [], createdAt: 0, attempts: 0, nextAttemptAt: 0,
    ...(kind === "local" ? { localExecution: {
      scope: "restore-alice", commandId: "already-accepted", now: 0, artifactHash: "artifact", identity: identities.alice!,
    } } : {}),
  });
  const entered = deferred(), release = deferred();
  const client = new GonvexClient(url, {
    project: "project", tenant: "tenant", identity: { sub: "alice", iss: "issuer" },
    errorReporting: false, localReplica: { storage },
    outbox: { store: { ...base, delete: async id => {
      await base.delete(id);
      if (id === 1) { entered.resolve(); await release.promise; }
    } } },
  });
  const oldRestore = client.localSessionReady;
  try {
    await entered.promise;
    if (rotate) {
      client.setAuth({ identity: { sub: "bob", iss: "issuer" } });
      await client.localSessionReady;
      expect(client.localReplica.entity("tasks", "restored")).toEqual({ id: "restored", title: "bob" });
    }
    release.resolve();
    await oldRestore;
    expect(client.localReplica.entity("tasks", "restored")).toEqual({ id: "restored", title: rotate ? "bob" : "alice" });
    expect((await base.load()).map(entry => [entry.id, entry.state])).toEqual([[2, "pending"], [3, "pending"]]);
  } finally { release.resolve(); client.close(); await oldRestore; }
});

it.each([false, true])("discard cannot remove a new owner's prediction with the same intent key, rotate=%s", async rotate => {
  const base = createKvOutboxStore(createMemoryGonvexKv());
  const storage = new MemoryLocalReplicaStorage();
  const key = "same-intent-key";
  for (const [index, name] of ["alice", "bob"].entries()) {
    const scope = ["identity", url, "project", "tenant", "issuer", name].join("\u0000");
    await storage.saveSession(scope, {
      directive: { protocolVersion: 1, scope: "tenant", visibilityScope: `visible-${name}`, epoch: "e" },
      identity: { auth: { account: { id: name } }, tenant: { id: "tenant" }, member: { id: name, accountId: name, permissions: {} } },
      artifactHash: "artifact",
    });
    await base.put({
      id: index + 1, scope, path: "tasks.save", args: { name }, idempotencyKey: key,
      entityKeys: ["tasks:shared"], state: "pending", createdAt: 1, attempts: 0, nextAttemptAt: 0,
      patches: [{ entity: "tasks", rowId: "shared", op: "upsert", fields: { id: "shared", title: name } }],
    });
  }
  const entered = deferred(), release = deferred();
  const client = new GonvexClient(url, {
    project: "project", tenant: "tenant", identity: { sub: "alice", iss: "issuer" },
    errorReporting: false, localReplica: { storage },
    outbox: { store: { ...base, delete: async id => {
      await base.delete(id);
      if (id === 1) { entered.resolve(); await release.promise; }
    } } },
  });
  try {
    await client.localSessionReady;
    expect(client.localReplica.entity("tasks", "shared")).toEqual({ id: "shared", title: "alice" });
    const discard = client.discardIntent(key);
    await entered.promise;
    if (rotate) {
      client.setAuth({ identity: { sub: "bob", iss: "issuer" } });
      await client.localSessionReady;
      expect(client.localReplica.entity("tasks", "shared")).toEqual({ id: "shared", title: "bob" });
    }
    release.resolve();
    const result = await discard;
    expect(client.localReplica.entity("tasks", "shared")).toEqual(rotate ? { id: "shared", title: "bob" } : undefined);
    expect(result).toBe(!rotate);
    const rows = await base.load();
    expect(rows).toEqual([expect.objectContaining({ id: 2, idempotencyKey: key, args: { name: "bob" }, state: "pending" })]);
  } finally { release.resolve(); client.close(); }
});
