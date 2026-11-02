import { expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, createKvOutboxStore, createMemoryGonvexKv } from "./index.js";
import type { LocalTransactionResult } from "@gonvex/local-runtime";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
const url = "ws://admission-ownership.test/ws";
const owner = ["identity", url, "project", "tenant", "issuer", "alice"].join("\u0000");
const identity = { auth: { account: { id: "alice" } }, tenant: { id: "tenant" }, member: { id: "alice", accountId: "alice", permissions: {} } };
const directive = { protocolVersion: 1 as const, scope: "tenant", visibilityScope: "visible-alice", epoch: "e" };

it.each((["prepare", "execute", "persist"] as const).flatMap(phase => [false, true].map(close => ({ phase, close }))))
("local admission fences $phase completion, close=$close", async ({ phase, close }) => {
  const storage = new MemoryLocalReplicaStorage();
  await storage.saveSession(owner, { directive, identity, artifactHash: "artifact" });
  const base = createKvOutboxStore(createMemoryGonvexKv());
  const entered = deferred(), release = deferred();
  const transaction: LocalTransactionResult = {
    result: "created", deferred: [], readTables: [], readRows: [], scanTables: [],
    patches: [{ entity: "tasks", rowId: "private", op: "insert", fields: { id: "private", title: "alice" } }],
  };
  const execute = vi.fn(async () => {
    if (phase !== "persist") { entered.resolve(); await release.promise; }
    return structuredClone(transaction);
  });
  const client = new GonvexClient(url, {
    project: "project", tenant: "tenant", identity: { sub: "alice", iss: "issuer" }, errorReporting: false,
    localReplica: { storage },
    localRuntime: { ...(phase === "prepare" ? { mode: "portable" as const } : {}), artifactHash: "artifact", tables: ["tasks"], create: () => ({
      ready: Promise.resolve(), execute, executeRead: execute, replay: vi.fn(), close: vi.fn(),
    }) },
    outbox: { store: { ...base, strictPersistence: true, put: async entry => {
      await base.put(entry);
      if (phase === "persist") { entered.resolve(); await release.promise; }
    } } },
  });
  let outcome: Promise<{ value: unknown } | { error: unknown }> | undefined;
  try {
    await client.localSessionReady;
    outcome = client.reducer({ kind: "reducer", path: "tasks.save", localExecution: 1, offline: { mode: "allowed" } }, {})
      .then(value => ({ value }), error => ({ error }));
    await entered.promise;
    if (close) client.close();
    release.resolve();
    const result = await outcome;
    expect(result).toMatchObject(close ? { error: { code: "superseded" } } : { value: "created" });
    const rows = await base.load();
    expect(rows).toHaveLength(close && phase !== "persist" ? 0 : 1);
    if (rows.length) expect(rows).toMatchObject([{ scope: owner, state: "pending", args: {} }]);
    expect(client.localReplica.entity("tasks", "private")).toEqual(close ? undefined : { id: "private", title: "alice" });
  } finally { release.resolve(); if (outcome) await outcome; client.close(); }
});
