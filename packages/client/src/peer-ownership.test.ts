import { expect, it, vi } from "vitest";
import { GonvexClient, MemoryLocalReplicaStorage, createKvOutboxStore, createMemoryGonvexKv } from "./index.js";
import type { ReducerOutboxEntry } from "./outbox.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
const url = "ws://peer-ownership.test/ws";
const owner = (name: string) => ["identity", url, "project", "tenant", "issuer", name].join("\u0000");

it.each(["unchanged", "account", "return", "close"] as const)("peer refresh retains its invoking activation across %s", async change => {
  const storage = new MemoryLocalReplicaStorage();
  for (const name of ["alice", "bob"]) await storage.saveSession(owner(name), {
    directive: { protocolVersion: 1, scope: "tenant", visibilityScope: `visible-${name}`, epoch: "e" },
    identity: { auth: { account: { id: name } }, tenant: { id: "tenant" }, member: { id: name, accountId: name, permissions: {} } },
    artifactHash: "artifact",
  });
  const base = createKvOutboxStore(createMemoryGonvexKv());
  const entry = (title: string): ReducerOutboxEntry => ({
    id: 1, scope: owner("alice"), path: "tasks.save", args: { title }, idempotencyKey: "alice-intent",
    entityKeys: ["tasks:private"], state: "failed", createdAt: 1, attempts: 1, nextAttemptAt: 0,
    patches: [{ entity: "tasks", rowId: "private", op: "upsert", fields: { id: "private", title } }],
  });
  await base.put(entry("original"));
  const entered = deferred(), release = deferred();
  let hold = false;
  let peer = () => {};
  const client = new GonvexClient(url, {
    project: "project", tenant: "tenant", identity: { sub: "alice", iss: "issuer" }, errorReporting: false,
    localReplica: { storage }, outbox: { store: { ...base, shared: true, strictPersistence: true,
      subscribePeer: listener => { peer = listener; return () => { peer = () => {}; }; },
      load: async scope => {
        const entries = (await base.load()).filter(row => !scope || row.scope === scope);
        if (hold && scope === owner("alice")) { hold = false; entered.resolve(); await release.promise; }
        return entries;
      },
    } },
  });
  try {
    await client.localSessionReady;
    expect(client.localReplica.entity("tasks", "private")?.title).toBe("original");
    await base.put(entry("peer-old")); hold = true; peer();
    await entered.promise;
    if (change === "account" || change === "return") {
      client.setAuth({ identity: { sub: "bob", iss: "issuer" } });
      await client.localSessionReady;
      if (change === "return") {
        await base.put(entry("latest"));
        client.setAuth({ identity: { sub: "alice", iss: "issuer" } });
        await client.localSessionReady;
        expect(client.localReplica.entity("tasks", "private")?.title).toBe("latest");
      }
    } else if (change === "close") client.close();
    release.resolve();
    // Observe refresh completion, so a silent early return cannot make a
    // stale-publication assertion pass before the callback actually resumes.
    await vi.waitFor(() => expect((client as unknown as { peerRefreshScheduled: boolean }).peerRefreshScheduled).toBe(false));
    expect(client.localReplica.entity("tasks", "private")?.title).toBe(change === "return" ? "latest" : change === "unchanged" ? "peer-old" : undefined);
    expect(await base.load()).toEqual([entry(change === "return" ? "latest" : "peer-old")]);
  } finally { release.resolve(); client.close(); }
});
