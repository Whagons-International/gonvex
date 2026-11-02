import { expect, it, vi } from "vitest";
import { GonvexClient, createKvOutboxStore, createMemoryGonvexKv } from "./index.js";
import type { ReducerOutboxEntry } from "./outbox.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
const url = "ws://intent-refresh.test/ws";
const owner = (name: string) => ["identity", url, "project", "tenant", "issuer", name].join("\u0000");

it.each(["unchanged", "account", "tenant", "return", "close"] as const)(
  "intent snapshot refresh retains its invoking activation across %s", async change => {
    const base = createKvOutboxStore(createMemoryGonvexKv());
    const entry = (path: string): ReducerOutboxEntry => ({
      id: 1, scope: owner("alice"), path, args: {}, idempotencyKey: "alice-intent",
      entityKeys: [], state: "rejected", createdAt: 1, attempts: 1, nextAttemptAt: 0,
    });
    await base.put(entry("tasks.old"));
    const entered = deferred(), release = deferred();
    let hold = false;
    const client = new GonvexClient(url, {
      project: "project", tenant: "tenant", identity: { sub: "alice", iss: "issuer" },
      errorReporting: false,
      outbox: { store: { ...base, shared: true, strictPersistence: true,
        load: async scope => {
          const entries = (await base.load()).filter(row => !scope || row.scope === scope);
          if (hold && scope === owner("alice")) {
            hold = false; entered.resolve(); await release.promise;
          }
          return entries;
        },
      } },
    });
    const observed: string[][] = [];
    let unsubscribe = () => {};
    try {
      await client.listIntents();
      hold = true;
      unsubscribe = client.subscribeIntents(() => observed.push(client.intentsSnapshot().map(row => row.reducer)));
      await entered.promise;
      if (change === "account" || change === "return") {
        client.setAuth({ identity: { sub: "bob", iss: "issuer" } });
        await client.localSessionReady;
        if (change === "return") {
          await base.put(entry("tasks.latest"));
          client.setAuth({ identity: { sub: "alice", iss: "issuer" } });
          await client.localSessionReady;
        }
      } else if (change === "tenant") {
        client.setAuth({ tenant: "other" });
        await client.localSessionReady;
      } else if (change === "close") client.close();
      observed.length = 0;
      release.resolve();
      await vi.waitFor(() => expect((client as unknown as { intentsRefreshRunning: boolean }).intentsRefreshRunning).toBe(false));
      if (change === "unchanged") {
        expect(observed).toContainEqual(["tasks.old"]);
        expect(client.intentsSnapshot().map(row => row.reducer)).toEqual(["tasks.old"]);
      } else {
        // Observe every notification, including a stale value overwritten by a
        // later refresh. Final-state-only checks would miss the disclosure.
        expect(observed, "superseded intent refresh published a stale snapshot").not.toContainEqual(["tasks.old"]);
        if (change === "return") expect(client.intentsSnapshot().map(row => row.reducer)).toEqual(["tasks.latest"]);
        else expect(client.intentsSnapshot()).toEqual([]);
      }
      expect(await base.load()).toEqual([entry(change === "return" ? "tasks.latest" : "tasks.old")]);
    } finally { release.resolve(); unsubscribe(); client.close(); }
  },
);
