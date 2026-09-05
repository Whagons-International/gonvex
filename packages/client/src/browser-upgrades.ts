import { Dexie, type Table } from "dexie";
import { IndexedDBLocalReplicaStorage } from "./indexeddb-replica.js";
import { migrateClientData, migrationChain, type ClientContract, type ClientMigration } from "./client-upgrades.js";
import type { LocalReplicaStorage, ReplicaSnapshot } from "./local-replica.js";
import type { OutboxStore, ReducerOutboxEntry } from "./outbox.js";

type UpgradeRecord = { key: string; version: number; journal?: {
  snapshots: Record<string, ReplicaSnapshot>; entries: ReducerOutboxEntry[];
} };
export type BrowserUpgradeOptions = {
  replicaName: string;
  outboxName: string;
  contract: ClientContract;
  /** Version assigned to data written before this mechanism was introduced. */
  initialVersion: number;
  migrations: readonly ClientMigration[];
  onRequired?: (error: Error) => void;
};

/** All app access goes through version-fenced SDK stores, including old tabs. */
export function browserUpgradeStorage(options: BrowserUpgradeOptions): {
  storage: LocalReplicaStorage; store: OutboxStore; ready: Promise<void>; close(): void;
} {
  const replica = new IndexedDBLocalReplicaStorage(options.replicaName);
  const queue = new Dexie(options.outboxName) as Dexie & { entries: Table<ReducerOutboxEntry, number> };
  queue.version(1).stores({ entries: "++id, state, nextAttemptAt" });
  queue.version(2).stores({ entries: "++id, scope, state, nextAttemptAt, [scope+state], [scope+nextAttemptAt]" });
  const meta = new Dexie(`${options.replicaName}-upgrades`) as Dexie & { state: Table<UpgradeRecord, string> };
  meta.version(1).stores({ state: "&key" });
  const lockName = `gonvex-upgrade:${options.replicaName}:${options.outboxName}`;
  const locks = globalThis.navigator?.locks;
  const channel = typeof BroadcastChannel === "function" ? new BroadcastChannel(lockName) : undefined;
  let closed = false;
  const requireUpgrade = (error: Error) => { options.onRequired?.(error); return error; };
  if (channel) channel.onmessage = event => {
    if (event.data?.version !== options.contract.version) requireUpgrade(new Error("Another tab upgraded the application. Reload to continue."));
  };
  const ready = (async () => {
    if (!locks) throw new Error("Safe offline upgrades require browser Web Locks support");
    await locks.request(lockName, { mode: "exclusive" }, async () => {
      await Promise.all([meta.open(), queue.open()]);
      let record = await meta.state.get("contract") ?? { key: "contract", version: options.initialVersion };
      // A interrupted upgrade must finish its exact staged payload before another upgrade starts.
      const finish = async () => {
        if (!record.journal) return;
        for (const [scope, snapshot] of Object.entries(record.journal.snapshots)) await replica.replaceSnapshot(snapshot, scope);
        await queue.transaction("rw", queue.entries, async () => {
          await queue.entries.clear();
          await queue.entries.bulkPut(record.journal!.entries);
        });
        record = { key: "contract", version: record.version };
        await meta.state.put(record);
      };
      // An older tab must never finish a newer migration and then hydrate it.
      if (record.version > options.contract.version) throw new Error("Stored data requires a newer application build");
      await finish();
      const chain = migrationChain(record.version, options.contract.version, options.migrations);
      if (chain.length) {
        const snapshots: Record<string, ReplicaSnapshot> = {};
        for (const scope of await replica.listScopes()) {
          const snapshot = await replica.load(scope);
          if (snapshot) snapshots[scope] = snapshot;
        }
        const entries = await queue.entries.toArray();
        if (entries.some(entry => !entry.scope)) throw new Error("Unowned offline work needs recovery before upgrading");
        // No writes until every transform succeeds. The journal is temporary upgrade data,
        // never a second source of application state. Applying it is repeatable after a crash.
        const journal = migrateClientData(snapshots, entries, chain);
        record = { key: "contract", version: options.contract.version, journal };
        await meta.state.put(record);
        await finish();
        channel?.postMessage({ version: record.version });
      } else await meta.state.put(record);
    });
  })().catch(error => { throw requireUpgrade(error instanceof Error ? error : new Error(String(error))); });
  // The SDK may initialize storage before React subscribes; retain the rejected ready promise.
  void ready.catch(() => undefined);
  const guarded = async <T>(run: () => Promise<T>): Promise<T> => {
    await ready;
    if (closed) throw new Error("Application storage is closed");
    return locks!.request(lockName, { mode: "shared" }, async () => {
      const record = await meta.state.get("contract");
      if (record?.version !== options.contract.version || record.journal) {
        throw requireUpgrade(new Error("Application storage changed. Reload before continuing."));
      }
      return run();
    });
  };
  const storage = new Proxy(replica, {
    get(target, property) {
      const value = Reflect.get(target, property);
      return typeof value === "function" ? (...args: unknown[]) => guarded(() => value.apply(target, args)) : value;
    },
  });
  return {
    storage, ready,
    store: {
      strictPersistence: true,
      allocateId: () => guarded(async () => {
        const last = await queue.entries.orderBy('id').last();
        return meta.transaction('rw', meta.state, async () => {
          const counter = await meta.state.get('sequence');
          const version = Math.max(counter?.version ?? 0, last?.id ?? 0) + 1;
          await meta.state.put({ key: 'sequence', version });
          return version;
        });
      }),
      load: () => guarded(() => queue.entries.toArray()),
      put: entry => guarded(async () => { await queue.entries.put(entry); }),
      delete: id => guarded(() => queue.entries.delete(id)),
      clear: scope => guarded(() => scope ? queue.entries.where("scope").equals(scope).delete().then(() => undefined) : queue.entries.clear()),
    },
    close() { closed = true; channel?.close(); replica.close(); queue.close(); meta.close(); },
  };
}
