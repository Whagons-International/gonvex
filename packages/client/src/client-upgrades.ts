import type { JsonValue } from "@gonvex/protocol";
import type { ReplicaSnapshot } from "./local-replica.js";
import type { ReducerOutboxEntry } from "./outbox.js";

export type ClientContract = { version: number; offlineMaxAgeMs: number };
export type ClientMigration = {
  from: number;
  to: number;
  /** Pure synchronous transformation of confirmed rows. Never optimistic patches. */
  replica?: (snapshot: ReplicaSnapshot) => ReplicaSnapshot;
  /** Throw if the user's meaning cannot be preserved. The original queue survives. */
  intent?: (intent: { path: string; args: JsonValue }) => { path: string; args: JsonValue };
};

export function migrationChain(from: number, to: number, migrations: readonly ClientMigration[]): ClientMigration[] {
  if (!Number.isSafeInteger(from) || from < 1 || !Number.isSafeInteger(to) || to < from) {
    throw new Error(`Unsupported client contract transition ${from} -> ${to}`);
  }
  const chain: ClientMigration[] = [];
  for (let version = from; version < to; version++) {
    const candidates = migrations.filter(m => m.from === version);
    if (candidates.length !== 1 || candidates[0]!.to !== version + 1) {
      throw new Error(`Missing or ambiguous client migration ${version} -> ${version + 1}`);
    }
    chain.push(candidates[0]!);
  }
  return chain;
}

export function migrateClientData(
  snapshots: Record<string, ReplicaSnapshot>, entries: readonly ReducerOutboxEntry[],
  chain: readonly ClientMigration[],
): { snapshots: Record<string, ReplicaSnapshot>; entries: ReducerOutboxEntry[] } {
  let nextSnapshots = structuredClone(snapshots);
  let nextEntries = structuredClone([...entries]);
  for (const migration of chain) {
    nextSnapshots = Object.fromEntries(Object.entries(nextSnapshots).map(([scope, snapshot]) => {
      const next = migration.replica ? migration.replica(snapshot) : snapshot;
      // Shape migration does not establish server freshness or preserve query membership contracts.
      delete next.cursor;
      next.liveQueries = {};
      return [scope, next];
    }));
    nextEntries = nextEntries.map(entry => {
      const originalPath = entry.receiptPath ?? entry.path;
      const intent = migration.intent?.({ path: entry.path, args: structuredClone(entry.args) as JsonValue })
        ?? { path: entry.path, args: entry.args as JsonValue };
      if (!intent.path || intent.args === undefined) throw new Error("Client migration returned an invalid intent");
      return { ...entry, path: intent.path, args: intent.args,
        receiptPath: originalPath, patches: [],
        // Committed responses awaiting a watermark must be reconciled by their original receipt too.
        state: "pending", nextAttemptAt: 0 };
    });
  }
  return { snapshots: nextSnapshots, entries: nextEntries };
}
