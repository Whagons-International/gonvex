import { reserveKvOutboxId, withKvOutboxOperation } from "./kv-sequence.js";
import { OutboxRecoveryError, validateOutboxEntry } from "./outbox-recovery.js";
import type { ReducerOutboxEntry, OutboxStore } from "./outbox.js";

/** Minimal host persistence boundary used by the reducer outbox and adapters. */
export type GonvexKv = {
  get(table: string, key: string): Promise<string | undefined>;
  set(table: string, key: string, value: string): Promise<void>;
  delete(table: string, key: string): Promise<void>;
  list(table: string): Promise<Array<{ key: string; value: string }>>;
  clear(table: string, keyPrefix?: string): Promise<void>;
};

export const kvReducerOutboxTable = "reducer_outbox";

export function createMemoryGonvexKv(): GonvexKv {
  const tables = new Map<string, Map<string, string>>();
  const table = (name: string) => {
    let rows = tables.get(name);
    if (!rows) { rows = new Map(); tables.set(name, rows); }
    return rows;
  };
  return {
    async get(name, key) { return table(name).get(key); },
    async set(name, key, value) { table(name).set(key, value); },
    async delete(name, key) { table(name).delete(key); },
    async list(name) { return [...table(name)].map(([key, value]) => ({ key, value })); },
    async clear(name, prefix) {
      const rows = table(name);
      if (!prefix) { rows.clear(); return; }
      for (const key of rows.keys()) if (key.startsWith(prefix)) rows.delete(key);
    },
  };
}

function parseKvIntent(raw: string, key: string): ReducerOutboxEntry {
  let entry: unknown;
  try { entry = JSON.parse(raw); }
  catch (cause) { throw new OutboxRecoveryError("Invalid durable outbox JSON; journal recovery is required", { cause }); }
  validateOutboxEntry(entry, key);
  return entry;
}

export function createKvOutboxStore(kv: GonvexKv): OutboxStore {
  const load = async () => (await kv.list(kvReducerOutboxTable))
    .map(({ key, value }) => parseKvIntent(value, key))
    .sort((left, right) => left.id - right.id);
  return {
    strictPersistence: true,
    allocateId: () => reserveKvOutboxId(kv),
    load: () => withKvOutboxOperation(kv, load),
    put: entry => withKvOutboxOperation(kv, async () => {
      validateOutboxEntry(entry, String(entry.id));
      await kv.set(kvReducerOutboxTable, String(entry.id), JSON.stringify(entry));
    }),
    update: (id, change) => withKvOutboxOperation(kv, async () => {
      const key = String(id), raw = await kv.get(kvReducerOutboxTable, key);
      if (raw === undefined) return undefined;
      const prior = parseKvIntent(raw, key);
      const scope = prior.scope, intentKey = prior.idempotencyKey;
      const next = change(prior);
      validateOutboxEntry(next, key);
      if (next.scope !== scope || next.idempotencyKey !== intentKey)
        throw new OutboxRecoveryError("Durable outbox identity cannot change");
      const value = JSON.stringify(next);
      if (value !== raw) await kv.set(kvReducerOutboxTable, key, value);
      return next;
    }),
    delete: id => withKvOutboxOperation(kv, async () => {
      const key = String(id), raw = await kv.get(kvReducerOutboxTable, key);
      if (raw === undefined) return;
      parseKvIntent(raw, key);
      await kv.delete(kvReducerOutboxTable, key);
    }),
    clear: scope => withKvOutboxOperation(kv, async () => {
      // Validate all rows before deleting any of an explicitly selected scope.
      const entries = await load();
      for (const entry of entries) if (scope === undefined || entry.scope === scope)
        await kv.delete(kvReducerOutboxTable, String(entry.id));
    }),
  };
}
