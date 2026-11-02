import { OutboxRecoveryError, validateOutboxEntry } from "./outbox-recovery.js";
import type { GonvexKv } from "./kv-stores.js";

export const kvReducerOutboxMetaTable = "reducer_outbox_meta";
const operations = new WeakMap<GonvexKv, Promise<void>>();

export function withKvOutboxOperation<T>(kv: GonvexKv, run: () => Promise<T>): Promise<T> {
  const previous = operations.get(kv) ?? Promise.resolve();
  const result = previous.then(run);
  operations.set(kv, result.then(() => undefined, () => undefined));
  return result;
}

/** Serialize allocators sharing this host object. A KV host with multiple
 * writer processes must supply an atomic allocator instead. */
export function reserveKvOutboxId(kv: GonvexKv): Promise<number> {
  return withKvOutboxOperation(kv, async () => {
    const raw = await kv.get(kvReducerOutboxMetaTable, "sequence");
    let allocated = 0;
    if (raw !== undefined) {
      let value: unknown;
      try { value = JSON.parse(raw); }
      catch (cause) { throw new OutboxRecoveryError("Invalid durable outbox sequence; recovery is required", { cause }); }
      if (!value || typeof value !== "object" || Array.isArray(value)) throw new OutboxRecoveryError("Invalid durable outbox sequence; recovery is required");
      const record = value as { version?: unknown; allocated?: unknown };
      if (record.version !== 1 || typeof record.allocated !== "number" || !Number.isSafeInteger(record.allocated) || record.allocated < 0)
        throw new OutboxRecoveryError("Unsupported or invalid durable outbox sequence; recovery is required");
      allocated = record.allocated;
    }
    // Seed old journals without metadata, and honor explicit imported IDs.
    // Validate the complete history before changing its allocation checkpoint.
    for (const { key, value } of await kv.list("reducer_outbox")) {
      let entry: unknown;
      try { entry = JSON.parse(value); }
      catch (cause) { throw new OutboxRecoveryError("Invalid durable outbox JSON; recovery is required", { cause }); }
      validateOutboxEntry(entry, key);
      allocated = Math.max(allocated, entry.id);
    }
    if (allocated >= Number.MAX_SAFE_INTEGER) throw new OutboxRecoveryError("Durable outbox sequence is exhausted; recovery is required");
    const id = allocated + 1;
    // A failed append burns the reserved position. Retirement and scope clear
    // never erase this separate allocation checkpoint.
    await kv.set(kvReducerOutboxMetaTable, "sequence", JSON.stringify({ version: 1, allocated: id }));
    return id;
  });
}
