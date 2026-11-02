import { OutboxRecoveryError, coalescingAdmission } from "@gonvex/client";
import type { OutboxStore, ReducerOutboxEntry, ReducerOutboxState } from "@gonvex/client";
import type { ExpoSQLiteDatabase } from "./index.js";
import { withSQLiteOperation } from './database-operations.js';

const outboxStates: ReadonlySet<ReducerOutboxState> = new Set(["pending", "inflight", "committed", "failed", "rejected"]);

type OutboxRow = { id: number; scope_json: string; intent_key_json: string; state: string; value: string };

/**
 * Durable reducer outbox storage on Expo SQLite.
 *
 * One row per intent, keyed by the SDK's sequence id, with every mutation in
 * its own SQLite transaction. Unlike a single JSON blob rewritten on each
 * change, an update touches one row and a crash can never truncate the whole
 * queue. Ids come from a persisted sequence, so a deleted entry's id is never
 * reused across restarts.
 *
 * Pass the same database handle that backs the Local Replica or a dedicated
 * one; the tables are prefixed with `_gonvex_outbox`.
 */
export class ExpoSQLiteOutboxStore implements OutboxStore {
  readonly strictPersistence = true;
  private initialized?: Promise<void>;

  constructor(private readonly database: ExpoSQLiteDatabase) {}

  load(scope?: string): Promise<ReducerOutboxEntry[]> {
    return this.serial(async () => {
      const rows = scope === undefined
        ? await this.database.getAllAsync<OutboxRow>(`SELECT id,json_quote(scope) AS scope_json,json_quote(idempotency_key) AS intent_key_json,state,value FROM _gonvex_outbox ORDER BY id`)
        : await this.database.getAllAsync<OutboxRow>(`SELECT id,json_quote(scope) AS scope_json,json_quote(idempotency_key) AS intent_key_json,state,value FROM _gonvex_outbox WHERE scope = ? ORDER BY id`, scope);
      const entries: ReducerOutboxEntry[] = [];
      for (const row of rows) {
        const entry = parseRow(row);
        if (entry) entries.push(entry);
      }
      return entries;
    });
  }

  put(entry: ReducerOutboxEntry): Promise<void> {
    return this.serial(() => this.transaction(() => this.write(entry)));
  }

  delete(id: number): Promise<void> {
    return this.serial(async () => {
      await this.database.runAsync(`DELETE FROM _gonvex_outbox WHERE id = ?`, id);
    });
  }

  deleteMany(ids: number[]): Promise<void> {
    return this.serial(() => this.transaction(async () => {
      for (const id of ids) await this.database.runAsync(`DELETE FROM _gonvex_outbox WHERE id = ?`, id);
    }));
  }

  clear(scope?: string): Promise<void> {
    return this.serial(async () => {
      if (scope === undefined) await this.database.runAsync(`DELETE FROM _gonvex_outbox`);
      else await this.database.runAsync(`DELETE FROM _gonvex_outbox WHERE scope = ?`, scope);
    });
  }

  allocateId(): Promise<number> {
    return this.serial(() => this.transaction(() => this.reserveId()));
  }

  append(draft: Omit<ReducerOutboxEntry, "id">): Promise<ReducerOutboxEntry> {
    return this.serial(() => this.transaction(async () => {
      const entry = { ...draft, id: await this.reserveId() } as ReducerOutboxEntry;
      await this.write(entry);
      return entry;
    }));
  }

  update(id: number, change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry): Promise<ReducerOutboxEntry | undefined> {
    return this.serial(() => this.transaction(async () => {
      const row = await this.database.getFirstAsync<OutboxRow>(`SELECT id,json_quote(scope) AS scope_json,json_quote(idempotency_key) AS intent_key_json,state,value FROM _gonvex_outbox WHERE id = ?`, id);
      const prior = row ? parseRow(row) : undefined;
      if (!prior) return undefined;
      const scope = prior.scope, intentKey = prior.idempotencyKey;
      const next = change(prior);
      if (next.id !== id || next.scope !== scope || next.idempotencyKey !== intentKey) {
        throw new Error("Outbox identity cannot change");
      }
      if (JSON.stringify(next) !== row!.value) await this.write(next);
      return next;
    }));
  }

  updateMany(ids: number[], change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry): Promise<ReducerOutboxEntry[]> {
    return this.serial(() => this.transaction(async () => {
      const entries: ReducerOutboxEntry[] = [];
      for (const id of ids) {
        const row = await this.database.getFirstAsync<OutboxRow>(`SELECT id,json_quote(scope) AS scope_json,json_quote(idempotency_key) AS intent_key_json,state,value FROM _gonvex_outbox WHERE id = ?`, id);
        const prior = row ? parseRow(row) : undefined;
        if (!prior) continue;
        const scope = prior.scope, intentKey = prior.idempotencyKey;
        const next = change(prior);
        if (next.id !== id || next.scope !== scope || next.idempotencyKey !== intentKey) throw new Error("Outbox identity cannot change");
        if (JSON.stringify(next) !== row!.value) await this.write(next);
        entries.push(next);
      }
      return entries;
    }));
  }

  replacePending(id: number, draft: Omit<ReducerOutboxEntry, "id">): Promise<ReducerOutboxEntry | undefined> {
    return this.serial(() => this.transaction(async () => {
      const row = await this.database.getFirstAsync<OutboxRow>(`SELECT id,json_quote(scope) AS scope_json,json_quote(idempotency_key) AS intent_key_json,state,value FROM _gonvex_outbox WHERE id = ?`, id);
      const prior = row ? parseRow(row) : undefined;
      if (!coalescingAdmission(draft.state, draft.knownUnsent) || !prior || !coalescingAdmission(prior.state, prior.knownUnsent) || prior.scope !== draft.scope || prior.path !== draft.path || prior.coalesceKey !== draft.coalesceKey || prior.sentAt !== undefined || prior.attempts || prior.lastError) return undefined;
      const next = { ...draft, id };
      await this.write(next);
      return next;
    }));
  }

  /**
   * Import entries from a legacy whole-list JSON blob (for example the value
   * an app kept under an AsyncStorage key). Runs in one transaction and is
   * idempotent: an entry whose scope and idempotency key already exist is
   * skipped, so re-running after a crash never duplicates an intent. Legacy
   * ids are preserved where compatible with enqueue order. Collisions and
   * subsequent rows receive fresh positions together. Conflicting existing
   * positions abort the import, retaining the source for recovery.
   */
  importLegacy(legacy: string | readonly unknown[] | null | undefined): Promise<LegacyOutboxImport> {
    const parsed = typeof legacy === "string" ? JSON.parse(legacy) as unknown : legacy;
    if (parsed === null || parsed === undefined) return Promise.resolve({ imported: 0, skipped: 0, invalid: 0 });
    if (!Array.isArray(parsed)) return Promise.reject(new Error("Legacy Gonvex outbox must be a JSON array"));
    return this.serial(() => this.transaction(async () => {
      const sorted = parsed
        .map((value) => validEntry(value))
        .filter((entry): entry is ReducerOutboxEntry => entry !== undefined)
        .sort((left, right) => left.id - right.id);
      const result: LegacyOutboxImport = { imported: 0, skipped: 0, invalid: parsed.length - sorted.length };
      let previousId = 0;
      for (const entry of sorted) {
        const existingRow = await this.database.getFirstAsync<OutboxRow>(
          `SELECT id,json_quote(scope) AS scope_json,json_quote(idempotency_key) AS intent_key_json,state,value FROM _gonvex_outbox WHERE scope = ? AND idempotency_key = ?`, entry.scope, entry.idempotencyKey,
        );
        if (existingRow) {
          const existing = parseRow(existingRow);
          if (existing.id < previousId) throw new OutboxRecoveryError("Legacy outbox order conflicts with existing command positions; recovery is required");
          previousId = existing.id;
          result.skipped += 1;
          continue;
        }
        const taken = await this.database.getFirstAsync<{ id: number }>(`SELECT id FROM _gonvex_outbox WHERE id = ?`, entry.id);
        // A legacy row that was mid-send when the app died is resumed exactly
        // like the SDK's own crash recovery: back to pending, same key.
        const imported: ReducerOutboxEntry = {
          ...entry,
          knownUnsent: false,
          // Once an earlier collision moved the sequence forward, later
          // legacy rows must move with it to retain their enqueue order.
          ...(taken || entry.id <= previousId ? { id: await this.reserveId() } : {}),
          ...(entry.state === "inflight" ? { state: "pending" as const } : {}),
        };
        await this.write(imported);
        previousId = imported.id;
        result.imported += 1;
      }
      return result;
    }));
  }

  private initialize() {
    return (this.initialized ??= this.database.withTransactionAsync(async () => {
      await this.database.execAsync(`
        CREATE TABLE IF NOT EXISTS _gonvex_outbox (
          id INTEGER PRIMARY KEY,
          scope TEXT NOT NULL,
          idempotency_key TEXT NOT NULL,
          state TEXT NOT NULL,
          value TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS _gonvex_outbox_scope ON _gonvex_outbox(scope, id);
        CREATE UNIQUE INDEX IF NOT EXISTS _gonvex_outbox_intent ON _gonvex_outbox(scope, idempotency_key);
        CREATE TABLE IF NOT EXISTS _gonvex_outbox_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
      `);
    }).catch((error) => {
      this.initialized = undefined;
      throw error;
    }));
  }

  /**
   * Expo's withTransactionAsync is not exclusive against other statements on
   * the same connection. Share admission with every replica and outbox
   * instance using this handle, including reads and schema initialization.
   */
  private serial<T>(run: () => Promise<T>): Promise<T> {
    return withSQLiteOperation(this.database, async () => {
      await this.initialize();
      return run();
    });
  }

  private async transaction<T>(run: () => Promise<T>): Promise<T> {
    let result: T | undefined;
    await this.database.withTransactionAsync(async () => { result = await run(); });
    return result as T;
  }

  private async reserveId(): Promise<number> {
    const counter = await this.database.getFirstAsync<{ value: number }>(`SELECT value FROM _gonvex_outbox_meta WHERE key = 'sequence'`);
    const highest = await this.database.getFirstAsync<{ id: number | null }>(`SELECT MAX(id) AS id FROM _gonvex_outbox`);
    const allocated = counter?.value ?? 0, maximum = highest?.id ?? 0;
    if (!Number.isSafeInteger(allocated) || allocated < 0 || !Number.isSafeInteger(maximum) || maximum < 0) {
      throw new OutboxRecoveryError("Invalid durable SQLite outbox sequence; recovery is required");
    }
    const previous = Math.max(allocated, maximum);
    if (previous >= Number.MAX_SAFE_INTEGER) throw new OutboxRecoveryError("Durable SQLite outbox sequence is exhausted; recovery is required");
    const id = previous + 1;
    await this.database.runAsync(
      `INSERT INTO _gonvex_outbox_meta (key, value) VALUES ('sequence', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      id,
    );
    return id;
  }

  private async write(entry: ReducerOutboxEntry) {
    if (!validEntry(entry)) throw new OutboxRecoveryError("Invalid durable SQLite outbox intent; recovery is required");
    await this.database.runAsync(
      `INSERT OR REPLACE INTO _gonvex_outbox (id, scope, idempotency_key, state, value) VALUES (?, ?, ?, ?, ?)`,
      entry.id, entry.scope, entry.idempotencyKey, entry.state, JSON.stringify(entry),
    );
  }
}

export type LegacyOutboxImport = {
  imported: number;
  /** Entries already present (same scope and idempotency key). */
  skipped: number;
  /** Values that were not recognizable outbox entries. */
  invalid: number;
};

export function expoSQLiteOutbox(database: ExpoSQLiteDatabase): ExpoSQLiteOutboxStore {
  return new ExpoSQLiteOutboxStore(database);
}

/** The subset of AsyncStorage the one-time migration needs. */
export type LegacyKeyValueStorage = {
  getItem(key: string): Promise<string | null>;
  removeItem(key: string): Promise<void>;
};

/**
 * One-time move of a legacy AsyncStorage outbox blob into SQLite. Run it
 * before constructing the GonvexClient. The legacy key is removed only after
 * the import committed and every row was recognized. An unreadable or partially
 * unresolved blob throws and is left untouched so no
 * intent is ever lost. Safe to call on every launch.
 */
export async function migrateLegacyOutbox(options: {
  store: ExpoSQLiteOutboxStore;
  storage: LegacyKeyValueStorage;
  key: string;
}): Promise<LegacyOutboxImport> {
  const raw = await options.storage.getItem(options.key);
  if (raw === null || raw === undefined) return { imported: 0, skipped: 0, invalid: 0 };
  const result = await options.store.importLegacy(raw);
  // An explicit invalid count does not authorize deleting unknown history.
  // Keep the whole source until every row has a recognized owner and intent.
  if (result.invalid > 0) throw new OutboxRecoveryError("Legacy outbox contains unresolved intents; the original source was retained");
  await options.storage.removeItem(options.key);
  return result;
}

function parseEntry(value: string): ReducerOutboxEntry {
  let parsed: unknown;
  try { parsed = JSON.parse(value); }
  catch (cause) { throw new OutboxRecoveryError("Invalid durable SQLite outbox JSON; recovery is required", { cause }); }
  const entry = validEntry(parsed);
  if (!entry) throw new OutboxRecoveryError("Invalid durable SQLite outbox intent; recovery is required");
  return entry;
}

function parseRow(row: OutboxRow): ReducerOutboxEntry {
  const entry = parseEntry(row.value);
  // Some SQLite bridges truncate TEXT at embedded NULs. JSON quoting in SQL
  // preserves the complete indexed owner/key before the bridge reads them.
  if (entry.id !== row.id || entry.scope !== JSON.parse(row.scope_json) || entry.idempotencyKey !== JSON.parse(row.intent_key_json) || entry.state !== row.state) {
    throw new OutboxRecoveryError("Inconsistent durable SQLite outbox identity or state; recovery is required");
  }
  return entry;
}

function validEntry(value: unknown): ReducerOutboxEntry | undefined {
  if (!value || typeof value !== "object") return undefined;
  const entry = value as Partial<ReducerOutboxEntry>;
  if (
    typeof entry.id !== "number" || !Number.isSafeInteger(entry.id) || entry.id <= 0
    || typeof entry.scope !== "string" || entry.scope.length === 0
    || typeof entry.path !== "string" || entry.path.length === 0
    || !Object.prototype.hasOwnProperty.call(entry, "args") || entry.args === undefined
    || typeof entry.idempotencyKey !== "string" || entry.idempotencyKey.length === 0
    || typeof entry.state !== "string" || !outboxStates.has(entry.state)
  ) return undefined;
  return {
    ...entry,
    entityKeys: Array.isArray(entry.entityKeys) ? entry.entityKeys : [],
    createdAt: typeof entry.createdAt === "number" ? entry.createdAt : 0,
    attempts: typeof entry.attempts === "number" ? entry.attempts : 0,
    nextAttemptAt: typeof entry.nextAttemptAt === "number" ? entry.nextAttemptAt : 0,
  } as ReducerOutboxEntry;
}
