import type { OutboxStore, ReducerOutboxEntry, ReducerOutboxState } from "@gonvex/client";
import type { ExpoSQLiteDatabase } from "./index.js";

const outboxStates: ReadonlySet<ReducerOutboxState> = new Set(["pending", "inflight", "committed", "failed", "rejected"]);

type OutboxRow = { value: string };

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
  readonly strictPersistence = false;
  private initialized?: Promise<void>;
  private queue: Promise<unknown> = Promise.resolve();

  constructor(private readonly database: ExpoSQLiteDatabase) {}

  load(scope?: string): Promise<ReducerOutboxEntry[]> {
    return this.serial(async () => {
      const rows = scope === undefined
        ? await this.database.getAllAsync<OutboxRow>(`SELECT value FROM _gonvex_outbox ORDER BY id`)
        : await this.database.getAllAsync<OutboxRow>(`SELECT value FROM _gonvex_outbox WHERE scope = ? ORDER BY id`, scope);
      const entries: ReducerOutboxEntry[] = [];
      for (const row of rows) {
        const entry = parseEntry(row.value);
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
      const row = await this.database.getFirstAsync<OutboxRow>(`SELECT value FROM _gonvex_outbox WHERE id = ?`, id);
      const prior = row ? parseEntry(row.value) : undefined;
      if (!prior) return undefined;
      const next = change(prior);
      if (next.id !== prior.id || next.scope !== prior.scope || next.idempotencyKey !== prior.idempotencyKey) {
        throw new Error("Outbox identity cannot change");
      }
      if (JSON.stringify(next) !== JSON.stringify(prior)) await this.write(next);
      return next;
    }));
  }

  /**
   * Import entries from a legacy whole-list JSON blob (for example the value
   * an app kept under an AsyncStorage key). Runs in one transaction and is
   * idempotent: an entry whose scope and idempotency key already exist is
   * skipped, so re-running after a crash never duplicates an intent. Legacy
   * ids are preserved to keep enqueue order; an id already taken by a
   * different intent gets a fresh sequence id.
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
      for (const entry of sorted) {
        const existing = await this.database.getFirstAsync<{ id: number }>(
          `SELECT id FROM _gonvex_outbox WHERE scope = ? AND idempotency_key = ?`, entry.scope, entry.idempotencyKey,
        );
        if (existing) { result.skipped += 1; continue; }
        const taken = await this.database.getFirstAsync<{ id: number }>(`SELECT id FROM _gonvex_outbox WHERE id = ?`, entry.id);
        // A legacy row that was mid-send when the app died is resumed exactly
        // like the SDK's own crash recovery: back to pending, same key.
        const imported: ReducerOutboxEntry = {
          ...entry,
          ...(taken ? { id: await this.reserveId() } : {}),
          ...(entry.state === "inflight" ? { state: "pending" as const } : {}),
        };
        await this.write(imported);
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
   * the same connection. Serialize this store's own work so two outbox
   * transactions never interleave.
   */
  private serial<T>(run: () => Promise<T>): Promise<T> {
    const job = this.queue.then(async () => {
      await this.initialize();
      return run();
    });
    this.queue = job.catch(() => undefined);
    return job;
  }

  private async transaction<T>(run: () => Promise<T>): Promise<T> {
    let result: T | undefined;
    await this.database.withTransactionAsync(async () => { result = await run(); });
    return result as T;
  }

  private async reserveId(): Promise<number> {
    const counter = await this.database.getFirstAsync<{ value: number }>(`SELECT value FROM _gonvex_outbox_meta WHERE key = 'sequence'`);
    const highest = await this.database.getFirstAsync<{ id: number | null }>(`SELECT MAX(id) AS id FROM _gonvex_outbox`);
    const id = Math.max(Number(counter?.value ?? 0), Number(highest?.id ?? 0)) + 1;
    await this.database.runAsync(
      `INSERT INTO _gonvex_outbox_meta (key, value) VALUES ('sequence', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      id,
    );
    return id;
  }

  private async write(entry: ReducerOutboxEntry) {
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
 * the import committed; an unreadable blob throws and is left untouched so no
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
  await options.storage.removeItem(options.key);
  return result;
}

function parseEntry(value: string): ReducerOutboxEntry | undefined {
  try {
    return validEntry(JSON.parse(value));
  } catch {
    return undefined;
  }
}

function validEntry(value: unknown): ReducerOutboxEntry | undefined {
  if (!value || typeof value !== "object") return undefined;
  const entry = value as Partial<ReducerOutboxEntry>;
  if (
    typeof entry.id !== "number" || !Number.isSafeInteger(entry.id) || entry.id <= 0
    || typeof entry.scope !== "string" || entry.scope.length === 0
    || typeof entry.path !== "string"
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
