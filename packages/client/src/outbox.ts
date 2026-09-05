import type { Dexie as DexieDatabase, Table } from "dexie";
import type { OptimisticPatch } from "./optimistic.js";
import type { LocalExecution } from "@gonvex/local-runtime";

export type ReducerOutboxOptions = {
  databaseName?: string;
  indexedDB?: IDBFactory;
  IDBKeyRange?: typeof IDBKeyRange;
  enabled?: boolean;
  /**
   * Injectable durable record storage for runtimes without IndexedDB (React
   * Native). When provided, the queue semantics still run in the SDK; the
   * store only persists entries so they survive an app restart.
   */
  store?: OutboxStore;
};

/**
 * A dumb durable record store backing {@link StoreReducerOutbox}. It holds
 * whole entries keyed by id and knows nothing about queue semantics — total
 * order, causal barriers, and inflight recovery all stay in the SDK.
 */
export type OutboxStore = {
  /** Version-fenced stores must never fall back to unpersisted sends. */
  strictPersistence?: boolean;
  /** Reserve globally unique sequence numbers when several tabs share a store. */
  allocateId?(): Promise<number>;
  /** Every persisted entry across all scopes; called once to hydrate. */
  load(): Promise<ReducerOutboxEntry[]>;
  /** Insert or replace the entry with this id. */
  put(entry: ReducerOutboxEntry): Promise<void>;
  delete(id: number): Promise<void>;
  clear(scope?: string): Promise<void>;
  close?(): void;
};

export type ReducerOutboxEntry = {
  /** Original receipt namespace survives operation renames across client upgrades. */
  receiptPath?: string;
  /** Auto-incremented sequence number. Lower ids always happened first. */
  id: number;
  /** Authenticated project/tenant/user identity that owns this reducer. */
  scope: string;
  path: string;
  args: unknown;
  idempotencyKey: string;
  /** Entity identifiers whose writes must retain enqueue order. */
  entityKeys: string[];
  /** Optimistic UI state restored while this entry awaits a server result. */
  patches?: OptimisticPatch[];
  localExecution?: LocalExecution;
  createdAt: number;
  attempts: number;
  nextAttemptAt: number;
  lastError?: string;
  state: "pending" | "inflight" | "committed";
};

export type EnqueueReducer = {
  scope: string;
  path: string;
  args: unknown;
  idempotencyKey?: string;
  entityKeys?: string[];
  patches?: OptimisticPatch[];
  localExecution?: LocalExecution;
  /** Direct online sends start inflight so the background drain cannot race them. */
  state?: "pending" | "inflight";
};

export type ReducerOutbox = {
  enqueue(reducer: EnqueueReducer): Promise<ReducerOutboxEntry>;
  loadAll(scope: string): Promise<ReducerOutboxEntry[]>;
  /** Observe current records without performing startup inflight recovery. */
  list(scope: string): Promise<ReducerOutboxEntry[]>;
  updateLocal(id: number, patches: OptimisticPatch[], execution: LocalExecution): Promise<void>;
  nextReady(scope: string, now: number): Promise<ReducerOutboxEntry | undefined>;
  markInflight(id: number): Promise<void>;
  /** Return a just-admitted entry to pending without recording a failed attempt. */
  markPending(id: number): Promise<void>;
  markCommitted(id: number): Promise<void>;
  ack(id: number): Promise<void>;
  fail(id: number, error: string): Promise<void>;
  count(scope: string): Promise<number>;
  clear(scope: string): Promise<void>;
  subscribe(listener: () => void): () => void;
};

type NewReducerOutboxEntry = Omit<ReducerOutboxEntry, "id"> & { id?: number };

type ReducerOutboxDatabase = DexieDatabase & {
  entries: Table<ReducerOutboxEntry, number, NewReducerOutboxEntry>;
};

/**
 * A durable, totally ordered reducer queue.
 *
 * The auto-incremented id is the enqueue order and is never reused while the
 * database exists. `nextReady` may skip unrelated writes, but it never skips a
 * lower-id write touching the same entity. An inflight entry remains a causal
 * barrier until it is acknowledged, and `loadAll` recovers abandoned inflight
 * work after a crash by returning it to pending.
 *
 * IndexedDB is an optimization for durability, not a prerequisite for the
 * optimistic reducer path. Disabled or failed storage permanently degrades
 * this instance to the same queue semantics in memory for the current session.
 */
export class DexieReducerOutbox implements ReducerOutbox {
  private readonly databaseName: string;
  private readonly indexedDB?: IDBFactory;
  private readonly keyRange?: typeof IDBKeyRange;
  private readonly listeners = new Set<() => void>();
  private readonly memoryEntries = new Map<number, ReducerOutboxEntry>();
  private database?: ReducerOutboxDatabase;
  private databasePromise?: Promise<ReducerOutboxDatabase>;
  private memoryOnly: boolean;
  private nextMemoryId = 1;

  constructor(options: ReducerOutboxOptions = {}) {
    this.databaseName = options.databaseName ?? "gonvex-outbox";
    this.indexedDB = options.indexedDB;
    this.keyRange = options.IDBKeyRange;
    this.memoryOnly = options.enabled === false
      || !(options.indexedDB ?? globalThis.indexedDB)
      || !(options.IDBKeyRange ?? globalThis.IDBKeyRange);
  }

  async enqueue(reducer: EnqueueReducer): Promise<ReducerOutboxEntry> {
    const createdAt = Date.now();
    const entry: NewReducerOutboxEntry = {
      scope: reducer.scope,
      path: reducer.path,
      args: cloneValue(reducer.args),
      idempotencyKey: reducer.idempotencyKey ?? createIdempotencyKey(),
      entityKeys: [...(reducer.entityKeys ?? [])],
      patches: reducer.patches?.map(clonePatch),
      ...(reducer.localExecution ? { localExecution: cloneValue(reducer.localExecution) } : {}),
      createdAt,
      attempts: 0,
      nextAttemptAt: createdAt,
      state: reducer.state ?? "pending",
    };

    if (this.memoryOnly) {
      if (reducer.localExecution) throw new Error("Durable storage is required for local reducer execution");
      return this.enqueueInMemory(entry);
    }
    try {
      const database = await this.open();
      const id = await database.entries.add(entry);
      const stored = { ...entry, id } as ReducerOutboxEntry;
      this.remember(stored);
      this.notify();
      return cloneEntry(stored);
    } catch (error) {
      if (reducer.localExecution) throw new Error("Could not persist local reducer; no changes were staged", { cause: error });
      this.degradeToMemory();
      return this.enqueueInMemory(entry);
    }
  }

  async loadAll(scope: string): Promise<ReducerOutboxEntry[]> {
    if (this.memoryOnly) return this.loadAllFromMemory(scope);
    try {
      const database = await this.open();
      let changed = false;
      let entries: ReducerOutboxEntry[] = [];
      await database.transaction("rw", database.entries, async () => {
        entries = await database.entries.where("scope").equals(scope).sortBy("id");
        entries = entries.map((entry) => {
          if (entry.state !== "inflight") return entry;
          changed = true;
          return { ...entry, state: "pending" as const };
        });
        if (changed) await database.entries.bulkPut(entries);
      });
      this.replaceMemoryEntriesForScope(scope, entries);
      if (changed) this.notify();
      return entries.map(cloneEntry);
    } catch {
      this.degradeToMemory();
      return this.loadAllFromMemory(scope);
    }
  }

  async list(scope: string): Promise<ReducerOutboxEntry[]> {
    if (this.memoryOnly) return this.sortedMemoryEntries(scope).map(cloneEntry);
    const entries = await (await this.open()).entries.where("scope").equals(scope).sortBy("id");
    this.replaceMemoryEntriesForScope(scope, entries);
    return entries.map(cloneEntry);
  }

  async updateLocal(id: number, patches: OptimisticPatch[], execution: LocalExecution): Promise<void> {
    if (this.memoryOnly) throw new Error("Durable storage is required for local reducer replay");
    const database = await this.open();
    await database.transaction("rw", database.entries, async () => {
      const entry = await database.entries.get(id);
      if (!entry) return;
      const updated = { ...entry, patches: patches.map(clonePatch), localExecution: cloneValue(execution) };
      await database.entries.put(updated);
      this.remember(updated);
    });
  }

  async nextReady(scope: string, now: number): Promise<ReducerOutboxEntry | undefined> {
    if (this.memoryOnly) return cloneOptionalEntry(firstReady(this.sortedMemoryEntries(scope), now));
    try {
      const database = await this.open();
      const entries = await database.entries.where("scope").equals(scope).sortBy("id");
      this.replaceMemoryEntriesForScope(scope, entries);
      return cloneOptionalEntry(firstReady(entries, now));
    } catch {
      this.degradeToMemory();
      return cloneOptionalEntry(firstReady(this.sortedMemoryEntries(scope), now));
    }
  }

  async markInflight(id: number): Promise<void> {
    if (this.memoryOnly) {
      this.markInflightInMemory(id);
      return;
    }
    try {
      const database = await this.open();
      let updated: ReducerOutboxEntry | undefined;
      await database.transaction("rw", database.entries, async () => {
        const entry = await database.entries.get(id);
        if (!entry || entry.state === "inflight") return;
        updated = { ...entry, state: "inflight" as const };
        await database.entries.put(updated);
      });
      if (!updated) return;
      this.remember(updated);
      this.notify();
    } catch {
      this.degradeToMemory();
      this.markInflightInMemory(id);
    }
  }

  async markPending(id: number): Promise<void> {
    if (this.memoryOnly) {
      this.markPendingInMemory(id);
      return;
    }
    try {
      const database = await this.open();
      let updated: ReducerOutboxEntry | undefined;
      await database.transaction("rw", database.entries, async () => {
        const entry = await database.entries.get(id);
        if (!entry || entry.state === "pending") return;
        updated = {
          ...entry,
          state: "pending",
          nextAttemptAt: Date.now(),
          lastError: undefined,
        };
        await database.entries.put(updated);
      });
      if (!updated) return;
      this.remember(updated);
      this.notify();
    } catch {
      this.degradeToMemory();
      this.markPendingInMemory(id);
    }
  }

  async markCommitted(id: number): Promise<void> {
    if (this.memoryOnly) {
      this.markCommittedInMemory(id);
      return;
    }
    try {
      const database = await this.open();
      let updated: ReducerOutboxEntry | undefined;
      await database.transaction("rw", database.entries, async () => {
        const entry = await database.entries.get(id);
        if (!entry || entry.state === "committed") return;
        updated = { ...entry, state: "committed" as const };
        await database.entries.put(updated);
      });
      if (!updated) return;
      this.remember(updated);
      this.notify();
    } catch {
      this.degradeToMemory();
      this.markCommittedInMemory(id);
    }
  }

  async ack(id: number): Promise<void> {
    if (this.memoryOnly) {
      this.ackInMemory(id);
      return;
    }
    try {
      const database = await this.open();
      const entry = await database.entries.get(id);
      if (!entry) return;
      await database.entries.delete(id);
      this.memoryEntries.delete(id);
      this.notify();
    } catch {
      this.degradeToMemory();
      this.ackInMemory(id);
    }
  }

  async fail(id: number, error: string): Promise<void> {
    if (this.memoryOnly) {
      this.failInMemory(id, error);
      return;
    }
    try {
      const database = await this.open();
      let updated: ReducerOutboxEntry | undefined;
      await database.transaction("rw", database.entries, async () => {
        const entry = await database.entries.get(id);
        if (!entry) return;
        updated = failedEntry(entry, error, Date.now());
        await database.entries.put(updated);
      });
      if (!updated) return;
      this.remember(updated);
      this.notify();
    } catch {
      this.degradeToMemory();
      this.failInMemory(id, error);
    }
  }

  async count(scope: string): Promise<number> {
    if (this.memoryOnly) return this.sortedMemoryEntries(scope).length;
    try {
      return await (await this.open()).entries.where("scope").equals(scope).count();
    } catch {
      this.degradeToMemory();
      return this.sortedMemoryEntries(scope).length;
    }
  }

  async clear(scope: string): Promise<void> {
    // Snapshot ids synchronously. An enqueue that starts after clear must not
    // be swallowed by a later scope-wide IndexedDB delete after open() yields.
    const ids = this.sortedMemoryEntries(scope).map((entry) => entry.id);
    for (const [id, entry] of this.memoryEntries) {
      if (entry.scope === scope) this.memoryEntries.delete(id);
    }
    if (!this.memoryOnly && ids.length > 0) {
      try {
        await (await this.open()).entries.bulkDelete(ids);
      } catch {
        this.degradeToMemory();
      }
    }
    this.notify();
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private enqueueInMemory(entry: NewReducerOutboxEntry): ReducerOutboxEntry {
    const stored = { ...entry, id: this.nextMemoryId++ } as ReducerOutboxEntry;
    this.remember(stored);
    this.notify();
    return cloneEntry(stored);
  }

  private loadAllFromMemory(scope: string): ReducerOutboxEntry[] {
    let changed = false;
    for (const [id, entry] of this.memoryEntries) {
      if (entry.scope !== scope) continue;
      if (entry.state !== "inflight") continue;
      this.memoryEntries.set(id, { ...entry, state: "pending" });
      changed = true;
    }
    if (changed) this.notify();
    return this.sortedMemoryEntries(scope).map(cloneEntry);
  }

  private markInflightInMemory(id: number) {
    const entry = this.memoryEntries.get(id);
    if (!entry || entry.state === "inflight") return;
    this.memoryEntries.set(id, { ...entry, state: "inflight" });
    this.notify();
  }

  private markPendingInMemory(id: number) {
    const entry = this.memoryEntries.get(id);
    if (!entry || entry.state === "pending") return;
    this.memoryEntries.set(id, {
      ...entry,
      state: "pending",
      nextAttemptAt: Date.now(),
      lastError: undefined,
    });
    this.notify();
  }

  private markCommittedInMemory(id: number) {
    const entry = this.memoryEntries.get(id);
    if (!entry || entry.state === "committed") return;
    this.memoryEntries.set(id, { ...entry, state: "committed" });
    this.notify();
  }

  private ackInMemory(id: number) {
    if (!this.memoryEntries.delete(id)) return;
    this.notify();
  }

  private failInMemory(id: number, error: string) {
    const entry = this.memoryEntries.get(id);
    if (!entry) return;
    this.memoryEntries.set(id, failedEntry(entry, error, Date.now()));
    this.notify();
  }

  private sortedMemoryEntries(scope?: string) {
    return [...this.memoryEntries.values()]
      .filter((entry) => scope === undefined || entry.scope === scope)
      .sort((left, right) => left.id - right.id);
  }

  private remember(entry: ReducerOutboxEntry) {
    this.memoryEntries.set(entry.id, cloneEntry(entry));
    this.nextMemoryId = Math.max(this.nextMemoryId, entry.id + 1);
  }

  private replaceMemoryEntriesForScope(scope: string, entries: ReducerOutboxEntry[]) {
    for (const [id, entry] of this.memoryEntries) {
      if (entry.scope === scope) this.memoryEntries.delete(id);
    }
    for (const entry of entries) this.remember(entry);
  }

  private notify() {
    for (const listener of this.listeners) {
      try {
        listener();
      } catch {
        // A subscriber must not be able to break reducer delivery.
      }
    }
  }

  private degradeToMemory() {
    this.memoryOnly = true;
    this.database?.close();
    this.database = undefined;
    this.databasePromise = undefined;
  }

  private async open(): Promise<ReducerOutboxDatabase> {
    if (!this.databasePromise) {
      this.databasePromise = this.createDatabase().catch((error) => {
        this.databasePromise = undefined;
        throw error;
      });
    }
    return this.databasePromise;
  }

  private async createDatabase(): Promise<ReducerOutboxDatabase> {
    const indexedDBValue = this.indexedDB ?? globalThis.indexedDB;
    const keyRangeValue = this.keyRange ?? globalThis.IDBKeyRange;
    if (!indexedDBValue || !keyRangeValue) throw new Error("indexeddb-unavailable");
    const { Dexie } = await import("dexie");
    const database = new Dexie(this.databaseName, {
      indexedDB: indexedDBValue,
      IDBKeyRange: keyRangeValue,
    }) as ReducerOutboxDatabase;
    database.version(1).stores({
      entries: "++id, state, nextAttemptAt",
    });
    database.version(2).stores({
      entries: "++id, scope, state, nextAttemptAt, [scope+state], [scope+nextAttemptAt]",
    }).upgrade(async (transaction) => {
      // Version 1 did not record an authenticated owner. Replaying one of
      // those rows after an account switch is unsafe, so leave it quarantined
      // by deleting it instead of guessing which identity created it.
      await transaction.table("entries").filter((entry) => (
        typeof entry.scope !== "string" || entry.scope.length === 0
      )).delete();
    });
    database.on("versionchange", () => database.close());
    await database.open();
    this.database = database;
    return database;
  }
}

/**
 * The same totally ordered queue semantics as {@link DexieReducerOutbox},
 * run entirely in memory with every state transition written through an
 * injected {@link OutboxStore}. Hydration replays `store.load()` once and
 * seeds the auto-increment id from the highest persisted id, so replayed
 * entries keep their original ids, idempotency keys, and enqueue order.
 *
 * The store is an optimization for durability, not a prerequisite for the
 * optimistic reducer path. A failing store permanently degrades this
 * instance to the same queue semantics in memory for the current session.
 */
export class StoreReducerOutbox implements ReducerOutbox {
  private readonly store: OutboxStore;
  private readonly listeners = new Set<() => void>();
  private readonly entries = new Map<number, ReducerOutboxEntry>();
  private readonly ready: Promise<void>;
  private memoryOnly: boolean;
  private nextId = 1;

  constructor(store: OutboxStore, options: { enabled?: boolean } = {}) {
    this.store = store;
    this.memoryOnly = options.enabled === false;
    this.ready = this.memoryOnly ? Promise.resolve() : this.hydrate();
  }

  async enqueue(reducer: EnqueueReducer): Promise<ReducerOutboxEntry> {
    await this.ready;
    const createdAt = Date.now();
    const entry: ReducerOutboxEntry = {
      id: this.store.allocateId ? await this.store.allocateId() : this.nextId++,
      scope: reducer.scope,
      path: reducer.path,
      args: cloneValue(reducer.args),
      idempotencyKey: reducer.idempotencyKey ?? createIdempotencyKey(),
      entityKeys: [...(reducer.entityKeys ?? [])],
      patches: reducer.patches?.map(clonePatch),
      ...(reducer.localExecution ? { localExecution: cloneValue(reducer.localExecution) } : {}),
      createdAt,
      attempts: 0,
      nextAttemptAt: createdAt,
      state: reducer.state ?? "pending",
    };
    if (reducer.localExecution) {
      if (this.memoryOnly) throw new Error("Durable storage is required for local reducer execution");
      await this.store.put(cloneEntry(entry));
    } else await this.persistPut(entry);
    this.entries.set(entry.id, entry);
    this.notify();
    return cloneEntry(entry);
  }

  async loadAll(scope: string): Promise<ReducerOutboxEntry[]> {
    await this.ready;
    const recovered: ReducerOutboxEntry[] = [];
    for (const [id, entry] of this.entries) {
      if (entry.scope !== scope || entry.state !== "inflight") continue;
      const pending = { ...entry, state: "pending" as const };
      this.entries.set(id, pending);
      recovered.push(pending);
    }
    await Promise.all(recovered.map((entry) => this.persistPut(entry)));
    if (recovered.length > 0) this.notify();
    return this.sortedEntries(scope).map(cloneEntry);
  }

  async list(scope: string): Promise<ReducerOutboxEntry[]> {
    await this.ready;
    return this.sortedEntries(scope).map(cloneEntry);
  }

  async updateLocal(id: number, patches: OptimisticPatch[], execution: LocalExecution): Promise<void> {
    await this.ready;
    if (this.memoryOnly) throw new Error("Durable storage is required for local reducer replay");
    const entry = this.entries.get(id);
    if (!entry) return;
    const updated = { ...entry, patches: patches.map(clonePatch), localExecution: cloneValue(execution) };
    await this.store.put(cloneEntry(updated));
    this.entries.set(id, updated);
  }

  async nextReady(scope: string, now: number): Promise<ReducerOutboxEntry | undefined> {
    await this.ready;
    return cloneOptionalEntry(firstReady(this.sortedEntries(scope), now));
  }

  async markInflight(id: number): Promise<void> {
    await this.ready;
    const entry = this.entries.get(id);
    if (!entry || entry.state === "inflight") return;
    const updated = { ...entry, state: "inflight" as const };
    this.entries.set(id, updated);
    await this.persistPut(updated);
    this.notify();
  }

  async markPending(id: number): Promise<void> {
    await this.ready;
    const entry = this.entries.get(id);
    if (!entry || entry.state === "pending") return;
    const updated: ReducerOutboxEntry = {
      ...entry,
      state: "pending",
      nextAttemptAt: Date.now(),
      lastError: undefined,
    };
    this.entries.set(id, updated);
    await this.persistPut(updated);
    this.notify();
  }

  async markCommitted(id: number): Promise<void> {
    await this.ready;
    const entry = this.entries.get(id);
    if (!entry || entry.state === "committed") return;
    const updated = { ...entry, state: "committed" as const };
    this.entries.set(id, updated);
    await this.persistPut(updated);
    this.notify();
  }

  async ack(id: number): Promise<void> {
    await this.ready;
    if (!this.entries.delete(id)) return;
    await this.persistDelete(id);
    this.notify();
  }

  async fail(id: number, error: string): Promise<void> {
    await this.ready;
    const entry = this.entries.get(id);
    if (!entry) return;
    const updated = failedEntry(entry, error, Date.now());
    this.entries.set(id, updated);
    await this.persistPut(updated);
    this.notify();
  }

  async count(scope: string): Promise<number> {
    await this.ready;
    return this.sortedEntries(scope).length;
  }

  async clear(scope: string): Promise<void> {
    await this.ready;
    // Delete by snapshotted id, never scope-wide, so an enqueue racing this
    // clear is not swallowed by a later store delete.
    const ids: number[] = [];
    for (const [id, entry] of this.entries) {
      if (entry.scope !== scope) continue;
      this.entries.delete(id);
      ids.push(id);
    }
    await Promise.all(ids.map((id) => this.persistDelete(id)));
    this.notify();
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private async hydrate() {
    try {
      for (const entry of await this.store.load()) {
        this.entries.set(entry.id, cloneEntry(entry));
        this.nextId = Math.max(this.nextId, entry.id + 1);
      }
    } catch (error) {
      if (this.store.strictPersistence) throw error;
      this.degradeToMemory();
    }
  }

  private async persistPut(entry: ReducerOutboxEntry) {
    if (this.memoryOnly) return;
    try {
      await this.store.put(cloneEntry(entry));
    } catch (error) {
      if (this.store.strictPersistence) throw error;
      this.degradeToMemory();
    }
  }

  private async persistDelete(id: number) {
    if (this.memoryOnly) return;
    try {
      await this.store.delete(id);
    } catch (error) {
      if (this.store.strictPersistence) throw error;
      this.degradeToMemory();
    }
  }

  private sortedEntries(scope: string) {
    return [...this.entries.values()]
      .filter((entry) => entry.scope === scope)
      .sort((left, right) => left.id - right.id);
  }

  private notify() {
    for (const listener of this.listeners) {
      try {
        listener();
      } catch {
        // A subscriber must not be able to break reducer delivery.
      }
    }
  }

  private degradeToMemory() {
    this.memoryOnly = true;
    try {
      this.store.close?.();
    } catch {
      // Losing the store only loses durability, never the queue.
    }
  }
}

export function createReducerOutbox(options: ReducerOutboxOptions = {}): ReducerOutbox {
  if (options.store) return new StoreReducerOutbox(options.store, { enabled: options.enabled });
  return new DexieReducerOutbox(options);
}

function firstReady(entries: ReducerOutboxEntry[], now: number) {
  const blockedEntityKeys = new Set<string>();
  for (const entry of entries) {
    // The runtime already accepted committed entries. They remain only to
    // protect stale cached projections until reconciliation and must not block
    // a newer pending write to the same entity.
    if (entry.state === "committed") continue;
    if (
      entry.state === "pending"
      && entry.nextAttemptAt <= now
      && !entry.entityKeys.some((key) => blockedEntityKeys.has(key))
    ) {
      return entry;
    }
    for (const key of entry.entityKeys) blockedEntityKeys.add(key);
  }
  return undefined;
}

function failedEntry(entry: ReducerOutboxEntry, error: string, now: number): ReducerOutboxEntry {
  const attempts = entry.attempts + 1;
  return {
    ...entry,
    attempts,
    state: "pending",
    nextAttemptAt: now + Math.min(30_000, 1_000 * (2 ** attempts)),
    lastError: error,
  };
}

function createIdempotencyKey() {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function cloneEntry(entry: ReducerOutboxEntry): ReducerOutboxEntry {
  return {
    ...entry,
    args: cloneValue(entry.args),
    entityKeys: [...entry.entityKeys],
    patches: entry.patches?.map(clonePatch),
    ...(entry.localExecution ? { localExecution: cloneValue(entry.localExecution) } : {}),
  };
}

function cloneOptionalEntry(entry: ReducerOutboxEntry | undefined) {
  return entry ? cloneEntry(entry) : undefined;
}

function cloneValue<T>(value: T): T {
  try {
    return globalThis.structuredClone(value);
  } catch {
    return value;
  }
}

function clonePatch(patch: OptimisticPatch): OptimisticPatch {
  if (patch.op === "delete") return { ...patch };
  return { ...patch, fields: cloneValue(patch.fields) };
}
