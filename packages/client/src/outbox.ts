import type { Dexie as DexieDatabase, Table } from "dexie";
import type { OptimisticPatch } from "./optimistic.js";
import type { LocalExecution } from "@gonvex/local-runtime";
import type { ReducerErrorClass } from "@gonvex/protocol";

export type { ReducerErrorClass };

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
  shared?: boolean;
  coordinate?<T>(scope: string, lane: 'intent' | 'delivery', run: () => Promise<T>): Promise<T>;
  subscribePeer?(listener: () => void): () => void;
  update?(id: number, change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry): Promise<ReducerOutboxEntry | undefined>;
  /** Version-fenced stores must never fall back to unpersisted sends. */
  strictPersistence?: boolean;
  /** Reserve globally unique sequence numbers when several tabs share a store. */
  allocateId?(): Promise<number>;
  /** Reserve the sequence and persist one complete local intent under one storage fence. */
  append?(entry: Omit<ReducerOutboxEntry, "id">): Promise<ReducerOutboxEntry>;
  /** Every persisted entry across all scopes; called once to hydrate. */
  load(scope?: string): Promise<ReducerOutboxEntry[]>;
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
  /** Failed deliveries that count toward the retry budget. */
  attempts: number;
  nextAttemptAt: number;
  lastError?: string;
  /** Classification of `lastError`; absent for entries written by older SDKs. */
  lastErrorClass?: OutboxErrorClass;
  /** When the entry entered its current `failed` or `rejected` state. */
  settledAt?: number;
  state: ReducerOutboxState;
};

/**
 * Lifecycle of one durable intent.
 *
 * - `pending`: waiting to be sent (possibly backing off after a failure).
 * - `inflight`: sent, waiting for the server's verdict.
 * - `committed`: the server accepted it; retained until reconciliation.
 * - `failed`: transient delivery failures exhausted the retry budget. The
 *   intent and its optimistic prediction are kept until the app retries or
 *   discards it. A failed entry no longer blocks later intents.
 * - `rejected`: the server permanently refused it. Its prediction was rolled
 *   back; the record stays so a UI can explain what happened until the app
 *   dismisses (discards) or retries it.
 */
export type ReducerOutboxState = "pending" | "inflight" | "committed" | "failed" | "rejected";

/** Server classification, plus `network` for a dropped connection with no verdict. */
export type OutboxErrorClass = ReducerErrorClass | "network";

export type OutboxFailureOptions = {
  errorClass?: OutboxErrorClass;
  /** False keeps `attempts` unchanged (connectivity loss, re-authentication). */
  countAttempt?: boolean;
  /** Park as `failed` once counted attempts reach this many. */
  maxAttempts?: number;
  /** Cap for exponential backoff. Default 30s. */
  maxBackoffMs?: number;
  /** Explicit retry delay instead of exponential backoff. */
  delayMs?: number;
};

export type ReducerOutboxScopeSummary = {
  scope: string;
  /** Every entry for the scope, including failed and rejected records. */
  count: number;
  oldestCreatedAt: number;
};

/** Entries whose optimistic prediction is still part of the local overlay. */
export function outboxEntryIsLive(entry: Pick<ReducerOutboxEntry, "state">): boolean {
  return entry.state !== "rejected";
}

/** Entries that still wait for delivery without user action. */
export function outboxEntryIsQueued(entry: Pick<ReducerOutboxEntry, "state">): boolean {
  return entry.state === "pending" || entry.state === "inflight" || entry.state === "committed";
}

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
  recoverInflight?(scope: string): Promise<void>;
  enqueue(reducer: EnqueueReducer): Promise<ReducerOutboxEntry>;
  loadAll(scope: string): Promise<ReducerOutboxEntry[]>;
  /** Observe current records without performing startup inflight recovery. */
  list(scope: string): Promise<ReducerOutboxEntry[]>;
  updateLocal(id: number, patches: OptimisticPatch[], execution: LocalExecution): Promise<void>;
  nextReady(scope: string, now: number): Promise<ReducerOutboxEntry | undefined>;
  /** Resolves true when the entry is inflight; failed/rejected/missing entries are never revived. */
  markInflight(id: number): Promise<boolean | void>;
  /** Return a just-admitted entry to pending without recording a failed attempt. */
  markPending(id: number): Promise<void>;
  markCommitted(id: number): Promise<void>;
  ack(id: number): Promise<void>;
  /** Record a failed delivery: back off, or park as `failed` once the budget is spent. */
  fail(id: number, error: string, options?: OutboxFailureOptions): Promise<ReducerOutboxEntry | undefined | void>;
  /** Record a permanent server rejection durably instead of deleting the entry. */
  reject(id: number, error: string): Promise<ReducerOutboxEntry | undefined>;
  /** Re-arm a failed or rejected entry with a fresh retry budget. */
  retry(id: number): Promise<ReducerOutboxEntry | undefined>;
  /** Atomically delete an entry only while it is in one of `states`. */
  discard(id: number, states: readonly ReducerOutboxState[]): Promise<ReducerOutboxEntry | undefined>;
  /** Owners with durable entries, including identities that never signed back in. */
  listScopes(): Promise<ReducerOutboxScopeSummary[]>;
  /** Entries still queued for delivery (excludes failed and rejected records). */
  count(scope: string): Promise<number>;
  clear(scope: string): Promise<void>;
  /** Delete every durable entry for a scope that is not currently active. */
  purgeScope(scope: string): Promise<number>;
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

  async markInflight(id: number): Promise<boolean> {
    const { before, after } = await this.transition(id, markInflightChange);
    return (after ?? before)?.state === "inflight";
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
        if (!entry || entry.state === "pending" || isParked(entry)) return;
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

  async fail(id: number, error: string, options: OutboxFailureOptions = {}): Promise<ReducerOutboxEntry | undefined> {
    const now = Date.now();
    const { after } = await this.transition(id, (entry) => failChange(entry, error, now, options));
    return after ?? undefined;
  }

  async reject(id: number, error: string): Promise<ReducerOutboxEntry | undefined> {
    const now = Date.now();
    const { after } = await this.transition(id, (entry) => rejectChange(entry, error, now));
    return after ?? undefined;
  }

  async retry(id: number): Promise<ReducerOutboxEntry | undefined> {
    const now = Date.now();
    const { after } = await this.transition(id, (entry) => retryChange(entry, now));
    return after ?? undefined;
  }

  async discard(id: number, states: readonly ReducerOutboxState[]): Promise<ReducerOutboxEntry | undefined> {
    const { before, after } = await this.transition(id, (entry) => (states.includes(entry.state) ? null : undefined));
    return after === null ? before : undefined;
  }

  async listScopes(): Promise<ReducerOutboxScopeSummary[]> {
    if (this.memoryOnly) return summarizeScopes(this.sortedMemoryEntries());
    try {
      return summarizeScopes(await (await this.open()).entries.toArray());
    } catch {
      this.degradeToMemory();
      return summarizeScopes(this.sortedMemoryEntries());
    }
  }

  async count(scope: string): Promise<number> {
    if (this.memoryOnly) return this.sortedMemoryEntries(scope).filter(outboxEntryIsQueued).length;
    try {
      return await (await this.open()).entries.where("scope").equals(scope).filter(outboxEntryIsQueued).count();
    } catch {
      this.degradeToMemory();
      return this.sortedMemoryEntries(scope).filter(outboxEntryIsQueued).length;
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

  async purgeScope(scope: string): Promise<number> {
    let removed = 0;
    for (const [id, entry] of this.memoryEntries) {
      if (entry.scope !== scope) continue;
      this.memoryEntries.delete(id);
      removed += 1;
    }
    if (!this.memoryOnly) {
      try {
        // Foreign scopes are never hydrated into memory, so delete by index.
        removed = Math.max(removed, await (await this.open()).entries.where("scope").equals(scope).delete());
      } catch {
        this.degradeToMemory();
      }
    }
    if (removed > 0) this.notify();
    return removed;
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

  private markPendingInMemory(id: number) {
    const entry = this.memoryEntries.get(id);
    if (!entry || entry.state === "pending" || isParked(entry)) return;
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

  /**
   * Apply one atomic read-modify-write. `change` returns the replacement,
   * `null` to delete the entry, or `undefined` to leave it untouched.
   */
  private async transition(id: number, change: EntryChange): Promise<TransitionResult> {
    if (this.memoryOnly) return this.transitionInMemory(id, change);
    try {
      const database = await this.open();
      let before: ReducerOutboxEntry | undefined;
      let after: ReducerOutboxEntry | null | undefined;
      await database.transaction("rw", database.entries, async () => {
        const entry = await database.entries.get(id);
        if (!entry) return;
        before = entry;
        after = change(cloneEntry(entry));
        if (after === null) await database.entries.delete(id);
        else if (after) await database.entries.put(after);
      });
      if (after === null) this.memoryEntries.delete(id);
      else if (after) this.remember(after);
      if (after !== undefined) this.notify();
      return { before: before && cloneEntry(before), after: after ? cloneEntry(after) : after };
    } catch {
      this.degradeToMemory();
      return this.transitionInMemory(id, change);
    }
  }

  private transitionInMemory(id: number, change: EntryChange): TransitionResult {
    const entry = this.memoryEntries.get(id);
    if (!entry) return {};
    const after = change(cloneEntry(entry));
    if (after === null) this.memoryEntries.delete(id);
    else if (after) this.memoryEntries.set(id, cloneEntry(after));
    if (after !== undefined) this.notify();
    return { before: cloneEntry(entry), after: after ? cloneEntry(after) : after };
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
    this.ready = this.memoryOnly || store.shared ? Promise.resolve() : this.hydrate();
  }

  async enqueue(reducer: EnqueueReducer): Promise<ReducerOutboxEntry> {
    await this.ready;
    const createdAt = Date.now();
    const draft: Omit<ReducerOutboxEntry, "id"> = {
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
    let entry: ReducerOutboxEntry;
    if (reducer.localExecution && this.memoryOnly) throw new Error("Durable storage is required for local reducer execution");
    if (reducer.localExecution && this.store.append) {
      entry = await this.store.append(draft);
    } else {
      entry = { ...draft, id: this.store.allocateId ? await this.store.allocateId() : this.nextId++ };
      if (reducer.localExecution) await this.store.put(cloneEntry(entry));
      else await this.persistPut(entry);
    }
    this.entries.set(entry.id, entry);
    this.notify();
    return cloneEntry(entry);
  }

  async loadAll(scope: string): Promise<ReducerOutboxEntry[]> {
    await this.ready;
    await this.refreshShared(scope);
    // Only the holder of the cross-tab delivery lock may recover inflight work.
    if (this.store.shared) return this.sortedEntries(scope).map(cloneEntry);
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
    await this.refreshShared(scope);
    return this.sortedEntries(scope).map(cloneEntry);
  }

  async updateLocal(id: number, patches: OptimisticPatch[], execution: LocalExecution): Promise<void> {
    await this.ready;
    if (this.memoryOnly) throw new Error("Durable storage is required for local reducer replay");
    if(this.store.update) { await this.updateShared(id,entry=>({...entry,patches:patches.map(clonePatch),localExecution:cloneValue(execution)}));return; }
    const entry = this.entries.get(id);
    if (!entry) return;
    const updated = { ...entry, patches: patches.map(clonePatch), localExecution: cloneValue(execution) };
    await this.store.put(cloneEntry(updated));
    this.entries.set(id, updated);
  }

  async nextReady(scope: string, now: number): Promise<ReducerOutboxEntry | undefined> {
    await this.ready;
    await this.refreshShared(scope);
    return cloneOptionalEntry(firstReady(this.sortedEntries(scope), now));
  }

  async markInflight(id: number): Promise<boolean> {
    await this.ready;
    const { before, after } = await this.transition(id, markInflightChange);
    return (after ?? before)?.state === "inflight";
  }

  async markPending(id: number): Promise<void> {
    await this.ready;
    if(this.store.update) { await this.updateShared(id,entry=>isParked(entry) ? entry : ({...entry,state:'pending',nextAttemptAt:Date.now(),lastError:undefined}));this.notify();return; }
    const entry = this.entries.get(id);
    if (!entry || entry.state === "pending" || isParked(entry)) return;
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
    if(this.store.update) { await this.updateShared(id,entry=>({...entry,state:'committed'}));this.notify();return; }
    const entry = this.entries.get(id);
    if (!entry || entry.state === "committed") return;
    const updated = { ...entry, state: "committed" as const };
    this.entries.set(id, updated);
    await this.persistPut(updated);
    this.notify();
  }

  async ack(id: number): Promise<void> {
    await this.ready;
    if (!this.entries.delete(id) && !this.store.shared) return;
    await this.persistDelete(id);
    this.notify();
  }

  async fail(id: number, error: string, options: OutboxFailureOptions = {}): Promise<ReducerOutboxEntry | undefined> {
    await this.ready;
    const now = Date.now();
    const { after } = await this.transition(id, (entry) => failChange(entry, error, now, options));
    return after ?? undefined;
  }

  async reject(id: number, error: string): Promise<ReducerOutboxEntry | undefined> {
    await this.ready;
    const now = Date.now();
    const { after } = await this.transition(id, (entry) => rejectChange(entry, error, now));
    return after ?? undefined;
  }

  async retry(id: number): Promise<ReducerOutboxEntry | undefined> {
    await this.ready;
    const now = Date.now();
    const { after } = await this.transition(id, (entry) => retryChange(entry, now));
    return after ?? undefined;
  }

  async discard(id: number, states: readonly ReducerOutboxState[]): Promise<ReducerOutboxEntry | undefined> {
    await this.ready;
    const { before, after } = await this.transition(id, (entry) => (states.includes(entry.state) ? null : undefined));
    return after === null ? before : undefined;
  }

  async listScopes(): Promise<ReducerOutboxScopeSummary[]> {
    await this.ready;
    if (this.store.shared && !this.memoryOnly) return summarizeScopes(await this.store.load());
    return summarizeScopes([...this.entries.values()]);
  }

  async count(scope: string): Promise<number> {
    await this.ready;
    await this.refreshShared(scope);
    return this.sortedEntries(scope).filter(outboxEntryIsQueued).length;
  }

  async clear(scope: string): Promise<void> {
    await this.ready;
    await this.refreshShared(scope);
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

  async purgeScope(scope: string): Promise<number> {
    await this.ready;
    const persisted = this.store.shared && !this.memoryOnly ? await this.store.load(scope) : [];
    const ids = new Set(persisted.filter((entry) => entry.scope === scope).map((entry) => entry.id));
    for (const [id, entry] of this.entries) {
      if (entry.scope !== scope) continue;
      this.entries.delete(id);
      ids.add(id);
    }
    if (!this.memoryOnly) {
      try {
        await this.store.clear(scope);
      } catch (error) {
        if (this.store.strictPersistence) throw error;
        this.degradeToMemory();
      }
    }
    if (ids.size > 0) this.notify();
    return ids.size;
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async recoverInflight(scope:string):Promise<void> {
    await this.ready;await this.refreshShared(scope);
    for(const entry of this.sortedEntries(scope)) if(entry.state==='inflight') await this.markPending(entry.id);
  }

  private async refreshShared(scope:string) {
    if(!this.store.shared || this.memoryOnly) return;
    const entries=await this.store.load(scope);
    this.entries.clear();
    for(const entry of entries) this.entries.set(entry.id,cloneEntry(entry));
  }

  /**
   * Apply one atomic read-modify-write. `change` returns the replacement,
   * `null` to delete the entry, or `undefined` to leave it untouched.
   * Shared stores run the decision inside their own transaction so another
   * tab's delivery cannot interleave; a deletion first fences the entry as
   * `rejected` (never sendable), then removes it.
   */
  private async transition(id: number, change: EntryChange): Promise<TransitionResult> {
    if (this.store.update && !this.memoryOnly) {
      let before: ReducerOutboxEntry | undefined;
      let decided: ReducerOutboxEntry | null | undefined;
      const stored = await this.store.update(id, (entry) => {
        before = cloneEntry(entry);
        decided = change(cloneEntry(entry));
        if (decided === undefined) return entry;
        if (decided === null) return { ...entry, state: "rejected", lastError: entry.lastError ?? "Discarded" };
        return decided;
      });
      if (decided === null) {
        this.entries.delete(id);
        await this.persistDelete(id);
      } else if (stored) this.entries.set(id, cloneEntry(stored));
      else this.entries.delete(id);
      if (decided !== undefined) this.notify();
      return { before, after: decided === null ? null : decided === undefined ? undefined : stored && cloneEntry(stored) };
    }
    const entry = this.entries.get(id);
    if (!entry) return {};
    const after = change(cloneEntry(entry));
    if (after === null) {
      this.entries.delete(id);
      await this.persistDelete(id);
    } else if (after) {
      this.entries.set(id, after);
      await this.persistPut(after);
    }
    if (after !== undefined) this.notify();
    return { before: cloneEntry(entry), after: after ? cloneEntry(after) : after };
  }

  private async updateShared(id:number,change:(entry:ReducerOutboxEntry)=>ReducerOutboxEntry) {
    const entry=await this.store.update!(id,change);
    if(entry) this.entries.set(id,cloneEntry(entry));else this.entries.delete(id);
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

type EntryChange = (entry: ReducerOutboxEntry) => ReducerOutboxEntry | null | undefined;
type TransitionResult = { before?: ReducerOutboxEntry; after?: ReducerOutboxEntry | null };

function firstReady(entries: ReducerOutboxEntry[], now: number) {
  const blockedEntityKeys = new Set<string>();
  for (const entry of entries) {
    // The runtime already accepted committed entries. They remain only to
    // protect stale cached projections until reconciliation and must not block
    // a newer pending write to the same entity.
    if (entry.state === "committed") continue;
    // Parked (failed) and rejected records wait for the app, not the server.
    // They must not hold every later intent hostage. Later intents that truly
    // depended on them are validated by the server and rejected on their own.
    if (entry.state === "failed" || entry.state === "rejected") continue;
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

export const DEFAULT_OUTBOX_MAX_BACKOFF_MS = 30_000;

/** Exponential backoff after `attempts` counted failures. */
export function outboxBackoffMs(attempts: number, maxBackoffMs = DEFAULT_OUTBOX_MAX_BACKOFF_MS) {
  return Math.min(maxBackoffMs, 1_000 * (2 ** Math.min(attempts, 30)));
}

function failedEntry(entry: ReducerOutboxEntry, error: string, now: number, options: OutboxFailureOptions = {}): ReducerOutboxEntry {
  const counted = options.countAttempt !== false;
  const attempts = counted ? entry.attempts + 1 : entry.attempts;
  const park = counted
    && options.maxAttempts !== undefined
    && attempts >= options.maxAttempts;
  return {
    ...entry,
    attempts,
    state: park ? "failed" : "pending",
    nextAttemptAt: now + (options.delayMs ?? outboxBackoffMs(attempts, options.maxBackoffMs)),
    lastError: error,
    ...(options.errorClass ? { lastErrorClass: options.errorClass } : {}),
    ...(park ? { settledAt: now } : {}),
  };
}

/** Failed and rejected records leave their state only through retry(). */
function isParked(entry: Pick<ReducerOutboxEntry, "state">) {
  return entry.state === "failed" || entry.state === "rejected";
}

function markInflightChange(entry: ReducerOutboxEntry): ReducerOutboxEntry | undefined {
  // A parked or rejected record only leaves its state through retry().
  if (entry.state === "inflight" || entry.state === "failed" || entry.state === "rejected") return undefined;
  return { ...entry, state: "inflight" };
}

function failChange(entry: ReducerOutboxEntry, error: string, now: number, options: OutboxFailureOptions) {
  // A concurrent discard/reject/commit already decided this entry's fate.
  if (isParked(entry) || entry.state === "committed") return undefined;
  return failedEntry(entry, error, now, options);
}

function rejectChange(entry: ReducerOutboxEntry, error: string, now: number): ReducerOutboxEntry {
  return {
    ...entry,
    state: "rejected",
    lastError: error,
    lastErrorClass: "rejected",
    nextAttemptAt: now,
    settledAt: now,
  };
}

function retryChange(entry: ReducerOutboxEntry, now: number): ReducerOutboxEntry | undefined {
  if (entry.state !== "failed" && entry.state !== "rejected") return undefined;
  const { lastError: _error, lastErrorClass: _class, settledAt: _settled, ...rest } = entry;
  // Same id, same idempotency key: the server replays instead of re-applying
  // if an earlier attempt did commit before its response was lost.
  return { ...rest, state: "pending", attempts: 0, nextAttemptAt: now };
}

function summarizeScopes(entries: Iterable<ReducerOutboxEntry>): ReducerOutboxScopeSummary[] {
  const scopes = new Map<string, ReducerOutboxScopeSummary>();
  for (const entry of entries) {
    if (typeof entry.scope !== "string") continue;
    const summary = scopes.get(entry.scope);
    if (summary) {
      summary.count += 1;
      summary.oldestCreatedAt = Math.min(summary.oldestCreatedAt, entry.createdAt);
    } else scopes.set(entry.scope, { scope: entry.scope, count: 1, oldestCreatedAt: entry.createdAt });
  }
  return [...scopes.values()].sort((left, right) => left.oldestCreatedAt - right.oldestCreatedAt);
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
