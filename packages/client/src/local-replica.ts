import type { JsonValue, ReplicaCursor, SubscriptionRevision } from "@gonvex/protocol";
import type { OptimisticPatch } from "./optimistic.js";

export type ReplicaRow = Record<string, JsonValue>;

export type ReplicaChange = {
  entity: string;
  id: string;
  operation: "insert" | "update" | "delete";
  oldValue?: ReplicaRow;
  newValue?: ReplicaRow;
  changedColumns?: string[];
};

export type ReplicaWindow = {
  signature: string;
  kind: "live" | "replica";
  entity: string;
  key: string;
  ids: string[];
  cursor?: ReplicaCursor;
  completeness: "complete" | "partial";
  source: "server" | "cache";
  resultSkeleton?: JsonValue;
  resultPath?: string[];
  scalar?: JsonValue;
  windowRevision?: string;
  subscriptionRevision?: SubscriptionRevision;
  mode?: "eager" | "progressive";
  truncated?: boolean;
  orderBy?: string;
  orderDirection?: "asc" | "desc";
  maxRows?: number;
  maxBytes?: number;
  hashes?: Record<string, string>;
};

export type ReplicaTransaction = {
  cursor: ReplicaCursor;
  originCommandId?: string;
  changes: ReplicaChange[];
  memberships?: ReplicaWindow[];
};

export type ReplicaSnapshot = {
  cursor?: ReplicaCursor;
  entities: Record<string, Record<string, ReplicaRow>>;
  liveQueries: Record<string, ReplicaWindow>;
};

/** Opaque persistence namespace for one deployment/project/tenant identity. */
export type ReplicaScope = string;

const defaultReplicaScope: ReplicaScope = "default";

/**
 * Storage implementations must persist the complete transaction atomically.
 * The SQLite adapter maps this call to BEGIN/apply/cursor/COMMIT; IndexedDB
 * implementations use one readwrite transaction over the same stores.
 */
export interface LocalReplicaStorage {
  load(scope?: ReplicaScope): Promise<ReplicaSnapshot | undefined>;
  applyTransaction(transaction: ReplicaTransaction, snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void>;
  /** Persist a normalized Query/Collection materialization atomically. */
  replaceSnapshot?(snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void>;
  replaceWindow?(window: ReplicaWindow, snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void>;
  applyWindowDelta?(
    window: ReplicaWindow,
    delta: { upserts: ReplicaRow[]; deleted: string[] },
    snapshot: ReplicaSnapshot,
    scope?: ReplicaScope,
  ): Promise<void>;
  removeWindow?(signature: string, snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void>;
  clear?(scope?: ReplicaScope): Promise<void>;
}

type PendingCommand = {
  commandId: string;
  patches: OptimisticPatch[];
  committedRevision?: number;
};

export type ReplicaFreshness = "current" | "verifying" | "offline";

export type LiveQueryResult<T extends ReplicaRow = ReplicaRow> = {
  rows: T[];
  /** Ordered normalized entity IDs owned by the retained query window. */
  ids: string[];
  total?: number;
  offset?: number;
  limit?: number;
  source: "server" | "cache";
  completeness: "complete" | "partial";
  freshness: ReplicaFreshness;
  supported?: boolean;
  unsupportedOperator?: string;
};

export type ReplicaCollectionState<T extends ReplicaRow = ReplicaRow> = LiveQueryResult<T> & {
  truncated: boolean;
  computedRevision: number;
};

/**
 * Read-only view of the normalized Local Replica exposed to application code.
 *
 * Mutations are deliberately absent from this interface. The Gonvex client
 * applies committed transactions, optimistic reducer effects, scope changes,
 * and cache/window updates internally so application code cannot create a
 * second state-management path or advance the local replica by hand.
 */
export interface LocalReplicaView {
  cursor(): ReplicaCursor | undefined;
  freshness(): ReplicaFreshness;
  version(): number;
  subscribe(listener: () => void): () => void;
  hasPendingCommand(commandId: string): boolean;
  getWindow(signature: string): ReplicaWindow | undefined;
  listWindows(): ReplicaWindow[];
  windowRows<T extends ReplicaRow = ReplicaRow>(signature: string): T[];
  entity<T extends ReplicaRow = ReplicaRow>(entity: string, id: string): T | undefined;
  entityBatch<T extends ReplicaRow = ReplicaRow>(entity: string, ids: readonly string[]): Array<T | undefined>;
  entityRows<T extends ReplicaRow = ReplicaRow>(entity: string): T[];
  entityCompleteness(entity: string): "complete" | "partial";
  liveQuery<T extends ReplicaRow = ReplicaRow>(signature: string): LiveQueryResult<T>;
  collectionState<T extends ReplicaRow = ReplicaRow>(signature: string): ReplicaCollectionState<T>;
  hasLiveQuery(signature: string): boolean;
  snapshot(): ReplicaSnapshot;
}

export class LocalReplica implements LocalReplicaView {
  private cursorValue?: ReplicaCursor;
  private entities = new Map<string, Map<string, ReplicaRow>>();
  private liveQueries = new Map<string, ReplicaWindow>();
  /** Rows introduced by a materialized window may be reclaimed conservatively. */
  private readonly windowOwned = new Map<string, Map<string, Set<string>>>();
  private pendingCommands = new Map<string, PendingCommand>();
  private listeners = new Set<() => void>();
  private persistence = Promise.resolve();
  private application = Promise.resolve();
  private hydration?: Promise<void>;
  private freshnessValue: ReplicaFreshness = "verifying";
  private versionValue = 0;
  private scopeValue: ReplicaScope = defaultReplicaScope;
  // The default scope starts as an immediately usable empty in-memory store.
  // Persistence is restored only by hydrate()/activateScope(), so direct
  // callers can still materialize before an async adapter is consulted.
  private scopeLoaded = true;
  private scopeActivation?: Promise<void>;
  private scopeActivationName?: ReplicaScope;
  private scopeActivationGeneration = 0;

  constructor(private readonly storage?: LocalReplicaStorage) {}

  hydrate(): Promise<void> {
    if (this.hydration) return this.hydration;
    this.hydration = this.activateScope(defaultReplicaScope, true);
    return this.hydration;
  }

  /**
   * Atomically switch the authoritative store to an opaque persistence scope.
   * A newer request supersedes an older one even if the older storage read is
   * slow (or resolves after auth has already changed again).
   */
  activateScope(scope: ReplicaScope = defaultReplicaScope, forceReload = false): Promise<void> {
    const nextScope = normalizeReplicaScope(scope);
    if (!forceReload && this.scopeLoaded && this.scopeValue === nextScope) return this.application;
    if (this.scopeActivation && this.scopeActivationName === nextScope) return this.scopeActivation;
    // Fail closed immediately while the async read is in flight. This keeps a
    // synchronous auth change from exposing the prior identity's rows.
    const wasLoaded = this.scopeLoaded;
    this.scopeLoaded = false;
    if (wasLoaded) this.notify();
    const generation = ++this.scopeActivationGeneration;
    const activation = this.application.then(async () => {
      // A prior transaction may still be in the storage queue. Wait for it so
      // a same-scope activation cannot load a snapshot from before its commit.
      await this.persistence;
      const snapshot = await this.storage?.load(nextScope);
      if (generation !== this.scopeActivationGeneration) return;
      this.scopeValue = nextScope;
      this.scopeLoaded = true;
      this.cursorValue = snapshot?.cursor;
      this.entities = snapshot ? entitiesFromSnapshot(snapshot.entities) : new Map();
      this.liveQueries = snapshot
        ? new Map(Object.entries(snapshot.liveQueries).map(([key, value]) => [key, normalizeWindow(value)]))
        : new Map();
      this.windowOwned.clear();
      // Optimistic commands belong to the old identity and must never be
      // projected while the newly restored scope is becoming authoritative.
      this.pendingCommands.clear();
      this.freshnessValue = "verifying";
      this.notify();
    });
    this.application = activation.catch(() => undefined);
    this.scopeActivation = activation;
    this.scopeActivationName = nextScope;
    return activation;
  }

  cursor() {
    return this.scopeLoaded && this.cursorValue ? { ...this.cursorValue } : undefined;
  }

  freshness() {
    return this.freshnessValue;
  }

  version() { return this.versionValue; }

  setFreshness(freshness: ReplicaFreshness) {
    if (freshness === this.freshnessValue) return;
    this.freshnessValue = freshness;
    this.notify();
  }

  subscribe(listener: () => void) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  applyOptimistic(commandId: string, patches: OptimisticPatch[]) {
    commandId = commandId.trim();
    if (!commandId) throw new Error("optimistic commandId is required");
    this.pendingCommands.set(commandId, { commandId, patches: patches.map(cloneOptimisticPatch) });
    this.notify();
  }

  acknowledgeCommand(commandId: string, committedRevision?: number) {
    const pending = this.pendingCommands.get(commandId);
    if (!pending) return;
    if (!committedRevision) return;
    pending.committedRevision = committedRevision;
    this.reconcileCommands();
  }

  rejectCommand(commandId: string) {
    if (!this.pendingCommands.delete(commandId)) return;
    this.notify();
  }

  /** True while an optimistic command still contributes visible patches. */
  hasPendingCommand(commandId: string): boolean {
    return this.pendingCommands.has(commandId);
  }

  applyTransaction(transaction: ReplicaTransaction, scope?: ReplicaScope): Promise<void> {
    const requestedScope = scope === undefined ? undefined : normalizeReplicaScope(scope);
    const application = this.application.then(() => this.applyTransactionNow(transaction, requestedScope));
    this.application = application.catch(() => undefined);
    return application;
  }

  materializeWindow(input: {
    signature: string;
    kind?: "live" | "replica";
    entity: string;
    key: string;
    rows: ReplicaRow[];
    completeness: "complete" | "partial";
    source: "server" | "cache";
    cursor?: ReplicaCursor;
    resultSkeleton?: JsonValue;
    resultPath?: string[];
    scalar?: JsonValue;
    windowRevision?: string;
    subscriptionRevision?: SubscriptionRevision;
    mode?: "eager" | "progressive";
    truncated?: boolean;
    orderBy?: string;
    orderDirection?: "asc" | "desc";
    maxRows?: number;
    maxBytes?: number;
    hashes?: Record<string, string>;
    removedIDs?: string[];
    scope?: ReplicaScope;
  }): Promise<void> {
    const application = this.application.then(() => this.materializeWindowNow(input));
    this.application = application.catch(() => undefined);
    return application;
  }

  /** Replace one server/cache window while retaining shared normalized entities. */
  replaceWindow(input: Parameters<LocalReplica["materializeWindow"]>[0]): Promise<void> {
    return this.materializeWindow(input);
  }

  /** Apply a bounded window delta and persist it in the same transaction. */
  applyWindowDelta(input: {
    signature: string;
    kind?: "live" | "replica";
    entity: string;
    key: string;
    upserts: ReplicaRow[];
    deleted: string[];
    completeness?: "complete" | "partial";
    source?: "server" | "cache";
    cursor?: ReplicaCursor;
    resultSkeleton?: JsonValue;
    resultPath?: string[];
    scalar?: JsonValue;
    windowRevision?: string;
    subscriptionRevision?: SubscriptionRevision;
    mode?: "eager" | "progressive";
    truncated?: boolean;
    orderBy?: string;
    orderDirection?: "asc" | "desc";
    maxRows?: number;
    maxBytes?: number;
    hashes?: Record<string, string>;
    removedIDs?: string[];
    scope?: ReplicaScope;
  }): Promise<void> {
    const application = this.application.then(async () => {
      const existing = this.liveQueries.get(input.signature);
      const deleted = new Set(input.deleted.map(String));
      const ids = (existing?.ids ?? []).filter((id) => !deleted.has(id));
      for (const row of input.upserts) {
        const rawID = row[input.key];
        const id = typeof rawID === "string" || typeof rawID === "number" ? String(rawID) : "";
        if (id && !ids.includes(id)) ids.push(id);
      }
      const rows = ids
        .map((id) => input.upserts.find((row) => String(row[input.key]) === id) ?? this.entities.get(input.entity)?.get(id))
        .filter((row): row is ReplicaRow => row !== undefined)
        .map(cloneRow);
      await this.materializeWindowNow({
        ...input,
        rows,
        completeness: input.completeness ?? existing?.completeness ?? "partial",
        source: input.source ?? existing?.source ?? "server",
        removedIDs: [...deleted],
      });
    });
    this.application = application.catch(() => undefined);
    return application;
  }

  getWindow(signature: string): ReplicaWindow | undefined {
    const window = this.liveQueries.get(signature);
    return window ? cloneWindow(window) : undefined;
  }

  listWindows(): ReplicaWindow[] {
    return [...this.liveQueries.values()].map(cloneWindow);
  }

  windowRows<T extends ReplicaRow = ReplicaRow>(signature: string): T[] {
    return this.liveQuery<T>(signature).rows;
  }

  removeWindow(signature: string, scope?: ReplicaScope): Promise<void> {
    const application = this.application.then(async () => {
      if (scope !== undefined && normalizeReplicaScope(scope) !== this.scopeValue) return;
      if (!this.liveQueries.has(signature)) return;
      const nextQueries = new Map(this.liveQueries);
      nextQueries.delete(signature);
      const snapshot = snapshotFrom(this.cursorValue, this.entities, nextQueries);
      if (this.storage?.removeWindow) {
        await this.persist(() => this.storage!.removeWindow!(signature, snapshot, this.scopeValue));
      } else if (this.storage?.replaceSnapshot) {
        await this.persist(() => this.storage!.replaceSnapshot!(snapshot, this.scopeValue));
      }
      this.liveQueries = nextQueries;
      this.pruneOwnedEntitiesAfterRemoval(signature, nextQueries);
      this.notify();
    });
    this.application = application.catch(() => undefined);
    return application;
  }

  clear(scope?: ReplicaScope): Promise<void> {
    const application = this.application.then(async () => {
      if (scope !== undefined && normalizeReplicaScope(scope) !== this.scopeValue) return;
      if (this.storage?.clear) await this.persist(() => this.storage!.clear!(this.scopeValue));
      this.cursorValue = undefined;
      this.entities.clear();
      this.liveQueries.clear();
      this.windowOwned.clear();
      this.pendingCommands.clear();
      this.freshnessValue = "verifying";
      this.notify();
    });
    this.application = application.catch(() => undefined);
    return application;
  }

  private async materializeWindowNow(input: {
    signature: string;
    kind?: "live" | "replica";
    entity: string;
    key: string;
    rows: ReplicaRow[];
    completeness: "complete" | "partial";
    source: "server" | "cache";
    cursor?: ReplicaCursor;
    resultSkeleton?: JsonValue;
    resultPath?: string[];
    scalar?: JsonValue;
    windowRevision?: string;
    subscriptionRevision?: SubscriptionRevision;
    mode?: "eager" | "progressive";
    truncated?: boolean;
    orderBy?: string;
    orderDirection?: "asc" | "desc";
    maxRows?: number;
    maxBytes?: number;
    hashes?: Record<string, string>;
    removedIDs?: string[];
    scope?: ReplicaScope;
  }) {
    if (input.scope !== undefined && normalizeReplicaScope(input.scope) !== this.scopeValue) return;
    if (!input.signature.trim() || !input.entity.trim() || !input.key.trim()) {
      throw new Error("replica materialization requires signature, entity, and key");
    }
    const nextEntities = cloneEntities(this.entities);
    // Clone memberships before changing them. Persistence may be asynchronous,
    // and readers must continue to observe the prior complete version until the
    // transaction commits locally and the single state swap below runs.
    const nextQueries = new Map(
      [...this.liveQueries.entries()].map(([signature, window]) => [signature, cloneWindow(window)]),
    );
    if (input.cursor && this.cursorValue && input.cursor.epoch !== this.cursorValue.epoch) {
      nextEntities.clear();
      nextQueries.clear();
    }
    const entityRows = nextEntities.get(input.entity) ?? new Map<string, ReplicaRow>();
    nextEntities.set(input.entity, entityRows);
    const ids: string[] = [];
    for (const row of input.rows) {
      const rawID = row[input.key];
      const id = typeof rawID === "string" || typeof rawID === "number" ? String(rawID) : "";
      if (!id) continue;
      ids.push(id);
      entityRows.set(id, cloneRow(row));
    }
    const previous = nextQueries.get(input.signature);
    const window: ReplicaWindow = {
      signature: input.signature,
      kind: input.kind ?? "live",
      entity: input.entity,
      key: input.key,
      ids,
      cursor: input.cursor ? { ...input.cursor } : undefined,
      completeness: input.completeness,
      source: input.source,
      resultSkeleton: input.resultSkeleton === undefined ? undefined : structuredClone(input.resultSkeleton),
      resultPath: input.resultPath ? [...input.resultPath] : undefined,
      scalar: input.scalar === undefined ? undefined : structuredClone(input.scalar),
      windowRevision: input.windowRevision,
      subscriptionRevision: input.subscriptionRevision ? { ...input.subscriptionRevision } : undefined,
      mode: input.mode,
      truncated: input.truncated,
      orderBy: input.orderBy,
      orderDirection: input.orderDirection,
      maxRows: input.maxRows,
      maxBytes: input.maxBytes,
      hashes: input.hashes ? { ...input.hashes } : undefined,
    };
    nextQueries.set(input.signature, window);
    for (const id of input.removedIDs ?? []) {
      const stillReferenced = [...nextQueries.values()].some((candidate) => candidate.ids.includes(id));
      if (!stillReferenced) nextEntities.get(input.entity)?.delete(id);
    }
    this.trackWindowOwnership(window, input.rows, previous);
    let nextCursor = this.cursorValue;
    if (input.cursor && (!nextCursor || input.cursor.epoch !== nextCursor.epoch || input.cursor.revision > nextCursor.revision)) {
      nextCursor = { ...input.cursor };
    }
    const snapshot = snapshotFrom(nextCursor, nextEntities, nextQueries);
    const writeScope = this.scopeValue;
    if (this.storage?.replaceWindow) {
      await this.persist(() => this.storage!.replaceWindow!(window, snapshot, writeScope));
    } else if (this.storage?.replaceSnapshot) {
      await this.persist(() => this.storage!.replaceSnapshot!(snapshot, writeScope));
    }
    this.entities = nextEntities;
    this.liveQueries = nextQueries;
    this.pruneOwnedEntities(input.signature);
    this.cursorValue = nextCursor;
    if (input.source === "server") this.freshnessValue = "current";
    this.notify();
  }

  private async applyTransactionNow(transaction: ReplicaTransaction, scope?: ReplicaScope) {
    if (scope !== undefined && scope !== this.scopeValue) return;
    validateTransaction(transaction);
    if (this.cursorValue?.epoch === transaction.cursor.epoch && transaction.cursor.revision <= this.cursorValue.revision) {
      return;
    }

    const nextEntities = cloneEntities(this.entities);
    // A transaction is invisible until its storage write succeeds. Deep-clone
    // memberships because row deletion edits their ordered ID arrays.
    const nextQueries = new Map(
      [...this.liveQueries.entries()].map(([signature, window]) => [signature, cloneWindow(window)]),
    );
    if (this.cursorValue && this.cursorValue.epoch !== transaction.cursor.epoch) {
      nextEntities.clear();
      nextQueries.clear();
      this.windowOwned.clear();
    }
    for (const change of transaction.changes) {
      const rows = nextEntities.get(change.entity) ?? new Map<string, ReplicaRow>();
      nextEntities.set(change.entity, rows);
      if (change.operation === "delete") {
        rows.delete(change.id);
        for (const window of nextQueries.values()) {
          if (window.ids.includes(change.id)) {
            window.ids = window.ids.filter((id) => id !== change.id);
          }
        }
      }
      else if (change.newValue) rows.set(change.id, cloneRow(change.newValue));
    }
    for (const membership of transaction.memberships ?? []) {
      nextQueries.set(membership.signature, normalizeWindow(membership));
    }

    const snapshot = snapshotFrom(transaction.cursor, nextEntities, nextQueries);
    const writeScope = this.scopeValue;
    await this.persist(() => this.storage?.applyTransaction(transaction, snapshot, writeScope));

    // Publish the whole committed transaction in one state swap and notify UI
    // exactly once. No subscriber can observe a partial entity/query update.
    this.entities = nextEntities;
    this.liveQueries = nextQueries;
    this.cursorValue = { ...transaction.cursor };
    this.freshnessValue = "current";
    if (transaction.originCommandId) this.pendingCommands.delete(transaction.originCommandId);
    this.reconcileCommands(false);
    this.notify();
  }

  entity<T extends ReplicaRow = ReplicaRow>(entity: string, id: string): T | undefined {
    if (!this.scopeLoaded) return undefined;
    let row = this.entities.get(entity)?.get(id);
    let selected = row ? cloneRow(row) : undefined;
    for (const command of this.pendingCommands.values()) {
      for (const patch of command.patches) {
        if ((patch.entity ?? patch.collection) !== entity || patch.rowId !== id) continue;
        if (patch.op === "delete") selected = undefined;
        if (patch.op === "insert") selected = cloneRow(patch.fields as ReplicaRow);
        if (patch.op === "upsert") selected = cloneRow(patch.fields as ReplicaRow);
        if (patch.op === "patch") selected = { ...(selected ?? {}), ...(patch.fields as ReplicaRow) };
      }
    }
    return selected as T | undefined;
  }

  /** Resolve several IDs from one atomic Local Replica version. */
  entityBatch<T extends ReplicaRow = ReplicaRow>(entity: string, ids: readonly string[]): Array<T | undefined> {
    return ids.map((id) => this.entity<T>(entity, id));
  }

  /** All cached rows for one normalized entity, including optimistic overlays. */
  entityRows<T extends ReplicaRow = ReplicaRow>(entity: string): T[] {
    if (!this.scopeLoaded) return [];
    const ids = new Set(this.entities.get(entity)?.keys() ?? []);
    for (const command of this.pendingCommands.values()) {
      for (const patch of command.patches) {
        if ((patch.entity ?? patch.collection) === entity) ids.add(patch.rowId);
      }
    }
    return [...ids]
      .map((id) => this.entity<T>(entity, id))
      .filter((row): row is T => row !== undefined);
  }

  /** Exact only when an authoritative, non-truncated Replica Collection covers the entity. */
  entityCompleteness(entity: string): "complete" | "partial" {
    if (!this.scopeLoaded) return "partial";
    return [...this.liveQueries.values()].some((window) => (
      window.kind === "replica"
      && window.entity === entity
      && window.completeness === "complete"
      && window.truncated !== true
    )) ? "complete" : "partial";
  }

  liveQuery<T extends ReplicaRow = ReplicaRow>(signature: string): LiveQueryResult<T> {
    if (!this.scopeLoaded) {
      return { rows: [], ids: [], source: "cache", completeness: "partial", freshness: this.freshnessValue };
    }
    const membership = this.liveQueries.get(signature);
    if (!membership) {
      return { rows: [], ids: [], source: "cache", completeness: "partial", freshness: this.freshnessValue };
    }
    const rows = membership.ids
      .map((id) => this.entity<T>(membership.entity, id))
      .filter((row): row is T => row !== undefined);
    const metadata = windowResultMetadata(membership.resultSkeleton, membership.resultPath);
    return {
      rows,
      ids: [...membership.ids],
      ...metadata,
      source: this.freshnessValue === "current" ? membership.source : "cache",
      completeness: membership.completeness,
      freshness: this.freshnessValue,
    };
  }

  /** Rows and protocol-owned completeness for one Replica Collection window. */
  collectionState<T extends ReplicaRow = ReplicaRow>(signature: string): ReplicaCollectionState<T> {
    const result = this.liveQuery<T>(signature);
    const window = this.scopeLoaded ? this.liveQueries.get(signature) : undefined;
    return {
      ...result,
      truncated: window?.truncated === true,
      computedRevision: window?.cursor?.revision ?? window?.subscriptionRevision?.sequence ?? 0,
    };
  }

  hasLiveQuery(signature: string) {
    return this.scopeLoaded && this.liveQueries.has(signature);
  }

  snapshot(): ReplicaSnapshot {
    if (!this.scopeLoaded) return { entities: {}, liveQueries: {} };
    return snapshotFrom(this.cursorValue, this.entities, this.liveQueries);
  }

  private reconcileCommands(notify = true) {
    const revision = this.cursorValue?.revision ?? 0;
    let changed = false;
    for (const [commandId, command] of this.pendingCommands) {
      if (command.committedRevision && revision >= command.committedRevision) {
        this.pendingCommands.delete(commandId);
        changed = true;
      }
    }
    if (changed && notify) this.notify();
  }

  private persist(operation: () => Promise<void> | undefined) {
    const attempt = this.persistence.then(async () => { await operation(); });
    this.persistence = attempt.catch(() => undefined);
    return attempt;
  }

  private notify() {
    this.versionValue += 1;
    for (const listener of [...this.listeners]) listener();
  }

  private trackWindowOwnership(window: ReplicaWindow, rows: ReplicaRow[], previous?: ReplicaWindow) {
    const owned = this.windowOwned.get(window.signature) ?? new Map<string, Set<string>>();
    const oldIDs = new Set(previous?.ids ?? []);
    const newIDs = new Set(window.ids);
    for (const id of oldIDs) {
      if (!newIDs.has(id)) owned.delete(id);
    }
    for (const row of rows) {
      const rawID = row[window.key];
      const id = typeof rawID === "string" || typeof rawID === "number" ? String(rawID) : "";
      if (!id) continue;
      const owners = owned.get(id) ?? new Set<string>();
      owners.add(window.signature);
      owned.set(id, owners);
    }
    this.windowOwned.set(window.signature, owned);
  }

  private pruneOwnedEntities(signature: string) {
    const owned = this.windowOwned.get(signature);
    if (!owned) return;
    const sourceWindow = this.liveQueries.get(signature);
    for (const [id] of owned) {
      const stillReferenced = [...this.liveQueries.values()].some((window) => (
        window.entity === sourceWindow?.entity && window.ids.includes(id)
      ));
      if (!stillReferenced && sourceWindow) {
        this.entities.get(sourceWindow.entity)?.delete(id);
      }
    }
    this.windowOwned.delete(signature);
  }

  private pruneOwnedEntitiesAfterRemoval(signature: string, remaining: Map<string, ReplicaWindow>) {
    const owned = this.windowOwned.get(signature);
    if (!owned) return;
    const removed = [...owned.keys()];
    for (const id of removed) {
      const stillReferenced = [...remaining.values()].some((window) => window.ids.includes(id));
      if (stillReferenced) continue;
      // The removed window's entity cannot be recovered from the map after the
      // removal. Keep the row conservatively rather than risk deleting a row
      // populated by a transaction or another entity projection.
    }
    this.windowOwned.delete(signature);
  }
}

export class MemoryLocalReplicaStorage implements LocalReplicaStorage {
  private readonly values = new Map<ReplicaScope, ReplicaSnapshot>();
  async load(scope: ReplicaScope = defaultReplicaScope) {
    const value = this.values.get(normalizeReplicaScope(scope));
    return value ? cloneSnapshot(value) : undefined;
  }
  async applyTransaction(_transaction: ReplicaTransaction, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope) {
    this.values.set(normalizeReplicaScope(scope), cloneSnapshot(snapshot));
  }
  async replaceSnapshot(snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope) {
    this.values.set(normalizeReplicaScope(scope), cloneSnapshot(snapshot));
  }
  async replaceWindow(_window: ReplicaWindow, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope) {
    this.values.set(normalizeReplicaScope(scope), cloneSnapshot(snapshot));
  }
  async applyWindowDelta(_window: ReplicaWindow, _delta: { upserts: ReplicaRow[]; deleted: string[] }, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope) {
    this.values.set(normalizeReplicaScope(scope), cloneSnapshot(snapshot));
  }
  async removeWindow(_signature: string, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope) {
    this.values.set(normalizeReplicaScope(scope), cloneSnapshot(snapshot));
  }
  async clear(scope: ReplicaScope = defaultReplicaScope) {
    this.values.delete(normalizeReplicaScope(scope));
  }
}

function validateTransaction(transaction: ReplicaTransaction) {
  if (!transaction.cursor.epoch.trim() || transaction.cursor.revision <= 0) {
    throw new Error("replica transaction requires a positive revision and epoch");
  }
  for (const change of transaction.changes) {
    if (!change.entity.trim() || !change.id.trim()) throw new Error("replica change requires entity and id");
    if (change.operation !== "delete" && !change.newValue) throw new Error("replica upsert requires newValue");
  }
}

function cloneOptimisticPatch(patch: OptimisticPatch): OptimisticPatch {
  if (patch.op === "delete") return { ...patch };
  if (patch.op === "insert") return { ...patch, fields: structuredClone(patch.fields) };
  return { ...patch, fields: structuredClone(patch.fields) };
}

function cloneRow(row: ReplicaRow) { return structuredClone(row); }
function windowResultMetadata(result: JsonValue | undefined, resultPath: readonly string[] | undefined): Pick<LiveQueryResult, "total" | "offset" | "limit"> {
  if (!result || typeof result !== "object" || Array.isArray(result) || !resultPath?.length) return {};
  let current: JsonValue = result;
  for (const part of resultPath.slice(0, -1)) {
    if (typeof current !== "object" || current === null || Array.isArray(current)) return {};
    current = current[part] as JsonValue;
  }
  if (typeof current !== "object" || current === null || Array.isArray(current)) return {};
  const record = current as Record<string, JsonValue>;
  const metadata: Pick<LiveQueryResult, "total" | "offset" | "limit"> = {};
  if (typeof record.total === "number" && Number.isSafeInteger(record.total) && record.total >= 0) metadata.total = record.total;
  if (typeof record.offset === "number" && Number.isSafeInteger(record.offset) && record.offset >= 0) metadata.offset = record.offset;
  if (typeof record.limit === "number" && Number.isSafeInteger(record.limit) && record.limit >= 0) metadata.limit = record.limit;
  return metadata;
}
function normalizeWindow(value: ReplicaWindow | (Omit<ReplicaWindow, "kind"> & { kind?: ReplicaWindow["kind"] })): ReplicaWindow {
  return {
    ...value,
    kind: value.kind ?? "live",
    key: value.key ?? "id",
    ids: [...value.ids],
    cursor: value.cursor ? { ...value.cursor } : undefined,
    resultPath: value.resultPath ? [...value.resultPath] : undefined,
    subscriptionRevision: value.subscriptionRevision ? { ...value.subscriptionRevision } : undefined,
    hashes: value.hashes ? { ...value.hashes } : undefined,
  };
}
function cloneWindow(value: ReplicaWindow): ReplicaWindow { return normalizeWindow(value); }
function cloneEntities(source: Map<string, Map<string, ReplicaRow>>) {
  return new Map([...source].map(([entity, rows]) => [entity, new Map([...rows].map(([id, row]) => [id, cloneRow(row)]))]));
}
function entitiesFromSnapshot(source: ReplicaSnapshot["entities"]) {
  return new Map(Object.entries(source).map(([entity, rows]) => [entity, new Map(Object.entries(rows).map(([id, row]) => [id, cloneRow(row)]))]));
}
function snapshotFrom(cursor: ReplicaCursor | undefined, entities: Map<string, Map<string, ReplicaRow>>, liveQueries: Map<string, ReplicaWindow>): ReplicaSnapshot {
  return {
    cursor: cursor ? { ...cursor } : undefined,
    entities: Object.fromEntries([...entities].map(([entity, rows]) => [entity, Object.fromEntries([...rows].map(([id, row]) => [id, cloneRow(row)]))])),
    liveQueries: Object.fromEntries([...liveQueries].map(([key, value]) => [key, cloneWindow(value)])),
  };
}
function cloneSnapshot(value: ReplicaSnapshot) {
  return snapshotFrom(value.cursor, entitiesFromSnapshot(value.entities), new Map(Object.entries(value.liveQueries).map(([key, window]) => [key, normalizeWindow(window)])));
}

function normalizeReplicaScope(scope: ReplicaScope): ReplicaScope {
  if (typeof scope !== "string" || !scope.trim()) return defaultReplicaScope;
  return scope;
}
