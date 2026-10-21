import { RowRecency } from "./row-recency.js";
import { ResidentMap } from "./resident-map.js";
import { replicaAggregateDigest } from "./replica-integrity.js";
import type { DataPredicate } from '@gonvex/module-sdk';
import type { JsonValue, PublicInvocationProvenance, ReplicaCursor, SubscriptionRevision } from "@gonvex/protocol";
import type { OptimisticPatch } from "./optimistic.js";
import type { LocalExecution } from "@gonvex/local-runtime";
import type { LocalPatch } from '@gonvex/local-runtime';
import type { ReducerReadView } from '@gonvex/local-runtime/portable';
import { memoryReadView, overlayReadView, type ReadCoverage } from '@gonvex/local-runtime/read-view';

export type LocalReplicaSession = {
  lastOnlineAtMs?: number;
  directive: { protocolVersion: 1; scope: string; visibilityScope: string; epoch: string };
  identity: LocalExecution["identity"];
  artifactHash: string;
};

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
  aggregateDigest?: string;
  hashes?: Record<string, string>;
};

/** Static Replica Collection metadata plus the arguments for one subscription. */
export type ReplicaCollectionPlan = {
  table: string;
  key: string;
  equalFilters?: Readonly<Record<string, string>>;
  excludeWhenSet?: readonly string[];
  orderBy?: string;
  orderDirection?: "asc" | "desc";
};

export type ReplicaTransaction = {
  cursor: ReplicaCursor;
  originCommandId?: string;
  provenance?: PublicInvocationProvenance;
  changes: ReplicaChange[];
  memberships?: ReplicaWindow[];
};

export type ReplicaSnapshot = {
  /** Tables with disk rows deliberately omitted from the resident working set. */
  residentPartialTables?: string[];
  storageSequence?:number;
  cursor?: ReplicaCursor;
  entities: Record<string, Record<string, ReplicaRow>>;
  liveQueries: Record<string, ReplicaWindow>;
};

export type ReplicaStorageChanges = {
  sequence:number;
  reset?:boolean;
  entities:Record<string,Record<string,ReplicaRow | null>>;
  windows:Record<string,ReplicaWindow | null>;
  removedMemberships?: Array<{signature:string;id:string}>;
};

/** Opaque persistence namespace for one deployment/project/tenant identity. */
export type ReplicaScope = string;


const defaultReplicaScope: ReplicaScope = "default";

/** Returned only after an atomic metadata-only commit accepts the exact supplied
 * windows without changing rows or resetting their epoch. The adjacent sequence
 * pair comes from that transaction; consumers must compare it with their own
 * reconciled sequence before skipping peer catch-up. Older adapters return void. */
export type ReplicaMetadataCommit = {
  scope: ReplicaScope;
  previousSequence: number;
  sequence: number;
};

/** Committed values reflect per-field authority and tombstones, not merely the
 * requested writes. A receipt can replace own-write catch-up only at an adjacent
 * sequence; an intervening peer commit still requires readChanges. */
export type ReplicaStorageCommit = ReplicaMetadataCommit & { changes: ReplicaStorageChanges };

export type ReplicaWindowDelta = {
  upserts: ReplicaRow[];
  deleted: string[];
  /** Explicit membership edits avoid inspecting/serializing the complete window.
   * after/before identify final neighbors for a new or moved member. */
  memberships?: Array<{ id: string; moved?: boolean; after?: string; before?: string }>;
};

/**
 * Storage implementations must persist the complete transaction atomically.
 * The SQLite adapter maps this call to BEGIN/apply/cursor/COMMIT; IndexedDB
 * implementations use one readwrite transaction over the same stores.
 */
export interface LocalReplicaStorage {
  loadWorkingSet?(scope:string,budget:{maxRows:number;maxBytes:number;preferredWindows?:readonly string[]}):Promise<ReplicaSnapshot | undefined>;
  loadWindowRows?(scope:string,signature:string):Promise<{window:ReplicaWindow;rows:ReplicaRow[]} | undefined>;
  loadEntityRows?(scope:string,entity:string,ids:readonly string[]):Promise<Array<{id:string;row:ReplicaRow}>>;
  /** Opt into an in-process membership predicate instead of resident ID arrays. */
  readonly supportsResidentLookup?: boolean;
  /** Accept explicit per-member edits and return committed row authority. */
  readonly supportsMembershipDeltas?: boolean;
  /** Working-set rows are newly decoded and may be frozen by the replica. */
  readonly ownsWorkingSetRows?: boolean;
  /** loadWindowRows/loadEntityRows return fresh rows exclusively owned by the
   * caller. Adapters retaining aliases must leave this false. */
  readonly ownsHydratedRows?: boolean;
  readChanges?(scope:string, afterSequence:number,interest?:{rows:Record<string,string[]>;hasRow?:(entity:string,id:string)=>boolean;windows:string[];availableRows:number;availableBytes:number;maxRows:number;maxBytes:number}):Promise<ReplicaStorageChanges>;
  subscribePeer?(listener:(scope:string)=>void):()=>void;
  withReadView?<T>(scope: string, coverage: ReadCoverage, run: (view: ReducerReadView) => Promise<T>): Promise<T>;
  /** Last server-authorized identity/visibility metadata, partitioned by login. */
  loadSession?(identityScope: string): Promise<LocalReplicaSession | undefined>;
  saveSession?(identityScope: string, session: LocalReplicaSession | undefined): Promise<void>;
  load(scope?: ReplicaScope): Promise<ReplicaSnapshot | undefined>;
  applyTransaction(transaction: ReplicaTransaction, snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void | ReplicaStorageCommit>;
  /** Advance ready Replica Collection cursors without rewriting normalized rows. */
  advanceWatermark?(windows: readonly ReplicaWindow[], cursor: ReplicaCursor | undefined, scope?: ReplicaScope): Promise<void | ReplicaMetadataCommit>;
  /** Persist a normalized Query/Collection materialization atomically. */
  replaceSnapshot?(snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void>;
  replaceWindow?(window: ReplicaWindow, snapshot: ReplicaSnapshot, scope?: ReplicaScope, projection?:readonly ReplicaRow[]): Promise<void>;
  applyWindowDelta?(
    window: ReplicaWindow,
    delta: ReplicaWindowDelta,
    snapshot: ReplicaSnapshot,
    scope?: ReplicaScope,
  ): Promise<void | ReplicaMetadataCommit | ReplicaStorageCommit>;
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
  /**
   * True only after this exact Replica Collection subscription receives replica.ready.
   * Direct LocalReplicaView reads omit it because verification belongs to the transport subscription.
   */
  isUpToDate?: boolean;
  truncated: boolean;
  computedRevision: number;
};

/** Collection state returned by a live client watch or React hook. */
export type ReplicaCollectionSubscriptionState<T extends ReplicaRow = ReplicaRow> =
  ReplicaCollectionState<T> & { isUpToDate: boolean };

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
  entityVersion(entity: string, id?: string): number;
  subscribe(listener: (changedIDs?: ReadonlySet<string>) => void, selection?: ReplicaSubscription): () => void;
  hasPendingCommand(commandId: string): boolean;
  getWindow(signature: string): ReplicaWindow | undefined;
  listWindows(): ReplicaWindow[];
  windowRows<T extends ReplicaRow = ReplicaRow>(signature: string): T[];
  entity<T extends ReplicaRow = ReplicaRow>(entity: string, id: string): T | undefined;
  entityBatch<T extends ReplicaRow = ReplicaRow>(entity: string, ids: readonly string[]): Array<T | undefined>;
  entityRows<T extends ReplicaRow = ReplicaRow>(entity: string): T[];
  entityCompleteness(entity: string): "complete" | "partial";
  liveQuery<T extends ReplicaRow = ReplicaRow>(signature: string): LiveQueryResult<T>;
  /** Stable immutable snapshot for reactive consumers. */
  liveQuerySnapshot<T extends ReplicaRow = ReplicaRow>(signature: string): LiveQueryResult<T>;
  collectionState<T extends ReplicaRow = ReplicaRow>(signature: string): ReplicaCollectionState<T>;
  hasLiveQuery(signature: string): boolean;
  snapshot(): ReplicaSnapshot;
}

/** Replica values are JSON. Freeze each new subtree once; old subtrees are shared. */
function immutable<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) immutable(child);
    Object.freeze(value);
  }
  return value;
}

const residentRowSizes = new WeakMap<ReplicaRow, number>();
function residentRowSize(row: ReplicaRow): number {
  let size = residentRowSizes.get(row);
  if (size === undefined) {
    size = JSON.stringify(row).length * 2 + 256;
    residentRowSizes.set(row, size);
  }
  return size;
}

/** Accounting belongs to each structurally shared table version. */
const residentTableBytes = new WeakMap<Map<string, ReplicaRow>, number>();
function residentRows(source?: Iterable<readonly [string, ReplicaRow]>): Map<string, ReplicaRow> {
  if (source instanceof ResidentMap && residentTableBytes.has(source)) {
    const rows = source.fork();
    // Publication clears this journal. Preserve earlier edits when catch-up
    // forks a table that still contains an unpublished local transaction.
    for (const id of source.changedIDs) rows.changedIDs.add(id);
    residentTableBytes.set(rows, residentTableBytes.get(source)!);
    return rows;
  }
  const rows = new ResidentMap<ReplicaRow>();
  residentTableBytes.set(rows, 0);
  if (source) for (const [id, row] of source) setResidentRow(rows, id, row);
  return rows;
}
function setResidentRow(rows: Map<string, ReplicaRow>, id: string, row: ReplicaRow): void {
  const prior = rows.get(id);
  if (prior === row) return;
  immutable(row);
  residentTableBytes.set(rows, (residentTableBytes.get(rows) ?? 0) - (prior ? residentRowSize(prior) : 0) + residentRowSize(row));
  rows.set(id, row);
}
function deleteResidentRow(rows: Map<string, ReplicaRow>, id: string): boolean {
  const prior = rows.get(id);
  if (prior) residentTableBytes.set(rows, residentTableBytes.get(rows)! - residentRowSize(prior));
  return rows.delete(id);
}

/** Narrow a notification to rows or a window. Offline computed plans can also
 * observe relation tables, so their windows opt into broad offline delivery. */
export type ReplicaSubscription = { entity: string; ids?: readonly string[] } | { window: string; offlineGlobal?: boolean };

type EqualityIndex = { entity: string; column: string; values: Map<string, Set<string>>; bytes: number; scan?: Iterator<[string, ReplicaRow]> };

export class LocalReplica implements LocalReplicaView {
  private readonly retainedWindows = new Map<string, number>();
  private readonly retainedRows = new Map<string,Map<string,number>>();
  private readonly residentPartialTables = new Set<string>();
  private readonly rowSizes = residentRowSizes;
  private readonly recency = new RowRecency();
  private membershipBytes = 0;
  private residencyVersionValue = 0;
  private readonly equalityIndexes = new Map<string, EqualityIndex>();
  private equalityTimer?: ReturnType<typeof setTimeout>;
  private readonly entityRowArrays = new Map<string, { version: number; rows: ReplicaRow[] }>();
  private residentBytes = 0;
  private residentCount = 0;
  private readonly residencyBudget = {maxRows:20_000,maxBytes:16*1024*1024};
  private cursorValue?: ReplicaCursor;
  private entityTables = new Map<string, Map<string, ReplicaRow>>();
  private get entities() { return this.entityTables; }
  private set entities(next: Map<string, Map<string, ReplicaRow>>) {
    // Only table totals are consulted here, never resident rows.
    let bytes = 0, count = 0;
    for (const rows of next.values()) {
      bytes += residentTableBytes.get(rows)!;
      count += rows.size;
    }
    if (this.storage?.withReadView && this.storage?.loadWindowRows) {
      const reset = !next.size || [...this.entityTables].every(([entity,old]) => {
        const rows = next.get(entity);
        return !rows || rows instanceof ResidentMap && !rows.sharesLineage(old);
      });
      if (reset) this.recency.clear();
      else for (const [entity,old] of this.entityTables) {
        const rows = next.get(entity);
        if (!rows || rows instanceof ResidentMap && !rows.sharesLineage(old)) {
          for (const id of old.keys()) this.recency.delete(`${entity}\0${id}`);
        }
      }
      for (const [entity,rows] of next) if (reset || rows !== this.entityTables.get(entity)) {
        const previous = this.entityTables.get(entity);
        const replaced = rows instanceof ResidentMap && (!previous || !rows.sharesLineage(previous));
        const ids = reset || replaced ? rows.keys() : rows instanceof ResidentMap ? rows.changedIDs : rows.keys();
        for (const id of ids) {
          const key = `${entity}\0${id}`;
          if (reset || replaced || rows.has(id)) this.recency.set(key, { entity, id }, this.rowIsPinned(entity,id));
          else this.recency.delete(key);
        }
      }
    }
    for (const [entity, rows] of next) if (rows !== this.entityTables.get(entity) && rows instanceof ResidentMap) {
      for (const id of rows.changedIDs) for (const index of this.equalityIndexes.values()) {
        if (index.entity === entity) this.indexEqualityRow(index,id,rows.get(id),this.entityTables.get(entity)?.get(id));
      }
      rows.changedIDs.clear();
    }
    this.entityTables = next;
    this.residentBytes = bytes;
    this.residentCount = count;
  }
  private queryWindows = new Map<string, ReplicaWindow>();
  private readonly memberships = new Map<string, Map<string, string | Set<string>>>();
  private readonly largeMemberships = new Map<string, Map<string, readonly string[]>>();
  private get liveQueries() { return this.queryWindows; }
  private set liveQueries(next: Map<string, ReplicaWindow>) {
    for (const [signature, old] of this.queryWindows) {
      const current = next.get(signature);
      if (current?.entity === old.entity && current.ids === old.ids) continue;
      this.membershipBytes -= windowMembershipSize(old);
      const table = this.memberships.get(old.entity);
      const large = this.largeMemberships.get(old.entity);
      large?.delete(signature);
      if (!large?.size) this.largeMemberships.delete(old.entity);
      for (const id of old.ids) {
        const owners = table?.get(id);
        if (owners === signature) table?.delete(id);
        else if (owners instanceof Set) {
          owners.delete(signature);
          if (owners.size === 1) table!.set(id,owners.values().next().value!);
          else if (!owners.size) table?.delete(id);
        }
        this.refreshPin(old.entity,id);
      }
      if (!table?.size) this.memberships.delete(old.entity);
    }
    for (const [signature, window] of next) {
      const old = this.queryWindows.get(signature);
      if (old?.entity === window.entity && old.ids === window.ids) continue;
      this.membershipBytes += windowMembershipSize(window);
      if (!window.ids.length) continue;
      if (window.ids.length > 1024) {
        let large = this.largeMemberships.get(window.entity);
        if (!large) { large = new Map(); this.largeMemberships.set(window.entity,large); }
        large.set(signature,[...window.ids].sort());
        for (const id of window.ids) this.refreshPin(window.entity,id);
        continue;
      }
      let table = this.memberships.get(window.entity);
      if (!table) { table = new Map(); this.memberships.set(window.entity, table); }
      for (const id of window.ids) {
        const owners = table.get(id);
        if (owners === undefined) table.set(id,signature);
        else if (typeof owners === 'string') {
          if (owners !== signature) table.set(id,new Set([owners,signature]));
        } else owners.add(signature);
        this.refreshPin(window.entity,id);
      }
    }
    this.queryWindows = next;
  }
  private *windowsForRow(entity: string, id: string): IterableIterator<string> {
    const owners = this.memberships.get(entity)?.get(id);
    if (typeof owners === 'string') yield owners;
    else if (owners) yield* owners;
    for (const [signature, ids] of this.largeMemberships.get(entity) ?? []) {
      let low = 0, high = ids.length;
      while (low < high) {
        const middle = (low + high) >>> 1;
        if (ids[middle]! < id) low = middle + 1;
        else high = middle;
      }
      if (ids[low] === id) yield signature;
    }
  }
  private readonly replicaPlans = new Map<string, { definition: ReplicaCollectionPlan; args: ReplicaRow }>();
  private pendingCommands = new Map<string, PendingCommand>();
  private listeners = new Set<() => void>();
  private readonly entityListeners = new Map<string, Set<() => void>>();
  private readonly rowListeners = new Map<string, Map<string, Set<() => void>>>();
  private readonly offlineListeners = new Set<() => void>();
  private readonly windowListeners = new Map<string, Set<() => void>>();
  private readonly changedEntities = new Map<string, Set<string>>();
  private readonly changedWindows = new Set<string>();
  private invalidateSubscriptions = false;
  private notifiedFreshness: ReplicaFreshness = "verifying";
  private persistence = Promise.resolve();
  private storageSequence = 0;
  private application = Promise.resolve();
  private applicationRunning = false;
  private disposed = false;
  private readonly applicationJobs: Array<{ urgent: boolean; run: () => Promise<void>; cancel: () => void }> = [];

  private enqueueApplication<T>(run: () => Promise<T>, urgent = false): Promise<T> {
    if (this.disposed) return Promise.reject(new Error("Local replica is closed"));
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const result = new Promise<T>((done, failed) => { resolve = done; reject = failed; });
    this.applicationJobs.push({ urgent, cancel: () => reject(new Error("Local replica is closed")), run: async () => { try { resolve(await run()); } catch (error) { reject(error); } finally { if (this.disposed) this.releaseMemory(); } } });
    this.application = Promise.all([this.application, result]).then(() => undefined, () => undefined);
    if (!this.applicationRunning) {
      this.applicationRunning = true;
      queueMicrotask(() => { void this.drainApplicationJobs(); });
    }
    return result;
  }

  private async drainApplicationJobs() {
    while (this.applicationJobs.length) {
      // Finish the active transaction atomically. Between transactions, a user
      // read can precede queued hydration without reordering server writes.
      const urgent = this.applicationJobs.findIndex(job => job.urgent);
      const [job] = this.applicationJobs.splice(urgent < 0 ? 0 : urgent, 1);
      await job!.run();
    }
    this.applicationRunning = false;
  }
  private hydration?: Promise<void>;
  private freshnessValue: ReplicaFreshness = "verifying";
  private versionValue = 0;
  private executionVersionValue = 0;
  private executionVersionFloor = 0;
  private readonly executionEntityVersions = new Map<string, number>();
  private readonly executionRowVersions = new Map<string, number>();
  private entityVersionClock = 0;
  private entityVersionFloor = 0;
  private readonly entityVersions = new Map<string, number>();
  private windowVersionClock = 0;
  private windowVersionFloor = 0;
  private readonly windowVersions = new Map<string, number>();
  private readonly rowVersions = new Map<string, number>();
  private readonly windowRowsVersions = new Map<string, number>();
  private scopeValue: ReplicaScope = defaultReplicaScope;
  // The default scope starts as an immediately usable empty in-memory store.
  // Persistence is restored only by hydrate()/activateScope(), so direct
  // callers can still materialize before an async adapter is consulted.
  private scopeLoaded = true;
  private scopeActivation?: Promise<void>;
  private scopeActivationName?: ReplicaScope;
  private scopeActivationGeneration = 0;

  constructor(private readonly storage?: LocalReplicaStorage, options:{maxResidentRows?:number;maxResidentBytes?:number}={}) {
    for(const [name,value] of Object.entries(options))if(value!==undefined && (!Number.isSafeInteger(value) || value<=0))throw new Error(`Invalid replica residency budget: ${name}`);
    this.residencyBudget.maxRows=options.maxResidentRows ?? this.residencyBudget.maxRows;
    this.residencyBudget.maxBytes=options.maxResidentBytes ?? this.residencyBudget.maxBytes;
  }

  /** Release a discarded client's in-memory state without deleting durable data. */
  dispose(): void {
    this.disposed = true;
    this.scopeLoaded = false;
    this.scopeActivationGeneration++;
    for (const job of this.applicationJobs.splice(0)) job.cancel();
    this.releaseMemory();
  }

  private releaseMemory(): void {
    this.entities = new Map();
    this.liveQueries = new Map();
    this.pendingCommands.clear();
    this.listeners.clear();
    this.entityListeners.clear(); this.rowListeners.clear(); this.windowListeners.clear(); this.offlineListeners.clear();
    this.changedEntities.clear(); this.changedWindows.clear();
    this.recency.clear();
    this.retainedWindows.clear();
    this.retainedRows.clear();
    this.residentPartialTables.clear();

    this.replicaPlans.clear();
    this.readSnapshots.clear();
    this.equalityIndexes.clear();
    if (this.equalityTimer !== undefined) clearTimeout(this.equalityTimer);
    this.equalityTimer = undefined;
    this.entityRowArrays.clear();
    this.entityVersions.clear();
    this.rowVersions.clear();
    this.windowVersions.clear();
    this.windowRowsVersions.clear();
  }

  hydrate(): Promise<void> {
    if (this.hydration) return this.hydration;
    this.hydration = this.activateScope(defaultReplicaScope, true);
    return this.hydration;
  }

  /**
   * Atomically switch the authoritative store to an opaque persistence scope.
   * A newer request supersedes an older one even if the older storage read is
   * slow (or resolves after auth has already changed again).
   * Pass restoreFromStorage=false for an empty, unauthorized partition.
   */
  activateScope(scope: ReplicaScope = defaultReplicaScope, forceReload = false, restoreFromStorage = true): Promise<void> {
    const nextScope = normalizeReplicaScope(scope);
    if (!forceReload && this.scopeLoaded && this.scopeValue === nextScope) return this.application;
    if (this.scopeActivation && this.scopeActivationName === nextScope) return this.scopeActivation;
    // Fail closed immediately while the async read is in flight. This keeps a
    // synchronous auth change from exposing the prior identity's rows.
    const wasLoaded = this.scopeLoaded;
    this.scopeLoaded = false;
    if (wasLoaded) { this.invalidateEntityVersions(); this.notify(true); }
    const generation = ++this.scopeActivationGeneration;
    const activation = this.enqueueApplication(async () => {
      // A prior transaction may still be in the storage queue. Wait for it so
      // a same-scope activation cannot load a snapshot from before its commit.
      await this.persistence;
      const snapshot = !restoreFromStorage ? undefined : this.storage?.loadWorkingSet
        ? await this.storage.loadWorkingSet(nextScope,{
          // Bootstrap only a bounded page plus references. Retained windows and
          // entity leases recover additional rows through indexed reads later.
          maxRows:Math.min(this.residencyBudget.maxRows,2048),
          maxBytes:Math.min(this.residencyBudget.maxBytes,4*1024*1024),
          preferredWindows:[...this.retainedWindows.keys()],
        })
        : await this.storage?.load(nextScope);
      if (generation !== this.scopeActivationGeneration) return;
      this.scopeValue = nextScope;
      this.storageSequence = snapshot?.storageSequence ?? 0;
      this.scopeLoaded = true;
      this.recency.clear();
      this.entities = snapshot ? entitiesFromSnapshot(snapshot.entities, this.storage?.ownsWorkingSetRows) : new Map();
      this.residentPartialTables.clear();
      for(const entity of snapshot?.residentPartialTables ?? []) this.residentPartialTables.add(entity);
      this.invalidateEntityVersions();
      this.liveQueries = snapshot
        ? new Map(Object.entries(snapshot.liveQueries).map(([key, value]) => [key, normalizeWindow(value)]))
        : new Map();
      this.cursorValue = hydratedTransactionFloor(snapshot?.cursor, this.liveQueries);
      for (const signature of this.liveQueries.keys()) this.markWindowChanged(signature);

      // Optimistic commands belong to the old identity and must never be
      // projected while the newly restored scope is becoming authoritative.
      this.pendingCommands.clear();
      this.freshnessValue = "verifying";
      this.notify(true);
      queueMicrotask(()=>{
        if(!restoreFromStorage || generation!==this.scopeActivationGeneration)return;
        for(const signature of this.retainedWindows.keys())this.retainWindow(signature)();
        for(const [entity,ids] of this.retainedRows)this.retainRows(entity,[...ids.keys()])();
      });
    });

    this.scopeActivation = activation;
    this.scopeActivationName = nextScope;
    return activation;
  }

  /** Active views retain their rows. Cold collections stay durable on disk and
   * reducer reads can use the indexed adapter without hydrating the table. */
  retainWindow(signature:string):()=>void {
    const wasRetained = this.retainedWindows.has(signature);
    this.retainedWindows.set(signature,(this.retainedWindows.get(signature) ?? 0)+1);
    if (!wasRetained) {
      const window = this.liveQueries.get(signature);
      if (window) for (const id of window.ids) this.refreshPin(window.entity,id);
    }
    const scopeGeneration=this.scopeActivationGeneration;
    const operation=this.enqueueApplication(async()=>{
      if(!this.scopeLoaded || !this.retainedWindows.has(signature) || scopeGeneration!==this.scopeActivationGeneration || !this.storage?.loadWindowRows)return;
      const existing=this.liveQueries.get(signature);
      if(existing?.ids.every(id=>this.entities.get(existing.entity)?.has(id)))return;
      const loaded=await this.storage.loadWindowRows(this.scopeValue,signature);
      if(!loaded || !this.retainedWindows.has(signature) || scopeGeneration!==this.scopeActivationGeneration)return;
      const table=residentRows(this.entities.get(loaded.window.entity));
      for(const row of loaded.rows)setResidentRow(table,String(row[loaded.window.key]),this.storage?.ownsHydratedRows ? row : cloneRow(row));
      this.entities=new Map(this.entities).set(loaded.window.entity,table);
      this.liveQueries = new Map(this.liveQueries).set(signature,normalizeWindow(loaded.window));
      if(loaded.window.kind==='replica' && loaded.window.completeness==='complete' && !loaded.window.truncated && loaded.rows.length===loaded.window.ids.length)this.residentPartialTables.delete(loaded.window.entity);
      this.markEntityChanged(loaded.window.entity,loaded.window.ids);
      this.markWindowsForRows(loaded.window.entity, new Set(loaded.window.ids), signature);
      this.markWindowChanged(signature);
      this.notify(true);
    }, true);

    let released=false;
    return ()=>{
      if(released)return;released=true;
      const count=this.retainedWindows.get(signature) ?? 0;
      if(count>1)this.retainedWindows.set(signature,count-1);
      else {
        this.retainedWindows.delete(signature);this.readSnapshots.delete(signature);
        const window=this.liveQueries.get(signature);
        if(window)for(const id of window.ids)this.refreshPin(window.entity,id);
        this.trimResidentRows();
      }
    };
  }

  private residentCoverage(coverage:ReadCoverage):ReadCoverage {
    if(!this.residentPartialTables.size)return coverage;
    return Object.fromEntries(Object.entries(coverage).map(([table,value])=>[table,this.residentPartialTables.has(table)?{...value,complete:false,completeWhere:undefined}:value]));
  }

  retainRows(entity:string,ids:readonly string[]):()=>void {
    const scopeGeneration=this.scopeActivationGeneration;
    let retained=this.retainedRows.get(entity);
    if(!retained){retained=new Map();this.retainedRows.set(entity,retained);}
    for(const id of ids){retained.set(id,(retained.get(id)??0)+1);this.refreshPin(entity,id);}
    const operation=this.enqueueApplication(async()=>{
      if(!this.scopeLoaded || !this.storage?.loadEntityRows || scopeGeneration!==this.scopeActivationGeneration)return;
      const missing=ids.filter(id=>!this.entities.get(entity)?.has(id) && this.retainedRows.get(entity)?.has(id));
      if(!missing.length)return;
      const rows=await this.storage.loadEntityRows(this.scopeValue,entity,missing);
      if(scopeGeneration!==this.scopeActivationGeneration)return;
      const next=residentRows(this.entities.get(entity));
      const loadedIDs = new Set<string>();
      for (const { id, row } of rows) if (this.retainedRows.get(entity)?.has(id)) {
        setResidentRow(next, id, this.storage?.ownsHydratedRows ? row : cloneRow(row));
        loadedIDs.add(id);
      }
      this.entities=new Map(this.entities).set(entity,next);
      this.markEntityChanged(entity,missing);
      this.markWindowsForRows(entity, loadedIDs);
      this.notify(true);
    }, true);

    let released=false;
    return ()=>{
      if(released)return;released=true;
      const current=this.retainedRows.get(entity);
      for(const id of ids){const count=current?.get(id)??0;if(count>1)current!.set(id,count-1);else current?.delete(id);this.refreshPin(entity,id);}
      if(!current?.size)this.retainedRows.delete(entity);
      this.trimResidentRows();
    };
  }

  private trimResidentRows() {
    // Without indexed persistence, eviction would lose the only readable copy.
    if(!this.storage?.withReadView || !this.storage?.loadWindowRows)return;
    let bytes=this.residentBytes,count=this.residentCount;
    if(bytes+this.metadataBytes()<=this.residencyBudget.maxBytes && count<=this.residencyBudget.maxRows)return;
    // Derived caches can be rebuilt without disk reads. Reclaim them before
    // selecting victims so their stale row references cannot force eviction.
    this.readSnapshots.clear();
    this.entityRowArrays.clear();
    this.equalityIndexes.clear();
    if(bytes+this.metadataBytes()<=this.residencyBudget.maxBytes && count<=this.residencyBudget.maxRows)return;
    const pending = new Map<string, Set<string>>();
    for (const command of this.pendingCommands.values()) for (const patch of command.patches) {
      const entity = patch.entity ?? patch.collection!;
      let ids = pending.get(entity);
      if (!ids) { ids = new Set(); pending.set(entity, ids); }
      ids.add(patch.rowId);
    }
    const next = new Map(this.entities);
    const copied = new Set<string>();
    const removed = new Map<string, Set<string>>();
    const skipped: Array<{entity:string;id:string;key:string}> = [];
    while (this.recency.oldest()) {
      const { key, entity, id } = this.recency.oldest()!;
      if (bytes + this.metadataBytes() <= this.residencyBudget.maxBytes && count <= this.residencyBudget.maxRows) break;
      if (pending.get(entity)?.has(id)) { this.recency.pin(key,true);skipped.push({entity,id,key});continue; }
      if (!copied.has(entity)) { next.set(entity, residentRows(next.get(entity))); copied.add(entity); }
      const rows = next.get(entity)!;
      const row = rows.get(id);
      if (!row) { this.recency.delete(key); continue; }
      deleteResidentRow(rows, id); count--; bytes -= this.rowSizes.get(row)!;
      this.recency.delete(key);
      this.residentPartialTables.add(entity);
      let ids = removed.get(entity);
      if (!ids) { ids = new Set(); removed.set(entity, ids); }
      ids.add(id);
    }
    for(const {entity,id} of skipped)this.refreshPin(entity,id);
    if (!removed.size) return;
    this.entities = next;
    for (const [entity, ids] of removed) {
      this.markEntityChanged(entity, ids);
      const windows = new Set<string>();
      for (const id of ids) {
        this.rowVersions.delete(`${entity}\0${id}`);
        this.executionRowVersions.delete(JSON.stringify([entity, id]));
        for (const signature of this.windowsForRow(entity, id)) windows.add(signature);
      }
      for (const signature of windows) this.markWindowChanged(signature);
    }
    this.residencyVersionValue++;
  }

  private metadataBytes(): number {
    let snapshots = 0;
    for (const snapshot of this.readSnapshots.values()) snapshots += snapshot.rows.length * 8 + snapshot.cache.size * 96;
    let arrays = 0;
    for (const cached of this.entityRowArrays.values()) arrays += cached.rows.length * 8;
    let indexes = 0;
    for (const index of this.equalityIndexes.values()) indexes += index.bytes + index.values.size * 80;
    return arrays + indexes + this.membershipBytes + this.recency.size * 256 + this.rowVersions.size * 80
      + this.executionRowVersions.size * 80 + this.windowVersions.size * 80 + this.windowRowsVersions.size * 80 + snapshots;
  }

  private touchRow(entity: string, id: string): void { this.recency.touch(`${entity}\0${id}`); }
  private rowIsPinned(entity:string,id:string):boolean {
    if (this.retainedRows.get(entity)?.has(id) === true) return true;
    if (this.retainedWindows.size) for (const signature of this.windowsForRow(entity,id)) {
      if (this.retainedWindows.has(signature)) return true;
    }
    return false;
  }
  private refreshPin(entity:string,id:string):void { this.recency.pin(`${entity}\0${id}`,this.rowIsPinned(entity,id)); }

  cursor() {
    return this.scopeLoaded && this.cursorValue ? { ...this.cursorValue } : undefined;
  }

  /** Consume the durable replica, including commits delivered by another tab.
   * Predictions remain a separate overlay; peers never advance our socket's
   * acknowledged cursor or publish a half-applied transaction. */
  synchronizeStorage(): Promise<void> {
    if (!this.storage?.readChanges) return Promise.resolve();
    const scope = this.scopeValue;
    const application = this.enqueueApplication(async () => {
      if (!this.scopeLoaded || scope !== this.scopeValue || !this.storage?.readChanges) return;
      await this.persistence;
      const entities = new Map(this.entities);
      const windows = new Map(this.liveQueries);
      if (!await this.mergeStoredChanges(entities, windows)) return;
      this.entities = entities;
      this.liveQueries = windows;
      this.notify(true);
    }, true);

    return application;
  }

  private async mergeStoredChanges(entities: Map<string, Map<string, ReplicaRow>>, windows: Map<string, ReplicaWindow>, committed?: ReplicaStorageChanges): Promise<boolean> {
    if (!committed && !this.storage?.readChanges) return false;
    const useLookup = this.storage?.supportsResidentLookup;
    const residentIds: Record<string, string[]> = useLookup || !this.storage?.loadWorkingSet ? {} : Object.fromEntries([...entities].map(([table,rows])=>[table,[...rows.keys()]]));
    const residentBytes=this.residentBytes,residentCount=this.residentCount;
    for(const command of this.pendingCommands.values())for(const patch of command.patches){const entity=patch.entity ?? patch.collection!;(residentIds[entity]??=[]).push(patch.rowId);}
    const changes = committed ?? await this.storage!.readChanges!(this.scopeValue, this.storageSequence,this.storage!.loadWorkingSet ? {
      rows:residentIds,
      ...(useLookup ? {hasRow:(entity:string,id:string)=>entities.get(entity)?.has(id) ?? false} : {}),
      windows:[...this.retainedWindows.keys()],
      availableRows:Math.max(0,this.residencyBudget.maxRows-residentCount),
      availableBytes:Math.max(0,this.residencyBudget.maxBytes-residentBytes),
      ...this.residencyBudget,
    } : undefined);
    if (changes.sequence === this.storageSequence) return false;
    if (changes.reset) {
      entities.clear();
      windows.clear();

      this.invalidateEntityVersions();
    }
    for (const [entity, values] of Object.entries(changes.entities)) {
      const rows = residentRows(entities.get(entity));
      const changedIDs = new Set<string>();
      for (const [id, row] of Object.entries(values)) {
        if (row === null) {
          if (deleteResidentRow(rows,id)) changedIDs.add(id);
        } else if (!sameReplicaValue(rows.get(id), row)) {
          setResidentRow(rows,id,cloneRow(row));
          changedIDs.add(id);
        }
      }
      if (!changedIDs.size) continue;
      entities.set(entity, rows);
      this.markEntityChanged(entity, changedIDs);
      const affected = new Set<string>();
      for (const id of changedIDs) for (const signature of this.windowsForRow(entity,id)) if (windows.has(signature)) affected.add(signature);
      for (const signature of affected) this.markWindowChanged(signature);
    }
    for (const [signature, window] of Object.entries(changes.windows)) {
      const previous = windows.get(signature);
      if (window === null) {
        if (windows.delete(signature)) this.markWindowChanged(signature);
        continue;
      }
      if (sameReplicaValue(previous, window)) continue;
      const membershipChanged = !sameWindowRowDefinition(previous, window);
      windows.set(signature, normalizeWindow(window));
      if(window.ids.some(id=>!entities.get(window.entity)?.has(id)))this.residentPartialTables.add(window.entity);
      // Row changes have already invalidated their windows above. A storage
      // watermark changes metadata, not membership or the visible row array.
      this.markWindowChanged(signature, membershipChanged);
    }
    for (const { signature, id } of changes.removedMemberships ?? []) {
      const window = windows.get(signature);
      if (!window || !window.ids.includes(id)) continue;
      windows.set(signature, { ...window, ids: window.ids.filter(member => member !== id) });
      this.markWindowChanged(signature);
    }
    this.storageSequence = changes.sequence;
    return true;
  }

  freshness() {
    return this.freshnessValue;
  }

  version() { return this.versionValue; }

  /** Changes to the authoritative execution snapshot, excluding UI metadata and predictions. */
  executionVersion() { return this.executionVersionValue; }
  executionReadVersion(entity: string, rowId?: string) {
    if (rowId === undefined) return this.executionEntityVersions.get(entity) ?? this.executionVersionFloor;
    const resident = this.recency.version(`${entity}\0${rowId}`,true);
    const known = resident === undefined ? this.executionRowVersions.get(JSON.stringify([entity, rowId])) : Math.max(resident,this.executionVersionFloor);
    return known ?? (this.hasResidentRow(entity,rowId) ? this.executionVersionFloor : this.executionEntityVersions.get(entity) ?? this.executionVersionFloor);
  }
  entityVersion(entity: string, id?: string) { return id === undefined ? this.entityVersions.get(entity) ?? this.entityVersionFloor : this.rowVersion(entity, id); }
  rowVersion(entity: string, id: string) {
    const resident = this.recency.version(`${entity}\0${id}`);
    const known = resident === undefined ? this.rowVersions.get(`${entity}\0${id}`) : Math.max(resident,this.entityVersionFloor);
    return known ?? (this.hasResidentRow(entity,id) ? this.entityVersionFloor : this.entityVersions.get(entity) ?? this.entityVersionFloor);
  }
  private hasResidentRow(entity:string,id:string):boolean {
    return this.storage?.withReadView && this.storage?.loadWindowRows
      ? this.recency.has(`${entity}\0${id}`) : this.entities.get(entity)?.has(id) === true;
  }

  residencyVersion(): number { return this.residencyVersionValue; }

  dependencyVersion(tables: readonly string[]): string {
    const entities = new Set(tables);
    return JSON.stringify([this.scopeActivationGeneration, this.scopeLoaded,
      tables.map(entity => [this.entityVersion(entity), this.entityCompleteness(entity)]),
      [...this.liveQueries.values()].filter(window => entities.has(window.entity)).map(window => this.windowVersion(window.signature)),
      [...this.replicaPlans.values()].filter(plan => entities.has(plan.definition.table))]);
  }

  /** Monotonic version for one materialized window and its referenced rows. */
  windowVersion(signature: string) { return this.windowVersions.get(signature) ?? this.windowVersionFloor; }
  windowRowsVersion(signature: string) { return this.windowRowsVersions.get(signature) ?? this.windowVersionFloor; }

  setFreshness(freshness: ReplicaFreshness) {
    if (freshness === this.freshnessValue) return;
    this.freshnessValue = freshness;
    this.notify();
  }

  subscribe(listener: (changedIDs?: ReadonlySet<string>) => void, selection?: ReplicaSubscription) {
    if (!selection) {
      this.listeners.add(listener);
      return () => { this.listeners.delete(listener); };
    }
    const releases: Array<() => void> = [];
    const callback = (changedIDs?: ReadonlySet<string>) => listener(changedIDs);
    const add = (index: Map<string, Set<() => void>>, key: string) => {
      let group = index.get(key);
      if (!group) { group = new Set(); index.set(key, group); }
      // Each registration is independent, even when callbacks are reused.
      group.add(callback);
      releases.push(() => { group!.delete(callback); if (!group!.size) index.delete(key); });
    };
    if ("window" in selection) {
      add(this.windowListeners, selection.window);
      if (selection.offlineGlobal) {
        this.offlineListeners.add(callback);
        releases.push(() => { this.offlineListeners.delete(callback); });
      }
    }
    else if (!selection.ids) add(this.entityListeners, selection.entity);
    else {
      let index = this.rowListeners.get(selection.entity);
      if (!index) { index = new Map(); this.rowListeners.set(selection.entity, index); }
      for (const id of new Set(selection.ids)) add(index, id);
      releases.push(() => {
        if (!index!.size) this.rowListeners.delete(selection.entity);
        for (const id of selection.ids!) if (!index!.has(id) && !this.hasResidentRow(selection.entity,id)) this.rowVersions.delete(`${selection.entity}\0${id}`);
      });
    }
    let released = false;
    return () => { if (released) return; released = true; for (const release of releases) release(); };
  }

  applyOptimistic(commandId: string, patches: OptimisticPatch[]) {
    commandId = commandId.trim();
    if (!commandId) throw new Error("optimistic commandId is required");
    const clonedPatches = patches.map(cloneOptimisticPatch);
    const previous = this.pendingCommands.get(commandId);
    if (previous && sameReplicaValue(previous.patches,clonedPatches)) return;
    if (previous) this.markWindowsForPatches(previous.patches);
    this.pendingCommands.set(commandId, { commandId, patches: clonedPatches });
    this.markWindowsForPatches(clonedPatches);
    this.notify();
  }

  /** SDK-only atomic replacement after ordered local reducer replay. */
  replaceOptimistic(commands: readonly { commandId: string; patches: OptimisticPatch[] }[]) {
    // Rebase is also triggered by unrelated authoritative updates. An identical
    // overlay must not dirty every selector or schedule another application render.
    const previous = [...this.pendingCommands.values()];
    if (previous.length === commands.length && commands.every((command, index) =>
      command.commandId === previous[index]!.commandId &&
      JSON.stringify(command.patches) === JSON.stringify(previous[index]!.patches))) return;
    for (const command of this.pendingCommands.values()) this.markWindowsForPatches(command.patches);
    const previousCommands = new Map(this.pendingCommands);
    this.pendingCommands.clear();
    for (const command of commands) {
      const patches = command.patches.map(cloneOptimisticPatch);
      this.pendingCommands.set(command.commandId, { commandId: command.commandId, patches, committedRevision: previousCommands.get(command.commandId)?.committedRevision });
      this.markWindowsForPatches(patches);
    }
    this.notify();
  }

  /** Register generated Replica metadata without persisting it with the window. */
  registerReplicaCollection(
    signature: string,
    definition: ReplicaCollectionPlan,
    args: ReplicaRow = {},
  ) {
    this.replicaPlans.set(signature, {
      definition: {
        ...definition,
        equalFilters: definition.equalFilters ? { ...definition.equalFilters } : undefined,
        excludeWhenSet: definition.excludeWhenSet ? [...definition.excludeWhenSet] : undefined,
      },
      args: cloneRow(args),
    });
  }

  acknowledgeCommand(commandId: string, committedRevision?: number) {
    const pending = this.pendingCommands.get(commandId);
    if (!pending) return;
    if (!committedRevision) return;
    pending.committedRevision = committedRevision;
    this.reconcileCommands();
  }

  rejectCommand(commandId: string) {
    const command = this.pendingCommands.get(commandId);
    if (!command || !this.pendingCommands.delete(commandId)) return;
    this.markWindowsForPatches(command.patches);
    this.notify();
  }

  /** True while an optimistic command still contributes visible patches. */
  hasPendingCommand(commandId: string): boolean {
    return this.pendingCommands.has(commandId);
  }

  applyTransaction(transaction: ReplicaTransaction, scope?: ReplicaScope): Promise<void> {
    const requestedScope = scope === undefined ? undefined : normalizeReplicaScope(scope);
    const application = this.enqueueApplication(() => this.applyTransactionNow(transaction, requestedScope));

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
    const application = this.enqueueApplication(() => this.materializeWindowNow(input));

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
    const application = this.enqueueApplication(async () => {
      const existing = this.liveQueries.get(input.signature);
      const deleted = new Set(input.deleted.map(String));
      const ids = (existing?.ids ?? []).filter((id) => !deleted.has(id));
      const present = new Set(ids);
      for (const row of input.upserts) {
        const rawID = row[input.key];
        const id = typeof rawID === "string" || typeof rawID === "number" ? String(rawID) : "";
        if (id && !present.has(id)) { ids.push(id); present.add(id); }
      }
      const upserts = new Map(input.upserts.map(row => [String(row[input.key]), row]));
      const rows = ids
        .map((id) => upserts.has(id) ? {...this.entities.get(input.entity)?.get(id),...upserts.get(id)} : this.entities.get(input.entity)?.get(id))
        .filter((row): row is ReplicaRow => row !== undefined);
      const plan = this.replicaPlans.get(input.signature)?.definition;
      const orderBy = input.orderBy ?? existing?.orderBy ?? plan?.orderBy;
      const orderDirection = input.orderDirection ?? existing?.orderDirection ?? plan?.orderDirection;
      if ((input.kind ?? existing?.kind) === 'replica' && orderBy) {
        rows.sort((a,b) => compareReplicaMembershipRows(a,b,input.key,orderBy,orderDirection));
      }
      await this.materializeWindowNow({
        ...input,
        rows,
        completeness: input.completeness ?? existing?.completeness ?? "partial",
        source: input.source ?? existing?.source ?? "server",
        removedIDs: [...deleted],
      }, { upserts: input.upserts, deleted: [...deleted] });
    });

    return application;
  }

  /**
   * Advance a set of ready Replica Collection cursors for one server
   * watermark. This is deliberately separate from window materialization:
   * watermarks contain no row data, so persistence must update only window and
   * cursor metadata in one storage transaction.
   */
  advanceWatermark(
    revision: number,
    signatures: readonly string[] = [],
    scope?: ReplicaScope,
  ): Promise<void> {
    const requestedScope = scope === undefined ? undefined : normalizeReplicaScope(scope);
    const application = this.enqueueApplication(async () => {
      if (requestedScope !== undefined && requestedScope !== this.scopeValue) return;
      if (!Number.isSafeInteger(revision) || revision < 0 || signatures.length === 0) return;

      const eligible = new Set(signatures);
      const nextQueries = new Map(this.liveQueries);
      const changedWindows: ReplicaWindow[] = [];
      for (const [signature, window] of nextQueries) {
        if (!eligible.has(signature) || window.kind !== "replica" || !window.cursor) continue;
        if (window.cursor.revision >= revision) continue;
        const advanced = { ...window, cursor: { ...window.cursor, revision } };
        nextQueries.set(signature, advanced);
        changedWindows.push(advanced);
      }
      if (changedWindows.length === 0) return;

      // Compute the shared floor exactly once from the complete next window
      // set. A watermark cannot outrun an older retained Replica Collection.
      const nextCursor = replicaTransactionFloor(this.cursorValue, nextQueries);
      const writeScope = this.scopeValue;
      if (this.storage?.advanceWatermark) {
        const receipt = await this.persist(() => this.storage!.advanceWatermark!(changedWindows, nextCursor, writeScope));
        this.acceptMetadataCommit(receipt, writeScope);
      } else if (this.storage?.replaceSnapshot) {
        // Compatibility for older/custom storage adapters. Current IndexedDB
        // and SQLite adapters implement the metadata-only operation above.
        const snapshot = storageSnapshotFrom(nextCursor, this.entities, nextQueries);
        await this.persist(() => this.storage!.replaceSnapshot!(snapshot, writeScope));
      } else if (this.storage?.replaceWindow) {
        // The original storage contract only exposed replaceWindow. Preserve
        // durability for those adapters by reusing one computed snapshot;
        // this path is intentionally serial and is not used by the bundled
        // normalized adapters.
        const snapshot = storageSnapshotFrom(nextCursor, this.entities, nextQueries);
        for (const window of changedWindows) {
          await this.persist(() => this.storage!.replaceWindow!(window, snapshot, writeScope));
        }
      }

      this.liveQueries = nextQueries;
      for (const window of changedWindows) this.markWindowChanged(window.signature, false);
      this.cursorValue = nextCursor;
      this.notify();
    });

    return application;
  }

  windowResumeState(signature: string): { cursor: ReplicaCursor; digest: string } | undefined {
    const window = this.liveQueries.get(signature);
    if (!window?.cursor || !window.hashes) return undefined;
    window.aggregateDigest ??= replicaAggregateDigest(window.hashes);
    return { cursor: { ...window.cursor }, digest: window.aggregateDigest };
  }

  getWindow(signature: string): ReplicaWindow | undefined {
    const window = this.liveQueries.get(signature);
    if (window?.hashes && !window.aggregateDigest) window.aggregateDigest = replicaAggregateDigest(window.hashes);
    return window ? cloneWindow(window) : undefined;
  }

  listWindows(): ReplicaWindow[] {
    return [...this.liveQueries.values()].map(cloneWindow);
  }

  windowRows<T extends ReplicaRow = ReplicaRow>(signature: string): T[] {
    return this.liveQuery<T>(signature).rows;
  }

  /**
   * Return only rows in the committed server membership. Optimistic rows are
   * deliberately excluded because this is used for resume keys and integrity
   * hashes, never for rendering.
   */
  committedWindowRows<T extends ReplicaRow = ReplicaRow>(signature: string): T[] {
    if (!this.scopeLoaded) return [];
    const window = this.liveQueries.get(signature);
    if (!window) return [];
    return window.ids
      .map((id) => this.entities.get(window.entity)?.get(id))
      .filter((row): row is ReplicaRow => row !== undefined)
      .map(cloneRow) as T[];
  }

  removeWindow(signature: string, scope?: ReplicaScope): Promise<void> {
    const application = this.enqueueApplication(async () => {
      if (scope !== undefined && normalizeReplicaScope(scope) !== this.scopeValue) return;
      if (!this.liveQueries.has(signature)) return;
      const nextQueries = new Map(this.liveQueries);
      const removed = nextQueries.get(signature)!;
      nextQueries.delete(signature);
      const snapshot = storageSnapshotFrom(this.cursorValue, this.entities, nextQueries);
      if (this.storage?.removeWindow) {
        await this.persist(() => this.storage!.removeWindow!(signature, snapshot, this.scopeValue));
      } else if (this.storage?.replaceSnapshot) {
        await this.persist(() => this.storage!.replaceSnapshot!(snapshot, this.scopeValue));
      }
      this.liveQueries = nextQueries;
      this.markWindowChanged(signature);

      this.markEntityChanged(removed.entity);
      this.notify(true);
    });

    return application;
  }

  clear(scope?: ReplicaScope): Promise<void> {
    const application = this.enqueueApplication(async () => {
      if (scope !== undefined && normalizeReplicaScope(scope) !== this.scopeValue) return;
      if (this.storage?.clear) await this.persist(() => this.storage!.clear!(this.scopeValue));
      this.cursorValue = undefined;
      this.entities = new Map();
      this.invalidateEntityVersions();
      for (const signature of this.liveQueries.keys()) this.markWindowChanged(signature);
      this.liveQueries = new Map();

      this.pendingCommands.clear();
      this.freshnessValue = "verifying";
      this.notify(true);
    });

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
  }, delta?: { upserts: ReplicaRow[]; deleted: string[] }) {
    if (input.scope !== undefined && normalizeReplicaScope(input.scope) !== this.scopeValue) return;
    if (!input.signature.trim() || !input.entity.trim() || !input.key.trim()) {
      throw new Error("replica materialization requires signature, entity, and key");
    }
    // This materialization changes one entity map and one membership. Other
    // published maps/windows stay immutable and can be shared during persistence.
    const nextEntities = new Map(this.entities);
    const nextQueries = new Map(this.liveQueries);
    const epochChanged = Boolean(input.cursor && this.cursorValue && input.cursor.epoch !== this.cursorValue.epoch);
    if (epochChanged) {
      nextEntities.clear();
      nextQueries.clear();
    }
    const entityRows = residentRows(nextEntities.get(input.entity));
    nextEntities.set(input.entity, entityRows);
    const ids: string[] = [];
    const changedRowIDs = new Set<string>();
    const changedIDs = delta && !epochChanged ? new Set(delta.upserts.map(row => String(row[input.key]))) : undefined;
    for (const row of input.rows) {
      const rawID = row[input.key];
      const id = typeof rawID === "string" || typeof rawID === "number" ? String(rawID) : "";
      if (!id) continue;
      ids.push(id);
      if (changedIDs && !changedIDs.has(id)) continue;
      // Different Replica Collections may project different columns from the
      // same table. They all hydrate one normalized entity, so a narrow
      // projection must update the fields it owns without erasing fields
      // supplied by another collection.
      const priorRow = entityRows.get(id);
      if (priorRow && Object.keys(row).every(key => Object.prototype.hasOwnProperty.call(priorRow, key) && sameReplicaValue(priorRow[key], row[key]))) continue;
      setResidentRow(entityRows,id, { ...(priorRow ?? {}), ...cloneRow(row) });
      changedRowIDs.add(id);
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
      aggregateDigest: input.hashes ? replicaAggregateDigest(input.hashes) : undefined,
      hashes: input.hashes ? { ...input.hashes } : undefined,
    };
    nextQueries.set(input.signature, window);
    const presentIDs = new Set(ids);
    for (const id of input.removedIDs ?? []) {
      const stillReferenced = presentIDs.has(id) || [...nextQueries.values()].some(candidate => candidate.signature !== input.signature && candidate.entity === input.entity && [...this.windowsForRow(input.entity,id)].includes(candidate.signature));
      if (!stillReferenced && deleteResidentRow(entityRows,id)) changedRowIDs.add(id);
    }

    // A collection cursor proves only that collection's materialized rows.
    // Advancing the connection-wide transaction floor to the newest single
    // snapshot can discard an older, still-unapplied transaction for another
    // collection. The only revision proven by snapshots alone is the minimum
    // cursor shared by every materialized Replica window in the epoch.
    const nextCursor = replicaTransactionFloor(this.cursorValue, nextQueries);
    const snapshot = storageSnapshotFrom(nextCursor, nextEntities, nextQueries);
    const membershipChanged = !sameWindowRowDefinition(previous, window);
    const rowsChanged = membershipChanged || changedRowIDs.size > 0;
    const writeScope = this.scopeValue;
    let metadataCommit: void | ReplicaMetadataCommit | ReplicaStorageCommit = undefined;
    if (!epochChanged && this.storage?.applyWindowDelta && (delta || !rowsChanged)) {
      const writeDelta: ReplicaWindowDelta = delta ? {
        upserts: this.storage.supportsMembershipDeltas ? delta.upserts : delta.upserts.filter(row => changedRowIDs.has(String(row[input.key]))),
        deleted: delta.deleted,
        ...(this.storage.supportsMembershipDeltas ? { memberships: membershipEdits(window, previous, delta.upserts) } : {}),
      } : { upserts: [], deleted: [], ...(this.storage.supportsMembershipDeltas ? {
        memberships: Object.keys(window.hashes ?? {}).filter(id => window.hashes![id] !== previous?.hashes?.[id]).map(id => ({ id })),
      } : {}) };
      metadataCommit = await this.persist(() => this.storage!.applyWindowDelta!(window, writeDelta, snapshot, writeScope));
    } else if (this.storage?.replaceWindow) {
      await this.persist(() => this.storage!.replaceWindow!(window, snapshot, writeScope,input.rows));
    } else if (this.storage?.replaceSnapshot) {
      await this.persist(() => this.storage!.replaceSnapshot!(snapshot, writeScope));
    }
    if (epochChanged) this.invalidateEntityVersions();
    // The adapter can prove that this metadata commit immediately follows our
    // last reconciled sequence. Our next maps already contain that exact write.
    // A peer write in between breaks the proof and retains normal catch-up.
    if (!await this.acceptStorageCommit(metadataCommit, writeScope, nextEntities, nextQueries) && this.storage?.readChanges) {
      await this.mergeStoredChanges(nextEntities, nextQueries);
    }
    this.entities = nextEntities;
    this.liveQueries = nextQueries;
    if(window.kind==='replica' && window.completeness==='complete' && !window.truncated && window.ids.every(id=>nextEntities.get(window.entity)?.has(id)))this.residentPartialTables.delete(window.entity);
    if (changedRowIDs.size) {
      this.markEntityChanged(input.entity, changedRowIDs);
      const affected = new Set<string>();
      for (const id of changedRowIDs) for (const signature of this.windowsForRow(input.entity, id)) affected.add(signature);
      for (const signature of affected) if (signature !== input.signature) this.markWindowChanged(signature);
    }
    this.markWindowChanged(input.signature, rowsChanged);

    this.cursorValue = nextCursor;
    if (input.source === "server") this.freshnessValue = "current";
    this.notify(true);
  }

  private async applyTransactionNow(transaction: ReplicaTransaction, scope?: ReplicaScope) {
    if (scope !== undefined && scope !== this.scopeValue) return;
    validateTransaction(transaction);
    if (this.cursorValue?.epoch === transaction.cursor.epoch && transaction.cursor.revision <= this.cursorValue.revision) {
      return;
    }

    const nextEntities = new Map(this.entities);
    const copiedEntities = new Set<string>();
    // Copy only changed memberships; untouched immutable windows stay shared.
    const nextQueries = new Map(this.liveQueries);
    const changedWindows = new Set<string>();
    const epochChanged = Boolean(this.cursorValue && this.cursorValue.epoch !== transaction.cursor.epoch);
    if (epochChanged) {
      for (const signature of nextQueries.keys()) changedWindows.add(signature);
      nextEntities.clear();
      nextQueries.clear();
    }
    for (const change of transaction.changes) {
      if (!epochChanged) for (const signature of this.windowsForRow(change.entity, change.id)) changedWindows.add(signature);
      if (!copiedEntities.has(change.entity)) {
        nextEntities.set(change.entity, residentRows(nextEntities.get(change.entity)));
        copiedEntities.add(change.entity);
      }
      const rows = nextEntities.get(change.entity)!;
      if (change.operation === "delete") {
        deleteResidentRow(rows,change.id);

      }
      // Change-feed rows are projections, just like collection snapshots.
      // A narrow subscription must not erase fields supplied by another one.
      else if (change.newValue) setResidentRow(rows,change.id, {...rows.get(change.id),...cloneRow(change.newValue)});
    }
    const deleted = new Map<string, Set<string>>();
    for (const change of transaction.changes) if (change.operation === "delete") {
      let ids = deleted.get(change.entity);
      if (!ids) { ids = new Set(); deleted.set(change.entity, ids); }
      ids.add(change.id);
    }
    for (const signature of changedWindows) {
      const window = nextQueries.get(signature);
      const ids = window && deleted.get(window.entity);
      if (window && ids) nextQueries.set(signature, { ...window, ids: window.ids.filter(id => !ids.has(id)) });
    }
    for (const membership of transaction.memberships ?? []) {
      nextQueries.set(membership.signature, normalizeWindow(membership));
      changedWindows.add(membership.signature);
    }

    const snapshot = storageSnapshotFrom(transaction.cursor, nextEntities, nextQueries);
    const writeScope = this.scopeValue;
    const receipt = await this.persist(() => this.storage?.applyTransaction(transaction, snapshot, writeScope));
    if (!await this.acceptStorageCommit(receipt, writeScope, nextEntities, nextQueries) && this.storage?.readChanges) await this.mergeStoredChanges(nextEntities, nextQueries);

    if (epochChanged) {
      this.invalidateEntityVersions();

    }
    // Publish the whole committed transaction in one state swap and notify UI
    // exactly once. No subscriber can observe a partial entity/query update.
    this.entities = nextEntities;
    this.liveQueries = nextQueries;
    const changedRows = new Map<string, Set<string>>();
    for (const change of transaction.changes) {
      let ids = changedRows.get(change.entity);
      if (!ids) { ids = new Set(); changedRows.set(change.entity, ids); }
      ids.add(change.id);
    }
    for (const [entity, ids] of changedRows) this.markEntityChanged(entity, ids);
    for (const signature of changedWindows) this.markWindowChanged(signature);
    this.cursorValue = { ...transaction.cursor };
    this.freshnessValue = "current";
    // A change-feed transaction carries normalized row changes, but not the
    // per-subscription membership windows. Keep the optimistic command until
    // the Reducer result supplies its committed revision; otherwise a newly
    // created row can disappear from a complete Replica Collection between
    // the transaction and its membership delta.
    this.reconcileCommands(false);
    this.notify(true);
  }

  /** Returned rows, including nested JSON, are immutable shared values. */
  entity<T extends ReplicaRow = ReplicaRow>(entity: string, id: string): T | undefined {
    if (!this.scopeLoaded) return undefined;
    this.touchRow(entity, id);
    let row = this.entities.get(entity)?.get(id);
    let selected = row;
    for (const command of this.pendingCommands.values()) {
      for (const patch of command.patches) {
        if ((patch.entity ?? patch.collection) !== entity || patch.rowId !== id) continue;
        if (patch.op === "delete") selected = undefined;
        if (patch.op === "insert") selected = patch.fields as ReplicaRow;
        if (patch.op === "upsert") selected = patch.fields as ReplicaRow;
        if (patch.op === "patch") selected = { ...(selected ?? {}), ...(patch.fields as ReplicaRow) };
      }
    }
    return immutable(selected) as T | undefined;
  }

  /** Resolve several IDs from one atomic Local Replica version. */
  entityBatch<T extends ReplicaRow = ReplicaRow>(entity: string, ids: readonly string[]): Array<T | undefined> {
    if (!this.scopeLoaded) return ids.map(() => undefined);
    const base = this.entities.get(entity);
    if (this.pendingCommands.size === 0) return ids.map(id => {
      this.touchRow(entity, id);
      const row = base?.get(id);
      return row === undefined ? undefined : immutable(row) as T;
    });
    const rows = new Map<string, ReplicaRow | undefined>();
    for (const id of ids) { this.touchRow(entity, id); rows.set(id, base?.get(id)); }
    if (rows.size) for (const command of this.pendingCommands.values()) {
      for (const patch of command.patches) {
        if ((patch.entity ?? patch.collection) !== entity || !rows.has(patch.rowId)) continue;
        if (patch.op === 'delete') rows.set(patch.rowId, undefined);
        else if (patch.op === 'insert' || patch.op === 'upsert') rows.set(patch.rowId, patch.fields as ReplicaRow);
        else if (patch.op === 'patch') rows.set(patch.rowId, { ...(rows.get(patch.rowId) ?? {}), ...(patch.fields as ReplicaRow) });
      }
    }
    // Preserve caller order; duplicate IDs share an immutable value.
    return ids.map(id => {
      const row = rows.get(id);
      return row === undefined ? undefined : immutable(row) as T;
    });
  }

  /** All cached rows for one normalized entity, including optimistic overlays. */
  entityRows<T extends ReplicaRow = ReplicaRow>(entity: string): T[] {
    if (!this.scopeLoaded) return [];
    if (this.pendingCommands.size === 0) {
      return this.cachedEntityRows(entity).slice() as T[];
    }
    // Apply the journal once for the table, not once per row. Keep deleted
    // entries as tombstones until projection so delete/reinsert retains the
    // established ID order. Base rows and journal fields stay untouched.
    const rows = new Map<string, ReplicaRow | undefined>(this.entities.get(entity));
    for (const command of this.pendingCommands.values()) {
      for (const patch of command.patches) {
        if ((patch.entity ?? patch.collection) !== entity) continue;
        if (patch.op === "delete") rows.set(patch.rowId, undefined);
        else if (patch.op === "insert" || patch.op === "upsert") rows.set(patch.rowId, patch.fields as ReplicaRow);
        else if (patch.op === "patch") rows.set(patch.rowId, { ...(rows.get(patch.rowId) ?? {}), ...(patch.fields as ReplicaRow) });
      }
    }
    const result: T[] = [];
    // Freeze final projections; their nested fields already belong to the journal.
    for (const row of rows.values()) if (row !== undefined) result.push(immutable(row) as T);
    return result;
  }

  /** Internal shared array. Public entityRows keeps its caller-owned container. */
  cachedEntityRows(entity: string): readonly ReplicaRow[] {
    if (!this.scopeLoaded) return [];
    const version = this.entityVersion(entity);
    const cached = this.entityRowArrays.get(entity);
    if (cached?.version === version) return cached.rows;
    const rows = this.pendingCommands.size ? this.entityRows(entity) : Array.from(this.entities.get(entity)?.values() ?? []);
    if (this.entityRowArrays.size >= 64) this.entityRowArrays.delete(this.entityRowArrays.keys().next().value!);
    this.entityRowArrays.set(entity, { version, rows });
    return rows;
  }

  /** Lazily retain at most four equality indexes. Pending overlays use the full
   * evaluator so journal ordering and insertion semantics remain unchanged. */
  equalityRows(entity: string, column: string, value: unknown): readonly ReplicaRow[] {
    if (!this.scopeLoaded) return [];
    if (this.pendingCommands.size) return this.entityRows(entity);
    const key = JSON.stringify([entity, column]);
    let index = this.equalityIndexes.get(key);
    if (!index) {
      if (this.equalityIndexes.size >= 4) this.equalityIndexes.delete(this.equalityIndexes.keys().next().value!);
      index = { entity, column, values: new Map(), bytes: 0, scan: this.entities.get(entity)?.entries() };
      this.equalityIndexes.set(key, index);
    }
    this.advanceEqualityIndex(index,512);
    if (index.scan) {
      this.scheduleEqualityIndexes();
      // Preserve synchronous query results while the extra index builds in
      // bounded turns. No partially indexed subset can omit matching rows.
      return this.cachedEntityRows(entity);
    }
    const table = this.entities.get(entity);
    const ids = [...index.values.get(JSON.stringify(value) ?? 'undefined') ?? []];
    if (table instanceof ResidentMap) ids.sort((a, b) => table.position(a) - table.position(b));
    return ids.map(id => table!.get(id)!);
  }

  private advanceEqualityIndex(index: EqualityIndex, limit: number): void {
    for (let i = 0; i < limit && index.scan; i++) {
      const entry = index.scan.next();
      if (entry.done) { index.scan = undefined; break; }
      const id = entry.value[0];
      this.indexEqualityRow(index,id,this.entities.get(index.entity)?.get(id));
    }
  }

  private scheduleEqualityIndexes(): void {
    if (this.equalityTimer !== undefined) return;
    this.equalityTimer = setTimeout(() => {
      this.equalityTimer = undefined;
      const index = [...this.equalityIndexes.values()].find(index=>index.scan);
      if (!index || this.disposed) return;
      this.advanceEqualityIndex(index,512);
      if ([...this.equalityIndexes.values()].some(index=>index.scan)) this.scheduleEqualityIndexes();
    },0);
  }

  private indexEqualityRow(index: EqualityIndex, id: string, row?: ReplicaRow, priorRow?: ReplicaRow): void {
    const prior = priorRow ? JSON.stringify(priorRow[index.column]) ?? 'undefined' : undefined;
    const next = row ? JSON.stringify(row[index.column]) ?? 'undefined' : undefined;
    if (prior !== undefined && prior !== next) {
      const bucket = index.values.get(prior);
      if (bucket?.delete(id)) index.bytes -= 64 + id.length * 2;
      if (!bucket?.size) index.values.delete(prior);
    }
    if (next !== undefined) {
      let bucket = index.values.get(next);
      if (!bucket) { bucket = new Set(); index.values.set(next, bucket); }
      if (!bucket.has(id)) { bucket.add(id); index.bytes += 64 + id.length * 2; }
    }
  }

  /** Serialize against confirmed publications, then capture just the pending journal.
   * IndexedDB keeps the base transaction consistent without copying any tables.
   */
  pendingReducerPatches(): LocalPatch[] {
    return [...this.pendingCommands.values()].flatMap(command => command.patches.map(patch => patch.op === 'delete'
      ? {entity:patch.entity ?? patch.collection!,rowId:patch.rowId,op:'delete' as const}
      : {entity:patch.entity ?? patch.collection!,rowId:patch.rowId,op:patch.op === 'upsert' ? 'insert' as const : patch.op,fields:structuredClone(patch.fields) as ReplicaRow}
    ));
  }

  withReadView<T>(coverage: () => ReadCoverage, pending: boolean | readonly LocalPatch[], run: (view: ReducerReadView) => Promise<T>, residentOnly = false): Promise<T> {
    if (residentOnly) {
      if (!this.scopeLoaded) return Promise.reject(new Error('Local replica scope is not ready'));
      const known = coverage();
      const patches: LocalPatch[] = typeof pending !== 'boolean' ? [...pending] : pending ? [...this.pendingCommands.values()].flatMap(command => command.patches.map(patch => patch.op === 'delete'
        ? {entity:patch.entity ?? patch.collection!,rowId:patch.rowId,op:'delete' as const}
        : {entity:patch.entity ?? patch.collection!,rowId:patch.rowId,op:patch.op === 'upsert' ? 'insert' as const : patch.op,fields:patch.fields as ReplicaRow}
      )) : [];
      // A short speculative read never waits for disk. Its publication is
      // checked against the captured execution version, and durable admission
      // re-executes against the coordinated journal before sending anything.
      return run(overlayReadView(memoryReadView(this.entities,this.residentCoverage(known),true),known,patches));
    }
    const operation = this.enqueueApplication(async () => {
      if (!this.scopeLoaded) throw new Error('Local replica scope is not ready');
      await this.persistence;
      const known = coverage();
      const patches: LocalPatch[] = typeof pending !== 'boolean' ? [...pending] : pending ? [...this.pendingCommands.values()].flatMap(command => command.patches.map(patch => {
        const entity = patch.entity ?? patch.collection!;
        return patch.op === 'delete' ? {entity,rowId:patch.rowId,op:'delete' as const} : {entity,rowId:patch.rowId,op:patch.op === 'upsert' ? 'insert' as const : patch.op,fields:patch.fields as ReplicaRow};
      })) : [];
      const apply = (base: ReducerReadView) => run(overlayReadView(base,known,patches));
      // The visible working set is already resident. Execute against that
      // consistent view first instead of sending hot row reads back to disk.
      // A missing row/field retries the whole prediction in one disk read
      // transaction, never mixes rows from two different snapshots.
      try { return await apply(memoryReadView(this.entities,this.residentCoverage(known),true)); }
      catch (error) {
        if (!(error instanceof Error) || error.name !== 'IncompleteReplicaError' || !this.storage?.withReadView) throw error;
        return this.storage.withReadView(this.scopeValue,known,apply);
      }
    }, true);

    return operation;
  }

  /** SDK execution snapshot: capture immutable row references now, copy a table only when used. */
  captureExecutionRows(includePending: boolean): (entity: string) => ReplicaRow[] {
    const captured = new Map(this.entities);
    const patches = includePending ? [...this.pendingCommands.values()].flatMap(command => command.patches) : [];
    const loaded = this.scopeLoaded;
    const materialized = new Map<string, ReplicaRow[]>();
    return entity => {
      const prior = materialized.get(entity);
      if (prior) return prior;
      const rows = new Map(loaded ? captured.get(entity) ?? [] : []);
      const ids = new Set(rows.keys());
      if (loaded) for (const patch of patches) {
        if ((patch.entity ?? patch.collection) !== entity) continue;
        ids.add(patch.rowId);
        if (patch.op === "delete") rows.delete(patch.rowId);
        else if (patch.op === "patch") rows.set(patch.rowId, { ...rows.get(patch.rowId), ...patch.fields } as ReplicaRow);
        else rows.set(patch.rowId, patch.fields as ReplicaRow);
      }
      const result = [...ids].flatMap(id => { const row = rows.get(id); return row ? [cloneRow(row)] : []; });
      materialized.set(entity, result);
      return result;
    };
  }

  /** Complete registered slices prove narrower reads without another subscription. */
  captureReadCoverage(base: ReadCoverage): ReadCoverage {
    const coverage = { ...base };
    for (const [signature, {definition, args}] of this.replicaPlans) {
      const known = coverage[definition.table];
      if (!known || !this.windowIsComplete(signature)) continue;
      const terms: DataPredicate[] = [];
      let valid = true;
      for (const [argument, column] of Object.entries(definition.equalFilters ?? {})) {
        const value = args[argument];
        if (value === undefined || value !== null && !['string','number','boolean'].includes(typeof value)) { valid = false; break; }
        terms.push({column, op:'eq', value: value as string | number | boolean | null});
      }
      if (!valid) continue;
      for (const column of definition.excludeWhenSet ?? []) terms.push({column, op:'isNull'});
      coverage[definition.table] = {
        ...known,
        complete: known.complete || terms.length === 0,
        completeWhere: terms.length ? [...(known.completeWhere ?? []), {and:terms}] : known.completeWhere,
      };
    }
    return coverage;
  }

  /** Read execution eligibility without copying a window's ordered IDs and hashes. */
  windowIsComplete(signature: string): boolean {
    const window = this.liveQueries.get(signature);
    return this.scopeLoaded && window?.completeness === "complete" && window.truncated !== true;
  }

  /** A watermark reads only cursor/integrity presence, never copied IDs or hashes. */
  windowCanAdvance(signature: string, revision: number): boolean {
    const window = this.liveQueries.get(signature);
    return Boolean(window?.cursor && window.cursor.revision < revision && window.hashes);
  }

  /** SDK watch snapshots share the immutable resident rows. */
  watchRows<T extends ReplicaRow>(signature: string, cache: Map<string, { version: number; row: T | undefined }>): T[] {
    const window = this.scopeLoaded ? this.liveQueries.get(signature) : undefined;
    if (!window) { cache.clear(); return []; }
    // A pending write to a different row must not disable shared immutable
    // snapshots for every newly mounted view in the application.
    let pendingRows: Set<string> | undefined;
    for (const command of this.pendingCommands.values()) {
      for (const patch of command.patches) {
        if ((patch.entity ?? patch.collection) === window.entity) (pendingRows ??= new Set()).add(patch.rowId);
      }
    }
    const read = (id: string): T | undefined => {
      this.touchRow(window.entity, id);
      const version = this.rowVersion(window.entity, id);
      const prior = cache.get(id);
      if (prior?.version === version) return prior.row;
      const base = !pendingRows?.has(id) ? this.entities.get(window.entity)?.get(id) : undefined;
      let next: T | undefined;
      if (base) next = base as T;
      else next = this.entity<T>(window.entity, id);
      // A matching server echo or removal of its prediction changes protocol
      // versions without changing the row the component sees.
      const row = prior && sameReplicaValue(prior.row, next) ? prior.row as T | undefined : next;
      cache.set(id, { version, row });
      return row;
    };
    const ids = this.effectiveMembership(window, read);
    const retained = new Set(ids);
    for (const id of cache.keys()) if (!retained.has(id)) cache.delete(id);
    return ids.map(read).filter((row): row is T => row !== undefined);
  }

  /** Exact only when an authoritative, non-truncated Replica Collection covers the entity. */
  entityCompleteness(entity: string): "complete" | "partial" {
    if (!this.scopeLoaded || this.residentPartialTables.has(entity)) return "partial";
    return [...this.liveQueries.values()].some((window) => (
      window.kind === "replica"
      && window.entity === entity
      && window.completeness === "complete"
      && window.truncated !== true
    )) ? "complete" : "partial";
  }

  // Bounded derived snapshots, containing only rows in recently read windows.
  private readonly readSnapshots = new Map<string, { version: number; rowsVersion: number; freshness?: ReplicaFreshness; rows: ReplicaRow[]; cache: Map<string, { version: number; row: ReplicaRow | undefined }>; result?: LiveQueryResult }>();
  liveQuerySnapshot<T extends ReplicaRow = ReplicaRow>(signature: string): LiveQueryResult<T> {
    let snapshot = this.readSnapshots.get(signature);
    if (!snapshot) {
      if (this.readSnapshots.size >= 64) {
        const cold = [...this.readSnapshots.keys()].find(key => !this.retainedWindows.has(key));
        if (cold !== undefined) this.readSnapshots.delete(cold);
      }
      snapshot = { version: -1, rowsVersion: -1, rows: [], cache: new Map() };
    }
    // Mounted windows cannot be evicted. Reordering them on every store read
    // only allocates Map tombstones; LRU order matters for cold windows alone.
    if (!this.retainedWindows.has(signature)) this.readSnapshots.delete(signature);
    this.readSnapshots.set(signature, snapshot);
    if (snapshot.version !== this.windowVersion(signature) || snapshot.freshness !== this.freshnessValue) {
      const rowsVersion = this.windowRowsVersion(signature);
      if (snapshot.rowsVersion !== rowsVersion) {
        const rows = this.watchRows(signature, snapshot.cache);
        if (rows.length !== snapshot.rows.length || rows.some((row, index) => row !== snapshot.rows[index])) snapshot.rows = rows;
        snapshot.rowsVersion = rowsVersion;
      }
      const next = this.liveQuery(signature, snapshot.rows);
      const previous = snapshot.result;
      if (!previous || previous.rows !== next.rows || !sameReplicaValue({...previous,rows:undefined}, {...next,rows:undefined})) snapshot.result = next;
      snapshot.version = this.windowVersion(signature);
      snapshot.freshness = this.freshnessValue;
    }
    return snapshot.result as LiveQueryResult<T>;
  }

  liveQuery<T extends ReplicaRow = ReplicaRow>(signature: string, cachedRows?: T[]): LiveQueryResult<T> {
    return this.windowResult<T>(signature, cachedRows);
  }

  private windowResult<T extends ReplicaRow>(signature: string, cachedRows?: T[]): LiveQueryResult<T> {
    if (!this.scopeLoaded) {
      return { rows: [], ids: [], source: "cache", completeness: "partial", freshness: this.freshnessValue };
    }
    const membership = this.liveQueries.get(signature);
    if (!membership) {
      return { rows: [], ids: [], source: "cache", completeness: "partial", freshness: this.freshnessValue };
    }
    const ids = cachedRows ? cachedRows.map(row => String(row[membership.key])) : this.effectiveMembership(membership);
    const rows = cachedRows ?? ids
      .map((id) => this.entity<T>(membership.entity, id))
      .filter((row): row is T => row !== undefined);
    const metadata = windowResultMetadata(membership.resultSkeleton, membership.resultPath);
    return {
      rows,
      ids,
      ...metadata,
      source: this.freshnessValue === "current" ? membership.source : "cache",
      completeness: membership.ids.every(id=>this.entities.get(membership.entity)?.has(id)) ? membership.completeness : 'partial',
      freshness: this.freshnessValue,
    };
  }

  /** Rows and protocol-owned completeness for one Replica Collection window. */
  collectionState<T extends ReplicaRow = ReplicaRow>(signature: string, cachedRows?: T[]): ReplicaCollectionState<T> {
    const result = this.windowResult<T>(signature, cachedRows);
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
        this.markWindowsForPatches(command.patches);
        changed = true;
      }
    }
    if (changed && notify) this.notify();
  }

  private async acceptStorageCommit(receipt: void | ReplicaMetadataCommit | ReplicaStorageCommit, scope: ReplicaScope,
    entities: Map<string, Map<string, ReplicaRow>>, windows: Map<string, ReplicaWindow>): Promise<boolean> {
    const previous = this.storageSequence;
    const adjacent = this.acceptMetadataCommit(receipt, scope);
    if (receipt && 'changes' in receipt && receipt.scope === scope && scope === this.scopeValue
      && Number.isSafeInteger(receipt.previousSequence) && receipt.previousSequence >= previous
      && Number.isSafeInteger(receipt.sequence) && receipt.sequence === receipt.previousSequence + 1) {
      // A rejected stale write may leave the disk row at an older sequence.
      // Apply that authority correction before peer catch-up; later disk changes
      // still win, and a gap must not advance our reconciled sequence.
      this.storageSequence = previous;
      await this.mergeStoredChanges(entities, windows, receipt.changes);
      if (!adjacent) this.storageSequence = previous;
    }
    return adjacent;
  }

  private acceptMetadataCommit(receipt: void | ReplicaMetadataCommit, scope: ReplicaScope): boolean {
    if (!receipt || receipt.scope !== scope || scope !== this.scopeValue
      || !Number.isSafeInteger(receipt.previousSequence) || receipt.previousSequence < 0
      || !Number.isSafeInteger(receipt.sequence) || receipt.sequence !== receipt.previousSequence + 1
      || receipt.previousSequence !== this.storageSequence) return false;
    this.storageSequence = receipt.sequence;
    return true;
  }

  private persist<T>(operation: () => Promise<T> | undefined) {
    const attempt = this.persistence.then(async () => await operation());
    this.persistence = attempt.then(() => undefined, () => undefined);
    return attempt;
  }

  private notify(executionChanged = false) {
    if (this.disposed) return;
    if(executionChanged)this.trimResidentRows();
    if (executionChanged) {
      this.executionVersionValue += 1;
      if (this.invalidateSubscriptions) {
        this.executionVersionFloor = this.executionVersionValue;
        this.executionEntityVersions.clear();
        this.executionRowVersions.clear();
      } else for (const [entity, ids] of this.changedEntities) {
        this.executionEntityVersions.set(entity, this.executionVersionValue);
        for (const id of ids) {
          if (this.entities.get(entity)?.has(id)) {
            if (!this.recency.setVersion(`${entity}\0${id}`,this.executionVersionValue,true)) this.executionRowVersions.set(JSON.stringify([entity,id]),this.executionVersionValue);
          }
          else this.executionRowVersions.delete(JSON.stringify([entity,id]));
        }
      }
    }
    for (const [entity,ids] of this.changedEntities) for (const id of ids) {
      if (!this.hasResidentRow(entity,id) && !this.rowListeners.get(entity)?.has(id)) this.rowVersions.delete(`${entity}\0${id}`);
    }
    this.versionValue += 1;
    const callbacks = new Map<(changedIDs?: ReadonlySet<string>) => void, ReadonlySet<string> | undefined>();
    for (const listener of this.listeners) callbacks.set(listener, undefined);
    const add = (group?: Set<() => void>, ids?: ReadonlySet<string>) => {
      if (group) for (const callback of group) callbacks.set(callback, ids);
    };
    if (this.freshnessValue === "offline") add(this.offlineListeners);
    const freshnessChanged = this.notifiedFreshness !== this.freshnessValue;
    this.notifiedFreshness = this.freshnessValue;
    if (this.invalidateSubscriptions || freshnessChanged) {
      for (const group of this.entityListeners.values()) add(group);
      for (const index of this.rowListeners.values()) for (const group of index.values()) add(group);
      for (const group of this.windowListeners.values()) add(group);
    } else {
      for (const [entity, ids] of this.changedEntities) {
        add(this.entityListeners.get(entity), ids);
        const index = this.rowListeners.get(entity);
        for (const id of ids) add(index?.get(id), ids);
      }
      for (const signature of this.changedWindows) add(this.windowListeners.get(signature));
    }
    // Clear before callbacks so reentrant optimistic writes get their own batch.
    this.changedEntities.clear(); this.changedWindows.clear(); this.invalidateSubscriptions = false;
    for (const [callback, ids] of callbacks) callback(ids);
  }

  /** Residency changes must wake every overlapping window, not only the lease
   * that loaded the rows. Otherwise a cached empty projection can outlive them. */
  private markWindowsForRows(entity: string, ids: ReadonlySet<string>, except?: string) {
    if (!ids.size) return;
    for (const window of this.liveQueries.values()) {
      if (window.signature !== except && window.entity === entity && window.ids.some(id => ids.has(id))) {
        this.markWindowChanged(window.signature);
      }
    }
  }

  private markEntityChanged(entity: string, ids?: Iterable<string>) {
    const version = ++this.entityVersionClock;
    this.entityRowArrays.delete(entity);
    this.entityVersions.set(entity, version);
    let changed = this.changedEntities.get(entity);
    if (!changed) { changed = new Set(); this.changedEntities.set(entity, changed); }
    if (ids === undefined) for (const [key, index] of this.equalityIndexes) if (index.entity === entity) this.equalityIndexes.delete(key);
    for (const id of ids ?? this.entities.get(entity)?.keys() ?? []) {
      if (!this.recency.setVersion(`${entity}\0${id}`, version)) this.rowVersions.set(`${entity}\0${id}`, version);
      changed.add(id);
    }
  }

  private invalidateEntityVersions() {
    this.invalidateSubscriptions = true;
    this.residencyVersionValue++;
    this.entityVersionFloor = ++this.entityVersionClock;
    this.equalityIndexes.clear();
    if (this.equalityTimer !== undefined) clearTimeout(this.equalityTimer);
    this.equalityTimer = undefined;
    this.entityRowArrays.clear();
    this.entityVersions.clear();
    this.rowVersions.clear();
    this.readSnapshots.clear();
    this.windowVersionFloor = ++this.windowVersionClock;
    this.windowVersions.clear(); this.windowRowsVersions.clear();
  }

  private markWindowChanged(signature: string, rowsChanged = true) {
    this.changedWindows.add(signature);
    this.windowVersionClock += 1;
    if (!this.liveQueries.has(signature) && !this.windowListeners.has(signature) && !this.retainedWindows.has(signature)) {
      this.windowVersionFloor = this.windowVersionClock;
      this.windowVersions.delete(signature); this.windowRowsVersions.delete(signature); this.readSnapshots.delete(signature);
      return;
    }
    this.windowVersions.set(signature, this.windowVersionClock);
    if (rowsChanged) this.windowRowsVersions.set(signature, this.windowVersionClock);
  }

  private markWindowsForPatches(patches: readonly OptimisticPatch[]) {
    const affected = new Set<string>();
    for (const patch of patches) {
      const entity = patch.entity ?? patch.collection;
      if (entity) this.markEntityChanged(entity, [patch.rowId]);
      if (entity) for (const signature of this.windowsForRow(entity, patch.rowId)) affected.add(signature);
      for (const [signature, window] of this.liveQueries) {
        if (window.entity !== entity) continue;
        // A complete Replica Collection can gain a new ID only when its
        // generated membership plan is available. Partial windows and Live
        // Queries intentionally retain their committed membership.
        if (window.kind === "replica" && window.completeness === "complete" && window.truncated !== true
          && this.replicaPlans.has(signature)) affected.add(signature);
      }
    }
    for (const signature of affected) this.markWindowChanged(signature);
  }

  /**
   * Apply optimistic membership transiently. The persisted window.ids remain
   * the committed server membership, so rejection and reconciliation are
   * reversible and never write optimistic IDs to storage.
   */
  private effectiveMembership(window: ReplicaWindow, read: (id: string) => ReplicaRow | undefined = id => this.entity(window.entity, id)): string[] {
    const plan = this.replicaPlans.get(window.signature);
    const patches = [...this.pendingCommands.values()]
      .flatMap((command) => command.patches)
      .filter((patch) => (patch.entity ?? patch.collection) === window.entity);
    if (
      window.kind !== "replica"
      || window.completeness !== "complete"
      || window.truncated === true
      || !plan
      || plan.definition.table !== window.entity
      || plan.definition.key !== window.key
      || patches.length === 0
    ) return [...window.ids];

    const ids = new Set(window.ids);
    const touched = new Set(patches.map((patch) => patch.rowId));
    for (const id of touched) {
      const row = read(id);
      if (row && replicaRowMatchesPlan(row, plan.definition, plan.args)) ids.add(id);
      else ids.delete(id);
    }
    const effectiveIDs = [...ids];
    const membershipChanged = ids.size !== window.ids.length || window.ids.some((id) => !ids.has(id));
    if (!membershipChanged && !plan.definition.orderBy) return [...window.ids];

    const committed = new Set(window.ids);
    const additions = [...ids].filter((id) => !committed.has(id));
    const retained = window.ids.filter((id) => ids.has(id));
    const orderBy = plan.definition.orderBy;
    if (!orderBy) return [...retained, ...additions.sort(compareReplicaKeys)];
    // The comparator may run O(n log n) times. Resolve each immutable row once.
    const rows = new Map(effectiveIDs.map(id => [id, read(id)]));
    return effectiveIDs.sort((left, right) => compareReplicaMembershipRows(
      rows.get(left),
      rows.get(right),
      window.key,
      orderBy,
      plan.definition.orderDirection,
    ));
  }
}

const membershipSizes = new WeakMap<string[], number>();
function windowMembershipSize(window: ReplicaWindow): number {
  let size = membershipSizes.get(window.ids);
  if (size === undefined) {
    size = 256;
    for (const id of window.ids) size += 144 + id.length * 2;
    membershipSizes.set(window.ids, size);
  }
  return size + (window.hashes ? window.ids.length * 112 : 0);
}

function sameWindowRowDefinition(previous: ReplicaWindow | undefined, next: ReplicaWindow): boolean {
  return previous !== undefined && previous.entity === next.entity && previous.key === next.key
    && previous.kind === next.kind && previous.completeness === next.completeness
    && previous.truncated === next.truncated && previous.orderBy === next.orderBy
    && previous.orderDirection === next.orderDirection && previous.ids.length === next.ids.length
    && previous.ids.every((id, index) => id === next.ids[index]);
}

function replicaRowMatchesPlan(
  row: ReplicaRow,
  definition: ReplicaCollectionPlan,
  args: ReplicaRow,
): boolean {
  // The module manifest encodes equalFilters as argument name -> row column
  // (for example `{ id: "_id" }`). Keep this aligned with artifact parsing.
  for (const [argument, column] of Object.entries(definition.equalFilters ?? {})) {
    if (!Object.prototype.hasOwnProperty.call(row, column)
      || !Object.prototype.hasOwnProperty.call(args, argument)
      || row[column] !== args[argument]) return false;
  }
  for (const column of definition.excludeWhenSet ?? []) {
    if (row[column] !== null && row[column] !== undefined) return false;
  }
  return true;
}

function compareReplicaKeys(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function compareReplicaMembershipRows(
  left: ReplicaRow | undefined,
  right: ReplicaRow | undefined,
  key: string,
  orderBy: string,
  orderDirection: "asc" | "desc" | undefined,
): number {
  const leftValue = replicaOrderValue(left?.[orderBy]);
  const rightValue = replicaOrderValue(right?.[orderBy]);
  if (leftValue === null && rightValue !== null) return 1;
  if (leftValue !== null && rightValue === null) return -1;
  if (leftValue !== null && rightValue !== null && leftValue !== rightValue) {
    const direction = orderDirection === "asc" ? 1 : -1;
    return leftValue < rightValue ? -direction : direction;
  }
  return compareReplicaKeys(String(left?.[key] ?? ""), String(right?.[key] ?? ""));
}

function replicaOrderValue(value: JsonValue | undefined): string | number | null {
  return typeof value === "string" || typeof value === "number" ? value : null;
}

export class MemoryLocalReplicaStorage implements LocalReplicaStorage {
  private readonly sessions = new Map<string, LocalReplicaSession>();
  async loadSession(scope: string) { const value = this.sessions.get(scope); return value ? structuredClone(value) : undefined; }
  async saveSession(scope: string, value: LocalReplicaSession | undefined) {
    if (value) this.sessions.set(scope, structuredClone(value)); else this.sessions.delete(scope);
  }
  private readonly values = new Map<ReplicaScope, ReplicaSnapshot>();
  async load(scope: ReplicaScope = defaultReplicaScope) {
    const value = this.values.get(normalizeReplicaScope(scope));
    return value ? cloneSnapshot(value) : undefined;
  }
  async applyTransaction(_transaction: ReplicaTransaction, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope) {
    this.values.set(normalizeReplicaScope(scope), cloneSnapshot(snapshot));
  }
  async advanceWatermark(windows: readonly ReplicaWindow[], cursor: ReplicaCursor | undefined, scope: ReplicaScope = defaultReplicaScope) {
    const normalizedScope = normalizeReplicaScope(scope);
    const existing = this.values.get(normalizedScope);
    if (!existing) return;
    const snapshot = cloneSnapshot(existing);
    for (const window of windows) snapshot.liveQueries[window.signature] = cloneWindow(window);
    snapshot.cursor = cursor ? { ...cursor } : undefined;
    this.values.set(normalizedScope, snapshot);
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
  if (patch.op === "insert") return { ...patch, fields: immutable(structuredClone(patch.fields)) };
  return { ...patch, fields: immutable(structuredClone(patch.fields)) };
}

function sameReplicaValue(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (!left || !right || typeof left !== "object" || typeof right !== "object") return false;
  if (Array.isArray(left)) return Array.isArray(right) && left.length === right.length && left.every((value, index) => sameReplicaValue(value, right[index]));
  if (Array.isArray(right)) return false;
  const a = left as Record<string, unknown>, b = right as Record<string, unknown>;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every(key => Object.prototype.hasOwnProperty.call(b, key) && sameReplicaValue(a[key], b[key]));
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
function replicaTransactionFloor(
  current: ReplicaCursor | undefined,
  windows: ReadonlyMap<string, ReplicaWindow>,
): ReplicaCursor | undefined {
  const cursors = [...windows.values()]
    .filter((window) => window.kind === "replica" && window.cursor !== undefined)
    .map((window) => window.cursor!);
  if (cursors.length === 0) return current ? { ...current } : undefined;

  const epoch = cursors[0]!.epoch;
  if (cursors.some((cursor) => cursor.epoch !== epoch)) {
    // Epoch replacement is reconciled by materializeWindowNow. Until every
    // retained window agrees, snapshots cannot prove a global transaction
    // floor.
    return current?.epoch === epoch ? { ...current } : undefined;
  }
  const provenRevision = Math.min(...cursors.map((cursor) => cursor.revision));
  if (current?.epoch === epoch && current.revision >= provenRevision) return { ...current };
  return { epoch, revision: provenRevision };
}
function hydratedTransactionFloor(
  persisted: ReplicaCursor | undefined,
  windows: ReadonlyMap<string, ReplicaWindow>,
): ReplicaCursor | undefined {
  const cursors = [...windows.values()]
    .filter((window) => window.kind === "replica" && window.cursor !== undefined)
    .map((window) => window.cursor!);
  if (cursors.length === 0) return persisted ? { ...persisted } : undefined;
  const epoch = cursors[0]!.epoch;
  if (cursors.some((cursor) => cursor.epoch !== epoch)) return undefined;
  const provenRevision = Math.min(...cursors.map((cursor) => cursor.revision));
  if (persisted?.epoch !== epoch) return { epoch, revision: provenRevision };
  // Older clients persisted the maximum individual snapshot revision as the
  // global cursor. Clamp it on hydration so upgrading cannot retain that
  // unsafe floor across a browser restart.
  return { epoch, revision: Math.min(persisted.revision, provenRevision) };
}
function entitiesFromSnapshot(source: ReplicaSnapshot["entities"], owned = false) {
  return new Map(Object.entries(source).map(([entity, rows]) => [entity, residentRows(Object.entries(rows).map(([id, row]) => [id, owned ? row : cloneRow(row)] as const))]));
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

/** Build snapshot accessors only if an adapter needs them. Metadata-only writes
 * never enumerate tables/windows; delta adapters clone values on first access. */
function storageSnapshotFrom(cursor: ReplicaCursor | undefined, source: Map<string, Map<string, ReplicaRow>>, windows: Map<string, ReplicaWindow>): ReplicaSnapshot {
  // Capture membership now even if an adapter reads its snapshot after the
  // persistence promise resolves and reconciliation replaces map entries.
  const capturedSource = new Map(source);
  const capturedWindows = new Map(windows);
  const snapshot = { cursor: cursor ? { ...cursor } : undefined } as ReplicaSnapshot;
  Object.defineProperty(snapshot, "entities", { enumerable: true, configurable: true, get() {
    const entities: ReplicaSnapshot["entities"] = {};
    for (const [entity, rows] of capturedSource) {
      // Committed maps and rows are immutable. Retain that version without
      // allocating entries for unrelated tables; clone values on first access.
      const captured = rows;
      Object.defineProperty(entities, entity, { enumerable: true, configurable: true, get() {
        const value: Record<string, ReplicaRow> = {};
        for (const [id, row] of captured) Object.defineProperty(value, id, { enumerable: true, configurable: true, get() {
          const copy = cloneRow(row);
          Object.defineProperty(value, id, { enumerable: true, configurable: true, writable: true, value: copy });
          return copy;
        }});
        Object.defineProperty(entities, entity, { enumerable: true, configurable: true, writable: true, value });
        return value;
      }});
    }
    Object.defineProperty(snapshot, "entities", { enumerable: true, configurable: true, writable: true, value: entities });
    return entities;
  }});
  Object.defineProperty(snapshot, "liveQueries", { enumerable: true, configurable: true, get() {
    const liveQueries: ReplicaSnapshot["liveQueries"] = {};
    for (const [key, window] of capturedWindows) Object.defineProperty(liveQueries, key, { enumerable: true, configurable: true, get() {
      const copy = cloneWindow(window);
      Object.defineProperty(liveQueries, key, { enumerable: true, configurable: true, writable: true, value: copy });
      return copy;
  }});
  Object.defineProperty(snapshot, "liveQueries", { enumerable: true, configurable: true, writable: true, value: liveQueries });
  return liveQueries;
  }});
  return snapshot;
}

function membershipEdits(window: ReplicaWindow, previous: ReplicaWindow | undefined, upserts: readonly ReplicaRow[]): NonNullable<ReplicaWindowDelta['memberships']> {
  const changed = new Set(upserts.map(row => String(row[window.key])));
  const prior = new Set(previous?.ids ?? []);
  const positions = new Map<string, number>();
  // Existing unordered members retain their keys. Ordered edits and new members
  // use final neighbors; this is RAM work, never a full membership disk rewrite.
  if (window.orderBy || previous?.orderBy || upserts.some(row => !prior.has(String(row[window.key])))) {
    window.ids.forEach((id, index) => positions.set(id, index));
  }
  return [...changed].filter(id => window.ids.includes(id)).sort((a, b) => (positions.get(a) ?? 0) - (positions.get(b) ?? 0)).map(id => {
    if (!window.orderBy && !previous?.orderBy && prior.has(id)) return { id };
    const index = positions.get(id)!;
    let next = index + 1;
    while (next < window.ids.length && changed.has(window.ids[next]!)) next++;
    return { id, moved: true, after: window.ids[index - 1], before: window.ids[next] };
  });
}
