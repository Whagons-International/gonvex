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
  loadWorkingSet?(scope:string,budget:{maxRows:number;maxBytes:number}):Promise<ReplicaSnapshot | undefined>;
  loadWindowRows?(scope:string,signature:string):Promise<{window:ReplicaWindow;rows:ReplicaRow[]} | undefined>;
  loadEntityRows?(scope:string,entity:string,ids:readonly string[]):Promise<Array<{id:string;row:ReplicaRow}>>;
  readChanges?(scope:string, afterSequence:number,interest?:{rows:Record<string,string[]>;windows:string[];availableRows:number;availableBytes:number;maxRows:number;maxBytes:number}):Promise<ReplicaStorageChanges>;
  subscribePeer?(listener:(scope:string)=>void):()=>void;
  withReadView?<T>(scope: string, coverage: ReadCoverage, run: (view: ReducerReadView) => Promise<T>): Promise<T>;
  /** Last server-authorized identity/visibility metadata, partitioned by login. */
  loadSession?(identityScope: string): Promise<LocalReplicaSession | undefined>;
  saveSession?(identityScope: string, session: LocalReplicaSession | undefined): Promise<void>;
  load(scope?: ReplicaScope): Promise<ReplicaSnapshot | undefined>;
  applyTransaction(transaction: ReplicaTransaction, snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void>;
  /** Advance ready Replica Collection cursors without rewriting normalized rows. */
  advanceWatermark?(windows: readonly ReplicaWindow[], cursor: ReplicaCursor | undefined, scope?: ReplicaScope): Promise<void>;
  /** Persist a normalized Query/Collection materialization atomically. */
  replaceSnapshot?(snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void>;
  replaceWindow?(window: ReplicaWindow, snapshot: ReplicaSnapshot, scope?: ReplicaScope, projection?:readonly ReplicaRow[]): Promise<void>;
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
  /** Stable immutable snapshot for reactive consumers. */
  liveQuerySnapshot<T extends ReplicaRow = ReplicaRow>(signature: string): LiveQueryResult<T>;
  collectionState<T extends ReplicaRow = ReplicaRow>(signature: string): ReplicaCollectionState<T>;
  hasLiveQuery(signature: string): boolean;
  snapshot(): ReplicaSnapshot;
}

export class LocalReplica implements LocalReplicaView {
  private readonly retainedWindows = new Map<string, number>();
  private readonly retainedRows = new Map<string,Map<string,number>>();
  private readonly residentPartialTables = new Set<string>();
  private readonly rowSizes = new WeakMap<ReplicaRow, number>();
  private readonly visibleRowCopies = new WeakMap<ReplicaRow, ReplicaRow>();
  private readonly residencyBudget = {maxRows:20_000,maxBytes:16*1024*1024};
  private cursorValue?: ReplicaCursor;
  private entities = new Map<string, Map<string, ReplicaRow>>();
  private liveQueries = new Map<string, ReplicaWindow>();
  /** Rows introduced by a materialized window may be reclaimed conservatively. */
  private readonly windowOwned = new Map<string, Map<string, Set<string>>>();
  private readonly replicaPlans = new Map<string, { definition: ReplicaCollectionPlan; args: ReplicaRow }>();
  private pendingCommands = new Map<string, PendingCommand>();
  private listeners = new Set<() => void>();
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
  private entityVersionClock = 0;
  private entityVersionFloor = 0;
  private readonly entityVersions = new Map<string, number>();
  private windowVersionClock = 0;
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
    this.entities.clear();
    this.liveQueries.clear();
    this.pendingCommands.clear();
    this.listeners.clear();
    this.retainedWindows.clear();
    this.retainedRows.clear();
    this.residentPartialTables.clear();
    this.windowOwned.clear();
    this.replicaPlans.clear();
    this.readSnapshots.clear();
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
   */
  activateScope(scope: ReplicaScope = defaultReplicaScope, forceReload = false): Promise<void> {
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
      const snapshot = this.storage?.loadWorkingSet
        ? await this.storage.loadWorkingSet(nextScope,this.residencyBudget)
        : await this.storage?.load(nextScope);
      if (generation !== this.scopeActivationGeneration) return;
      this.scopeValue = nextScope;
      this.storageSequence = snapshot?.storageSequence ?? 0;
      this.scopeLoaded = true;
      this.entities = snapshot ? entitiesFromSnapshot(snapshot.entities) : new Map();
      this.residentPartialTables.clear();
      for(const entity of snapshot?.residentPartialTables ?? []) this.residentPartialTables.add(entity);
      this.invalidateEntityVersions();
      this.liveQueries = snapshot
        ? new Map(Object.entries(snapshot.liveQueries).map(([key, value]) => [key, normalizeWindow(value)]))
        : new Map();
      this.cursorValue = hydratedTransactionFloor(snapshot?.cursor, this.liveQueries);
      for (const signature of this.liveQueries.keys()) this.markWindowChanged(signature);
      this.windowOwned.clear();
      // Optimistic commands belong to the old identity and must never be
      // projected while the newly restored scope is becoming authoritative.
      this.pendingCommands.clear();
      this.freshnessValue = "verifying";
      this.notify(true);
      queueMicrotask(()=>{
        if(generation!==this.scopeActivationGeneration)return;
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
    this.retainedWindows.set(signature,(this.retainedWindows.get(signature) ?? 0)+1);
    const scopeGeneration=this.scopeActivationGeneration;
    const operation=this.enqueueApplication(async()=>{
      if(!this.scopeLoaded || !this.retainedWindows.has(signature) || scopeGeneration!==this.scopeActivationGeneration || !this.storage?.loadWindowRows)return;
      const existing=this.liveQueries.get(signature);
      if(existing?.ids.every(id=>this.entities.get(existing.entity)?.has(id)))return;
      const loaded=await this.storage.loadWindowRows(this.scopeValue,signature);
      if(!loaded || !this.retainedWindows.has(signature) || scopeGeneration!==this.scopeActivationGeneration)return;
      const table=new Map(this.entities.get(loaded.window.entity));
      for(const row of loaded.rows)table.set(String(row[loaded.window.key]),row);
      this.entities=new Map(this.entities).set(loaded.window.entity,table);
      this.liveQueries.set(signature,normalizeWindow(loaded.window));
      if(loaded.window.kind==='replica' && loaded.window.completeness==='complete' && !loaded.window.truncated && loaded.rows.length===loaded.window.ids.length)this.residentPartialTables.delete(loaded.window.entity);
      this.markEntityChanged(loaded.window.entity,loaded.window.ids);
      this.markWindowChanged(signature);
      this.notify(true);
    }, true);

    let released=false;
    return ()=>{
      if(released)return;released=true;
      const count=this.retainedWindows.get(signature) ?? 0;
      if(count>1)this.retainedWindows.set(signature,count-1);
      else {this.retainedWindows.delete(signature);this.readSnapshots.delete(signature);this.trimResidentRows();}
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
    for(const id of ids)retained.set(id,(retained.get(id)??0)+1);
    const operation=this.enqueueApplication(async()=>{
      if(!this.scopeLoaded || !this.storage?.loadEntityRows || scopeGeneration!==this.scopeActivationGeneration)return;
      const missing=ids.filter(id=>!this.entities.get(entity)?.has(id) && this.retainedRows.get(entity)?.has(id));
      if(!missing.length)return;
      const rows=await this.storage.loadEntityRows(this.scopeValue,entity,missing);
      if(scopeGeneration!==this.scopeActivationGeneration)return;
      const next=new Map(this.entities.get(entity));
      for(const {id,row} of rows)if(this.retainedRows.get(entity)?.has(id))next.set(id,row);
      this.entities=new Map(this.entities).set(entity,next);
      this.markEntityChanged(entity,missing);this.notify(true);
    }, true);

    let released=false;
    return ()=>{
      if(released)return;released=true;
      const current=this.retainedRows.get(entity);
      for(const id of ids){const count=current?.get(id)??0;if(count>1)current!.set(id,count-1);else current?.delete(id);}
      if(!current?.size)this.retainedRows.delete(entity);
      this.trimResidentRows();
    };
  }

  private trimResidentRows() {
    // Without indexed persistence, eviction would lose the only readable copy.
    if(!this.storage?.withReadView || !this.storage?.loadWindowRows)return;
    let bytes=0,count=0;
    for(const rows of this.entities.values())for(const row of rows.values()){
      let size=this.rowSizes.get(row);
      if(size===undefined){size=JSON.stringify(row).length*2+256;this.rowSizes.set(row,size);}
      bytes+=size;count++;
    }
    if(bytes<=this.residencyBudget.maxBytes && count<=this.residencyBudget.maxRows)return;
    const pinned=new Map<string,Set<string>>();
    const pin=(entity:string,id:string)=>{let ids=pinned.get(entity);if(!ids){ids=new Set();pinned.set(entity,ids);}ids.add(id);};
    for(const signature of this.retainedWindows.keys()){
      const window=this.liveQueries.get(signature);
      if(window)for(const id of window.ids)pin(window.entity,id);
    }
    for(const [entity,ids] of this.retainedRows)for(const id of ids.keys())pin(entity,id);
    for(const command of this.pendingCommands.values())for(const patch of command.patches)pin(patch.entity ?? patch.collection!,patch.rowId);
    const next=new Map(this.entities);
    let evicted=false;
    for(const [entity,rows] of this.entities){
      let copied:Map<string,ReplicaRow>|undefined;
      for(const [id,row] of rows){
        if(bytes<=this.residencyBudget.maxBytes && count<=this.residencyBudget.maxRows)break;
        if(pinned.get(entity)?.has(id))continue;
        copied ??=new Map(rows);copied.delete(id);count--;bytes-=this.rowSizes.get(row)!;
        this.rowVersions.delete(`${entity}\0${id}`);this.residentPartialTables.add(entity);evicted=true;
      }
      if(copied)next.set(entity,copied);
    }
    if(evicted){this.entities=next;this.readSnapshots.clear();}
  }

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

  private async mergeStoredChanges(entities: Map<string, Map<string, ReplicaRow>>, windows: Map<string, ReplicaWindow>): Promise<boolean> {
    if (!this.storage?.readChanges) return false;
    const residentIds=Object.fromEntries([...entities].map(([table,rows])=>[table,[...rows.keys()]]));
    let residentBytes=0,residentCount=0;
    for(const rows of entities.values())for(const row of rows.values()){
      let size=this.rowSizes.get(row);if(size===undefined){size=JSON.stringify(row).length*2+256;this.rowSizes.set(row,size);}
      residentBytes+=size;residentCount++;
    }
    for(const command of this.pendingCommands.values())for(const patch of command.patches){const entity=patch.entity ?? patch.collection!;(residentIds[entity]??=[]).push(patch.rowId);}
    const changes = await this.storage.readChanges(this.scopeValue, this.storageSequence,this.storage.loadWorkingSet ? {
      rows:residentIds,
      windows:[...this.retainedWindows.keys()],
      availableRows:Math.max(0,this.residencyBudget.maxRows-residentCount),
      availableBytes:Math.max(0,this.residencyBudget.maxBytes-residentBytes),
      ...this.residencyBudget,
    } : undefined);
    if (changes.sequence === this.storageSequence) return false;
    if (changes.reset) {
      entities.clear();
      windows.clear();
      this.windowOwned.clear();
      this.invalidateEntityVersions();
    }
    for (const [entity, values] of Object.entries(changes.entities)) {
      const rows = new Map(entities.get(entity));
      const changedIDs = new Set<string>();
      for (const [id, row] of Object.entries(values)) {
        if (row === null) {
          if (rows.delete(id)) changedIDs.add(id);
        } else if (!sameReplicaValue(rows.get(id), row)) {
          rows.set(id, row);
          changedIDs.add(id);
        }
      }
      if (!changedIDs.size) continue;
      entities.set(entity, rows);
      this.markEntityChanged(entity, changedIDs);
      for (const window of windows.values()) {
        if (window.entity === entity && window.ids.some(id => changedIDs.has(id))) this.markWindowChanged(window.signature);
      }
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
    this.storageSequence = changes.sequence;
    return true;
  }

  freshness() {
    return this.freshnessValue;
  }

  version() { return this.versionValue; }

  /** Changes to the authoritative execution snapshot, excluding UI metadata and predictions. */
  executionVersion() { return this.executionVersionValue; }
  entityVersion(entity: string, id?: string) { return id === undefined ? this.entityVersions.get(entity) ?? this.entityVersionFloor : this.rowVersion(entity, id); }
  rowVersion(entity: string, id: string) { return this.rowVersions.get(`${entity}\0${id}`) ?? this.entityVersionFloor; }

  /** Monotonic version for one materialized window and its referenced rows. */
  windowVersion(signature: string) { return this.windowVersions.get(signature) ?? 0; }
  windowRowsVersion(signature: string) { return this.windowRowsVersions.get(signature) ?? 0; }

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
      for (const row of input.upserts) {
        const rawID = row[input.key];
        const id = typeof rawID === "string" || typeof rawID === "number" ? String(rawID) : "";
        if (id && !ids.includes(id)) ids.push(id);
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
        await this.persist(() => this.storage!.advanceWatermark!(changedWindows, nextCursor, writeScope));
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
      this.pruneOwnedEntitiesAfterRemoval(signature, nextQueries);
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
      this.entities.clear();
      this.invalidateEntityVersions();
      for (const signature of this.liveQueries.keys()) this.markWindowChanged(signature);
      this.liveQueries.clear();
      this.windowOwned.clear();
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
    const entityRows = new Map(nextEntities.get(input.entity));
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
      entityRows.set(id, { ...(priorRow ?? {}), ...cloneRow(row) });
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
      hashes: input.hashes ? { ...input.hashes } : undefined,
    };
    nextQueries.set(input.signature, window);
    for (const id of input.removedIDs ?? []) {
      const stillReferenced = [...nextQueries.values()].some((candidate) => candidate.ids.includes(id));
      if (!stillReferenced && nextEntities.get(input.entity)?.delete(id)) changedRowIDs.add(id);
    }
    this.trackWindowOwnership(window, input.rows, previous);
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
    if (!epochChanged && this.storage?.applyWindowDelta && (delta || !rowsChanged)) {
      const writeDelta = delta ? { upserts: delta.upserts.filter(row => changedRowIDs.has(String(row[input.key]))), deleted: delta.deleted } : { upserts: [], deleted: [] };
      await this.persist(() => this.storage!.applyWindowDelta!(window, writeDelta, snapshot, writeScope));
    } else if (this.storage?.replaceWindow) {
      await this.persist(() => this.storage!.replaceWindow!(window, snapshot, writeScope,input.rows));
    } else if (this.storage?.replaceSnapshot) {
      await this.persist(() => this.storage!.replaceSnapshot!(snapshot, writeScope));
    }
    if (epochChanged) this.invalidateEntityVersions();
    if (this.storage?.readChanges) await this.mergeStoredChanges(nextEntities, nextQueries);
    this.entities = nextEntities;
    this.liveQueries = nextQueries;
    if(window.kind==='replica' && window.completeness==='complete' && !window.truncated && window.ids.every(id=>nextEntities.get(window.entity)?.has(id)))this.residentPartialTables.delete(window.entity);
    if (changedRowIDs.size) {
      this.markEntityChanged(input.entity, changedRowIDs);
      for (const candidate of this.liveQueries.values()) {
        if (candidate.signature !== input.signature && candidate.entity === input.entity && candidate.ids.some(id => changedRowIDs.has(id))) this.markWindowChanged(candidate.signature);
      }
    }
    this.markWindowChanged(input.signature, rowsChanged);
    this.pruneOwnedEntities(input.signature);
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
      for (const [signature, window] of nextQueries) {
        if (window.entity === change.entity && window.ids.includes(change.id)) changedWindows.add(signature);
      }
      if (!copiedEntities.has(change.entity)) {
        nextEntities.set(change.entity, new Map(nextEntities.get(change.entity)));
        copiedEntities.add(change.entity);
      }
      const rows = nextEntities.get(change.entity)!;
      if (change.operation === "delete") {
        rows.delete(change.id);
        for (const window of nextQueries.values()) {
          if (window.entity === change.entity && window.ids.includes(change.id)) {
            nextQueries.set(window.signature, { ...window, ids: window.ids.filter((id) => id !== change.id) });
          }
        }
      }
      // Change-feed rows are projections, just like collection snapshots.
      // A narrow subscription must not erase fields supplied by another one.
      else if (change.newValue) rows.set(change.id, {...rows.get(change.id),...cloneRow(change.newValue)});
    }
    for (const membership of transaction.memberships ?? []) {
      nextQueries.set(membership.signature, normalizeWindow(membership));
      changedWindows.add(membership.signature);
    }

    const snapshot = storageSnapshotFrom(transaction.cursor, nextEntities, nextQueries);
    const writeScope = this.scopeValue;
    await this.persist(() => this.storage?.applyTransaction(transaction, snapshot, writeScope));
    if (this.storage?.readChanges) await this.mergeStoredChanges(nextEntities, nextQueries);

    if (epochChanged) {
      this.invalidateEntityVersions();
      this.windowOwned.clear();
    }
    // Publish the whole committed transaction in one state swap and notify UI
    // exactly once. No subscriber can observe a partial entity/query update.
    this.entities = nextEntities;
    this.liveQueries = nextQueries;
    for (const change of transaction.changes) this.markEntityChanged(change.entity, [change.id]);
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
    if (!this.scopeLoaded) return ids.map(() => undefined);
    const base = this.entities.get(entity);
    if (this.pendingCommands.size === 0) return ids.map(id => {
      const row = base?.get(id);
      return row === undefined ? undefined : cloneRow(row) as T;
    });
    const rows = new Map<string, ReplicaRow | undefined>();
    for (const id of ids) rows.set(id, base?.get(id));
    if (rows.size) for (const command of this.pendingCommands.values()) {
      for (const patch of command.patches) {
        if ((patch.entity ?? patch.collection) !== entity || !rows.has(patch.rowId)) continue;
        if (patch.op === 'delete') rows.set(patch.rowId, undefined);
        else if (patch.op === 'insert' || patch.op === 'upsert') rows.set(patch.rowId, patch.fields as ReplicaRow);
        else if (patch.op === 'patch') rows.set(patch.rowId, { ...(rows.get(patch.rowId) ?? {}), ...(patch.fields as ReplicaRow) });
      }
    }
    // Preserve caller order and return independent values for duplicate IDs.
    return ids.map(id => {
      const row = rows.get(id);
      return row === undefined ? undefined : cloneRow(row) as T;
    });
  }

  /** All cached rows for one normalized entity, including optimistic overlays. */
  entityRows<T extends ReplicaRow = ReplicaRow>(entity: string): T[] {
    if (!this.scopeLoaded) return [];
    if (this.pendingCommands.size === 0) {
      return Array.from(this.entities.get(entity)?.values() ?? [], row => cloneRow(row) as T);
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
    // Clone only the final values, including nested patch fields. Callers must
    // not be able to mutate either authoritative rows or the pending journal.
    for (const row of rows.values()) if (row !== undefined) result.push(cloneRow(row) as T);
    return result;
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
      return run(overlayReadView(memoryReadView(this.entities,this.residentCoverage(known)),known,patches));
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
      try { return await apply(memoryReadView(this.entities,this.residentCoverage(known))); }
      catch (error) {
        if (!(error instanceof Error) || error.name !== 'IncompleteReplicaError' || !this.storage?.withReadView) throw error;
        return this.storage.withReadView(this.scopeValue,known,apply);
      }
    }, true);

    return operation;
  }

  /** SDK execution snapshot: capture immutable row references now, copy a table only when used. */
  captureExecutionRows(includePending: boolean): (entity: string) => ReplicaRow[] {
    const captured = new Map([...this.entities].map(([entity, rows]) => [entity, [...rows.entries()]]));
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

  /** SDK watch snapshots: unchanged rows are not cloned again on every notification. */
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
      const version = this.rowVersion(window.entity, id);
      const prior = cache.get(id);
      if (prior?.version === version) return prior.row;
      const base = !pendingRows?.has(id) ? this.entities.get(window.entity)?.get(id) : undefined;
      let next: T | undefined;
      if (base) {
        let copy = this.visibleRowCopies.get(base);
        if (!copy) { copy = cloneRow(base); this.visibleRowCopies.set(base, copy); }
        next = copy as T;
      } else next = this.entity<T>(window.entity, id);
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

  private persist(operation: () => Promise<void> | undefined) {
    const attempt = this.persistence.then(async () => { await operation(); });
    this.persistence = attempt.catch(() => undefined);
    return attempt;
  }

  private notify(executionChanged = false) {
    if (this.disposed) return;
    if(executionChanged)this.trimResidentRows();
    if (executionChanged) this.executionVersionValue += 1;
    this.versionValue += 1;
    for (const listener of [...this.listeners]) listener();
  }

  private markEntityChanged(entity: string, ids?: Iterable<string>) {
    const version = ++this.entityVersionClock;
    this.entityVersions.set(entity, version);
    for (const id of ids ?? this.entities.get(entity)?.keys() ?? []) this.rowVersions.set(`${entity}\0${id}`, version);
  }

  private invalidateEntityVersions() {
    this.entityVersionFloor = ++this.entityVersionClock;
    this.entityVersions.clear();
    this.rowVersions.clear();
    this.readSnapshots.clear();
  }

  private markWindowChanged(signature: string, rowsChanged = true) {
    this.windowVersionClock += 1;
    this.windowVersions.set(signature, this.windowVersionClock);
    if (rowsChanged) this.windowRowsVersions.set(signature, this.windowVersionClock);
  }

  private markWindowsForPatches(patches: readonly OptimisticPatch[]) {
    const affected = new Set<string>();
    for (const patch of patches) {
      const entity = patch.entity ?? patch.collection;
      if (entity) this.markEntityChanged(entity, [patch.rowId]);
      for (const [signature, window] of this.liveQueries) {
        if (window.entity !== entity) continue;
        if (window.ids.includes(patch.rowId)) {
          affected.add(signature);
          continue;
        }
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
    const referenced = new Set<string>();
    for (const window of this.liveQueries.values()) {
      if (window.entity === sourceWindow?.entity) for (const id of window.ids) referenced.add(id);
    }
    for (const [id] of owned) {
      if (!referenced.has(id) && sourceWindow) {
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
  if (patch.op === "insert") return { ...patch, fields: structuredClone(patch.fields) };
  return { ...patch, fields: structuredClone(patch.fields) };
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
