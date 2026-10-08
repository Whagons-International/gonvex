import { Dexie, type Table, type Collection, type Transaction } from "dexie";
import { entityLookupKeys, entityRecord, indexedDBReadView, type EntityRecord, type ReadCoverage } from './indexeddb-read-view.js';
import type { ReducerReadView } from '@gonvex/local-runtime/portable';
import {mergeReplicaRecord} from './replica-record.js';
import type {ReplicaCursor} from '@gonvex/protocol';
import type {
  LocalReplicaStorage,
  ReplicaScope,
  ReplicaSnapshot,
  ReplicaTransaction,
  ReplicaWindow,
  ReplicaRow,
  ReplicaStorageChanges,
  ReplicaMetadataCommit,
} from "./local-replica.js";

type WindowRecord = { scope: ReplicaScope; signature: string; value: string; sequence?:number; deleted?:boolean };
type MetaRecord = { scope: ReplicaScope; key: string; value: string };
type LegacySnapshotRecord = { scope?: ReplicaScope; key?: string; snapshot: ReplicaSnapshot };
type ReplicaDatabase = Dexie & {
  entities: Table<EntityRecord, [string, string, string]>;
  windows: Table<WindowRecord, [string, string]>;
  meta: Table<MetaRecord, [string, string]>;
  snapshots: Table<LegacySnapshotRecord, string>;
};

const defaultReplicaScope: ReplicaScope = "default";
const replicaSchemaVersion = 6;
const lookupPolicyKey = (entity: string) => `lookupColumns:${entity}`;
// Bound native IDB request bursts so another database's durable intent journal
// can run between batches. Four-row batches made medium snapshots spend most
// of their time on native request round trips. Keep one atomic transaction.
const writeBatchSize = 32;
const writeIndexBudget = 64;
const windowBatchSize = 64;
const windowBatchBytes = 16 * 1024;
const windowEncoder = new TextEncoder();

// Small replica checkpoints usually change only a few rows. IndexedDB getAll
// avoids a JS/Dexie continuation per row. The fallback remains streaming so a
// large peer update cannot pull an unbounded table into a temporary array.
async function visitRecords<T, Key>(collection: Collection<T, Key>, visit: (record: T) => void): Promise<void> {
  const page = await collection.clone().limit(128).toArray();
  for (const record of page) visit(record);
  if (page.length === 128) await collection.clone().offset(128).each(visit);
}

/** Atomic normalized web persistence for the Gonvex Local Replica. */
export class IndexedDBLocalReplicaStorage implements LocalReplicaStorage {
  private readonly database: ReplicaDatabase;
  private initialized?: Promise<void>;
  // Cache policy only within the native transaction. Peers can learn columns
  // between transactions; a process-wide cache would silently miss their rows.
  private readonly lookupPolicies = new WeakMap<Transaction, Map<string, Promise<readonly string[]>>>();

  private indexedColumns(scope: string, entity: string): Promise<readonly string[]> {
    const transaction = Dexie.currentTransaction!;
    let policies = this.lookupPolicies.get(transaction);
    if (!policies) { policies = new Map(); this.lookupPolicies.set(transaction, policies); }
    const key = JSON.stringify([scope, entity]);
    let columns = policies.get(key);
    if (!columns) {
      columns = this.database.meta.get([scope, lookupPolicyKey(entity)]).then(record => record ? JSON.parse(record.value) as string[] : []);
      policies.set(key, columns);
    }
    return columns;
  }

  private readonly jobs = new Map<string, Promise<void>>();
  private closed = false;
  private backgroundQueue: Promise<void> = Promise.resolve();
  private readonly generations = new Map<string, number>();
  private readonly writePolicies = new WeakMap<Transaction, Map<string, Promise<readonly string[]>>>();
  private seeds: Readonly<Record<string, readonly string[]>> = {};

  configureLookupColumns(columns: Readonly<Record<string, readonly string[]>>) {
    this.seeds = columns;
  }

  /** Wait for derived work, for diagnostics only. Reducer reads never call this. */
  async waitForIndexBackfills(): Promise<void> {
    while (this.jobs.size) await Promise.all(this.jobs.values());
  }

  private async backgroundTurn(): Promise<void> {
    await new Promise<void>(resolve => {
      if (typeof requestIdleCallback === 'function') requestIdleCallback(() => resolve(), { timeout: 100 });
      else setTimeout(resolve, 0);
    });
  }

  private scheduleColumns(scope: string, demands: Map<string, Set<string>>) {
    for (const [entity, columns] of demands) for (const column of columns) {
      const key = JSON.stringify([scope, entity, column]);
      if (this.closed || this.jobs.has(key)) continue;
      const generation = this.generations.get(scope) ?? 0;
      // One builder per instance avoids enqueuing a burst of overlapping write
      // transactions when a Reducer demands several new columns at once.
      const job = this.backgroundQueue.then(() => Dexie.ignoreTransaction(() => this.learnColumn(scope, entity, column, generation))).catch(() => {
        // Partial keys stay invisible. A later scoped scan retries the build.
      }).finally(() => { this.jobs.delete(key); });
      this.jobs.set(key, job);
      this.backgroundQueue = job;
    }
  }

  private writeColumns(scope: string, entity: string): Promise<readonly string[]> {
    const transaction = Dexie.currentTransaction!;
    let policies = this.writePolicies.get(transaction);
    if (!policies) { policies = new Map(); this.writePolicies.set(transaction, policies); }
    const key = JSON.stringify([scope, entity]);
    let columns = policies.get(key);
    if (!columns) { columns = this.loadWriteColumns(scope, entity); policies.set(key, columns); }
    return columns;
  }

  private async loadWriteColumns(scope: string, entity: string): Promise<readonly string[]> {
    const ready = await this.indexedColumns(scope, entity);
    const pendingKey = `lookupPending:${entity}`;
    const record = await this.database.meta.get([scope, pendingKey]);
    const pending: Record<string, string> = Object.assign(Object.create(null), record ? JSON.parse(record.value) : {});
    const added = (this.seeds[entity] ?? []).filter(column => !ready.includes(column) && !pending[column]);
    if (added.length) {
      if (!await this.database.entities.where('[scope+entity]').equals([scope, entity]).count()) {
        const columns = [...new Set([...ready, ...added])];
        await this.database.meta.put({ scope, key: lookupPolicyKey(entity), value: JSON.stringify(columns) });
        this.lookupPolicies.get(Dexie.currentTransaction!)?.delete(JSON.stringify([scope, entity]));
        return [...new Set([...columns, ...Object.keys(pending)])];
      }
      this.scheduleColumns(scope, new Map([[entity, new Set(added)]]));
      for (const column of added) pending[column] = crypto.randomUUID();
      await this.database.meta.put({ scope, key: pendingKey, value: JSON.stringify(pending) });
    }
    return [...new Set([...ready, ...Object.keys(pending)])];
  }

  private async learnColumn(scope: string, entity: string, column: string, generation: number): Promise<void> {
    if (this.closed || (this.generations.get(scope) ?? 0) !== generation) return;
    await this.backgroundTurn();
    if (this.closed || (this.generations.get(scope) ?? 0) !== generation) return;
    const key = `lookupPending:${entity}`;
    const token = await this.database.transaction('rw', this.database.entities, this.database.meta, async () => {
      if ((await this.indexedColumns(scope, entity)).includes(column)) return undefined;
      const record = await this.database.meta.get([scope, key]);
      const pending: Record<string, string> = Object.assign(Object.create(null), record ? JSON.parse(record.value) : {});
      if (this.closed || (this.generations.get(scope) ?? 0) !== generation) return undefined;
      pending[column] ??= crypto.randomUUID();
      await this.database.meta.put({ scope, key, value: JSON.stringify(pending) });
      return pending[column];
    });
    if (!token) return;
    let after: [string, string, string] | undefined;
    for (;;) {
      await this.backgroundTurn();
      if (this.closed || (this.generations.get(scope) ?? 0) !== generation) return;
      const done = await this.database.transaction('rw', this.database.entities, this.database.meta, async () => {
        const record = await this.database.meta.get([scope, key]);
        const pending: Record<string, string> = Object.assign(Object.create(null), record ? JSON.parse(record.value) : {});
        if (pending[column] !== token) return true; // scope cleared or another build finished
        const ready = await this.indexedColumns(scope, entity);
        const records = await this.database.entities.where('[scope+entity+id]')
          .between(after ?? [scope, entity], [scope, entity, []], !after, false).limit(32).toArray();
        for (const row of records) row.lookupKeys = row.deleted ? [] : entityLookupKeys(scope, entity, JSON.parse(row.value), [...new Set([...ready, ...Object.keys(pending)])]);
        await this.writeRecords(records);
        if (records.length) { after = [scope, entity, records.at(-1)!.id]; return false; }
        await this.database.meta.put({ scope, key: lookupPolicyKey(entity), value: JSON.stringify([...new Set([...ready, column])]) });
        delete pending[column];
        await this.database.meta.put({ scope, key, value: JSON.stringify(pending) });
        return true;
      });
      if (done) return;
    }
  }
  private readonly channel?:BroadcastChannel;
  private readonly peers=new Set<(scope:string)=>void>();

  constructor(name = "gonvex-local-replica") {
    this.database = new Dexie(name, { cache: 'disabled' }) as ReplicaDatabase;
    // Gonvex owns subscription delivery and peer synchronization. These private
    // tables are never queried through Dexie.liveQuery; collecting index ranges
    // and retaining a second query cache duplicates work on every replica write.
    this.database.unuse({ stack: 'dbcore', name: 'Cache' });
    this.database.unuse({ stack: 'dbcore', name: 'Observability' });
    this.channel = typeof BroadcastChannel === 'function' ? new BroadcastChannel(`gonvex-replica:${name}`) : undefined;
    if(this.channel) this.channel.onmessage=event=>{if(typeof event.data?.scope==='string')for(const listener of this.peers)listener(event.data.scope);};
    this.database.version(1).stores({ snapshots: "&key" });
    this.database.version(2).stores({ snapshots: "&scope" }).upgrade(async (transaction) => {
      await transaction.table("snapshots").toCollection().modify((record: LegacySnapshotRecord & { key?: string }) => {
        record.scope = defaultReplicaScope;
        delete record.key;
      });
    });
    this.database.version(3).stores({
      entities: "[scope+entity+id], scope, [scope+entity]",
      windows: "[scope+signature], scope",
      meta: "[scope+key], scope",
      // Retain the old table only as a one-time upgrade source. It is never
      // read after initialization and is removed from the active schema.
      snapshots: "&scope",
    }).upgrade(async (transaction) => {
      const snapshots = await transaction.table("snapshots").toArray() as LegacySnapshotRecord[];
      const entities = transaction.table("entities");
      const windows = transaction.table("windows");
      const meta = transaction.table("meta");
      for (const record of snapshots) {
        const scope = record.scope ?? defaultReplicaScope;
        const snapshot = record.snapshot;
        for (const [entity, rows] of Object.entries(snapshot.entities ?? {})) {
          for (const [id, value] of Object.entries(rows)) {
            await entities.put({ scope, entity, id, value: JSON.stringify(value) });
          }
        }
        for (const [signature, window] of Object.entries(snapshot.liveQueries ?? {})) {
          await windows.put({ scope, signature, value: JSON.stringify(window) });
        }
        if (snapshot.cursor) {
          await meta.put({ scope, key: "cursor", value: JSON.stringify(snapshot.cursor) });
        }
      }
      // The upgrade transaction is atomic: only remove the legacy full
      // snapshots after every entity/window/cursor has been copied into the
      // normalized stores. Keeping the source rows would retain a second,
      // row-bearing server-state store indefinitely.
      await transaction.table("snapshots").clear();
    });
    this.database.version(4).stores({
      entities: '[scope+entity+id], scope, [scope+entity], *lookupKeys',
    }).upgrade(async transaction => {
      await transaction.table('entities').toCollection().modify((record: EntityRecord) => {
        // Version six removes blanket keys. Do not build throwaway indexes
        // when upgrading an older normalized database through version four.
        record.lookupKeys = [];
      });
    });
    this.database.version(5).stores({
      entities:'[scope+entity+id], scope, [scope+entity], *lookupKeys, [scope+sequence]',
      windows:'[scope+signature], scope, [scope+sequence]',
    });
    this.database.version(replicaSchemaVersion).stores({}).upgrade(async transaction => {
      // Row values, authority, tombstones, windows and session/cursor metadata
      // survive. Durable outbox and application upgrade journals use other DBs.
      await transaction.table('entities').toCollection().modify((record: EntityRecord) => { record.lookupKeys = []; });
    });
  }

  subscribePeer(listener:(scope:string)=>void) {this.peers.add(listener);return()=>{this.peers.delete(listener);};}

  private async nextSequence(scope:string):Promise<number> {
    const prior=await this.database.meta.get([scope,'sequence']);
    const sequence=Number(prior?.value ?? 0)+1;
    await this.database.meta.put({scope,key:'sequence',value:String(sequence)});
    return sequence;
  }

  private async versionedRecord(scope:string,entity:string,id:string,row:ReplicaRow|null,cursor:ReplicaCursor|undefined,sequence:number,previous:EntityRecord|undefined):Promise<EntityRecord> {
    const resolved=cursor ?? {epoch:previous?.authority?.epoch ?? '',revision:0};
    const before=previous ? {row:previous.deleted ? null : JSON.parse(previous.value),authority:previous.authority ?? {epoch:resolved.epoch,fields:{}}} : undefined;
    const merged=mergeReplicaRecord(before,row,resolved);
    // Reconnect snapshots often repeat the same projected values/revisions.
    // Preserve the stored record for a no-op; a revision-only advance reuses
    // its serialized value and secondary keys while updating authority.
    if (previous && merged === before) return previous;
    if (previous && before && merged.row === before.row) return {...previous, authority:merged.authority, sequence};
    const record=merged.row ? entityRecord(scope,entity,id,merged.row,await this.writeColumns(scope,entity)) : {scope,entity,id,value:'null',lookupKeys:[],deleted:true};
    return {...record,authority:merged.authority,sequence};
  }

  private async writeEntity(scope:string,entity:string,id:string,row:ReplicaRow|null,cursor:ReplicaCursor|undefined,sequence:number) {
    const previous=await this.database.entities.get([scope,entity,id]);
    const record=await this.versionedRecord(scope,entity,id,row,cursor,sequence,previous);
    if (record !== previous) await this.database.entities.put(record);
    return record;
  }

  private async writeRecords(records: readonly EntityRecord[]) {
    // A row may update dozens of multiEntry keys. Bound the native index work,
    // rather than just the request count, so a wide snapshot cannot monopolize
    // IndexedDB while another database admits a durable interactive intent.
    // Await only IDB work and retain the enclosing atomic transaction.
    let batch: EntityRecord[] = [];
    let weight = 0;
    for (const record of records) {
      // Primary key, scope, entity and sequence indexes accompany lookupKeys.
      // A single wider row remains indivisible; never split its native write.
      const nextWeight = 4 + (record.lookupKeys?.length ?? 0);
      if (batch.length && weight + nextWeight > writeIndexBudget) {
        await this.database.entities.bulkPut(batch);
        batch = []; weight = 0;
      }
      batch.push(record); weight += nextWeight;
    }
    if (batch.length) await this.database.entities.bulkPut(batch);
  }

  private async writeRows(scope:string,entity:string,key:string,rows:readonly ReplicaRow[],cursor:ReplicaCursor|undefined,sequence:number) {
    return this.writeRowEntries(scope,entity,rows.map(row=>[String(row[key]),row]),cursor,sequence);
  }

  private async writeRowEntries(scope:string,entity:string,rows:readonly (readonly [string,ReplicaRow])[],cursor:ReplicaCursor|undefined,sequence:number) {
    // Repeated reconnect projections can be no-ops, but new metadata seeds
    // still need policy registration before those rows bypass key construction.
    if (rows.length && this.seeds[entity]?.length) await this.writeColumns(scope, entity);
    // Bounded batches retain only the incoming rows and their previous values.
    for(let offset=0;offset<rows.length;offset+=writeBatchSize) {
      const batch=rows.slice(offset,offset+writeBatchSize);
      const prior=await this.database.entities.bulkGet(batch.map(([id])=>[scope,entity,id]) as [string,string,string][]);
      const records = (await Promise.all(batch.map(([id,row],index)=>this.versionedRecord(scope,entity,id,row,cursor,sequence,prior[index])))).filter((record,index)=>record !== prior[index]);
      if (records.length) await this.writeRecords(records);
    }
  }

  private async writeWindows(scope:string,windows:readonly ReplicaWindow[],sequence:number) {
    // Small/empty windows need few native round trips; large memberships must
    // not share an unbounded burst. A single window remains indivisible. Every
    // flush still belongs to the caller's original atomic transaction.
    let batch: Array<{ window: ReplicaWindow; value: string }> = [];
    let bytes = 0;
    let accepted = true;
    const flush = async () => {
      if (!batch.length) return;
      const prior=await this.database.windows.bulkGet(batch.map(({window})=>[scope,window.signature]) as [string,string][]);
      await this.database.windows.bulkPut(batch.flatMap(({window,value},index)=>{
        const record=prior[index];
        const before=record && !record.deleted ? JSON.parse(record.value) as ReplicaWindow : undefined;
        if(before?.cursor && (!window.cursor || (before.cursor.epoch===window.cursor.epoch && before.cursor.revision>window.cursor.revision))) {
          accepted = false;
          return [{...record!,sequence}];
        }
        // Serialization already isolates persisted values. Copying ordered IDs
        // and large integrity maps before stringifying doubled temporary memory
        // for every cursor-only checkpoint.
        return [{scope,sequence,signature:window.signature,value}];
      }));
      batch = []; bytes = 0;
    };
    for (const window of windows) {
      const value = JSON.stringify({ ...window, kind: window.kind ?? 'live', key: window.key ?? 'id' });
      const size = windowEncoder.encode(value).byteLength;
      if (batch.length && (batch.length >= windowBatchSize || bytes + size > windowBatchBytes)) await flush();
      batch.push({ window, value }); bytes += size;
    }
    await flush();
    return accepted;
  }

  private async saveCursor(scope:string,cursor:ReplicaCursor|undefined) {
    if(!cursor)return;
    const epoch=await this.database.meta.get([scope,'epoch']);
    if(epoch && JSON.parse(epoch.value).current!==cursor.epoch)throw new Error('This tab has an obsolete replica epoch. Reconnect before continuing.');
    const prior=await this.database.meta.get([scope,'cursor']);
    const before=prior ? JSON.parse(prior.value) as ReplicaCursor : undefined;
    if(before?.epoch===cursor.epoch && before.revision>cursor.revision)return;
    await this.database.meta.put({scope,key:'cursor',value:JSON.stringify(cursor)});
  }

  private async acceptEpoch(scope:string,cursor:ReplicaCursor|undefined,sequence:number) {
    if(!cursor)return false;
    const prior=await this.database.meta.get([scope,'epoch']);
    const legacyCursor=prior ? undefined : await this.database.meta.get([scope,'cursor']);
    const state=prior ? JSON.parse(prior.value) as {current:string;retired:string[]} : legacyCursor ? {current:(JSON.parse(legacyCursor.value) as ReplicaCursor).epoch,retired:[]} : undefined;
    if(state?.current===cursor.epoch) {
      if(!prior)await this.database.meta.put({scope,key:'epoch',value:JSON.stringify(state)});
      return false;
    }
    if(state?.retired.includes(cursor.epoch))throw new Error('This tab has an obsolete replica epoch. Reconnect before continuing.');
    if(state) {
      await this.database.entities.where('scope').equals(scope).delete();
      await this.database.windows.where('scope').equals(scope).delete();
      await this.database.meta.put({scope,key:'resetSequence',value:String(sequence)});
    }
    await this.database.meta.put({scope,key:'epoch',value:JSON.stringify({current:cursor.epoch,retired:state ? [...state.retired,state.current] : []})});
    return Boolean(state);
  }

  private async pruneRows(scope:string,window:ReplicaWindow,ids:readonly string[],cursor:ReplicaCursor|undefined,sequence:number) {
    if(!ids.length)return;
    const retained=new Set<string>();
    await visitRecords(this.database.windows.where('scope').equals(scope),record=>{
      if(record.deleted || record.signature===window.signature)return;
      const other=JSON.parse(record.value) as ReplicaWindow;
      if(other.entity===window.entity)for(const id of other.ids)retained.add(id);
    });
    for(const id of ids)if(!retained.has(id))await this.writeEntity(scope,window.entity,id,null,cursor,sequence);
  }

  async readChanges(scope:string,afterSequence:number,interest?:{rows:Record<string,string[]>;windows:string[];availableRows:number;availableBytes:number;maxRows:number;maxBytes:number}):Promise<ReplicaStorageChanges> {
    await this.initialize();
    return this.database.transaction('r',this.database.entities,this.database.windows,this.database.meta,async()=>{
      const sequence=Number((await this.database.meta.get([scope,'sequence']))?.value ?? 0);
      const reset=afterSequence < Number((await this.database.meta.get([scope,'resetSequence']))?.value ?? 0);
      const changes:ReplicaStorageChanges={sequence,reset,entities:{},windows:{}};
      if(sequence===afterSequence)return changes;
      const wanted=interest ? new Map(Object.entries(interest.rows).map(([table,ids])=>[table,new Set(ids)])) : undefined;
      let availableRows=interest ? reset ? interest.maxRows : interest.availableRows : 0;
      let availableBytes=interest ? reset ? interest.maxBytes : interest.availableBytes : 0;
      const windows=reset ? this.database.windows.where('scope').equals(scope) : this.database.windows.where('[scope+sequence]').between([scope,afterSequence],[scope,sequence],false,true);
      await visitRecords(windows,record=>{
        const window:ReplicaWindow|null=record.deleted ? null : JSON.parse(record.value);
        changes.windows[record.signature]=window;
        if(window && wanted && interest?.windows.includes(record.signature)){
          let ids=wanted.get(window.entity);if(!ids){ids=new Set();wanted.set(window.entity,ids);}
          for(const id of window.ids)ids.add(id);
        }
      });
      const rows=reset ? this.database.entities.where('scope').equals(scope) : this.database.entities.where('[scope+sequence]').between([scope,afterSequence],[scope,sequence],false,true);
      await visitRecords(rows,record=>{
        if(wanted && !wanted.get(record.entity)?.has(record.id)){
          const size=record.value.length*2+256;
          if(record.deleted || availableRows<=0 || availableBytes<size)return;
          availableRows--;availableBytes-=size;
        }
        (changes.entities[record.entity]??={})[record.id]=record.deleted ? null : JSON.parse(record.value);
      });
      return changes;
    });
  }

  private initialize() {
    return this.initialized ??= this.database.open().then(() => undefined);
  }

  async withReadView<T>(scope: string, coverage: ReadCoverage, run: (view: ReducerReadView) => Promise<T>): Promise<T> {
    await this.initialize();
    scope = normalizeScope(scope);
    const demands = new Map<string, Set<string>>();
    try {
      return await this.database.transaction('r', this.database.entities, this.database.meta, async () => {
        // Database reads run normally in this transaction. Dexie.waitFor must
        // never surround work that also uses this transaction; keep it limited
        // to the deterministic crypto promise supplied by the ID allocator.
        return run({ ...indexedDBReadView(this.database.entities, scope, coverage, entity => this.indexedColumns(scope, entity), (entity, column) => {
          let columns = demands.get(entity);
          if (!columns) { columns = new Set(); demands.set(entity, columns); }
          columns.add(column);
        }),
          keepAliveFor: promise => Dexie.waitFor(promise),
        });
      });
    } finally {
      // An incomplete reducer also teaches us its read columns, often before
      // hydration. Derived work never delays or replaces its result/error.
      this.scheduleColumns(scope, demands);
    }
  }

  async listScopes(): Promise<string[]> {
    await this.initialize();
    const [entities, windows, meta] = await Promise.all([
      this.database.entities.orderBy("scope").uniqueKeys(),
      this.database.windows.orderBy("scope").uniqueKeys(),
      // Derived index policy alone is not application data. In particular,
      // browser upgrade fencing must not treat an empty learned scope as a
      // legacy snapshot that needs application migrations.
      this.database.meta.orderBy('scope').filter(record => !record.key.startsWith('lookupColumns:') && !record.key.startsWith('lookupPending:')).keys(),
    ]);
    return [...new Set([...entities, ...windows, ...meta].map(String))];
  }

  async loadSession(scope: string): Promise<import("./local-replica.js").LocalReplicaSession | undefined> {
    await this.initialize();
    const record = await this.database.meta.get([scope, "session"]);
    return record ? JSON.parse(record.value) : undefined;
  }

  async saveSession(scope: string, session: import("./local-replica.js").LocalReplicaSession | undefined): Promise<void> {
    await this.initialize();
    if (session) await this.database.meta.put({ scope, key: "session", value: JSON.stringify(session) });
    else await this.database.meta.delete([scope, "session"]);
  }

  async load(scope: ReplicaScope = defaultReplicaScope): Promise<ReplicaSnapshot | undefined> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    return this.database.transaction('r',this.database.entities,this.database.windows,this.database.meta,async()=>{
    const [entityRecords, windowRecords, cursor] = await Promise.all([
      this.database.entities.where("scope").equals(normalizedScope).toArray(),
      this.database.windows.where("scope").equals(normalizedScope).toArray(),
      this.database.meta.get([normalizedScope, "cursor"]),
    ]);
    if (entityRecords.length === 0 && windowRecords.length === 0 && !cursor) return undefined;
    const entities: ReplicaSnapshot["entities"] = {};
    for (const record of entityRecords) if(!record.deleted) (entities[record.entity] ??= {})[record.id] = JSON.parse(record.value) as ReplicaRow;
    const liveQueries: ReplicaSnapshot["liveQueries"] = {};
    for (const record of windowRecords) if(!record.deleted) liveQueries[record.signature] = JSON.parse(record.value) as ReplicaWindow;
    return {
      storageSequence:Number((await this.database.meta.get([normalizedScope,'sequence']))?.value ?? 0),
      cursor: cursor ? JSON.parse(cursor.value) : undefined,
      entities,
      liveQueries,
    };
    });
  }

  async loadWorkingSet(scope:string,budget:{maxRows:number;maxBytes:number}):Promise<ReplicaSnapshot|undefined> {
    await this.initialize();
    return this.database.transaction('r',this.database.entities,this.database.windows,this.database.meta,async()=>{
      const entities:ReplicaSnapshot['entities']={},liveQueries:ReplicaSnapshot['liveQueries']={},partial=new Set<string>();
      let bytes=0,count=0,hasRows=false,full=false;
      // getAll-backed pages avoid one JS/Dexie cursor continuation per row.
      // Keep both temporary decoding and retained rows bounded independently.
      let after: [string, string, string] | undefined;
      while (!full) {
        const page = await this.database.entities.where('[scope+entity+id]')
          .between(after ?? [scope], [scope, []], !after, false)
          .limit(Math.min(256, Math.max(1, budget.maxRows - count + 1))).toArray();
        if (!page.length) break;
        for (const record of page) {
          if (record.deleted) continue;
          hasRows = true;
          const size = record.value.length * 2 + 256;
          if (count >= budget.maxRows || bytes + size > budget.maxBytes) { partial.add(record.entity); full = true; break; }
          (entities[record.entity] ??= {})[record.id] = JSON.parse(record.value);
          bytes += size; count++;
        }
        const last = page[page.length - 1]!;
        after = [last.scope, last.entity, last.id];
      }
      await visitRecords(this.database.windows.where('scope').equals(scope),record=>{
        if(record.deleted)return;
        const window:ReplicaWindow=JSON.parse(record.value);liveQueries[record.signature]=window;
        if(window.ids.some(id=>!entities[window.entity]?.[id]))partial.add(window.entity);
      });
      const cursor=await this.database.meta.get([scope,'cursor']);
      if(!hasRows && !Object.keys(liveQueries).length && !cursor)return undefined;
      return {entities,liveQueries,residentPartialTables:[...partial],cursor:cursor?JSON.parse(cursor.value):undefined,storageSequence:Number((await this.database.meta.get([scope,'sequence']))?.value??0)};
    });
  }

  async loadEntityRows(scope:string,entity:string,ids:readonly string[]):Promise<Array<{id:string;row:ReplicaRow}>> {
    await this.initialize();
    return this.database.transaction('r',this.database.entities,async()=>{
    const rows:Array<{id:string;row:ReplicaRow}>=[];
    for(let offset=0;offset<ids.length;offset+=500){
      const records=await this.database.entities.bulkGet(ids.slice(offset,offset+500).map(id=>[scope,entity,id]) as [string,string,string][]);
      for(const record of records)if(record && !record.deleted)rows.push({id:record.id,row:JSON.parse(record.value)});
    }
    return rows;
    });
  }

  async loadWindowRows(scope:string,signature:string) {
    await this.initialize();
    return this.database.transaction('r',this.database.entities,this.database.windows,async()=>{
      const record=await this.database.windows.get([scope,signature]);
      if(!record || record.deleted)return undefined;
      const window:ReplicaWindow=JSON.parse(record.value);
      return {window,rows:(await this.loadEntityRows(scope,window.entity,window.ids)).map(entry=>entry.row)};
    });
  }

  async applyTransaction(transaction: ReplicaTransaction, _snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.acceptEpoch(normalizedScope,transaction.cursor,sequence);
      const grouped = new Map<string, {entity: string; id: string; changes: ReplicaTransaction['changes']}>();
      for (const change of transaction.changes) {
        const key = JSON.stringify([change.entity, change.id]);
        let entry = grouped.get(key);
        if (!entry) { entry = {entity: change.entity, id: change.id, changes: []}; grouped.set(key, entry); }
        entry.changes.push(change);
      }
      const entries = [...grouped.values()];
      const deleted = new Map<string, Set<string>>();
      for (let offset = 0; offset < entries.length; offset += writeBatchSize) {
        const batch = entries.slice(offset, offset + writeBatchSize);
        const previous = await this.database.entities.bulkGet(batch.map(entry => [normalizedScope, entry.entity, entry.id]) as [string, string, string][]);
        const records: EntityRecord[] = [];
        for (const [index, entry] of batch.entries()) {
          let record = previous[index];
          for (const change of entry.changes) {
            if (change.operation !== 'delete' && !change.newValue) continue;
            record = await this.versionedRecord(normalizedScope, entry.entity, entry.id, change.operation === 'delete' ? null : change.newValue!, transaction.cursor, sequence, record);
          }
          if (!record) continue;
          records.push(record);
          if (record.deleted) {
            let ids = deleted.get(entry.entity);
            if (!ids) { ids = new Set(); deleted.set(entry.entity, ids); }
            ids.add(entry.id);
          }
        }
        if (records.length) await this.writeRecords(records);
      }
      if (deleted.size) {
        const windows = await this.database.windows.where('scope').equals(normalizedScope).toArray();
        const changed: WindowRecord[] = [];
        for (const record of windows) {
          if (record.deleted) continue;
          const window = JSON.parse(record.value) as ReplicaWindow;
          const ids = deleted.get(window.entity);
          if (!ids || !window.ids.some(id => ids.has(id)) || (window.cursor?.epoch === transaction.cursor.epoch && window.cursor.revision > transaction.cursor.revision)) continue;
          window.ids = window.ids.filter(id => !ids.has(id));
          changed.push({...record, sequence, value: JSON.stringify(window)});
        }
        await this.database.windows.bulkPut(changed);
      }
      if (transaction.memberships?.length) await this.writeWindows(normalizedScope,transaction.memberships,sequence);
      await this.saveCursor(normalizedScope,transaction.cursor);
    });
    this.channel?.postMessage({scope:normalizedScope});
  }

  async advanceWatermark(
    windows: readonly ReplicaWindow[],
    cursor: ReplicaSnapshot["cursor"],
    scope: ReplicaScope = defaultReplicaScope,
  ): Promise<void | ReplicaMetadataCommit> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    // A watermark contains no entity changes. Keep this transaction limited to
    // window metadata and the shared cursor so large normalized replicas are
    // never rewritten once per retained collection.
    const receipt = await this.database.transaction("rw", this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      const accepted = await this.writeWindows(normalizedScope,windows,sequence);
      await this.saveCursor(normalizedScope,cursor);
      if (accepted && windows.length) return { scope: normalizedScope, previousSequence: sequence - 1, sequence };
    });
    this.channel?.postMessage({scope:normalizedScope});
    return receipt;
  }

  async replaceWindow(window: ReplicaWindow, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope, projection?:readonly ReplicaRow[]): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.acceptEpoch(normalizedScope,window.cursor ?? snapshot.cursor,sequence);
      // A window replacement owns only its projected entity rows. Rewriting the
      // complete normalized replica for every snapshot made startup quadratic:
      // each newly opened collection persisted every entity loaded by all prior
      // collections, and `replica.ready` repeated the same work. Persist this
      // window atomically while leaving unrelated entity tables untouched.
      const rows = () => snapshot.entities[window.entity] ?? {};
      const previousRecord = await this.database.windows.get([normalizedScope, window.signature]);
      if (previousRecord && !previousRecord.deleted) {
        const previous = JSON.parse(previousRecord.value) as ReplicaWindow;
        const nextIDs = new Set(window.ids);
        await this.pruneRows(normalizedScope,window,previous.ids.filter(id=>!nextIDs.has(id) && rows()[id]===undefined),window.cursor ?? snapshot.cursor,sequence);
      }
      await this.writeRows(normalizedScope,window.entity,window.key,projection ?? window.ids.flatMap(id=>rows()[id] ? [rows()[id]!] : []),window.cursor ?? snapshot.cursor,sequence);
      await this.writeWindows(normalizedScope,[window],sequence);
      await this.saveCursor(normalizedScope,snapshot.cursor);
    });
    this.channel?.postMessage({scope:normalizedScope});
  }

  async applyWindowDelta(window: ReplicaWindow, delta: { upserts: ReplicaRow[]; deleted: string[] }, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void | ReplicaMetadataCommit> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    const receipt = await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      const reset = await this.acceptEpoch(normalizedScope,window.cursor ?? snapshot.cursor,sequence);
      const rows = delta.deleted.length ? snapshot.entities[window.entity] ?? {} : {};
      await this.pruneRows(normalizedScope,window,delta.deleted.filter(id=>rows[id]===undefined),window.cursor ?? snapshot.cursor,sequence);
      await this.writeRows(normalizedScope,window.entity,window.key,delta.upserts,window.cursor ?? snapshot.cursor,sequence);
      const accepted = await this.writeWindows(normalizedScope,[window],sequence);
      await this.saveCursor(normalizedScope,snapshot.cursor);
      if (accepted && !reset && delta.upserts.length === 0 && delta.deleted.length === 0) {
        return { scope: normalizedScope, previousSequence: sequence - 1, sequence };
      }
    });
    this.channel?.postMessage({scope:normalizedScope});
    return receipt;
  }

  async removeWindow(signature: string, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.database.windows.put({scope:normalizedScope,signature,value:'null',deleted:true,sequence});
      await this.saveCursor(normalizedScope,snapshot.cursor);
    });
    this.channel?.postMessage({scope:normalizedScope});
  }

  async replaceSnapshot(snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.acceptEpoch(normalizedScope,snapshot.cursor,sequence);
      await this.database.entities.where("scope").equals(normalizedScope).delete();
      await this.database.windows.where("scope").equals(normalizedScope).delete();
      await this.database.meta.delete([normalizedScope, "cursor"]);
      await this.database.meta.put({scope:normalizedScope,key:'resetSequence',value:String(sequence)});
      for (const [entity, rows] of Object.entries(snapshot.entities)) {
        const entries = Object.entries(rows);
        // Empty tables have no IndexedDB work. Repeatedly awaiting already
        // resolved native async helpers can let a browser commit this transaction
        // before the next populated table is written (Dexie.PrematureCommitError).
        if (entries.length) await this.writeRowEntries(normalizedScope,entity,entries,snapshot.cursor,sequence);
      }
      const windows = Object.values(snapshot.liveQueries);
      if (windows.length) await this.writeWindows(normalizedScope,windows,sequence);
      await this.saveCursor(normalizedScope,snapshot.cursor);
    });
    this.channel?.postMessage({scope:normalizedScope});
  }

  async clear(scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    this.generations.set(normalizedScope, (this.generations.get(normalizedScope) ?? 0) + 1);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.database.entities.where("scope").equals(normalizedScope).delete();
      await this.database.windows.where("scope").equals(normalizedScope).delete();
      await this.database.meta.where("scope").equals(normalizedScope).and(record=>record.key!=='sequence' && record.key!=='epoch').delete();
      await this.database.meta.put({scope:normalizedScope,key:'resetSequence',value:String(sequence)});
    });
    // Also cancel jobs scheduled by writes that were ahead of clear in IDB.
    this.generations.set(normalizedScope, (this.generations.get(normalizedScope) ?? 0) + 1);
    this.channel?.postMessage({scope:normalizedScope});
  }

  close() {
    this.closed = true;
    this.channel?.close();
    this.peers.clear();
    this.database.close();
  }
}

export function indexedDBLocalReplica(name?: string): LocalReplicaStorage {
  return new IndexedDBLocalReplicaStorage(name);
}

function normalizeScope(scope: ReplicaScope): ReplicaScope {
  return typeof scope === "string" && scope.trim() ? scope : defaultReplicaScope;
}
