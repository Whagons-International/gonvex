import { Dexie, type Table, type Collection } from "dexie";
import { entityRecord, indexedDBReadView, type EntityRecord, type ReadCoverage } from './indexeddb-read-view.js';
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
const replicaSchemaVersion = 5;
// Keep native IDB request bursts short so another database's durable intent
// journal can run between batches. All batches remain in one atomic transaction.
const writeBatchSize = 4;

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
        record.lookupKeys = entityRecord(record.scope, record.entity, record.id, JSON.parse(record.value)).lookupKeys;
      });
    });
    this.database.version(replicaSchemaVersion).stores({
      entities:'[scope+entity+id], scope, [scope+entity], *lookupKeys, [scope+sequence]',
      windows:'[scope+signature], scope, [scope+sequence]',
    });
  }

  subscribePeer(listener:(scope:string)=>void) {this.peers.add(listener);return()=>{this.peers.delete(listener);};}

  private async nextSequence(scope:string):Promise<number> {
    const prior=await this.database.meta.get([scope,'sequence']);
    const sequence=Number(prior?.value ?? 0)+1;
    await this.database.meta.put({scope,key:'sequence',value:String(sequence)});
    return sequence;
  }

  private versionedRecord(scope:string,entity:string,id:string,row:ReplicaRow|null,cursor:ReplicaCursor|undefined,sequence:number,previous:EntityRecord|undefined):EntityRecord {
    const resolved=cursor ?? {epoch:previous?.authority?.epoch ?? '',revision:0};
    const before=previous ? {row:previous.deleted ? null : JSON.parse(previous.value),authority:previous.authority ?? {epoch:resolved.epoch,fields:{}}} : undefined;
    const merged=mergeReplicaRecord(before,row,resolved);
    // Reconnect snapshots often repeat the same projected values/revisions.
    // Preserve the stored record for a no-op; a revision-only advance reuses
    // its serialized value and secondary keys while updating authority.
    if (previous && merged === before) return previous;
    if (previous && before && merged.row === before.row) return {...previous, authority:merged.authority, sequence};
    const record=merged.row ? entityRecord(scope,entity,id,merged.row) : {scope,entity,id,value:'null',lookupKeys:[],deleted:true};
    return {...record,authority:merged.authority,sequence};
  }

  private async writeEntity(scope:string,entity:string,id:string,row:ReplicaRow|null,cursor:ReplicaCursor|undefined,sequence:number) {
    const previous=await this.database.entities.get([scope,entity,id]);
    const record=this.versionedRecord(scope,entity,id,row,cursor,sequence,previous);
    if (record !== previous) await this.database.entities.put(record);
    return record;
  }

  private async writeRows(scope:string,entity:string,key:string,rows:readonly ReplicaRow[],cursor:ReplicaCursor|undefined,sequence:number) {
    return this.writeRowEntries(scope,entity,rows.map(row=>[String(row[key]),row]),cursor,sequence);
  }

  private async writeRowEntries(scope:string,entity:string,rows:readonly (readonly [string,ReplicaRow])[],cursor:ReplicaCursor|undefined,sequence:number) {
    // Bounded batches retain only the incoming rows and their previous values.
    for(let offset=0;offset<rows.length;offset+=writeBatchSize) {
      const batch=rows.slice(offset,offset+writeBatchSize);
      const prior=await this.database.entities.bulkGet(batch.map(([id])=>[scope,entity,id]) as [string,string,string][]);
      const records = batch.map(([id,row],index)=>this.versionedRecord(scope,entity,id,row,cursor,sequence,prior[index])).filter((record,index)=>record !== prior[index]);
      if (records.length) await this.database.entities.bulkPut(records);
    }
  }

  private async writeWindows(scope:string,windows:readonly ReplicaWindow[],sequence:number) {
    const prior=await this.database.windows.bulkGet(windows.map(window=>[scope,window.signature]) as [string,string][]);
    await this.database.windows.bulkPut(windows.flatMap((window,index)=>{
      const record=prior[index];
      const before=record && !record.deleted ? JSON.parse(record.value) as ReplicaWindow : undefined;
      if(before?.cursor && (!window.cursor || (before.cursor.epoch===window.cursor.epoch && before.cursor.revision>window.cursor.revision)))return [{...record!,sequence}];
      return [{scope,sequence,signature:window.signature,value:JSON.stringify(normalizeWindow(window))}];
    }));
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
    if(!cursor)return;
    const prior=await this.database.meta.get([scope,'epoch']);
    const legacyCursor=prior ? undefined : await this.database.meta.get([scope,'cursor']);
    const state=prior ? JSON.parse(prior.value) as {current:string;retired:string[]} : legacyCursor ? {current:(JSON.parse(legacyCursor.value) as ReplicaCursor).epoch,retired:[]} : undefined;
    if(state?.current===cursor.epoch) {
      if(!prior)await this.database.meta.put({scope,key:'epoch',value:JSON.stringify(state)});
      return;
    }
    if(state?.retired.includes(cursor.epoch))throw new Error('This tab has an obsolete replica epoch. Reconnect before continuing.');
    if(state) {
      await this.database.entities.where('scope').equals(scope).delete();
      await this.database.windows.where('scope').equals(scope).delete();
      await this.database.meta.put({scope,key:'resetSequence',value:String(sequence)});
    }
    await this.database.meta.put({scope,key:'epoch',value:JSON.stringify({current:cursor.epoch,retired:state ? [...state.retired,state.current] : []})});
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
    return this.database.transaction('r', this.database.entities, async () => {
      // Database reads run normally in this transaction. Dexie.waitFor must
      // never surround work that also uses this transaction; keep it limited
      // to the deterministic crypto promise supplied by the ID allocator.
      return run({ ...indexedDBReadView(this.database.entities, normalizeScope(scope), coverage),
        keepAliveFor: promise => Dexie.waitFor(promise),
      });
    });
  }

  async listScopes(): Promise<string[]> {
    await this.initialize();
    const [entities, windows, meta] = await Promise.all([
      this.database.entities.orderBy("scope").uniqueKeys(),
      this.database.windows.orderBy("scope").uniqueKeys(),
      this.database.meta.orderBy("scope").uniqueKeys(),
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
            record = this.versionedRecord(normalizedScope, entry.entity, entry.id, change.operation === 'delete' ? null : change.newValue!, transaction.cursor, sequence, record);
          }
          if (!record) continue;
          records.push(record);
          if (record.deleted) {
            let ids = deleted.get(entry.entity);
            if (!ids) { ids = new Set(); deleted.set(entry.entity, ids); }
            ids.add(entry.id);
          }
        }
        await this.database.entities.bulkPut(records);
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
      await this.writeWindows(normalizedScope,transaction.memberships ?? [],sequence);
      await this.saveCursor(normalizedScope,transaction.cursor);
    });
    this.channel?.postMessage({scope:normalizedScope});
  }

  async advanceWatermark(
    windows: readonly ReplicaWindow[],
    cursor: ReplicaSnapshot["cursor"],
    scope: ReplicaScope = defaultReplicaScope,
  ): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    // A watermark contains no entity changes. Keep this transaction limited to
    // window metadata and the shared cursor so large normalized replicas are
    // never rewritten once per retained collection.
    await this.database.transaction("rw", this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.writeWindows(normalizedScope,windows,sequence);
      await this.saveCursor(normalizedScope,cursor);
    });
    this.channel?.postMessage({scope:normalizedScope});
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

  async applyWindowDelta(window: ReplicaWindow, delta: { upserts: ReplicaRow[]; deleted: string[] }, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.acceptEpoch(normalizedScope,window.cursor ?? snapshot.cursor,sequence);
      const rows = delta.deleted.length ? snapshot.entities[window.entity] ?? {} : {};
      await this.pruneRows(normalizedScope,window,delta.deleted.filter(id=>rows[id]===undefined),window.cursor ?? snapshot.cursor,sequence);
      await this.writeRows(normalizedScope,window.entity,window.key,delta.upserts,window.cursor ?? snapshot.cursor,sequence);
      await this.writeWindows(normalizedScope,[window],sequence);
      await this.saveCursor(normalizedScope,snapshot.cursor);
    });
    this.channel?.postMessage({scope:normalizedScope});
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
      await this.writeWindows(normalizedScope,Object.values(snapshot.liveQueries),sequence);
      await this.saveCursor(normalizedScope,snapshot.cursor);
    });
    this.channel?.postMessage({scope:normalizedScope});
  }

  async clear(scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.database.entities.where("scope").equals(normalizedScope).delete();
      await this.database.windows.where("scope").equals(normalizedScope).delete();
      await this.database.meta.where("scope").equals(normalizedScope).and(record=>record.key!=='sequence' && record.key!=='epoch').delete();
      await this.database.meta.put({scope:normalizedScope,key:'resetSequence',value:String(sequence)});
    });
    this.channel?.postMessage({scope:normalizedScope});
  }

  close() {
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

function normalizeWindow(value: ReplicaWindow | (Omit<ReplicaWindow, "kind"> & { kind?: ReplicaWindow["kind"] })): ReplicaWindow {
  return {
    ...value,
    kind: value.kind ?? "live",
    key: value.key ?? "id",
    ids: [...value.ids],
    resultPath: value.resultPath ? [...value.resultPath] : undefined,
    hashes: value.hashes ? { ...value.hashes } : undefined,
  };
}
