import { Dexie, type Table, type Collection, type Transaction } from "dexie";
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
  ReplicaMetadataCommit,
  ReplicaStorageCommit,
  ReplicaWindowDelta,
  ReplicaWindowWriteContext,
} from "./local-replica.js";

type MemberRecord = {
  scope: string; signature: string; generation: number; entity: string; id: string;
  order: string; hash?: Uint8Array; duplicateOrders?: string[]; hashOnly?: 1;
};
type WindowRecord = { scope: ReplicaScope; signature: string; value: string; sequence?:number; deleted?:boolean; generation?:number; startupSequence?:number };
type MetaRecord = { scope: ReplicaScope; key: string; value: string };
type ReplicaDatabase = Dexie & {
  entities: Table<EntityRecord, [string, string, string]>;
  windows: Table<WindowRecord, [string, string]>;
  meta: Table<MetaRecord, [string, string]>;
  members: Table<MemberRecord, [string, string, number, string]>;
};

const defaultReplicaScope: ReplicaScope = "default";

// Bound native IDB request bursts so another database's durable intent journal
// can run between batches. Four-row batches made medium snapshots spend most
// of their time on native request round trips. Keep one atomic transaction.
const writeBatchSize = 32;
const writeIndexBudget = 64;
// Membership/checkpoint records contain no row payloads. Keep these bursts
// bounded too, but avoid splitting a typical 100-owner delete into 13 writes.
const membershipBatchSize = 128;

// Small replica checkpoints usually change only a few rows. IndexedDB getAll
// avoids a JS/Dexie continuation per row. The fallback remains streaming so a
// large peer update cannot pull an unbounded table into a temporary array.
async function visitRecords<T, Key>(collection: Collection<T, Key>, visit: (record: T) => void): Promise<void> {
  const page = await collection.clone().limit(128).toArray();
  for (const record of page) visit(record);
  if (page.length === 128) await collection.clone().offset(128).each(visit);
}

/** Atomic normalized web persistence for the Gonvex Local Replica. */
export class IndexedDBEngineReplicaStorage implements LocalReplicaStorage {
  readonly supportsResidentLookup = true;
  readonly supportsMembershipDeltas = true;
  // Working-set entity values are parsed afresh on every load. Startup indexes
  // contain serialized membership only, so freezing rows cannot affect storage.
  readonly ownsWorkingSetRows = true;
  private readonly database: ReplicaDatabase;
  private initialized?: Promise<void>;
  private readonly commits = new WeakMap<Transaction, ReplicaStorageCommit>();

  private receipt() { return this.commits.get(Dexie.currentTransaction!); }

  private recordChanges(records: readonly EntityRecord[]) {
    const receipt = this.receipt();
    if (!receipt) return;
    for (const record of records) (receipt.changes.entities[record.entity] ??= Object.create(null))[record.id] = record.deleted ? null : JSON.parse(record.value);
  }

  private async captureRows(scope: string, entity: string, ids: readonly string[]) {
    const receipt = this.receipt();
    if (!receipt || !ids.length) return;
    for (let offset = 0; offset < ids.length; offset += writeBatchSize) {
      const batch = ids.slice(offset, offset + writeBatchSize);
      const records = await this.database.entities.bulkGet(batch.map(id => [scope, entity, id]) as [string, string, string][]);
      for (const [index, id] of batch.entries()) {
        const record = records[index];
        (receipt.changes.entities[entity] ??= Object.create(null))[id] = !record || record.deleted ? null : JSON.parse(record.value);
      }
    }
  }

  private async commit(scope: string, run: (sequence: number) => Promise<void>): Promise<ReplicaStorageCommit> {
    await this.initialize();
    const receipt = await this.database.transaction('rw', this.database.entities, this.database.windows, this.database.members, this.database.meta, async () => {
      const sequence = await this.nextSequence(scope);
      const receipt: ReplicaStorageCommit = { scope, previousSequence: sequence - 1, sequence,
        changes: { sequence, entities: Object.create(null), windows: Object.create(null) } };
      this.commits.set(Dexie.currentTransaction!, receipt);
      await run(sequence);
      return receipt;
    });
    this.channel?.postMessage({ scope });
    this.scheduleCleanup(scope);
    return receipt;
  }
  private readonly channel?:BroadcastChannel;
  private readonly peers=new Set<(scope:string)=>void>();

  constructor(namespace = "gonvex-local-replica") {
    const name = `${namespace}:engine-members-v1`;
    this.database = new Dexie(name, { cache: 'disabled' }) as ReplicaDatabase;
    // Gonvex owns subscription delivery and peer synchronization. These private
    // tables are never queried through Dexie.liveQuery; collecting index ranges
    // and retaining a second query cache duplicates work on every replica write.
    this.database.unuse({ stack: 'dbcore', name: 'Cache' });
    this.database.unuse({ stack: 'dbcore', name: 'Observability' });
    this.channel = typeof BroadcastChannel === 'function' ? new BroadcastChannel(`gonvex-replica:${name}`) : undefined;
    if(this.channel) this.channel.onmessage=event=>{if(typeof event.data?.scope==='string')for(const listener of this.peers)listener(event.data.scope);};
    this.database.version(1).stores({
      entities: '[scope+entity+id], scope, [scope+entity], *lookupKeys, [scope+sequence]',
      windows: '[scope+signature], scope, [scope+sequence]',
      meta: '[scope+key], scope',
      members: '[scope+signature+generation+id], scope, [scope+entity+id], [scope+signature+generation+order], [scope+signature+generation+hashOnly]',
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
    this.recordChanges([record]);
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
    this.recordChanges(records);
  }

  private async writeRows(scope:string,entity:string,key:string,rows:readonly ReplicaRow[],cursor:ReplicaCursor|undefined,sequence:number) {
    return this.writeRowEntries(scope,entity,rows.map(row=>[String(row[key]),row]),cursor,sequence);
  }

  private async writeRowEntries(scope:string,entity:string,rows:readonly (readonly [string,ReplicaRow])[],cursor:ReplicaCursor|undefined,sequence:number) {
    // Bounded batches retain only the incoming rows and their previous values.
    for(let offset=0;offset<rows.length;offset+=writeBatchSize) {
      const batch=rows.slice(offset,offset+writeBatchSize);
      const prior=await this.database.entities.bulkGet(batch.map(([id])=>[scope,entity,id]) as [string,string,string][]);
      const records = batch.map(([id,row],index)=>this.versionedRecord(scope,entity,id,row,cursor,sequence,prior[index]));
      this.recordChanges(records);
      const changed = records.filter((record,index)=>record !== prior[index]);
      if (changed.length) await this.writeRecords(changed);
    }
  }

  private startupPrefix(signature: string, generation: number) {
    return `startup:${JSON.stringify([signature, generation])}:`;
  }

  private memberCacheRecord(member: MemberRecord | { scope: string; signature: string; generation: number; id: string; deleted: true }) {
    const value = 'deleted' in member ? { id: member.id, deleted: true } : {
      id: member.id, order: member.order, hash: member.hash ? hexHash(member.hash) : undefined,
      duplicateOrders: member.duplicateOrders, hashOnly: member.hashOnly,
    };
    return { scope: member.scope,
      key: this.startupPrefix(member.signature, member.generation) + JSON.stringify(member.id), value: JSON.stringify(value) };
  }

  private async cacheMember(member: MemberRecord | { scope: string; signature: string; generation: number; id: string; deleted: true }) {
    await this.database.meta.put(this.memberCacheRecord(member));
  }

  private async putMember(member: MemberRecord) {
    await this.database.members.put(member);
    await this.cacheMember(member);
  }

  private async deleteMember(scope: string, signature: string, generation: number, id: string) {
    await this.database.members.delete([scope, signature, generation, id]);
    await this.cacheMember({ scope, signature, generation, id, deleted: true });
  }

  private async startupWindow(record: WindowRecord, metadata: Omit<ReplicaWindow, 'ids' | 'hashes'> & { hasHashes?: boolean }): Promise<ReplicaWindow | undefined> {
    if (record.startupSequence === undefined || record.startupSequence !== record.sequence) return;
    const prefix = this.startupPrefix(record.signature, record.generation!);
    const cached = await this.database.meta.get([record.scope, prefix]);
    if (!cached) return;
    const base = JSON.parse(cached.value) as { sequence: number; ids: string[]; hashes?: Record<string, string> };
    if (base.sequence !== record.generation || !Array.isArray(base.ids)) return;
    const patches = await this.database.meta.where('[scope+key]')
      .between([record.scope, prefix], [record.scope, prefix + '\uffff'], false, false).toArray();
    const { hasHashes, ...window } = metadata;
    if (!patches.length) return { ...window, ids: base.ids, ...(hasHashes ? { hashes: base.hashes ?? {} } : {}) };
    const members = new Map<string, { id: string; order: string; duplicateOrders?: string[]; hash?: string; hashOnly?: 1 }>();
    base.ids.forEach((id, index) => {
      const prior = members.get(id);
      if (prior) (prior.duplicateOrders ??= []).push(initialOrder(index));
      else members.set(id, { id, order: initialOrder(index), hash: base.hashes?.[id] });
    });
    for (const [id, hash] of Object.entries(base.hashes ?? {})) if (!members.has(id)) members.set(id, { id, order: '', hash, hashOnly: 1 });
    for (const patch of patches) {
      const member = JSON.parse(patch.value);
      if (member.deleted) members.delete(member.id); else members.set(member.id, member);
    }
    const ordered = [...members.values()].flatMap(member => member.hashOnly ? [] :
      [member.order, ...member.duplicateOrders ?? []].map(order => ({ id: member.id, order })));
    ordered.sort((a, b) => a.order < b.order ? -1 : a.order > b.order ? 1 : 0);
    return { ...window, ids: ordered.map(member => member.id),
      ...(hasHashes ? { hashes: Object.fromEntries([...members.values()].filter(member => member.hash !== undefined).map(member => [member.id, member.hash!])) } : {}) };
  }

  private async materializeWindow(record: WindowRecord): Promise<ReplicaWindow> {
    const metadata = JSON.parse(record.value) as Omit<ReplicaWindow, 'ids' | 'hashes'> & { hasHashes?: boolean };
    // The optional index is a cache. Membership remains the recovery source.
    let cached: ReplicaWindow | undefined;
    try { cached = await this.startupWindow(record, metadata); }
    catch (error) { if (!(error instanceof SyntaxError)) throw error; }
    if (cached) return cached;
    const members = await this.database.members.where('[scope+signature+generation+order]')
      .between([record.scope, record.signature, record.generation!, ''], [record.scope, record.signature, record.generation!, '\uffff']).toArray();
    const { hasHashes, ...window } = metadata;
    const ids: string[] = [], hashes: Record<string, string> = Object.create(null);
    const duplicates = members.some(member => member.duplicateOrders?.length);
    if (duplicates) {
      const ordered = members.flatMap(member => member.hashOnly ? [] :
        [member.order, ...member.duplicateOrders ?? []].map(order => ({ id: member.id, order })));
      ordered.sort((a, b) => a.order < b.order ? -1 : a.order > b.order ? 1 : 0);
      for (const member of ordered) ids.push(member.id);
    }
    for (const member of members) {
      if (!duplicates && !member.hashOnly) ids.push(member.id);
      if (hasHashes && member.hash) {
        const hash = hexHash(member.hash);
        if (member.id === '__proto__') Object.defineProperty(hashes, member.id, {
          value: hash, enumerable: true, configurable: true, writable: true,
        });
        else hashes[member.id] = hash;
      }
    }
    return { ...window, ids, ...(hasHashes ? { hashes } : {}) };
  }

  private async writeMetadata(scope: string, window: ReplicaWindow, sequence: number, generation: number, cached = false) {
    const { ids, hashes, ...metadata } = window;
    await this.database.windows.put({ scope, signature: window.signature, sequence, generation, startupSequence: cached ? sequence : undefined,
      value: JSON.stringify({ ...metadata, hasHashes: hashes !== undefined }) });
  }

  private async acceptsWindow(scope: string, window: ReplicaWindow) {
    const prior = await this.database.windows.get([scope, window.signature]);
    const before = prior && !prior.deleted ? JSON.parse(prior.value) as ReplicaWindow : undefined;
    return { prior, accepted: !(before?.cursor && (!window.cursor ||
      (before.cursor.epoch === window.cursor.epoch && before.cursor.revision > window.cursor.revision))) };
  }

  private async writeWindows(scope: string, windows: readonly ReplicaWindow[], sequence: number) {
    let accepted = true;
    for (const window of new Map(windows.map(window => [window.signature, window])).values()) {
      const check = await this.acceptsWindow(scope, window);
      if (!check.accepted) {
        accepted = false;
        const receipt = this.receipt();
        if (receipt && check.prior) receipt.changes.windows[window.signature] = await this.materializeWindow(check.prior);
        continue;
      }
      // Build an inactive generation and switch it in this same atomic transaction.
      const generation = sequence;
      const records = new Map<string, MemberRecord>();
      window.ids.forEach((id, index) => {
        const previous = records.get(id);
        if (previous) (previous.duplicateOrders ??= []).push(initialOrder(index));
        else records.set(id, { scope, signature: window.signature, generation, entity: window.entity, id,
          order: initialOrder(index), hash: binaryHash(window.hashes?.[id]) });
      });
      // Normalized deletes can remove IDs ahead of the collection checkpoint.
      // Preserve that checkpoint's certificate without retaining ownership.
      for (const [id, hash] of Object.entries(window.hashes ?? {})) {
        if (!records.has(id)) records.set(id, { scope, signature: window.signature, generation, entity: window.entity, id,
          order: '', hash: binaryHash(hash), hashOnly: 1 });
      }
      const members = [...records.values()];
      for (let offset = 0; offset < members.length; offset += 128) {
        await this.database.members.bulkPut(members.slice(offset, offset + 128));
      }
      if (check.prior && !check.prior.deleted) await this.retireGeneration(scope, window.signature, check.prior.generation!);
      await this.database.meta.put({ scope, key: this.startupPrefix(window.signature, generation),
        value: JSON.stringify({ sequence: generation, ids: window.ids,
          hashes: window.hashes === undefined ? undefined : Object.fromEntries(members.filter(member => member.hash).map(member => [member.id, hexHash(member.hash!)])) }) });
      await this.writeMetadata(scope, window, sequence, generation, true);
      const receipt = this.receipt();
      if (receipt) receipt.changes.windows[window.signature] = window;
    }
    return accepted;
  }

  private async retireGeneration(scope: string, signature: string, generation: number) {
    await this.database.meta.put({ scope, key: `retired:${JSON.stringify([signature, generation])}`,
      value: JSON.stringify({ signature, generation }) });
    this.pendingCleanup.add(scope);
  }

  /** Delete at most limit retired records. Active generations are never scanned. */
  async cleanupRetiredMemberships(scope = defaultReplicaScope, limit = 32): Promise<number> {
    await this.initialize();
    return this.database.transaction('rw', this.database.members, this.database.meta, async () => {
      const retired = await this.database.meta.where('[scope+key]')
        .between([scope, 'retired:'], [scope, 'retired;'], true, false).first();
      if (!retired) return 0;
      const { signature, generation } = JSON.parse(retired.value) as { signature: string; generation: number };
      const keys = await this.database.members.where('[scope+signature+generation+order]')
        .between([scope, signature, generation, ''], [scope, signature, generation, '\uffff']).limit(limit).primaryKeys();
      await this.database.members.bulkDelete(keys);
      if (keys.length < limit) {
        const prefix = this.startupPrefix(signature, generation);
        const cachedKeys = await this.database.meta.where('[scope+key]')
          .between([scope, prefix], [scope, prefix + '\uffff'], true, false).limit(limit - keys.length).primaryKeys();
        await this.database.meta.bulkDelete(cachedKeys);
        if (cachedKeys.length < limit - keys.length) await this.database.meta.delete([scope, retired.key]);
      }
      return keys.length;
    });
  }

  private closed = false;
  private readonly pendingCleanup = new Set<string>();
  private readonly cleanupTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private scheduleCleanup(scope: string) {
    if (this.closed || !this.pendingCleanup.has(scope) || this.cleanupTimers.has(scope)) return;
    this.cleanupTimers.set(scope, setTimeout(() => {
      this.cleanupTimers.delete(scope);
      void this.cleanupRetiredMemberships(scope).then(async () => {
        if (this.closed) return;
        const pending = await this.database.meta.where('[scope+key]')
          .between([scope, 'retired:'], [scope, 'retired;'], true, false).first();
        if (pending) this.scheduleCleanup(scope); else this.pendingCleanup.delete(scope);
      }).catch(() => { this.pendingCleanup.delete(scope); });
    }, 50));
  }

  private async rebalance(scope: string, signature: string, generation: number, order: string) {
    const index = this.database.members.where('[scope+signature+generation+order]');
    const before = await index.between([scope, signature, generation, ''], [scope, signature, generation, order], true, false).reverse().limit(17).toArray();
    const after = await index.between([scope, signature, generation, order], [scope, signature, generation, '\uffff'], true, true).limit(17).toArray();
    const lower = before.length === 17 ? before.pop()!.order : undefined;
    const upper = after.length === 17 ? after.pop()!.order : undefined;
    const segment = [...before.reverse(), ...after];
    if (segment.some(member => member.duplicateOrders?.length)) return;
    const assign = (start: number, end: number, left?: string, right?: string) => {
      if (start >= end) return;
      const middle = Math.floor((start + end) / 2);
      const key = orderBetween(left, right);
      segment[middle]!.order = key;
      assign(start, middle, left, key); assign(middle + 1, end, key, right);
    };
    assign(0, segment.length, lower, upper);
    await this.database.members.bulkPut(segment);
    for (const member of segment) await this.cacheMember(member);
  }

  private async reconcileCheckpointHashes(scope: string, window: ReplicaWindow, generation: number) {
    const hashes = await this.database.members.where('[scope+signature+generation+hashOnly]')
      .equals([scope, window.signature, generation, 1]).toArray();
    for (const member of hashes) if (!window.hashes || !Object.hasOwn(window.hashes, member.id)) {
      await this.deleteMember(scope, member.signature, generation, member.id);
    }
  }

  private async owners(scope: string, entity: string, id: string, windows = new Map<string, WindowRecord | undefined>()) {
    const members = await this.database.members.where('[scope+entity+id]').equals([scope, entity, id]).toArray();
    const active: Array<{ member: MemberRecord; window: WindowRecord }> = [];
    let signatures = [...new Set(members.filter(member => !member.hashOnly).map(member => member.signature))]
      .filter(signature => !windows.has(signature));
    if (signatures.length >= writeBatchSize && windows.size < writeBatchSize) {
      // bulkGet still issues one native request per key. Most replicas have
      // few small window records; one bounded getAll avoids that request fanout.
      const records = await this.database.windows.where('scope').equals(scope).limit(membershipBatchSize).toArray();
      for (const record of records) windows.set(record.signature, record);
      signatures = signatures.filter(signature => !windows.has(signature));
    }
    if (signatures.length) {
      const records = await this.database.windows.bulkGet(signatures.map(signature => [scope, signature]));
      signatures.forEach((signature, index) => windows.set(signature, records[index]));
    }
    for (const member of members) {
      if (member.hashOnly) continue;
      const window = windows.get(member.signature);
      if (window && !window.deleted && window.generation === member.generation) active.push({ member, window });
    }
    return active;
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
      await this.database.members.where('scope').equals(scope).delete();
      await this.database.meta.where('[scope+key]').between([scope, 'startup:'], [scope, 'startup;'], true, false).delete();
      await this.database.meta.put({scope,key:'resetSequence',value:String(sequence)});
    }
    await this.database.meta.put({scope,key:'epoch',value:JSON.stringify({current:cursor.epoch,retired:state ? [...state.retired,state.current] : []})});
    if (state && this.receipt()) this.receipt()!.changes.reset = true;
    return Boolean(state);
  }

  private async pruneRows(scope:string,window:ReplicaWindow,ids:readonly string[],cursor:ReplicaCursor|undefined,sequence:number) {
    for (const id of ids) {
      if (!(await this.owners(scope, window.entity, id)).some(owner => owner.member.signature !== window.signature)) {
        await this.writeEntity(scope, window.entity, id, null, cursor, sequence);
      } else await this.captureRows(scope, window.entity, [id]);
    }
  }

  async readChanges(scope:string,afterSequence:number,interest?:{rows:Record<string,string[]>;hasRow?:(entity:string,id:string)=>boolean;windows:string[];availableRows:number;availableBytes:number;maxRows:number;maxBytes:number}):Promise<ReplicaStorageChanges> {
    await this.initialize();
    return this.database.transaction('r',this.database.entities,this.database.windows,this.database.members,this.database.meta,async()=>{
      const sequence=Number((await this.database.meta.get([scope,'sequence']))?.value ?? 0);
      const reset=afterSequence < Number((await this.database.meta.get([scope,'resetSequence']))?.value ?? 0);
      const changes:ReplicaStorageChanges={sequence,reset,entities:Object.create(null),windows:Object.create(null)};
      if(sequence===afterSequence)return changes;
      const wanted=interest ? new Map(Object.entries(interest.rows).map(([table,ids])=>[table,new Set(ids)])) : undefined;
      let availableRows=interest ? reset ? interest.maxRows : interest.availableRows : 0;
      let availableBytes=interest ? reset ? interest.maxBytes : interest.availableBytes : 0;
      const windows=reset ? this.database.windows.where('scope').equals(scope) : this.database.windows.where('[scope+sequence]').between([scope,afterSequence],[scope,sequence],false,true);
      for (const record of await windows.toArray()) {
        const window:ReplicaWindow|null=record.deleted ? null : await this.materializeWindow(record);
        changes.windows[record.signature]=window;
        if(window && wanted && interest?.windows.includes(record.signature)){
          let ids=wanted.get(window.entity);if(!ids){ids=new Set();wanted.set(window.entity,ids);}
          for(const id of window.ids)ids.add(id);
        }
      }
      const rows=reset ? this.database.entities.where('scope').equals(scope) : this.database.entities.where('[scope+sequence]').between([scope,afterSequence],[scope,sequence],false,true);
      await visitRecords(rows,record=>{
        if(wanted && !wanted.get(record.entity)?.has(record.id) && !interest?.hasRow?.(record.entity,record.id)){
          const size=record.value.length*2+256;
          if(record.deleted || availableRows<=0 || availableBytes<size)return;
          availableRows--;availableBytes-=size;
        }
        (changes.entities[record.entity]??=Object.create(null))[record.id]=record.deleted ? null : JSON.parse(record.value);
      });
      return changes;
    });
  }

  private initialize() {
    return this.initialized ??= this.database.open().then(async () => {
      // Read only scope keys and retirement markers, not the large startup blobs.
      let after: string | undefined;
      for (;;) {
        const keys = await this.database.meta.where('[scope+key]')
          .between(after === undefined ? [] : [after, []], [[]], true, false).limit(1).primaryKeys();
        const scope = keys[0]?.[0];
        if (scope === undefined) break;
        const retired = await this.database.meta.where('[scope+key]')
          .between([scope, 'retired:'], [scope, 'retired;'], true, false).limit(1).toArray();
        if (retired.length) { this.pendingCleanup.add(scope); this.scheduleCleanup(scope); }
        after = scope;
      }
    });
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
    return this.database.transaction('r',this.database.entities,this.database.windows,this.database.members,this.database.meta,async()=>{
    const [entityRecords, windowRecords, cursor] = await Promise.all([
      this.database.entities.where("scope").equals(normalizedScope).toArray(),
      this.database.windows.where("scope").equals(normalizedScope).toArray(),
      this.database.meta.get([normalizedScope, "cursor"]),
    ]);
    if (entityRecords.length === 0 && windowRecords.length === 0 && !cursor) return undefined;
    const entities: ReplicaSnapshot["entities"] = Object.create(null);
    for (const record of entityRecords) if(!record.deleted) (entities[record.entity] ??= Object.create(null))[record.id] = JSON.parse(record.value) as ReplicaRow;
    const liveQueries: ReplicaSnapshot["liveQueries"] = Object.create(null);
    for (const record of windowRecords) if(!record.deleted) liveQueries[record.signature] = await this.materializeWindow(record);
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
    return this.database.transaction('r',this.database.entities,this.database.windows,this.database.members,this.database.meta,async()=>{
      const entities:ReplicaSnapshot['entities']={},liveQueries:ReplicaSnapshot['liveQueries']={},partial=new Set<string>();
      let bytes=0,count=0,hasRows=false,full=false,averageBytes=0;
      // Read ahead by at most one small page while decoding the current page.
      // Do not prefetch across either resident budget; membership is atomic in
      // this same read transaction even while a native request is outstanding.
      const readPage = (after: [string, string, string] | undefined, rows: number, usedBytes: number) => {
        const limit = Math.min(256, Math.max(1, budget.maxRows - rows + 1),
          Number.isFinite(budget.maxBytes) ? Math.max(1, Math.ceil((budget.maxBytes - usedBytes) / (averageBytes || 256)) + 1) : 256);
        const page = this.database.entities.where('[scope+entity+id]')
          .between(after ?? [scope], [scope, []], !after, false).limit(limit).toArray();
        // A decoding error can abort before we await the prefetched request.
        // Its rejection is still propagated when awaited on the normal path.
        void page.catch(() => {});
        return { page, limit };
      };
      let pending = readPage(undefined, count, bytes);
      while (!full) {
        const { page: request, limit } = pending;
        const page = await request;
        if (!page.length) break;
        const last = page[page.length - 1]!;
        const after: [string, string, string] = [last.scope, last.entity, last.id];
        let nextRows = count, nextBytes = bytes, pageBytes = 0;
        for (const record of page) {
          const size = record.value.length * 2 + 256;
          pageBytes += size;
          if (!record.deleted) { nextRows++; nextBytes += size; }
        }
        averageBytes = pageBytes / page.length;
        const next = page.length === limit && nextRows < budget.maxRows && nextBytes < budget.maxBytes
          ? readPage(after, nextRows, nextBytes) : undefined;
        for (const record of page) {
          if (record.deleted) continue;
          hasRows = true;
          const size = record.value.length * 2 + 256;
          if (count >= budget.maxRows || bytes + size > budget.maxBytes) { partial.add(record.entity); full = true; break; }
          (entities[record.entity] ??= Object.create(null))[record.id] = JSON.parse(record.value);
          bytes += size; count++;
        }
        if (full || page.length < limit) break;
        pending = next ?? readPage(after, count, bytes);
      }
      const records = (await this.database.windows.where('scope').equals(scope).toArray()).filter(record => !record.deleted);
      for (let offset = 0; offset < records.length; offset += 4) {
        const windows = await Promise.all(records.slice(offset, offset + 4).map(record => this.materializeWindow(record)));
        for (const window of windows) {
          liveQueries[window.signature]=window;
          if(window.ids.some(id=>!entities[window.entity]?.[id]))partial.add(window.entity);
        }
      }
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
    return this.database.transaction('r',this.database.entities,this.database.windows,this.database.members,this.database.meta,async()=>{
      const record=await this.database.windows.get([scope,signature]);
      if(!record || record.deleted)return undefined;
      const window = await this.materializeWindow(record);
      return {window,rows:(await this.loadEntityRows(scope,window.entity,window.ids)).map(entry=>entry.row)};
    });
  }

  async applyTransaction(transaction: ReplicaTransaction, _snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<ReplicaStorageCommit> {
    const normalizedScope = normalizeScope(scope);
    return this.commit(normalizedScope, async sequence => {
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
          this.recordChanges([record]);
          if (!record.deleted && entry.changes.some(change => change.operation === 'delete')) {
            for (const owner of await this.owners(normalizedScope, entry.entity, entry.id)) {
              this.receipt()!.changes.windows[owner.member.signature] = await this.materializeWindow(owner.window);
            }
          }
          // Snapshots can already cover a delayed or repeated transaction.
          // Preserve per-field authority, but do not rewrite an unchanged row
          // and all its indexes while another database admits a user intent.
          if (record !== previous[index]) records.push(record);
          if (record.deleted) {
            let ids = deleted.get(entry.entity);
            if (!ids) { ids = new Set(); deleted.set(entry.entity, ids); }
            ids.add(entry.id);
          }
        }
        if (records.length) await this.writeRecords(records);
      }
      const removals: [string, string, number, string][] = [], retained: MemberRecord[] = [], patches: MetaRecord[] = [];
      const checkpoints = new Map<string, WindowRecord>(), ownerWindows = new Map<string, WindowRecord | undefined>();
      for (const [entity, ids] of deleted) for (const id of ids) {
        for (const { member, window } of await this.owners(normalizedScope, entity, id, ownerWindows)) {
          const metadata = JSON.parse(window.value) as ReplicaWindow & { hasHashes?: boolean };
          if (metadata.cursor?.epoch === transaction.cursor.epoch && metadata.cursor.revision > transaction.cursor.revision) continue;
          // Retain projected hashes until the collection certificate advances,
          // without retaining membership/ownership. Batch native requests while
          // keeping rows, startup patches and checkpoints in this transaction.
          const keepHash = metadata.hasHashes && member.hash;
          if (keepHash) retained.push({ ...member, hashOnly: 1 });
          else removals.push([normalizedScope, member.signature, member.generation, id]);
          patches.push({ scope: normalizedScope,
            key: this.startupPrefix(member.signature, member.generation) + JSON.stringify(id),
            value: JSON.stringify(keepHash ? { id, order: member.order, hash: hexHash(member.hash!),
              duplicateOrders: member.duplicateOrders, hashOnly: 1 } : { id, deleted: true }) });
          checkpoints.set(member.signature, { ...window, sequence,
            startupSequence: window.startupSequence !== undefined && window.startupSequence === window.sequence ? sequence : undefined });
          (this.receipt()!.changes.removedMemberships ??= []).push({ signature: member.signature, id });
        }
      }
      const windows = [...checkpoints.values()];
      const edits = Math.max(removals.length, retained.length, patches.length, windows.length);
      for (let offset = 0; offset < edits; offset += membershipBatchSize) {
        // These keys are disjoint and all writes are private until commit.
        await Promise.all([
          removals.length > offset ? this.database.members.bulkDelete(removals.slice(offset, offset + membershipBatchSize)) : undefined,
          retained.length > offset ? this.database.members.bulkPut(retained.slice(offset, offset + membershipBatchSize)) : undefined,
          patches.length > offset ? this.database.meta.bulkPut(patches.slice(offset, offset + membershipBatchSize)) : undefined,
          windows.length > offset ? this.database.windows.bulkPut(windows.slice(offset, offset + membershipBatchSize)) : undefined,
        ]);
      }
      if (transaction.memberships?.length) await this.writeWindows(normalizedScope,transaction.memberships,sequence);
      await this.saveCursor(normalizedScope,transaction.cursor);
    });
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
    const receipt = await this.database.transaction("rw", this.database.windows,this.database.members, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      let accepted = true;
      for (const window of windows) {
        const check = await this.acceptsWindow(normalizedScope, window);
        if (!check.accepted) { accepted = false; continue; }
        if (!check.prior || check.prior.deleted) { await this.writeWindows(normalizedScope, [window], sequence); continue; }
        await this.reconcileCheckpointHashes(normalizedScope, window, check.prior.generation!);
        await this.writeMetadata(normalizedScope, window, sequence, check.prior.generation!, check.prior.startupSequence !== undefined && check.prior.startupSequence === check.prior.sequence);
      }
      await this.saveCursor(normalizedScope,cursor);
      if (accepted && windows.length) return { scope: normalizedScope, previousSequence: sequence - 1, sequence };
    });
    this.channel?.postMessage({scope:normalizedScope});
    this.scheduleCleanup(normalizedScope);
    return receipt;
  }

  async replaceWindow(window: ReplicaWindow, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope, projection?:readonly ReplicaRow[], context?: ReplicaWindowWriteContext): Promise<ReplicaStorageCommit> {
    const normalizedScope = normalizeScope(scope);
    return this.commit(normalizedScope, async sequence => {
      await this.acceptEpoch(normalizedScope,window.cursor ?? snapshot.cursor,sequence);
      // A window replacement owns only its projected entity rows. Rewriting the
      // complete normalized replica for every snapshot made startup quadratic:
      // each newly opened collection persisted every entity loaded by all prior
      // collections, and `replica.ready` repeated the same work. Persist this
      // window atomically while leaving unrelated entity tables untouched.
      const rows = () => snapshot.entities[window.entity] ?? {};
      const check = await this.acceptsWindow(normalizedScope, window);
      if (check.accepted && check.prior && !check.prior.deleted) {
        const previous = await this.materializeWindow(check.prior);
        const nextIDs = new Set(window.ids);
        await this.pruneRows(normalizedScope,window,previous.ids.filter(id=>!nextIDs.has(id) && rows()[id]===undefined),window.cursor ?? snapshot.cursor,sequence);
      }
      await this.writeRows(normalizedScope,window.entity,window.key,projection ?? window.ids.flatMap(id=>rows()[id] ? [rows()[id]!] : []),window.cursor ?? snapshot.cursor,sequence);
      await this.writeWindows(normalizedScope,[window],sequence);
      await this.captureRows(normalizedScope, window.entity, context?.removedIDs ?? []);
      await this.saveCursor(normalizedScope,snapshot.cursor);
    });
  }

  async applyWindowDelta(window: ReplicaWindow, delta: ReplicaWindowDelta, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<ReplicaStorageCommit> {
    const normalizedScope = normalizeScope(scope);
    return this.commit(normalizedScope, async sequence => {
      const reset = await this.acceptEpoch(normalizedScope, window.cursor ?? snapshot.cursor, sequence);
      const check = await this.acceptsWindow(normalizedScope, window);
      const generation = check.prior?.generation ?? sequence;
      const rows = delta.deleted.length ? snapshot.entities[window.entity] ?? {} : {};
      if (check.accepted) await this.pruneRows(normalizedScope, window, delta.deleted.filter(id => rows[id] === undefined), window.cursor ?? snapshot.cursor, sequence);
      await this.writeRows(normalizedScope, window.entity, window.key, delta.upserts, window.cursor ?? snapshot.cursor, sequence);
      if (!check.accepted) {
        await this.captureRows(normalizedScope, window.entity, delta.deleted);
        if (check.prior) this.receipt()!.changes.windows[window.signature] = await this.materializeWindow(check.prior);
      } else if (reset || !check.prior || check.prior.deleted) {
        await this.writeWindows(normalizedScope, [window], sequence);
      } else {
        for (const id of delta.deleted) await this.deleteMember(normalizedScope, window.signature, generation, id);
        const changed = new Set(delta.upserts.map(row => String(row[window.key])));
        const edits: NonNullable<ReplicaWindowDelta['memberships']> = delta.memberships ?? window.ids.flatMap((id, index) => {
          if (!changed.has(id)) return [];
          let next = index + 1;
          while (next < window.ids.length && changed.has(window.ids[next]!)) next++;
          return [{ id, moved: Boolean(window.orderBy), after: window.ids[index - 1], before: window.ids[next] }];
        });
        // Existing, stationary members have no order dependencies. Read and
        // write these records together, including their startup-cache patches,
        // inside this same transaction. Inserts, hash-only tombstones and moves
        // keep the sequential path because later neighbors can depend on them.
        const stationary = edits.every(change => !change.moved);
        for (let offset = 0; offset < edits.length; offset += membershipBatchSize) {
          const batch = edits.slice(offset, offset + membershipBatchSize);
          if (stationary) {
            const previous = await this.database.members.bulkGet(batch.map(change =>
              [normalizedScope, window.signature, generation, change.id] as [string, string, number, string]));
            if (batch.every((_, index) => previous[index] && !previous[index]!.hashOnly && previous[index]!.order.length <= 64)) {
              const members = batch.map((change, index): MemberRecord => ({
                scope: normalizedScope, signature: window.signature, generation, entity: window.entity,
                id: change.id, order: previous[index]!.order,
                hash: binaryHash(window.hashes?.[change.id]), duplicateOrders: previous[index]!.duplicateOrders,
              }));
              await Promise.all([
                this.database.members.bulkPut(members),
                this.database.meta.bulkPut(members.map(member => this.memberCacheRecord(member))),
              ]);
              continue;
            }
          }
          for (const change of batch) {
            const key: [string, string, number, string] = [normalizedScope, window.signature, generation, change.id];
            const prior = await this.database.members.get(key);
            if ((!prior || prior.hashOnly) && !change.moved) {
              const index = window.ids.indexOf(change.id);
              if (index < 0) continue;
              change.moved = true;
              if (!delta.memberships) { change.after ??= window.ids[index - 1]; }
              else { change.after = window.ids[index - 1]; change.before = window.ids[index + 1]; }
            }
            const order = change.moved || !prior ? orderBetween(
              change.after ? (await this.database.members.get([normalizedScope, window.signature, generation, change.after]))?.order : undefined,
              change.before ? (await this.database.members.get([normalizedScope, window.signature, generation, change.before]))?.order : undefined,
            ) : prior.order;
            await this.putMember({ scope: normalizedScope, signature: window.signature, generation,
              entity: window.entity, id: change.id, order,
              hash: binaryHash(window.hashes?.[change.id]), duplicateOrders: prior?.duplicateOrders });
            if (order.length > 64) await this.rebalance(normalizedScope, window.signature, generation, order);
          }
        }
        await this.reconcileCheckpointHashes(normalizedScope, window, generation);
        await this.writeMetadata(normalizedScope, window, sequence, generation, check.prior.startupSequence !== undefined && check.prior.startupSequence === check.prior.sequence);
        this.receipt()!.changes.windows[window.signature] = window;
      }
      await this.saveCursor(normalizedScope, snapshot.cursor);
    });
  }

  async removeWindow(signature: string, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.windows,this.database.members, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      const prior = await this.database.windows.get([normalizedScope, signature]);
      if (prior && !prior.deleted) await this.retireGeneration(normalizedScope, signature, prior.generation!);
      await this.database.windows.put({scope:normalizedScope,signature,value:'null',deleted:true,sequence});
      await this.saveCursor(normalizedScope,snapshot.cursor);
    });
    this.channel?.postMessage({scope:normalizedScope});
    this.scheduleCleanup(normalizedScope);
  }

  async replaceSnapshot(snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows,this.database.members, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.acceptEpoch(normalizedScope,snapshot.cursor,sequence);
      const current = await this.database.meta.get([normalizedScope, 'cursor']);
      const cursor = current ? JSON.parse(current.value) as ReplicaCursor : undefined;
      const stale = Boolean(cursor && snapshot.cursor && cursor.epoch === snapshot.cursor.epoch && cursor.revision > snapshot.cursor.revision);
      if (!stale) {
        // A same-epoch replacement must retain tombstones and field authority.
        // Absence is a versioned delete, not permission to forget newer replay.
        let after: [string, string, string] | undefined;
        for (;;) {
          const page = await this.database.entities.where('[scope+entity+id]')
            .between(after ?? [normalizedScope], [normalizedScope, []], !after, false).limit(128).toArray();
          if (!page.length) break;
          const absent = page.filter(record => !Object.hasOwn(snapshot.entities[record.entity] ?? {}, record.id));
          const records = absent.map(record => this.versionedRecord(normalizedScope, record.entity, record.id, null, snapshot.cursor, sequence, record));
          if (records.length) await this.writeRecords(records.filter((record, index) => record !== absent[index]));
          const last = page.at(-1)!; after = [last.scope, last.entity, last.id];
        }
        for (const record of await this.database.windows.where('scope').equals(normalizedScope).toArray()) {
          if (record.deleted || snapshot.liveQueries[record.signature]) continue;
          const window = JSON.parse(record.value) as ReplicaWindow;
          if (window.cursor && (!snapshot.cursor || (window.cursor.epoch === snapshot.cursor.epoch && window.cursor.revision > snapshot.cursor.revision))) continue;
          await this.retireGeneration(normalizedScope, record.signature, record.generation!);
          await this.database.windows.put({ ...record, deleted: true, value: 'null', sequence });
        }
        await this.database.meta.put({scope:normalizedScope,key:'resetSequence',value:String(sequence)});
      }
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
    this.scheduleCleanup(normalizedScope);
  }

  async clear(scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.initialize();
    const normalizedScope = normalizeScope(scope);
    await this.database.transaction("rw", this.database.entities, this.database.windows,this.database.members, this.database.meta, async () => {
      const sequence=await this.nextSequence(normalizedScope);
      await this.database.entities.where("scope").equals(normalizedScope).delete();
      await this.database.windows.where("scope").equals(normalizedScope).delete();
      await this.database.members.where("scope").equals(normalizedScope).delete();
      await this.database.meta.where("scope").equals(normalizedScope).and(record=>record.key!=='sequence' && record.key!=='epoch').delete();
      await this.database.meta.put({scope:normalizedScope,key:'resetSequence',value:String(sequence)});
    });
    this.channel?.postMessage({scope:normalizedScope});
    this.scheduleCleanup(normalizedScope);
  }

  close() {
    this.closed = true;
    for (const timer of this.cleanupTimers.values()) clearTimeout(timer);
    this.cleanupTimers.clear();
    this.channel?.close();
    this.peers.clear();
    this.database.close();
  }
}

export function indexedDBEngineReplica(name?: string): LocalReplicaStorage {
  return new IndexedDBEngineReplicaStorage(name);
}

function normalizeScope(scope: ReplicaScope): ReplicaScope {
  return typeof scope === "string" && scope.trim() ? scope : defaultReplicaScope;
}

function binaryHash(hash: string | undefined): Uint8Array | undefined {
  if (hash === undefined) return undefined;
  return Uint8Array.from({ length: 32 }, (_, index) => parseInt(hash.slice(index * 2, index * 2 + 2), 16));
}
const hexBytes = Array.from({ length: 256 }, (_, byte) => byte.toString(16).padStart(2, '0'));
function hexHash(hash: Uint8Array): string {
  let value = '';
  for (let index = 0; index < hash.length; index++) value += hexBytes[hash[index]!]!;
  return value;
}
function initialOrder(index: number): string { return (index + 1).toString(16).padStart(12, '0') + '8'; }
function orderBetween(lower?: string, upper?: string): string {
  if (lower !== undefined && upper !== undefined && lower >= upper) throw new Error('Invalid membership neighbors');
  let prefix = '';
  for (let index = 0; ; index++) {
    const left = index < (lower?.length ?? 0) ? parseInt(lower![index]!, 16) : 0;
    const right = index < (upper?.length ?? 0) ? parseInt(upper![index]!, 16) : 15;
    if (right - left > 1) return prefix + Math.floor((left + right) / 2).toString(16);
    prefix += left.toString(16);
    if (left !== right) upper = undefined;
  }
}
