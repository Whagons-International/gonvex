import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {Dexie} from 'dexie';
import {IndexedDBLocalReplicaStorage} from './indexeddb-replica';
import {LocalReplica,type ReplicaWindow} from './local-replica';

let storage:IndexedDBLocalReplicaStorage;
beforeEach(()=>{
  Dexie.dependencies.indexedDB=new IDBFactory();Dexie.dependencies.IDBKeyRange=IDBKeyRange;
  storage=new IndexedDBLocalReplicaStorage(`residency-${Math.random()}`);
});
afterEach(()=>storage.close());
const rows=Array.from({length:20},(_,i)=>({_id:`row-${String(i).padStart(2,'0')}`,name:`Name ${i}`,status:'new'}));
const window:ReplicaWindow={signature:'all',kind:'replica',entity:'items',key:'_id',ids:rows.map(row=>row._id),completeness:'complete',source:'server',cursor:{epoch:'e',revision:1}};
async function seed(){await storage.replaceWindow(window,{entities:{items:Object.fromEntries(rows.map(row=>[row._id,row]))},liveQueries:{all:window}},'scope',rows);}
const coverage=()=>({items:{key:'_id',complete:true,columns:['_id','name','status']}});

it('does not mistake a complete disk slice for complete resident rows', async () => {
  await seed();
  const replica = new LocalReplica(storage,{maxResidentRows:3});
  await replica.activateScope('scope');
  const known=()=>({items:{key:'_id',complete:false,columns:['_id','status'],completeWhere:[{column:'status',op:'eq' as const,value:'new'}]}});
  const read={table:'items',columns:['_id'],where:{column:'status',op:'eq' as const,value:'new'}};
  expect((await replica.withReadView(known,false,view=>view.select(read),true)).complete).toBe(false);
  const result=await replica.withReadView(known,false,async view=>{
    const selected=await view.select(read);
    if(!selected.complete){const error=new Error('requires disk');error.name='IncompleteReplicaError';throw error;}
    return selected;
  });
  expect(result.rows).toHaveLength(20);
  expect(replica.entityRows('items')).toHaveLength(3);
});

it('hydrates a bounded working set while indexed reducer reads retain complete disk coverage',async()=>{
  await seed();
  const replica=new LocalReplica(storage,{maxResidentRows:3});
  await replica.activateScope('scope');
  expect(replica.entityRows('items')).toHaveLength(3);
  expect(replica.collectionState('all').completeness).toBe('partial');
  const cold=await replica.withReadView(coverage,false,view=>view.select({table:'items',where:{column:'_id',op:'eq',value:'row-19'}}));
  // Direct read views signal incomplete coverage; the portable executor raises
  // IncompleteReplicaError to retry the entire reducer against the disk view.
  expect(cold.complete).toBe(false);
  const result=await replica.withReadView(coverage,false,async view=>{
    const read=await view.select({table:'items',where:{column:'_id',op:'eq',value:'row-19'}});
    if(!read.complete){const error=new Error('cold');error.name='IncompleteReplicaError';throw error;}
    return read;
  });
  expect(result.rows).toEqual([rows[19]]);
  expect(result.complete).toBe(true);
  expect(replica.entityRows('items')).toHaveLength(3);
  expect(Object.keys((await storage.load('scope'))!.entities.items!)).toHaveLength(20);
});

it('pins active entity views, loads them by ID, and releases cold RAM without deleting disk rows',async()=>{
  await seed();const replica=new LocalReplica(storage,{maxResidentRows:3});await replica.activateScope('scope');
  const release=replica.retainRows('items',['row-19']);
  await replica.withReadView(coverage,false,async()=>undefined);
  expect(replica.entity('items','row-19')).toEqual(rows[19]);
  expect(replica.entityRows('items')).toHaveLength(3);
  release();
  expect(Object.keys((await storage.load('scope'))!.entities.items!)).toHaveLength(20);
});

it('retains an active collection atomically and trims it after its view closes',async()=>{
  await seed();const replica=new LocalReplica(storage,{maxResidentRows:3});await replica.activateScope('scope');
  const release=replica.retainWindow('all');await replica.withReadView(coverage,false,async()=>undefined);
  expect(replica.windowRows('all')).toEqual(rows);
  expect(replica.collectionState('all').completeness).toBe('complete');
  release();expect(replica.entityRows('items')).toHaveLength(3);
  expect((await storage.loadWindowRows('scope','all'))?.rows).toEqual(rows);
});

it('peer refreshes update hot rows without pulling unrelated cold rows into RAM',async()=>{
  await seed();const replica=new LocalReplica(storage,{maxResidentRows:3});await replica.activateScope('scope');
  await storage.applyTransaction({cursor:{epoch:'e',revision:2},changes:rows.map(row=>({entity:'items',id:row._id,operation:'update' as const,newValue:{...row,status:'done'}}))},{entities:{},liveQueries:{}},'scope');
  await replica.synchronizeStorage();
  expect(replica.entityRows('items')).toHaveLength(3);
  expect(replica.entityRows('items').every(row=>row.status==='done')).toBe(true);
  expect((await storage.loadEntityRows('scope','items',['row-19']))[0]?.row.status).toBe('done');
});

it('limits bytes independently of row count and retains custom primary keys without a window',async()=>{
  const entities={members:Object.fromEntries(Array.from({length:10},(_,i)=>[String(i),{memberKey:String(i),bio:'x'.repeat(1000)}]))};
  await storage.replaceSnapshot({entities,liveQueries:{}},'scope');
  const replica=new LocalReplica(storage,{maxResidentRows:100,maxResidentBytes:3000});
  await replica.activateScope('scope');
  expect(replica.entityRows('members')).toHaveLength(1);
  const release=replica.retainRows('members',['9']);
  await replica.withReadView(()=>({}),false,async()=>undefined);
  expect(replica.entity('members','9')).toEqual(entities.members['9']);
  expect(replica.entityRows('members')).toHaveLength(1);
  release();
  expect(Object.keys((await storage.load('scope'))!.entities.members!)).toHaveLength(10);
});

it('does not install rows from an outstanding hydration after the tenant changes',async()=>{
  await seed();
  const replica=new LocalReplica(storage,{maxResidentRows:3});await replica.activateScope('scope');
  const original=storage.loadEntityRows.bind(storage);
  let resume!:()=>void;
  let started!:()=>void;
  const ready=new Promise<void>(resolve=>{started=resolve;});
  storage.loadEntityRows=async(...args)=>{const rows=await original(...args);started();await new Promise<void>(resolve=>{resume=resolve;});return rows;};
  const release=replica.retainRows('items',['row-19']);await ready;
  const changed=replica.activateScope('other');resume();await changed;
  expect(replica.entity('items','row-19')).toBeUndefined();
  release();
});

it('restores bounded pages across table boundaries without leaking another scope', async () => {
  const records = Array.from({length: 600}, (_, index) => ({_id: String(index).padStart(4, '0'), name: `row ${index}`}));
  const snapshot = {entities: {items: Object.fromEntries(records.map(row => [row._id, row]))}, liveQueries: {}};
  await storage.replaceSnapshot(snapshot, 'scope');
  await storage.replaceSnapshot({entities: {items: {foreign: {_id: 'foreign'}}}, liveQueries: {}}, 'scope-next');
  const bounded = await storage.loadWorkingSet('scope', {maxRows: 300, maxBytes: 1024 * 1024});
  expect(Object.keys(bounded!.entities.items!)).toHaveLength(300);
  expect(bounded!.entities.items!['0299']).toEqual(records[299]);
  expect(bounded!.entities.items!.foreign).toBeUndefined();
  const complete = await storage.loadWorkingSet('scope', {maxRows: 1000, maxBytes: 1024 * 1024});
  expect(Object.keys(complete!.entities.items!)).toHaveLength(600);
});


it('peer catch-up checks resident membership without enumerating resident IDs', async () => {
  await seed(); const replica = new LocalReplica(storage,{maxResidentRows:3}); await replica.activateScope('scope');
  const table = replica['entities'].get('items')!;
  const keys = vi.spyOn(table,'keys').mockImplementation(()=>{throw new Error('must not enumerate resident IDs');});
  const hot = [...table][0]![0];
  await storage.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'items',id:hot,operation:'update',newValue:{status:'done'}}]},{entities:{},liveQueries:{}},'scope');
  await replica.synchronizeStorage();
  expect(replica.entity('items',hot)!.status).toBe('done');
  expect(keys).not.toHaveBeenCalled(); keys.mockRestore();
  expect(replica.entityRows('items')).toHaveLength(3);
});

it('evicts least recently read rows while keeping retained rows pinned and metadata bounded', async()=>{
  const replica=new LocalReplica(storage,{maxResidentRows:3});
  await replica.materializeWindow({signature:'small',entity:'items',key:'id',rows:[{id:'a'},{id:'b'},{id:'c'}],completeness:'partial',source:'server'});
  replica.entity('items','a');
  const release=replica.retainRows('items',['b']);
  await replica.withReadView(()=>({}),false,async()=>undefined);
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'items',id:'d',operation:'insert',newValue:{id:'d'}}]});
  expect(replica.entity('items','c')).toBeUndefined();
  expect(replica.entity('items','a')).toBeDefined(); expect(replica.entity('items','b')).toBeDefined();
  release();
  for(let i=2;i<40;i++)await replica.applyTransaction({cursor:{epoch:'e',revision:i},changes:[{entity:'items',id:String(i),operation:'insert',newValue:{id:String(i)}}]});
  expect(replica['recency'].size).toBe(3);
  expect(replica['rowVersions'].size).toBeLessThanOrEqual(3);
  expect(replica['executionRowVersions'].size).toBeLessThanOrEqual(3);
});

it('loads UI-retained windows before alphabetical cold rows and transfers decoded ownership once',async()=>{
  await seed();
  const active={...window,signature:'active',ids:['row-19']};
  await storage.replaceWindow(active,{entities:{items:{'row-19':rows[19]!}},liveQueries:{active}},'scope',[rows[19]!]);
  const working=await storage.loadWorkingSet('scope',{maxRows:3,maxBytes:100000,preferredWindows:['active']});
  expect(working!.entities.items!['row-19']).toEqual(rows[19]);
  const load=vi.spyOn(storage,'loadWorkingSet').mockResolvedValue(working);
  const replica=new LocalReplica(storage,{maxResidentRows:3});await replica.activateScope('scope');
  expect(load).toHaveBeenCalledOnce();
  expect(replica.entity('items','row-19')).toBe(working!.entities.items!['row-19']);
  expect(Object.isFrozen(working!.entities.items!['row-19'])).toBe(true);
});

it('updates equality indexes after peer changes publish and does not index a failed staged write',async()=>{
  await seed();const replica=new LocalReplica(storage,{maxResidentRows:100});await replica.activateScope('scope');
  expect(replica.equalityRows('items','status','new')).toHaveLength(20);
  await storage.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'items',id:'row-00',operation:'update',newValue:{status:'done'}}]},{entities:{},liveQueries:{}},'scope');
  await replica.synchronizeStorage();
  expect(replica.equalityRows('items','status','done').map(row=>row._id)).toEqual(['row-00']);
  expect(replica.equalityRows('items','status','new')).toHaveLength(19);
  const fail=vi.spyOn(storage,'applyTransaction').mockRejectedValueOnce(new Error('disk'));
  await expect(replica.applyTransaction({cursor:{epoch:'e',revision:3},changes:[{entity:'items',id:'row-00',operation:'update',newValue:{status:'reverted'}}]})).rejects.toThrow('disk');
  expect(replica.equalityRows('items','status','done').map(row=>row._id)).toEqual(['row-00']);
  expect(replica.equalityRows('items','status','reverted')).toEqual([]);fail.mockRestore();
});

it('publishes local updates, inserts and deletes to indexes when catching up with a peer write', async () => {
  await seed();
  const replica = new LocalReplica(storage, {maxResidentRows:100});
  await replica.activateScope('scope');
  expect(replica.equalityRows('items','status','new')).toHaveLength(20);

  await storage.applyTransaction({cursor:{epoch:'e',revision:2},changes:[
    {entity:'items',id:'row-01',operation:'update',newValue:{status:'peer'}},
  ]},{entities:{},liveQueries:{}},'scope');
  await replica.applyTransaction({cursor:{epoch:'e',revision:3},changes:[
    {entity:'items',id:'row-00',operation:'update',newValue:{status:'local'}},
    {entity:'items',id:'row-new',operation:'insert',newValue:{_id:'row-new',status:'local'}},
    {entity:'items',id:'row-02',operation:'delete'},
  ]});

  expect(replica.equalityRows('items','status','local').map(row=>row._id)).toEqual(['row-00','row-new']);
  expect(replica.equalityRows('items','status','peer').map(row=>row._id)).toEqual(['row-01']);
  expect(replica.equalityRows('items','status','new')).toEqual(replica.entityRows('items').filter(row=>row.status==='new'));
  expect(replica['recency'].has('items\0row-new')).toBe(true);
  expect(replica['recency'].has('items\0row-02')).toBe(false);
});

it.each(['snapshots','arrays','indexes'] as const)('reclaims %s before evicting resident rows under byte pressure', async cache => {
  await seed();
  const replica = new LocalReplica(storage, {maxResidentRows:100});
  await replica.activateScope('scope');
  const budget = replica['residentBytes'] + replica['metadataBytes']();
  if (cache === 'snapshots') replica.liveQuerySnapshot('all');
  else if (cache === 'arrays') replica.entityRows('items');
  else replica.equalityRows('items','name','Name 0');
  expect(replica['residentBytes'] + replica['metadataBytes']()).toBeGreaterThan(budget);

  replica['residencyBudget'].maxBytes = budget;
  replica['trimResidentRows']();

  expect(replica['residentCount']).toBe(20);
  expect(replica['residentBytes'] + replica['metadataBytes']()).toBeLessThanOrEqual(budget);
  expect(replica.entityRows('items')).toEqual(rows);
});

it('evicts only the oldest unpinned row when row bytes still exceed the budget after cache reclamation', async () => {
  await seed();
  const replica = new LocalReplica(storage, {maxResidentRows:100});
  await replica.activateScope('scope');
  const release = replica.retainRows('items',['row-00']);
  await replica.withReadView(coverage,false,async()=>undefined);
  const budget = replica['residentBytes'] + replica['metadataBytes']() - 1;
  replica.liveQuerySnapshot('all');
  replica.entityRows('items');
  replica.equalityRows('items','name','Name 0');
  replica.entity('items','row-01');

  replica['residencyBudget'].maxBytes = budget;
  replica['trimResidentRows']();

  expect(replica['residentCount']).toBe(19);
  expect(replica.entity('items','row-02')).toBeUndefined();
  expect(replica.entity('items','row-00')).toEqual(rows[0]);
  expect(replica.entity('items','row-01')).toEqual(rows[1]);
  expect(replica['residentBytes'] + replica['metadataBytes']()).toBeLessThanOrEqual(budget);
  expect(Object.keys((await storage.load('scope'))!.entities.items!)).toHaveLength(20);
  release();
});

it('publishes monotonic peer-insert versions for a previously missing row and reclaims released missing-row subscriptions',async()=>{
  await seed();const replica=new LocalReplica(storage,{maxResidentRows:100});await replica.activateScope('scope');
  await replica.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'items',id:'row-00',operation:'update',newValue:{status:'updated'}}]});
  const missing=replica.entityVersion('items','new');
  await storage.applyTransaction({cursor:{epoch:'e',revision:3},changes:[{entity:'items',id:'new',operation:'insert',newValue:{_id:'new',status:'new'}}]},{entities:{},liveQueries:{}},'scope');
  await replica.synchronizeStorage();
  expect(replica.entityVersion('items','new')).toBeGreaterThan(missing);
  const release=replica.subscribe(()=>{}, {entity:'items',ids:['new']});
  await replica.applyTransaction({cursor:{epoch:'e',revision:4},changes:[{entity:'items',id:'new',operation:'delete'}]});
  expect(replica['rowVersions'].has('items\0new')).toBe(true);
  release();expect(replica['rowVersions'].has('items\0new')).toBe(false);
});

it('drops old recency entries on an epoch replacement of the same table without scanning unchanged writes',async()=>{
  const replica=new LocalReplica(storage,{maxResidentRows:100});
  await replica.materializeWindow({signature:'small',entity:'items',key:'id',rows:[{id:'a'},{id:'b'}],completeness:'partial',source:'server',cursor:{epoch:'old',revision:1}});
  await replica.applyTransaction({cursor:{epoch:'new',revision:1},changes:[{entity:'items',id:'z',operation:'insert',newValue:{id:'z'}}]});
  expect(replica['recency'].size).toBe(1);
  expect(replica['recency'].has('items\0a')).toBe(false);
  expect(replica['recency'].has('items\0z')).toBe(true);
  replica.dispose();expect(replica['recency'].size).toBe(0);
});


it('hydrates a large replica window with paged reads and exact ordered membership',async()=>{
  const many=Array.from({length:2050},(_,i)=>({_id:`large-${String(i).padStart(5,'0')}`,name:`Task ${i}`}));
  const large:ReplicaWindow={...window,signature:'large',ids:many.map(row=>row._id).reverse()};
  await storage.replaceWindow(large,{entities:{items:Object.fromEntries(many.map(row=>[row._id,row]))},liveQueries:{large}},'scope',many);
  await storage.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'items',id:'not-a-member',operation:'insert',newValue:{_id:'not-a-member',name:'Must not leak'}}]},{entities:{},liveQueries:{}},'scope');
  const perKey=vi.spyOn(storage,'loadEntityRows');
  const loaded=await storage.loadWindowRows('scope','large');
  expect(perKey).not.toHaveBeenCalled();
  expect(loaded?.rows).toEqual([...many].reverse());
  expect(loaded?.window.ids).toEqual(large.ids);
});
