import {afterEach,beforeEach,expect,it} from 'vitest';
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
