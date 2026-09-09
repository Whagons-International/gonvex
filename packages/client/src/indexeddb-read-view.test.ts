import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb';
import { Dexie } from 'dexie';
import { IndexedDBLocalReplicaStorage } from './indexeddb-replica.js';
import { overlayReadView } from '@gonvex/local-runtime/read-view';
import { reducerRowId } from '@gonvex/module-sdk';
import { entityRecord } from './indexeddb-read-view.js';
import { pagedReplicaReadView } from './paged-read-view.js';

let storage: IndexedDBLocalReplicaStorage;
const coverage = {tasks:{key:'_id',complete:true},assignments:{key:'_id',complete:true}};
beforeEach(()=>{
  Object.assign(globalThis,{indexedDB:new IDBFactory(),IDBKeyRange});
  Dexie.dependencies.indexedDB=globalThis.indexedDB;Dexie.dependencies.IDBKeyRange=IDBKeyRange;
  storage=new IndexedDBLocalReplicaStorage(`reads-${crypto.randomUUID()}`);
});
afterEach(()=>{storage.close();vi.restoreAllMocks();});

describe('IndexedDB reducer reads',()=>{
  it.each(['indexeddb', 'paged'] as const)('excludes superseded partial rows before validating %s reads', async adapter => {
    const rows = [{_id:'a',taskId:'task'}, {_id:'b',taskId:'task',deletedAt:null}];
    const known = {assignments:{key:'_id',complete:true,columns:['_id','taskId','deletedAt']}};
    await storage.replaceSnapshot({entities:{assignments:Object.fromEntries(rows.map(row=>[row._id,row]))},liveQueries:{}},'tenant');
    const read = async (base: Parameters<typeof overlayReadView>[0]) => {
      const view = overlayReadView(base,known,[{entity:'assignments',rowId:'a',op:'delete'}]);
      expect(await view.select({table:'assignments',where:{column:'deletedAt',op:'isNull'},orderBy:[{column:'_id'}],limit:1})).toEqual({rows:[rows[1]],complete:true});
      expect((await base.select({table:'assignments',where:{column:'deletedAt',op:'isNull'}})).complete).toBe(false);
    };
    if(adapter==='indexeddb') await storage.withReadView('tenant',known,read);
    else await read(pagedReplicaReadView(known,async (_request,after)=>after?[]:rows.map(row=>({id:row._id,row}))));
  });
  it('deserializes only the requested primary-key record',async()=>{
    const tasks=Object.fromEntries(Array.from({length:2000},(_,i)=>[`t${i}`,{_id:`t${i}`,name:`Task ${i}`,count:i}]));
    await storage.replaceSnapshot({entities:{tasks},liveQueries:{}},'tenant');
    const parse=vi.spyOn(JSON,'parse');
    const result=await storage.withReadView('tenant',coverage,view=>view.select({table:'tasks',where:{column:'_id',op:'eq',value:'t123'},limit:1}));
    expect(result).toEqual({rows:[tasks.t123],complete:true});
    expect(parse.mock.calls.filter(([value])=>typeof value==='string'&&value.includes('"name":"Task '))).toHaveLength(1);
  });
  it('uses secondary indexes for a parent lookup and excludes other scopes',async()=>{
    const assignments=Object.fromEntries(Array.from({length:1000},(_,i)=>[`a${i}`,{_id:`a${i}`,taskId:`t${i%100}`,active:i%2===0}]));
    await storage.replaceSnapshot({entities:{assignments},liveQueries:{}},'tenant');
    await storage.replaceSnapshot({entities:{assignments:{secret:{_id:'secret',taskId:'t42',active:true}}},liveQueries:{}},'other');
    const parse=vi.spyOn(JSON,'parse');
    const result=await storage.withReadView('tenant',coverage,view=>view.select({table:'assignments',where:{and:[{column:'taskId',op:'eq',value:'t42'},{column:'active',op:'eq',value:true}]}}));
    expect(result.rows).toHaveLength(10);
    expect(result.rows.every(row=>row.taskId==='t42'&&row._id!=='secret')).toBe(true);
    expect(parse.mock.calls.filter(([value])=>typeof value==='string'&&value.includes('"taskId"'))).toHaveLength(10);
  });
  it('updates and deletes secondary keys with the authoritative row',async()=>{
    await storage.replaceSnapshot({entities:{tasks:{a:{_id:'a',status:'new'}}},liveQueries:{}},'tenant');
    const read=(status:string)=>storage.withReadView('tenant',coverage,view=>view.select({table:'tasks',where:{column:'status',op:'eq',value:status}}));
    expect((await read('new')).rows).toHaveLength(1);
    await storage.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{_id:'a',status:'progress'}}]},{entities:{},liveQueries:{}},'tenant');
    expect((await read('new')).rows).toEqual([]);
    expect((await read('progress')).rows).toHaveLength(1);
    await storage.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'delete'}]},{entities:{},liveQueries:{}},'tenant');
    expect((await read('progress')).rows).toEqual([]);
  });
  it('returns bounded sorted results, including null ordering',async()=>{
    const tasks={a:{_id:'a',rank:null},b:{_id:'b',rank:3},c:{_id:'c',rank:1},d:{_id:'d',rank:2}};
    await storage.replaceSnapshot({entities:{tasks},liveQueries:{}},'tenant');
    const result=await storage.withReadView('tenant',coverage,view=>view.select({table:'tasks',orderBy:[{column:'rank',nulls:'last'}],limit:2}));
    expect(result.rows.map(row=>row._id)).toEqual(['c','d']);
  });
  it('reports incomplete negative reads instead of inventing absence',async()=>{
    await storage.replaceSnapshot({entities:{tasks:{a:{_id:'a',status:'new'}}},liveQueries:{}},'tenant');
    const partial={tasks:{key:'_id',complete:false}};
    const read=(id:string)=>storage.withReadView('tenant',partial,view=>view.select({table:'tasks',where:{column:'_id',op:'eq',value:id},limit:1}));
    expect((await read('a')).complete).toBe(true);
    expect((await read('missing')).complete).toBe(false);
  });
  it('does not confuse a missing projected field with SQL NULL',async()=>{
    await storage.replaceSnapshot({entities:{tasks:{a:{_id:'a',status:'new'}}},liveQueries:{}},'tenant');
    const result=await storage.withReadView('tenant',{tasks:{key:'_id',complete:true,columns:['_id','status','approvalId']}},view=>view.select({table:'tasks',where:{column:'_id',op:'eq',value:'a'},limit:1}));
    expect(result.complete).toBe(false);
  });
  it('proves a negative predicate for a known primary key in a partial collection',async()=>{
    await storage.replaceSnapshot({entities:{tasks:{a:{_id:'a',status:'new'}}},liveQueries:{}},'tenant');
    const result=await storage.withReadView('tenant',{tasks:{key:'_id',complete:false}},view=>view.select({table:'tasks',where:{and:[{column:'_id',op:'eq',value:'a'},{column:'status',op:'eq',value:'done'}]},limit:1}));
    expect(result).toEqual({rows:[],complete:true});
  });
  it('backfills indexes from an existing version-three database',async()=>{
    storage.close();
    const name=`upgrade-${crypto.randomUUID()}`;const legacy=new Dexie(name);
    legacy.version(3).stores({entities:'[scope+entity+id], scope, [scope+entity]',windows:'[scope+signature], scope',meta:'[scope+key], scope',snapshots:'&scope'});
    await legacy.table('entities').put({scope:'tenant',entity:'tasks',id:'a',value:JSON.stringify({_id:'a',status:'new'})});
    legacy.close();storage=new IndexedDBLocalReplicaStorage(name);
    const result=await storage.withReadView('tenant',coverage,view=>view.select({table:'tasks',where:{column:'status',op:'eq',value:'new'}}));
    expect(result.rows).toEqual([{_id:'a',status:'new'}]);
  });
});

it.each(['indexeddb','paged'] as const)('uses filtered coverage for projected %s reads without requiring unrelated columns', async adapter => {
  const rows=[{_id:'a',taskId:'one',deletedAt:null}];
  const known={assignments:{key:'_id',complete:false,columns:['_id','taskId','deletedAt','note'],completeWhere:[{column:'deletedAt',op:'isNull' as const}]}};
  await storage.replaceSnapshot({entities:{assignments:{a:rows[0]}},liveQueries:{}},'tenant');
  const run=async (view:Parameters<typeof overlayReadView>[0])=>{
    const scoped={table:'assignments',columns:['_id'],where:{and:[{column:'taskId',op:'eq' as const,value:'one'},{column:'deletedAt',op:'isNull' as const}]}};
    expect(await view.select(scoped)).toEqual({rows:[{_id:'a'}],complete:true});
    expect((await view.select({...scoped,where:{column:'taskId',op:'eq',value:'one'}})).complete).toBe(false);
    expect((await view.select({...scoped,columns:['note']})).complete).toBe(false);
  };
  if(adapter==='indexeddb') await storage.withReadView('tenant',known,run);
  else await run(pagedReplicaReadView(known,async (_read,after)=>after?[]:rows.map(row=>({id:row._id,row}))));
});

it.each(['indexeddb', 'paged'] as const)('resolves complete primary-key batches from a partial %s collection', async adapter => {
  const tasks = { a: { _id: 'a', status: 'new' }, b: { _id: 'b', status: 'done' } };
  await storage.replaceSnapshot({ entities: { tasks }, liveQueries: {} }, 'tenant');
  const partial = { tasks: { key: '_id', complete: false } };
  const verify = async (view: any) => {
    const read = { table: 'tasks', columns: ['_id', 'status'], where: { column: '_id', op: 'in', values: ['a', 'b'] } };
    expect(await view.select(read)).toEqual({ rows: Object.values(tasks), complete: true });
    expect((await view.select({ ...read, where: { ...read.where, values: ['a', 'missing'] } })).complete).toBe(false);
    expect((await view.select({ ...read, columns: ['_id', 'privateNote'] })).complete).toBe(false);
  };
  if (adapter === 'indexeddb') await storage.withReadView('tenant', partial, verify);
  else await verify(pagedReplicaReadView(partial, async (_read, after) => after ? [] : Object.entries(tasks).map(([id, row]) => ({ id, row }))));
});


it('omits unused NULL lookup keys while keeping nullable rows and indexed scalar values', async () => {
  const row = { _id: 't', description: null, deletedAt: null, statusId: 'new', active: false };
  const record = entityRecord('tenant', 'tasks', 't', row);
  expect(record.lookupKeys).toHaveLength(3);
  expect(JSON.parse(record.value)).toEqual(row);
  await storage.replaceSnapshot({ entities: { tasks: { t: row } }, liveQueries: {} }, 'tenant');
  expect(await storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'deletedAt', op: 'isNull' } }))).toEqual({ rows: [row], complete: true });
});


it('keeps crypto alive between indexed reads without wrapping database work in waitFor', async () => {
  await storage.replaceSnapshot({ entities: { tasks: { a: { _id: 'a', status: 'new' } } }, liveQueries: {} }, 'tenant');
  const original = Dexie.waitFor.bind(Dexie);
  let keepingAlive = false;
  const wait = vi.spyOn(Dexie, 'waitFor').mockImplementation(((promise: Promise<unknown>) => {
    keepingAlive = true;
    return original(promise).finally(() => { keepingAlive = false; });
  }) as typeof Dexie.waitFor);
  await storage.withReadView('tenant', coverage, async base => {
    const view = overlayReadView(base, coverage, []);
    const first = await view.select({ table: 'tasks', where: { column: '_id', op: 'eq', value: 'a' } });
    expect(keepingAlive).toBe(false);
    const id = await reducerRowId({ tenant: { id: 'tenant' }, auth: { account: { id: 'account' } }, invocation: { commandId: 'intent' }, db: { keepAliveFor: view.keepAliveFor } } as any, 'assignments', 0);
    expect(id).toMatch(/^[0-9a-f-]{36}$/);
    expect(keepingAlive).toBe(false);
    expect(await view.select({ table: 'tasks', where: { column: '_id', op: 'eq', value: 'a' } })).toEqual(first);
  });
  expect(wait).toHaveBeenCalledTimes(1);
});
