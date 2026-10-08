import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { IDBFactory, IDBKeyRange, IDBObjectStore } from 'fake-indexeddb';
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
    await storage.withReadView('tenant',coverage,view=>view.select({table:'assignments',where:{column:'taskId',op:'eq',value:'t42'}}));
    const parse=vi.spyOn(JSON,'parse');
    const result=await storage.withReadView('tenant',coverage,view=>view.select({table:'assignments',where:{and:[{column:'taskId',op:'eq',value:'t42'},{column:'active',op:'eq',value:true}]}}));
    expect(result.rows).toHaveLength(10);
    expect(result.rows.every(row=>row.taskId==='t42'&&row._id!=='secret')).toBe(true);
    expect(parse.mock.calls.filter(([value])=>typeof value==='string'&&value.includes('"taskId":'))).toHaveLength(10);
  }, 60000);
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
  it('reads existing version-three records after removing blanket indexes',async()=>{
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
  const record = entityRecord('tenant', 'tasks', 't', row, ['statusId', 'active', 'deletedAt']);
  expect(record.lookupKeys).toHaveLength(2);
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

it('scans an unindexed equality within its entity/scope, then learns only the chosen column', async () => {
  const tasks = { a: { _id: 'a', status: 'new', unused: 'x' }, b: { _id: 'b', status: 'done', unused: 'x' } };
  await storage.replaceSnapshot({ entities: { tasks, assignments: { a: tasks.a } }, liveQueries: {} }, 'tenant');
  await storage.replaceSnapshot({ entities: { tasks: { secret: { _id: 'secret', status: 'new' } } }, liveQueries: {} }, 'other');
  const db = (storage as any).database as Dexie;
  expect((await db.table('entities').get(['tenant', 'tasks', 'a'])).lookupKeys).toEqual([]);
  const read = () => storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { and: [{ column: 'status', op: 'eq', value: 'new' }, { column: 'unused', op: 'eq', value: 'x' }] } }));
  expect(await read()).toEqual({ rows: [tasks.a], complete: true });
  expect((await db.table('entities').get(['tenant', 'tasks', 'a'])).lookupKeys).toEqual([['tenant', 'tasks', 'status', 3, 'new']]);
  expect((await db.table('entities').get(['tenant', 'assignments', 'a'])).lookupKeys).toEqual([]);
  expect((await db.table('entities').get(['other', 'tasks', 'secret'])).lookupKeys).toEqual([]);
  expect(await read()).toEqual({ rows: [tasks.a], complete: true });
  expect((await storage.withReadView('tenant', { tasks: { key: '_id', complete: false } }, view => view.select({ table: 'tasks', where: { column: 'unseen', op: 'eq', value: 'missing' } }))).complete).toBe(false);
});

it('shares learned policy across reopen and peer writes, including newly learned IN columns', async () => {
  const name = `learned-${crypto.randomUUID()}`;
  storage.close(); storage = new IndexedDBLocalReplicaStorage(name);
  const peer = new IndexedDBLocalReplicaStorage(name);
  try {
    await storage.replaceSnapshot({ entities: { tasks: { a: { _id: 'a', status: 'new', parent: 'p' } } }, liveQueries: {} }, 'tenant');
    const read = () => storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'status', op: 'in', values: ['new', 'done'] } }));
    await read();
    await peer.applyTransaction({ cursor: { epoch: 'e', revision: 1 }, changes: [{ entity: 'tasks', id: 'b', operation: 'insert', newValue: { _id: 'b', status: 'done', parent: 'p' } }] }, { entities: {}, liveQueries: {} }, 'tenant');
    storage.close(); storage = new IndexedDBLocalReplicaStorage(name);
    expect((await read()).rows.map(row => row._id)).toEqual(['a', 'b']);
    await storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'parent', op: 'eq', value: 'p' } }));
    await peer.applyTransaction({ cursor: { epoch: 'e', revision: 2 }, changes: [{ entity: 'tasks', id: 'b', operation: 'update', newValue: { _id: 'b', status: 'done', parent: 'q' } }] }, { entities: {}, liveQueries: {} }, 'tenant');
    expect((await storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'parent', op: 'eq', value: 'q' } }))).rows.map(row => row._id)).toEqual(['b']);
  } finally { peer.close(); }
});

it('upgrades version-five records in place without changing authority, tombstones or durable metadata', async () => {
  const name = `v5-${crypto.randomUUID()}`;
  const legacy = new Dexie(name);
  legacy.version(5).stores({ entities: '[scope+entity+id], scope, [scope+entity], *lookupKeys, [scope+sequence]', windows: '[scope+signature], scope, [scope+sequence]', meta: '[scope+key], scope', snapshots: '&scope' });
  const outbox = new Dexie(`${name}-outbox`);
  outbox.version(2).stores({ entries: '++id, scope, state' });
  const intent = { id: 1, scope: 'tenant', state: 'queued', args: { task: 'a' } };
  await outbox.table('entries').put(intent);
  const journal = new Dexie(`${name}-upgrades`);
  journal.version(1).stores({ state: '&key' });
  const staged = { key: 'contract', version: 2, journal: { snapshots: { tenant: { entities: {}, liveQueries: {} } }, entries: [intent] } };
  await journal.table('state').put(staged);
  const row = { _id: 'a', status: 'new' };
  const record = { ...entityRecord('tenant', 'tasks', 'a', row, Object.keys(row)), sequence: 7, authority: { epoch: 'e', fields: { status: 4 } } };
  const tombstone = { scope: 'tenant', entity: 'tasks', id: 'gone', value: 'null', deleted: true, sequence: 8, authority: { epoch: 'e', fields: {}, deleted: 8 }, lookupKeys: [] };
  await legacy.table('entities').bulkPut([record, tombstone]);
  const window = { scope: 'tenant', signature: 'tasks', value: JSON.stringify({ signature: 'tasks', entity: 'tasks', key: '_id', ids: ['a'], kind: 'replica', completeness: 'complete', source: 'server' }), sequence: 7 };
  await legacy.table('windows').put(window);
  const metadata = ['cursor', 'session', 'sequence', 'resetSequence', 'epoch'].map(key => ({ scope: 'tenant', key, value: key === 'sequence' ? '8' : JSON.stringify({ preserved: key }) }));
  await legacy.table('meta').bulkPut(metadata);
  legacy.close(); storage.close(); storage = new IndexedDBLocalReplicaStorage(name);
  await storage.listScopes();
  const db = (storage as any).database as Dexie;
  expect(await db.table('entities').get(['tenant', 'tasks', 'a'])).toEqual({ ...record, lookupKeys: [] });
  expect(await db.table('entities').get(['tenant', 'tasks', 'gone'])).toEqual(tombstone);
  expect(await db.table('windows').get(['tenant', 'tasks'])).toEqual(window);
  expect(await db.table('meta').toArray()).toEqual(expect.arrayContaining(metadata));
  expect(await storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'status', op: 'eq', value: 'new' } }))).toEqual({ rows: [row], complete: true });
  outbox.close(); journal.close();
  await Promise.all([outbox.open(), journal.open()]);
  expect(await outbox.table('entries').toArray()).toEqual([intent]);
  expect(await journal.table('state').get('contract')).toEqual(staged);
  outbox.close(); journal.close();
});

it('prefers a learned conjunct and avoids learning columns for primary-key and zero-limit reads', async () => {
  await storage.replaceSnapshot({ entities: { tasks: { a: { _id: 'a', status: 'new', note: 'x' } } }, liveQueries: {} }, 'tenant');
  await storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'status', op: 'eq', value: 'new' } }));
  await storage.withReadView('tenant', coverage, async view => {
    expect((await view.select({ table: 'tasks', where: { and: [{ column: 'note', op: 'eq', value: 'x' }, { column: 'status', op: 'eq', value: 'new' }] } })).rows).toHaveLength(1);
    expect((await view.select({ table: 'tasks', where: { and: [{ column: 'note', op: 'eq', value: 'x' }, { column: '_id', op: 'eq', value: 'a' }] } })).rows).toHaveLength(1);
    expect(await view.select({ table: 'tasks', where: { column: 'note', op: 'eq', value: 'x' }, limit: 0 })).toEqual({ rows: [], complete: true });
  });
  expect((await (storage as any).database.table('meta').get(['tenant', 'lookupColumns:tasks'])).value).toBe('["status"]');
});

it('rolls back learned keys and policy together when backfill fails', async () => {
  await storage.replaceSnapshot({ entities: { tasks: { a: { _id: 'a', status: 'new' } } }, liveQueries: {} }, 'tenant');
  const original = (storage as any).writeRecords.bind(storage);
  const write = vi.spyOn(storage as any, 'writeRecords').mockImplementationOnce(async records => {
    await original(records);
    throw new Error('storage failure');
  });
  const read = () => storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'status', op: 'eq', value: 'new' } }));
  expect((await read()).rows).toEqual([{ _id: 'a', status: 'new' }]);
  const db = (storage as any).database as Dexie;
  expect(await db.table('meta').get(['tenant', 'lookupColumns:tasks'])).toBeUndefined();
  expect((await db.table('entities').get(['tenant', 'tasks', 'a'])).lookupKeys).toEqual([]);
  write.mockRestore();
  expect(await read()).toEqual({ rows: [{ _id: 'a', status: 'new' }], complete: true });
});

it('merges concurrent column demands without losing either peer policy', async () => {
  const name = `concurrent-${crypto.randomUUID()}`;
  storage.close(); storage = new IndexedDBLocalReplicaStorage(name);
  const peer = new IndexedDBLocalReplicaStorage(name);
  try {
    await storage.replaceSnapshot({ entities: { tasks: { a: { _id: 'a', status: 'new', parent: 'p' } } } , liveQueries: {} }, 'tenant');
    await Promise.all([
      storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'status', op: 'eq', value: 'new' } })),
      peer.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'parent', op: 'eq', value: 'p' } })),
    ]);
    const record = await (storage as any).database.table('entities').get(['tenant', 'tasks', 'a']);
    expect(record.lookupKeys).toHaveLength(2);
    expect(record.lookupKeys.map((key: any[]) => key[2]).sort()).toEqual(['parent', 'status']);
  } finally { peer.close(); }
});

it('learns demands even when an incomplete reducer throws before hydration', async () => {
  const unavailable = new Error('IncompleteReplicaError');
  await expect(storage.withReadView('tenant', {}, async view => {
    expect((await view.select({ table: 'tasks', where: { column: 'parent', op: 'eq', value: 'p' } })).complete).toBe(false);
    throw unavailable;
  })).rejects.toBe(unavailable);
  expect(await storage.listScopes()).toEqual([]);
  await storage.replaceSnapshot({ entities: { tasks: { a: { _id: 'a', parent: 'p', unused: 'x' } } }, liveQueries: {} }, 'tenant');
  expect((await (storage as any).database.table('entities').get(['tenant', 'tasks', 'a'])).lookupKeys).toEqual([['tenant', 'tasks', 'parent', 3, 'p']]);
});

it('reads column policy once per entity/write transaction and never for a primary-key read', async () => {
  await storage.listScopes();
  const get = vi.spyOn(IDBObjectStore.prototype, 'get');
  const rows = Object.fromEntries(Array.from({ length: 125 }, (_, i) => [`t${i}`, { _id: `t${i}`, status: 'new' }]));
  await storage.replaceSnapshot({ entities: { tasks: rows }, liveQueries: {} }, 'tenant');
  expect(get.mock.calls.filter(([key]) => Array.isArray(key) && key[1] === 'lookupColumns:tasks')).toHaveLength(1);
  get.mockClear();
  await storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: '_id', op: 'eq', value: 't42' } }));
  expect(get.mock.calls.filter(([key]) => Array.isArray(key) && key[1] === 'lookupColumns:tasks')).toHaveLength(0);
});

it('keeps long-string equality and OR predicates on scoped scans without learning unusable indexes', async () => {
  const note = 'x'.repeat(257);
  await storage.replaceSnapshot({ entities: { tasks: { a: { _id: 'a', status: 'new', note }, b: { _id: 'b', status: 'done', note: 'short' } } }, liveQueries: {} }, 'tenant');
  await storage.withReadView('tenant', coverage, view => view.select({ table: 'tasks', where: { column: 'status', op: 'eq', value: 'new' } }));
  await storage.withReadView('tenant', coverage, async view => {
    expect((await view.select({ table: 'tasks', where: { column: 'note', op: 'eq', value: note } })).rows.map(row => row._id)).toEqual(['a']);
    expect((await view.select({ table: 'tasks', where: { or: [{ column: 'status', op: 'eq', value: 'new' }, { column: 'note', op: 'eq', value: 'short' }] } })).rows.map(row => row._id)).toEqual(['a', 'b']);
  });
  expect((await (storage as any).database.table('meta').get(['tenant', 'lookupColumns:tasks'])).value).toBe('["status"]');
});
