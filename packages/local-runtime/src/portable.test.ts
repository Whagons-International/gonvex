import { describe, expect, it, vi } from 'vitest';
import { getRow, matchesDataPredicate, reducer, schema, selectRows } from '@gonvex/module-sdk';
import type { DataRead, JsonObject, ReducerContext } from '@gonvex/module-sdk';
import { orderDataRows, PortableReducerRuntime, type ReducerReadView } from './portable.js';
import type { LocalExecution } from './index.js';
import type { LocalSchema } from './schema.js';

const tables: LocalSchema = {
  tasks: { key: '_id', columns: { _id: {type:'text',nullable:false},statusId:{type:'text',nullable:false},count:{type:'bigint',nullable:false,default:'0'} } },
  logs: { key: '_id', columns: { _id: {type:'text',nullable:false},taskId:{type:'text',nullable:false},_creationTime:{type:'bigint',nullable:false} } },
};
const execution: LocalExecution = {scope:'scope',commandId:'command-1',now:1234,artifactHash:'artifact-1',identity:{auth:{account:{id:'account-1'}},tenant:{id:'tenant-1'},member:{id:'member-1',accountId:'account-1',permissions:{}}}};
const make = (run:(ctx:ReducerContext,args:any)=>Promise<any>) => new PortableReducerRuntime({schema:tables,artifactHash:'artifact-1',reducers:{edit:reducer({args:schema.any(),result:schema.any(),run})}});
function source(data: Record<string, JsonObject[]>): ReducerReadView {
  return { select:async read=>{
    const rows = (data[read.table]??[]).filter(row=>!read.where||matchesDataPredicate(row,read.where));
    orderDataRows(rows,read);
    return {rows:read.limit===undefined?rows:rows.slice(0,read.limit),complete:Object.hasOwn(data,read.table)};
  } };
}

describe('portable reducer executor',()=>{
  it('loads only the requested reducer, shares preload with execution, and retries failed downloads', async () => {
    const definition = reducer({ args: schema.any(), result: schema.any(), run: async () => 'done' });
    const used = vi.fn().mockRejectedValueOnce(new Error('offline')).mockResolvedValue(definition);
    const unused = vi.fn().mockResolvedValue(definition);
    const runtime = new PortableReducerRuntime({ schema: tables, artifactHash: 'artifact-1', reducers: { used, unused } });
    expect(used).not.toHaveBeenCalled();
    await expect(runtime.prepare('used')).rejects.toThrow('offline');
    await Promise.all([runtime.prepare('used'), runtime.prepare('used')]);
    expect((await runtime.execute('used', {}, source({}), execution)).result).toBe('done');
    expect(used).toHaveBeenCalledTimes(2);
    expect(unused).not.toHaveBeenCalled();
  });

  it('updates one record without requesting a collection snapshot',async()=>{
    const requested:DataRead[]=[];
    const result=await make(async ctx=>{
      const task=await getRow<JsonObject>(ctx.db,'tasks','_id','t1');
      await ctx.db.update('tasks','t1',{statusId:'progress',count:Number(task!.count)+1});
      return null;
    }).execute('edit',{}, {select:async read=>{requested.push(read);expect(read.where).toEqual({column:'_id',op:'eq',value:'t1'});expect(read.limit).toBe(1);return {rows:[{_id:'t1',statusId:'new',count:0}],complete:true};}},execution);
    expect(result.patches).toEqual([{entity:'tasks',rowId:'t1',op:'patch',fields:{statusId:'progress',count:1}}]);
    expect(requested).toHaveLength(2);
  });
  it('has read-your-writes, replenishes limited reads, and does not mutate source rows',async()=>{
    const data={tasks:[{_id:'a',statusId:'new',count:1},{_id:'b',statusId:'new',count:2},{_id:'c',statusId:'new',count:3}]};
    const result=await make(async ctx=>{
      await ctx.db.delete('tasks','a');
      await ctx.db.update('tasks','b',{count:10});
      return await selectRows(ctx.db,{table:'tasks',columns:['_id'],orderBy:[{column:'count'}],limit:1});
    }).execute('edit',{},source(data),execution);
    expect(result.result).toEqual([{_id:'c'}]);
    expect(data.tasks[0]).toMatchObject({_id:'a',count:1});
    expect(data.tasks[1]!.count).toBe(2);
  });
  it('rolls back all staged writes when the reducer fails',async()=>{
    const data={tasks:[{_id:'a',statusId:'new',count:0}]};
    await expect(make(async ctx=>{await ctx.db.update('tasks','a',{count:1});throw new Error('denied');}).execute('edit',{},source(data),execution)).rejects.toThrow('denied');
    expect(data.tasks[0]!.count).toBe(0);
  });
  it('preserves catchable validation errors without staging a partial write',async()=>{
    const result=await make(async ctx=>{
      try{await ctx.db.update('tasks','a',{count:4,statusId:null});}catch{}
      return getRow(ctx.db,'tasks','_id','a');
    }).execute('edit',{},source({tasks:[{_id:'a',statusId:'new',count:0}]}),execution);
    expect(result.patches).toEqual([]);
    expect(result.result).toMatchObject({count:0,statusId:'new'});
  });
  it('keeps deterministic insert, action and scheduler IDs across replay',async()=>{
    const runtime=make(async ctx=>({first:await ctx.db.insert('logs',{taskId:'a'}),second:await ctx.db.insert('logs',{taskId:'a'}),action:await ctx.actions.enqueue('notify',{}),job:await ctx.scheduler.runAfter(100,'notify')}));
    const first=await runtime.execute('edit',{},source({logs:[]}),execution);
    const second=await runtime.execute('edit',{},source({logs:[]}),execution);
    expect(first).toEqual(second);
    expect(first.result).toMatchObject({action:'aa14126b-2f16-814a-8623-07601307140c',job:'job_c9983b7c-273c-8f8e-91df-ef1b9ba899f6'});
    expect(first.patches[0]!.rowId).not.toBe(first.patches[1]!.rowId);
  });
  it('distinguishes missing coverage from absent records and rejects raw SQL',async()=>{
    await expect(make(ctx=>getRow(ctx.db,'tasks','_id','a')).execute('edit',{},source({}),execution)).rejects.toMatchObject({name:'IncompleteReplicaError'});
    const absent=await make(async ctx=>({missing:!(await getRow(ctx.db,'tasks','_id','a'))})).execute('edit',{},source({tasks:[]}),execution);
    expect(absent.result).toEqual({missing:true});
    await expect(make(ctx=>ctx.db.query('SELECT 1')).execute('edit',{},source({}),execution)).rejects.toMatchObject({name:'UnsupportedLocalOperationError'});
  });
});

it('projects only the fields a reducer reads and deletes without loading unrelated columns', async () => {
  const { memoryReadView } = await import('./read-view.js');
  const view = memoryReadView(new Map([['tasks',new Map([['t1',{_id:'t1',statusId:'new'}]])]]), {
    tasks:{key:'_id',complete:true,columns:['_id','statusId','count']},
  });
  const result = await make(async ctx => {
    const rows = await selectRows<JsonObject>(ctx.db,{table:'tasks',columns:['_id'],where:{column:'statusId',op:'eq',value:'new'}});
    for (const row of rows) await ctx.db.delete('tasks',String(row._id));
    return rows.length;
  }).execute('edit',{},view,execution);
  expect(result.result).toBe(1);
  expect(result.patches).toEqual([{entity:'tasks',rowId:'t1',op:'delete'}]);
});
