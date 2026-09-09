import {afterAll,expect,it} from 'vitest';
import {reducer,schema,insertDataRows,updateDataRows,deleteDataRows,selectRows,lockData,applyDataWrites,summarizeRows,updateDataWhere,deleteDataWhere,existsDataRows,selectDataBatch} from '@gonvex/module-sdk';
import {LocalReducerRuntime, type LocalSnapshot} from '../test/postgres-reference.js';
import {createPortableReducer} from './portable-client.js';

const definition=reducer({args:schema.object({}),result:schema.any(),run:async ctx=>{
  await lockData(ctx.db,'inventory');
  const inserted=await insertDataRows(ctx,'items',[{name:'first',count:1},{name:'second',count:2,note:null},{name:'third',count:3}]);
  await updateDataRows(ctx.db,'items',[{id:String(inserted[0]!._id),fields:{count:7,note:'changed'}},{id:String(inserted[1]!._id),fields:{count:8}}]);
  await deleteDataRows(ctx.db,'items',[String(inserted[2]!._id)]);
  return selectRows(ctx.db,{table:'items',orderBy:[{column:'name'}]});
}});
const fanout=reducer({args:schema.object({}),result:schema.any(),run:async ctx=>{
  await applyDataWrites(ctx,[
    {kind:'insert',table:'items',rows:[{_id:'new',name:'created',count:1}]},
    {kind:'update',table:'items',rows:[{id:'existing',fields:{count:4}}]},
    {kind:'deleteWhere',table:'links',read:{where:{column:'name',op:'eq',value:'remove'}}},
  ]);
  return {items:await selectRows(ctx.db,{table:'items',orderBy:[{column:'_id'}]}),links:await selectRows(ctx.db,{table:'links',orderBy:[{column:'_id'}]})};
}});
const failedBatch=reducer({args:schema.object({}),result:schema.any(),run:async ctx=>{
  try {await applyDataWrites(ctx,[{kind:'update',table:'items',rows:[{id:'existing',fields:{count:99}}]},{kind:'insert',table:'items',rows:[{_id:'invalid',name:null,count:1}]}]);} catch {}
  return selectRows(ctx.db,{table:'items',orderBy:[{column:'_id'}]});
}});
const mixedInserts=reducer({args:schema.object({}),result:schema.any(),run:async ctx=>{
  const before=await ctx.db.insert('items',{name:'before',count:1});
  const bulk=await insertDataRows(ctx,'items',[{_id:'explicit',name:'explicit',count:1},{name:'generated',count:2}]);
  const after=await ctx.db.insert('items',{name:'after',count:3});
  return {before,bulk,after};
}});
const metrics=reducer({args:schema.object({}),result:schema.any(),run:async ctx=>({
  totals:await summarizeRows(ctx.db,{table:'items',where:{column:'name',transform:'lowerTrim',op:'eq',value:'match'}},{count:{op:'count'},sum:{op:'sum',column:'count'},positive:{op:'count',where:{column:'count',op:'gt',value:0}}}),
  ordered:await selectRows(ctx.db,{table:'items',orderBy:[{column:'count'}],limit:3}),
})});
const sparseUpdates=reducer({args:schema.object({}),result:schema.any(),run:async ctx=>{
  await updateDataRows(ctx.db,'items',[{id:'absent',fields:{count:100}},{id:'a',fields:{count:4}}]);
  const updated=await updateDataWhere(ctx.db,{table:'items',where:{column:'count',op:'gt',value:0}},{note:'updated'});
  const deleted=await deleteDataWhere(ctx.db,{table:'items',where:{column:'count',op:'lt',value:0}});
  return {updated,deleted,rows:await selectRows(ctx.db,{table:'items'})};
}});
const localSchema={items:{key:'_id',columns:{_id:{type:'text',nullable:false},name:{type:'text',nullable:false},count:{type:'integer',nullable:false,default:'0'},note:{type:'text',nullable:true}}}};
const existence=reducer({args:schema.object({}),result:schema.any(),run:ctx=>existsDataRows(ctx.db,Array.from({length:300},(_,i)=>({table:'items',where:{and:[{column:'name',op:'eq',value:`name-${i}`},{column:'note',op:'isNull'}]}})))});
const batched=reducer({args:schema.object({}),result:schema.any(),run:ctx=>selectDataBatch(ctx.db,Array.from({length:35},(_,i)=>({table:"items",columns:["name","count"],where:{column:"count",op:"gt",value:i},orderBy:[{column:"count",direction:"desc"}],limit:2})))});
const numericSuffix=reducer({args:schema.object({}),result:schema.any(),run:ctx=>selectRows(ctx.db,{table:'items',columns:['name'],orderBy:[{column:'name',transform:'numericSuffix',direction:'desc',nulls:'last'}],limit:3})});
const options={schema:{...localSchema,links:localSchema.items},reducers:{edit:definition,fanout,failedBatch,mixedInserts,metrics,sparseUpdates,existence,batched,numericSuffix},artifactHash:'test'};
const postgres=new LocalReducerRuntime(options),portable=createPortableReducer(options);
afterAll(async()=>{portable.close();await postgres.close();});
const execution={scope:'test',commandId:'intent',now:1,artifactHash:'test',identity:{auth:{account:{id:'person'}},tenant:{id:'tenant'},member:{id:'member',accountId:'person',permissions:{}}}};
it('sorts document counters numerically across digit boundaries and padded imports',async()=>{
  const rows=['PR-999','PR-1000','PR-0001001','missing'].map((name,i)=>({_id:String(i),name,count:0,note:null}));
  const snapshot:LocalSnapshot={scope:'test',tables:{items:{complete:true,rows}}};
  const local=await portable.execute('numericSuffix',{},snapshot,execution);
  expect(local.result).toEqual((await postgres.execute('numericSuffix',{},snapshot,execution)).result);
  expect(local.result).toEqual([{name:'PR-0001001'},{name:'PR-1000'},{name:'PR-999'}]);
},20000);
it('matches bounded existence batches across PostgreSQL and local reads',async()=>{
  const snapshot:LocalSnapshot={scope:'test',tables:{items:{complete:true,rows:[{_id:'a',name:'name-2',count:1,note:null},{_id:'b',name:'name-267',count:1,note:null},{_id:'c',name:'name-268',count:1,note:'excluded'}]}}};
  const local=await portable.execute('existence',{},snapshot,execution);
  expect(local.result).toEqual((await postgres.execute('existence',{},snapshot,execution)).result);
  expect(local.result).toEqual(Array.from({length:300},(_,i)=>i===2||i===267));
},20000);
it('runs the same typed bulk writes with PostgreSQL and the portable engine',async()=>{
  const snapshot:LocalSnapshot={scope:'test',tables:{items:{complete:true,rows:[]}}};
  const actual=await portable.execute('edit',{},snapshot,execution);
  const authoritative=await postgres.execute('edit',{},snapshot,execution);
  expect(actual.result).toEqual(authoritative.result);
  expect((actual.result as any[]).map(row=>[row.name,row.count,row.note])).toEqual([['first',7,'changed'],['second',8,null]]);
  expect(await portable.execute('edit',{},snapshot,execution)).toEqual(actual);
  expect(snapshot.tables.items!.rows).toEqual([]);
},20000);
it('matches missing-row bulk updates and returns affected counts without transferring every ID',async()=>{
  const input:LocalSnapshot={scope:'test',tables:{items:{complete:true,rows:[{_id:'a',name:'A',count:1,note:null},{_id:'b',name:'B',count:-1,note:null}]}}};
  const actual=await portable.execute('sparseUpdates',{},input,execution);
  expect(actual.result).toEqual((await postgres.execute('sparseUpdates',{},input,execution)).result);
  expect(actual.result).toEqual({updated:1,deleted:1,rows:[{_id:'a',name:'A',count:4,note:'updated'}]});
});
it('matches PostgreSQL for multi-table writes and predicate deletion',async()=>{
  const snapshot:LocalSnapshot={scope:'test',tables:{items:{complete:true,rows:[{_id:'existing',name:'keep',count:0,note:null}]},links:{complete:true,rows:[{_id:'gone',name:'remove',count:1,note:null},{_id:'kept',name:'keep',count:1,note:null}]}}};
  const actual=await portable.execute('fanout',{},snapshot,execution);
  expect(actual.result).toEqual((await postgres.execute('fanout',{},snapshot,execution)).result);
  expect((actual.result as any).links.map((row:any)=>row._id)).toEqual(['kept']);
});
it('does not publish partial writes when a batch failure is caught by the reducer',async()=>{
  const row={_id:'existing',name:'keep',count:0,note:null};
  const snapshot:LocalSnapshot={scope:'test',tables:{items:{complete:true,rows:[row]}}};
  const actual=await portable.execute('failedBatch',{},snapshot,execution);
  expect(actual.result).toEqual([row]);
  expect(actual.patches).toEqual([]);
});
it('keeps the same IDs when mixing explicit, generated, batch, and individual inserts',async()=>{
  const snapshot:LocalSnapshot={scope:'test',tables:{items:{complete:true,rows:[]}}};
  const local=await portable.execute('mixedInserts',{},snapshot,execution);
  expect(local.result).toEqual((await postgres.execute('mixedInserts',{},snapshot,execution)).result);
});
it('inserts intent-owned batch IDs without requiring the entire history table',async()=>{
  const partial:LocalSnapshot={scope:'test',tables:{items:{complete:false,rows:[]}}};
  const add=reducer({args:schema.object({}),result:schema.any(),run:ctx=>insertDataRows(ctx,'items',[{name:'new',count:1}])});
  const host=createPortableReducer({...options,reducers:{add}});
  expect((await host.execute('add',{},partial,execution)).patches).toHaveLength(1);
  host.close();
});
it('matches filtered aggregates and deterministic ties despite different row arrival order',async()=>{
  const rows=[{_id:'z',name:'other',count:8,note:null},{_id:'b',name:' Match ',count:3,note:null},{_id:'a',name:'MATCH',count:3,note:null},{_id:'c',name:'match',count:-1,note:null}];
  const local=await portable.execute('metrics',{}, {scope:'test',tables:{items:{complete:true,rows}}},execution);
  const server=await postgres.execute('metrics',{}, {scope:'test',tables:{items:{complete:true,rows:[...rows].reverse()}}},execution);
  expect(local.result).toEqual(server.result);
  expect((local.result as any).totals).toEqual({count:3,sum:5,positive:2});
  expect((local.result as any).ordered.map((row:any)=>row._id)).toEqual(['c','a','b']);
});

it('matches independent batched projections, parameters, ordering and empty reads',async()=>{
 const snapshot:LocalSnapshot={scope:'test',tables:{items:{complete:true,rows:[{_id:'a',name:'A',count:1,note:null},{_id:'b',name:'B',count:33,note:null},{_id:'c',name:'C',count:34,note:null}]}}};
 const local=await portable.execute('batched',{},snapshot,execution);
 expect(local.result).toEqual((await postgres.execute('batched',{},snapshot,execution)).result);
 expect((local.result as any[])[0]).toEqual([{name:'C',count:34},{name:'B',count:33}]);
 expect((local.result as any[])[34]).toEqual([]);
});
