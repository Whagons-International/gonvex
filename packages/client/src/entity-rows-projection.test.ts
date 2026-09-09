import { expect, it, vi } from 'vitest';
import { LocalReplica } from './local-replica';

it('projects ordered inserts, overwrites, deletes, resurrection, and rollback without exposing mutable data', async () => {
  const replica = new LocalReplica();
  await replica.replaceWindow({signature:'tasks',kind:'replica',entity:'tasks',key:'id',rows:[{id:'a',nested:{value:1}},{id:'b',value:2}],completeness:'complete',source:'server'});
  replica.applyOptimistic('one',[
    {entity:'tasks',rowId:'a',op:'patch',fields:{nested:{value:3}}},
    {entity:'tasks',rowId:'c',op:'insert',fields:{id:'c',value:4}},
    {entity:'other',rowId:'a',op:'patch',fields:{value:99}},
    {entity:'tasks',rowId:'b',op:'delete'},
  ]);
  replica.applyOptimistic('two',[
    {entity:'tasks',rowId:'b',op:'upsert',fields:{id:'b',value:5}},
    {entity:'tasks',rowId:'c',op:'delete'},
    {entity:'tasks',rowId:'d',op:'patch',fields:{id:'d',value:6}},
  ]);
  const projected = replica.entityRows('tasks');
  expect(projected).toEqual([{id:'a',nested:{value:3}},{id:'b',value:5},{id:'d',value:6}]);
  (projected[0].nested as {value:number}).value = 500;
  expect(replica.entityRows('tasks')[0].nested).toEqual({value:3});
  replica.rejectCommand('two');
  expect(replica.entityRows('tasks')).toEqual([{id:'a',nested:{value:3}},{id:'c',value:4}]);
  replica.rejectCommand('one');
  expect(replica.entityRows('tasks')).toEqual([{id:'a',nested:{value:1}},{id:'b',value:2}]);
});

it('matches individual row reads for mixed pending journals', async () => {
  const replica = new LocalReplica();
  const ids = Array.from({length:50}, (_,i)=>String(i));
  await replica.replaceWindow({signature:'tasks',kind:'replica',entity:'tasks',key:'id',rows:ids.map(id=>({id,value:0})),completeness:'complete',source:'server'});
  for(let i=0;i<120;i++) {
    const id=String((i*17)%75); if(!ids.includes(id))ids.push(id);
    const op = i%4;
    replica.applyOptimistic(String(i),[{entity:'tasks',rowId:id,...(op===0?{op:'delete' as const}:{op:op===1?'insert' as const:op===2?'upsert' as const:'patch' as const,fields:{id,value:i}})}]);
  }
  const expected = ids.map(id=>replica.entity('tasks',id)).filter(Boolean);
  const journalScan = vi.spyOn(replica['pendingCommands'], 'values');
  expect(replica.entityRows('tasks')).toEqual(expected);
  expect(journalScan).toHaveBeenCalledTimes(1);
  journalScan.mockRestore();
});

it('projects an ID batch in one journal pass while preserving duplicates and detached nested values', async () => {
  const replica = new LocalReplica();
  await replica.replaceWindow({signature:'tasks',kind:'replica',entity:'tasks',key:'id',rows:[{id:'a',value:1},{id:'b',value:2}],completeness:'complete',source:'server'});
  replica.applyOptimistic('one',[
    {entity:'tasks',rowId:'a',op:'patch',fields:{nested:{value:3}}},
    {entity:'tasks',rowId:'b',op:'delete'},
    {entity:'tasks',rowId:'c',op:'insert',fields:{id:'c',value:4}},
    {entity:'other',rowId:'a',op:'delete'},
  ]);
  const ids = ['c','a','missing','b','a'];
  const expected = ids.map(id=>replica.entity('tasks',id));
  const scan = vi.spyOn(replica['pendingCommands'],'values');
  const result = replica.entityBatch('tasks',ids);
  expect(result).toEqual(expected);
  expect(scan).toHaveBeenCalledTimes(1);
  (result[1]!.nested as {value:number}).value=500;
  expect(result[4]!.nested).toEqual({value:3});
  expect(replica.entity('tasks','a')!.nested).toEqual({value:3});
  scan.mockRestore();
});
