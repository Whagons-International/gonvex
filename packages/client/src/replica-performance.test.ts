import { expect, it, vi } from 'vitest';
import { LocalReplica } from './local-replica';

const materialize = (replica: LocalReplica, entity = 'tasks', signature = entity, rows = [{id:'a',value:1},{id:'b',value:2}]) => replica.materializeWindow({signature,entity,key:'id',rows,completeness:'complete',source:'server'});

it('shares deeply frozen owned JSON through single, batch, table and watch reads', async () => {
  const replica = new LocalReplica();
  const input = {id:'a',nested:{list:[{value:1}]}};
  await replica.materializeWindow({signature:'tasks',entity:'tasks',key:'id',rows:[input],completeness:'complete',source:'server'});
  input.nested.list[0]!.value = 9;
  const row = replica.entity('tasks','a')!;
  expect(replica.entity('tasks','a')).toBe(row);
  expect(replica.entityBatch('tasks',['a','a'])).toEqual([row,row]);
  expect(replica.entityBatch('tasks',['a'])[0]).toBe(row);
  expect(replica.entityRows('tasks')[0]).toBe(row);
  expect(replica.watchRows('tasks',new Map())[0]).toBe(row);
  expect(() => { (row.nested as any).list[0].value = 10; }).toThrow(TypeError);
  expect(() => { (row.nested as any).list.push({value:10}); }).toThrow(TypeError);
  const patch = {nested:{list:[{value:2}]}};
  replica.applyOptimistic('edit',[{entity:'tasks',rowId:'a',op:'patch',fields:patch}]);
  patch.nested.list[0]!.value = 99;
  const predicted = replica.entity('tasks','a')!;
  expect(() => { (predicted.nested as any).list[0].value = 99; }).toThrow(TypeError);
  expect((predicted.nested as any).list[0].value).toBe(2);
  expect((row.nested as any).list[0].value).toBe(1);
  replica.rejectCommand('edit');
  expect(replica.entity('tasks','a')).toBe(row);
  const update = {nested:{list:[{value:3}]}};
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'a',operation:'update',newValue:update}]});
  update.nested.list[0]!.value = 99;
  const committed = replica.entity('tasks','a')!;
  expect(() => { (committed.nested as any).list[0].value = 99; }).toThrow(TypeError);
  expect((committed.nested as any).list[0].value).toBe(3);
  expect((row.nested as any).list[0].value).toBe(1);
});

it('dispatches to touched entity, row and window subscriptions once per atomic commit', async () => {
  const replica = new LocalReplica();
  await materialize(replica); await materialize(replica,'other');
  await materialize(replica,'tasks','visible',[{id:'b',value:2}]);
  const table = vi.fn(), a = vi.fn(), b = vi.fn(), both = vi.fn(), other = vi.fn(), all = vi.fn(), visible = vi.fn(), global = vi.fn();
  replica.subscribe(table,{entity:'tasks'}); replica.subscribe(a,{entity:'tasks',ids:['a']});
  replica.subscribe(b,{entity:'tasks',ids:['b']}); replica.subscribe(both,{entity:'tasks',ids:['a','a','b']});
  replica.subscribe(other,{entity:'other'}); replica.subscribe(all,{window:'tasks'}); replica.subscribe(visible,{window:'visible'}); replica.subscribe(global);
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{value:3}}]});
  for (const callback of [table,a,both,all,global]) expect(callback).toHaveBeenCalledTimes(1);
  for (const callback of [b,other,visible]) expect(callback).not.toHaveBeenCalled();
  expect(a.mock.calls[0]![0]).toEqual(new Set(['a']));
  await replica.applyTransaction({cursor:{epoch:'e',revision:2},changes:['a','b'].map(id=>({entity:'tasks',id,operation:'delete' as const}))});
  expect(both).toHaveBeenCalledTimes(2); expect(visible).toHaveBeenCalledTimes(1);
  replica.applyOptimistic('other',[{entity:'other',rowId:'a',op:'patch',fields:{value:4}}]);
  expect(a).toHaveBeenCalledTimes(2); expect(other).toHaveBeenCalledTimes(1);
  replica.setFreshness('offline'); expect(other).toHaveBeenCalledTimes(2); expect(visible).toHaveBeenCalledTimes(2);
  await replica.activateScope('new'); expect(a.mock.calls.length).toBeGreaterThan(2);
});

it('releases independently registered callbacks and cleans all subscription indexes', async () => {
  const replica = new LocalReplica(); await materialize(replica);
  const callback = vi.fn();
  const releases = [replica.subscribe(callback,{entity:'tasks',ids:['a','b']}),replica.subscribe(callback,{entity:'tasks',ids:['a']}),replica.subscribe(callback,{window:'tasks'}),replica.subscribe(callback,{entity:'tasks'})];
  releases[0]!(); releases[0]!();
  replica.applyOptimistic('edit',[{entity:'tasks',rowId:'a',op:'patch',fields:{value:3}}]);
  expect(callback).toHaveBeenCalledTimes(3);
  releases.forEach(release=>release());
  expect(replica['rowListeners'].size).toBe(0); expect(replica['entityListeners'].size).toBe(0); expect(replica['windowListeners'].size).toBe(0);
});

it('maintains exact residency totals across writes, deletes, epochs and failed persistence without scanning cold tables', async () => {
  let fail = false;
  const replica = new LocalReplica({load:async()=>undefined,applyTransaction:async()=>{if(fail)throw new Error('disk');},withReadView:async(_scope,_coverage,run)=>run({} as any),loadWindowRows:async()=>undefined}, {maxResidentRows:100});
  await materialize(replica); await materialize(replica,'cold');
  const check = () => {
    const rows = [...replica['entities'].values()].flatMap(table=>[...table.values()]);
    expect(replica['residentCount']).toBe(rows.length);
    expect(replica['residentBytes']).toBe(rows.reduce((sum,row)=>sum+JSON.stringify(row).length*2+256,0));
  };
  check();
  const scan = vi.spyOn(replica['entities'].get('cold')!,'values').mockImplementation(()=>{throw new Error('resident scan');});
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{value:'x'.repeat(100)}}]});
  replica['trimResidentRows'](); expect(scan).not.toHaveBeenCalled(); scan.mockRestore(); check();
  await replica.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'b',operation:'delete'},{entity:'tasks',id:'c',operation:'insert',newValue:{id:'c'}}]}); check();
  fail = true;
  await expect(replica.applyTransaction({cursor:{epoch:'e',revision:3},changes:[{entity:'tasks',id:'d',operation:'insert',newValue:{id:'d'}}]})).rejects.toThrow('disk'); check();
  expect(replica.entity('tasks','d')).toBeUndefined(); fail = false;
  await replica.applyTransaction({cursor:{epoch:'new',revision:1},changes:[{entity:'tasks',id:'z',operation:'insert',newValue:{id:'z'}}]}); check();
  expect(replica['residentCount']).toBe(1);
  replica.dispose(); expect(replica['residentBytes']).toBe(0); expect(replica['residentCount']).toBe(0);
});


it('old unsubscribe handles cannot remove new registrations for the same row', async () => {
  const replica = new LocalReplica(); await materialize(replica);
  const first = replica.subscribe(() => {}, {entity:'tasks',ids:['a']}); first();
  const callback = vi.fn(); const second = replica.subscribe(callback,{entity:'tasks',ids:['a']}); first();
  replica.applyOptimistic('edit',[{entity:'tasks',rowId:'a',op:'patch',fields:{value:3}}]);
  expect(callback).toHaveBeenCalledOnce(); second();
});

it('keeps computed offline windows reactive to relation-table changes', async () => {
  const replica = new LocalReplica(); await materialize(replica); await materialize(replica,'relations');
  const computed = vi.fn(), normal = vi.fn();
  replica.subscribe(computed,{window:'tasks',offlineGlobal:true}); replica.subscribe(normal,{window:'tasks'});
  replica.applyOptimistic('online',[{entity:'relations',rowId:'a',op:'patch',fields:{value:3}}]);
  expect(computed).not.toHaveBeenCalled(); expect(normal).not.toHaveBeenCalled();
  replica.setFreshness('offline'); computed.mockClear(); normal.mockClear();
  replica.applyOptimistic('offline',[{entity:'relations',rowId:'a',op:'patch',fields:{value:4}}]);
  expect(computed).toHaveBeenCalledOnce(); expect(normal).not.toHaveBeenCalled();
});
