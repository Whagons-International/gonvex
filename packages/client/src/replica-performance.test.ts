import { expect, it, vi } from 'vitest';
import { LocalReplica } from './local-replica';

const materialize = (replica: LocalReplica, entity = 'tasks', signature = entity, rows = [{id:'a',value:1},{id:'b',value:2}]) => replica.materializeWindow({signature,entity,key:'id',rows,completeness:'complete',source:'server'});

it('does not rebuild unchanged reverse memberships for a single-row collection edit', async () => {
  const replica = new LocalReplica();
  const rows = Array.from({length:2000}, (_, i) => ({id:String(i), value:i}));
  await replica.materializeWindow({signature:'large',kind:'replica',entity:'tasks',key:'id',rows,completeness:'complete',source:'server'});
  await materialize(replica, 'tasks', 'small', rows.slice(0,2));
  const repin = vi.spyOn(replica as any, 'refreshPin');
  const changed = vi.fn(), untouched = vi.fn();
  replica.subscribe(changed, {window:'large'});
  replica.subscribe(untouched, {window:'small'});
  await replica.applyWindowDelta({signature:'large',kind:'replica',entity:'tasks',key:'id',upserts:[{id:'1000',value:9}],deleted:[]});
  expect(replica.entity('tasks','1000')!.value).toBe(9);
  expect(changed).toHaveBeenCalledOnce();
  expect(untouched).not.toHaveBeenCalled();
  expect(repin.mock.calls.filter(([, id]) => id !== '1000')).toEqual([]);
  repin.mockRestore();
  await replica.applyWindowDelta({signature:'large',kind:'replica',entity:'tasks',key:'id',upserts:[{id:'new',value:4}],deleted:['0']});
  expect(replica.getWindow('large')!.ids).toEqual([...rows.slice(1).map(row=>row.id), 'new']);
  changed.mockClear();
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'new',operation:'update',newValue:{value:5}}]});
  expect(changed).toHaveBeenCalledOnce();
  replica.dispose();
});

it('accounts for integrity maps added or removed without changing membership', async () => {
  const replica = new LocalReplica();
  const input = {signature:'tasks',kind:'replica' as const,entity:'tasks',key:'id',rows:[{id:'a'},{id:'b'}],completeness:'complete' as const,source:'server' as const};
  await replica.materializeWindow(input);
  const before = replica['membershipBytes'];
  await replica.materializeWindow({...input,hashes:{a:'0'.repeat(64),b:'1'.repeat(64)}});
  expect(replica['membershipBytes']).toBe(before + 2 * 112);
  await replica.materializeWindow(input);
  expect(replica['membershipBytes']).toBe(before);
  replica.dispose();
});

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

it('captures reducer tables lazily and isolates speculative reads across awaited commits', async () => {
  const replica = new LocalReplica(); await materialize(replica);
  const table = replica['entities'].get('tasks')!;
  const entries = vi.spyOn(table, 'entries');
  const captured = replica.captureExecutionRows(false);
  expect(entries).not.toHaveBeenCalled(); entries.mockRestore();
  let resume!:()=>void;
  const paused = new Promise<void>(resolve=>{resume=resolve;});
  const read = replica.withReadView(()=>({tasks:{key:'id',complete:true}}),false,async view=>{
    const first=await view.select({table:'tasks',where:{column:'id',op:'eq',value:'a'}});
    await paused;
    const second=await view.select({table:'tasks',where:{column:'id',op:'eq',value:'a'}});
    expect(second).toEqual(first);
    expect(second.rows[0]).toBe(first.rows[0]);
    expect(()=>{second.rows[0]!.value=999;}).toThrow(TypeError);
  },true);
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{value:9}}]});
  resume(); await read;
  expect(captured('tasks')[0]!.value).toBe(1);
  expect(replica.entity('tasks','a')!.value).toBe(9);
});

it('maintains equality candidates in table order across writes, deletes, rejection and scope resets', async () => {
  const replica = new LocalReplica(); await materialize(replica);
  expect(replica.equalityRows('tasks','value',1).map(row=>row.id)).toEqual(['a']);
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'b',operation:'update',newValue:{value:1}}]});
  expect(replica.equalityRows('tasks','value',1).map(row=>row.id)).toEqual(['a','b']);
  replica.applyOptimistic('edit',[{entity:'tasks',rowId:'a',op:'patch',fields:{value:2}}]);
  expect(replica.equalityRows('tasks','value',1)).toEqual(replica.entityRows('tasks'));
  replica.rejectCommand('edit');
  await replica.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'a',operation:'delete'}]});
  expect(replica.equalityRows('tasks','value',1).map(row=>row.id)).toEqual(['b']);
  await replica.activateScope('other');
  expect(replica.equalityRows('tasks','value',1)).toEqual([]);
});

it('updates reverse memberships once for burst deletes and reclaims removed window metadata', async () => {
  const replica = new LocalReplica(); await materialize(replica); await materialize(replica,'other');
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:['a','b'].map(id=>({entity:'tasks',id,operation:'delete' as const}))});
  expect(replica.getWindow('tasks')!.ids).toEqual([]);
  expect(replica['memberships'].has('tasks')).toBe(false);
  expect(replica['rowVersions'].has('tasks\0a')).toBe(false);
  await replica.removeWindow('other');
  expect(replica['memberships'].size).toBe(0);
  expect(replica['membershipBytes']).toBe(256);
});

it('invalidates each changed table once while publishing final repeated-row values', async () => {
  const replica = new LocalReplica(); await materialize(replica); await materialize(replica,'other');
  const mark = vi.spyOn(replica as any,'markEntityChanged');
  const callback = vi.fn(); replica.subscribe(callback,{entity:'tasks'});
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[
    {entity:'tasks',id:'a',operation:'update',newValue:{value:3}},
    {entity:'tasks',id:'a',operation:'update',newValue:{value:4}},
    {entity:'tasks',id:'b',operation:'update',newValue:{value:5}},
    {entity:'other',id:'a',operation:'update',newValue:{value:6}},
  ]});
  expect(mark.mock.calls.map(call=>call[0])).toEqual(['tasks','other']);
  expect(callback).toHaveBeenCalledOnce();
  expect(callback.mock.calls[0]![0]).toEqual(new Set(['a','b']));
  expect(replica.entity('tasks','a')!.value).toBe(4);
});

it('builds large equality indexes in bounded turns without losing concurrent edits', async () => {
  vi.useFakeTimers();
  const replica = new LocalReplica();
  try {
    const rows = Array.from({length:3000},(_,i)=>({id:String(i),value:i%2}));
    await materialize(replica,'tasks','tasks',rows);
    const candidates = replica.equalityRows('tasks','value',1);
    expect(candidates).toHaveLength(3000);
    const index = [...replica['equalityIndexes'].values()][0]!;
    expect(index.scan).toBeDefined();
    expect([...index.values.values()].reduce((n,ids)=>n+ids.size,0)).toBe(512);
    await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[
      {entity:'tasks',id:'0',operation:'update',newValue:{value:1}},
      {entity:'tasks',id:'2501',operation:'delete'},
      {entity:'tasks',id:'3001',operation:'insert',newValue:{id:'3001',value:1}},
    ]});
    await vi.runAllTimersAsync();
    expect(index.scan).toBeUndefined();
    expect(replica.equalityRows('tasks','value',1)).toEqual(replica.entityRows('tasks').filter(row=>row.value===1));
    await replica.activateScope('other');
    expect(replica.equalityRows('tasks','value',1)).toEqual([]);
  } finally { replica.dispose(); vi.useRealTimers(); }
});

it('finds overlapping large and small memberships through deletions and replacement', async () => {
  const replica = new LocalReplica();
  const rows = Array.from({length:2000},(_,i)=>({id:String(i),value:i}));
  await materialize(replica,'tasks','large',rows);
  await materialize(replica,'tasks','small',rows.slice(0,2));
  const large=vi.fn(),small=vi.fn();
  replica.subscribe(large,{window:'large'}); replica.subscribe(small,{window:'small'});
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'0',operation:'delete'}]});
  expect(large).toHaveBeenCalledOnce(); expect(small).toHaveBeenCalledOnce();
  expect(replica.getWindow('large')!.ids).not.toContain('0');
  expect(replica.getWindow('small')!.ids).toEqual(['1']);
  await materialize(replica,'tasks','large',rows.slice(2,3));
  expect(replica['largeMemberships'].size).toBe(0);
  large.mockClear(); small.mockClear();
  await replica.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'tasks',id:'1',operation:'update',newValue:{value:9}}]});
  expect(large).not.toHaveBeenCalled(); expect(small).toHaveBeenCalledOnce();
  replica.dispose(); expect(replica['memberships'].size).toBe(0);
});

it('applies invalidation floors to compact resident row and execution versions', async () => {
  const replica = new LocalReplica({load:async()=>undefined,applyTransaction:async()=>{},withReadView:async(_scope,_coverage,run)=>run({} as any),loadWindowRows:async()=>undefined});
  await materialize(replica);
  const before = [replica.rowVersion('tasks','a'),replica.executionReadVersion('tasks','a')];
  replica['invalidateEntityVersions'](); replica['notify'](true);
  expect(replica.rowVersion('tasks','a')).toBeGreaterThan(before[0]!);
  expect(replica.executionReadVersion('tasks','a')).toBeGreaterThan(before[1]!);
  const unchanged = [replica.rowVersion('tasks','a'),replica.executionReadVersion('tasks','a')];
  await replica.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'b',operation:'update',newValue:{value:3}}]});
  expect([replica.rowVersion('tasks','a'),replica.executionReadVersion('tasks','a')]).toEqual(unchanged);
  expect(replica['rowVersions'].size).toBe(0); expect(replica['executionRowVersions'].size).toBe(0);
  replica.dispose();
});
