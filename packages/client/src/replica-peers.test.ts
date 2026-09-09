import 'fake-indexeddb/auto';
import {afterEach, expect, it, vi} from 'vitest';
import {IndexedDBLocalReplicaStorage} from './indexeddb-replica.js';
import {LocalReplica} from './local-replica.js';

const stores: IndexedDBLocalReplicaStorage[] = [];
afterEach(() => stores.splice(0).forEach(store => store.close()));
async function peers() {
  const name = `replica-peers-${crypto.randomUUID()}`;
  const a = new IndexedDBLocalReplicaStorage(name);
  const b = new IndexedDBLocalReplicaStorage(name);
  stores.push(a, b);
  const first = new LocalReplica(a), second = new LocalReplica(b);
  await Promise.all([first.hydrate(), second.hydrate()]);
  return {first, second, a, b};
}
const window = (revision: number, rows: Record<string, any>[], signature = 'tasks') => ({
  signature, kind:'replica' as const, entity:'tasks', key:'_id', rows,
  completeness:'complete' as const, source:'server' as const,
  cursor:{epoch:'one',revision},
});

it('receives confirmed peer edits without dropping a later local prediction or changing the socket cursor', async () => {
  const {first, second} = await peers();
  await first.materializeWindow(window(1, [{_id:'a',status:'new',priority:'low'}]));
  await second.synchronizeStorage();
  second.applyOptimistic('pending', [{entity:'tasks',rowId:'a',op:'patch',fields:{priority:'high'}}]);
  const cursor = second.cursor();
  await first.applyTransaction({cursor:{epoch:'one',revision:2},changes:[{entity:'tasks',id:'a',operation:'update',newValue:{_id:'a',status:'progress'}}]});
  await second.synchronizeStorage();
  expect(second.entity('tasks','a')).toEqual({_id:'a',status:'progress',priority:'high'});
  second.rejectCommand('pending');
  expect(second.entity('tasks','a')).toEqual({_id:'a',status:'progress',priority:'low'});
  expect(second.cursor()).toEqual(cursor);
});

it('older projections fill missing columns but cannot overwrite newer shared fields', async () => {
  const {first, second, a} = await peers();
  await first.materializeWindow(window(5, [{_id:'a',status:'progress'}]));
  await second.materializeWindow(window(3, [{_id:'a',status:'new',description:'older projection'}], 'details'));
  const expected = {_id:'a',status:'progress',description:'older projection'};
  expect(second.entity('tasks','a')).toEqual(expected);
  await first.synchronizeStorage();
  expect(first.entity('tasks','a')).toEqual(expected);
  expect((await a.load())?.entities.tasks?.a).toEqual(expected);
});

it('does not stamp previously merged columns with a narrower projection revision', async () => {
  const {first, second} = await peers();
  await first.materializeWindow(window(1, [{_id:'a',status:'new',priority:'low'}]));
  await second.synchronizeStorage();
  await second.materializeWindow(window(3, [{_id:'a',priority:'high'}], 'priorities'));
  await first.materializeWindow(window(2, [{_id:'a',status:'progress'}], 'statuses'));
  expect(first.entity('tasks','a')).toEqual({_id:'a',priority:'high',status:'progress'});
});

it('does not resurrect deleted entities from delayed snapshots', async () => {
  const {first, second} = await peers();
  await first.materializeWindow(window(1, [{_id:'a',status:'new'}]));
  await second.synchronizeStorage();
  await first.applyTransaction({cursor:{epoch:'one',revision:4},changes:[{entity:'tasks',id:'a',operation:'delete'}]});
  await second.materializeWindow(window(2, [{_id:'a',status:'new'}]));
  expect(second.entity('tasks','a')).toBeUndefined();
  await first.synchronizeStorage();
  expect(first.entity('tasks','a')).toBeUndefined();
});

it('reads only records changed since its checkpoint and publishes one atomic peer update', async () => {
  const {first, second, b} = await peers();
  await first.materializeWindow(window(1, Array.from({length:1000}, (_,i) => ({_id:String(i),status:'new'}))));
  await second.synchronizeStorage();
  const reads = vi.spyOn(b,'readChanges');
  const listener = vi.fn(); second.subscribe(listener);
  await first.applyTransaction({cursor:{epoch:'one',revision:2},changes:[{entity:'tasks',id:'2',operation:'update',newValue:{status:'progress'}}]});
  await second.synchronizeStorage();
  const changes = await reads.mock.results[0]!.value;
  expect(Object.keys(changes.entities.tasks)).toEqual(['2']);
  expect(listener).toHaveBeenCalledTimes(1);
  await second.synchronizeStorage();
  expect(listener).toHaveBeenCalledTimes(1);
}, 20000);

it('rejects writes from a retired epoch and replaces peers atomically after a reset', async () => {
  const {first, second, a} = await peers();
  await first.materializeWindow(window(1, [{_id:'old'}]));
  await second.synchronizeStorage();
  await first.materializeWindow({...window(1,[{_id:'new'}]),cursor:{epoch:'two',revision:1}});
  await second.synchronizeStorage();
  expect(second.entityRows('tasks')).toEqual([{_id:'new'}]);
  await expect(second.materializeWindow(window(2,[{_id:'old'}]))).rejects.toThrow('obsolete replica epoch');
  expect((await a.load())?.entities.tasks).toEqual({new:{_id:'new'}});
});

it('persists a multi-page commit atomically and preserves repeated-row projection order', async () => {
  const {first, second, a} = await peers();
  await first.materializeWindow(window(1, [{_id:'removed',status:'new'}, {_id:'edited',status:'new',priority:'low'}]));
  await second.synchronizeStorage();
  await a.applyTransaction({cursor:{epoch:'one',revision:2},changes:[
    ...Array.from({length:300}, (_,i) => ({entity:'tasks',id:`new-${i}`,operation:'insert' as const,newValue:{_id:`new-${i}`,status:'new'}})),
    {entity:'tasks',id:'edited',operation:'update',newValue:{status:'progress'}},
    {entity:'tasks',id:'removed',operation:'delete'},
    {entity:'tasks',id:'edited',operation:'update',newValue:{priority:'high'}},
  ]}, {entities:{},liveQueries:{}});
  const observed: number[] = [];
  second.subscribe(() => observed.push(second.entityRows('tasks').length));
  await second.synchronizeStorage();
  expect(observed).toEqual([301]);
  expect(second.entity('tasks','removed')).toBeUndefined();
  expect(second.entity('tasks','edited')).toEqual({_id:'edited',status:'progress',priority:'high'});
  expect((await a.load())!.liveQueries.tasks!.ids).toEqual(['edited']);
});
