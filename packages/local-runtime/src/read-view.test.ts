import { expect, it, vi } from 'vitest';
import { memoryReadView, overlayReadView } from './read-view.js';
import type { LocalPatch } from './index.js';
import { matchesDataPredicate, type DataPredicate } from '@gonvex/module-sdk';

it.each(['delete', 'insert'] as const)('ignores incomplete server rows superseded by a pending %s', async op => {
  const coverage = { assignments: { key: '_id', complete: true, columns: ['_id', 'taskId', 'deletedAt'] } };
  const base = memoryReadView(new Map([['assignments', new Map([
    ['a', { _id: 'a', taskId: 'task' }],
    ['b', { _id: 'b', taskId: 'task', deletedAt: null }],
  ])]]), coverage);
  const view = overlayReadView(base, coverage, [op === 'delete'
    ? { entity: 'assignments', rowId: 'a', op }
    : { entity: 'assignments', rowId: 'a', op, fields: { _id: 'a', taskId: 'task', deletedAt: null } }]);
  const result = await view.select({ table: 'assignments', where: { column: 'deletedAt', op: 'isNull' }, orderBy: [{column: '_id'}] });
  expect(result.complete).toBe(true);
  expect(result.rows.map(row => row._id)).toEqual(op === 'delete' ? ['b'] : ['a', 'b']);
  // Unknown fields in rows that still contribute to the result remain unsafe.
  expect((await base.select({ table: 'assignments', where: { column: 'deletedAt', op: 'isNull' } })).complete).toBe(false);
});

it('does not read unrelated pending edits when looking up one task', async () => {
  const coverage = { tasks: { key: '_id', complete: true } };
  const tasks = new Map(Array.from({ length: 1000 }, (_, i) => [`t${i}`, { _id: `t${i}`, priority: 'normal' }]));
  const base = memoryReadView(new Map([['tasks', tasks]]), coverage);
  const select = vi.spyOn(base, 'select');
  const patches: LocalPatch[] = [...tasks.keys()].map(rowId => ({ entity: 'tasks', rowId, op: 'patch', fields: { priority: 'high' } }));
  const view = overlayReadView(base, coverage, patches);
  expect(await view.select({ table: 'tasks', where: { column: '_id', op: 'eq', value: 't42' }, limit: 1 })).toMatchObject({ rows: [{ _id: 't42', priority: 'high' }], complete: true });
  expect(select).toHaveBeenCalledTimes(1);
});

it('bounds overlay reads to requested primary keys, including keys inside conjunctions', async () => {
  const coverage = { tasks: { key: '_id', complete: true } };
  const tasks = new Map(Array.from({ length: 1000 }, (_, i) => [`t${i}`, { _id: `t${i}`, priority: 'normal' }]));
  const base = memoryReadView(new Map([['tasks', tasks]]), coverage);
  const select = vi.spyOn(base, 'select');
  const patches: LocalPatch[] = [...tasks.keys()].map(rowId => ({ entity: 'tasks', rowId, op: 'patch', fields: { priority: 'high' } }));
  const view = overlayReadView(base, coverage, patches);
  expect(await view.select({ table: 'tasks', where: { and: [
    { column: '_id', op: 'in', values: ['t42', 't43', 't42'] },
    { column: 'priority', op: 'eq', value: 'high' },
  ] } })).toEqual({ rows: [{ _id: 't42', priority: 'high' }, { _id: 't43', priority: 'high' }], complete: true });
  // Initial selection plus one bounded fetch for rows that only match after edits.
  expect(select).toHaveBeenCalledTimes(2);
});

it('preserves partial coverage, exclusions, replacements and deletion for bounded overlay reads', async () => {
  const coverage = { tasks: { key: '_id', complete: false } };
  const base = memoryReadView(new Map([['tasks', new Map([
    ['a', {_id:'a', rank:1}], ['b', {_id:'b', rank:2}], ['c', {_id:'c', rank:3}],
  ])]]), coverage);
  const view = overlayReadView(base, coverage, [
    {entity:'tasks',rowId:'a',op:'delete'},
    {entity:'tasks',rowId:'b',op:'insert',fields:{_id:'b',rank:5}},
    {entity:'tasks',rowId:'c',op:'patch',fields:{rank:0}},
    {entity:'tasks',rowId:'unrelated',op:'insert',fields:{_id:'unrelated',rank:-1}},
  ]);
  const read = {table:'tasks',where:{column:'_id',op:'in' as const,values:['a','b','c']},orderBy:[{column:'rank'}],limit:1};
  expect(await view.select(read)).toEqual({rows:[{_id:'c',rank:0}],complete:true});
  expect(await view.select(read,['c'])).toEqual({rows:[{_id:'b',rank:5}],complete:true});
  expect((await view.select({...read,where:{...read.where,values:['b','missing']}})).complete).toBe(false);
});

it('replenishes a sorted window after pending deletions and moves', async () => {
  const coverage = { tasks: { key: '_id', complete: true } };
  const base = memoryReadView(new Map([['tasks', new Map([
    ['a', { _id: 'a', rank: 1 }], ['b', { _id: 'b', rank: 2 }], ['c', { _id: 'c', rank: 3 }], ['d', { _id: 'd', rank: 4 }],
  ])]]), coverage);
  const view = overlayReadView(base, coverage, [
    { entity: 'tasks', rowId: 'a', op: 'delete' },
    { entity: 'tasks', rowId: 'd', op: 'patch', fields: { rank: 0 } },
  ]);
  expect((await view.select({ table: 'tasks', orderBy: [{ column: 'rank' }], limit: 2 })).rows.map(row => row._id)).toEqual(['d', 'b']);
});

it('fetches pending rows that enter a filtered window in one bounded base read', async () => {
  const coverage = {tasks:{key:'_id',complete:true}};
  const tasks = new Map(Array.from({length:500},(_,i)=>[String(i),{_id:String(i),priority:'normal',rank:i}]));
  const base = memoryReadView(new Map([['tasks',tasks]]),coverage);
  const select = vi.spyOn(base,'select');
  const view = overlayReadView(base,coverage,[...tasks.keys()].map(rowId=>({entity:'tasks',rowId,op:'patch',fields:{priority:'high'}})));
  const result = await view.select({table:'tasks',where:{column:'priority',op:'eq',value:'high'},orderBy:[{column:'rank',direction:'desc'}],limit:3});
  expect(result).toEqual({complete:true,rows:[499,498,497].map(rank=>({_id:String(rank),rank,priority:'high'}))});
  expect(select).toHaveBeenCalledTimes(2);
  expect(select.mock.calls[1][0].where).toEqual({column:'_id',op:'in',values:[...tasks.keys()]});
});

it('uses a complete filtered slice only for reads contained by its predicate', async () => {
  const coverage = { assignments: { key: '_id', complete: false, columns: ['_id','taskId','deletedAt','privateNote'], completeWhere: [{ and: [{ column: 'taskId', op: 'eq' as const, value: 'one' }, { column: 'deletedAt', op: 'isNull' as const }] }] } };
  const view = memoryReadView(new Map([['assignments', new Map([['a', {_id:'a',taskId:'one',deletedAt:null}]])]]), coverage);
  const scoped = {table:'assignments',columns:['_id'],where:{and:[{column:'taskId',op:'eq' as const,value:'one'},{column:'deletedAt',op:'isNull' as const}]}};
  expect(await view.select(scoped)).toEqual({rows:[{_id:'a'}],complete:true});
  expect((await view.select({...scoped,where:{column:'deletedAt',op:'isNull'}})).complete).toBe(false);
  expect((await view.select({...scoped,where:{or:[scoped.where,{column:'taskId',op:'eq',value:'other'}]}})).complete).toBe(false);
  expect((await view.select({...scoped,columns:['privateNote']})).complete).toBe(false);
  const pending = overlayReadView(view,coverage,[{entity:'assignments',rowId:'a',op:'delete'}]);
  expect(await pending.select(scoped)).toEqual({rows:[],complete:true});
});

it('resolves a bounded list of resident primary keys without requiring the whole table', async () => {
  const statuses = new Map([['new', { _id: 'new', final: false }], ['done', { _id: 'done', final: true }], ['unrelated', { _id: 'unrelated' }]]);
  const scan = vi.spyOn(statuses, 'values');
  const view = memoryReadView(new Map([['statuses', statuses]]), { statuses: { key: '_id', complete: false } });
  const read = { table: 'statuses', columns: ['_id', 'final'], where: { column: '_id', op: 'in' as const, values: ['new', 'done', 'new'] } };
  expect(await view.select(read)).toEqual({ complete: true, rows: [{ _id: 'done', final: true }, { _id: 'new', final: false }] });
  expect(scan).not.toHaveBeenCalled();
  expect((await view.select({ ...read, where: { ...read.where, values: ['new', 'missing'] } })).complete).toBe(false);
  expect((await view.select({ ...read, columns: ['_id', 'name'] })).complete).toBe(false);
  expect(await view.select({ ...read, where: { ...read.where, values: [] } })).toEqual({ complete: true, rows: [] });
});

it('does not materialize the required-column set once per scanned row', async () => {
  const rows = new Map(Array.from({length:1000},(_,i)=>[String(i),{_id:String(i),value:i}]));
  const view = memoryReadView(new Map([['tasks',rows]]),{tasks:{key:'_id',complete:true,columns:['_id','value']}});
  const iterations = vi.spyOn(Set.prototype, Symbol.iterator);
  try {
    const result = await view.select({table:'tasks'});
    expect(result.rows).toHaveLength(1000);
    expect(result.complete).toBe(true);
    expect(iterations.mock.calls.length).toBeLessThan(10);
  } finally { iterations.mockRestore(); }
});

it.each<DataPredicate>([
  {column:'value',op:'in',values:[null,'1',1,true]},
  {column:'value',op:'in',values:[]},
  {and:[{column:'value',op:'in',values:['1',1]},{column:'value',op:'notNull'}]},
  {or:[{column:'value',op:'in',values:['1']},{column:'value',op:'isNull'}]},
])('keeps SQL membership semantics for indexed predicates %j', async where => {
  const rows=[null,'1',1,true,false,2].map((value,i)=>({_id:String(i),value}));
  const view=memoryReadView(new Map([['tasks',new Map(rows.map(row=>[row._id,row]))]]),{tasks:{key:'_id',complete:true}});
  expect((await view.select({table:'tasks',where})).rows).toEqual(rows.filter(row=>matchesDataPredicate(row,where)));
});
