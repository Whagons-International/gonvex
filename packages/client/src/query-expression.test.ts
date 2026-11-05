import { describe, expect, it, vi } from "vitest";
import { evaluateLiveExpression, runOfflineLiveQuery, type LiveQueryPlan } from "./query-expression";

it('evaluates 10,000 primitive predicates without serializing row fields', () => {
  const stringify = JSON.stringify;
  let calls = 0;
  JSON.stringify = (...args: Parameters<typeof JSON.stringify>) => { calls++; return stringify(...args); };
  try {
    const expression = { operator: 'and', children: [
      { operator: 'eq', column: 'space', value: { literal: 'a' } },
      { operator: 'neq', column: 'deletedAt', value: { literal: 1 } },
    ] } as const;
    const row = { space: 'a', deletedAt: null };
    for (let i = 0; i < 10_000; i++) expect(evaluateLiveExpression(expression, row, {})).toBe(true);
    expect(calls).toBe(0);
  } finally { JSON.stringify = stringify; }
});

it('preserves JSON equality for JSON scalar, array and object values', () => {
  for (const [left, right] of [[null, null], [false, 0], [1, '1'], ['a', 'a'], [[1, 'a'], [1, 'a']], [[1], [2]], [{ a: 1 }, { a: 1 }], [{ a: 1 }, { a: 2 }]]) {
    expect(evaluateLiveExpression({ operator: 'eq', column: 'x', value: { literal: right as any } }, { x: left }, {})).toBe(JSON.stringify(left) === JSON.stringify(right));
  }
  expect(evaluateLiveExpression({ operator: 'and' }, {}, {})).toBe(true);
  expect(evaluateLiveExpression({ operator: 'or' }, {}, {})).toBe(false);
});

describe("portable Live Query plan", () => {
  it('uses normalized reference values for offline ordering with SQL null and ID tie rules',()=>{
    const values=[{id:'b',statusId:'z'},{id:'a',statusId:'z'},{id:'c',statusId:'a'},{id:'d',statusId:null}];
    const plan={table:'tasks',key:'id',index:{table:'taskGrid',key:'taskId',columns:['statusId'],sortColumns:{statusId:['statusOrder']},referenceFields:{statusOrder:{table:'statuses',foreignKey:'statusId',column:'order'}}},sort:{defaultColumn:'statusId',defaultDirection:'asc' as const,allowedColumns:['statusId'],directionArgument:'direction'}};
    const reference=(_table:string,id:string)=>({order:id==='z'?1:2});
    expect(runOfflineLiveQuery(values,plan,{},'complete',reference).rows.map(row=>row.id)).toEqual(['a','b','c','d']);
    expect(runOfflineLiveQuery(values,plan,{direction:'desc'},'complete',reference).rows.map(row=>row.id)).toEqual(['d','c','a','b']);
    expect(runOfflineLiveQuery(values,plan,{},'complete').completeness).toBe('partial');
  });
  const rows = [
    { id: "1", title: "Broken Freezer", priority: "urgent", deadline: 3 },
    { id: "2", title: "Replace light", priority: "normal", deadline: 1 },
    { id: "3", title: "Freezer inspection", priority: "urgent", deadline: 2 },
  ];

  it("runs the server Live Query AST against cached rows", () => {
    const result = runOfflineLiveQuery(rows, {
      table: "tasks",
      key: "id",
      where: { operator: "eq", column: "priority", value: { argument: "priority" } },
      search: { argument: "search", columns: ["title"] },
      sort: { columnArgument: "sort", directionArgument: "direction", allowedColumns: ["deadline"], defaultColumn: "deadline", defaultDirection: "asc" },
      window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 100 },
    }, { priority: "urgent", search: "freezer", sort: "deadline", direction: "asc", offset: 0, limit: 1 }, "partial");
    expect(result).toEqual({ rows: [rows[2]], completeness: "partial", supported: true });
  });

  it("refuses server-only plans instead of pretending cached data is exact", () => {
    expect(runOfflineLiveQuery(rows, { table: "tasks", key: "id", serverOnly: true }, {}, "complete"))
      .toMatchObject({ supported: false, unsupportedOperator: "serverOnly" });
  });

  it("reports the exact cached match count before slicing an exact-count window", () => {
    const result = runOfflineLiveQuery(rows, {
      table: "tasks",
      key: "id",
      where: { operator: "eq", column: "priority", value: { literal: "urgent" } },
      window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 100, count: "exact" },
    }, { offset: 1, limit: 1 }, "complete");
    expect(result).toMatchObject({ rows: [rows[2]], total: 2, offset: 1, limit: 1, completeness: "complete", supported: true });
  });

  it("applies the same structured filters used by the server and counts before slicing", () => {
    const result = runOfflineLiveQuery(rows, {
      table: "tasks",
      key: "id",
      filters: { argument: "filters", allowedColumns: ["title", "deadline"], allowedOperators: ["contains", "greaterThan"] },
      window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 100, count: "exact" },
    }, { filters: [{ column: "title", operator: "contains", value: "freezer" }], offset: 0, limit: 1 }, "complete");
    expect(result).toMatchObject({ rows: [rows[0]], total: 2, offset: 0, limit: 1, supported: true });
  });

  it("does not silently ignore malformed or unallowlisted filters offline", () => {
    const plan = { table: "tasks", key: "id", filters: { argument: "filters", allowedColumns: ["title"], allowedOperators: ["contains"] } } as const;
    expect(runOfflineLiveQuery(rows, plan, { filters: [{ column: "deadline", operator: "contains", value: "1" }] }, "complete"))
      .toMatchObject({ supported: false, unsupportedOperator: "invalidFilter" });
  });
});


describe('related query scopes', () => {
  it('deduplicates related membership, excludes revoked rows, and retains incomplete coverage', () => {
    const items = [{ id: 'owned', space: 'inbox' }, { id: 'routed', space: 'elsewhere' }, { id: 'hidden', space: 'elsewhere' }];
    const related = [
      { itemId: 'routed', space: 'inbox', member: 'me', revoked: null },
      { itemId: 'routed', space: 'inbox', member: 'me', revoked: null },
      { itemId: 'hidden', space: 'inbox', member: 'other', revoked: null },
      { itemId: 'hidden', space: 'inbox', member: 'me', revoked: 1 },
    ];
    const plan: any = { table: 'items', key: 'id', where: { operator: 'or', children: [
      { operator: 'eq', column: 'space', value: { argument: 'space' } },
      { operator: 'inRelation', column: 'id', relation: { table: 'routing', column: 'itemId', where: { operator: 'and', children: [
        { operator: 'eq', column: 'space', value: { argument: 'space' } },
        { operator: 'eq', column: 'member', value: { context: 'member.id' } },
        { operator: 'eq', column: 'revoked', value: { literal: null } },
      ] } } },
    ] }, window: { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 100, maxLimit: 100, count: 'exact' } };
    const options = { context: { 'member.id': 'me' }, relationRows: () => ({ rows: related, completeness: 'complete' as const }) };
    const result = runOfflineLiveQuery(items, plan, { space: 'inbox' }, 'complete', undefined, options);
    expect(result.rows.map(row => row.id)).toEqual(['owned', 'routed']);
    expect(result.total).toBe(2);
    expect(result.completeness).toBe('complete');
    expect(runOfflineLiveQuery(items, plan, { space: 'inbox' }, 'complete', undefined, { ...options, relationRows: () => ({ rows: related, completeness: 'partial' }) }).completeness).toBe('partial');
    expect(runOfflineLiveQuery(items, plan, { space: 'inbox' }, 'complete').supported).toBe(false);
  });
});

it('matches full stable sorting for bounded top-k windows, ties, nulls, offsets and both directions',()=>{
  const rows=Array.from({length:513},(_,i)=>({id:String(i%17),order:i%11===0?null:(i*71)%100}));
  for(const direction of ['asc','desc'] as const)for(const offset of [0,1,100,600])for(const limit of [1,20,1000]){
    const plan={table:'tasks',key:'id',sort:{allowedColumns:['order'],defaultColumn:'order',defaultDirection:direction},window:{offsetArgument:'offset',limitArgument:'limit',defaultLimit:20,maxLimit:1000,count:'exact' as const}};
    const full=runOfflineLiveQuery(rows,{...plan,window:undefined},{},'complete').rows;
    const result=runOfflineLiveQuery(rows,plan,{offset,limit},'complete');
    expect(result.rows).toEqual(full.slice(offset,offset+limit)); expect(result.total).toBe(rows.length);
  }
});

it('reuses frozen search text but observes changes in caller-owned mutable rows',()=>{
  const rows=[{id:'a',title:'First'}];
  const plan={table:'tasks',key:'id',search:{argument:'search',columns:['title']}};
  expect(runOfflineLiveQuery(rows,plan,{search:'first'},'complete').rows).toHaveLength(1);
  rows[0]!.title='Second';
  expect(runOfflineLiveQuery(rows,plan,{search:'first'},'complete').rows).toHaveLength(0);
  Object.freeze(rows[0]);
  expect(runOfflineLiveQuery(rows,plan,{search:'SECOND'},'complete').rows).toHaveLength(1);
  expect(runOfflineLiveQuery(rows,plan,{search:'second'},'complete').rows).toHaveLength(1);
});

it('scans 10,000 matches without materializing intermediate corpus arrays and prepares oneOf once', () => {
  const rows = Array.from({ length: 10_000 }, (_, i) => ({ id: String(i), title: `Task ${i}`, priority: i % 2 ? 'urgent' : 'normal' }));
  // The input corpus supports indexed reads. Intermediate filter/map/slice or
  // spread copies would fail this check even if final window rows were correct.
  for (const method of ['filter', 'map', 'slice', Symbol.iterator]) {
    Object.defineProperty(rows, method, { value: () => { throw new Error('full corpus materialization'); } });
  }
  const plan: LiveQueryPlan = {
    table: 'tasks', key: 'id',
    where: { operator: 'neq', column: 'priority', value: { literal: 'excluded' } },
    filters: { argument: 'filters', allowedColumns: ['priority'], allowedOperators: ['oneOf'] },
    search: { argument: 'search', columns: ['title'], booleanTerms: true },
    window: { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 100, maxLimit: 100, count: 'exact' },
  };
  const parse = vi.spyOn(JSON, 'parse');
  try {
    const result = runOfflineLiveQuery(rows, plan, { filters: [{ column: 'priority', operator: 'oneOf', value: '["urgent"]' }], search: 'task $and 9', offset: 2, limit: 5 }, 'complete');
    const expected = Array.from({ length: rows.length }, (_, i) => rows[i]!).filter(row => row.priority === 'urgent' && row.title.includes('9'));
    expect(result).toMatchObject({ rows: expected.slice(2, 7), total: expected.length, offset: 2, limit: 5, completeness: 'complete' });
    expect(result.rows[0]).toBe(expected[2]);
    expect(parse).toHaveBeenCalledTimes(1);
  } finally { parse.mockRestore(); }
});

it('validates sort fields and related completeness beyond a bounded result window', () => {
  const rows: Array<{ id: string; rank?: number; statusId: string }> = Array.from({ length: 10_000 }, (_, i) => ({ id: String(i), rank: i, statusId: String(i) }));
  const plan: LiveQueryPlan = {
    table: 'tasks', key: 'id',
    sort: { allowedColumns: ['rank'], defaultColumn: 'rank', defaultDirection: 'asc' },
    window: { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 2, maxLimit: 10, count: 'exact' },
  };
  delete rows.at(-1)!.rank;
  expect(runOfflineLiveQuery(rows, plan, {}, 'complete')).toMatchObject({ rows: [], supported: false, unsupportedOperator: 'missingSortField', completeness: 'partial' });
  const referencePlan: LiveQueryPlan = { ...plan, index: { table: 'taskGrid', key: 'id', columns: [], sortColumns: { rank: ['statusOrder'] },
    referenceFields: { statusOrder: { table: 'statuses', foreignKey: 'statusId', column: 'order', fallback: 10_000 } } } };
  const reference = vi.fn((_table: string, id: string) => id === '9999' ? undefined : { order: Number(id) });
  const result = runOfflineLiveQuery(rows, referencePlan, {}, 'complete', reference);
  expect(result).toMatchObject({ rows: [rows[0], rows[1]], total: rows.length, completeness: 'partial', supported: true });
  expect(result.rows[0]).toBe(rows[0]);
  expect(reference).toHaveBeenCalledTimes(rows.length);
});

it('matches independent sorting with reference keys, filters, stable ties and offset limits', () => {
  const rows = Array.from({ length: 431 }, (_, position) => ({ id: String(position % 13), group: position % 4, statusId: position % 7 ? String(position % 11) : null, position }));
  for (const direction of ['asc', 'desc'] as const) for (const offset of [0, 5, 500]) {
    const plan: LiveQueryPlan = {
      table: 'tasks', key: 'id', where: { operator: 'neq', column: 'group', value: { literal: 2 } },
      index: { table: 'taskGrid', key: 'id', columns: [], sortColumns: { statusId: ['statusOrder'] },
        referenceFields: { statusOrder: { table: 'statuses', foreignKey: 'statusId', column: 'order' } } },
      sort: { allowedColumns: ['statusId'], defaultColumn: 'statusId', defaultDirection: direction },
      window: { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 23, maxLimit: 23, count: 'exact' },
    };
    const expected = rows.filter(row => row.group !== 2).sort((a, b) => {
      const left = a.statusId === null ? null : Number(a.statusId) % 3;
      const right = b.statusId === null ? null : Number(b.statusId) % 3;
      const order = left === null ? right === null ? 0 : 1 : right === null ? -1 : left - right;
      return (direction === 'desc' ? -order : order) || a.id.localeCompare(b.id);
    });
    const result = runOfflineLiveQuery(rows, plan, { offset, limit: 23 }, 'complete', (_table, id) => ({ order: Number(id) % 3 }));
    expect(result.rows).toEqual(expected.slice(offset, offset + 23));
    expect(result.total).toBe(expected.length);
    if (result.rows.length) expect(result.rows[0]).toBe(expected[offset]);
  }
});

it('retains empty boolean-search and malformed oneOf behavior', () => {
  const rows = [{ id: 'a', title: 'Alpha', value: 'null' }, { id: 'b', title: 'Beta', value: 'other' }];
  const plan: LiveQueryPlan = { table: 'tasks', key: 'id', search: { argument: 'search', columns: ['title'], booleanTerms: true } };
  expect(runOfflineLiveQuery(rows, plan, { search: '$or' }, 'complete').rows).toEqual([]);
  expect(runOfflineLiveQuery(rows, plan, { search: '$and' }, 'complete').rows).toEqual(rows);
  expect(runOfflineLiveQuery(rows, plan, { search: 'alpha $or beta' }, 'complete').rows).toEqual(rows);
  const filterPlan: LiveQueryPlan = { table: 'tasks', key: 'id', filters: { argument: 'filters', allowedColumns: ['value'], allowedOperators: ['oneOf'] } };
  for (const value of ['invalid json', '{}', 'null']) expect(runOfflineLiveQuery(rows, filterPlan, { filters: [{ column: 'value', operator: 'oneOf', value }] }, 'complete').rows).toEqual([]);
  expect(runOfflineLiveQuery(rows, filterPlan, { filters: [{ column: 'value', operator: 'oneOf', value: '[null]' }] }, 'complete').rows).toEqual([rows[0]]);
});
