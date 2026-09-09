import { describe, expect, it } from "vitest";
import { runOfflineLiveQuery } from "./query-expression";

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
