import { expect, it, vi } from 'vitest';
import { runOfflineLiveQueryAsync } from './generated-query-async';
import { runOfflineLiveQuery, type LiveQueryPlan } from './query-expression';

const window = { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 2, maxLimit: 2, count: 'exact' as const };
const plan: LiveQueryPlan = { table: 'items', key: 'id', window, where: { operator: 'inRelation', column: 'id', relation: {
  table: 'links', column: 'item', where: { operator: 'eq', column: 'active', value: { literal: true } },
} } };

it('evaluates high-cardinality relation membership without retaining all related keys', async () => {
  const roots = [{ id: '1999', title: 'last' }], links = Array.from({ length: 2000 }, (_, id) => ({ id: String(id), item: String(id), active: true }));
  const expected = runOfflineLiveQuery(roots, plan, {}, 'complete', undefined, { relationRows: () => ({ rows: links, completeness: 'complete' }) });
  let visited = 0, closed = false;
  const result = await runOfflineLiveQueryAsync(roots, plan, {}, 'complete', { relationRows: () => ({ completeness: 'complete', rows: (async function* () {
    try { for (const row of links) { visited++; yield row; } } finally { closed = true; }
  })() }) }, { candidateBytes: 1024 });
  expect(result).toEqual(expected); expect(visited).toBe(2000); expect(closed).toBe(true);
  expect(() => runOfflineLiveQuery(roots, plan, {}, 'complete', undefined, { relationRows: () => ({ rows: links, completeness: 'complete' }) }, { candidateBytes: 1024 })).toThrow('candidate byte budget');
});

it('short-circuits relation streams and closes both iterators after a failed read', async () => {
  const failure = Error('read failed'); let relatedClosed = false, rootsClosed = false, visited = 0;
  const roots = (async function* () { try { yield { id: 'first' }; yield { id: 'last' }; } finally { rootsClosed = true; } })();
  const read = { relationRows: () => ({ completeness: 'complete' as const, rows: (async function* () {
    try { visited++; yield { item: 'first', active: true }; visited++; throw failure; } finally { relatedClosed = true; }
  })() }) };
  await expect(runOfflineLiveQueryAsync(roots, plan, {}, 'complete', read, { candidateBytes: 4096 })).rejects.toBe(failure);
  expect(visited).toBe(3); expect(relatedClosed).toBe(true); expect(rootsClosed).toBe(true);
});

it('preflights missing identity inside a relation before consuming any stream', async () => {
  const rows = vi.fn(), contextual: LiveQueryPlan = { ...plan, where: { ...plan.where!, relation: { ...plan.where!.relation!, where: { operator: 'eq', column: 'owner', value: { context: 'member.id' } } } } };
  const result = await runOfflineLiveQueryAsync((async function* () { rows(); yield { id: 'a' }; })(), contextual, {}, 'complete', {
    relationRows: () => ({ rows: [], completeness: 'complete' }),
  }, { candidateBytes: 4096 });
  expect(result).toMatchObject({ supported: false, unsupportedOperator: 'missingIdentityContext' }); expect(rows).not.toHaveBeenCalled();
});

it('matches numeric filters, null ordering, referenced sort values and limit argument semantics', async () => {
  const rows = [{ id: 'b', value: null, status: 'b' }, { id: 'a', value: 5, status: 'a' }, { id: 'c', value: 12, status: 'b' }];
  const statuses = { a: { order: 2 }, b: { order: 1 } }, reference = (_table: string, id: string) => statuses[id as keyof typeof statuses];
  const ordered: LiveQueryPlan = { table: 'items', key: 'id', window,
    filters: { argument: 'filters', allowedColumns: ['value'], allowedOperators: ['inRange', 'empty', 'notEquals'] },
    sort: { columnArgument: 'sort', directionArgument: 'direction', allowedColumns: ['value', 'status'], defaultColumn: 'value', defaultDirection: 'asc' },
    index: { table: 'index', key: 'id', columns: ['order'], sortColumns: { status: ['order', 'value'] }, referenceFields: { order: { table: 'statuses', foreignKey: 'status', column: 'order' } } } };
  for (const sort of ['value', 'status', 'invalid']) for (const direction of ['asc', 'desc']) for (const limit of [0, 1, 20, '2bad']) for (const filters of [[], [{ column: 'value', operator: 'inRange', value: '4', valueTo: '20' }], [{ column: 'value', operator: 'empty', value: '' }]]) {
    const args = { sort, direction, limit, filters };
    const actual = await runOfflineLiveQueryAsync(rows, ordered, args, 'complete', { referenceRow: async (...args) => reference(...args) }, { candidateBytes: 4096 });
    expect(actual).toEqual(runOfflineLiveQuery(rows, ordered, args, 'complete', reference));
  }
});

it('refuses a server-only relation predicate rather than claiming a complete empty result', async () => {
  const input: LiveQueryPlan = { ...plan, where: { ...plan.where!, relation: { ...plan.where!.relation!, where: { operator: 'server' } } } };
  expect(await runOfflineLiveQueryAsync([{ id: 'a' }], input, {}, 'complete', {
    relationRows: () => ({ rows: [{ item: 'a' }], completeness: 'complete' }),
  }, { candidateBytes: 4096 })).toMatchObject({ supported: false, unsupportedOperator: 'server' });
});
