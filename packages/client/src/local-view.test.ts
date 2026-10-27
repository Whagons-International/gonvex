import { expect, it } from 'vitest';
import { readReplicaLiveQuery, replicaQueryDependencies, type ReplicaLiveQueryRequest, type ReplicaLiveQuerySource } from './replica-live-query.js';
import type { LocalViewPlan, LocalViewValue } from './local-view.js';

type Row = Record<string, string | number | null>;
const schema = { tasks: { key: '_id', fields: { _id: 'text', name: 'text', rank: 'number', statusId: 'text' } },
  statuses: { key: '_id', fields: { _id: 'text', name: 'text' } },
  pivots: { key: '_id', fields: { _id: 'text', taskId: 'text', fieldId: 'text', value: 'json' } } } as const;
const field = (column: string): LocalViewValue => ({ kind: 'field', column });
const present = (column: string): LocalViewValue => ({ kind: 'coalesce', values: [field(column)] });

/** Captured canonical rows plus named Replica memberships, matching the
 * storage contract: an authoritative membership limits rows and lookups, and
 * a missing or incomplete membership makes the result partial. */
function memoryStore(tables: Record<string, Row[]>) {
  const memberships = new Map<string, { ids: string[]; complete: boolean }>(
    Object.entries(tables).map(([table, rows]) => [table, { ids: rows.map(row => String(row._id)), complete: true }]));
  const counters = { gets: 0 };
  const byId = (table: string, id: string) => tables[table]?.find(row => row._id === id);
  function query(input: ReplicaLiveQueryRequest) {
    const authorities = new Map(Object.entries(input.authoritativeSources ?? {}));
    let partial = false;
    const membership = (signature: string | undefined) => { if (signature === undefined) return undefined;
      const found = memberships.get(signature); if (!found?.complete) partial = true; return found?.ids ?? []; };
    const member = (table: string) => membership(authorities.get(table));
    function* rows(table: string, root = false) {
      // The root source limits only root rows; same-table relations and
      // lookups use that table's own authority, if any.
      const ids = root && input.sourceSignature !== undefined ? membership(input.sourceSignature) : member(table);
      if (!ids) { yield* tables[table] ?? []; return; }
      for (const id of new Set(ids)) { const row = byId(table, id); if (row) yield row; else partial = true; }
    }
    const source: ReplicaLiveQuerySource = {
      key: table => table in schema ? '_id' : undefined,
      rows: table => rows(table) as Iterable<Record<string, never>>,
      rootRows: () => rows(input.plan.table, true) as Iterable<Record<string, never>>,
      get: (table, id) => { counters.gets++; const ids = member(table); return (ids && !ids.includes(id) ? undefined : byId(table, id)) as Record<string, never> | undefined; },
      validate() {}, validateIdentity() {},
    };
    const result = readReplicaLiveQuery(source, input, { candidateBytes: 4 * 1024 * 1024, examined: 10000000 });
    if (partial) result.completeness = 'partial';
    return result;
  }
  return { memberships, counters, query };
}
function fixture(count = 1000) {
  const tasks: Row[] = Array.from({ length: count }, (_, n) => ({ _id: String(n).padStart(5, '0'), name: 'Task ' + n, rank: n % 7 ? n : null, statusId: n % 2 ? 'a' : 'b' }));
  const tables: Record<string, Row[]> = { tasks, statuses: [{ _id: 'a', name: 'Needle' }, { _id: 'b', name: 'Other' }],
    pivots: tasks.map((row, n) => ({ _id: 'p' + row._id, taskId: row._id, fieldId: 'custom', value: n % 11 ? String(count - n) : null })) };
  const coverage = Object.fromEntries(Object.entries(schema).map(([table, info]) => [table, { key: '_id', columns: Object.keys(info.fields), complete: true }]));
  const request = (view: LocalViewPlan, patches?: ReplicaLiveQueryRequest['patches']): ReplicaLiveQueryRequest => ({ plan: { table: 'tasks', key: '_id',
    columns: Object.keys(schema.tasks.fields), window: { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 3, maxLimit: 3 } }, args: {}, coverage, view, patches });
  const source = { key: (table: string) => table in schema ? '_id' : undefined, rows: (table: string) => tables[table]!,
    get: (table: string, id: string) => tables[table]?.find(row => row._id === id), validate() {}, validateIdentity() {} };
  const store = memoryStore(tables);
  return { tables, coverage, request, source, store, query: (input: ReplicaLiveQueryRequest) => store.query(input) };
}
const defaultView: LocalViewPlan = { orderBy: [{ value: field('name'), direction: 'asc', mode: 'natural' }], window: { offset: 0, limit: 3 }, locale: 'en-US' };

it('constructs bounded nested projection arrays with complete lookup/relation dependencies', () => {
  const f = fixture(1);
  const tuple: LocalViewValue = { kind: 'array', values: [field('_id'),
    { kind: 'lookup', table: 'statuses', id: field('statusId'), value: field('name') },
    { kind: 'related', table: 'pivots', foreignKey: 'taskId', match: field('_id'), reduce: 'array',
      value: { kind: 'array', values: [field('_id'), field('value')] } },
  ] };
  const input = f.request({ ...defaultView, project: [tuple] });
  expect(replicaQueryDependencies(input).sort()).toEqual(['pivots', 'statuses', 'tasks']);
  expect(f.query(input)).toMatchObject({ rows: [], completeness: 'complete',
    projected: [{ id: '00000', values: [['00000', 'Other', [['p00000', null]]]] }] });
});

it('charges array construction incrementally and rejects malformed arrays even on empty sources', () => {
  const f = fixture(1), source = { key: (table: string) => table in schema ? '_id' : undefined,
    rows: (table: string) => f.tables[table]!, get: (table: string, id: string) => f.tables[table]?.find(row => row._id === id), validate() {}, validateIdentity() {} };
  const view: LocalViewPlan = { ...defaultView, project: [{ kind: 'array', values: Array.from({ length: 100 }, () => field('name')) }] };
  expect(() => readReplicaLiveQuery(source, f.request(view), { candidateBytes: 1000, examined: 10000 })).toThrow('candidate byte budget');
  expect(() => readReplicaLiveQuery({ ...source, rows: () => [] }, f.request({ ...defaultView,
    project: [{ kind: 'array', values: null } as unknown as LocalViewValue] }), { candidateBytes: 10000, examined: 10000 })).toThrow('Invalid local view array');
});

it('executes natural and independent multi-sort over cold canonical rows, counts empty windows, and never writes', () => {
  const f = fixture(), before = JSON.stringify(f.tables);
  const result = f.query(f.request(defaultView));
  expect(result.rows.map(row => row._id)).toEqual(['00000', '00001', '00002']); expect(result.total).toBe(1000); expect(result.completeness).toBe('complete');
  const multi: LocalViewPlan = { ...defaultView, orderBy: [{ value: { kind: 'literal', value: 1 }, mode: 'natural', direction: 'asc' }, { value: field('_id'), mode: 'natural', direction: 'desc' }] };
  expect(f.query(f.request(multi)).rows.map(row => row._id)).toEqual(['00999', '00998', '00997']);
  expect(f.query(f.request({ ...defaultView, window: { offset: 100, limit: 0 } }))).toMatchObject({ rows: [], total: 1000, limit: 0, offset: 100 });
  expect(JSON.stringify(f.tables)).toBe(before);
});

it('combines lookup search with finite numeric predicates and ordered root/reference predictions without persistence', () => {
  const f = fixture(30);
  const view: LocalViewPlan = { ...defaultView, where: { kind: 'and', terms: [
    { kind: 'text', op: 'contains', text: 'needle', value: { kind: 'cast', mode: 'text', value: { kind: 'lookup', table: 'statuses', id: field('statusId'), value: field('name') } } },
    { kind: 'compare', mode: 'number', op: 'gt', left: { kind: 'cast', mode: 'number', value: present('rank') }, right: { kind: 'literal', value: 5 } },
  ] } };
  const input = f.request(view, [{ entity: 'tasks', rowId: '00001', op: 'patch', fields: { rank: 100 } },
    { entity: 'statuses', rowId: 'b', op: 'patch', fields: { name: 'Needle predicted' } },
    { entity: 'tasks', rowId: '00000', op: 'delete' },
    { entity: 'tasks', rowId: 'created', op: 'insert', fields: { _id: 'created', name: 'Task 1.5', rank: 200, statusId: 'a' } }]);
  expect(replicaQueryDependencies(input).sort()).toEqual(['statuses', 'tasks']);
  const result = f.query(input);
  expect(result.rows.map(row => row._id)).toEqual(['00001', 'created', '00006']);
  // Twenty original finite ranks above five, one predicted rank and one insert.
  expect(result.total).toBe(22);
  expect(f.tables.statuses!.find(row => row._id === 'b')?.name).toBe('Other'); expect(f.tables.tasks!.find(row => row._id === '00001')?.rank).toBe(1); expect(f.tables.tasks!.find(row => row._id === 'created')).toBeUndefined();
});

it('orders canonical pivot values as numbers, keeps blanks last in both directions and captures predicted pivot changes', () => {
  const f = fixture(30);
  const pivot: LocalViewValue = { kind: 'cast', mode: 'numericText', value: { kind: 'related', table: 'pivots', foreignKey: 'taskId', match: field('_id'),
    where: { kind: 'compare', mode: 'strict', op: 'eq', left: field('fieldId'), right: { kind: 'literal', value: 'custom' } }, value: field('value'), reduce: 'last' } };
  const input = f.request({ ...defaultView, orderBy: [{ value: pivot, mode: 'natural', direction: 'asc', blanks: 'last' }] },
    [{ entity: 'pivots', rowId: 'p00001', op: 'patch', fields: { value: '-3.5' } }]);
  expect(f.query(input).rows.map(row => row._id)).toEqual(['00001', '00029', '00028']);
  input.view = { ...input.view!, orderBy: [{ value: pivot, mode: 'natural', direction: 'desc', blanks: 'last' }], window: { offset: 27, limit: 3 } };
  expect(f.query(input).rows.map(row => row._id)).toEqual(['00000', '00011', '00022']);
  expect(f.tables.pivots!.find(row => row._id === 'p00001')?.value).toBe('29');
});

it('refuses incomplete dependencies, missing identity and invalid plans instead of returning false completeness', () => {
  const f = fixture(30), lookup: LocalViewValue = { kind: 'lookup', table: 'statuses', id: field('statusId'), value: field('name') };
  const input = f.request({ ...defaultView, orderBy: [{ value: lookup, mode: 'natural', direction: 'asc' }] });
  input.coverage.statuses = { key: '_id', columns: ['_id'], complete: true };
  expect(f.query(input).completeness).toBe('partial');
  input.view = { ...defaultView, where: { kind: 'compare', mode: 'strict', op: 'eq', left: field('_id'), right: { kind: 'context', key: 'member.id' } } };
  expect(f.query(input)).toMatchObject({ rows: [], supported: false, unsupportedOperator: 'missingIdentityContext' });
  input.view = { ...defaultView, window: { offset: -1, limit: 3 } };
  expect(() => f.query(input)).toThrow('window');
});

it('enforces retained relation and selection budgets, including when the requested output is empty', () => {
  const f = fixture(30), source = { key: (table: string) => table in schema ? '_id' : undefined, rows: (table: string) => f.tables[table]!,
    get: (table: string, id: string) => f.tables[table]?.find(row => row._id === id), validate() {}, validateIdentity() {} };
  const view: LocalViewPlan = { ...defaultView, where: { kind: 'exists', table: 'pivots', foreignKey: 'taskId', match: field('_id') }, window: { offset: 0, limit: 0 } };
  expect(() => readReplicaLiveQuery(source, f.request(view), { candidateBytes: 100, examined: 100000 })).toThrow('candidate byte budget');
  expect(() => readReplicaLiveQuery(source, f.request(defaultView), { candidateBytes: 100000, examined: 1 })).toThrow('work budget');
  expect(readReplicaLiveQuery(source, f.request(defaultView), { candidateBytes: 100000, examined: 100000 }).total).toBe(30);
});

it('uses exact source membership and first-position deduplication rather than every cached table row', () => {
  const f = fixture(30), view: LocalViewPlan = { orderBy: [], window: { offset: 0, limit: 10 } };
  f.store.memberships.set('subset', { ids: ['00003', '00001', '00003'], complete: true });
  const input = { ...f.request(view), sourceSignature: 'subset' };
  const result = f.query(input);
  expect(result).toMatchObject({ total: 2, completeness: 'complete' }); expect(result.rows.map(row => row._id)).toEqual(['00003', '00001']);
  const selfRelated: LocalViewPlan = { ...view, where: { kind: 'exists', table: 'tasks', foreignKey: '_id', match: { kind: 'literal', value: '00029' } } };
  expect(f.query({ ...input, view: selfRelated }).total).toBe(2);
  expect(f.query({ ...input, sourceSignature: 'absent' })).toMatchObject({ total: 0, rows: [], completeness: 'partial' });
});

it('does not search a stale dependency entity excluded from its authoritative complete Replica membership', () => {
  const f = fixture(30);
  // A cached status row outside the complete statuses membership is stale.
  f.tables.statuses!.push({ _id: 'ghost', name: 'Needle stale label' });
  f.tables.tasks!.find(row => row._id === '00000')!.statusId = 'ghost';
  const input = { ...f.request({ ...defaultView, where: { kind: 'text', op: 'contains', text: 'needle', value: { kind: 'cast', mode: 'text',
    value: { kind: 'lookup', table: 'statuses', id: field('statusId'), value: field('name') } } } }), authoritativeSources: { tasks: 'tasks', statuses: 'statuses' } };
  const result = f.query(input);
  expect(result).toMatchObject({ total: 15, completeness: 'complete' }); expect(result.rows.map(row => row._id)).toEqual(['00001', '00003', '00005']);
  expect(f.tables.statuses!.find(row => row._id === 'ghost')?.name).toBe('Needle stale label');
});

it('validates malformed expressions on empty inputs and counts a huge zero-output offset without a candidate prefix', () => {
  const f = fixture(1);
  const source = { key: (table: string) => table in schema ? '_id' : undefined, rows: (table: string) => f.tables[table]!,
    get: (table: string, id: string) => f.tables[table]?.find(row => row._id === id), validate() {}, validateIdentity() {} };
  expect(readReplicaLiveQuery(source, f.request({ ...defaultView, window: { offset: 1000000, limit: 0 } }), { candidateBytes: 100, examined: 10000 })).toMatchObject({ rows: [], total: 1, limit: 0 });
  const empty = { ...source, rows: () => [] };
  expect(() => readReplicaLiveQuery(empty, f.request({ ...defaultView, where: { kind: 'compare', mode: 'strict', op: 'lt', left: field('rank'), right: { kind: 'literal', value: 1 } } }),
    { candidateBytes: 10000, examined: 10000 })).toThrow('requires equality');
  expect(() => readReplicaLiveQuery(empty, f.request({ ...defaultView, orderBy: [{ value: { kind: 'literal', value: Infinity }, mode: 'natural', direction: 'asc' }] }),
    { candidateBytes: 10000, examined: 10000 })).toThrow('JSON value');
  expect(() => f.query(f.request({ ...defaultView, orderBy: [{ value: { kind: 'cast', mode: 'number', value: field('name') }, mode: 'number', direction: 'asc' }] }))).toThrow('numeric sort value');
});

it('charges actual per-row expression work rather than only source rows and expression construction', () => {
  const f = fixture(100), source = { key: (table: string) => table in schema ? '_id' : undefined, rows: (table: string) => f.tables[table]!,
    get: (table: string, id: string) => f.tables[table]?.find(row => row._id === id), validate() {}, validateIdentity() {} };
  const view: LocalViewPlan = { ...defaultView, where: { kind: 'and', terms: Array.from({ length: 10 }, () => ({ kind: 'constant', value: true })) } };
  expect(() => readReplicaLiveQuery(source, f.request(view), { candidateBytes: 100000, examined: 800 })).toThrow('work budget');
  expect(readReplicaLiveQuery(source, f.request(view), { candidateBytes: 100000, examined: 10000 }).total).toBe(100);
});

it('counts every matching group before pagination with pending edits on the same captured root', () => {
  const f = fixture(1000);
  const view: LocalViewPlan = { ...defaultView, groupBy: present('statusId'),
    where: { kind: 'compare', mode: 'number', op: 'gte', left: field('rank'), right: { kind: 'literal', value: 10 } },
    window: { offset: 5, limit: 2 } };
  const patches: ReplicaLiveQueryRequest['patches'] = [
    { entity: 'tasks', rowId: '00011', op: 'patch', fields: { statusId: 'b' } },
    { entity: 'tasks', rowId: '00012', op: 'delete' },
    { entity: 'tasks', rowId: 'new', op: 'insert', fields: { _id: 'new', name: 'Task 1001', rank: 1001, statusId: null } },
  ];
  const input = f.request(view, patches), result = f.query(input);
  const expected = f.tables.tasks.filter(row => row._id !== '00012').map(row => row._id === '00011' ? { ...row, statusId: 'b' } : row);
  expected.push({ _id: 'new', name: 'Task 1001', rank: 1001, statusId: null });
  const counts = new Map<string, number>();
  for (const row of expected.filter(row => row.rank != null && Number(row.rank) >= 10)) {
    const key = String(row.statusId ?? ''); counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  expect(result.groups).toEqual([...counts].map(([key, count]) => ({ key, count })));
  expect(result.total).toBe([...counts.values()].reduce((a, b) => a + b, 0)); expect(result.rows).toHaveLength(2);
  expect(result.completeness).toBe('complete');
  input.view = { ...view, window: { offset: 1000000, limit: 0 } };
  expect(f.query(input)).toMatchObject({ rows: [], groups: result.groups, total: result.total });
  expect(f.tables.tasks!.find(row => row._id === '00011')!.statusId).toBe('a');
});

it('tracks grouped lookup dependencies, exact source membership and missing group fields', () => {
  const f = fixture(30), view: LocalViewPlan = { orderBy: [], window: { offset: 0, limit: 0 },
    groupBy: { kind: 'lookup', table: 'statuses', id: field('statusId'), value: field('name') } };
  expect(replicaQueryDependencies(f.request(view)).sort()).toEqual(['statuses', 'tasks']);
  f.store.memberships.set('subset', { ids: ['00001', '00002', '00001'], complete: true });
  const input = { ...f.request(view), sourceSignature: 'subset' };
  expect(f.query(input)).toMatchObject({ rows: [], total: 2, completeness: 'complete', groups: [{ key: 'Needle', count: 1 }, { key: 'Other', count: 1 }] });
  input.coverage.statuses = { key: '_id', columns: ['_id'], complete: true };
  expect(f.query(input).completeness).toBe('partial');
  input.view = { ...view, groupBy: field('absent') };
  expect(f.query(input).completeness).toBe('partial');
});

it('bounds distinct group memory and rejects malformed group expressions even on empty sources', () => {
  const f = fixture(30), source = { key: (table: string) => table in schema ? '_id' : undefined, rows: (table: string) => f.tables[table]!,
    get: (table: string, id: string) => f.tables[table]?.find(row => row._id === id), validate() {}, validateIdentity() {} };
  const view: LocalViewPlan = { orderBy: [], window: { offset: 0, limit: 0 }, groupBy: field('_id') };
  expect(() => readReplicaLiveQuery(source, f.request(view), { candidateBytes: 200, examined: 100000 })).toThrow('candidate byte budget');
  expect(() => readReplicaLiveQuery({ ...source, rows: () => [] }, f.request({ ...view, groupBy: { kind: 'field', column: '' } }), { candidateBytes: 10000, examined: 10000 })).toThrow('field');
  expect(() => readReplicaLiveQuery(source, f.request({ ...view, groupBy: { kind: 'literal', value: {} } }), { candidateBytes: 10000, examined: 10000 })).toThrow('group value');
  expect(readReplicaLiveQuery({ ...source, rows: () => [] }, f.request(view), { candidateBytes: 10000, examined: 10000 })).toMatchObject({ rows: [], total: 0, groups: [], completeness: 'complete' });
});

it('selects first, middle and last pages of 31012 wide rows under the existing logical budget', () => {
  const f = fixture(1), body = 'x'.repeat(20000);
  const rows = Array.from({ length: 31012 }, (_, n) => ({ _id: String(n), name: 'Task ' + n, rank: n, statusId: String(n % 3), body }));
  const byId = new Map(rows.map(row => [row._id, row])); let gets = 0;
  const source = { key: (table: string) => table in schema ? '_id' : undefined,
    rows: () => rows, get: (_table: string, id: string) => { gets++; return byId.get(id); }, validate() {}, validateIdentity() {} };
  for (const direction of ['asc', 'desc'] as const) for (const offset of [0, 15500, 31009, 50000]) {
    const view: LocalViewPlan = { orderBy: [{ value: field('rank'), mode: 'number', direction }], groupBy: field('statusId'), window: { offset, limit: 3 } };
    const before = gets, result = readReplicaLiveQuery(source, f.request(view), { candidateBytes: 4 * 1024 * 1024, examined: 10000000 });
    const expected = (direction === 'asc' ? rows : [...rows].reverse()).slice(offset, offset + 3);
    expect(result.rows).toEqual(expected); expect(gets - before).toBe(expected.length);
    expect(result).toMatchObject({ total: rows.length, completeness: 'complete', groups: [{ key: '0', count: 10338 }, { key: '1', count: 10337 }, { key: '2', count: 10337 }] });
  }
  // Compact selection does not exempt returned row bodies from the byte budget.
  expect(() => readReplicaLiveQuery(source, f.request({ orderBy: [], window: { offset: 0, limit: 110 } }),
    { candidateBytes: 4 * 1024 * 1024, examined: 10000000 })).toThrow('candidate byte budget');
});

it('preserves stable ties and blank-last mixed sorts when selecting either end of every page', () => {
  const f = fixture(1);
  const rows = Array.from({ length: 60 }, (_, n) => ({ _id: String(n), name: 'Task ' + n % 3, rank: n % 7 ? n % 4 : null, statusId: String(n % 2) }));
  const byId = new Map(rows.map(row => [row._id, row]));
  const source = { key: (table: string) => table in schema ? '_id' : undefined, rows: () => rows,
    get: (_table: string, id: string) => byId.get(id), validate() {}, validateIdentity() {} };
  const collator = new Intl.Collator('en-US', { numeric: true, sensitivity: 'base' });
  for (const a of ['asc', 'desc'] as const) for (const b of ['asc', 'desc'] as const) {
    const expected = [...rows].sort((left, right) => {
      if ((left.rank == null) !== (right.rank == null)) return left.rank == null ? 1 : -1;
      const rank = left.rank == null || right.rank == null ? 0 : left.rank - right.rank;
      return rank * (a === 'asc' ? 1 : -1) || collator.compare(left.name, right.name) * (b === 'asc' ? 1 : -1);
    });
    for (const offset of [0, 1, 15, 29, 30, 31, 47, 59, 60, 1000000]) for (const limit of [0, 1, 7, 100]) {
      const view: LocalViewPlan = { locale: 'en-US', orderBy: [
        { value: field('rank'), mode: 'natural', direction: a, blanks: 'last' },
        { value: field('name'), mode: 'natural', direction: b },
      ], window: { offset, limit } };
      const result = readReplicaLiveQuery(source, f.request(view), { candidateBytes: 4 * 1024 * 1024, examined: 10000000 });
      expect(result.rows).toEqual(expected.slice(offset, offset + limit)); expect(result.total).toBe(60);
    }
  }
});

it('reads sparse ordered windows on one root without expanding collapsed gaps or counting groups twice', () => {
  const f = fixture(1000);
  const view: LocalViewPlan = { ...defaultView, groupBy: field('statusId'), window: { offset: 0, limit: 250,
    ranges: [{ offset: 997, limit: 10 }, { offset: 2, limit: 2 }, { offset: 3, limit: 2 }, { offset: 20, limit: 0 }] } };
  const result = f.query(f.request(view));
  expect(result.positions).toEqual([2, 3, 4, 997, 998, 999]);
  expect(result.rows.map(row => row._id)).toEqual(['00002', '00003', '00004', '00997', '00998', '00999']);
  expect(result).toMatchObject({ total: 1000, groups: [{ key: 'b', count: 500 }, { key: 'a', count: 500 }], completeness: 'complete' });
  view.window.ranges = [];
  expect(f.query(f.request(view))).toMatchObject({ rows: [], positions: [], total: 1000, groups: result.groups });
  view.window.ranges = [{ offset: Number.MAX_SAFE_INTEGER, limit: 1 }];
  expect(() => f.query(f.request(view))).toThrow('range');
});

it('returns ID-only and scalar projections with the same exact sparse ordering/counts, without admitting task bodies', () => {
  const f = fixture(1000);
  const baseline = f.query(f.request({ ...defaultView, window: { offset: 900, limit: 3 } }));
  const projected = f.query(f.request({ ...defaultView, project: [field('rank')], window: { offset: 900, limit: 3 } }));
  expect(projected.rows).toEqual([]); expect(projected.total).toBe(1000); expect(projected.completeness).toBe('complete');
  expect(projected.projected).toEqual(baseline.rows.map(row => ({ id: row._id, values: [row.rank] })));
  const gets = f.store.counters.gets;
  const ids = f.query(f.request({ ...defaultView, project: [], window: { offset: 0, limit: 3, ranges: [{ offset: 0, limit: 2 }, { offset: 998, limit: 2 }] } }));
  expect(ids.projected).toEqual(['00000','00001','00998','00999'].map(id => ({ id, values: [] })));
  expect(ids.positions).toEqual([0,1,998,999]); expect(ids.rows).toEqual([]); expect(f.store.counters.gets).toBe(gets);
});
it('checks only projection-required columns, tracks conditional lookup dependencies, and refuses non-JSON projection values', () => {
  const f = fixture(30), conditional: LocalViewValue = { kind: 'if', condition: { kind: 'test', op: 'empty', value: present('rank') },
    then: { kind: 'literal', value: 'empty' }, else: { kind: 'lookup', table: 'statuses', id: field('statusId'), value: field('name') } };
  const input = f.request({ ...defaultView, project: [conditional], window: { offset: 0, limit: 30 } });
  expect(replicaQueryDependencies(input).sort()).toEqual(['statuses', 'tasks']);
  const result = f.query(input);
  expect(result.projected![0]).toEqual({ id: '00000', values: ['empty'] }); expect(result.projected![1]).toEqual({ id: '00001', values: ['Needle'] });
  input.view!.project = []; input.coverage.tasks = { key: '_id', columns: ['_id','name'], complete: true };
  expect(f.query(input).completeness).toBe('complete');
  input.view!.project = [field('rank')]; expect(f.query(input).completeness).toBe('partial');
  input.view!.project = [{ kind: 'cast', mode: 'number', value: { kind: 'literal', value: 'not numeric' } }];
  expect(() => f.query(input)).toThrow('Invalid local view JSON value');
});

it('combines 10000 long-ID relation memberships, substring filtering and priority sorting under the existing 4MiB budget', () => {
  const tasks = Array.from({ length: 10000 }, (_, n) => ({ _id: 'task:' + String(n).padStart(5, '0') + ':fixture-d15108d5-d789-6c97-b47f-8fe81b30964d-long-id', name: `Task ${n} needle`, rank: n }));
  const taskUsers = tasks.flatMap((row, n) => [{ _id: `u${n}`, taskId: row._id, userId: 'other' }, { _id: `v${n}`, taskId: row._id, userId: n % 3 ? 'other' : 'viewer' }]);
  let gets = 0;
  const tables = { tasks, taskUsers };
  const source = { key: (table: string) => table in tables ? '_id' : undefined,
    rows: (table: string) => tables[table as keyof typeof tables], get: () => { gets++; throw Error('ID projection must not hydrate bodies'); }, validate() {}, validateIdentity() {} };
  const assigned = { kind: 'exists' as const, table: 'taskUsers', foreignKey: 'taskId', match: field('_id'),
    where: { kind: 'compare' as const, op: 'eq' as const, mode: 'strict' as const, left: field('userId'), right: { kind: 'literal' as const, value: 'viewer' } } };
  const view: LocalViewPlan = { project: [], where: { kind: 'and', terms: [
    { kind: 'text', op: 'contains', value: field('name'), text: 'needle' },
    { kind: 'exists', table: 'taskUsers', foreignKey: 'taskId', match: field('_id') }] },
    orderBy: [{ value: { kind: 'if', condition: assigned, then: { kind: 'literal', value: 0 }, else: { kind: 'literal', value: 1 } }, direction: 'asc', mode: 'number' },
      { value: field('rank'), direction: 'desc', mode: 'number' }], window: { offset: 0, limit: 2000 } };
  const coverage = Object.fromEntries(Object.entries(tables).map(([table, rows]) => [table, { key: '_id', columns: Object.keys(rows[0]!), complete: true }]));
  const input: ReplicaLiveQueryRequest = { plan: { table: 'tasks', key: '_id', columns: ['_id', 'name', 'rank'], window: { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 2000, maxLimit: 2000 } }, args: {}, coverage, view };
  const result = readReplicaLiveQuery(source, input, { candidateBytes: 4 * 1024 * 1024, examined: 10000000 });
  const expected = [...tasks].sort((a, b) => Number(a.rank % 3 !== 0) - Number(b.rank % 3 !== 0) || b.rank - a.rank).slice(0, 2000);
  expect(result).toMatchObject({ rows: [], total: 10000, completeness: 'complete' }); expect(result.projected?.map(row => row.id)).toEqual(expected.map(row => row._id)); expect(gets).toBe(0);
});
