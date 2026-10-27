import type { JsonValue } from '@gonvex/protocol';
import type { LocalPatch } from '@gonvex/local-runtime';
import { mapResult, type MaybePromise, type RowStream } from './local-view-stream.js';
import { readIsCovered, type ReadCoverage } from '@gonvex/local-runtime/read-view';
import { runOfflineLiveQuery, liveScopeCoverage, liveQueryDependencies, type LiveQueryExpression, type LiveQueryPlan, type OfflineLiveQueryResult } from './query-expression.js';
import { localViewDependencies, readLocalView, readLocalViewAsync, type AsyncLocalViewSource, type LocalViewPlan } from './local-view.js';
import { LocalQueryBudgetError } from './local-query-budget.js';
import { runOfflineLiveQueryAsync } from './generated-query-async.js';

export type ReplicaLiveQueryRequest = {
  plan: LiveQueryPlan;
  args: Record<string, JsonValue>;
  coverage: ReadCoverage;
  context?: Readonly<Record<string, JsonValue>>;
  patches?: readonly LocalPatch[];
  /** Application-owned local semantics, separate from the server SQL plan. */
  view?: LocalViewPlan;
  /** Root membership/order owned by the generated Replica source. */
  sourceSignature?: string;
  /** Fixed exclusions declared by this exact root source. They also constrain
   * same-table lookups/relations and the pending journal overlay. */
  sourceExclusions?: readonly string[];
  /** Explicit generated related collections, scoped to this read only. */
  sourceBindings?: Readonly<Record<string, { signature: string; exclusions: readonly string[] }>>;
  /** Complete, unfiltered Replica memberships that own dependency tables. */
  authoritativeSources?: Readonly<Record<string, string>>;
};
export function replicaQueryDependencies(input: Pick<ReplicaLiveQueryRequest, 'plan' | 'view'>): string[] {
  return input.view ? localViewDependencies(input.plan.table, input.view) : liveQueryDependencies(input.plan);
}
export type ReplicaLiveQueryResult = {
  scope: string;
  sequence: number;
  result: OfflineLiveQueryResult<Record<string, JsonValue>>;
};
type Row = Record<string, JsonValue>;
export type ReplicaLiveQuerySource = {
  key(table: string): string | undefined;
  rows(table: string): Iterable<Row>;
  rootRows?(): Iterable<Row>;
  /** Canonical base for predictions omitted by a conservative index arm. */
  rootBase?(id: string): Row | undefined;
  get(table: string, id: string): Row | undefined;
  validate(table: string, id: string, row: Row): void;
  validateIdentity(table: string, id: string): void;
};

function prepareReplicaQuery(source: Pick<ReplicaLiveQuerySource, 'key' | 'validate' | 'validateIdentity'>,
  input: ReplicaLiveQueryRequest, budget: { candidateBytes: number; examined: number }) {
  const { plan, args, coverage } = input;
  if (input.sourceSignature !== undefined && (!input.view || typeof input.sourceSignature !== 'string' || !input.sourceSignature)) throw Error('Invalid local view source signature');
  if (!plan || typeof plan.table !== 'string' || !plan.table || typeof plan.key !== 'string' || !plan.key) throw Error('Invalid Live Query table or key');
  if (input.sourceExclusions !== undefined && (!input.view || !input.sourceSignature || !Array.isArray(input.sourceExclusions)
    || input.sourceExclusions.some(column => typeof column !== 'string' || !column || plan.columns && !plan.columns.includes(column)))) {
    throw Error('Invalid local view source exclusions');
  }
  if (!plan.window) throw Error('Disk Live Query requires a bounded window');
  if (![plan.window.defaultLimit, plan.window.maxLimit].every(value => Number.isSafeInteger(value) && value >= 0)
    || !plan.window.offsetArgument || !plan.window.limitArgument) throw Error('Invalid Live Query window');
  if (source.key(plan.table) !== plan.key) throw Error('Invalid Live Query table or key');
  if (!Number.isSafeInteger(budget.examined) || budget.examined < 0) throw Error('Invalid Live Query work budget');
  let examined = 0;
  if (!Number.isSafeInteger(budget.candidateBytes) || budget.candidateBytes < 0) throw Error('Invalid Live Query candidate budget');
  let predictionBytes = 0;
  const visit = () => { if (++examined > budget.examined) throw new LocalQueryBudgetError('workBudget', 'Live Query work budget'); };
  const patches = new Map<string, Map<string, LocalPatch[]>>();
  for (const patch of input.patches ?? []) {
    visit(); predictionBytes += JSON.stringify(patch).length * 2 + 256;
    if (predictionBytes > budget.candidateBytes) throw new LocalQueryBudgetError('candidateByteBudget', 'Live Query candidate byte budget');
    source.validateIdentity(patch.entity, patch.rowId);
    if (!['delete', 'patch', 'insert'].includes(patch.op)) throw Error('Invalid Live Query prediction');
    let table = patches.get(patch.entity); if (!table) patches.set(patch.entity, table = new Map());
    let chain = table.get(patch.rowId); if (!chain) table.set(patch.rowId, chain = []);
    chain.push(patch);
  }
  const exclusions = new Map<string, readonly string[]>();
  if (input.sourceExclusions) exclusions.set(plan.table, input.sourceExclusions);
  for (const [table, binding] of Object.entries(input.sourceBindings ?? {})) {
    if (table === plan.table || !source.key(table) || !binding.signature || input.authoritativeSources?.[table] !== binding.signature
      || !Array.isArray(binding.exclusions) || binding.exclusions.some(column => typeof column !== 'string' || !column
        || coverage[table]?.columns && !coverage[table]!.columns!.includes(column))) throw Error('Invalid local view related source exclusions');
    exclusions.set(table, binding.exclusions);
  }
  const admittedRow = (table: string, row: Row | undefined) =>
    exclusions.get(table)?.some(column => row?.[column] !== null && row?.[column] !== undefined) ? undefined : row;
  let unprovedSliceEntry = false;
  const overlay = (table: string, id: string, row: Row | undefined) => {
    for (const patch of patches.get(table)?.get(id) ?? []) {
      visit();
      if (patch.op === 'delete') row = undefined;
      else if (patch.op === 'insert') row = patch.fields;
      else if (row) row = { ...row, ...patch.fields };
      else if (exclusions.get(table)?.some(column => Object.hasOwn(patch.fields, column)
        && patch.fields[column] == null)) {
        // A patch cannot reconstruct a row excluded from authoritative source
        // membership. It may now belong in this slice, but physical cache rows
        // from another source are not evidence of visibility or a full base.
        unprovedSliceEntry = true;
      }
    }
    if (row && patches.get(table)?.has(id)) source.validate(table, id, row);
    return admittedRow(table, row);
  };
  return { plan, args, coverage, patches, overlay, admittedRow, visit, predictionBytes, sliceIncomplete: () => unprovedSliceEntry };
}

/** Evaluate one generated plan over a captured canonical root. Predictions
 * overlay it for this read only; neither rows nor membership are persisted. */
export function readReplicaLiveQuery(source: ReplicaLiveQuerySource, input: ReplicaLiveQueryRequest,
  budget: { candidateBytes: number; examined: number }): OfflineLiveQueryResult<Row> {
  const { plan, args, coverage, patches, overlay, admittedRow, visit, predictionBytes, sliceIncomplete } = prepareReplicaQuery(source, input, budget);
  function* rows(table: string, root = false): Generator<Row> {
    const key = source.key(table); if (!key) return;
    const changed = patches.get(table), seen = new Set<string>();
    for (const stored of root ? source.rootRows?.() ?? source.rows(table) : source.rows(table)) {
      visit();
      const id = String(stored[key]);
      if (!changed?.has(id)) { const row = admittedRow(table, stored); if (row) yield row; continue; }
      seen.add(id); const row = overlay(table, id, stored); if (row) yield row;
    }
    for (const id of changed?.keys() ?? []) if (!seen.has(id)) {
      const row = overlay(table, id, root ? source.rootBase?.(id) : undefined); if (row) yield row;
    }
  }
  if (input.view) {
    const result = readLocalView({ key: source.key, rows, rootRows: () => rows(plan.table, true), get: (table, id) => {
      visit(); return overlay(table, id, source.get(table, id));
    } }, plan.table, plan.key, input.view, coverage, input.context, {
      candidateBytes: budget.candidateBytes - predictionBytes, visit,
    }, plan.columns);
    if (sliceIncomplete()) result.completeness = 'partial';
    return result;
  }
  const { complete, references } = generatedReplicaCoverage(source, input);
  let referenceIncomplete = false;
  const result = runOfflineLiveQuery(rows(plan.table), plan, args, complete(plan.table, plan.where) ? 'complete' : 'partial', (table, id) => {
    visit();
    const row = source.key(table) ? overlay(table, id, source.get(table, id)) : undefined;
    if (row && [...references.get(table) ?? []].some(column => !Object.hasOwn(row, column))) referenceIncomplete = true;
    return row;
  }, {
    context: input.context,
    relationRows: (table, where) => ({ rows: rows(table), completeness: complete(table, where) ? 'complete' : 'partial' }),
  }, { candidateBytes: budget.candidateBytes - predictionBytes });
  if (referenceIncomplete) result.completeness = 'partial';
  return result;
}

/** Coverage checks are shared by resident and streamed generated plans. */
function generatedReplicaCoverage(source: Pick<ReplicaLiveQuerySource, 'key'>, input: ReplicaLiveQueryRequest) {
  const { plan, args, coverage } = input;
  const required = new Map<string, Set<string>>();
  const require = (table: string, column?: string) => {
    if (!column) return;
    let fields = required.get(table); if (!fields) required.set(table, fields = new Set()); fields.add(column);
  };
  const scopeColumns = (table: string, where?: LiveQueryExpression): void => {
    if (!where) return;
    require(table, where.column);
    if (where.relation) { require(where.relation.table, where.relation.column); scopeColumns(where.relation.table, where.relation.where); }
    for (const child of where.children ?? []) scopeColumns(table, child);
  };
  require(plan.table, plan.key); scopeColumns(plan.table, plan.where);
  for (const column of plan.columns ?? []) require(plan.table, column);
  const filters = plan.filters && args[plan.filters.argument];
  if (Array.isArray(filters)) for (const filter of filters) if (filter && typeof filter === 'object' && !Array.isArray(filter) && typeof filter.column === 'string') require(plan.table, filter.column);
  if (plan.search && String(args[plan.search.argument] ?? '').trim()) for (const column of plan.search.offlineColumns ?? plan.search.columns) require(plan.table, column);
  const requestedSort = plan.sort && String(plan.sort.columnArgument ? args[plan.sort.columnArgument] ?? '' : '');
  const sortColumn = plan.sort && (plan.sort.allowedColumns.includes(requestedSort || '') ? requestedSort! : plan.sort.defaultColumn);
  const references = new Map<string, Set<string>>();
  for (const field of sortColumn ? plan.index?.sortColumns?.[sortColumn] ?? [sortColumn] : []) {
    const reference = plan.index?.referenceFields?.[field];
    if (!reference) require(plan.table, field);
    else {
      require(plan.table, reference.foreignKey);
      let fields = references.get(reference.table); if (!fields) references.set(reference.table, fields = new Set()); fields.add(reference.column);
    }
  }
  const complete = (table: string, where?: LiveQueryExpression) => {
    const known = coverage[table];
    return source.key(table) === known?.key && readIsCovered(known, { table, where: liveScopeCoverage(where, args) })
      && [...required.get(table) ?? []].every(column => !known?.columns || known.columns.includes(column));
  };
  return { complete, references };
}

export type AsyncReplicaLiveQuerySource = Pick<ReplicaLiveQuerySource, 'key' | 'validate' | 'validateIdentity'> & {
  rows(table: string): RowStream<Row>;
  rootRows?(): RowStream<Row>;
  rootOrder?: AsyncLocalViewSource['rootOrder'];
  rootDistinctOrder?: string;
  /** The proved ordered stream includes every admitted canonical root, with
   * no omitted null/missing keys. Its counting pass also validates semantics. */
  rootDistinctExhaustive?: boolean;
  rootBase?(id: string): MaybePromise<Row | undefined>;
  get(table: string, id: string): MaybePromise<Row | undefined>;
  prepareRelated?: AsyncLocalViewSource['prepareRelated'];
  streamRelatedExistence?: boolean;
  /** Include changed canonical IDs in source order, even if a prediction moves
   * their foreign key into this match. Unseen inserts are appended by overlay. */
  relatedRows?(table: string, foreignKey: string, match: unknown, includeIds?: readonly string[]): {
    rows: RowStream<Row>; complete: boolean;
  };
};

/** The same admitted prediction chains as the synchronous reader. Only changed
 * IDs are tracked, so scanning a large source does not create a corpus ID set.
 * The caller owns one canonical storage transaction for the entire operation. */
export async function readReplicaLiveQueryAsync(source: AsyncReplicaLiveQuerySource, input: ReplicaLiveQueryRequest,
  budget: { candidateBytes: number; examined: number }): Promise<OfflineLiveQueryResult<Row>> {
  const { plan, coverage, patches, overlay, admittedRow, visit, predictionBytes, sliceIncomplete } = prepareReplicaQuery(source, input, budget);
  const changedIds = new Map([...patches].map(([table, changes]) => [table, [...changes.keys()]]));
  let orderedColumn = source.rootRows ? source.rootDistinctOrder : undefined, predictionRowsBytes = 0;
  const orderedPredictions: Row[] = [];
  if (orderedColumn !== undefined && patches.get(plan.table)?.size) {
    if (!source.rootBase) orderedColumn = undefined;
    else for (const id of patches.get(plan.table)!.keys()) {
      const row = overlay(plan.table, id, await source.rootBase(id));
      if (!row) continue;
      if (row[orderedColumn] != null && typeof row[orderedColumn] !== 'string') { orderedColumn = undefined; break; }
      predictionRowsBytes += JSON.stringify(row).length * 2 + 256;
      if (predictionBytes + predictionRowsBytes > budget.candidateBytes) throw new LocalQueryBudgetError('candidateByteBudget', 'Local view candidate byte budget');
      orderedPredictions.push(row);
    }
    if (orderedColumn === undefined) { orderedPredictions.length = 0; predictionRowsBytes = 0; }
    else orderedPredictions.sort((a, b) => { visit(); return String(a[orderedColumn!] ?? '') < String(b[orderedColumn!] ?? '') ? -1 : String(a[orderedColumn!] ?? '') > String(b[orderedColumn!] ?? '') ? 1 : 0; });
  }
  async function* rows(table: string, root = false, selected?: RowStream<Row>): AsyncGenerator<Row> {
    const key = source.key(table); if (!key) return;
    const changed = patches.get(table), seen = new Set<string>();
    for await (const stored of selected ?? (root ? source.rootRows?.() ?? source.rows(table) : source.rows(table))) {
      visit(); const id = String(stored[key]);
      if (!changed?.has(id)) { const row = admittedRow(table, stored); if (row) yield row; continue; }
      seen.add(id); const row = overlay(table, id, stored); if (row) yield row;
    }
    for (const id of changed?.keys() ?? []) if (!seen.has(id)) {
      const row = overlay(table, id, root ? await source.rootBase?.(id) : undefined); if (row) yield row;
    }
  }
  if (!input.view) {
    const { complete, references } = generatedReplicaCoverage(source, input);
    let referenceIncomplete = false;
    const result = await runOfflineLiveQueryAsync(rows(plan.table, true), plan, input.args,
      complete(plan.table, plan.where) ? 'complete' : 'partial', {
        context: input.context,
        referenceRow: async (table, id) => {
          visit(); const row = source.key(table) ? overlay(table, id, await source.get(table, id)) : undefined;
          if (row && [...references.get(table) ?? []].some(column => !Object.hasOwn(row, column))) referenceIncomplete = true;
          return row;
        },
        relationRows: (table, where, foreignKey, match) => {
          const selected = foreignKey === undefined ? undefined : source.relatedRows?.(table, foreignKey, match, changedIds.get(table));
          return { rows: rows(table, false, selected?.rows), completeness: complete(table, where) && selected?.complete !== false ? 'complete' : 'partial' };
        },
      }, { candidateBytes: budget.candidateBytes - predictionBytes });
    if (referenceIncomplete || sliceIncomplete()) result.completeness = 'partial';
    return result;
  }
  async function* orderedRoots(): AsyncGenerator<Row> {
    let next = 0;
    for await (const stored of source.rootRows!()) {
      visit(); const id = String(stored[plan.key]);
      if (patches.get(plan.table)?.has(id)) continue;
      const row = admittedRow(plan.table, stored); if (!row) continue;
      while (next < orderedPredictions.length && String(orderedPredictions[next]![orderedColumn!] ?? '') <= String(row[orderedColumn!])) yield orderedPredictions[next++]!;
      yield row;
    }
    while (next < orderedPredictions.length) yield orderedPredictions[next++]!;
  }
  const overlaid: AsyncLocalViewSource = { key: source.key, rows,
    rootRows: () => rows(plan.table, true, source.rootDistinctOrder !== undefined ? source.rows(plan.table) : undefined),
    rootOrder: source.rootOrder,
    get: (table, id) => { visit(); return mapResult(source.get(table, id), row => overlay(table, id, row)); },
    prepareRelated: source.prepareRelated,
    streamRelatedExistence: source.streamRelatedExistence,
    ...(source.relatedRows ? { relatedRows: (table: string, foreignKey: string, match: unknown) => {
      const selected = source.relatedRows!(table, foreignKey, match, changedIds.get(table));
      return { complete: selected.complete, rows: rows(table, false, selected.rows) };
    } } : {}),
    ...(orderedColumn !== undefined ? { rootDistinctOrder: orderedColumn, rootRows: orderedRoots,
      // orderedRoots also merges every admitted pending row, including null
      // key moves/inserts. Only omitted canonical roots need another pass.
      ...(!source.rootDistinctExhaustive ? { rootValidationRows: () => rows(plan.table, false, source.rows(plan.table)) } : {}) } : {}),
  };
  const result = await readLocalViewAsync(overlaid, plan.table, plan.key, input.view, coverage, input.context,
    { candidateBytes: budget.candidateBytes - predictionBytes - predictionRowsBytes, visit }, plan.columns);
  if (sliceIncomplete()) result.completeness = 'partial';
  return result;
}

/** Application LocalView callers keep their explicit plan contract. */
export async function readReplicaLocalViewAsync(source: AsyncReplicaLiveQuerySource, input: ReplicaLiveQueryRequest,
  budget: { candidateBytes: number; examined: number }): Promise<OfflineLiveQueryResult<Row>> {
  if (!input.view) throw Error('Asynchronous local view requires a view plan');
  return readReplicaLiveQueryAsync(source, input, budget);
}
