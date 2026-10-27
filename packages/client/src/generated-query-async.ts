import type { JsonValue } from '@gonvex/protocol';
import { LocalQueryBudgetError } from './local-query-budget.js';
import type { MaybePromise, RowStream } from './local-view-stream.js';
import { compareLiveQueryValues as compare, evaluateLiveExpression, nonNegativeInteger, smallestRowSelection, structuredFilterPredicate,
  type LiveQueryExpression, type LiveQueryPlan, type LiveQueryValue, type OfflineLiveQueryResult } from './query-expression.js';

type Row = Record<string, unknown>;
export type AsyncGeneratedQueryRead = {
  context?: Readonly<Record<string, JsonValue>>;
  referenceRow?(table: string, id: string): MaybePromise<Row | undefined>;
  relationRows?(table: string, where: LiveQueryExpression | undefined, foreignKey?: string, match?: unknown): {
    rows: RowStream<Row>; completeness: 'complete' | 'partial';
  };
};

/** Generated-plan semantics over a captured disk stream. Relation membership
 * uses a short-circuiting stream instead of retaining all foreign keys. An
 * indexed provider may seek by foreignKey/match without changing the plan. */
export async function runOfflineLiveQueryAsync<T extends Row>(rows: RowStream<T>, plan: LiveQueryPlan,
  args: Record<string, JsonValue>, completeness: 'complete' | 'partial', read: AsyncGeneratedQueryRead,
  budget: { candidateBytes: number }): Promise<OfflineLiveQueryResult<T>> {
  if (!Number.isSafeInteger(budget.candidateBytes) || budget.candidateBytes < 0) throw Error('Invalid Live Query candidate budget');
  let retained = 0, unsupported: string | undefined;
  const reserve = (bytes: number) => {
    retained += bytes;
    if (retained > budget.candidateBytes) throw new LocalQueryBudgetError('candidateByteBudget', 'Live Query candidate byte budget');
  };
  const cost = (value: unknown) => JSON.stringify(value).length * 2 + 256;
  const resolve = (value: LiveQueryValue | undefined): LiveQueryValue | undefined => {
    if (!value?.context) return value;
    const found = read.context?.[value.context]; if (found === undefined) unsupported ??= 'missingIdentityContext';
    return { literal: found ?? null };
  };
  const prepare = (expression: LiveQueryExpression | undefined): LiveQueryExpression | undefined => {
    if (!expression) return;
    const result = { ...expression, value: resolve(expression.value), valueTo: resolve(expression.valueTo),
      children: expression.children?.map(child => prepare(child)!) };
    if (expression.operator === 'server') unsupported ??= 'server';
    if (expression.operator === 'inRelation') {
      const relation = expression.relation, where = prepare(relation?.where);
      const related = relation && read.relationRows?.(relation.table, where);
      if (!related || !relation) unsupported ??= 'missingRelation';
      else {
        if (related.completeness !== 'complete') completeness = 'partial';
        result.relation = { ...relation, where };
      }
    }
    return result;
  };
  const where = prepare(plan.where); unsupported ??= plan.serverOnly ? 'serverOnly' : undefined;
  if (unsupported) return { rows: [], completeness, supported: false, unsupportedOperator: unsupported };
  const filters = structuredFilterPredicate(plan.filters, args);
  if (!filters.supported) return { rows: [], completeness, supported: false, unsupportedOperator: filters.reason };
  const matches = async (expression: LiveQueryExpression, row: Row): Promise<boolean> => {
    if (expression.operator === 'and' || expression.operator === 'or') {
      const all = expression.operator === 'and';
      for (const child of expression.children ?? []) if (await matches(child, row) !== all) return !all;
      return all;
    }
    if (expression.operator === 'not') return expression.children?.length === 1 && !await matches(expression.children[0]!, row);
    if (expression.operator !== 'inRelation') return evaluateLiveExpression(expression, row, args);
    const relation = expression.relation!, left = expression.column ? row[expression.column] : undefined;
    if (left == null) return false;
    const related = read.relationRows!(relation.table, relation.where, relation.column, left);
    if (related.completeness !== 'complete') completeness = 'partial';
    for await (const candidate of related.rows) if (candidate[relation.column] != null && String(candidate[relation.column]) === String(left)
      && (!relation.where || await matches(relation.where, candidate))) return true;
    return false;
  };
  const search = plan.search ? String(args[plan.search.argument] ?? '').trim().toLocaleLowerCase() : '';
  const separator = plan.search?.booleanTerms && search.includes('$and') ? '$and' : plan.search?.booleanTerms && search.includes('$or') ? '$or' : '';
  const terms = (separator ? search.split(separator) : [search]).map(term => term.trim()).filter(Boolean);
  const columns = plan.search?.offlineColumns ?? plan.search?.columns ?? [];
  if (search && plan.search?.sources?.length) completeness = 'partial';
  const offset = plan.window ? nonNegativeInteger(args[plan.window.offsetArgument], 0) : undefined;
  const requested = plan.window ? nonNegativeInteger(args[plan.window.limitArgument], plan.window.defaultLimit) : undefined;
  const limit = plan.window ? Math.min(requested || plan.window.defaultLimit, plan.window.maxLimit || requested || plan.window.defaultLimit) : undefined;
  const requestedColumn = plan.sort?.columnArgument ? String(args[plan.sort.columnArgument] ?? '') : '';
  const column = plan.sort && (plan.sort.allowedColumns.includes(requestedColumn) ? requestedColumn : plan.sort.defaultColumn);
  const requestedDirection = plan.sort?.directionArgument ? String(args[plan.sort.directionArgument] ?? '').toLocaleLowerCase() : '';
  const direction = requestedDirection === 'asc' || requestedDirection === 'desc' ? requestedDirection : plan.sort?.defaultDirection;
  const fields = column ? plan.index?.sortColumns?.[column] ?? [column] : [];
  type Ordered = { row: T; values: unknown[] };
  const comparator = (a: Ordered, b: Ordered) => {
    for (let index = 0; index < fields.length; index++) {
      const left = a.values[index], right = b.values[index];
      const order = left == null ? right == null ? 0 : 1 : right == null ? -1 : compare(left, right);
      if (order) return direction === 'desc' ? -order : order;
    }
    return compare(a.row[plan.key], b.row[plan.key]);
  };
  const selection = plan.sort && plan.window ? smallestRowSelection<Ordered>(offset! + limit!, comparator, cost, reserve) : undefined;
  const ordered: Ordered[] = [], result: T[] = []; let matched = 0, missingSortField = false;
  for await (const row of rows) {
    if (where && !await matches(where, row) || !filters.matches(row)) continue;
    if (search && plan.search) {
      const found = (term: string) => columns.some(column => String(row[column] ?? '').toLowerCase().includes(term));
      if (!(separator === '$or' ? terms.some(found) : terms.every(found))) continue;
    }
    matched++;
    if (!plan.sort) {
      if (!plan.window || matched > offset! && matched <= offset! + limit!) { reserve(cost(row)); result.push(row); }
      continue;
    }
    const values: unknown[] = [];
    for (const field of fields) {
      const reference = plan.index?.referenceFields?.[field];
      if (!reference) { if (!Object.hasOwn(row, field)) missingSortField = true; values.push(row[field]); }
      else {
        const id = row[reference.foreignKey], related = id == null ? undefined : await read.referenceRow?.(reference.table, String(id));
        if (id != null && !related) completeness = 'partial';
        values.push(related?.[reference.column] ?? reference.fallback ?? null);
      }
    }
    const entry = { row, values };
    if (selection) selection.add(entry); else { reserve(cost(entry)); ordered.push(entry); }
  }
  if (missingSortField) return { rows: [], completeness: 'partial', supported: false, unsupportedOperator: 'missingSortField' };
  if (plan.sort) for (const value of selection ? selection.finish().slice(offset, offset! + limit!) : ordered.sort(comparator)) result.push(value.row);
  const total = plan.window?.count === 'exact' ? matched : undefined;
  return { rows: result, ...(total === undefined ? {} : { total, offset, limit }), completeness, supported: true };
}
