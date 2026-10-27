import type { JsonValue } from "@gonvex/protocol";
import type { DataPredicate } from "@gonvex/module-sdk";
import { LocalQueryBudgetError } from './local-query-budget.js';

export type LiveQueryValue = { context?: "account.id" | "member.id" | "tenant.id"; argument?: string; literal?: JsonValue };
export type FilterOperator = "contains" | "notContains" | "equals" | "notEquals" | "startsWith" | "endsWith" | "empty" | "notEmpty" | "oneOf" | "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" | "inRange";

export type LiveQueryExpression = {
  operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "range" | "in" | "contains" | "containsInsensitive" | "and" | "or" | "not" | "server" | "inRelation" | "arrayContains";
  column?: string;
  value?: LiveQueryValue;
  valueTo?: LiveQueryValue;
  children?: readonly LiveQueryExpression[];
  relation?: { table: string; column: string; where?: LiveQueryExpression };
};

/** The exact structured plan compiled to SQL by the Gonvex server. */
export type LiveQueryPlan = {
  table: string;
  key: string;
  columns?: readonly string[];
  resultPath?: readonly string[];
  where?: LiveQueryExpression;
  search?: { argument: string; columns: readonly string[]; booleanTerms?: boolean; sources?: readonly {table:string;key:string;column:string;dependencies?:readonly string[]}[]; offlineColumns?:readonly string[] };
  index?: {table:string;key:string;columns:readonly string[];sortColumns?:Readonly<Record<string,readonly string[]>>;dependencies?:readonly string[];referenceFields?:Readonly<Record<string,{table:string;foreignKey:string;column:string;fallback?:JsonValue}>>};
  filters?: { argument: string; allowedColumns: readonly string[]; allowedOperators: readonly FilterOperator[]; columnTypes?: Readonly<Record<string, "text" | "number">> };
  sort?: {
    columnArgument?: string;
    directionArgument?: string;
    allowedColumns: readonly string[];
    defaultColumn: string;
    defaultDirection: "asc" | "desc";
  };
  window?: {
    offsetArgument: string;
    limitArgument: string;
    defaultLimit: number;
    maxLimit: number;
    count?: "exact";
  };
  serverOnly?: boolean;
};

export type OfflineLiveQueryResult<T> = {
  rows: T[];
  /** Count of matching cached rows before the requested window, when exact counting was requested. */
  total?: number;
  /** Local-view group counts across all matching rows, before pagination. */
  groups?: { key: string; count: number }[];
  /** Data-relative ranks for a local view that requests sparse windows. */
  positions?: number[];
  /** Bounded local-view output without hydrating canonical entity bodies. */
  projected?: { id: string; values: JsonValue[] }[];
  offset?: number;
  limit?: number;
  completeness: "complete" | "partial";
  supported: boolean;
  unsupportedOperator?: string;
};

const relatedMemberships = new WeakMap<LiveQueryExpression, ReadonlySet<string>>();

export type RelatedQueryRead = {
  context?: Readonly<Record<string, JsonValue>>;
  relationRows?: (table: string, where: LiveQueryExpression | undefined) => { rows: Iterable<Record<string, unknown>>; completeness: "complete" | "partial" };
};

/** Optional working-memory guard for disk iterators. It accounts for retained
 * relation keys and selected row/sort values, not the caller's source corpus
 * or the browser process. Exhaustion throws before returning any result. */
export type LiveQueryBudget = { candidateBytes: number };

/** Lower each related scope once, avoiding an N-by-M scan for every root row. */
function resolveRelatedScope(expression: LiveQueryExpression | undefined, args: Record<string, JsonValue>, read: RelatedQueryRead,
  state: { incomplete: boolean; unsupported?: string; reserve(bytes: number): void }): LiveQueryExpression | undefined {
  if (!expression) return undefined;
  const resolve = (value: LiveQueryValue | undefined): LiveQueryValue | undefined => {
    if (!value?.context) return value;
    const contextValue = read.context?.[value.context];
    if (contextValue === undefined) state.unsupported = 'missingIdentityContext';
    return { literal: contextValue ?? null };
  };
  if (expression.operator === 'inRelation') {
    const relation = expression.relation;
    const where = resolveRelatedScope(relation?.where, args, read, state);
    const related = relation && read.relationRows?.(relation.table, where);
    if (!related || !relation) { state.unsupported = 'missingRelation'; return expression; }
    if (related.completeness !== 'complete') state.incomplete = true;
    const keys = new Set<string>();
    for (const row of related.rows) {
      if ((!where || evaluateLiveExpression(where, row, args)) && row[relation.column] != null) {
        const key = String(row[relation.column]);
        if (!keys.has(key)) { state.reserve(key.length * 2 + 128); keys.add(key); }
      }
    }
    // The internal membership set is not serialized or retained across evaluations.
    const lowered: LiveQueryExpression = { operator: 'in', column: expression.column };
    relatedMemberships.set(lowered, keys);
    return lowered;
  }
  return { ...expression, value: resolve(expression.value), valueTo: resolve(expression.valueTo),
    ...(expression.children ? { children: expression.children.map(child => resolveRelatedScope(child, args, read, state)!) } : {}) };
}

/** Execute the server-issued Live Query AST against the bounded cached corpus. */
export function runOfflineLiveQuery<T extends Record<string, unknown>>(
  rows: Iterable<T>,
  plan: LiveQueryPlan,
  args: Record<string, JsonValue>,
  completeness: "complete" | "partial",
  referenceRow?: (table:string,id:string)=>Record<string,unknown>|undefined,
  relatedRead: RelatedQueryRead = {},
  budget?: LiveQueryBudget,
): OfflineLiveQueryResult<T> {
  if (budget && (!Number.isSafeInteger(budget.candidateBytes) || budget.candidateBytes < 0)) throw Error('Invalid Live Query candidate budget');
  let retainedBytes = 0;
  const reserve = (bytes: number) => {
    retainedBytes += bytes;
    if (budget && retainedBytes > budget.candidateBytes) throw new LocalQueryBudgetError('candidateByteBudget', 'Live Query candidate byte budget');
  };
  const cost = (value: unknown) => budget ? JSON.stringify(value).length * 2 + 256 : 0;
  const state: { incomplete: boolean; unsupported?: string; reserve(bytes: number): void } = { incomplete: false, reserve };
  const where = resolveRelatedScope(plan.where, args, relatedRead, state);
  const unsupported = state.unsupported ?? (plan.serverOnly ? "serverOnly" : firstUnsupported(where));
  if (state.incomplete) completeness = 'partial';
  if (unsupported) return { rows: [], completeness, supported: false, unsupportedOperator: unsupported };

  const filterResult = structuredFilterPredicate(plan.filters, args);
  if (!filterResult.supported) return { rows: [], completeness, supported: false, unsupportedOperator: filterResult.reason };
  const matchesFilters = filterResult.matches;
  const search = plan.search ? String(args[plan.search.argument] ?? "").trim().toLocaleLowerCase() : "";
  const separator = plan.search?.booleanTerms && search.includes('$and') ? '$and' : plan.search?.booleanTerms && search.includes('$or') ? '$or' : '';
  const terms = (separator ? search.split(separator) : [search]).map(term => term.trim()).filter(Boolean);
  const columns = plan.search?.offlineColumns ?? plan.search?.columns ?? [];
  if (search && plan.search) {
    // A partial working set cannot prove matches in server-only search documents.
    if (plan.search.sources?.length) completeness = 'partial';
  }
  let matched = 0;
  function* candidates() {
    for (const row of rows) {
      if (where && !evaluateLiveExpression(where, row, args) || !matchesFilters(row)) continue;
      if (search && plan.search) {
        const matches = (term: string) => columns.some(column => String(row[column] ?? '').toLowerCase().includes(term));
        if (!(separator === '$or' ? terms.some(matches) : terms.every(matches))) continue;
      }
      matched++;
      yield row;
    }
  }
  const offset = plan.window ? nonNegativeInteger(args[plan.window.offsetArgument], 0) : undefined;
  const requestedLimit = plan.window ? nonNegativeInteger(args[plan.window.limitArgument], plan.window.defaultLimit) : undefined;
  const limit = plan.window ? Math.min(requestedLimit || plan.window.defaultLimit, plan.window.maxLimit || requestedLimit || plan.window.defaultLimit) : undefined;
  let result: T[];
  if (plan.sort) {
    const requestedColumn = plan.sort.columnArgument ? String(args[plan.sort.columnArgument] ?? "") : "";
    const column = plan.sort.allowedColumns.includes(requestedColumn) ? requestedColumn : plan.sort.defaultColumn;
    const requestedDirection = plan.sort.directionArgument ? String(args[plan.sort.directionArgument] ?? "").toLocaleLowerCase() : "";
    const direction = requestedDirection === "asc" || requestedDirection === "desc" ? requestedDirection : plan.sort.defaultDirection;
    const fields = plan.index?.sortColumns?.[column] ?? [column];
    type Ordered = { row: T; values: unknown[] };
    let missingSortField = false;
    function* ordered(): Generator<Ordered> {
      for (const row of candidates()) {
        const values = fields.map(field => {
          const reference = plan.index?.referenceFields?.[field];
          if (!reference) { if (!Object.hasOwn(row, field)) missingSortField = true; return row[field]; }
          const id = row[reference.foreignKey];
          const related = id == null ? undefined : referenceRow?.(reference.table, String(id));
          if (id != null && !related) completeness = 'partial';
          return related?.[reference.column] ?? reference.fallback ?? null;
        });
        yield { row, values };
      }
    }
    const comparator = (left: Ordered, right: Ordered) => {
      for(let i=0;i<fields.length;i++){
        const a=left.values[i],b=right.values[i];
        // PostgreSQL ASC puts NULL last; DESC puts it first.
        const order=a==null ? b==null ? 0 : 1 : b==null ? -1 : compare(a,b);
        if(order)return direction==='desc'?-order:order;
      }
      return compare(left.row[plan.key],right.row[plan.key]);
    };
    const selected = plan.window ? smallestRows(ordered(), offset! + limit!, comparator, cost, reserve) : [...(function* () {
      for (const entry of ordered()) { reserve(cost(entry)); yield entry; }
    })()].sort(comparator);
    if (missingSortField) return { rows: [], completeness: 'partial', supported: false, unsupportedOperator: 'missingSortField' };
    result = (plan.window ? selected.slice(offset, offset! + limit!) : selected).map(entry => entry.row);
  } else {
    result = [];
    // Continue visiting every match for exact counts and coverage, even after
    // the requested page is full. Only the requested rows remain retained.
    for (const row of candidates()) if (!plan.window || matched > offset! && matched <= offset! + limit!) { reserve(cost(row)); result.push(row); }
  }
  const total = plan.window?.count === "exact" ? matched : undefined;
  return {
    rows: result,
    ...(total === undefined ? {} : { total }),
    ...(total === undefined || offset === undefined ? {} : { offset }),
    ...(total === undefined || limit === undefined ? {} : { limit }),
    completeness,
    supported: true,
  };
}

/** A bounded max heap, with input position preserving stable-sort ties. */
export function smallestRows<T>(rows: Iterable<T>, count: number, compareRows: (a: T, b: T) => number,
  cost: (row: T) => number, reserve: (bytes: number) => void): T[] {
  const selection = smallestRowSelection(count, compareRows, cost, reserve);
  for (const row of rows) selection.add(row);
  return selection.finish();
}

/** The same bounded selection for synchronous and asynchronous row streams. */
export function smallestRowSelection<T>(count: number, compareRows: (a: T, b: T) => number,
  cost: (row: T) => number, reserve: (bytes: number) => void) {
  type Entry = { row: T; position: number; cost: number };
  const compare = (a: Entry, b: Entry) => compareRows(a.row, b.row) || a.position - b.position;
  const heap: Entry[] = [];
  let nextPosition = 0;
  const add = (row: T) => {
    const position = nextPosition++;
    if (!count) return;
    if (heap.length < count) {
      const entry = { row, position, cost: cost(row) }; reserve(entry.cost);
      heap.push(entry);
      let i = heap.length - 1;
      while (i > 0) {
        const parent = (i - 1) >>> 1;
        if (compare(heap[parent]!, entry) >= 0) break;
        heap[i] = heap[parent]!; i = parent;
      }
      heap[i] = entry;
    } else if ((compareRows(row,heap[0]!.row) || position - heap[0]!.position) < 0) {
      const entry = { row, position, cost: cost(row) }; reserve(entry.cost - heap[0]!.cost);
      let i = 0;
      while (i * 2 + 1 < heap.length) {
        let child = i * 2 + 1;
        if (child + 1 < heap.length && compare(heap[child + 1]!, heap[child]!) > 0) child++;
        if (compare(entry, heap[child]!) >= 0) break;
        heap[i] = heap[child]!; i = child;
      }
      heap[i] = entry;
    }
  };
  return { add, finish: () => heap.sort(compare).map(entry => entry.row) };
}

/** All tables that can affect evaluation, including reference sorts and coverage. */
export function liveQueryDependencies(plan: LiveQueryPlan): string[] {
  const tables = new Set([plan.table]);
  const visit = (expression?: LiveQueryExpression): void => {
    if (expression?.relation) { tables.add(expression.relation.table); visit(expression.relation.where); }
    expression?.children?.forEach(visit);
  };
  visit(plan.where);
  for (const reference of Object.values(plan.index?.referenceFields ?? {})) tables.add(reference.table);
  for (const table of plan.index?.dependencies ?? []) tables.add(table);
  for (const source of plan.search?.sources ?? []) {
    tables.add(source.table);
    for (const table of source.dependencies ?? []) tables.add(table);
  }
  return [...tables];
}

/** Only a conjunctive equality can safely restrict the candidate corpus. */
export function liveQueryEquality(expression: LiveQueryExpression | undefined, args: Record<string, JsonValue>): { column: string; value: unknown } | undefined {
  if (expression?.operator === 'and') {
    for (const child of expression.children ?? []) {
      const equality = liveQueryEquality(child, args);
      if (equality) return equality;
    }
  }
  if (expression?.operator !== 'eq' || !expression.column || expression.value?.context) return undefined;
  const value = resolveValue(expression.value, args);
  return value === null || ['string', 'number', 'boolean', 'undefined'].includes(typeof value) ? { column: expression.column, value } : undefined;
}

export function structuredFilterPredicate(
  definition: LiveQueryPlan["filters"],
  args: Record<string, JsonValue>,
): { matches(row: Record<string, unknown>): boolean; supported: true } | { supported: false; reason: string } {
  if (!definition) return { matches: () => true, supported: true };
  const raw = args[definition.argument];
  if (raw === undefined || raw === null) return { matches: () => true, supported: true };
  if (!Array.isArray(raw)) return { supported: false, reason: "invalidFilter" };
  const allowedColumns = new Set(definition.allowedColumns);
  const allowedOperators = new Set(definition.allowedOperators);
  const filters: Array<{ column: string; operator: FilterOperator; value: string; valueTo?: string }> = [];
  for (const candidate of raw) {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) return { supported: false, reason: "invalidFilter" };
    const filter = candidate as Record<string, unknown>;
    if (typeof filter.column !== "string" || !allowedColumns.has(filter.column) || typeof filter.operator !== "string" || !allowedOperators.has(filter.operator as FilterOperator) || typeof filter.value !== "string" || (filter.valueTo !== undefined && typeof filter.valueTo !== "string")) {
      return { supported: false, reason: "invalidFilter" };
    }
    filters.push({ column: filter.column, operator: filter.operator as FilterOperator, value: filter.value, ...(filter.valueTo === undefined ? {} : { valueTo: filter.valueTo }) });
  }
  return { matches: row => filters.every(filter => evaluateFilter(row[filter.column], filter)), supported: true };
}

function evaluateFilter(left: unknown, filter: { operator: FilterOperator; value: string; valueTo?: string }): boolean {
  const text = String(left ?? "");
  const value = filter.value;
  const comparisonValue = typeof left === "number" ? Number(value) : value;
  const comparisonValueTo = typeof left === "number" ? Number(filter.valueTo ?? "") : (filter.valueTo ?? "");
  switch (filter.operator) {
    case "contains": return text.toLocaleLowerCase().includes(value.toLocaleLowerCase());
    case "notContains": return !text.toLocaleLowerCase().includes(value.toLocaleLowerCase());
    case "equals": return text === value;
    case "notEquals": return text !== value;
    case "startsWith": return text.toLocaleLowerCase().startsWith(value.toLocaleLowerCase());
    case "endsWith": return text.toLocaleLowerCase().endsWith(value.toLocaleLowerCase());
    case "empty": return left === null || left === undefined || text === "";
    case "notEmpty": return left !== null && left !== undefined && text !== "";
    case "oneOf": {
      try { const choices = JSON.parse(value); return Array.isArray(choices) && choices.some((choice) => String(choice) === text); } catch { return false; }
    }
    case "lessThan": return compare(left, comparisonValue) < 0;
    case "lessThanOrEqual": return compare(left, comparisonValue) <= 0;
    case "greaterThan": return compare(left, comparisonValue) > 0;
    case "greaterThanOrEqual": return compare(left, comparisonValue) >= 0;
    case "inRange": return compare(left, comparisonValue) >= 0 && compare(left, comparisonValueTo) <= 0;
  }
}

export function evaluateLiveExpression(
  expression: LiveQueryExpression,
  row: Record<string, unknown>,
  args: Record<string, JsonValue>,
): boolean {
  const children = expression.children ?? [];
  if (expression.operator === "and") return children.every((child) => evaluateLiveExpression(child, row, args));
  if (expression.operator === "or") return children.some((child) => evaluateLiveExpression(child, row, args));
  if (expression.operator === "not") return children.length === 1 && !evaluateLiveExpression(children[0]!, row, args);
  if (expression.operator === "server") return false;

  const left = expression.column ? row[expression.column] : undefined;
  const right = resolveValue(expression.value, args);
  if (expression.operator === "eq") return equal(left, right);
  if (expression.operator === "neq") return !equal(left, right);
  if (expression.operator === "contains") return String(left ?? "").includes(String(right ?? ""));
  if (expression.operator === "containsInsensitive") return String(left ?? "").toLocaleLowerCase().includes(String(right ?? "").toLocaleLowerCase());
  if (expression.operator === "arrayContains") return Array.isArray(left) && left.some(candidate => equal(candidate, right));
  if (expression.operator === "in" && relatedMemberships.has(expression)) return left != null && relatedMemberships.get(expression)!.has(String(left));
  if (expression.operator === "in") return Array.isArray(right) && right.some((candidate) => equal(left, candidate));
  const comparison = compare(left, right);
  if (expression.operator === "gt") return comparison > 0;
  if (expression.operator === "gte") return comparison >= 0;
  if (expression.operator === "lt") return comparison < 0;
  if (expression.operator === "lte") return comparison <= 0;
  if (expression.operator === "range") return comparison >= 0 && compare(left, resolveValue(expression.valueTo, args)) <= 0;
  return false;
}

function resolveValue(value: LiveQueryValue | undefined, args: Record<string, JsonValue>): unknown {
  if (!value) return undefined;
  if (value.argument) return args[value.argument];
  return value.literal;
}

function firstUnsupported(expression?: LiveQueryExpression): string | undefined {
  if (!expression) return undefined;
  if (expression.operator === "server") return "server";
  for (const child of expression.children ?? []) {
    const unsupported = firstUnsupported(child);
    if (unsupported) return unsupported;
  }
  return undefined;
}

export function nonNegativeInteger(value: JsonValue | undefined, fallback: number) {
  const parsed = typeof value === "number" ? value : typeof value === "string" ? Number.parseInt(value, 10) : Number.NaN;
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : fallback;
}

export function compareLiveQueryValues(left: unknown, right: unknown) {
  if (left === right) return 0;
  if (left === null || left === undefined) return -1;
  if (right === null || right === undefined) return 1;
  if (typeof left === "number" && typeof right === "number") return left - right;
  return String(left).localeCompare(String(right));
}

const compare = compareLiveQueryValues;

function equal(left: unknown, right: unknown) {
  return JSON.stringify(left) === JSON.stringify(right);
}

/** A weaker predicate is safe for coverage: it can only reject a usable slice. */
export function liveScopeCoverage(expression: LiveQueryExpression | undefined, args: Record<string, JsonValue>): DataPredicate | undefined {
  if (!expression) return undefined;
  if (expression.operator === 'and') {
    const and = (expression.children ?? []).map(child => liveScopeCoverage(child, args)).filter((child): child is DataPredicate => !!child);
    return and.length ? { and } : undefined;
  }
  if (expression.operator === 'or') {
    const or = (expression.children ?? []).map(child => liveScopeCoverage(child, args));
    return or.length && or.every((child): child is DataPredicate => !!child) ? { or } : undefined;
  }
  const value = resolveValue(expression.value, args);
  if (!expression.column || value !== null && !['string', 'number', 'boolean'].includes(typeof value)) return undefined;
  if (expression.operator === 'eq' && value === null) return { column: expression.column, op: 'isNull' };
  if (expression.operator === 'neq' && value === null) return { column: expression.column, op: 'notNull' };
  const operators = { eq: 'eq', neq: 'ne', gt: 'gt', gte: 'gte', lt: 'lt', lte: 'lte' } as const;
  const op = operators[expression.operator as keyof typeof operators];
  return op ? { column: expression.column, op, value: value as string | number | boolean | null } : undefined;
}
