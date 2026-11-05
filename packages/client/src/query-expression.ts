import type { JsonValue } from "@gonvex/protocol";
import type { DataPredicate } from "@gonvex/module-sdk";
import { QueryRowWindow } from "./query-row-window.js";

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
  offset?: number;
  limit?: number;
  completeness: "complete" | "partial";
  supported: boolean;
  unsupportedOperator?: string;
};

const relatedMemberships = new WeakMap<LiveQueryExpression, ReadonlySet<string>>();

export type RelatedQueryRead = {
  context?: Readonly<Record<string, JsonValue>>;
  relationRows?: (table: string, where: LiveQueryExpression | undefined) => { rows: readonly Record<string, unknown>[]; completeness: "complete" | "partial" };
};

/** Lower each related scope once, avoiding an N-by-M scan for every root row. */
function resolveRelatedScope(expression: LiveQueryExpression | undefined, args: Record<string, JsonValue>, read: RelatedQueryRead,
  state: { incomplete: boolean; unsupported?: string }): LiveQueryExpression | undefined {
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
      if ((!where || evaluateLiveExpression(where, row, args)) && row[relation.column] != null) keys.add(String(row[relation.column]));
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
  rows: readonly T[],
  plan: LiveQueryPlan,
  args: Record<string, JsonValue>,
  completeness: "complete" | "partial",
  referenceRow?: (table:string,id:string)=>Record<string,unknown>|undefined,
  relatedRead: RelatedQueryRead = {},
): OfflineLiveQueryResult<T> {
  const state: { incomplete: boolean; unsupported?: string } = { incomplete: false };
  const where = resolveRelatedScope(plan.where, args, relatedRead, state);
  const unsupported = state.unsupported ?? (plan.serverOnly ? "serverOnly" : firstUnsupported(where));
  if (state.incomplete) completeness = 'partial';
  if (unsupported) return { rows: [], completeness, supported: false, unsupportedOperator: unsupported };

  const filterResult = compileStructuredFilters(plan.filters, args);
  if (!filterResult.supported) return { rows: [], completeness, supported: false, unsupportedOperator: filterResult.reason };
  const filters = filterResult.filters;
  const search = plan.search ? String(args[plan.search.argument] ?? "").trim().toLocaleLowerCase() : "";
  const separator = search && plan.search?.booleanTerms && search.includes('$and') ? '$and'
    : search && plan.search?.booleanTerms && search.includes('$or') ? '$or' : '';
  const terms: string[] = [];
  if (search && !separator) terms.push(search);
  else if (search) for (const part of search.split(separator)) {
    const term = part.trim();
    if (term) terms.push(term);
  }
  const searchColumns = plan.search?.offlineColumns ?? plan.search?.columns ?? [];
  // A partial working set cannot prove matches in server-only search documents.
  if (search && plan.search?.sources?.length) completeness = 'partial';

  const offset = plan.window ? nonNegativeInteger(args[plan.window.offsetArgument], 0) : 0;
  const requestedLimit = plan.window ? nonNegativeInteger(args[plan.window.limitArgument], plan.window.defaultLimit) : rows.length;
  const limit = plan.window ? Math.min(requestedLimit || plan.window.defaultLimit,
    plan.window.maxLimit || requestedLimit || plan.window.defaultLimit) : rows.length;
  const take = Math.min(rows.length, offset + limit);
  let fields: readonly string[] = [];
  let direction = 1;
  if (plan.sort) {
    const requestedColumn = plan.sort.columnArgument ? String(args[plan.sort.columnArgument] ?? "") : "";
    const column = plan.sort.allowedColumns.includes(requestedColumn) ? requestedColumn : plan.sort.defaultColumn;
    const requestedDirection = plan.sort.directionArgument ? String(args[plan.sort.directionArgument] ?? "").toLocaleLowerCase() : "";
    direction = (requestedDirection === "asc" || requestedDirection === "desc" ? requestedDirection : plan.sort.defaultDirection) === 'desc' ? -1 : 1;
    fields = plan.index?.sortColumns?.[column] ?? [column];
  }
  // Reference keys are resolved once for each matching candidate. Only retained
  // heap slots own key arrays; this scratch array is reused across the scan.
  const references = fields.map(field => plan.index?.referenceFields?.[field]);
  const needsReferenceValues = references.some(reference => reference !== undefined);
  const values: unknown[] | undefined = needsReferenceValues ? new Array(fields.length) : undefined;
  const compareRows = (left: T, right: T, leftValues?: readonly unknown[], rightValues?: readonly unknown[]) => {
    for (let i = 0; i < fields.length; i++) {
      const a = leftValues ? leftValues[i] : left[fields[i]!];
      const b = rightValues ? rightValues[i] : right[fields[i]!];
      // Server SQL orders ASC NULLS LAST and DESC NULLS FIRST.
      const order = a == null ? b == null ? 0 : 1 : b == null ? -1 : compare(a, b);
      if (order) return direction * order;
    }
    return compare(left[plan.key], right[plan.key]);
  };
  // A full direct-field sort already needs its output array. Preserve native
  // stable sorting there rather than allocating numeric heap metadata for it.
  const window = plan.sort && (needsReferenceValues || take < rows.length) ? new QueryRowWindow<T>(take, compareRows) : undefined;
  let result: T[] = [];
  let total = 0;
  let missingSortField = false;
  for (let position = 0; position < rows.length; position++) {
    const row = rows[position]!;
    if (where && !evaluateLiveExpression(where, row, args)) continue;
    let matches = true;
    for (const filter of filters) if (!evaluateFilter(row[filter.column], filter)) { matches = false; break; }
    if (!matches || search && !matchesSearch(row, terms, searchColumns, separator === '$or')) continue;
    const matchPosition = total++;
    if (plan.sort) {
      for (let i = 0; i < fields.length; i++) {
        const field = fields[i]!;
        const reference = references[i];
        if (reference) {
          const id = row[reference.foreignKey];
          const related = id == null ? undefined : referenceRow?.(reference.table, String(id));
          if (id != null && !related) completeness = 'partial';
          values![i] = related?.[reference.column] ?? reference.fallback ?? null;
        } else {
          if (!Object.hasOwn(row, field)) missingSortField = true;
          if (values) values[i] = row[field];
        }
      }
      if (window) window.add(row, values);
      else result.push(row);
    } else if (matchPosition >= offset && matchPosition < offset + limit) result.push(row);
  }
  // Every match participates in validation and counting, including candidates
  // outside the result window. A bounded projection cannot prove completeness.
  if (missingSortField) return { rows: [], completeness: 'partial', supported: false, unsupportedOperator: 'missingSortField' };
  if (window) result = window.orderedRows(offset);
  else if (plan.sort) {
    result.sort(compareRows);
    if (plan.window) result = result.slice(offset, offset + limit);
  }
  return {
    rows: result,
    ...(plan.window?.count === 'exact' ? { total, offset, limit } : {}),
    completeness,
    supported: true,
  };
}

function matchesSearch(row: Record<string, unknown>, terms: readonly string[], columns: readonly string[], disjunction: boolean): boolean {
  for (const term of terms) {
    let matches = false;
    for (const column of columns) if (String(row[column] ?? '').toLowerCase().includes(term)) { matches = true; break; }
    if (matches === disjunction) return disjunction;
  }
  return !disjunction;
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

type StructuredFilter = { column: string; operator: FilterOperator; value: string; valueTo?: string; choices?: ReadonlySet<string> };
const INVALID_FILTERS = { supported: false, reason: 'invalidFilter' } as const;
function compileStructuredFilters(
  definition: LiveQueryPlan["filters"],
  args: Record<string, JsonValue>,
): { filters: StructuredFilter[]; supported: true } | { supported: false; reason: string } {
  const filters: StructuredFilter[] = [];
  if (!definition) return { filters, supported: true };
  const raw = args[definition.argument];
  if (raw === undefined || raw === null) return { filters, supported: true };
  if (!Array.isArray(raw)) return INVALID_FILTERS;
  const allowedColumns = new Set(definition.allowedColumns);
  const allowedOperators = new Set(definition.allowedOperators);
  for (const candidate of raw) {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) return INVALID_FILTERS;
    const filter = candidate as Record<string, unknown>;
    if (typeof filter.column !== "string" || !allowedColumns.has(filter.column) || typeof filter.operator !== "string" || !allowedOperators.has(filter.operator as FilterOperator) || typeof filter.value !== "string" || (filter.valueTo !== undefined && typeof filter.valueTo !== "string")) {
      return INVALID_FILTERS;
    }
    // allocation-allow loop-allocation: one prepared record per query filter replaces per-row filter parsing and never scales with corpus size
    const prepared: StructuredFilter = { column: filter.column, operator: filter.operator as FilterOperator, value: filter.value, ...(filter.valueTo === undefined ? {} : { valueTo: filter.valueTo }) };
    if (prepared.operator === 'oneOf') {
      // allocation-allow loop-allocation: query arguments own one reusable choice set per filter; the row scan performs only membership lookups
      const choices = new Set<string>();
      try {
        const parsed: unknown = JSON.parse(prepared.value);
        if (Array.isArray(parsed)) for (const value of parsed) choices.add(String(value));
      } catch { /* Invalid choices match no rows, as on the unprepared path. */ }
      prepared.choices = choices;
    }
    filters.push(prepared);
  }
  return { filters, supported: true };
}

function evaluateFilter(left: unknown, filter: StructuredFilter): boolean {
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
    case "oneOf": return filter.choices!.has(text);
    case "lessThan": return compare(left, comparisonValue) < 0;
    case "lessThanOrEqual": return compare(left, comparisonValue) <= 0;
    case "greaterThan": return compare(left, comparisonValue) > 0;
    case "greaterThanOrEqual": return compare(left, comparisonValue) >= 0;
    case "inRange": return compare(left, comparisonValue) >= 0 && compare(left, comparisonValueTo) <= 0;
  }
}

const EMPTY_EXPRESSIONS: readonly LiveQueryExpression[] = Object.freeze([]);

export function evaluateLiveExpression(
  expression: LiveQueryExpression,
  row: Record<string, unknown>,
  args: Record<string, JsonValue>,
): boolean {
  const children = expression.children ?? EMPTY_EXPRESSIONS;
  if (expression.operator === "and") {
    for (const child of children) if (!evaluateLiveExpression(child, row, args)) return false;
    return true;
  }
  if (expression.operator === "or") {
    for (const child of children) if (evaluateLiveExpression(child, row, args)) return true;
    return false;
  }
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

function nonNegativeInteger(value: JsonValue | undefined, fallback: number) {
  const parsed = typeof value === "number" ? value : typeof value === "string" ? Number.parseInt(value, 10) : Number.NaN;
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : fallback;
}

function compare(left: unknown, right: unknown) {
  if (left === right) return 0;
  if (left === null || left === undefined) return -1;
  if (right === null || right === undefined) return 1;
  if (typeof left === "number" && typeof right === "number") return left - right;
  return String(left).localeCompare(String(right));
}

function equal(left: unknown, right: unknown) {
  return left === right || (left !== null && right !== null && typeof left === "object" && typeof right === "object" && JSON.stringify(left) === JSON.stringify(right));
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
