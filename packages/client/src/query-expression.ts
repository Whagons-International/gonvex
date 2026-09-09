import type { JsonValue } from "@gonvex/protocol";
import type { DataPredicate } from "@gonvex/module-sdk";

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

  let result = where ? rows.filter((row) => evaluateLiveExpression(where, row, args)) : [...rows];
  const filterResult = applyStructuredFilters(result, plan.filters, args);
  if (!filterResult.supported) return { rows: [], completeness, supported: false, unsupportedOperator: filterResult.reason };
  result = filterResult.rows;
  const search = plan.search ? String(args[plan.search.argument] ?? "").trim().toLocaleLowerCase() : "";
  if (search && plan.search) {
    const separator = plan.search.booleanTerms && search.includes('$and') ? '$and' : plan.search.booleanTerms && search.includes('$or') ? '$or' : '';
    const terms = (separator ? search.split(separator) : [search]).map(term=>term.trim()).filter(Boolean);
    const columns = plan.search.offlineColumns ?? plan.search.columns;
    result = result.filter(row => {
      const matches = terms.map(term => columns.some(column => String(row[column] ?? '').toLowerCase().includes(term)));
      return separator === '$or' ? matches.some(Boolean) : matches.every(Boolean);
    });
    // A partial working set cannot prove matches in server-only search documents.
    if (plan.search.sources?.length) completeness = 'partial';
  }
  if (plan.sort) {
    const requestedColumn = plan.sort.columnArgument ? String(args[plan.sort.columnArgument] ?? "") : "";
    const column = plan.sort.allowedColumns.includes(requestedColumn) ? requestedColumn : plan.sort.defaultColumn;
    const requestedDirection = plan.sort.directionArgument ? String(args[plan.sort.directionArgument] ?? "").toLocaleLowerCase() : "";
    const direction = requestedDirection === "asc" || requestedDirection === "desc" ? requestedDirection : plan.sort.defaultDirection;
    const fields = plan.index?.sortColumns?.[column] ?? [column];
    const resolved = new Map<T, unknown[]>();
    for (const row of result) {
      resolved.set(row,fields.map(field=>{
        const reference=plan.index?.referenceFields?.[field];
        if(!reference)return row[field];
        const id=row[reference.foreignKey];
        const related=id == null ? undefined : referenceRow?.(reference.table,String(id));
        if(id!=null && !related)completeness='partial';
        return related?.[reference.column] ?? reference.fallback ?? null;
      }));
    }
    if(fields.some(field=>!plan.index?.referenceFields?.[field] && result.some(row=>!Object.hasOwn(row,field))))return {rows:[],completeness:'partial',supported:false,unsupportedOperator:'missingSortField'};
    result = [...result].sort((left, right) => {
      for(let i=0;i<fields.length;i++){
        const a=resolved.get(left)![i],b=resolved.get(right)![i];
        // PostgreSQL ASC puts NULL last; DESC puts it first.
        const order=a==null ? b==null ? 0 : 1 : b==null ? -1 : compare(a,b);
        if(order)return direction==='desc'?-order:order;
      }
      return compare(left[plan.key],right[plan.key]);
    });
  }
  const total = plan.window?.count === "exact" ? result.length : undefined;
  let offset: number | undefined;
  let limit: number | undefined;
  if (plan.window) {
    offset = nonNegativeInteger(args[plan.window.offsetArgument], 0);
    const requestedLimit = nonNegativeInteger(args[plan.window.limitArgument], plan.window.defaultLimit);
    limit = Math.min(requestedLimit || plan.window.defaultLimit, plan.window.maxLimit || requestedLimit || plan.window.defaultLimit);
    result = result.slice(offset, offset + limit);
  }
  return {
    rows: result,
    ...(total === undefined ? {} : { total }),
    ...(total === undefined || offset === undefined ? {} : { offset }),
    ...(total === undefined || limit === undefined ? {} : { limit }),
    completeness,
    supported: true,
  };
}

function applyStructuredFilters<T extends Record<string, unknown>>(
  rows: readonly T[],
  definition: LiveQueryPlan["filters"],
  args: Record<string, JsonValue>,
): { rows: T[]; supported: true } | { rows: T[]; supported: false; reason: string } {
  if (!definition) return { rows: [...rows], supported: true };
  const raw = args[definition.argument];
  if (raw === undefined || raw === null) return { rows: [...rows], supported: true };
  if (!Array.isArray(raw)) return { rows: [], supported: false, reason: "invalidFilter" };
  const allowedColumns = new Set(definition.allowedColumns);
  const allowedOperators = new Set(definition.allowedOperators);
  const filters: Array<{ column: string; operator: FilterOperator; value: string; valueTo?: string }> = [];
  for (const candidate of raw) {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) return { rows: [], supported: false, reason: "invalidFilter" };
    const filter = candidate as Record<string, unknown>;
    if (typeof filter.column !== "string" || !allowedColumns.has(filter.column) || typeof filter.operator !== "string" || !allowedOperators.has(filter.operator as FilterOperator) || typeof filter.value !== "string" || (filter.valueTo !== undefined && typeof filter.valueTo !== "string")) {
      return { rows: [], supported: false, reason: "invalidFilter" };
    }
    filters.push({ column: filter.column, operator: filter.operator as FilterOperator, value: filter.value, ...(filter.valueTo === undefined ? {} : { valueTo: filter.valueTo }) });
  }
  return { rows: rows.filter((row) => filters.every((filter) => evaluateFilter(row[filter.column], filter))), supported: true };
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
