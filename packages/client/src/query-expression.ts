import type { JsonValue } from "@gonvex/protocol";

export type LiveQueryValue = { argument?: string; literal?: JsonValue };
export type FilterOperator = "contains" | "notContains" | "equals" | "notEquals" | "startsWith" | "endsWith" | "empty" | "notEmpty" | "oneOf" | "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" | "inRange";

export type LiveQueryExpression = {
  operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "range" | "in" | "contains" | "containsInsensitive" | "and" | "or" | "not" | "server";
  column?: string;
  value?: LiveQueryValue;
  valueTo?: LiveQueryValue;
  children?: readonly LiveQueryExpression[];
};

/** The exact structured plan compiled to SQL by the Gonvex server. */
export type LiveQueryPlan = {
  table: string;
  key: string;
  columns?: readonly string[];
  resultPath?: readonly string[];
  where?: LiveQueryExpression;
  search?: { argument: string; columns: readonly string[] };
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

/** Execute the server-issued Live Query AST against the bounded cached corpus. */
export function runOfflineLiveQuery<T extends Record<string, unknown>>(
  rows: readonly T[],
  plan: LiveQueryPlan,
  args: Record<string, JsonValue>,
  completeness: "complete" | "partial",
): OfflineLiveQueryResult<T> {
  const unsupported = plan.serverOnly ? "serverOnly" : firstUnsupported(plan.where);
  if (unsupported) return { rows: [], completeness, supported: false, unsupportedOperator: unsupported };

  let result = plan.where ? rows.filter((row) => evaluateLiveExpression(plan.where!, row, args)) : [...rows];
  const filterResult = applyStructuredFilters(result, plan.filters, args);
  if (!filterResult.supported) return { rows: [], completeness, supported: false, unsupportedOperator: filterResult.reason };
  result = filterResult.rows;
  const search = plan.search ? String(args[plan.search.argument] ?? "").trim().toLocaleLowerCase() : "";
  if (search && plan.search) {
    result = result.filter((row) => plan.search!.columns.some((column) => String(row[column] ?? "").toLocaleLowerCase().includes(search)));
  }
  if (plan.sort) {
    const requestedColumn = plan.sort.columnArgument ? String(args[plan.sort.columnArgument] ?? "") : "";
    const column = plan.sort.allowedColumns.includes(requestedColumn) ? requestedColumn : plan.sort.defaultColumn;
    const requestedDirection = plan.sort.directionArgument ? String(args[plan.sort.directionArgument] ?? "").toLocaleLowerCase() : "";
    const direction = requestedDirection === "asc" || requestedDirection === "desc" ? requestedDirection : plan.sort.defaultDirection;
    result = [...result].sort((left, right) => direction === "desc" ? -compare(left[column], right[column]) : compare(left[column], right[column]));
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
