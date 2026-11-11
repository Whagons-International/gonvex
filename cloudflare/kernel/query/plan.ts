/**
 * Structured Live Query plans: the `liveQueryPlan` a module declares, with the
 * Rust runtime's validation and error texts (`live_query.rs` 229-517,
 * 1812-1921). The plan is metadata only: the kernel compiles it to SQL itself
 * and injects the table's visibility predicate, so a module cannot bypass row
 * authorization with arbitrary SQL.
 */
import { rustDebug } from "./json";

export interface LiveValue { context?: string; argument?: string; literal?: unknown }
export interface LiveRelation { table: string; column: string; where?: LiveExpression | null }
export interface LiveExpression {
  operator: string;
  column?: string;
  value?: LiveValue | null;
  valueTo?: LiveValue | null;
  children?: LiveExpression[];
  relation?: LiveRelation | null;
}
export interface LiveSearchSource { table: string; key: string; column: string; dependencies?: string[] }
export interface LiveSearch { argument: string; columns: string[]; booleanTerms?: boolean; sources?: LiveSearchSource[] }
export interface LiveIndex { table: string; key: string; columns: string[]; sortColumns?: Record<string, string[]>; dependencies?: string[] }
export interface LiveFilters { argument: string; allowedColumns: string[]; allowedOperators: string[]; columnTypes?: Record<string, string> }
export interface LiveSort { columnArgument?: string; directionArgument?: string; defaultColumn: string; defaultDirection: string; allowedColumns: string[] }
export interface LiveWindow { offsetArgument: string; limitArgument: string; defaultLimit: number; maxLimit: number; count?: string }

/** `LiveQueryPlan` (camelCase, as the manifest carries it). Unknown keys (`referenceFields`, `offlineColumns`) are ignored, as serde does. */
export interface LiveQueryPlan {
  table: string;
  key: string;
  columns?: string[];
  resultPath?: string[];
  where?: LiveExpression | null;
  search?: LiveSearch | null;
  filters?: LiveFilters | null;
  sort?: LiveSort | null;
  window?: LiveWindow | null;
  index?: LiveIndex | null;
  serverOnly?: boolean;
}

/** `LiveQueryError`, with its Display text as `message`. */
export class LiveQueryError extends Error {
  constructor(message: string, readonly kind: "arguments" | "visibility" | "invalid" | "database" = "invalid") {
    super(message);
  }
  static invalid(message: string): LiveQueryError { return new LiveQueryError(`invalid structured query: ${message}`, "invalid"); }
  static notAnObject(): LiveQueryError { return new LiveQueryError("structured query arguments must be an object", "arguments"); }
  static visibilityMissing(table: string): LiveQueryError { return new LiveQueryError(`visibility plan required for table ${rustDebug(table)}`, "visibility"); }
}

/** `quote`: a plain identifier (letters, digits, `_`, not starting with a digit, at most 128 bytes), double-quoted. */
export function quote(value: string): string {
  if (!value || utf8Length(value) > 128 || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw LiveQueryError.invalid(`SQL identifier ${rustDebug(value)} is invalid`);
  }
  return `"${value}"`;
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).length;
}

function requireName(value: string | undefined, label: string): void {
  if (!value || !value.trim()) throw LiveQueryError.invalid(`${label} is required`);
}

const FILTER_OPERATORS = new Set([
  "contains", "notContains", "equals", "notEquals", "startsWith", "endsWith", "empty", "notEmpty", "oneOf",
  "lessThan", "lessThanOrEqual", "greaterThan", "greaterThanOrEqual", "inRange",
]);

export function validFilterOperator(operator: string): boolean {
  return FILTER_OPERATORS.has(operator);
}

/** `LiveQueryPlan::validate`. */
export function validatePlan(plan: LiveQueryPlan): void {
  quote(plan.table);
  quote(plan.key);
  const columns = plan.columns ?? [];
  if (!columns.length) throw LiveQueryError.invalid(`query for ${rustDebug(plan.table)} must declare columns`);
  for (const column of columns) quote(column);
  if (!columns.includes(plan.key)) {
    throw LiveQueryError.invalid(`query for ${rustDebug(plan.table)} must include key ${rustDebug(plan.key)} in columns`);
  }
  if (plan.serverOnly) throw LiveQueryError.invalid("server-only arbitrary reactive predicates are not supported");
  if (plan.where) validateExpression(plan.where);
  if (plan.search) {
    requireName(plan.search.argument, "search argument");
    for (const column of plan.search.columns) quote(column);
    for (const source of plan.search.sources ?? []) {
      quote(source.table);
      quote(source.key);
      quote(source.column);
      for (const dependency of source.dependencies ?? []) quote(dependency);
    }
  }
  if (plan.index) {
    const index = plan.index;
    quote(index.table);
    quote(index.key);
    for (const column of index.columns) quote(column);
    for (const dependency of index.dependencies ?? []) quote(dependency);
    // BTreeMap order: logical names sorted by code unit (ASCII identifiers).
    for (const logical of Object.keys(index.sortColumns ?? {}).sort(byteOrder)) {
      quote(logical);
      const sortColumns = index.sortColumns![logical]!;
      if (!sortColumns.length) throw LiveQueryError.invalid("index sort must contain columns");
      for (const column of sortColumns) quote(column);
    }
  }
  if (plan.filters) {
    const filters = plan.filters;
    requireName(filters.argument, "filter argument");
    for (const column of filters.allowedColumns) quote(column);
    for (const operator of filters.allowedOperators) {
      if (!validFilterOperator(operator)) throw LiveQueryError.invalid(`unsupported allowed filter operator ${rustDebug(operator)}`);
    }
    for (const column of Object.keys(filters.columnTypes ?? {}).sort(byteOrder)) {
      const type = filters.columnTypes![column]!;
      if (!filters.allowedColumns.includes(column)) {
        throw LiveQueryError.invalid(`filter column type references unallowed column ${rustDebug(column)}`);
      }
      if (type !== "text" && type !== "number") {
        throw LiveQueryError.invalid(`filter column ${rustDebug(column)} has unsupported type ${rustDebug(type)}`);
      }
    }
  }
  if (plan.sort) {
    const sort = plan.sort;
    quote(sort.defaultColumn);
    if (sort.defaultDirection !== "asc" && sort.defaultDirection !== "desc") {
      throw LiveQueryError.invalid("default sort direction must be asc or desc");
    }
    for (const column of sort.allowedColumns) quote(column);
    if (!sort.allowedColumns.includes(sort.defaultColumn)) throw LiveQueryError.invalid("default sort column must be allowed");
  }
  if (plan.window) {
    const window = plan.window;
    requireName(window.offsetArgument, "window offset argument");
    requireName(window.limitArgument, "window limit argument");
    if (window.defaultLimit === 0 || window.maxLimit === 0 || window.maxLimit > 10_000) {
      throw LiveQueryError.invalid("window limits must be between 1 and 10000");
    }
    if (window.count && window.count !== "exact") throw LiveQueryError.invalid("window count must be exact");
  }
  for (const part of plan.resultPath ?? []) requireName(part, "result path segment");
}

/** `validate_expression`. */
function validateExpression(expression: LiveExpression): void {
  for (const value of [expression.value, expression.valueTo]) {
    if (value?.context && !["account.id", "member.id", "tenant.id"].includes(value.context)) {
      throw LiveQueryError.invalid("Invalid query identity context");
    }
  }
  if (expression.operator === "inRelation") {
    quote(expression.column ?? "");
    const relation = expression.relation;
    if (!relation) throw LiveQueryError.invalid("inRelation requires a relation");
    quote(relation.table);
    quote(relation.column);
    if (relation.where) validateExpression(relation.where);
    return;
  }
  const children = expression.children ?? [];
  switch (expression.operator) {
    case "and":
    case "or":
      if (!children.length) throw LiveQueryError.invalid(`${expression.operator} requires children`);
      break;
    case "not":
      if (children.length !== 1) throw LiveQueryError.invalid("not requires one child");
      break;
    case "server":
      throw LiveQueryError.invalid("server-only arbitrary reactive predicates are not supported");
    case "eq": case "neq": case "gt": case "gte": case "lt": case "lte": case "range": case "in":
    case "contains": case "containsInsensitive": case "arrayContains":
      quote(expression.column ?? "");
      break;
    default:
      throw LiveQueryError.invalid(`unsupported live query operator ${rustDebug(expression.operator)}`);
  }
  for (const child of children) validateExpression(child);
}

/** Tables named by `inRelation` anywhere in the predicate, sorted (`relation_tables`, a BTreeSet). */
export function relationTables(expression: LiveExpression | null | undefined): string[] {
  const out = new Set<string>();
  const walk = (node: LiveExpression | null | undefined) => {
    if (!node) return;
    if (node.relation) {
      out.add(node.relation.table);
      walk(node.relation.where);
    }
    for (const child of node.children ?? []) walk(child);
  };
  walk(expression);
  return [...out].sort(byteOrder);
}

/** Tables the plan reads without their own visibility mask: the index projection and search documents, with their declared dependencies. */
export function projectionTables(plan: LiveQueryPlan): Set<string> {
  const out = new Set<string>();
  if (plan.index) {
    out.add(plan.index.table);
    for (const table of plan.index.dependencies ?? []) out.add(table);
  }
  for (const source of plan.search?.sources ?? []) {
    out.add(source.table);
    for (const table of source.dependencies ?? []) out.add(table);
  }
  return out;
}

/** Rust's `BTreeMap<String, _>`/`BTreeSet<String>` order: by UTF-8 bytes (code points). */
export function byteOrder(a: string, b: string): number {
  if (a === b) return 0;
  const x = [...a], y = [...b];
  for (let i = 0; i < Math.min(x.length, y.length); i++) {
    const d = x[i]!.codePointAt(0)! - y[i]!.codePointAt(0)!;
    if (d) return d;
  }
  return x.length - y.length;
}
