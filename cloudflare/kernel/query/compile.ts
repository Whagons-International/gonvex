/**
 * Structured plan to SQL: a port of `execute_in_transaction` and its helpers
 * (`live_query.rs` 1032-1810). The SQL text is Rust's, so SQLite evaluates
 * the same predicates in the same order; only the 100-parameter limit of
 * Durable Object SQLite is handled afterwards (`parameters.ts`).
 */
import type { JsonKind } from "../storage";
import type { LiveExpression, LiveFilters, LiveQueryPlan, LiveSearch, LiveSort, LiveValue, LiveWindow } from "./plan";
import { LiveQueryError, quote } from "./plan";
import { rustDebug } from "./json";
import type { QueryVisibility, ResolvedVisibility, VisibilityPlan } from "./visibility";

export type Args = Readonly<Record<string, unknown>>;
export type Related = ReadonlyMap<string, { plan: VisibilityPlan; resolved: ResolvedVisibility }>;

export interface CompiledPlan {
  /** The window statement: one JSON object text per row. */
  readonly statement: string;
  readonly parameters: readonly unknown[];
  /** `SELECT count(*)` over the same predicates (exact window count with a result path). */
  readonly count: { readonly statement: string; readonly parameters: readonly unknown[] } | undefined;
  readonly offset: number;
  readonly limit: number;
}

const push = (parameters: unknown[], value: unknown) => { parameters.push(value); return `$${parameters.length}`; };

/** serde `Value` equality to null (absent and JSON null). */
const isNull = (value: unknown) => value === null || value === undefined;

/** `argument_text`: strings as-is, null/absent empty, anything else as JSON text. */
export function argumentText(value: unknown): string {
  if (isNull(value)) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

function liveValue(value: LiveValue | null | undefined, args: Args): unknown {
  if (!value) return null;
  if (value.argument) return Object.hasOwn(args, value.argument) ? args[value.argument] ?? null : null;
  return value.literal ?? null;
}

/** `nonnegative`: a u64 number or a decimal string. */
function nonnegative(value: unknown): number | undefined {
  if (typeof value === "number") return Number.isInteger(value) && value >= 0 ? value : undefined;
  if (typeof value === "string" && /^\+?\d+$/.test(value)) return Number(value);
  return undefined;
}

export function windowOf(definition: LiveWindow | null | undefined, args: Args): [offset: number, limit: number] {
  if (!definition) return [0, 0];
  const offset = nonnegative(args[definition.offsetArgument]) ?? 0;
  const requested = nonnegative(args[definition.limitArgument]);
  const limit = Math.min(requested !== undefined && requested > 0 ? requested : definition.defaultLimit, definition.maxLimit);
  return [offset, limit];
}

function sortOf(definition: LiveSort | null | undefined, args: Args): [column: string, direction: string] {
  if (!definition) return ["", ""];
  const requested = definition.columnArgument !== undefined ? args[definition.columnArgument] : undefined;
  const column = typeof requested === "string" && definition.allowedColumns.includes(requested) ? requested : definition.defaultColumn;
  const rawDirection = definition.directionArgument !== undefined ? args[definition.directionArgument] : undefined;
  const lowered = typeof rawDirection === "string" ? rustLowercase(rawDirection) : undefined;
  const direction = lowered === "asc" || lowered === "desc" ? lowered : definition.defaultDirection;
  return [column, direction];
}

/** `str::to_lowercase` (Unicode, like JavaScript's, except the final-sigma rule, which cannot produce "asc"/"desc"). */
const rustLowercase = (value: string) => value.toLowerCase();

/** `index_predicate`: moves source-column references onto the index projection. */
function indexPredicate(plan: LiveQueryPlan, sql: string): string {
  const index = plan.index;
  if (!index) return sql;
  let out = sql.split(`CAST(r.${quote(plan.key)} AS TEXT)`).join(`g.${quote(index.key)}`);
  for (const column of index.columns) {
    const name = quote(column);
    out = out.split(`r.${name}`).join(`g.${name}`);
  }
  return out;
}

function compileSource(plan: LiveQueryPlan): string {
  const source = `${quote(plan.table)} AS r`;
  const index = plan.index;
  return index ? `${source} JOIN ${quote(index.table)} AS g ON g.${quote(index.key)} = CAST(r.${quote(plan.key)} AS TEXT)` : source;
}

function compileOrder(plan: LiveQueryPlan, args: Args): string {
  const [column, rawDirection] = sortOf(plan.sort, args);
  const direction = rawDirection.toLowerCase() === "desc" ? "DESC NULLS FIRST" : "ASC NULLS LAST";
  const terms: string[] = [];
  const indexColumns = plan.index?.sortColumns && Object.hasOwn(plan.index.sortColumns, column) ? plan.index.sortColumns[column] : undefined;
  if (indexColumns) {
    for (const name of indexColumns) terms.push(`g.${quote(name)} ${direction}`);
  } else if (column) {
    terms.push(indexPredicate(plan, `r.${quote(column)} ${direction}`));
  }
  if (column !== plan.key) terms.push(`r.${quote(plan.key)} ASC NULLS LAST`);
  return terms.join(", ");
}

function compileExpression(
  expression: LiveExpression, args: Args, parameters: unknown[], alias: string, related: Related,
  context: Readonly<Record<string, string>>, visibility: QueryVisibility,
): string {
  const children = expression.children ?? [];
  switch (expression.operator) {
    case "inRelation": {
      const relation = expression.relation;
      if (!relation) throw LiveQueryError.invalid("inRelation requires a relation");
      const entry = related.get(relation.table);
      if (!entry) throw LiveQueryError.visibilityMissing(relation.table);
      const relatedAlias = `${alias}_related`;
      const guard = visibility.compilePredicate(entry.plan, entry.resolved, relatedAlias, parameters);
      const predicate = relation.where ? compileExpression(relation.where, args, parameters, relatedAlias, related, context, visibility) : "TRUE";
      return `CAST(${alias}.${quote(expression.column ?? "")} AS TEXT) IN (SELECT CAST(${relatedAlias}.${quote(relation.column)} AS TEXT) FROM ${quote(relation.table)} AS ${relatedAlias} WHERE (${guard}) AND (${predicate}))`;
    }
    case "and":
    case "or": {
      if (!children.length) return "FALSE";
      const parts = children.map((child) => `(${compileExpression(child, args, parameters, alias, related, context, visibility)})`);
      return `(${parts.join(expression.operator === "and" ? " AND " : " OR ")})`;
    }
    case "not": {
      if (children.length !== 1) throw LiveQueryError.invalid("live not expression requires one child");
      return `NOT (${compileExpression(children[0]!, args, parameters, alias, related, context, visibility)})`;
    }
    case "server":
      throw LiveQueryError.invalid("server-only arbitrary reactive predicates are not supported");
  }
  const left = `${alias}.${quote(expression.column ?? "")}`;
  let value: unknown;
  if (expression.value?.context) {
    if (!Object.hasOwn(context, expression.value.context)) throw LiveQueryError.invalid("Missing query identity context");
    value = context[expression.value.context];
  } else value = liveValue(expression.value, args);
  switch (expression.operator) {
    case "eq": return isNull(value) ? `${left} IS NULL` : `${left} = ${push(parameters, value)}`;
    case "neq": return isNull(value) ? `${left} IS NOT NULL` : `${left} <> ${push(parameters, value)}`;
    case "gt": return `${left} > ${push(parameters, value)}`;
    case "gte": return `${left} >= ${push(parameters, value)}`;
    case "lt": return `${left} < ${push(parameters, value)}`;
    case "lte": return `${left} <= ${push(parameters, value)}`;
    case "range": {
      const from = push(parameters, value);
      return `${left} BETWEEN ${from} AND ${push(parameters, liveValue(expression.valueTo, args))}`;
    }
    case "arrayContains":
      return `EXISTS (SELECT 1 FROM json_each(COALESCE(${left}, '[]')) WHERE value = ${push(parameters, value)})`;
    case "contains":
      return `instr(COALESCE(CAST(${left} AS TEXT), ''), CAST(${push(parameters, argumentText(value))} AS TEXT)) > 0`;
    case "containsInsensitive":
      return `instr(lower(COALESCE(CAST(${left} AS TEXT), '')), lower(CAST(${push(parameters, argumentText(value))} AS TEXT))) > 0`;
    case "in": {
      if (!Array.isArray(value) || !value.length) return "FALSE";
      const items = value.map((item) => push(parameters, item));
      return `${left} IN (${items.join(", ")})`;
    }
    default:
      throw LiveQueryError.invalid(`unsupported live query operator ${rustDebug(expression.operator)}`);
  }
}

function searchTerms(input: string, booleanTerms: boolean): [string[], " AND " | " OR "] {
  const text = rustTrim(input).toLowerCase();
  const operator = booleanTerms && text.includes("$and") ? "$and" : booleanTerms && text.includes("$or") ? "$or" : "";
  const terms = operator ? text.split(operator).map(rustTrim) : [text];
  return [terms.filter((term) => term.length > 0), operator === "$or" ? " OR " : " AND "];
}

/** Unicode White_Space, which `str::trim` strips (JavaScript's `trim` differs: it strips U+FEFF and keeps U+0085). */
const WHITE_SPACE = "[\\u0009-\\u000d\\u0020\\u0085\\u00a0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000]";
const TRIM = new RegExp(`^${WHITE_SPACE}+|${WHITE_SPACE}+$`, "g");
function rustTrim(value: string): string {
  return value.replace(TRIM, "");
}

function searchPattern(term: string): string {
  return `%${term.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_")}%`;
}

function compileSearch(plan: LiveQueryPlan, search: LiveSearch, args: Args, parameters: unknown[], indexFilter: string): string {
  const [terms, joiner] = searchTerms(argumentText(args[search.argument]), search.booleanTerms ?? false);
  const predicates: string[] = [];
  const documentSets: string[] = [];
  const sources = search.sources ?? [];
  const index = plan.index;
  const directIndex = index && sources.some((source) => source.table === index.table && source.key === index.key) ? index : undefined;
  for (const term of terms) {
    const pattern = push(parameters, searchPattern(term));
    const alternatives: string[] = [];
    for (const column of search.columns) alternatives.push(`CAST(r.${quote(column)} AS TEXT) LIKE ${pattern} ESCAPE '\\'`);
    if (sources.length) {
      if (directIndex) {
        for (const source of sources.filter((s) => s.table === directIndex.table && s.key === directIndex.key)) {
          alternatives.push(`g.${quote(source.column)} LIKE ${pattern} ESCAPE '\\'`);
        }
      }
      const documents = sources
        .filter((source) => !(directIndex && source.table === directIndex.table && source.key === directIndex.key))
        .map((source) => {
          let alias = "d", join = "", filter = "";
          if (index && indexFilter && index.table === source.table) {
            alias = "g";
            filter = ` AND (${indexFilter})`;
          } else if (index && indexFilter) {
            join = ` JOIN ${quote(index.table)} AS g ON g.${quote(index.key)} = d.${quote(source.key)}`;
            filter = ` AND (${indexFilter})`;
          }
          return `SELECT ${alias}.${quote(source.key)} FROM ${quote(source.table)} AS ${alias}${join} WHERE ${alias}.${quote(source.column)} LIKE ${pattern} ESCAPE '\\'${filter}`;
        });
      if (documents.length) {
        const union = documents.join(" UNION ");
        if (directIndex) alternatives.push(`g.${quote(directIndex.key)} IN (${union})`);
        else if (!search.columns.length) documentSets.push(`(${union})`);
        else alternatives.push(`CAST(r.${quote(plan.key)} AS TEXT) IN (${union})`);
      }
    }
    if (alternatives.length) predicates.push(`(${alternatives.join(" OR ")})`);
  }
  if (documentSets.length) {
    return `CAST(r.${quote(plan.key)} AS TEXT) IN (${documentSets.join(joiner === " OR " ? " UNION " : " INTERSECT ")})`;
  }
  return predicates.length ? `(${predicates.join(joiner)})` : "";
}

/** `parse_filter_number`: i64, else a finite f64. */
function filterNumber(value: string, index: number): number {
  if (/^[+-]?\d+$/.test(value)) {
    const big = BigInt(value);
    if (big >= -(2n ** 63n) && big < 2n ** 63n) return Number(big);
  }
  const parsed = rustParseF64(value);
  if (parsed === undefined || !Number.isFinite(parsed)) throw LiveQueryError.invalid(`live query filter ${index} requires a finite numeric value`);
  return parsed;
}

/** `str::parse::<f64>`: decimal or exponent forms, `inf`/`nan` spellings; no surrounding whitespace, no hex. */
function rustParseF64(value: string): number | undefined {
  if (/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(value)) return Number(value);
  if (/^[+-]?(inf|infinity|nan)$/i.test(value)) return value.startsWith("-") ? -Infinity : Infinity;
  return undefined;
}

function compileFilters(definition: LiveFilters, args: Args, parameters: unknown[], alias: string): string {
  if (!Object.hasOwn(args, definition.argument)) return "";
  const raw = args[definition.argument];
  if (isNull(raw)) return "";
  if (!Array.isArray(raw)) throw LiveQueryError.invalid(`live query filter argument ${rustDebug(definition.argument)} must be an array`);
  const allowedColumns = new Set(definition.allowedColumns);
  const allowedOperators = new Set(definition.allowedOperators);
  const predicates: string[] = [];
  raw.forEach((item: unknown, index: number) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) throw LiveQueryError.invalid(`live query filter ${index} must be an object`);
    const filter = item as Record<string, unknown>;
    const column = filter.column;
    if (typeof column !== "string" || !allowedColumns.has(column)) throw LiveQueryError.invalid(`live query filter ${index} has an unallowed column`);
    const operator = filter.operator;
    if (typeof operator !== "string" || !allowedOperators.has(operator)) throw LiveQueryError.invalid(`live query filter ${index} has an unallowed operator`);
    const value = filter.value;
    if (typeof value !== "string") throw LiveQueryError.invalid(`live query filter ${index} value must be a string`);
    const valueTo = typeof filter.valueTo === "string" ? filter.valueTo : undefined;
    if (Object.hasOwn(filter, "valueTo") && valueTo === undefined) throw LiveQueryError.invalid(`live query filter ${index} valueTo must be a string`);
    const left = `${alias}.${quote(column)}`;
    const text = `COALESCE(CAST(${left} AS TEXT), '')`;
    const columnType = definition.columnTypes && Object.hasOwn(definition.columnTypes, column) ? definition.columnTypes[column]! : "text";
    const ordered = ["lessThan", "lessThanOrEqual", "greaterThan", "greaterThanOrEqual", "inRange"].includes(operator);
    const numeric = ordered && columnType === "number";
    let parameterValue: unknown;
    if (numeric) parameterValue = filterNumber(value, index);
    else if (["contains", "notContains", "startsWith", "endsWith"].includes(operator)) {
      const escaped = value.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_");
      parameterValue = operator === "startsWith" ? `${escaped}%` : operator === "endsWith" ? `%${escaped}` : `%${escaped}%`;
    } else parameterValue = value;
    const valueArg = ["empty", "notEmpty", "oneOf"].includes(operator) ? "" : push(parameters, parameterValue);
    const comparisonLeft = columnType === "number" ? left : text;
    let predicate: string;
    switch (operator) {
      case "contains": case "startsWith": case "endsWith":
        predicate = value === "" ? `${text} LIKE ${valueArg} ESCAPE '\\'` : `CAST(${left} AS TEXT) LIKE ${valueArg} ESCAPE '\\'`;
        break;
      case "notContains":
        predicate = value === "" ? `${text} NOT LIKE ${valueArg} ESCAPE '\\'` : `(${left} IS NULL OR CAST(${left} AS TEXT) NOT LIKE ${valueArg} ESCAPE '\\')`;
        break;
      case "equals":
        predicate = value !== "" ? `CAST(${left} AS TEXT) = CAST(${valueArg} AS TEXT)` : `${text} = CAST(${valueArg} AS TEXT)`;
        break;
      case "notEquals": predicate = `${text} <> CAST(${valueArg} AS TEXT)`; break;
      case "empty": predicate = `${text} = ''`; break;
      case "notEmpty": predicate = `${text} <> ''`; break;
      case "oneOf": {
        let values: unknown;
        try { values = JSON.parse(value); } catch { values = undefined; }
        if (!Array.isArray(values)) throw LiveQueryError.invalid(`live query filter ${index} oneOf value must be a JSON array`);
        if (!values.length) { predicate = "FALSE"; break; }
        const includesEmpty = values.some((item) => argumentText(item) === "");
        const placeholders = values.map((item) => push(parameters, argumentText(item)));
        predicate = `${includesEmpty ? text : `CAST(${left} AS TEXT)`} IN (${placeholders.join(", ")})`;
        break;
      }
      case "lessThan": predicate = `${comparisonLeft} < ${valueArg}`; break;
      case "lessThanOrEqual": predicate = `${comparisonLeft} <= ${valueArg}`; break;
      case "greaterThan": predicate = `${comparisonLeft} > ${valueArg}`; break;
      case "greaterThanOrEqual": predicate = `${comparisonLeft} >= ${valueArg}`; break;
      case "inRange": {
        if (valueTo === undefined) throw LiveQueryError.invalid(`live query filter ${index} inRange requires valueTo`);
        const to = columnType === "number" ? filterNumber(valueTo, index) : valueTo;
        predicate = `${comparisonLeft} BETWEEN ${valueArg} AND ${push(parameters, to)}`;
        break;
      }
      default: throw LiveQueryError.invalid(`live query filter ${index} has an unsupported operator`);
    }
    predicates.push(`(${predicate})`);
  });
  return predicates.join(" AND ");
}

/** `json_value_expr` (store/src/rows.rs). */
export function jsonValueExpr(qualified: string, kind: JsonKind): string {
  const finite = `typeof(${qualified}) = 'real' AND abs(${qualified}) <= 1.7976931348623157e308`;
  const exact = `json(printf('%!.17g', ${qualified}))`;
  switch (kind) {
    case "json": return `CASE WHEN ${qualified} IS NULL THEN NULL WHEN json_valid(${qualified}) THEN json(${qualified}) ELSE ${qualified} END`;
    case "boolean": return `CASE WHEN ${qualified} IS NULL THEN NULL WHEN ${qualified} THEN json('true') ELSE json('false') END`;
    case "integer": case "real": return `CASE WHEN ${finite} THEN ${exact} ELSE ${qualified} END`;
    default: return `CASE WHEN typeof(${qualified}) = 'blob' THEN lower(hex(${qualified})) WHEN ${finite} THEN ${exact} ELSE ${qualified} END`;
  }
}

/** `json_object_expr`: one object per 60 columns, spliced (SQLite's 127-argument limit). */
export function jsonObjectExpr(alias: string, columns: readonly (readonly [string, JsonKind])[]): string {
  const pairs = columns.map(([name, kind]) => {
    const qualified = `${alias}."${name.replaceAll('"', '""')}"`;
    return `'${name.replaceAll("'", "''")}', ${jsonValueExpr(qualified, kind)}`;
  });
  const chunks: string[] = [];
  for (let at = 0; at < pairs.length; at += 60) chunks.push(`json_object(${pairs.slice(at, at + 60).join(", ")})`);
  if (!chunks.length) return "json_object()";
  if (chunks.length === 1) return chunks[0]!;
  return `json('{' || ${chunks.map((chunk) => `substr(${chunk}, 2, length(${chunk}) - 2)`).join(" || ',' || ")} || '}')`;
}

export interface CompileInput {
  readonly plan: LiveQueryPlan;
  readonly visibilityPlan: VisibilityPlan;
  readonly resolved: ResolvedVisibility;
  readonly args: Args;
  readonly related: Related;
  readonly visibility: QueryVisibility;
  /** Declared kinds of the plan table's columns (absent columns decode as text). */
  readonly kinds: ReadonlyMap<string, JsonKind>;
}

/** `execute_in_transaction`, up to the statements. */
export function compilePlan(input: CompileInput): CompiledPlan {
  const { plan, args, related, visibility, resolved } = input;
  const parameters: unknown[] = [];
  const predicates = [visibility.compilePredicate(input.visibilityPlan, resolved, "r", parameters)];
  if (!resolved.ephemeralMember) {
    parameters.push(resolved.direct["account.id"]);
    predicates.push(`EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = $${parameters.length} AND _gonvex_member.status = 'active')`);
  }
  if (plan.where) predicates.push(indexPredicate(plan, compileExpression(plan.where, args, parameters, "r", related, resolved.direct, visibility)));
  if (plan.filters) {
    const filter = compileFilters(plan.filters, args, parameters, "r");
    if (filter) predicates.push(indexPredicate(plan, filter));
  }
  if (plan.search) {
    const indexFilter = predicates.filter((p) => p.includes("g.") && !p.includes("r.") && !p.includes("SELECT ")).join(" AND ");
    const predicate = compileSearch(plan, plan.search, args, parameters, indexFilter);
    if (predicate) predicates.push(predicate);
  }
  const [offset, limit] = windowOf(plan.window, args);
  const order = compileOrder(plan, args);
  const source = compileSource(plan);
  const columns = (plan.columns ?? []).map((column) => { quote(column); return [column, input.kinds.get(column) ?? "text"] as const; });
  const base = [...parameters];
  let statement = `SELECT ${jsonObjectExpr("r", columns)} FROM ${source} WHERE ${predicates.join(" AND ")} ORDER BY ${order}`;
  if (limit > 0) statement += ` LIMIT ${push(parameters, limit)}`;
  else if (offset > 0) statement += " LIMIT -1";
  if (offset > 0) statement += ` OFFSET ${push(parameters, offset)}`;
  const count = plan.window?.count === "exact" && (plan.resultPath ?? []).length
    ? { statement: `SELECT count(*) FROM ${source} WHERE ${predicates.join(" AND ")}`, parameters: base }
    : undefined;
  return { statement, parameters, count, offset, limit };
}
