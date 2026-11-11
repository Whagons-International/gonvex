// The SQL form of a plan (visibility.rs `compile_predicate`,
// `compile_set_with_parameters`): snapshots and windows evaluate visibility
// inside SQLite. Parameters are JSON values bound by K01's kernel SQL
// (`StorageSession.run`, Rust's `Parameters::scalars`): strings as TEXT,
// integers as INTEGER, booleans as 0/1, arrays as JSON text read with
// `json_each`.

import { debugString, sortedText, type JsonValue } from "./json.ts";
import { matchExpression } from "./mask.ts";
import { contextKey, quote, roleOnlyValue, visibilityLiteral, VisibilityError, type VisibilitySet, type VisibilityExpression, type VisibilityPlan } from "./plan.ts";
import type { DirectContext, ResolvedVisibility } from "./resolved.ts";

let parentAliases = 0;
let viaAliases = 0;

/** A boolean SQL predicate over `rowAlias` that admits exactly the rows `plan` shows `resolved`. Appends its parameters. */
export function compilePredicate(plan: VisibilityPlan, resolved: ResolvedVisibility, rowAlias: string, parameters: JsonValue[]): string {
  return compileExpression(plan.predicate, plan, resolved, rowAlias, parameters, { slot: undefined });
}

interface AccountSlot {
  slot: number | undefined;
}

function compileExpression(expression: VisibilityExpression, plan: VisibilityPlan, resolved: ResolvedVisibility, rowAlias: string, parameters: JsonValue[], account: AccountSlot): string {
  if (plan.version === 2) {
    const value = roleOnlyValue(expression, resolved.role);
    if (value !== undefined) return value ? "TRUE" : "FALSE";
  }
  const accountArgument = (): string => {
    if (account.slot === undefined) {
      parameters.push(resolved.direct["account.id"] ?? null);
      account.slot = parameters.length;
    }
    return `$${account.slot}`;
  };
  const argument = (value: JsonValue): string => {
    parameters.push(value);
    return `$${parameters.length}`;
  };
  const recurse = (child: VisibilityExpression, alias = rowAlias, childPlan = plan, childResolved = resolved) =>
    compileExpression(child, childPlan, childResolved, alias, parameters, account);

  switch (expression.operator) {
    case "public":
      return "TRUE";
    case "inherits": {
      const parent = resolved.inherited.get(expression.table);
      if (!parent) throw VisibilityError.invalid(`inherited parent ${debugString(expression.table)} was not resolved`);
      const alias = `_gonvex_parent_${parentAliases++}`;
      const column = quote(expression.column);
      const key = quote(expression.key);
      const table = quote(expression.table);
      const parentPredicate = expression.predicate ? recurse(expression.predicate, alias) : recurse(parent.plan.predicate, alias, parent.plan, parent.resolved);
      let parentFilters = "";
      for (const constraint of expression.parentWhere) {
        parentFilters += ` AND ${compileLiteralComparison(alias, constraint.column, visibilityLiteral(constraint.value), parameters, false)}`;
      }
      const visible = `EXISTS (SELECT 1 FROM ${table} AS ${alias} WHERE ${alias}.${key} = ${rowAlias}.${column}${parentFilters} AND (${parentPredicate}))`;
      if (!expression.onMissing) return visible;
      const missing = `_gonvex_parent_${parentAliases++}`;
      const fallback = recurse(expression.onMissing);
      return `(${visible} OR (${rowAlias}.${column} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ${table} AS ${missing} WHERE ${missing}.${key} = ${rowAlias}.${column}) AND (${fallback})))`;
    }
    case "inheritsAny": {
      const via = expression.via;
      if (!via) throw VisibilityError.invalid("inheritsAny requires via");
      const parent = resolved.inherited.get(expression.table);
      if (!parent) throw VisibilityError.invalid(`inherited parent ${debugString(expression.table)} was not resolved`);
      const suffix = viaAliases++;
      const link = `_gonvex_via_${suffix}`;
      const alias = `_gonvex_any_parent_${suffix}`;
      const parentPredicate = expression.predicate ? recurse(expression.predicate, alias) : recurse(parent.plan.predicate, alias, parent.plan, parent.resolved);
      let parentFilters = "";
      for (const constraint of expression.parentWhere) {
        parentFilters += ` AND ${compileLiteralComparison(alias, constraint.column, visibilityLiteral(constraint.value), parameters, false)}`;
      }
      return `EXISTS (SELECT 1 FROM ${quote(via.table)} AS ${link} JOIN ${quote(expression.table)} AS ${alias} ON ${alias}.${quote(expression.key)} = ${link}.${quote(via.parentColumn)} WHERE ${link}.${quote(via.childColumn)} = ${rowAlias}.${quote(expression.column)}${parentFilters} AND (${parentPredicate}))`;
    }
    case "nonEmpty": {
      if (plan.version === 2) {
        const values = resolved.sets.get(expression.set);
        if (!values) throw VisibilityError.invalid(`mask principal set ${debugString(expression.set)} was not resolved`);
        return values.size === 0 ? "FALSE" : "TRUE";
      }
      const set = plan.sets.get(expression.set);
      if (!set) throw VisibilityError.invalid(`unknown set ${debugString(expression.set)}`);
      return `EXISTS (${compileSetWithParameters(set, resolved.direct, parameters)})`;
    }
    case "permission":
    case "role": {
      // SQL snapshots and in-memory deltas use the same admitted principal.
      // Ordinary members keep their live database role checks.
      if (resolved.ephemeralMember) return matchExpression(expression, resolved, {}) ? "TRUE" : "FALSE";
      const accountSql = accountArgument();
      const value = argument(typeof expression.value === "string" ? expression.value : "");
      if (expression.operator === "permission") {
        return `EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = ${accountSql} AND _gonvex_member.status = 'active' AND lower(COALESCE((SELECT CAST(value AS TEXT) FROM json_each(_gonvex_member.permissions) WHERE key = ${value} LIMIT 1), '')) IN ('true', '1'))`;
      }
      return `EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = ${accountSql} AND _gonvex_member.status = 'active' AND _gonvex_member.role = ${value})`;
    }
    case "eqContext": {
      const column = quote(expression.column);
      return `${rowAlias}.${column} = ${argument(resolved.direct[expression.context] ?? null)}`;
    }
    case "eq":
      return compileLiteralComparison(rowAlias, expression.column, visibilityLiteral(expression.value), parameters, false);
    case "inSet": {
      const column = quote(expression.column);
      if (plan.version === 2) {
        const values = resolved.sets.get(expression.set);
        if (!values) throw VisibilityError.invalid(`mask principal set ${debugString(expression.set)} was not resolved`);
        return `${rowAlias}.${column} IN (SELECT value FROM json_each(${argument(sortedText(values))}))`;
      }
      const set = plan.sets.get(expression.set);
      if (!set) throw VisibilityError.invalid(`unknown set ${debugString(expression.set)}`);
      return `${rowAlias}.${column} IN (${compileSetWithParameters(set, resolved.direct, parameters)})`;
    }
    case "and":
    case "or": {
      const children = expression.children.map((child) => `(${recurse(child)})`);
      return `(${children.join(expression.operator === "and" ? " AND " : " OR ")})`;
    }
    case "not": {
      const child = expression.children[0]!;
      // Double negation is the original admission decision.
      if (child.operator === "not") return recurse(child.children[0]!);
      if (child.operator === "eq") return compileLiteralComparison(rowAlias, child.column, visibilityLiteral(child.value), parameters, true);
      // Boolean admission: a nullable equality that does not admit a row is
      // false to the evaluator; SQL NOT would keep UNKNOWN. IS NOT TRUE
      // complements the child's admission, UNKNOWN included.
      return `(${recurse(child)}) IS NOT TRUE`;
    }
    default:
      return "FALSE";
  }
}

/** `SELECT DISTINCT CAST(selected AS TEXT) FROM …` for one set, with its own parameters. */
export function compileSet(set: VisibilitySet, direct: DirectContext): { sql: string; parameters: JsonValue[] } {
  const parameters: JsonValue[] = [];
  const sql = compileSetWithParameters(set, direct, parameters);
  return { sql, parameters };
}

export function compileSetWithParameters(set: VisibilitySet, direct: DirectContext, parameters: JsonValue[]): string {
  const baseAlias = set.alias || set.table;
  const aliases = new Map<string, string>([[baseAlias, "v0"]]);
  let from = `${quote(set.table)} AS v0`;
  let previous = baseAlias;
  set.joins.forEach((join, index) => {
    const logical = join.alias || join.table;
    const leftLogical = join.leftAlias || previous;
    const leftAlias = aliases.get(leftLogical);
    if (leftAlias === undefined) throw VisibilityError.invalid(`unknown left alias ${debugString(leftLogical)}`);
    const sqlAlias = `v${index + 1}`;
    from += ` JOIN ${quote(join.table)} AS ${sqlAlias} ON ${leftAlias}.${quote(join.leftColumn)} = ${sqlAlias}.${quote(join.rightColumn)}`;
    aliases.set(logical, sqlAlias);
    previous = logical;
  });
  const selectFrom = set.selectFrom || baseAlias;
  const selectedAlias = aliases.get(selectFrom);
  if (selectedAlias === undefined) throw VisibilityError.invalid(`unknown select alias ${debugString(selectFrom)}`);
  let query = `SELECT DISTINCT CAST(${selectedAlias}.${quote(set.select)} AS TEXT) FROM ${from}`;
  const predicates: string[] = [];
  for (const constraint of set.constraints) {
    let alias = "v0";
    if (constraint.table) {
      const found = aliases.get(constraint.table);
      if (found === undefined) throw VisibilityError.invalid(`unknown constraint alias ${debugString(constraint.table)}`);
      alias = found;
    }
    const hasContext = constraint.context !== "";
    const hasValue = constraint.value !== undefined;
    if (hasContext === hasValue) throw VisibilityError.invalid("visibility constraint requires exactly one of context or value");
    if (hasContext) {
      contextKey(constraint.context);
      parameters.push(direct[constraint.context] ?? null);
      predicates.push(`${alias}.${quote(constraint.column)} = $${parameters.length}`);
    } else {
      predicates.push(compileLiteralComparison(alias, constraint.column, visibilityLiteral(constraint.value), parameters, false));
    }
  }
  if (predicates.length) query += ` WHERE ${predicates.join(" AND ")}`;
  return query;
}

/** `row.column = literal`, `IS NULL` for null; negated: `IS DISTINCT FROM`, `IS NOT NULL`. */
export function compileLiteralComparison(rowAlias: string, column: string, literal: JsonValue, parameters: JsonValue[], negated: boolean): string {
  const quoted = quote(column);
  if (literal === null) return `${rowAlias}.${quoted} ${negated ? "IS NOT NULL" : "IS NULL"}`;
  parameters.push(literal);
  return `${rowAlias}.${quoted} ${negated ? "IS DISTINCT FROM" : "="} $${parameters.length}`;
}
