// The interpreted evaluator (visibility_mask.rs): pure row visibility. It is
// the reference semantics the SQL compiler and the compiled class programs
// must agree with. No database handle, no asynchronous work (V1).

import { field, get, isObject, jsonEqual, scalar, truthy, type JsonObject, type JsonValue } from "./json.ts";
import { literalOf, viaIndexName, type VisibilityConstraint, type VisibilityExpression, type VisibilityPlan } from "./plan.ts";
import type { ResolvedVisibility } from "./resolved.ts";

/** Literal constraints on an indexed row: equality, or IS NULL for a null literal (a missing column matches null). */
export function literalConstraintsHold(constraints: readonly VisibilityConstraint[], row: JsonValue | undefined): boolean {
  for (const constraint of constraints) {
    const literal = literalOf(constraint.value);
    const value = get(row, constraint.column);
    if (literal === undefined) return false;
    if (literal === null) {
      if (value !== undefined && value !== null) return false;
    } else if (value === undefined || !jsonEqual(value, literal)) return false;
  }
  return true;
}

export function rowMatches(plan: VisibilityPlan, resolved: ResolvedVisibility, row: JsonValue | undefined): boolean {
  return isObject(row) && matchExpression(plan.predicate, resolved, row);
}

function contains(resolved: ResolvedVisibility, set: string, value: JsonValue): boolean {
  return resolved.sets.get(set)?.has(scalar(value)) ?? false;
}

function parentAdmitted(expression: VisibilityExpression, resolved: ResolvedVisibility, parentRow: JsonValue, parentPlan: VisibilityPlan, parentResolved: ResolvedVisibility): boolean {
  if (!literalConstraintsHold(expression.parentWhere, parentRow) || !isObject(parentRow)) return false;
  return expression.predicate ? matchExpression(expression.predicate, resolved, parentRow) : matchExpression(parentPlan.predicate, parentResolved, parentRow);
}

export function matchExpression(expression: VisibilityExpression, resolved: ResolvedVisibility, row: JsonObject): boolean {
  switch (expression.operator) {
    case "public":
      return true;
    case "nonEmpty":
      return (resolved.sets.get(expression.set)?.size ?? 0) > 0;
    case "permission":
      return typeof expression.value === "string" && truthy(get(resolved.permissions, expression.value));
    case "role":
      return typeof expression.value === "string" && resolved.role === expression.value;
    case "eq": {
      const literal = literalOf(expression.value);
      const value = field(row, expression.column);
      return literal !== undefined && value !== undefined && jsonEqual(value, literal);
    }
    case "eqContext":
      return scalar(field(row, expression.column)) === resolved.direct[expression.context];
    case "inSet": {
      const value = field(row, expression.column);
      return value !== undefined && value !== null && contains(resolved, expression.set, value);
    }
    case "inherits": {
      const parent = resolved.inherited.get(expression.table);
      if (!parent) return false;
      const value = field(row, expression.column);
      if (value === undefined || value === null) return false;
      const parentRow = parent.row(scalar(value));
      if (parentRow !== undefined) return parentAdmitted(expression, resolved, parentRow, parent.plan, parent.resolved);
      return expression.onMissing !== undefined && matchExpression(expression.onMissing, resolved, row);
    }
    case "inheritsAny": {
      const parent = resolved.inherited.get(expression.table);
      const via = expression.via;
      if (!parent || !via) return false;
      const value = field(row, expression.column);
      if (value === undefined || value === null) return false;
      const links = resolved.indexes.get(viaIndexName(via, via.childColumn));
      if (!links) return false;
      return links.getAll(scalar(value)).some((link) => {
        const parentKey = get(link, via.parentColumn);
        if (parentKey === undefined || parentKey === null) return false;
        const parentRow = parent.row(scalar(parentKey));
        return parentRow !== undefined && parentAdmitted(expression, resolved, parentRow, parent.plan, parent.resolved);
      });
    }
    case "and":
      return expression.children.every((child) => matchExpression(child, resolved, row));
    case "or":
      return expression.children.some((child) => matchExpression(child, resolved, row));
    case "not":
      return !matchExpression(expression.children[0]!, resolved, row);
    default:
      return false;
  }
}
