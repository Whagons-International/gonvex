/**
 * Durable Object SQLite binds at most 100 parameters per statement (0011 §3).
 * Rust binds as many as the plan produces: an `in` predicate over a long
 * argument array, or a `oneOf` filter, binds one per value (S1: the only
 * statements over 100 are these runtime ones).
 *
 * `fitParameters` keeps the statement and its meaning and moves values into
 * JSON arrays bound as one parameter each:
 *
 * 1. Every list `IN ($a, $b, …)` of three or more becomes
 *    `IN (SELECT value FROM json_each($k))`. `json_each` yields each element
 *    with the storage class Rust binds it with (TEXT, INTEGER, REAL, NULL,
 *    booleans as 0/1, arrays and objects as JSON text), and its `value`
 *    column has no affinity, like a bound parameter, so `x IN (…)` compares
 *    exactly as before.
 * 2. If the statement still binds more than 100 (not produced by any plan
 *    today), every parameter is read from one JSON array with
 *    `json_extract($1, '$[i]')`, which returns the same storage classes.
 *
 * Placeholders are renumbered compactly. The kernel builds every statement
 * here, so `$N` never occurs inside a string literal or identifier.
 */
import { MAX_BOUND_PARAMETERS } from "../storage";

export interface Fitted {
  readonly statement: string;
  readonly parameters: readonly unknown[];
  /** Lists moved into json_each (0 when the statement already fitted). */
  readonly packedLists: number;
  /** True when step 2 was needed. */
  readonly packedAll: boolean;
}

const PLACEHOLDER = /\$(\d+)/g;
const LIST = /IN \((\$\d+(?:, \$\d+){2,})\)/g;

/** Renumbers `$N` in order of first use; values follow their placeholders. */
function compact(statement: string, values: readonly unknown[]): { statement: string; parameters: unknown[] } {
  const mapping = new Map<number, number>();
  const parameters: unknown[] = [];
  const text = statement.replace(PLACEHOLDER, (_, digits: string) => {
    const old = Number(digits);
    let next = mapping.get(old);
    if (next === undefined) {
      parameters.push(values[old - 1] ?? null);
      next = parameters.length;
      mapping.set(old, next);
    }
    return `$${next}`;
  });
  return { statement: text, parameters };
}

export function fitParameters(statement: string, parameters: readonly unknown[]): Fitted {
  if (parameters.length <= MAX_BOUND_PARAMETERS) return { statement, parameters, packedLists: 0, packedAll: false };
  const values = [...parameters];
  let packedLists = 0;
  const listed = statement.replace(LIST, (_, list: string) => {
    const items = list.split(", ").map((placeholder) => values[Number(placeholder.slice(1)) - 1] ?? null);
    values.push(items);
    packedLists++;
    return `IN (SELECT value FROM json_each($${values.length}))`;
  });
  const fitted = compact(listed, values);
  if (fitted.parameters.length <= MAX_BOUND_PARAMETERS) return { ...fitted, packedLists, packedAll: false };
  const all = fitted.parameters.map((value) => value === undefined ? null : value);
  const text = fitted.statement.replace(PLACEHOLDER, (_, digits: string) => `json_extract($1, '$[${Number(digits) - 1}]')`);
  return { statement: text, parameters: [all], packedLists, packedAll: true };
}
