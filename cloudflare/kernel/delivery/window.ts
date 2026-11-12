// A collection window: the collection's own filters, projection, key, SQLite's
// ordering of JSON scalars and the truncated-window boundary (replica.rs
// `collection_filters_admit`, `project_row`, `row_key`, `sql_order`,
// `within_boundary`, `ReplicaWindow`).

import { jsonEqual, type JsonObject, type JsonValue } from "../visibility/json.ts";
import { rowMatches } from "../visibility/mask.ts";
import type { VisibilityPlan } from "../visibility/plan.ts";
import type { ResolvedVisibility } from "../visibility/resolved.ts";
import { byteOrder } from "./canonical.ts";
import type { ReplicaDefinition } from "./definition.ts";

/** Counters of the rows a client holds; for a truncated ordered window, the (order value, key) of its last row. */
export interface ReplicaWindow {
  rows: number;
  /** Canonical JSON bytes of the held projections. */
  bytes: number;
  truncated: boolean;
  boundary: readonly [JsonValue, JsonValue] | undefined;
}

export const emptyWindow = (): ReplicaWindow => ({ rows: 0, bytes: 0, truncated: false, boundary: undefined });
export const copyWindow = (w: ReplicaWindow): ReplicaWindow => ({ rows: w.rows, bytes: w.bytes, truncated: w.truncated, boundary: w.boundary });

export class ReplicaError extends Error {}

export const errors = {
  moduleMissing: () => new ReplicaError("project has no active TypeScript module"),
  functionMissing: (path: string) => new ReplicaError(`replica function ${debug(path)} is not registered`),
  visibilityMissing: (table: string) => new ReplicaError(`visibility plan required for table ${debug(table)}`),
  digestNotNegotiated: () => new ReplicaError("sum128-v1 replica integrity was not negotiated"),
  invalidArguments: () => new ReplicaError("replica arguments must be an object"),
  missingKey: (key: string) => new ReplicaError(`replica row is missing key ${debug(key)}`),
  duplicateKey: (key: string) => new ReplicaError(`replica returned duplicate key ${debug(key)}`),
  invalidIdentifier: (value: string) => new ReplicaError(`replica SQL identifier ${debug(value)} is invalid`),
};

/** Rust `{:?}` of a string. */
export function debug(value: string): string {
  let out = '"';
  for (const ch of value) {
    const code = ch.codePointAt(0)!;
    if (ch === '"') out += '\\"';
    else if (ch === "\\") out += "\\\\";
    else if (ch === "\n") out += "\\n";
    else if (ch === "\r") out += "\\r";
    else if (ch === "\t") out += "\\t";
    else if (code === 0) out += "\\0";
    else if (code < 0x20 || code === 0x7f) out += `\\u{${code.toString(16)}}`;
    else out += ch;
  }
  return `${out}"`;
}

/** `quote`: an ASCII identifier of at most 63 characters, double-quoted. */
export function quote(value: string): string {
  if (value.length > 0 && value.length <= 63 && /^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) return `"${value}"`;
  throw errors.invalidIdentifier(value);
}

const isObject = (value: unknown): value is JsonObject => typeof value === "object" && value !== null && !Array.isArray(value);
const fieldOf = (row: JsonObject, column: string): JsonValue | undefined => (Object.hasOwn(row, column) ? row[column] : undefined);

/** `sql_order`: NULL, then numbers (booleans are integers), then text (binary). */
export function sqlOrder(left: JsonValue | undefined, right: JsonValue | undefined): number {
  const cls = (value: JsonValue | undefined) =>
    value === null || value === undefined ? 0 : typeof value === "boolean" || typeof value === "number" ? 1 : typeof value === "string" ? 2 : 3;
  const a = cls(left), b = cls(right);
  if (a !== b) return a - b;
  if (a === 0) return 0;
  if (a === 1) {
    const x = typeof left === "boolean" ? Number(left) : (left as number);
    const y = typeof right === "boolean" ? Number(right) : (right as number);
    return x < y ? -1 : x > y ? 1 : 0;
  }
  if (a === 2) return byteOrder(left as string, right as string);
  const l = JSON.stringify(left), r = JSON.stringify(right);
  return l < r ? -1 : l > r ? 1 : 0;
}

/** `within_boundary`: does `row` sort at or before the window's last row? */
export function withinBoundary(definition: ReplicaDefinition, boundary: readonly [JsonValue, JsonValue], row: JsonValue): boolean {
  const object = isObject(row) ? row : {};
  let order = sqlOrder(fieldOf(object, definition.orderBy) ?? null, boundary[0]);
  if (order === 0) order = sqlOrder(fieldOf(object, definition.key) ?? null, boundary[1]);
  return definition.orderDirection === "asc" ? order <= 0 : order >= 0;
}

/** `collection_filters_admit`: exclusions and argument equalities. */
export function collectionFiltersAdmit(row: JsonValue, args: JsonValue, definition: ReplicaDefinition): boolean {
  if (!isObject(row) || !isObject(args)) return false;
  for (const column of definition.excludeWhenSet) {
    const value = fieldOf(row, column);
    if (value !== undefined && value !== null) return false;
  }
  for (const [column, argument] of definition.equalFilters) {
    if (!jsonEqual(fieldOf(row, column), fieldOf(args, argument))) return false;
  }
  return true;
}

export function visible(plan: VisibilityPlan, resolved: ResolvedVisibility, row: JsonValue): boolean {
  return rowMatches(plan, resolved, row);
}

export function rowInCollection(row: JsonValue, args: JsonValue, definition: ReplicaDefinition, plan: VisibilityPlan, resolved: ResolvedVisibility): boolean {
  return collectionFiltersAdmit(row, args, definition) && visible(plan, resolved, row);
}

/** `project_row`: the declared columns present in the row, in declaration order; the key is required. */
export function projectRow(row: JsonValue, definition: ReplicaDefinition): JsonObject {
  if (!isObject(row)) throw errors.invalidArguments();
  const projected: JsonObject = {};
  for (const column of definition.columns) {
    const value = fieldOf(row, column);
    if (value !== undefined) projected[column] = value;
  }
  if (!Object.hasOwn(projected, definition.key)) throw errors.missingKey(definition.key);
  return projected;
}

/** `row_key`: a scalar key as text. */
export function rowKey(row: JsonValue, key: string): string {
  const value = isObject(row) ? fieldOf(row, key) : undefined;
  if (typeof value === "string") return value;
  if (typeof value === "number") return String(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  throw errors.missingKey(key);
}

/** Whether a truncated window's boundary admits `row` (undefined: no usable boundary). */
export function windowAdmits(definition: ReplicaDefinition, window: ReplicaWindow, row: JsonValue): boolean | undefined {
  if (!window.truncated) return true;
  if (!definition.orderBy || !window.boundary) return undefined;
  return withinBoundary(definition, window.boundary, row);
}

export { fieldOf, isObject };
