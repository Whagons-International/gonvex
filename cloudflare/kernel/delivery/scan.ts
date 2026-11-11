// A collection's visible rows, in order and within its budgets (replica.rs
// `scan_window`, `WindowCollector`), read in keyset pages.
//
// Rust streams one SQL statement from a repeatable-read transaction. A Durable
// Object has one connection and no snapshot across awaits, so the scan reads
// keyset pages of `STATEMENT_ROWS` rows. The caller runs every page of one
// scan while it holds the tenant's invocation queue, so no commit lands
// between pages and they all see one state. Rows are handed to the visitor one
// at a time and never collected here (128 MB isolates; brief: page and yield).

import { jsonObjectExpr } from "../query/compile.ts";
import type { StorageSession } from "../storage/index.ts";
import type { JsonKind } from "../storage/values.ts";
import type { JsonObject, JsonValue } from "../visibility/json.ts";
import type { VisibilityPlan } from "../visibility/plan.ts";
import type { ResolvedVisibility } from "../visibility/resolved.ts";
import { compilePredicate } from "../visibility/sql.ts";
import { canonical } from "./canonical.ts";
import type { ReplicaDefinition } from "./definition.ts";
import { emptyWindow, errors, isObject, quote, rowKey, type ReplicaWindow } from "./window.ts";

/** Rows per SQL statement of a scan. */
export const STATEMENT_ROWS = 256;

/** One admitted row: its key, its projection as SQLite wrote it, its canonical hash and bytes. */
export interface ScannedRow {
  readonly key: string;
  /** JSON text from `json_object` (sent verbatim in snapshot pages). */
  readonly text: string;
  readonly value: JsonObject;
  readonly hash: string;
  readonly bytes: number;
}

/** What one scan needs: the statement without keyset and LIMIT, and its parameters. */
export interface ScanPlan {
  readonly select: string;
  readonly where: string;
  readonly parameters: readonly JsonValue[];
  readonly orderColumn: string | undefined;
  readonly keyColumn: string;
  readonly descending: boolean;
}

/** `scan_window`'s statement, from the member's resolved visibility (SQL form of the plan). */
export function scanPlan(
  definition: ReplicaDefinition, plan: VisibilityPlan, resolved: ResolvedVisibility, rawArgs: JsonValue,
  kinds: ReadonlyMap<string, JsonKind>,
): ScanPlan {
  if (!isObject(rawArgs)) throw errors.invalidArguments();
  const parameters: JsonValue[] = [];
  const predicates = [compilePredicate(plan, resolved, "r", parameters)];
  // Verified support principals have no persisted Member; ordinary sessions keep the live membership check.
  if (!resolved.ephemeralMember) {
    parameters.push(resolved.direct["account.id"] ?? "");
    predicates.push(`EXISTS (SELECT 1 FROM members AS _gonvex_member WHERE _gonvex_member.account_id = $${parameters.length} AND _gonvex_member.status = 'active')`);
  }
  for (const column of definition.excludeWhenSet) predicates.push(`r.${quote(column)} IS NULL`);
  for (const [column, argument] of definition.equalFilters) {
    if (!Object.hasOwn(rawArgs, argument)) throw errors.invalidArguments();
    const value = rawArgs[argument]!;
    parameters.push(value);
    predicates.push(`r.${quote(column)} ${value === null ? "IS" : "="} $${parameters.length}`);
  }
  const columns = definition.columns.map((column) => {
    quote(column);
    return [column, kinds.get(column) ?? "text"] as const;
  });
  quote(definition.key);
  const orderColumn = definition.orderBy ? (quote(definition.orderBy), definition.orderBy) : undefined;
  return {
    select: `SELECT ${jsonObjectExpr("r", columns)} AS _gonvex_row, r.${quote(orderColumn ?? definition.key)} AS _gonvex_order, r.${quote(definition.key)} AS _gonvex_key FROM ${quote(definition.table)} AS r`,
    where: predicates.join(" AND "),
    parameters,
    orderColumn,
    keyColumn: definition.key,
    descending: Boolean(orderColumn) && definition.orderDirection !== "asc",
  };
}

type Position = readonly [SqlStorageValue, SqlStorageValue];

/** The page after `after` (keyset on `(order, key)`, SQLite's NULL placement). */
function pageStatement(scan: ScanPlan, after: Position | undefined, limit: number): { sql: string; parameters: unknown[] } {
  const parameters: unknown[] = [...scan.parameters];
  const order = `r.${quote(scan.orderColumn ?? scan.keyColumn)}`;
  const key = `r.${quote(scan.keyColumn)}`;
  let where = scan.where;
  if (after) {
    if (!scan.orderColumn) {
      parameters.push(after[1]);
      where += ` AND ${key} > $${parameters.length}`;
    } else {
      const [lastOrder, lastKey] = after;
      if (lastOrder === null) {
        parameters.push(lastKey);
        const k = `$${parameters.length}`;
        // NULLs sort first ascending, last descending.
        where += scan.descending ? ` AND (${order} IS NULL AND ${key} < ${k})` : ` AND (${order} IS NOT NULL OR ${key} > ${k})`;
      } else {
        parameters.push(lastOrder, lastKey);
        const o = `$${parameters.length - 1}`, k = `$${parameters.length}`;
        where += scan.descending
          ? ` AND (${order} < ${o} OR (${order} = ${o} AND ${key} < ${k}) OR ${order} IS NULL)`
          : ` AND (${order} > ${o} OR (${order} = ${o} AND ${key} > ${k}))`;
      }
    }
  }
  const direction = scan.descending ? "DESC" : "ASC";
  const orderBy = scan.orderColumn ? `${order} ${direction}, ${key} ${direction}` : `${key} ASC`;
  return { sql: `${scan.select} WHERE ${where} ORDER BY ${orderBy} LIMIT ${limit}`, parameters };
}

/** `WindowCollector`: admits rows one at a time (duplicate keys, row and byte budgets). */
export class WindowCollector {
  readonly window: ReplicaWindow = emptyWindow();
  private readonly seen = new Set<string>();
  private last: readonly [JsonValue, JsonValue] | undefined;

  /** `light`: a paging pass over rows already admitted once; only key, text and order (no hash, no budgets). */
  constructor(private readonly definition: ReplicaDefinition, private readonly light = false) {}

  /** The admitted row, or undefined when a budget truncated the window (stop reading). */
  push(text: string): ScannedRow | undefined {
    const definition = this.definition;
    if (this.light) {
      this.window.rows += 1;
      return { key: "", text, value: {}, hash: "", bytes: 0 };
    }
    const value = JSON.parse(text) as JsonObject;
    const key = rowKey(value, definition.key);
    if (this.seen.has(key)) throw errors.duplicateKey(key);
    this.seen.add(key);
    const window = this.window;
    if (definition.maxRows > 0 && window.rows >= definition.maxRows) {
      window.truncated = true;
      return undefined;
    }
    const { hash, bytes } = canonical(value);
    if (definition.maxBytes > 0 && window.bytes + bytes > definition.maxBytes) {
      window.truncated = true;
      return undefined;
    }
    window.rows += 1;
    window.bytes += bytes;
    if (definition.orderBy) this.last = [Object.hasOwn(value, definition.orderBy) ? value[definition.orderBy]! : null, value[definition.key] ?? null];
    return { key, text, value, hash, bytes };
  }

  finish(): ReplicaWindow {
    if (this.window.truncated) this.window.boundary = this.last;
    return this.window;
  }
}

export interface ScanProgress {
  /** Statements and rows read so far (numbers for the PR). */
  statements: number;
  rowsRead: number;
}

/**
 * Reads the window from the start, statement page by statement page, calling
 * `visit` for each admitted row. `visit` returns false to stop early (the
 * caller has seen enough). `between` runs after each `chunkBytes` of row text
 * (the caller drains what it sent); every read in one call must happen under
 * the tenant's invocation queue.
 */
export async function scanWindow(
  read: <T>(body: (db: StorageSession) => T) => Promise<T>,
  definition: ReplicaDefinition,
  scan: ScanPlan,
  visit: (row: ScannedRow) => boolean | void,
  options: { chunkBytes?: number; between?: () => Promise<void>; progress?: ScanProgress; light?: boolean } = {},
): Promise<ReplicaWindow> {
  const collector = new WindowCollector(definition, options.light ?? false);
  const chunkBytes = options.chunkBytes ?? Number.POSITIVE_INFINITY;
  let after: Position | undefined;
  let done = false;
  while (!done) {
    // One synchronous chunk inside one Query invocation.
    await read((db) => {
      let bytes = 0;
      while (!done && bytes < chunkBytes) {
        const { sql, parameters } = pageStatement(scan, after, STATEMENT_ROWS);
        const result = db.run(sql, parameters);
        if (options.progress) {
          options.progress.statements += 1;
          options.progress.rowsRead += result.rowsRead;
        }
        for (const row of result.rows) {
          const text = row[0];
          after = [row[1] ?? null, row[2] ?? null];
          const admitted = collector.push(typeof text === "string" ? text : String(text));
          if (!admitted || visit(admitted) === false) {
            done = true;
            break;
          }
          bytes += admitted.text.length;
        }
        if (result.rows.length < STATEMENT_ROWS) done = true;
      }
    });
    if (!done && options.between) await options.between();
  }
  return collector.finish();
}
