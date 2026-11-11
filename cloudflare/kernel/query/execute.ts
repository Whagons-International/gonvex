/**
 * One structured plan against one snapshot: `execute_live_query_plan` and
 * `execute_in_transaction` (`live_query.rs` 756-1174). Runs inside an open
 * K01 Query invocation (`TenantStorage.invoke("query", …)`), which never
 * commits, so every statement sees the same state.
 */
import type { StorageSession } from "../storage";
import type { TenantSession } from "../session/ports";
import { compilePlan, type Args } from "./compile";
import { JsonNumber, JsonObject, parseJson, stringify, type JsonTree } from "./json";
import type { QueryModule } from "./module";
import { fitParameters } from "./parameters";
import { LiveQueryError, relationTables, type LiveQueryPlan } from "./plan";
import type { QueryVisibility, ResolvedVisibility, VisibilityPlan } from "./visibility";

/** `live query result exceeds the 32 MiB limit` (bytes of the serialized result). */
export const RESULT_LIMIT_BYTES = 32 << 20;

export interface Snapshot {
  readonly result: JsonTree;
  /** `result` as serde_json writes it. */
  readonly text: string;
  readonly epoch: string;
  readonly revision: number;
  readonly resolved: ResolvedVisibility;
  /** Resolutions of the plans reached through `inRelation`, by table. */
  readonly related: ReadonlyMap<string, ResolvedVisibility>;
  /** The visibility fingerprints the result depends on (`resolved`, then each related plan in table order). */
  readonly fingerprint: string;
  readonly stats: ExecutionStats;
}

export interface ExecutionStats {
  statements: number;
  rowsRead: number;
  /** Lists moved into `json_each` to stay under 100 bound parameters. */
  packedLists: number;
}

export interface ExecutionContext {
  readonly db: StorageSession;
  readonly session: TenantSession;
  readonly module: QueryModule;
  readonly visibility: QueryVisibility;
}

const ROW_NOT_FOUND = "no rows returned by a query that expected to return at least one row";

/** The clock row (`_gonvex_sync_clock`), read in the snapshot. */
export function readClock(db: StorageSession): { epoch: string; revision: number } {
  const row = db.run("SELECT epoch, revision FROM _gonvex_sync_clock WHERE singleton = 1").rows[0];
  if (!row) throw new LiveQueryError(ROW_NOT_FOUND, "database");
  return { epoch: String(row[0]), revision: Math.max(0, Number(row[1])) };
}

/** Wraps errors that are not already Rust-shaped (storage and visibility errors carry their Display text). */
function rethrow(error: unknown): never {
  if (error instanceof LiveQueryError) throw error;
  throw new LiveQueryError(error instanceof Error ? error.message : String(error), "database");
}

export function executePlan(context: ExecutionContext, plan: LiveQueryPlan, visibilityPlan: VisibilityPlan, rawArgs: unknown): Snapshot {
  if (!rawArgs || typeof rawArgs !== "object" || Array.isArray(rawArgs)) throw LiveQueryError.notAnObject();
  const args = rawArgs as Args;
  const { db, session, module, visibility } = context;
  const stats: ExecutionStats = { statements: 0, rowsRead: 0, packedLists: 0 };
  const before = { statements: db.stats.statements, rowsRead: db.stats.rowsRead };
  try {
    const clock = readClock(db);
    const vctx = { db, session, plans: module.visibility, revision: clock.revision };
    const resolved = visibility.resolve(vctx, visibilityPlan);
    const related = new Map<string, { plan: VisibilityPlan; resolved: ResolvedVisibility }>();
    for (const table of relationTables(plan.where)) {
      const relatedPlan = module.visibility.get(table);
      if (!relatedPlan) throw LiveQueryError.visibilityMissing(table);
      related.set(table, { plan: relatedPlan, resolved: visibility.resolveRelated(vctx, relatedPlan, resolved) });
    }
    const kinds = db.catalogOf(plan.table).columns;
    const compiled = compilePlan({ plan, visibilityPlan, resolved, args, related, visibility, kinds });
    const window = fitParameters(compiled.statement, compiled.parameters);
    stats.packedLists += window.packedLists;
    const rows = db.run(window.statement, window.parameters).rows.map((row) => parseJson(String(row[0])));
    let total: number | undefined;
    if (compiled.count) {
      const count = fitParameters(compiled.count.statement, compiled.count.parameters);
      stats.packedLists += count.packedLists;
      total = Math.max(0, Number(db.run(count.statement, count.parameters).rows[0]?.[0] ?? 0));
    }
    const result = shapeResult(rows, plan.resultPath ?? [], total, compiled.offset, compiled.limit);
    const text = stringify(result);
    if (new TextEncoder().encode(text).length > RESULT_LIMIT_BYTES) throw LiveQueryError.invalid("live query result exceeds the 32 MiB limit");
    stats.statements = db.stats.statements - before.statements;
    stats.rowsRead = db.stats.rowsRead - before.rowsRead;
    const fingerprint = [resolved.fingerprint, ...[...related.values()].map((entry) => entry.resolved.fingerprint)].join(":");
    return { result, text, epoch: clock.epoch, revision: clock.revision, resolved, related: new Map([...related].map(([table, entry]) => [table, entry.resolved])), fingerprint, stats };
  } catch (error) {
    rethrow(error);
  }
}

/** `shape_result`: rows, or `{a: {b: {page: rows, total, offset, limit}}}` along the result path. */
export function shapeResult(rows: JsonTree[], path: readonly string[], total: number | undefined, offset: number, limit: number): JsonTree {
  if (!path.length) return rows;
  const leaf = new JsonObject().set(path[path.length - 1]!, rows);
  if (total !== undefined) {
    leaf.set("total", new JsonNumber(String(total)));
    leaf.set("offset", new JsonNumber(String(offset)));
    leaf.set("limit", new JsonNumber(String(limit)));
  }
  let value: JsonTree = leaf;
  for (const segment of path.slice(0, -1).reverse()) value = new JsonObject().set(segment, value);
  return value;
}
