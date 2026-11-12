// What delivery reads of the active module generation: Replica Collection
// definitions by path and visibility plans by table (modules.rs
// `ReplicaCollectionDefinition`, `ProjectModule.visibility`), from the stored
// `/dev/sync` manifest.

import { parsePlan, type VisibilityPlan } from "../visibility/plan.ts";

/** `ReplicaCollectionDefinition` (serde camelCase, defaults for absent fields). */
export interface ReplicaDefinition {
  readonly table: string;
  readonly key: string;
  readonly columns: readonly string[];
  /** column → argument name. Iterated in BTreeMap (byte) order. */
  readonly equalFilters: ReadonlyArray<readonly [string, string]>;
  readonly excludeWhenSet: readonly string[];
  readonly orderBy: string;
  readonly orderDirection: string;
  readonly mode: string;
  readonly maxRows: number;
  readonly maxBytes: number;
}

export interface ReplicaFunction {
  readonly path: string;
  readonly kind: string;
  readonly delivery: string;
  readonly replica: ReplicaDefinition | undefined;
}

export interface ReplicaModule {
  readonly projectId: string;
  readonly artifactHash: string;
  readonly functions: ReadonlyMap<string, ReplicaFunction>;
  readonly visibility: ReadonlyMap<string, VisibilityPlan>;
}

const text = (value: unknown): string => (typeof value === "string" ? value : "");
const count = (value: unknown): number => (typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.trunc(value) : 0);
const strings = (value: unknown): string[] => (Array.isArray(value) ? value.filter((v): v is string => typeof v === "string") : []);

export function parseDefinition(raw: unknown): ReplicaDefinition | undefined {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const r = raw as Record<string, unknown>;
  const filters = r.equalFilters && typeof r.equalFilters === "object" && !Array.isArray(r.equalFilters)
    ? Object.entries(r.equalFilters as Record<string, unknown>).filter((e): e is [string, string] => typeof e[1] === "string")
    : [];
  filters.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  return {
    table: text(r.table),
    key: text(r.key),
    columns: strings(r.columns),
    equalFilters: filters,
    excludeWhenSet: strings(r.excludeWhenSet),
    orderBy: text(r.orderBy),
    orderDirection: text(r.orderDirection),
    mode: text(r.mode),
    maxRows: count(r.maxRows),
    maxBytes: count(r.maxBytes),
  };
}

/** The replica view of a stored manifest (`manifest.functions` or `manifest.module.functions`). */
export function replicaModule(projectId: string, manifest: Record<string, unknown>): ReplicaModule {
  const module = (manifest.module ?? {}) as Record<string, unknown>;
  const table = (manifest.functions ?? module.functions ?? {}) as Record<string, Record<string, unknown>>;
  const functions = new Map<string, ReplicaFunction>();
  for (const [path, raw] of Object.entries(table)) {
    functions.set(path, { path, kind: text(raw.kind), delivery: text(raw.delivery), replica: parseDefinition(raw.replica) });
  }
  const plans = (manifest.visibility ?? module.visibility ?? {}) as Record<string, unknown>;
  const visibility = new Map<string, VisibilityPlan>();
  for (const [tableName, plan] of Object.entries(plans)) visibility.set(tableName, parsePlan(plan));
  return { projectId, artifactHash: text(module.hash) || text(manifest.artifactHash), functions, visibility };
}

/** A definition's identity (one shared definition per generation and path in Rust). */
export function bounded(definition: ReplicaDefinition): boolean {
  return definition.maxRows > 0 || definition.maxBytes > 0;
}
