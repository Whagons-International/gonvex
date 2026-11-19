/**
 * X15: the control object's parsed deployed manifest, per project.
 *
 * `gonvex_runtime_manifests.manifest` is ~2 MB of JSON (functions, schema,
 * visibility). Every sign-in `auth` frame used to parse it three times on the
 * singleton control object (`projectModule`, `moduleEpoch`, `moduleInfo`),
 * 25-50 ms of CPU each on Workers, and every other caller queued behind that.
 * Rust holds the deployed module in memory; this keeps the parsed row in the
 * object's memory instead.
 *
 * An entry is dropped when any statement through this `Sql` writes the table
 * (`Sql.manifestWrites`: `/dev/sync`, the operator release, project deletion)
 * and is also checked against the row's `(module_hash, updated_at)`, read on
 * every call, so a changed or deleted row is never served from memory.
 * Values are shared and must not be mutated.
 */
import type { Sql } from "./sql";

export interface CachedManifest {
  readonly hash: string;
  readonly manifest: Record<string, unknown>;
  /** Derived values computed once per row (e.g. the replica epoch per database URL). */
  readonly derived: Map<string, unknown>;
}

interface Entry extends CachedManifest { readonly updatedAt: number; readonly writes: number }

const caches = new WeakMap<Sql, Map<string, Entry>>();

/** The project's deployed manifest row, parsed once per row version; null when there is none. */
export function deployedManifest(sql: Sql, projectId: string): CachedManifest | null {
  let cache = caches.get(sql);
  if (!cache) caches.set(sql, (cache = new Map()));
  const head = sql.get<{ module_hash: string | null; updated_at: number | null }>(
    "SELECT module_hash,updated_at FROM gonvex_runtime_manifests WHERE project_id=$1",
    projectId,
  );
  if (!head) {
    cache.delete(projectId);
    return null;
  }
  const hash = String(head.module_hash ?? "");
  const updatedAt = Number(head.updated_at ?? 0);
  const hit = cache.get(projectId);
  if (hit && hit.writes === sql.manifestWrites && hit.hash === hash && hit.updatedAt === updatedAt) return hit;
  const text = sql.value<string>("SELECT manifest FROM gonvex_runtime_manifests WHERE project_id=$1", projectId);
  if (typeof text !== "string") {
    cache.delete(projectId);
    return null;
  }
  let manifest: Record<string, unknown>;
  try {
    const parsed = JSON.parse(text) as unknown;
    manifest = parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
  } catch {
    manifest = {};
  }
  const entry: Entry = { hash, updatedAt, writes: sql.manifestWrites, manifest, derived: new Map() };
  cache.set(projectId, entry);
  return entry;
}

/** `derive()` once per manifest row version and key. */
export function derivedOf<T>(entry: CachedManifest, key: string, derive: () => T): T {
  if (entry.derived.has(key)) return entry.derived.get(key) as T;
  const value = derive();
  entry.derived.set(key, value);
  return value;
}
