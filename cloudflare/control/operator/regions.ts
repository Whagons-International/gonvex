/**
 * Data residency in the control catalog: the region each tenant's data must stay in.
 *
 * A stack deployed with `GONVEX_JURISDICTION` places every object it creates in that jurisdiction
 * (kernel/assembly/residency.ts), so every tenant it hosts is in that region. The catalog records the region a tenant
 * was created for, and refuses a tenant whose region the stack cannot place. Placing tenants of different regions on
 * ONE stack (a per-region tenant namespace and files bucket chosen from this record) is the planned next step; see
 * docs/cloudflare/data-residency.md. Until then a region that differs from the stack's jurisdiction is refused.
 */
import type { Sql } from "../src/sql";

/** Regions a tenant may be created for (the jurisdictions both Durable Objects and R2 support). */
export const TENANT_REGIONS = ["eu", "fedramp"] as const;

export const REGION_SCHEMA = `
CREATE TABLE IF NOT EXISTS gonvex_cf_tenant_regions (
  project_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  region TEXT,
  recorded_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  PRIMARY KEY (project_id, tenant_id)
) WITHOUT ROWID;
`;

/** The stack's jurisdiction from the Worker's vars (`null`: none). An unknown value throws (fail closed). */
export function stackRegion(env: { GONVEX_JURISDICTION?: unknown }): string | null {
  const raw = env.GONVEX_JURISDICTION;
  if (raw === undefined || raw === null || raw === "") return null;
  if (typeof raw !== "string" || !(TENANT_REGIONS as readonly string[]).includes(raw)) throw new Error(`GONVEX_JURISDICTION ${JSON.stringify(raw)} is not one of ${TENANT_REGIONS.join(", ")}`);
  return raw;
}

/**
 * The region a new tenant gets, or an error message. `requested` is the client's `region` (absent: the stack's).
 */
export function admitRegion(stack: string | null, requested: unknown): { region: string | null } | { error: string } {
  if (requested === undefined || requested === null || requested === "") return { region: stack };
  if (typeof requested !== "string" || !(TENANT_REGIONS as readonly string[]).includes(requested)) {
    return { error: `region must be one of ${TENANT_REGIONS.join(", ")}` };
  }
  if (requested !== stack) {
    return { error: stack === null
      ? `region ${requested} needs a stack deployed with GONVEX_JURISDICTION=${requested}; this stack places objects without a jurisdiction`
      : `this stack keeps data in ${stack}; a ${requested} tenant needs a stack deployed with GONVEX_JURISDICTION=${requested}` };
  }
  return { region: requested };
}

export function recordRegion(sql: Sql, projectId: string, tenantId: string, region: string | null): void {
  sql.run(`INSERT INTO gonvex_cf_tenant_regions(project_id,tenant_id,region) VALUES($1,$2,$3)
           ON CONFLICT(project_id,tenant_id) DO NOTHING`, projectId, tenantId, region);
}

/** Recorded regions of a project's tenants (or every project's when `projectId` is empty). */
export function regionsOf(sql: Sql, projectId: string): Map<string, string | null> {
  const rows = sql.all<{ project_id: string; tenant_id: string; region: string | null }>(
    "SELECT project_id,tenant_id,region FROM gonvex_cf_tenant_regions WHERE ($1='' OR project_id=$1)", projectId);
  return new Map(rows.map((r) => [`${r.project_id}\u0000${r.tenant_id}`, r.region]));
}

/**
 * The region a tenant's data is in: the recorded one, else the stack's (a tenant created before regions were recorded
 * lives in this stack's objects, which are all in the stack's jurisdiction).
 */
export function tenantRegion(regions: Map<string, string | null>, stack: string | null, projectId: string, tenantId: string): string | null {
  const key = `${projectId}\u0000${tenantId}`;
  return regions.has(key) ? regions.get(key)! : stack;
}
