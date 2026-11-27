// Backups and residency evidence of a stack (kernel Worker). See docs/cloudflare/data-residency.md.
//
// - Cron (`scheduled`, set by deploy.mjs `backups`): snapshots Control and every live tenant into the `BACKUPS` bucket
//   under `<GONVEX_BACKUP_PREFIX|nightly>/<UTC time>/{control,tenants/<id>}` (snapshot.ts) and writes `index.json`
//   there and `<prefix>/latest.json`. Retention is the bucket's lifecycle rule. After it, restore drills (drillRestore).
// - Admin-key routes (404 without the key, like `/_gonvex/k19/topology`):
//     POST /_gonvex/backups/run                 the same backup now; body `{ "prefix"?: "..." }`
//     GET  /_gonvex/backups/latest              `latest.json`
//     POST /_gonvex/backups/restore             body `{ "object": "control" | "tenant", "tenant"?: id, "prefix": "<snapshot>" }`
//                                               fills a FRESH object from a snapshot, verifies it, then resets it
//     POST /_gonvex/backups/drill               body `{ "prefix": "<snapshot>" }`: restore into a scratch object, verify, wipe
//     GET  /_gonvex/residency?tenant=<id>       jurisdiction of the namespaces and, measured inside the objects, their colo

import { stackJurisdiction, type ObjectResidency } from "./residency";
import type { RestoreStep, SnapshotManifest } from "./snapshot";

export interface SnapshotPort {
  operatorSnapshot(prefix: string): Promise<SnapshotManifest>;
  operatorRestoreStep(prefix: string): Promise<RestoreStep>;
  operatorRestoreFinish(): Promise<void>;
  operatorRestoreDiscard(): Promise<void>;
  gonvexResidency(): Promise<ObjectResidency>;
}
interface ControlBackupPort extends SnapshotPort { operatorBackupTenants(): Promise<Array<{ projectId: string; tenantId: string }>> }

export interface BackupEnv {
  GONVEX_ADMIN_KEY?: string;
  GONVEX_JURISDICTION?: string;
  GONVEX_BACKUP_PREFIX?: string;
  /** "0" turns off the drills after each nightly backup. */
  GONVEX_BACKUP_DRILL?: string;
  GONVEX_RUNTIME_VERSION?: string;
  BACKUPS?: R2Bucket;
  CONTROL: DurableObjectNamespace;
  TENANT: DurableObjectNamespace;
}

const CONTROL_NAME = "control";
/** A restore step loop never runs forever: parts are ≤ 4 MiB, so this bounds a restore at tens of GB. */
const MAX_RESTORE_STEPS = 20_000;

function sameText(left: string, right: string): boolean {
  const a = new TextEncoder().encode(left), b = new TextEncoder().encode(right);
  return a.byteLength === b.byteLength && crypto.subtle.timingSafeEqual(a, b);
}

const control = (env: BackupEnv) => (env.CONTROL as { getByName(name: string): unknown }).getByName(CONTROL_NAME) as ControlBackupPort;
const tenant = (env: BackupEnv, id: string) => (env.TENANT as { getByName(name: string): unknown }).getByName(id) as SnapshotPort;

export interface BackupObjectResult { kind: "control" | "tenant"; name: string; prefix: string; ok: boolean; tables?: number; rows?: number; storedBytes?: number; blockedMs?: number; ms?: number; error?: string }
export interface DrillResult { prefix: string; scratch: string; ok: boolean; steps: number; rows?: number; mismatches?: number; quickCheck?: string; error?: string }
export interface BackupIndex { v: 1; stack: string | null; jurisdiction: string | null; source: string | null; prefix: string; startedAt: string; finishedAt: string; ok: boolean; objects: BackupObjectResult[]; drills?: DrillResult[] }

/** Scratch objects of restore drills; never a tenant name Control lists (checked before each drill). */
export const DRILL_PREFIX = "restore-drill-";

/** R2 keys of a backup run (structured: root + fixed names). */
const latestKey = (root: string) => `${root}/latest.json`;
const indexKey = (prefix: string) => `${prefix}/index.json`;
const stamp = (at: Date) => at.toISOString().replace(/\.\d{3}Z$/, "Z").replaceAll(":", "-");

/** Snapshots Control and every tenant Control lists. One failed object does not stop the others; `ok` is all of them. */
/** Restores the snapshot at `prefix` into the fresh scratch tenant object `scratch`, verifies it, then wipes it. */
export async function drillRestore(env: BackupEnv, prefix: string, scratch: string): Promise<DrillResult> {
  if (!scratch.startsWith(DRILL_PREFIX) || !/^[a-z0-9-]{1,120}$/.test(scratch)) return { prefix, scratch, ok: false, steps: 0, error: "invalid scratch name" };
  try {
    if ((await control(env).operatorBackupTenants()).some((t) => t.tenantId === scratch)) return { prefix, scratch, ok: false, steps: 0, error: "scratch name is a tenant" };
    const port = tenant(env, scratch);
    const { steps, result } = await restoreObject(port, prefix, { discard: true });
    const report = result.report;
    return { prefix, scratch, ok: result.phase === "done", steps, ...(report ? { rows: report.rows, mismatches: report.mismatches.length, quickCheck: report.quickCheck } : {}), ...(result.error ? { error: result.error.slice(0, 400) } : {}) };
  } catch (error) {
    return { prefix, scratch, ok: false, steps: 0, error: String((error as Error)?.message ?? error).slice(0, 400) };
  }
}

export async function runBackup(env: BackupEnv, options: { prefix?: string; stack?: string | null; now?: Date; drill?: boolean | "all" } = {}): Promise<BackupIndex> {
  if (!env.BACKUPS) throw new Error("no BACKUPS bucket is bound");
  const started = options.now ?? new Date();
  const root = (env.GONVEX_BACKUP_PREFIX || "nightly").replace(/\/+$/, "");
  const prefix = options.prefix ?? `${root}/${stamp(started)}`;
  const objects: BackupObjectResult[] = [];
  const one = async (kind: "control" | "tenant", name: string, port: SnapshotPort, at: string) => {
    const t0 = Date.now();
    try {
      const m = await port.operatorSnapshot(at);
      objects.push({ kind, name, prefix: at, ok: true, tables: m.totals.tables, rows: m.totals.rows, storedBytes: m.totals.storedBytes, blockedMs: m.blockedMs, ms: Date.now() - t0 });
    } catch (error) {
      objects.push({ kind, name, prefix: at, ok: false, ms: Date.now() - t0, error: String((error as Error)?.message ?? error).slice(0, 500) });
    }
  };
  await one("control", CONTROL_NAME, control(env), `${prefix}/control`);
  let tenants: Array<{ projectId: string; tenantId: string }> = [];
  try { tenants = await control(env).operatorBackupTenants(); }
  catch (error) { objects.push({ kind: "tenant", name: "*", prefix, ok: false, error: `tenant list: ${String((error as Error)?.message ?? error).slice(0, 400)}` }); }
  for (const { tenantId } of tenants) await one("tenant", tenantId, tenant(env, tenantId), `${prefix}/tenants/${tenantId}`);
  // Tested restores: every night the Control snapshot, on Sundays (UTC) every tenant snapshot too, each restored into
  // a scratch object, verified (checksums, schema, integrity) and wiped again.
  const drills: DrillResult[] = [];
  if (options.drill ?? env.GONVEX_BACKUP_DRILL !== "0") {
    const sunday = started.getUTCDay() === 0;
    for (const o of objects) {
      if (!o.ok || (o.kind === "tenant" && !sunday && options.drill !== "all")) continue;
      drills.push(await drillRestore(env, o.prefix, `${DRILL_PREFIX}${stamp(started).toLowerCase().replace(/[^a-z0-9]/g, "")}-${drills.length}`));
    }
  }
  const index: BackupIndex = {
    v: 1, stack: options.stack ?? null, jurisdiction: stackJurisdiction(env), source: env.GONVEX_RUNTIME_VERSION ?? null, prefix,
    startedAt: started.toISOString(), finishedAt: new Date().toISOString(), ok: objects.every((o) => o.ok) && drills.every((d) => d.ok), objects,
    ...(drills.length ? { drills } : {}),
  };
  await env.BACKUPS.put(indexKey(prefix), JSON.stringify(index, null, 2), { httpMetadata: { contentType: "application/json" } });
  if (!options.prefix) await env.BACKUPS.put(latestKey(root), JSON.stringify(index, null, 2), { httpMetadata: { contentType: "application/json" } });
  if (!index.ok) console.error("backup: some objects failed", JSON.stringify(objects.filter((o) => !o.ok)));
  return index;
}

/** Fills one fresh object from a snapshot: steps until done, then resets it so it serves the restored data. */
export async function restoreObject(port: SnapshotPort, prefix: string, options: { discard?: boolean } = {}): Promise<{ steps: number; result: RestoreStep }> {
  let result: RestoreStep = { phase: "schema" };
  let steps = 0;
  while (steps++ < MAX_RESTORE_STEPS) {
    result = await port.operatorRestoreStep(prefix);
    if (result.phase === "done" || result.phase === "failed") break;
  }
  if (options.discard) {
    // A drill (or any discarded restore) leaves nothing behind; the reset surfaces as a failed call.
    try { await port.operatorRestoreDiscard(); } catch { /* expected */ }
  } else if (result.phase === "done") {
    // The object resets itself (its in-memory state predates the restore); the reset surfaces as a failed call.
    try { await port.operatorRestoreFinish(); } catch { /* expected */ }
  }
  return { steps, result };
}

const json = (body: unknown, status = 200) => Response.json(body, { status, headers: { "cache-control": "no-store" } });

/** `/_gonvex/backups/*` and `/_gonvex/residency`; `null` for any other path. */
export async function handleBackupRequest(request: Request, env: BackupEnv, stack: string | null): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== "/_gonvex/residency" && !url.pathname.startsWith("/_gonvex/backups/")) return null;
  if (!env.GONVEX_ADMIN_KEY || !sameText(request.headers.get("authorization") ?? "", `Bearer ${env.GONVEX_ADMIN_KEY}`)) return new Response(null, { status: 404 });
  if (url.pathname === "/_gonvex/residency" && request.method === "GET") {
    const tenantId = url.searchParams.get("tenant");
    const listed = tenantId ? (await control(env).operatorBackupTenants()).some((t) => t.tenantId === tenantId) : false;
    if (tenantId && !listed) return json({ error: "no such tenant" }, 404);
    const idOf = (ns: DurableObjectNamespace, name: string) => { const id = ns.idFromName(name); return { id: id.toString(), jurisdiction: (id as { jurisdiction?: string }).jurisdiction ?? null }; };
    return json({
      stack, jurisdiction: stackJurisdiction(env),
      kernel: { colo: (request as { cf?: { colo?: string } }).cf?.colo ?? null },
      ids: { control: idOf(env.CONTROL, CONTROL_NAME), ...(tenantId ? { tenant: idOf(env.TENANT, tenantId) } : {}) },
      objects: { control: await control(env).gonvexResidency(), ...(tenantId ? { tenant: await tenant(env, tenantId).gonvexResidency() } : {}) },
    });
  }
  if (!env.BACKUPS) return json({ error: "no BACKUPS bucket is bound" }, 503);
  if (url.pathname === "/_gonvex/backups/latest" && request.method === "GET") {
    const root = (env.GONVEX_BACKUP_PREFIX || "nightly").replace(/\/+$/, "");
    const latest = await env.BACKUPS.get(latestKey(root));
    return latest ? new Response(latest.body, { headers: { "content-type": "application/json", "cache-control": "no-store" } }) : json({ error: "no backup yet" }, 404);
  }
  if (request.method !== "POST") return new Response(null, { status: 405 });
  const body = await request.json<Record<string, unknown>>().catch(() => ({} as Record<string, unknown>));
  const prefix = typeof body.prefix === "string" ? body.prefix.replace(/\/+$/, "") : undefined;
  if (prefix !== undefined && (!/^[A-Za-z0-9._\/-]+$/.test(prefix) || prefix.includes(".."))) return json({ error: "invalid prefix" }, 400);
  if (url.pathname === "/_gonvex/backups/run") return json(await runBackup(env, { ...(prefix ? { prefix } : {}), stack, drill: body.drill === "all" ? "all" : body.drill !== false }));
  if (url.pathname === "/_gonvex/backups/drill") {
    if (!prefix) return json({ error: "prefix (a snapshot) is required" }, 400);
    return json(await drillRestore(env, prefix, `${DRILL_PREFIX}${Date.now().toString(36)}`));
  }
  if (url.pathname === "/_gonvex/backups/restore") {
    if (!prefix) return json({ error: "prefix (a snapshot) is required" }, 400);
    const kind = body.object;
    if (kind === "control") return json(await restoreObject(control(env), prefix));
    if (kind === "tenant" && typeof body.tenant === "string" && /^[A-Za-z0-9_-]{1,120}$/.test(body.tenant)) return json(await restoreObject(tenant(env, body.tenant), prefix));
    return json({ error: "object must be control, or tenant with a tenant id" }, 400);
  }
  return new Response(null, { status: 404 });
}
