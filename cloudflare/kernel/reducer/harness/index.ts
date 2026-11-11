// A Worker around the K07 tenant object for workerd tests and the
// conformance backend `cloudflare-reducer`. It is the K05 socket path
// unchanged (router → gateway → control stand-in / tenant object), with:
//
//  - `KernelControlStub`: K05's control stand-in, plus Rust's /dev/sync order
//    for tenants: every tenant of the project migrates and activates the
//    release before the gateways are told (`moduleReloaded`); a new tenant gets
//    the active release when it is created. It is the deploy path's stand-in
//    until K19 (control/operator) lands.
//  - `HarnessTenant`: the K07 tenant object with plans built in the object.
//    Fine for the small fixture; real releases are built outside (0028 §3.1).
//  - `/_stub/session/tables/<t>` answered from the tenant object (checksums),
//    and `/_stub/reducer/prune` (receipt expiry on demand).
//
// Never deployed with real data: the stub routes need GONVEX_STUB_ADMIN_KEY.

import { ControlStubObject, handleStubRoute, handleWebSocketUpgrade, STUB_PREFIX } from "../../session/index";
import type { Json } from "../../session/decode";
import { type TenantRpc, rpc } from "../../session/rpc";
import { buildReleasePlan, MemoryPlanStore, migrationsFromFiles, type PlanStore } from "../../migrate/index.ts";
import { ReducerTenantObject, type ReducerTenantEnv } from "../object.ts";

export interface HarnessEnv extends ReducerTenantEnv {
  CONTROL_STUB: DurableObjectNamespace<any>;
  TENANT_STUB: DurableObjectNamespace<any>;
  GONVEX_STUB_ADMIN_KEY?: string;
  GONVEX_GATEWAY_SHARDS?: string;
  GONVEX_RUNTIME_VERSION?: string;
  GONVEX_PRESENCE?: string;
}

interface HarnessTenantRpc extends TenantRpc {
  activate(projectId: string, manifest: Record<string, unknown>, planId?: string | null): Promise<{ ok: true }>;
  readTable(table: string): Promise<Record<string, unknown>[]>;
  pruneReceipts(now?: number, force?: boolean, offlineMaxAgeMs?: number | null): Promise<unknown>;
  lastNumbers(): Promise<unknown>;
}

const tenantStub = (env: { TENANT_STUB: DurableObjectNamespace<any> }, tenantId: string) => rpc<HarnessTenantRpc>(env.TENANT_STUB, tenantId);

/** The manifest without the bundle code (the tenant object keeps the rest). */
function withoutCode(manifest: Record<string, unknown>): Record<string, unknown> {
  const module = { ...((manifest.module ?? {}) as Record<string, unknown>) };
  delete module.javascript;
  return { ...manifest, module };
}

export class KernelControlStub extends ControlStubObject {
  private manifests(): SqlStorage {
    const sql = this.ctx.storage.sql;
    sql.exec("CREATE TABLE IF NOT EXISTS k07_manifests (project_id TEXT PRIMARY KEY, manifest TEXT NOT NULL)");
    return sql;
  }

  private stored(projectId: string): Record<string, unknown> | null {
    const row = this.manifests().exec<{ manifest: string }>("SELECT manifest FROM k07_manifests WHERE project_id = ?", projectId).toArray()[0];
    return row ? (JSON.parse(row.manifest) as Record<string, unknown>) : null;
  }

  /** /dev/sync: migrate and activate every tenant of the project, then the reload fan-out (Rust's order). */
  override async deploy(projectId: string, manifest: Parameters<ControlStubObject["deploy"]>[1]): Promise<{ ok: boolean; functionCount: number }> {
    const stored = withoutCode(manifest as Record<string, unknown>);
    this.manifests().exec(
      "INSERT INTO k07_manifests (project_id, manifest) VALUES (?, ?) ON CONFLICT(project_id) DO UPDATE SET manifest = excluded.manifest",
      projectId, JSON.stringify(stored),
    );
    const tenants = this.ctx.storage.sql.exec<{ tenant_id: string }>("SELECT tenant_id FROM tenants WHERE project_id = ?", projectId).toArray();
    for (const { tenant_id } of tenants) await tenantStub(this.env, tenant_id).activate(projectId, stored);
    return super.deploy(projectId, manifest);
  }

  /** A new tenant gets the project's active release before it serves (CONTRACT E.a.6 rollout). */
  override async createTenant(projectId: string, tenantId: string, name: string): Promise<{ id: string }> {
    const created = await super.createTenant(projectId, tenantId, name);
    const manifest = this.stored(projectId);
    if (manifest) await tenantStub(this.env, tenantId).activate(projectId, manifest);
    return created;
  }
}

const plans = new MemoryPlanStore();

/** base64 file contents of `module.files` → text, for `migrationsFromFiles`. */
function migrationFiles(manifest: Record<string, unknown>): Record<string, string> {
  const files = (((manifest.module ?? {}) as Record<string, unknown>).files ?? {}) as Record<string, string>;
  const out: Record<string, string> = {};
  for (const [name, encoded] of Object.entries(files)) {
    if (!name.startsWith("migrations/")) continue;
    const bytes = Uint8Array.from(atob(encoded), (c) => c.charCodeAt(0));
    out[name] = new TextDecoder().decode(bytes);
  }
  return out;
}

export abstract class HarnessTenant extends ReducerTenantObject<HarnessEnv> {
  protected plans(): PlanStore { return plans; }

  /** Builds the release plan here (the fixture is tiny), then migrates and activates. */
  override async activate(projectId: string, manifest: Record<string, unknown>, planId: string | null = null): Promise<{ ok: true }> {
    let id = planId;
    if (id === null) {
      const migrations = await migrationsFromFiles(migrationFiles(manifest));
      const plan = await buildReleasePlan(migrations);
      plans.add(plan);
      id = plan.id;
    }
    return super.activate(projectId, manifest, id);
  }
}

const json = (body: unknown, status = 200) => Response.json(body, { status });

/** The harness Worker's fetch: `/ws` (K05), the stub routes, and the two K07 routes. */
export async function harnessFetch(request: Request, env: HarnessEnv): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname === "/") return json({ ok: true, service: "gonvex-cloudflare-k07-harness" });
  if (url.pathname === "/ws") return handleWebSocketUpgrade(request, env as unknown as Parameters<typeof handleWebSocketUpgrade>[1]);
  const key = env.GONVEX_STUB_ADMIN_KEY;
  const authorized = !!key && request.headers.get("authorization") === `Bearer ${key}`;
  if (url.pathname.startsWith(`${STUB_PREFIX}tables/`) && request.method === "GET") {
    if (!authorized) return json({ error: "unauthorized" }, 401);
    const table = decodeURIComponent(url.pathname.slice(`${STUB_PREFIX}tables/`.length));
    const rows = await tenantStub(env, String(url.searchParams.get("tenant") ?? "")).readTable(table);
    return json({ rows, total: rows.length });
  }
  if (url.pathname === "/_stub/reducer/prune" && request.method === "POST") {
    if (!authorized) return json({ error: "unauthorized" }, 401);
    // `offlineMaxAgeMs` overrides the module's window, so a scenario can expire receipts of a fixture that declares none.
    const body = await request.json<{ tenant: string; now?: number; offlineMaxAgeMs?: number | null }>();
    return json(await tenantStub(env, body.tenant).pruneReceipts(body.now ?? Date.now(), true, body.offlineMaxAgeMs) as Json);
  }
  if (url.pathname === "/_stub/reducer/numbers" && request.method === "GET") {
    if (!authorized) return json({ error: "unauthorized" }, 401);
    return json(await tenantStub(env, String(url.searchParams.get("tenant") ?? "")).lastNumbers() as Json);
  }
  if (url.pathname.startsWith(STUB_PREFIX)) return (await handleStubRoute(request, env as unknown as Parameters<typeof handleStubRoute>[1]))!;
  return json({ error: "not found" }, 404);
}
