// A Worker around the K10 tenant object for workerd tests and the conformance
// backend `cloudflare-delivery`: K07's harness (router → gateway → control
// stand-in → tenant object) with `ReplicaTenantObject` as the tenant class, so
// Reducers, the receipt query and Replica Collections all run in one object.
//
// Never deployed with real data: the stub routes need GONVEX_STUB_ADMIN_KEY.

import { buildReleasePlan, MemoryPlanStore, migrationsFromFiles, type PlanStore } from "../../migrate/index.ts";
import { harnessFetch, type HarnessEnv } from "../../reducer/harness/index.ts";
import { rpc } from "../../session/rpc";
import { ReplicaTenantObject } from "../object.ts";

export { KernelControlStub } from "../../reducer/harness/index.ts";
export type { HarnessEnv };

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

export abstract class ReplicaHarnessTenant extends ReplicaTenantObject<HarnessEnv> {
  protected plans(): PlanStore { return plans; }

  /** Builds the release plan here (the fixture is tiny), then migrates and activates. */
  override async activate(projectId: string, manifest: Record<string, unknown>, planId: string | null = null): Promise<{ ok: true }> {
    let id = planId;
    if (id === null) {
      const plan = await buildReleasePlan(await migrationsFromFiles(migrationFiles(manifest)));
      plans.add(plan);
      id = plan.id;
    }
    return super.activate(projectId, manifest, id);
  }

  /** Test seam: a new replica epoch for the database (a restore or an import without `--keep-epoch`). */
  async setEpoch(epoch: string): Promise<void> {
    this.ctx.storage.sql.exec("UPDATE _gonvex_sync_clock SET epoch = ? WHERE singleton = 1", epoch);
  }

  /** Test seam: the object restarts and loses its memory (an eviction or a crash); gateways keep their sockets. */
  async restart(): Promise<void> {
    this.ctx.abort("conformance: tenant object restart");
  }
}

const json = (body: unknown, status = 200) => Response.json(body, { status });

/** K07's harness routes, plus `/_stub/delivery/stats?tenant=` (the engine's counters). */
export async function deliveryFetch(request: Request, env: HarnessEnv): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname === "/") return json({ ok: true, service: "gonvex-cloudflare-k10-harness" });
  if (url.pathname === "/_stub/delivery/stats" && request.method === "GET") {
    const key = env.GONVEX_STUB_ADMIN_KEY;
    if (!key || request.headers.get("authorization") !== `Bearer ${key}`) return json({ error: "unauthorized" }, 401);
    const tenant = rpc<{ deliveryStats(): Promise<unknown> }>(env.TENANT_STUB, String(url.searchParams.get("tenant") ?? ""));
    return json(await tenant.deliveryStats());
  }
  if ((url.pathname === "/_stub/delivery/epoch" || url.pathname === "/_stub/delivery/restart") && request.method === "POST") {
    const key = env.GONVEX_STUB_ADMIN_KEY;
    if (!key || request.headers.get("authorization") !== `Bearer ${key}`) return json({ error: "unauthorized" }, 401);
    const body = await request.json<{ tenant: string; epoch?: string }>();
    const tenant = rpc<{ setEpoch(epoch: string): Promise<void>; restart(): Promise<void> }>(env.TENANT_STUB, body.tenant);
    if (url.pathname.endsWith("/epoch")) await tenant.setEpoch(String(body.epoch));
    else await tenant.restart().catch(() => undefined);
    return json({ ok: true });
  }
  return harnessFetch(request, env);
}
