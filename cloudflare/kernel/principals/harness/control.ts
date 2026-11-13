// K17's fixture-only activation extension of the promoted runtime adapter.
import { SessionControlObject } from "../../assembly/control";
import { rpc, type GatewayRpc } from "../../session/rpc";
interface HarnessTenantRpc { activate(project: string, manifest: Record<string, unknown>, planId?: string | null): Promise<{ ok: true }> }
interface HarnessControlEnv { TENANT: DurableObjectNamespace; GATEWAY: DurableObjectNamespace; GONVEX_GATEWAY_SHARDS?: string }
function withoutCode(manifest: Record<string, unknown>) {
  const module = { ...((manifest.module ?? {}) as Record<string, unknown>) }; delete module.javascript;
  return { ...manifest, module };
}
export class PrincipalsControlObject extends SessionControlObject {
  private get harnessEnv(): HarnessControlEnv { return this.env as unknown as HarnessControlEnv; }

  private tenant(tenantId: string): HarnessTenantRpc {
    return rpc<HarnessTenantRpc>(this.harnessEnv.TENANT as DurableObjectNamespace<any>, tenantId);
  }

  /** `/dev/sync`: store, migrate and activate every tenant, then tell the gateways (`ModuleReloaded`). */
  override async devSaveManifest(projectId: string, manifest: Record<string, unknown>) {
    const previous = this.manifestOf(projectId) !== null;
    const saved = await super.devSaveManifest(projectId, manifest);
    const stored = withoutCode(manifest);
    const tenants = this.sql.all("SELECT tenant_id FROM gonvex_runtime_tenants WHERE project_id=$1 AND deleted_at IS NULL", projectId);
    for (const row of tenants) await this.tenant(String(row.tenant_id)).activate(projectId, stored);
    if (previous) {
      const info = await this.moduleInfo(projectId);
      const shards = Number(this.harnessEnv.GONVEX_GATEWAY_SHARDS ?? 8) || 8;
      await Promise.all(Array.from({ length: shards }, (_, i) => rpc<GatewayRpc>(this.harnessEnv.GATEWAY as DurableObjectNamespace<any>, `gw-${i}`).moduleReloaded(projectId, info)));
    }
    return saved;
  }

  /** A new tenant gets the project's active release before it serves. */
  override async devCreateTenant(projectId: string, tenantId: string, name: string) {
    const created = await super.devCreateTenant(projectId, tenantId, name);
    const manifest = this.manifestOf(projectId);
    if (manifest) await this.tenant(tenantId).activate(projectId, withoutCode(manifest));
    return created;
  }
}
