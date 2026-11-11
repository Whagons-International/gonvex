/**
 * Session layer (package K05): the `/ws` path. The Worker entry re-exports
 * the Durable Object classes from here and routes `/ws` to
 * `handleWebSocketUpgrade`. See README.md.
 */
import { gatewayBindings } from "./bindings";
import { GatewayObjectBase, type GatewayEnv } from "./placement/gateway";
import type { SessionBindingsEnv } from "./bindings";
import { type TenantRpc, rpc } from "./rpc";

export { handleWebSocketUpgrade } from "./router";
export { handleStubRoute, STUB_PREFIX } from "./stub/routes";
export { ControlStubObject } from "./stub/control";
export { TenantStubObject } from "./stub/tenant";

/** The gateway object bound as `GATEWAY` (placement: `placement/gateway.ts`). */
export class GatewayObject extends GatewayObjectBase<GatewayEnv & SessionBindingsEnv> {
  protected bindings() { return gatewayBindings; }

  /** Benchmark seam: times `n` sequential gateway -> tenant RPCs (one hop). */
  async hopToTenant(tenant: string, n: number): Promise<number[]> {
    const stub = rpc<TenantRpc>(this.env.TENANT_STUB, tenant);
    await stub.ping();
    const out: number[] = [];
    for (let i = 0; i < n; i++) {
      const t = performance.now();
      await stub.ping();
      out.push(performance.now() - t);
    }
    return out;
  }
}
