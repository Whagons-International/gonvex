// Independent X01 package harness. This is NOT the K22 assembled serving runtime.
export { default, HelloObject, GatewayObject, ControlStubObject, TenantStubObject, PublicLimiterObject } from "../../../test/legacy-worker";
export { GatewayControllerObject } from "../controller";

import { GatewayObject } from "../../session";
import { GatewayControllerObject } from "../controller";
import type { GatewayEnv } from "../../session/placement/gateway";
import type { SessionBindingsEnv } from "../../session/bindings";

type IdOnlyEnv = GatewayEnv & SessionBindingsEnv & {
  ID_ONLY_GATEWAY: DurableObjectNamespace;
  ID_ONLY_CONTROLLER: DurableObjectNamespace;
};

// Controlled public-API case, not a reproduction of the remote failure:
// pinned workerd may retain ctx.id.name after eviction. Reconstruct the actual
// native ID with idFromString, for which the public API guarantees no name.
export class IdOnlyGateway extends GatewayObject {
  constructor(ctx: DurableObjectState, env: IdOnlyEnv) {
    const id = env.ID_ONLY_GATEWAY.idFromString(ctx.id.toString());
    // Keep the native DurableObjectState (the base constructor brand-checks it).
    Object.defineProperty(ctx, "id", { value: id });
    super(ctx, { ...env, GATEWAY: env.ID_ONLY_GATEWAY, GATEWAY_CONTROLLER: env.ID_ONLY_CONTROLLER });
  }
}

export class IdOnlyController extends GatewayControllerObject {
  constructor(ctx: DurableObjectState, env: IdOnlyEnv) { super(ctx, { GATEWAY: env.ID_ONLY_GATEWAY }); }
}

// ---- 04-hang repro: an outlet whose owning object is reset with delivery RPCs in flight -------------------------
import { DurableObject } from "cloudflare:workers";
import { GatewayOutlet, deliveryPressure, setRpcSlotStaleMs } from "../../delivery/outlet";
import type { ReplicaDelivery, ReplicaReceipt } from "../../delivery/protocol";

type OutletEnv = { OUTLET_SENDER: DurableObjectNamespace<OutletSender>; OUTLET_HOLDER: DurableObjectNamespace<OutletHolder> };
const outletPayload = (instance: string): ReplicaDelivery => ({ v: 1, instance, shared: [], sockets: {} });

/** A holder whose delivery RPCs stay in flight until `release` (a gateway that has not answered yet). */
export class OutletHolder extends DurableObject<OutletEnv> {
  private gates: Array<() => void> = [];
  private seen: string[] = [];
  private holding = true;
  async deliverReplica(_tenant: string, payload: ReplicaDelivery): Promise<ReplicaReceipt> {
    this.seen.push(payload.instance);
    if (this.holding) await new Promise<void>((resolve) => this.gates.push(resolve));
    return { v: 1, sockets: 0 };
  }
  release(): number { this.holding = false; const n = this.gates.length; for (const g of this.gates.splice(0)) g(); return n; }
  received(): string[] { return [...this.seen]; }
  hold(): void { this.holding = true; }
}

/** A tenant-like object that owns one `GatewayOutlet` (module-global RPC slots and credits are shared by the isolate). */
export class OutletSender extends DurableObject<OutletEnv> {
  private readonly outlet = new GatewayOutlet(`tenant:${this.ctx.id.toString()}`,
    () => this.env.OUTLET_HOLDER.get(this.env.OUTLET_HOLDER.idFromName("holder")) as never, async () => [],
    this.ctx.id.toString());
  /** `n` payloads to `n` holders: one RPC in flight per holder (the isolate allows 8). */
  async fill(n: number): Promise<ReturnType<typeof deliveryPressure>> {
    for (let i = 0; i < n; i++) void this.outlet.send(`h${i}`, outletPayload(`fill-${i}`)).catch(() => {});
    await new Promise((resolve) => setTimeout(resolve, 50));
    return deliveryPressure();
  }
  /** `k` more payloads for `host`, queued behind whatever it has in flight (not awaited). */
  async enqueueMore(host: string, k: number): Promise<ReturnType<typeof deliveryPressure>> {
    for (let i = 0; i < k; i++) void this.outlet.send(host, outletPayload(`queued-${host}-${i}`)).catch(() => {});
    await new Promise((resolve) => setTimeout(resolve, 10));
    return deliveryPressure();
  }
  /** One payload; whether its RPC completed within `ms`. */
  async sendOne(host: string, ms: number): Promise<{ outcome: string; pressure: ReturnType<typeof deliveryPressure> }> {
    const outcome = await Promise.race([
      this.outlet.send(host, outletPayload(`one-${host}`)).then(() => "delivered", (e: Error) => `rejected: ${e.message}`),
      new Promise<string>((resolve) => setTimeout(() => resolve("timeout"), ms)),
    ]);
    return { outcome, pressure: deliveryPressure() };
  }
  pressure(): ReturnType<typeof deliveryPressure> { return deliveryPressure(); }
  /** The isolate's stale-slot bound (module state shared with every outlet here); returns the previous one. */
  staleMs(ms: number): number { return setRpcSlotStaleMs(ms); }
  /** The platform resets the object (as "exceeded its CPU time limit and was reset" does). */
  die(): void { this.ctx.abort("test: object reset with delivery RPCs in flight"); }
}
