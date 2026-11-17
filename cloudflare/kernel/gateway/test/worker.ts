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
