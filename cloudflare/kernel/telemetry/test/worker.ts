/// <reference path="../../../control/src/modules.d.ts" />
/**
 * K15 test Worker: `/dev/*` goes to the control object, as the router does
 * (router/dev/routes.ts) for everything but `/dev/sync`.
 */
import { OperatorControlObject } from "../../../control/operator/object.ts";
import { StubTenantObject } from "../../../control/test-support/stub-tenant.ts";

export { OperatorControlObject, StubTenantObject };

export interface TelemetryTestEnv {
  CONTROL: DurableObjectNamespace<OperatorControlObject>;
  TENANT: DurableObjectNamespace<StubTenantObject>;
}

export default {
  async fetch(request: Request, env: TelemetryTestEnv): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/dev/")) return env.CONTROL.getByName("control").fetch(request);
    return new Response("not found", { status: 404 });
  },
} satisfies ExportedHandler<TelemetryTestEnv>;
