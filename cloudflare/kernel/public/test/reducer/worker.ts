// Test Worker for K16's anonymous-call specs: the K05 socket path with the
// address limiter, and the K16 harness tenant (harness/tenant.ts: K07's
// tenant object with public Query and Action calls composed) bound as
// TENANT_STUB, hosting K07's fixture plus the app's `actions` module.
import { GatewayObject } from "../../../session/index";
import type { ModuleExports } from "../../../host/src/index.js";
import { KernelControlStub, harnessFetch, type HarnessEnv } from "../../../reducer/harness/index.ts";
import { PublicCallsTenant } from "../../harness/tenant.ts";
import { bundleFor } from "./fixture.ts";

export { GatewayObject, KernelControlStub };
export { PublicLimiterObject } from "../../limiter";

export class TestTenant extends PublicCallsTenant {
  protected bundle(hash: string): ModuleExports | undefined { return bundleFor(hash) as ModuleExports | undefined; }
}

export default {
  fetch: (request: Request, env: HarnessEnv) => harnessFetch(request, env),
} satisfies ExportedHandler<HarnessEnv>;
