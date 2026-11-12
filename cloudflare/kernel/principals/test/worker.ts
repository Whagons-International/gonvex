// Test Worker for the K17 specs: the harness (K05 gateways, K06 control
// object, K07 tenant object) with the test fixture's bundle.
import type { ModuleExports } from "../../host/src/index.js";
import { PrincipalsGateway, principalsFetch, type PrincipalsEnv } from "../harness/index.ts";
import { PrincipalsControlObject } from "../harness/control.ts";
import { PrincipalsTenant } from "../harness/tenant.ts";
import { bundleFor } from "./fixture.ts";

export { PrincipalsControlObject, PrincipalsGateway };

export class TestPrincipalsTenant extends PrincipalsTenant {
  protected bundle(hash: string): ModuleExports | undefined { return bundleFor(hash) as ModuleExports | undefined; }
}

export default {
  fetch: (request: Request, env: PrincipalsEnv) => principalsFetch(request, env),
} satisfies ExportedHandler<PrincipalsEnv>;
