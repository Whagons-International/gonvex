// Test Worker: the K05 socket path (router, gateway, control stand-in) with
// the K07 tenant object bound as TENANT_STUB, hosting the conformance fixture;
// and a plain SQLite-backed object (REF) for the reference-integrity specs.
import { DurableObject } from "cloudflare:workers";
import { GatewayObject } from "../../session/index";
import type { ModuleExports } from "../../host/src/index.js";
import { HarnessTenant, KernelControlStub, harnessFetch, type HarnessEnv } from "../harness/index.ts";
import { bundleFor } from "./fixture.ts";

export { GatewayObject, KernelControlStub };
export { PublicLimiterObject } from "../../public/index";

/** A plain object: `reference/test` drives its storage through `runInDurableObject`. */
export class RefTestObject extends DurableObject {}

export class TestTenant extends HarnessTenant {
  protected bundle(hash: string): ModuleExports | undefined { return bundleFor(hash); }
}

export default {
  fetch: (request: Request, env: HarnessEnv) => harnessFetch(request, env),
} satisfies ExportedHandler<HarnessEnv>;
