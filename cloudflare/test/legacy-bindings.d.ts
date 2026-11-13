// Only the retained P01/K05 unit-test Worker has these fixture bindings.
import type { HelloObject } from "../src/hello";
import type { ControlStubObject } from "../kernel/session/stub/control";
import type { TenantStubObject } from "../kernel/session/stub/tenant";
import type { GatewayObject } from "../kernel/session/index";
export interface LegacyBindings {
  HELLO: DurableObjectNamespace<HelloObject>;
  CONTROL_STUB: DurableObjectNamespace<ControlStubObject>;
  TENANT_STUB: DurableObjectNamespace<TenantStubObject>;
  GONVEX_STUB_ADMIN_KEY?: string;
}
export interface LegacyWorkerBindings extends LegacyBindings { GATEWAY: DurableObjectNamespace<GatewayObject>; PUBLIC_LIMITER: DurableObjectNamespace<any>; GONVEX_RUNTIME_VERSION: string }
declare global { namespace Cloudflare { interface Env extends LegacyBindings {} } }
