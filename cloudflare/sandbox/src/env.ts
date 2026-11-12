// Bindings of the Worker that hosts the sandbox (its own script, 0048 §2.3):
// the per-tenant Container class and the deployment's registry.
import type { SandboxContainer } from "./container";
import type { SandboxRegistry } from "./registry";

export interface SandboxEnv {
  SANDBOX_CONTAINER: DurableObjectNamespace<SandboxContainer>;
  SANDBOX_REGISTRY: DurableObjectNamespace<SandboxRegistry>;
}
