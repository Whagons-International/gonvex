// Bindings the sandbox objects need. The Worker that hosts them declares the
// three classes as SQLite-backed Durable Objects and one Worker Loader.
import type { SandboxAnalysis } from "./analysis";
import type { SandboxRegistry } from "./registry";
import type { SandboxWorkspace } from "./workspace";

export interface SandboxEnv {
  SANDBOX_WORKSPACE: DurableObjectNamespace<SandboxWorkspace>;
  SANDBOX_ANALYSIS: DurableObjectNamespace<SandboxAnalysis>;
  SANDBOX_REGISTRY: DurableObjectNamespace<SandboxRegistry>;
  LOADER: WorkerLoader;
}

