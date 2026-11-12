// Bindings of the L09 candidate: three SQLite-backed objects per deployment
// class, the run slots per tenant (0048 §2.3) and one Worker Loader.
import type { SandboxRegistry } from "../../../sandbox/src/registry";
import type { SandboxAnalysis } from "./analysis";
import type { RunSlots } from "./slots";
import type { SandboxWorkspace } from "./workspace";

export interface SandboxEnv {
  SANDBOX_WORKSPACE: DurableObjectNamespace<SandboxWorkspace>;
  SANDBOX_ANALYSIS: DurableObjectNamespace<SandboxAnalysis>;
  SANDBOX_REGISTRY: DurableObjectNamespace<SandboxRegistry>;
  SANDBOX_SLOTS: DurableObjectNamespace<RunSlots>;
  LOADER: WorkerLoader;
}

/** The compatibility date of the Dynamic Worker that runs module code. */
export const RUNNER_COMPATIBILITY_DATE = "2026-10-23";

export { REGISTRY_NAME } from "../../../sandbox/src/constants";

/** The platform's isolate memory limit, which is what stops a run here (0047 §L09). */
export const PLATFORM_ISOLATE_BYTES = 128 * 1024 * 1024;
