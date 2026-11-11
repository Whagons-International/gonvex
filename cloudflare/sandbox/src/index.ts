// K20: `ctx.sandbox` on Cloudflare. See README.md and note 0040.
export type { ExecutionStatus, ImportedTable, ImportResult, SandboxBackend, SandboxExecution, SandboxHandle, SandboxScope } from "./backend";
export { DEFAULT_SANDBOX_CONFIG, PLATFORM_ISOLATE_BYTES, sandboxConfigFromEnv, type SandboxConfig } from "./config";
export { DynamicWorkerBackend } from "./dynamic";
export type { SandboxEnv } from "./env";
export { r2FileSource, type FilesReadTarget } from "./files";
export { SandboxService, type SandboxFileSource, type SandboxServiceOptions, type SandboxSession } from "./service";
export { SandboxAnalysis } from "./analysis";
export { SandboxRegistry } from "./registry";
export { SandboxWorkspace } from "./workspace";

import { sandboxConfigFromEnv } from "./config";
import { DynamicWorkerBackend } from "./dynamic";
import type { SandboxEnv } from "./env";
import { SandboxService, type SandboxFileSource } from "./service";

/** The service a Worker hands to K12's `ActionRuntime` as `sandbox`. */
export function sandboxFromEnv(env: SandboxEnv & Record<string, unknown>, files?: SandboxFileSource): SandboxService {
  return new SandboxService({ backend: new DynamicWorkerBackend(env), config: sandboxConfigFromEnv(env), files });
}
