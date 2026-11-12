// K20: `ctx.sandbox` on a Cloudflare Container running Rust's sandbox worker
// with DuckDB. See README.md and note 0047.
export type { ExecutionStatus, ImportedTable, ImportResult, SandboxBackend, SandboxExecution, SandboxHandle, SandboxScope } from "./backend";
export { DEFAULT_SANDBOX_CONFIG, sandboxConfigFromEnv, type SandboxConfig } from "./config";
export { ContainerBackend, containerName } from "./container-backend";
export { SandboxContainer, type SandboxContainerEnv } from "./container";
export type { SandboxEnv } from "./env";
export { filesSession, SandboxService, type SandboxFileSource, type SandboxServiceOptions, type SandboxSession } from "./service";
export { SandboxRegistry } from "./registry";

import { sandboxConfigFromEnv } from "./config";
import { ContainerBackend } from "./container-backend";
import type { SandboxEnv } from "./env";
import { SandboxService, type SandboxFileSource } from "./service";

/** The service the Action Worker hands to K12's runtime as `sandbox` (`configureActions`). */
export function sandboxFromEnv(env: SandboxEnv & Record<string, unknown>, files?: SandboxFileSource): SandboxService {
  return new SandboxService({ backend: new ContainerBackend(env), config: sandboxConfigFromEnv(env), files });
}
