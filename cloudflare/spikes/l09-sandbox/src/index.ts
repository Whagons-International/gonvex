// L09 candidate: `ctx.sandbox` on Dynamic Workers and Durable Object SQLite. See README.md.
export { DynamicWorkerBackend } from "./dynamic";
export type { SandboxEnv } from "./env";
export { SandboxAnalysis } from "./analysis";
export { RunSlots } from "./slots";
export { SandboxWorkspace } from "./workspace";
export { SandboxRegistry } from "../../../sandbox/src/registry";
export { sandboxConfigFromEnv, SandboxService, type SandboxSession } from "../../../sandbox/src/index";

import { sandboxConfigFromEnv } from "../../../sandbox/src/config";
import { SandboxService, type SandboxFileSource } from "../../../sandbox/src/service";
import { DynamicWorkerBackend } from "./dynamic";
import type { SandboxEnv } from "./env";

export function sandboxFromEnv(env: SandboxEnv & Record<string, unknown>, files?: SandboxFileSource): SandboxService {
  return new SandboxService({ backend: new DynamicWorkerBackend(env), config: sandboxConfigFromEnv(env), files });
}
