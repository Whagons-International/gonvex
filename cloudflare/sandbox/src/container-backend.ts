// `SandboxBackend` on the tenant's sandbox Container (container.ts). The
// deployment-wide counts are the registry's; everything else is the manager's.
import { unwrap } from "./answer";
import type { ImportResult, SandboxBackend, SandboxExecution, SandboxHandle, SandboxScope } from "./backend";
import type { SandboxConfig } from "./config";
import { REGISTRY_NAME } from "./constants";
import type { SandboxEnv } from "./env";

/** The Container that serves a tenant. */
export const containerName = (scope: { project: string; tenant: string }): string => JSON.stringify([scope.project, scope.tenant]);
const scopeKey = (scope: SandboxScope): string => JSON.stringify([scope.project, scope.tenant, scope.account]);

/** The limits the manager applies, from the Worker's policy. */
const limits = (config: SandboxConfig) => ({
  concurrency: config.concurrency,
  maxExecutions: config.maxExecutions,
  maxFileBytes: config.maxFileBytes,
  maxWorkspaceBytes: config.maxWorkspaceBytes,
  maxOutputBytes: config.maxOutputBytes,
  maxRows: config.maxRows,
  maxHeapBytes: config.maxHeapBytes,
});

export class ContainerBackend implements SandboxBackend {
  constructor(private readonly env: SandboxEnv, private readonly clock: () => number = Date.now) {}

  private container(scope: SandboxScope) {
    return this.env.SANDBOX_CONTAINER.getByName(containerName(scope));
  }

  private async call<T>(scope: SandboxScope, operation: string, args: Record<string, unknown>): Promise<T> {
    return JSON.parse(unwrap(await this.container(scope).call(operation, { scope, ...args }))) as T;
  }

  async create(scope: SandboxScope, request: { ttlMs: number; duckdb: boolean }, config: SandboxConfig): Promise<SandboxHandle> {
    const sandboxId = `sbx_${crypto.randomUUID()}`;
    const refused = await this.env.SANDBOX_REGISTRY.getByName(REGISTRY_NAME).admit({
      sandboxId,
      scope: scopeKey(scope),
      expiresAt: this.clock() + request.ttlMs,
      maxTotal: config.maxTotal,
      maxPerAccount: config.maxPerAccount,
    });
    if (refused !== null) throw new Error(refused);
    try {
      return await this.call<SandboxHandle>(scope, "create", { sandboxId, ttlMs: request.ttlMs, duckdb: request.duckdb });
    } catch (error) {
      await this.env.SANDBOX_REGISTRY.getByName(REGISTRY_NAME).forget(sandboxId).catch(() => undefined);
      throw error;
    }
  }

  async destroy(scope: SandboxScope, sandboxId: string): Promise<{ sandboxId: string; destroyed: true }> {
    const destroyed = await this.call<{ sandboxId: string; destroyed: true }>(scope, "destroy", { sandboxId });
    await this.env.SANDBOX_REGISTRY.getByName(REGISTRY_NAME).forget(sandboxId);
    return destroyed;
  }

  run(scope: SandboxScope, request: { sandboxId: string; code: string; timeoutMs: number }, config: SandboxConfig): Promise<SandboxExecution> {
    return this.call(scope, "run", { ...request, config: limits(config) });
  }

  cancel(scope: SandboxScope, sandboxId: string, executionId: string): Promise<SandboxExecution> {
    return this.call(scope, "cancel", { sandboxId, executionId });
  }

  status(scope: SandboxScope, sandboxId: string, executionId: string): Promise<SandboxExecution> {
    return this.call(scope, "status", { sandboxId, executionId });
  }

  async readFile(scope: SandboxScope, sandboxId: string, path: string, config: SandboxConfig): Promise<Uint8Array> {
    const { contentBase64 } = await this.call<{ contentBase64: string }>(scope, "read", { sandboxId, path, config: limits(config) });
    return Uint8Array.from(atob(contentBase64), (character) => character.charCodeAt(0));
  }

  async writeFile(scope: SandboxScope, sandboxId: string, path: string, content: Uint8Array, config: SandboxConfig): Promise<number> {
    let binary = "";
    for (let i = 0; i < content.length; i += 0x8000) binary += String.fromCharCode(...content.subarray(i, i + 0x8000));
    const { size } = await this.call<{ size: number }>(scope, "write", { sandboxId, path, contentBase64: btoa(binary), config: limits(config) });
    return size;
  }

  async importFile(scope: SandboxScope, request: { sandboxId: string; filename: string; body: ReadableStream<Uint8Array>; size: number }): Promise<ImportResult> {
    const reply = await this.container(scope).importFile({ scope, sandboxId: request.sandboxId, filename: request.filename }, request.body);
    return JSON.parse(unwrap(reply)) as ImportResult;
  }
}
