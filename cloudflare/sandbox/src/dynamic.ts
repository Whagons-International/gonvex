// `SandboxBackend` on Dynamic Workers and Durable Objects (note 0040).
import { unwrap } from "./answer";
import type { ImportResult, SandboxBackend, SandboxExecution, SandboxHandle, SandboxScope } from "./backend";
import type { SandboxConfig } from "./config";
import { REGISTRY_NAME } from "./constants";
import type { SandboxEnv } from "./env";
import { NOT_FOUND } from "./payload";
import { scopeKey, workspaceName } from "./workspace";

/** Rust's id shape: `sbx_` and a UUID v4. Anything else cannot name a sandbox. */
const SANDBOX_ID = /^sbx_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export class DynamicWorkerBackend implements SandboxBackend {
  constructor(private readonly env: SandboxEnv, private readonly clock: () => number = Date.now) {}

  private workspace(scope: SandboxScope, sandboxId: string) {
    if (!SANDBOX_ID.test(sandboxId)) throw new Error(NOT_FOUND);
    return this.env.SANDBOX_WORKSPACE.getByName(workspaceName(scope, sandboxId));
  }

  async create(scope: SandboxScope, request: { ttlMs: number; duckdb: boolean }, config: SandboxConfig): Promise<SandboxHandle> {
    const sandboxId = `sbx_${crypto.randomUUID()}`;
    const expiresAt = this.clock() + request.ttlMs;
    const refused = await this.env.SANDBOX_REGISTRY.getByName(REGISTRY_NAME).admit({
      sandboxId,
      scope: scopeKey(scope),
      expiresAt,
      maxTotal: config.maxTotal,
      maxPerAccount: config.maxPerAccount,
    });
    if (refused !== null) throw new Error(refused);
    unwrap(await this.workspace(scope, sandboxId).create({ scope, sandboxId, duckdb: request.duckdb, expiresAt }));
    return { sandboxId, expiresAt, duckdb: request.duckdb };
  }

  async destroy(scope: SandboxScope, sandboxId: string): Promise<{ sandboxId: string; destroyed: true }> {
    return unwrap(await this.workspace(scope, sandboxId).destroy(scope));
  }

  async run(scope: SandboxScope, request: { sandboxId: string; code: string; timeoutMs: number }, config: SandboxConfig): Promise<SandboxExecution> {
    return unwrap(await this.workspace(scope, request.sandboxId).run(scope, { code: request.code, timeoutMs: request.timeoutMs }, config));
  }

  async cancel(scope: SandboxScope, sandboxId: string, executionId: string): Promise<SandboxExecution> {
    return unwrap(await this.workspace(scope, sandboxId).cancel(scope, executionId));
  }

  async status(scope: SandboxScope, sandboxId: string, executionId: string): Promise<SandboxExecution> {
    return unwrap(await this.workspace(scope, sandboxId).status(scope, executionId));
  }

  async readFile(scope: SandboxScope, sandboxId: string, path: string, config: SandboxConfig): Promise<Uint8Array> {
    return unwrap(await this.workspace(scope, sandboxId).readFile(scope, path, config));
  }

  async writeFile(scope: SandboxScope, sandboxId: string, path: string, content: Uint8Array, config: SandboxConfig): Promise<number> {
    return unwrap(await this.workspace(scope, sandboxId).writeFile(scope, path, content, config));
  }

  async importFile(scope: SandboxScope, request: { sandboxId: string; filename: string; body: ReadableStream<Uint8Array>; size: number }, config: SandboxConfig): Promise<ImportResult> {
    let workspace;
    try {
      workspace = this.workspace(scope, request.sandboxId);
    } catch (error) {
      await request.body.cancel().catch(() => undefined);
      throw error;
    }
    return unwrap(await workspace.importFile(scope, { filename: request.filename, body: request.body, size: request.size }, config));
  }
}
