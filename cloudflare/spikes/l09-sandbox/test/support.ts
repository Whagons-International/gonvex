// What the tests drive the sandbox through: the service K12 calls, a session
// as K12 passes it, `ctx.sandbox` as the bootstrap builds it
// (gonvex_bootstrap.js:709-722) and a stand-in for K13's file read.
import { env } from "cloudflare:workers";
import { DEFAULT_SANDBOX_CONFIG, type SandboxConfig } from "../../../sandbox/src/config";
import { DynamicWorkerBackend } from "../src/dynamic";
import { SandboxService, type SandboxFileSource, type SandboxSession } from "../../../sandbox/src/service";
import type { FilesSession } from "../../../kernel/files/access";

let tenantCounter = 0;

/** A fresh tenant per test keeps the per-account limit of one test out of the next. */
export function session(overrides: Partial<{ projectId: string; tenantId: string; accountId: string }> = {}): SandboxSession {
  tenantCounter++;
  return {
    projectId: overrides.projectId ?? "whagons",
    tenantId: overrides.tenantId ?? `tenant-${tenantCounter}-${crypto.randomUUID().slice(0, 8)}`,
    account: { id: overrides.accountId ?? "account-1" },
  };
}

export const ENABLED: SandboxConfig = { ...DEFAULT_SANDBOX_CONFIG, enabled: true };

export function service(config: Partial<SandboxConfig> = {}, files?: SandboxFileSource): SandboxService {
  return new SandboxService({ backend: new DynamicWorkerBackend(env), config: { ...ENABLED, ...config }, files });
}

/** K13's `read_file`: IB1 owner check on private files, `uploaded` required, Rust's size text. */
export class MemoryFiles implements SandboxFileSource {
  private readonly files = new Map<string, { bytes: Uint8Array; ownerId: string; visibility: string; tenantId: string }>();

  add(fileId: string, content: Uint8Array | string, options: { tenantId: string; ownerId?: string; visibility?: string }): void {
    this.files.set(fileId, {
      bytes: typeof content === "string" ? new TextEncoder().encode(content) : content,
      ownerId: options.ownerId ?? "",
      visibility: options.visibility ?? "tenant",
      tenantId: options.tenantId,
    });
  }

  async readFile(session: FilesSession, fileId: string, maxBytes: number) {
    const file = this.files.get(fileId);
    if (!file || file.tenantId !== session.tenantId) throw new Error("file not found");
    if (file.visibility === "private" && file.ownerId !== session.accountId && file.ownerId !== session.memberId) throw new Error("file not found");
    if (file.bytes.byteLength > maxBytes) throw new Error("stored file exceeds the sandbox import limit");
    return { bytes: file.bytes };
  }
}

/** Rust's host-call failure as JS sees it (CONTRACT B.2a shared mechanics). */
export class HostCallError extends Error {}

/** `ctx.sandbox` of an Action declaring `sandbox: { duckdb }`. */
export function ctxSandbox(sandboxService: SandboxService, who: SandboxSession, duckdb = true) {
  const call = async (operation: string, payload: unknown): Promise<any> => {
    try {
      return await sandboxService.call(operation, payload ?? null, who, duckdb);
    } catch (error) {
      throw new HostCallError(`host call failed: sandbox host call failed [host_call_failed]: ${(error as Error).message}`);
    }
  };
  return {
    create: (options: Record<string, unknown> = {}) => call("create", options),
    destroy: (sandboxId: string) => call("destroy", { sandboxId }),
    run: (sandboxId: string, options: Record<string, unknown>) => call("run", { sandboxId, ...(options ?? {}) }),
    cancel: (sandboxId: string, executionId: string) => call("cancel", { sandboxId, executionId }),
    status: (sandboxId: string, executionId: string) => call("status", { sandboxId, executionId }),
    readFile: (sandboxId: string, path: string) => call("readFile", { sandboxId, path }),
    writeFile: (sandboxId: string, path: string, contentBase64: string) => call("writeFile", { sandboxId, path, contentBase64 }),
    readText: (sandboxId: string, path: string) => call("readText", { sandboxId, path }),
    writeText: (sandboxId: string, path: string, content: string) => call("writeText", { sandboxId, path, content: String(content) }),
    importFile: (sandboxId: string, options: Record<string, unknown>) => call("importFile", { sandboxId, ...(options ?? {}) }),
  };
}

export type CtxSandbox = ReturnType<typeof ctxSandbox>;

/** The message K12 hands the module for a sandbox failure. */
export const failure = (message: string) => `host call failed: sandbox host call failed [host_call_failed]: ${message}`;
