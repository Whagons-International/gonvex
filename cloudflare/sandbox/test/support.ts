// What the tests drive the sandbox through: the service K12 calls, a session
// as K12 passes it, `ctx.sandbox` as the bootstrap builds it
// (gonvex_bootstrap.js:709-722), and K13's real `ActionFiles` for uploads.
import { env } from "cloudflare:workers";
import { filesConfigFromEnv } from "../../kernel/files/config";
import { ActionFiles } from "../../kernel/files/rpc";
import { DEFAULT_SANDBOX_CONFIG, type SandboxConfig } from "../src/config";
import { ContainerBackend } from "../src/container-backend";
import { filesSession, SandboxService, type SandboxFileSource, type SandboxSession } from "../src/service";

let tenantCounter = 0;

/** A fresh tenant per test keeps one test's sandboxes, counts and run slots out of the next. */
export function session(overrides: Partial<{ projectId: string; tenantId: string; accountId: string; memberId: string }> = {}): SandboxSession {
  tenantCounter++;
  const accountId = overrides.accountId ?? "account-1";
  return {
    projectId: overrides.projectId ?? "whagons",
    tenantId: overrides.tenantId ?? `tenant-${tenantCounter}-${crypto.randomUUID().slice(0, 8)}`,
    account: { id: accountId },
    member: { id: overrides.memberId ?? `member-of-${accountId}` },
  };
}

export const ENABLED: SandboxConfig = { ...DEFAULT_SANDBOX_CONFIG, enabled: true };

const actionFiles = (tenantId: string) =>
  new ActionFiles({ tenant: env.TENANT.getByName(tenantId), bucket: env.FILES, config: filesConfigFromEnv(env as unknown as Record<string, unknown>) });

/** K13's `ActionFiles.readFile`, one tenant object per tenant, as the Action Worker builds it. */
export const k13Files: SandboxFileSource = {
  readFile: (who, fileId, maxBytes) => actionFiles(who.tenantId).readFile(who, fileId, maxBytes),
};

/** Stores a file through K13 (`ctx.storage.store`) and answers its id. */
export async function upload(who: SandboxSession, content: Uint8Array | string, options: { visibility?: "private" | "tenant"; contentType?: string } = {}): Promise<string> {
  const bytes = typeof content === "string" ? new TextEncoder().encode(content) : content;
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  const stored = (await actionFiles(who.tenantId).call(filesSession(who), "store", {
    contentBase64: btoa(binary),
    contentType: options.contentType ?? "application/octet-stream",
    visibility: options.visibility ?? "tenant",
    ownerId: who.account.id,
  })) as { id: string };
  return stored.id;
}

export function service(config: Partial<SandboxConfig> = {}, files: SandboxFileSource | undefined = k13Files): SandboxService {
  return new SandboxService({ backend: new ContainerBackend(env), config: { ...ENABLED, ...config }, files });
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

/** Which worker the manager runs: "rust" (the real binaries or image) or "stub" (CI). */
export async function workerKind(): Promise<"rust" | "stub"> {
  const health = (await (await fetch(`${env.SANDBOX_CONTAINER_URL}/healthz`)).json()) as { worker?: string };
  return health.worker === "stub" ? "stub" : "rust";
}
