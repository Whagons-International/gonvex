// `ctx.sandbox` for the Action runtime: K12's `SandboxPort`
// (cloudflare/actions/src/ports.ts). The reference is
// `Runtime::sandbox_call` and `SandboxManager::call` (RT/sandbox.rs:194-686).
import type { SandboxBackend, SandboxScope } from "./backend";
import type { SandboxConfig } from "./config";
import type { FilesSession } from "../../kernel/files/access";
import { asU64, decodeBase64Strict, encodeBase64, exactFields, requiredString, stringField, utf8Length } from "./payload";

/** The parts of K12's `ActionSession` the sandbox reads. */
export interface SandboxSession {
  projectId: string;
  tenantId: string;
  account: { id: string };
  member?: { id: string } | undefined;
}

/**
 * Sandbox `importFile`'s read (`StorageService::read_file`, RT/storage.rs:324-363):
 * K13's `ActionFiles.readFile` (kernel/files/rpc.ts) as it is, with its owner
 * checks (IB1), `uploaded` required and Rust's size text.
 */
export interface SandboxFileSource {
  readFile(session: FilesSession, fileId: string, maxBytes: number): Promise<{ bytes: Uint8Array }>;
}

/** K13's caller: the account id and the tenant Member id (0032 K13 §1). */
export function filesSession(session: SandboxSession): FilesSession {
  return { projectId: session.projectId, tenantId: session.tenantId, accountId: session.account.id, memberId: session.member?.id ?? "" };
}

export interface SandboxServiceOptions {
  backend: SandboxBackend;
  config: SandboxConfig;
  /** Absent: `importFile` fails with Rust's "storage is not configured". */
  files?: SandboxFileSource | undefined;
}

const STORAGE_NOT_CONFIGURED = "storage is not configured";

export class SandboxService {
  constructor(private readonly options: SandboxServiceOptions) {}

  /** K12 `SandboxPort.call`. Throws Rust's message; K12 adds the host-call prefix. */
  async call(operation: string, payload: unknown, session: SandboxSession, duckdb: boolean): Promise<unknown> {
    const { backend, config } = this.options;
    const scope: SandboxScope = { project: session.projectId, tenant: session.tenantId, account: session.account.id };
    // `sandbox_call` routes importFile around the manager's `call`: no policy
    // check and no prune before the storage read (RT/sandbox.rs:661-681).
    if (operation === "importFile") return this.importFile(scope, payload, session, duckdb);
    if (!config.enabled) throw new Error("sandbox is disabled by runtime policy");
    switch (operation) {
      case "create": {
        const fields = exactFields(payload, ["ttlMs"]);
        const ttlMs = asU64(fields["ttlMs"]) ?? config.defaultTtlMs;
        if (ttlMs > config.maxTtlMs) throw new Error("sandbox ttl exceeds runtime policy");
        return backend.create(scope, { ttlMs, duckdb }, config);
      }
      case "destroy": {
        const fields = exactFields(payload, ["sandboxId"]);
        return backend.destroy(scope, requiredString(fields, "sandboxId"), config);
      }
      case "run": {
        const fields = exactFields(payload, ["sandboxId", "code", "timeoutMs"]);
        const sandboxId = requiredString(fields, "sandboxId");
        const code = requiredString(fields, "code");
        if (utf8Length(code) > config.maxCodeBytes) throw new Error("sandbox code exceeds runtime policy");
        const timeoutMs = asU64(fields["timeoutMs"]) ?? config.defaultTimeoutMs;
        if (timeoutMs > config.maxTimeoutMs) throw new Error("sandbox timeout exceeds runtime policy");
        return backend.run(scope, { sandboxId, code, timeoutMs }, config);
      }
      case "cancel":
      case "status": {
        const fields = exactFields(payload, ["sandboxId", "executionId"]);
        const sandboxId = requiredString(fields, "sandboxId");
        const executionId = requiredString(fields, "executionId");
        return operation === "cancel"
          ? backend.cancel(scope, sandboxId, executionId, config)
          : backend.status(scope, sandboxId, executionId, config);
      }
      case "readFile":
      case "readText": {
        const fields = exactFields(payload, ["sandboxId", "path"]);
        const sandboxId = requiredString(fields, "sandboxId");
        const path = requiredString(fields, "path");
        const bytes = await backend.readFile(scope, sandboxId, path, config);
        if (operation === "readFile") return { contentBase64: encodeBase64(bytes), size: bytes.byteLength };
        try {
          return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
        } catch {
          throw new Error("sandbox file is not UTF-8");
        }
      }
      case "writeFile":
      case "writeText": {
        const text = operation === "writeText";
        const fields = exactFields(payload, text ? ["sandboxId", "path", "content"] : ["sandboxId", "path", "contentBase64"]);
        const sandboxId = requiredString(fields, "sandboxId");
        const path = requiredString(fields, "path");
        let content: Uint8Array;
        if (text) {
          content = new TextEncoder().encode(stringField(fields, "content"));
        } else {
          const decoded = decodeBase64Strict(stringField(fields, "contentBase64"));
          if (decoded === null) throw new Error("sandbox contentBase64 is invalid");
          content = decoded;
        }
        if (content.byteLength > config.maxFileBytes) throw new Error("sandbox file exceeds runtime policy");
        const size = await backend.writeFile(scope, sandboxId, path, content, config);
        return { path, size };
      }
      default:
        throw new Error(`unsupported sandbox operation ${JSON.stringify(operation)}`);
    }
  }

  private async importFile(scope: SandboxScope, payload: unknown, session: SandboxSession, duckdb: boolean): Promise<unknown> {
    const { backend, config, files } = this.options;
    if (!duckdb) throw new Error("sandbox importFile requires capabilities.sandbox.duckdb");
    const fields = exactFields(payload, ["sandboxId", "fileId", "filename"]);
    const sandboxId = requiredString(fields, "sandboxId");
    const fileId = requiredString(fields, "fileId");
    const filename = requiredString(fields, "filename");
    if (!files) throw new Error(STORAGE_NOT_CONFIGURED);
    const { bytes } = await files.readFile(filesSession(session), fileId, config.maxFileBytes);
    const body = new Blob([bytes]).stream();
    const size = bytes.byteLength;
    // `import_file` checks the name and the size before it looks the sandbox up.
    if (filename.trim() === "") {
      await body.cancel().catch(() => undefined);
      throw new Error("sandbox importFile requires the original filename");
    }
    if (size > config.maxFileBytes) {
      await body.cancel().catch(() => undefined);
      throw new Error("sandbox import exceeds runtime policy");
    }
    return backend.importFile(scope, { sandboxId, filename, body, size }, config);
  }
}
