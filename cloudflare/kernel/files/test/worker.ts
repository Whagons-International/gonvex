// Test Worker: a tenant object that hosts K13's files the way the kernel's
// tenant object will (TenantFiles + the FilesTenantRpc methods + K04's module
// host with the Reducer metadata source), and the /storage router in front.
import { DurableObject } from "cloudflare:workers";
import { TenantStorage } from "../../storage/index";
import { ModuleRegistry, TenantModuleHost, type FunctionContract, type ModuleArtifact } from "../../host/src/index";
import type { FilesSession } from "../access";
import { filesConfigFromEnv } from "../config";
import { reducerFiles } from "../reducer";
import { type FilesTenantRpc, filesRpc } from "../rpc";
import type { StoredObject } from "../tenant-files";
import { TenantFiles } from "../tenant-files";
import { handleStorageRequest, STORAGE_PREFIX } from "../../../router/storage/index";

const any = { kind: "any" };

/** One Reducer: `ctx.storage.getMetadata` (non-interactive, so the host grants `storage`). */
export function filesArtifact(): ModuleArtifact {
  const functions: FunctionContract[] = [
    { path: "files.metadata", kind: "reducer", export: "files_metadata", argsSchema: any, resultSchema: any, metadata: { interactive: false } },
  ];
  return {
    moduleId: "k13", generation: 1, artifactHash: "k13-1", clientContract: 1, functions,
    exports: { files_metadata: (ctx: { storage: { getMetadata(id: string): Promise<unknown> } }, args: { id: string }) => ctx.storage.getMetadata(args.id) },
    schema: { tables: {} },
  };
}

export class TestFilesTenant extends DurableObject<Env> implements FilesTenantRpc {
  readonly files: TenantFiles;
  readonly host: TenantModuleHost;
  private readonly rpc: FilesTenantRpc;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const config = filesConfigFromEnv(env as unknown as Record<string, unknown>);
    this.files = new TenantFiles({ sql: ctx.storage.sql, transactionSync: (closure) => ctx.storage.transactionSync(closure), bucket: env.FILES, config });
    this.files.ensureSchema();
    this.rpc = filesRpc(this.files);
    const registry = new ModuleRegistry("k13", {});
    this.host = new TenantModuleHost({ storage: new TenantStorage(ctx.storage), registry, files: reducerFiles({ bucket: env.FILES, configured: this.files.configured }) });
    registry.install(filesArtifact());
  }

  filesCall(session: FilesSession, operation: string, payload: unknown) { return this.rpc.filesCall(session, operation, payload); }
  filesPrepareStore(session: FilesSession, request: { contentType: string; visibility: string; ownerId: string }, fileId: string, size: number, checksum: string) {
    return this.rpc.filesPrepareStore(session, request, fileId, size, checksum);
  }
  filesRecordStored(session: FilesSession, stored: StoredObject) { return this.rpc.filesRecordStored(session, stored); }
  filesReadTarget(session: FilesSession, fileId: string) { return this.rpc.filesReadTarget(session, fileId); }
  filesClaimUpload(key: string) { return this.rpc.filesClaimUpload(key); }
  filesReleaseUpload(key: string, consumedAt: number) { return this.rpc.filesReleaseUpload(key, consumedAt); }
  filesDownloadTarget(key: string) { return this.rpc.filesDownloadTarget(key); }
}

export default {
  fetch(request, env): Promise<Response> | Response {
    const url = new URL(request.url);
    if (url.pathname.startsWith(STORAGE_PREFIX)) {
      return handleStorageRequest(request, {
        bucket: env.FILES,
        config: filesConfigFromEnv(env as unknown as Record<string, unknown>),
        tenant: (tenantId) => env.TENANT.getByName(tenantId) as unknown as FilesTenantRpc,
      });
    }
    return new Response("k13 files test worker", { status: 404 });
  },
} satisfies ExportedHandler<Env>;
