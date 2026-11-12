// The test Worker: the sandbox classes, and a tenant object with K13's files
// so `importFile` reads through K13's `ActionFiles` and its owner checks (IB1).
import { DurableObject } from "cloudflare:workers";
import type { FilesSession } from "../../kernel/files/access";
import { filesConfigFromEnv } from "../../kernel/files/config";
import { type FilesTenantRpc, filesRpc } from "../../kernel/files/rpc";
import { TenantFiles, type StoredObject } from "../../kernel/files/tenant-files";

export { SandboxContainer, SandboxRegistry } from "../src/index";

export class FilesTenant extends DurableObject<Env> implements FilesTenantRpc {
  private readonly rpc: FilesTenantRpc;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const files = new TenantFiles({
      sql: ctx.storage.sql,
      transactionSync: (closure) => ctx.storage.transactionSync(closure),
      bucket: env.FILES,
      config: filesConfigFromEnv(env as unknown as Record<string, unknown>),
    });
    files.ensureSchema();
    this.rpc = filesRpc(files);
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
  fetch(): Response {
    return new Response("k20 sandbox test worker");
  },
};
