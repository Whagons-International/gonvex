// What the tenant object exposes over Durable Object RPC for files, and the
// two callers of it: the Action Worker (K12, `ctx.storage.*` host calls and
// sandbox `importFile`) and the /storage router (router/storage).
//
// The tenant object implements `FilesTenantRpc` by delegating to its
// `TenantFiles` (see `filesRpc` below); K07's tenant object class adds the
// methods, the K13 test tenant shows how (test/worker.ts).
import type { FilesSession } from "./access";
import { type FilesConfig, MAX_STORE_BYTES } from "./config";
import { newFileId, objectKey } from "./keys";
import { parseStorageRequest } from "./request";
import type { FileMetadata } from "./schema";
import { decodeStoreContent, sha256Hex } from "./store";
import { type ClaimResult, type DownloadTarget, NOT_CONFIGURED, type ReadTarget, type StoredObject, TenantFiles } from "./tenant-files";

export interface FilesTenantRpc {
  filesCall(session: FilesSession, operation: string, payload: unknown): Promise<unknown>;
  filesPrepareStore(session: FilesSession, request: { contentType: string; visibility: string; ownerId: string }, fileId: string, size: number, checksum: string): Promise<StoredObject>;
  filesRecordStored(session: FilesSession, stored: StoredObject): Promise<FileMetadata>;
  filesReadTarget(session: FilesSession, fileId: string): Promise<ReadTarget>;
  filesClaimUpload(key: string): Promise<ClaimResult>;
  filesReleaseUpload(key: string, consumedAt: number): Promise<void>;
  filesDownloadTarget(key: string): Promise<DownloadTarget | null>;
}

/** The RPC methods, bound to a tenant object's `TenantFiles`. Spread into the object class or delegate to it. */
export function filesRpc(files: TenantFiles): FilesTenantRpc {
  return {
    filesCall: (session, operation, payload) => files.call(session, operation, payload),
    filesPrepareStore: async (session, request, fileId, size, checksum) => files.prepareStore(session, request, fileId, size, checksum),
    filesRecordStored: async (session, stored) => files.recordStored(session, stored),
    filesReadTarget: (session, fileId) => files.readTarget(session, fileId),
    filesClaimUpload: async (key) => files.claimUpload(key),
    filesReleaseUpload: async (key, consumedAt) => files.releaseUpload(key, consumedAt),
    filesDownloadTarget: async (key) => files.downloadTarget(key),
  };
}

export interface ActionFilesOptions {
  tenant: FilesTenantRpc;
  bucket: R2Bucket | undefined;
  config: FilesConfig | undefined;
}

/**
 * `ctx.storage.*` for an Action (K12). Everything but `store` is one RPC to
 * the tenant object. `store` decodes and writes its bytes here, in the Action
 * Worker: 32 MiB of content is 43 MiB of base64, more than one RPC argument
 * may carry (0022 §4.4), and the tenant object should not hold the bytes.
 */
export class ActionFiles {
  constructor(private readonly options: ActionFilesOptions) {}

  async call(session: FilesSession, operation: string, payload: unknown): Promise<unknown> {
    if (!this.options.config || !this.options.bucket) throw new Error(NOT_CONFIGURED);
    if (operation.trim() !== "store") return this.options.tenant.filesCall(session, operation, payload);
    const request = parseStorageRequest(payload);
    const bytes = decodeStoreContent(request.contentBase64, MAX_STORE_BYTES);
    const checksum = await sha256Hex(bytes);
    const fileId = newFileId();
    // The owner rule runs before any byte is written, so a refused store leaves no object behind.
    const stored = await this.options.tenant.filesPrepareStore(session, { contentType: request.contentType, visibility: request.visibility, ownerId: request.ownerId }, fileId, bytes.byteLength, checksum);
    const key = objectKey(session.projectId, session.tenantId, fileId);
    await this.options.bucket.put(key, bytes, request.contentType ? { httpMetadata: { contentType: request.contentType } } : {});
    try {
      return await this.options.tenant.filesRecordStored(session, stored);
    } catch (error) {
      await this.options.bucket.delete(key).catch(() => undefined);
      throw error;
    }
  }

  /** Sandbox `importFile` (`read_file`): owner check in the tenant, bytes from R2 here, Rust's limits and texts. */
  async readFile(session: FilesSession, fileId: string, maxBytes: number): Promise<{ bytes: Uint8Array; metadata: FileMetadata }> {
    if (!this.options.config || !this.options.bucket) throw new Error(NOT_CONFIGURED);
    const target = await this.options.tenant.filesReadTarget(session, fileId);
    const object = await this.options.bucket.get(target.objectKey);
    if (!object) throw new Error("object storage returned 404 Not Found");
    if (object.size > maxBytes) {
      await object.body.cancel();
      throw new Error("stored file exceeds the sandbox import limit");
    }
    return { bytes: new Uint8Array(await object.arrayBuffer()), metadata: target.metadata };
  }
}

