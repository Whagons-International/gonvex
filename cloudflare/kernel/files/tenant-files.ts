// The tenant object's side of files (RT/storage.rs StorageManager): the
// `_gonvex_files` rows, the single-use upload tickets (0012 R6), URL minting
// and the owner checks of IB1. R2 calls run here only where Rust's do (finalize
// HEAD, delete); upload and download bytes stream through the router, never
// through the tenant object.
import { FILE_NOT_FOUND, type FilesSession, mayAccess, ownerFor, visibilityFor } from "./access";
import { DEFAULT_DOWNLOAD_SECONDS, DEFAULT_UPLOAD_SECONDS, type FilesConfig, MAX_UPLOAD_BYTES, ttlSeconds } from "./config";
import { newFileId, objectKey, parseObjectKey } from "./keys";
import { parseStorageRequest, type StorageRequest } from "./request";
import { FILE_COLUMNS, FILES_SCHEMA, type FileMetadata, type FileRow, metadataOf, NOW_MS_SQL, visibilityOf } from "./schema";
import { proxyUrl, type SignatureScope } from "./signing";
import { objectUrl, presignUrl } from "./sigv4";
import { decodeStoreContent, sha256Hex } from "./store";

export const NOT_CONFIGURED = "storage is not configured";
export const UPLOAD_CONSUMED = "upload already consumed";

/** What a stored object adds to `_gonvex_files` (the bytes are already in R2). */
export interface StoredObject {
  fileId: string;
  size: number;
  checksum: string;
  contentType: string;
  visibility: string;
  ownerId: string;
}

export type ClaimResult = { ok: true; consumedAt: number } | { ok: false; error: string };

/** What the download proxy needs to verify a URL: the row's visibility and owner (no row: deleted or unknown). */
export interface DownloadTarget {
  visibility: string;
  ownerId: string;
}

/** What a reader (sandbox `importFile`) gets after the owner check: the metadata and the object key. */
export interface ReadTarget {
  metadata: FileMetadata;
  objectKey: string;
}

export interface TenantFilesOptions {
  sql: SqlStorage;
  /** `ctx.storage.transactionSync`: groups the statements of one step. */
  transactionSync: <T>(closure: () => T) => T;
  bucket: R2Bucket | undefined;
  /** `undefined`: storage is not configured, and every call fails with Rust's text. */
  config: FilesConfig | undefined;
  /** Wall clock in ms (tests pin it). */
  now?: () => number;
}

/** Statement counters, for the rows-read/written numbers each PR states. */
export interface FilesStats {
  statements: number;
  rowsRead: number;
  rowsWritten: number;
}

/** A tenant object's files. One per object; `ensureSchema` once per object instance. */
export class TenantFiles {
  readonly stats: FilesStats = { statements: 0, rowsRead: 0, rowsWritten: 0 };
  private schemaReady = false;
  private readonly now: () => number;

  constructor(private readonly options: TenantFilesOptions) {
    this.now = options.now ?? (() => Date.now());
  }

  get configured(): boolean { return this.options.config !== undefined && this.options.bucket !== undefined; }

  private get config(): FilesConfig {
    if (!this.options.config) throw new Error(NOT_CONFIGURED);
    return this.options.config;
  }

  private get bucket(): R2Bucket {
    if (!this.options.bucket) throw new Error(NOT_CONFIGURED);
    return this.options.bucket;
  }

  private exec<T extends Record<string, SqlStorageValue>>(sql: string, ...binds: SqlStorageValue[]): T[] {
    const cursor = this.options.sql.exec<T>(sql, ...binds);
    const rows = cursor.toArray();
    this.stats.statements++;
    this.stats.rowsRead += cursor.rowsRead;
    this.stats.rowsWritten += cursor.rowsWritten;
    return rows;
  }

  /**
   * Creates `_gonvex_files` and the ticket table if absent. Rust runs this DDL
   * on every storage call (`ensure_table`); the kernel runs it once per object
   * instance (E.d.1 "provision once"). The tenant object calls it from its
   * constructor so a Reducer's `getMetadata` never runs DDL inside its own
   * transaction.
   */
  ensureSchema(): void {
    if (this.schemaReady) return;
    this.options.transactionSync(() => { for (const statement of FILES_SCHEMA) this.exec(statement); });
    this.schemaReady = true;
  }

  // ---- ctx.storage.call (Actions) ------------------------------------------------

  /** `StorageManager::call`: the payload is parsed before the operation is matched, as in Rust. */
  async call(session: FilesSession, operation: string, payload: unknown): Promise<unknown> {
    if (!this.configured) throw new Error(NOT_CONFIGURED);
    const request = parseStorageRequest(payload);
    this.ensureSchema();
    switch (operation.trim()) {
      case "generateUploadUrl": return this.generateUploadUrl(session, request);
      case "getUrl": return this.downloadUrl(session, request.fileId, true, 0);
      case "generateDownloadUrl": return this.downloadUrl(session, request.fileId, false, request.ttlMs);
      case "getMetadata": return metadataOf(await this.metadata(session, request.fileId, true));
      case "delete": return this.delete(session, request.fileId);
      case "store": {
        const bytes = decodeStoreContent(request.contentBase64);
        const fileId = newFileId();
        const key = objectKey(session.projectId, session.tenantId, fileId);
        const stored = this.prepareStore(session, request, fileId, bytes.byteLength, await sha256Hex(bytes));
        await this.bucket.put(key, bytes, request.contentType ? { httpMetadata: { contentType: request.contentType } } : {});
        return this.recordStored(session, stored);
      }
      default: throw new Error(`unknown storage operation ${JSON.stringify(operation.trim())}`);
    }
  }

  /** The row a `store` will insert, after the owner rule; callers put the bytes only once this passes. */
  prepareStore(session: FilesSession, request: Pick<StorageRequest, "contentType" | "visibility" | "ownerId">, fileId: string, size: number, checksum: string): StoredObject {
    if (!this.configured) throw new Error(NOT_CONFIGURED);
    return {
      fileId,
      size,
      checksum,
      contentType: request.contentType,
      visibility: visibilityFor(session, visibilityOf(request.visibility)),
      ownerId: ownerFor(session, request.ownerId),
    };
  }

  /** Inserts the `uploaded` row of a stored object (storage.rs:296-317). The owner rule is checked again here. */
  recordStored(session: FilesSession, stored: StoredObject): FileMetadata {
    if (!this.configured) throw new Error(NOT_CONFIGURED);
    this.ensureSchema();
    const owner = ownerFor(session, stored.ownerId);
    const visibility = visibilityFor(session, visibilityOf(stored.visibility));
    const key = objectKey(session.projectId, session.tenantId, stored.fileId);
    const [row] = this.exec<FileRow & Record<string, SqlStorageValue>>(
      `INSERT INTO _gonvex_files (id,tenant_id,owner_id,bucket,object_key,content_type,size_bytes,checksum,visibility,status,uploaded_at)
       VALUES (?,?,?,?,?,?,?,?,?,'uploaded',${NOW_MS_SQL}) RETURNING ${FILE_COLUMNS}`,
      stored.fileId, session.tenantId, owner, this.config.bucket, key, stored.contentType, stored.size, stored.checksum, visibility,
    );
    return metadataOf(row!);
  }

  private async generateUploadUrl(session: FilesSession, request: StorageRequest): Promise<unknown> {
    if (request.size > MAX_UPLOAD_BYTES) throw new Error("upload too large");
    const config = this.config;
    const direct = request.size > config.proxyMaxBytes;
    if (direct && !config.direct) throw new Error(`uploads over ${config.proxyMaxBytes} bytes need a direct upload URL, which is not configured`);
    const fileId = newFileId();
    const key = objectKey(session.projectId, session.tenantId, fileId);
    const visibility = visibilityFor(session, visibilityOf(request.visibility));
    const owner = ownerFor(session, request.ownerId);
    const seconds = ttlSeconds(request.expiresMs, DEFAULT_UPLOAD_SECONDS);
    this.options.transactionSync(() => {
      this.exec(
        "INSERT INTO _gonvex_files (id,tenant_id,owner_id,bucket,object_key,content_type,size_bytes,visibility,status) VALUES (?,?,?,?,?,?,?,?,'pending')",
        fileId, session.tenantId, owner, config.bucket, key, request.contentType, Math.max(request.size, 0), visibility,
      );
      // Every proxy upload URL is ticketed (E.d.3: presign mode is no longer an unticketed bearer URL).
      if (!direct) this.exec("INSERT INTO _gonvex_storage_uploads (object_key,project_id,file_id) VALUES (?,?,?)", key, session.projectId, fileId);
    });
    const headers: Record<string, string> = {};
    if (request.contentType) headers["Content-Type"] = request.contentType;
    if (direct) {
      // Over the Worker body limit: a presigned R2 PUT. Single use comes from a signed `If-None-Match: *`
      // (R2 refuses a second PUT to the key), and the signed Content-Length binds the declared size.
      const signed: Record<string, string> = { "if-none-match": "*", "content-length": String(request.size) };
      if (request.contentType) signed["content-type"] = request.contentType;
      const url = await presignUrl(config.direct!, "PUT", objectUrl(config.direct!, key), seconds, new Date(this.now()), signed);
      headers["If-None-Match"] = "*";
      return { fileId, url, method: "PUT", headers };
    }
    const url = await proxyUrl(config.publicUrl, config.secret, key, this.nowSeconds() + seconds, { kind: "upload" });
    return { fileId, url, method: "POST", headers };
  }

  private nowSeconds(): number { return Math.floor(this.now() / 1000); }

  /** `getUrl` (finalize, 10 min) and `generateDownloadUrl` (no finalize, `ttlMs`). IB1: the owner check comes first. */
  private async downloadUrl(session: FilesSession, fileId: string, finalize: boolean, ttlMs: number): Promise<string> {
    const row = await this.metadata(session, fileId, finalize);
    const config = this.config;
    const scope: SignatureScope = row.visibility === "private" ? { kind: "private", ownerId: row.owner_id } : { kind: "download" };
    return proxyUrl(config.publicUrl, config.secret, row.object_key, this.nowSeconds() + ttlSeconds(ttlMs, DEFAULT_DOWNLOAD_SECONDS), scope);
  }

  /** `delete` (storage.rs:249-270): owner check, then the object (missing is fine), then the row. */
  private async delete(session: FilesSession, fileId: string): Promise<null> {
    const row = await this.metadata(session, fileId, false);
    await this.bucket.delete(row.object_key);
    this.exec("DELETE FROM _gonvex_files WHERE id=? AND tenant_id=?", fileId, session.tenantId);
    return null;
  }

  private row(fileId: string, tenantId: string): FileRow | undefined {
    return this.exec<FileRow & Record<string, SqlStorageValue>>(`SELECT ${FILE_COLUMNS} FROM _gonvex_files WHERE id=? AND tenant_id=?`, fileId, tenantId)[0];
  }

  /**
   * `metadata` (storage.rs:604-683) with IB1: an unknown file, a deleted one
   * and a private file of someone else all read "file not found". Finalize
   * does the R2 HEAD first and the update after it (no I/O in a transaction).
   */
  async metadata(session: FilesSession, fileId: string, finalize: boolean): Promise<FileRow> {
    if (!fileId.trim()) throw new Error("fileId is required");
    let row = this.row(fileId, session.tenantId);
    if (!row || row.deleted_at !== null || !mayAccess(row, session)) throw new Error(FILE_NOT_FOUND);
    if (finalize && row.status === "pending") {
      const head = await this.bucket.head(row.object_key);
      if (head) row = this.finalize(fileId, head) ?? row;
    }
    return row;
  }

  /** The finalize UPDATE (storage.rs:657-670); `undefined` if another call finalized it first. */
  private finalize(fileId: string, head: R2Object): FileRow | undefined {
    return this.exec<FileRow & Record<string, SqlStorageValue>>(
      `UPDATE _gonvex_files SET status='uploaded',size_bytes=?,content_type=COALESCE(NULLIF(?,''),content_type),checksum=?,uploaded_at=${NOW_MS_SQL}
       WHERE id=? AND status='pending' RETURNING ${FILE_COLUMNS}`,
      head.size, head.httpMetadata?.contentType ?? "", head.etag.replace(/^"|"$/g, ""), fileId,
    )[0];
  }

  /** Sandbox `importFile` (`read_file`, storage.rs:324-363): finalize, owner check, `uploaded` required. */
  async readTarget(session: FilesSession, fileId: string): Promise<ReadTarget> {
    if (!this.configured) throw new Error(NOT_CONFIGURED);
    this.ensureSchema();
    const row = await this.metadata(session, fileId, true);
    if (row.status !== "uploaded") throw new Error("file is not uploaded");
    return { metadata: metadataOf(row), objectKey: row.object_key };
  }

  // ---- the /storage proxy -------------------------------------------------------------

  /**
   * Claims a single-use upload ticket (storage.rs proxy_put, 382-449): refused
   * when consumed, or when the file row is no longer `pending`. A key with no
   * ticket is refused too: the kernel mints no unticketed proxy upload URL.
   */
  claimUpload(key: string): ClaimResult {
    if (!this.configured) return { ok: false, error: NOT_CONFIGURED };
    this.ensureSchema();
    return this.options.transactionSync((): ClaimResult => {
      const ticket = this.exec<{ project_id: string; file_id: string; consumed_at: number | null }>(
        "SELECT project_id,file_id,consumed_at FROM _gonvex_storage_uploads WHERE object_key=?", key,
      )[0];
      if (!ticket || ticket.consumed_at !== null) return { ok: false, error: UPLOAD_CONSUMED };
      const status = this.exec<{ status: string }>("SELECT status FROM _gonvex_files WHERE id=? AND object_key=? AND deleted_at IS NULL", ticket.file_id, key)[0]?.status;
      if (status !== "pending") return { ok: false, error: UPLOAD_CONSUMED };
      const claimed = this.exec<{ consumed_at: number }>(
        `UPDATE _gonvex_storage_uploads SET consumed_at=${NOW_MS_SQL} WHERE object_key=? AND consumed_at IS NULL RETURNING consumed_at`, key,
      )[0];
      return claimed ? { ok: true, consumedAt: claimed.consumed_at } : { ok: false, error: UPLOAD_CONSUMED };
    });
  }

  /** A failed PUT releases its claim (storage.rs:452-468). */
  releaseUpload(key: string, consumedAt: number): void {
    this.ensureSchema();
    this.exec("UPDATE _gonvex_storage_uploads SET consumed_at=NULL WHERE object_key=? AND consumed_at=?", key, consumedAt);
  }

  /**
   * 0012 R13: the download proxy's row lookup. `null` for an unknown or
   * deleted file. Read-only, so a lookup on a key no tenant owns writes nothing.
   */
  downloadTarget(key: string): DownloadTarget | null {
    const parsed = parseObjectKey(key);
    if (!parsed) return null;
    let row: { visibility: string; owner_id: string; deleted_at: number | null } | undefined;
    try {
      row = this.exec<{ visibility: string; owner_id: string; deleted_at: number | null }>(
        "SELECT visibility,owner_id,deleted_at FROM _gonvex_files WHERE id=? AND object_key=?", parsed.fileId, key,
      )[0];
    } catch (error) {
      if (String((error as Error)?.message).includes("no such table")) return null;
      throw error;
    }
    if (!row || row.deleted_at !== null) return null;
    return { visibility: row.visibility, ownerId: row.owner_id };
  }
}
