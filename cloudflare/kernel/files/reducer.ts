// Reducer `ctx.storage.getMetadata` (RT/host_calls.rs:768-780 →
// storage.rs reducer_metadata): inside the Reducer's own transaction, through
// K01's session so the statements count and abort like any kernel SQL.
// IB1 / 0012 R12: a private file of someone else reads "file not found".
import type { Identity } from "../host/src/context";
import type { FileMetadataSource } from "../host/src/database-host";
import type { StorageSession } from "../storage/index";
import { FILE_NOT_FOUND, type FilesSession, mayAccess } from "./access";
import { FILE_COLUMNS, type FileRow, metadataOf, NOW_MS_SQL } from "./schema";
import { NOT_CONFIGURED } from "./tenant-files";

export interface ReducerFilesOptions {
  /** `undefined`: storage is not configured (Rust's `require_configured`). */
  bucket: R2Bucket | undefined;
  configured: boolean;
}

function rows(result: { columnNames: readonly string[]; rows: readonly (readonly SqlStorageValue[])[] }): FileRow[] {
  return result.rows.map((values) => Object.fromEntries(result.columnNames.map((name, index) => [name, values[index]])) as unknown as FileRow);
}

export function reducerSession(identity: Identity): FilesSession {
  return {
    projectId: "",
    tenantId: String(identity.tenant?.id ?? ""),
    accountId: String(identity.account?.id ?? ""),
    memberId: String(identity.member?.id ?? ""),
  };
}

/**
 * The `FileMetadataSource` K04 hands to Reducers. The finalize HEAD of a
 * pending file is awaited inside the Reducer's storage transaction, as Rust
 * does it inside its tenant transaction (storage.rs:642-683).
 */
export function reducerFiles(options: ReducerFilesOptions): FileMetadataSource {
  return {
    async getMetadata(fileId: string, session: StorageSession, identity: Identity) {
      if (!options.configured || !options.bucket) throw new Error(NOT_CONFIGURED);
      const caller = reducerSession(identity);
      let row = rows(session.run(`SELECT ${FILE_COLUMNS} FROM _gonvex_files WHERE id=?1 AND tenant_id=?2`, [fileId, caller.tenantId]))[0];
      if (!row || row.deleted_at !== null || !mayAccess(row, caller)) throw new Error(FILE_NOT_FOUND);
      if (row.status === "pending") {
        const head = await options.bucket.head(row.object_key);
        if (head) {
          row = rows(session.run(
            `UPDATE _gonvex_files SET status='uploaded',size_bytes=?1,content_type=COALESCE(NULLIF(?2,''),content_type),checksum=?3,uploaded_at=${NOW_MS_SQL}
             WHERE id=?4 AND status='pending' RETURNING ${FILE_COLUMNS}`,
            [head.size, head.httpMetadata?.contentType ?? "", head.etag.replace(/^"|"$/g, ""), fileId],
          ))[0] ?? row;
        }
      }
      return metadataOf(row);
    },
  };
}
