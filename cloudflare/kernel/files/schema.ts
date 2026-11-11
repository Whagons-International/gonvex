// `_gonvex_files` (RT/storage.rs:30-44, 580-601) and the tenant-local upload
// tickets (S/control_schema.sql:350-358, moved into the tenant object by
// 0012 R6 with the tenant column implicit). The metadata JSON is Rust's
// `FileMetadata` serialization, field for field.

export const FILES_TABLE_DDL = `CREATE TABLE IF NOT EXISTS _gonvex_files (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  owner_id TEXT NOT NULL DEFAULT '',
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  content_type TEXT NOT NULL DEFAULT '',
  size_bytes INTEGER NOT NULL DEFAULT 0,
  checksum TEXT NOT NULL DEFAULT '',
  visibility TEXT NOT NULL DEFAULT 'private',
  status TEXT NOT NULL DEFAULT 'pending',
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  uploaded_at INTEGER,
  deleted_at INTEGER
)`;

/** Upload signatures are bearer credentials: a successful PUT keeps its ticket consumed, even after a delete. */
export const UPLOADS_TABLE_DDL = `CREATE TABLE IF NOT EXISTS _gonvex_storage_uploads (
  object_key TEXT PRIMARY KEY,
  project_id TEXT NOT NULL DEFAULT '',
  file_id TEXT NOT NULL DEFAULT '',
  consumed_at INTEGER
)`;

export const FILES_SCHEMA: readonly string[] = [
  FILES_TABLE_DDL,
  "CREATE INDEX IF NOT EXISTS _gonvex_files_tenant_idx ON _gonvex_files (tenant_id)",
  "CREATE INDEX IF NOT EXISTS _gonvex_files_owner_idx ON _gonvex_files (owner_id)",
  UPLOADS_TABLE_DDL,
];

export const NOW_MS_SQL = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

export const FILE_COLUMNS = "id,tenant_id,owner_id,bucket,object_key,content_type,size_bytes,checksum,visibility,status,created_at,uploaded_at,deleted_at";

export interface FileRow {
  id: string;
  tenant_id: string;
  owner_id: string;
  bucket: string;
  object_key: string;
  content_type: string;
  size_bytes: number;
  checksum: string;
  visibility: string;
  status: string;
  created_at: number;
  uploaded_at: number | null;
  deleted_at: number | null;
}

/** Rust's `FileMetadata` (camelCase; empty ownerId/contentType/checksum and a null uploadedAt are omitted). */
export interface FileMetadata {
  id: string;
  tenantId: string;
  ownerId?: string;
  bucket: string;
  objectKey: string;
  contentType?: string;
  size: number;
  checksum?: string;
  visibility: string;
  status: string;
  createdAt: string;
  uploadedAt?: string;
}

/** chrono `DateTime<Utc>` serde: RFC 3339, `Z`, fraction only when non-zero (SecondsFormat::AutoSi). */
export function rfc3339(millis: number): string {
  const iso = new Date(millis).toISOString();
  return millis % 1000 === 0 ? iso.replace(/\.000Z$/, "Z") : iso;
}

export function metadataOf(row: FileRow): FileMetadata {
  const out: FileMetadata = { id: row.id, tenantId: row.tenant_id } as FileMetadata;
  if (row.owner_id) out.ownerId = row.owner_id;
  out.bucket = row.bucket;
  out.objectKey = row.object_key;
  if (row.content_type) out.contentType = row.content_type;
  out.size = Number(row.size_bytes);
  if (row.checksum) out.checksum = row.checksum;
  out.visibility = row.visibility;
  out.status = row.status;
  out.createdAt = rfc3339(Number(row.created_at));
  if (row.uploaded_at !== null && row.uploaded_at !== undefined) out.uploadedAt = rfc3339(Number(row.uploaded_at));
  return out;
}

/** `visibility()`: `tenant` and `public` as requested, anything else `private`. */
export function visibilityOf(requested: string): "private" | "tenant" | "public" {
  const value = requested.trim();
  return value === "tenant" || value === "public" ? value : "private";
}
