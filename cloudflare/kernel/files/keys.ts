// Object keys and their URL encoding (RT/storage.rs object_key, safe_segment,
// aws_encode, encode_key). Byte-identical to the reference.

const encoder = new TextEncoder();

/** `safe_segment`: trims, maps every char outside `[A-Za-z0-9._-]` to `_`, then trims `_` and `.` from both ends. */
export function safeSegment(value: string): string {
  let out = "";
  for (const character of value.trim()) out += /^[A-Za-z0-9._-]$/.test(character) ? character : "_";
  return out.replace(/^[_.]+/, "").replace(/[_.]+$/, "");
}

/** `aws_encode`: RFC 3986 unreserved bytes stay, every other UTF-8 byte becomes `%XX` (upper case). */
export function awsEncode(value: string): string {
  let out = "";
  for (const byte of encoder.encode(value)) {
    const unreserved = (byte >= 0x41 && byte <= 0x5a) || (byte >= 0x61 && byte <= 0x7a) || (byte >= 0x30 && byte <= 0x39)
      || byte === 0x2d || byte === 0x5f || byte === 0x2e || byte === 0x7e;
    out += unreserved ? String.fromCharCode(byte) : `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
  }
  return out;
}

/** `encode_key`: each `/`-separated segment through `awsEncode`. */
export function encodeKey(key: string): string {
  return key.split("/").map(awsEncode).join("/");
}

/** `{safe(project)}/{safe(tenant)}/{fileId}` (storage.rs:996-1010). */
export function objectKey(projectId: string, tenantId: string, fileId: string): string {
  return `${safeSegment(projectId)}/${safeSegment(tenantId)}/${fileId}`;
}

export interface ParsedKey {
  project: string;
  tenant: string;
  fileId: string;
}

/**
 * The tenant object that owns a key. The router routes by the tenant segment
 * (tenant objects are `idFromName(tenantId)`). A tenant id that `safeSegment`
 * changes cannot be routed; its lookups find no row and fail closed (404).
 */
export function parseObjectKey(key: string): ParsedKey | undefined {
  const parts = key.split("/");
  if (parts.length !== 3) return undefined;
  const [project, tenant, fileId] = parts as [string, string, string];
  if (!project || !tenant || !fileId) return undefined;
  return { project, tenant, fileId };
}

/** `Uuid::new_v4().simple()`: 32 lower-case hex digits. */
export function newFileId(): string {
  return crypto.randomUUID().replaceAll("-", "");
}
