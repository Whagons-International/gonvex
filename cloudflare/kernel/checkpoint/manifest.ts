// Immutable R2 checkpoint manifest, R2 key scheme, and retention GC (Phase 3).
// Designed so Cloudflare `R2Bucket` satisfies `CheckpointBucket` structurally without wrapper overhead.

export const CHECKPOINT_SCHEMA_VERSION = 1 as const;
export const CHECKPOINT_CONTENT_TYPE_FRAMES = "application/x-wl-frames";
export const CHECKPOINT_CONTENT_TYPE_JSON = "application/json; charset=utf-8";
export const CHECKPOINT_CACHE_IMMUTABLE = "public, max-age=31536000, immutable";
export const CHECKPOINT_CACHE_LATEST = "private, max-age=5";

export interface CheckpointHttpMetadata {
  readonly contentType?: string;
  readonly cacheControl?: string;
}

export interface CheckpointObjectMeta {
  readonly key: string;
  readonly size: number;
  readonly etag: string;
}

export interface CheckpointObjectBody extends CheckpointObjectMeta {
  readonly httpMetadata?: CheckpointHttpMetadata;
  readonly body: ReadableStream<Uint8Array>;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
}

export interface CheckpointListResult {
  readonly objects: readonly CheckpointObjectMeta[];
  readonly truncated: boolean;
  readonly cursor?: string;
}

/**
 * Minimal object-storage interface satisfied structurally by Cloudflare `R2Bucket`
 * and by lightweight in-memory test buckets.
 */
export interface CheckpointBucket {
  put(
    key: string,
    value: Uint8Array | ArrayBuffer | string,
    options?: { readonly httpMetadata?: CheckpointHttpMetadata },
  ): Promise<CheckpointObjectMeta | null>;
  get(key: string): Promise<CheckpointObjectBody | null>;
  head(key: string): Promise<CheckpointObjectMeta | null>;
  list(options?: { readonly prefix?: string; readonly limit?: number; readonly cursor?: string }): Promise<CheckpointListResult>;
  delete(keys: string | string[]): Promise<void>;
}

/** One immutable WLP1 page object stored in R2. */
export interface CheckpointPageRef {
  readonly page: number;
  readonly key: string;
  readonly rows: number;
  readonly bytes: number;
  readonly etag: string;
}

/** Per-table summary inside an immutable checkpoint manifest. */
export interface CheckpointTableManifest {
  readonly tbl: number;
  readonly name: string;
  readonly totalRows: number;
  readonly totalBytes: number;
  readonly pages: readonly CheckpointPageRef[];
}

/** Immutable manifest written once per `(tenantId, epoch, seq)` checkpoint run. */
export interface CheckpointManifest {
  readonly v: typeof CHECKPOINT_SCHEMA_VERSION;
  readonly tenantId: string;
  readonly epoch: number;
  readonly seq: number;
  readonly createdAtMs: number;
  readonly frameWrapped: boolean;
  readonly totalRows: number;
  readonly totalPages: number;
  readonly totalBytes: number;
  readonly tables: readonly CheckpointTableManifest[];
}

/** Mutable pointer updated atomically last after all immutable pages + manifest exist in R2. */
export interface CheckpointLatestPointer {
  readonly v: typeof CHECKPOINT_SCHEMA_VERSION;
  readonly tenantId: string;
  readonly epoch: number;
  readonly seq: number;
  readonly manifestKey: string;
  readonly createdAtMs: number;
  readonly frameWrapped: boolean;
  readonly totalRows: number;
  readonly totalPages: number;
  readonly totalBytes: number;
}

export interface PruneCheckpointsResult {
  readonly keptRuns: number;
  readonly deletedRuns: number;
  readonly deletedKeys: number;
}

const SAFE_TENANT_RE = /^[A-Za-z0-9._-]{1,128}$/;

export function isValidTenantSegment(tenantId: string): boolean {
  return SAFE_TENANT_RE.test(tenantId) && tenantId !== "." && tenantId !== "..";
}

function assertTenant(tenantId: string): void {
  if (!isValidTenantSegment(tenantId)) {
    throw new RangeError(`invalid checkpoint tenantId: ${tenantId}`);
  }
}

function assertNonNegInt(label: string, value: number): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`invalid checkpoint ${label}: ${String(value)}`);
  }
}

export function checkpointPrefix(tenantId: string): string {
  assertTenant(tenantId);
  return `checkpoints/${tenantId}/`;
}

export function checkpointRunPrefix(tenantId: string, epoch: number, seq: number): string {
  assertTenant(tenantId);
  assertNonNegInt("epoch", epoch);
  assertNonNegInt("seq", seq);
  return `checkpoints/${tenantId}/${epoch}/${seq}/`;
}

export function checkpointPageKey(
  tenantId: string,
  epoch: number,
  seq: number,
  tbl: number,
  page: number,
): string {
  assertNonNegInt("tbl", tbl);
  assertNonNegInt("page", page);
  return `${checkpointRunPrefix(tenantId, epoch, seq)}${tbl}-${page}.wlp1`;
}

export function checkpointManifestKey(tenantId: string, epoch: number, seq: number): string {
  return `${checkpointRunPrefix(tenantId, epoch, seq)}manifest.json`;
}

export function checkpointLatestKey(tenantId: string): string {
  return `${checkpointPrefix(tenantId)}latest.json`;
}

export function serializeManifest(manifest: CheckpointManifest): string {
  return JSON.stringify(manifest);
}

export function parseManifest(raw: string): CheckpointManifest {
  const parsed = JSON.parse(raw) as Partial<CheckpointManifest>;
  if (
    parsed === null ||
    typeof parsed !== "object" ||
    parsed.v !== CHECKPOINT_SCHEMA_VERSION ||
    typeof parsed.tenantId !== "string" ||
    !Array.isArray(parsed.tables)
  ) {
    throw new Error("invalid checkpoint manifest payload");
  }
  return parsed as CheckpointManifest;
}

export function serializeLatestPointer(pointer: CheckpointLatestPointer): string {
  return JSON.stringify(pointer);
}

export function parseLatestPointer(raw: string): CheckpointLatestPointer {
  const parsed = JSON.parse(raw) as Partial<CheckpointLatestPointer>;
  if (
    parsed === null ||
    typeof parsed !== "object" ||
    parsed.v !== CHECKPOINT_SCHEMA_VERSION ||
    typeof parsed.tenantId !== "string" ||
    typeof parsed.manifestKey !== "string"
  ) {
    throw new Error("invalid checkpoint latest pointer payload");
  }
  return parsed as CheckpointLatestPointer;
}

interface RunGroup {
  readonly epoch: number;
  readonly seq: number;
  readonly keys: string[];
}

const R2_DELETE_BATCH = 1000;

/**
 * Retention GC for a tenant's R2 checkpoint prefix.
 * Keeps the newest `retainCount` `(epoch, seq)` runs and deletes all older page + manifest objects
 * in batched R2 `delete()` calls. Never deletes `latest.json`.
 */
export async function pruneOldCheckpoints(
  bucket: CheckpointBucket,
  tenantId: string,
  retainCount: number,
): Promise<PruneCheckpointsResult> {
  if (!Number.isInteger(retainCount) || retainCount < 1) {
    throw new RangeError(`retainCount must be an integer >= 1 (got ${String(retainCount)})`);
  }
  const prefix = checkpointPrefix(tenantId);
  const latestKey = checkpointLatestKey(tenantId);
  const groups = new Map<string, RunGroup>();
  const groupList: RunGroup[] = [];

  let cursor: string | undefined;
  for (;;) {
    const listOpts = cursor === undefined ? { prefix, limit: 1000 } : { prefix, limit: 1000, cursor };
    const page = await bucket.list(listOpts);
    for (let i = 0; i < page.objects.length; i++) {
      const key = page.objects[i]!.key;
      if (key === latestKey || !key.startsWith(prefix)) continue;
      const rest = key.slice(prefix.length);
      const s1 = rest.indexOf("/");
      if (s1 <= 0) continue;
      const s2 = rest.indexOf("/", s1 + 1);
      if (s2 <= s1 + 1) continue;
      const epoch = Number(rest.slice(0, s1));
      const seq = Number(rest.slice(s1 + 1, s2));
      if (!Number.isInteger(epoch) || epoch < 0 || !Number.isInteger(seq) || seq < 0) continue;
      const runKey = rest.slice(0, s2);
      let g = groups.get(runKey);
      if (g === undefined) {
        g = { epoch, seq, keys: [] };
        groups.set(runKey, g);
        groupList.push(g);
      }
      g.keys.push(key);
    }
    if (!page.truncated || page.cursor === undefined) break;
    cursor = page.cursor;
  }

  if (groupList.length <= retainCount) {
    return { keptRuns: groupList.length, deletedRuns: 0, deletedKeys: 0 };
  }

  groupList.sort((a, b) => (b.epoch !== a.epoch ? b.epoch - a.epoch : b.seq - a.seq));

  const toDelete: string[] = [];
  for (let i = retainCount; i < groupList.length; i++) {
    const ks = groupList[i]!.keys;
    for (let j = 0; j < ks.length; j++) toDelete.push(ks[j]!);
  }

  let off = 0;
  while (off < toDelete.length) {
    const end = off + R2_DELETE_BATCH < toDelete.length ? off + R2_DELETE_BATCH : toDelete.length;
    await bucket.delete(toDelete.slice(off, end));
    off = end;
  }

  return {
    keptRuns: retainCount,
    deletedRuns: groupList.length - retainCount,
    deletedKeys: toDelete.length,
  };
}
