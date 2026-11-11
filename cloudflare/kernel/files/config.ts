// Files configuration, read from Worker vars and secrets.
import type { S3Credentials } from "./sigv4";

export interface FilesConfig {
  /** `GONVEX_PUBLIC_URL`: the origin that serves `/storage/*` (the router). */
  publicUrl: string;
  /**
   * `GONVEX_STORAGE_SECRET`: HMAC key material for proxy URLs. Rust keys them on the S3 secret access key;
   * setting the same value keeps URLs minted by either runtime valid across a cutover.
   */
  secret: string;
  /** `GONVEX_STORAGE_BUCKET`: the R2 bucket name, recorded in `_gonvex_files.bucket`. */
  bucket: string;
  /**
   * Largest declared size that still gets a proxy upload URL. The Worker request-body limit is 100 MB on
   * Free and Pro plans (S4), so larger declared uploads get a direct presigned R2 URL. `GONVEX_STORAGE_PROXY_MAX_BYTES`.
   */
  proxyMaxBytes: number;
  /** R2 S3 API credentials for direct presigned uploads (`GONVEX_S3_*`); absent: uploads over `proxyMaxBytes` are refused. */
  direct?: S3Credentials | undefined;
}

/** Rust's declared-upload and proxy-body cap (`MAX_DIRECT_STORE_BYTES`, `lib.rs:818`). */
export const MAX_UPLOAD_BYTES = 128 << 20;
/** 0012 R26: `ctx.storage.store` decodes at most 32 MiB (Rust: 128 MiB, which a 128 MB isolate cannot hold as base64). */
export const MAX_STORE_BYTES = 32 << 20;
export const DEFAULT_PROXY_MAX_BYTES = 100_000_000;
export const DEFAULT_UPLOAD_SECONDS = 15 * 60;
export const DEFAULT_DOWNLOAD_SECONDS = 10 * 60;

const text = (value: unknown) => (typeof value === "string" ? value.trim() : "");

/** The configuration, or `undefined` when storage is not configured (Rust: "storage is not configured"). */
export function filesConfigFromEnv(env: Record<string, unknown>): FilesConfig | undefined {
  const publicUrl = text(env["GONVEX_PUBLIC_URL"]);
  const secret = text(env["GONVEX_STORAGE_SECRET"]);
  const bucket = text(env["GONVEX_STORAGE_BUCKET"]);
  if (!publicUrl || !secret || !bucket) return undefined;
  const limit = Number(text(env["GONVEX_STORAGE_PROXY_MAX_BYTES"]) || DEFAULT_PROXY_MAX_BYTES);
  const endpoint = text(env["GONVEX_S3_ENDPOINT"]);
  const accessKeyId = text(env["GONVEX_S3_ACCESS_KEY_ID"]);
  const secretAccessKey = text(env["GONVEX_S3_SECRET_ACCESS_KEY"]);
  const direct = endpoint && accessKeyId && secretAccessKey
    ? { endpoint, accessKeyId, secretAccessKey, bucket, region: text(env["GONVEX_S3_REGION"]) || "auto" }
    : undefined;
  return { publicUrl, secret, bucket, proxyMaxBytes: Number.isSafeInteger(limit) && limit > 0 ? limit : DEFAULT_PROXY_MAX_BYTES, direct };
}

/** `millis_or_default`: ms rounded up to whole seconds, or the default; then clamped like a presign (E.d.3). */
export function ttlSeconds(milliseconds: number, defaultSeconds: number): number {
  const seconds = milliseconds <= 0 ? defaultSeconds : Math.ceil(milliseconds / 1000);
  return Math.min(Math.max(seconds, 1), 604_800);
}
