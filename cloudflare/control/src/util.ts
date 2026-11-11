/** Hashing, token and id helpers with the reference's formats (control.rs:4253-4266, 4867). */
import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";

export function sha256Hex(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

export function sha256(value: string | Uint8Array): Uint8Array {
  return new Uint8Array(createHash("sha256").update(value).digest());
}

export function base64url(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64url");
}

export function fromBase64url(value: string): Uint8Array | null {
  if (!/^[A-Za-z0-9_-]*$/.test(value)) return null;
  // base64 with no padding: a length of 4n+1 cannot be decoded.
  if (value.length % 4 === 1) return null;
  return new Uint8Array(Buffer.from(value, "base64url"));
}

/** `uuid::Uuid::new_v4().simple()`: 32 lowercase hex characters. */
export const uuidSimple = (): string => randomUUID().replaceAll("-", "");

/** `random_id(prefix)`: `<prefix>_<uuid32hex>`. */
export const randomId = (prefix: string): string => `${prefix}_${uuidSimple()}`;

/** `secure_token(kind)`: `gvx_<kind>_<uuid32hex>.<base64url(32 random bytes)>`. */
export const secureToken = (kind: string): string => `gvx_${randomId(kind)}.${base64url(randomBytes(32))}`;

/** native_auth.rs `secure_token(kind)`: `gvx_<kind>_<base64url(32 random bytes)>`. */
export const nativeSecureToken = (kind: string): string => `gvx_${kind}_${base64url(randomBytes(32))}`;

/** store `random_token("dev")` used for grant reconnect credentials. */
export const reconnectToken = (): string => secureToken("dev");

export function constantTimeEqual(left: string | Uint8Array, right: string | Uint8Array): boolean {
  const a = typeof left === "string" ? Buffer.from(left) : Buffer.from(left);
  const b = typeof right === "string" ? Buffer.from(right) : Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}

/**
 * chrono `DateTime<Utc>::to_rfc3339()` for a millisecond timestamp:
 * `2026-11-11T00:00:00+00:00`, with `.mmm` only when the milliseconds are
 * non-zero (SecondsFormat::AutoSi).
 */
export function rfc3339(ms: number): string {
  const iso = new Date(ms).toISOString(); // 2026-11-11T00:00:00.000Z
  const [whole, fraction] = iso.slice(0, -1).split(".");
  return `${whole}${fraction && fraction !== "000" ? `.${fraction}` : ""}+00:00`;
}

/** control.rs `slug`. */
export function slug(value: string): string {
  let out = "";
  let separator = false;
  // `to_ascii_lowercase`: only ASCII letters change case.
  for (const char of value.trim().replace(/[A-Z]/g, (c) => c.toLowerCase())) {
    if (/^[a-z0-9]$/.test(char)) {
      out += char;
      separator = false;
    } else if (out.length > 0 && !separator) {
      out += "-";
      separator = true;
    }
  }
  return out.replace(/^-+|-+$/g, "");
}

/** Length in bytes, as Rust's `str::len`. */
export const byteLength = (value: string): number => new TextEncoder().encode(value).length;
