// Payload checks with RT/sandbox.rs's texts (712-752) and serde_json's
// number semantics. Every check runs in Rust's order, so the first error a
// module sees is the one Rust would report.

export const NOT_FOUND = "sandbox was not found";
export const PATH_ESCAPES = "sandbox path must be relative and cannot escape the workspace";

/** `exact_fields`: the payload is an object with no field outside `fields`. */
export function exactFields(value: unknown, fields: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("sandbox payload must be an object");
  const object = value as Record<string, unknown>;
  // serde_json has `preserve_order` (deno_core enables it): keys in document order.
  const unsupported = Object.keys(object).find((key) => !fields.includes(key));
  if (unsupported !== undefined) throw new Error(`sandbox payload contains unsupported field ${JSON.stringify(unsupported)}`);
  return object;
}

/** `required_string_allow_empty`. */
export function stringField(payload: Record<string, unknown>, key: string): string {
  const value = payload[key];
  if (typeof value !== "string") throw new Error(`${key} must be a string`);
  return value;
}

/** `required_string`. */
export function requiredString(payload: Record<string, unknown>, key: string): string {
  const value = stringField(payload, key);
  if (value === "") throw new Error(`${key} is required`);
  return value;
}

/**
 * `Value::as_u64`: only a JSON integer from 0 to 2^64-1. A float, a negative
 * number or any other type is "absent", so Rust falls back to the default.
 * A number at or above 1e21 reaches serde as exponent notation, a float.
 */
export function asU64(value: unknown): number | undefined {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0 || value >= 1e21) return undefined;
  return value;
}

/**
 * `safe_file` (and the worker's `file_path`): a relative path of normal
 * components. Rust's `Path::components` drops empty segments and interior
 * `.` segments, keeps a leading `.` (rejected), and rejects `..` and a
 * leading `/`. Answers the normalized key the file is stored under.
 */
export function workspacePath(relative: string): string {
  if (relative === "" || relative.startsWith("/")) throw new Error(PATH_ESCAPES);
  const parts = relative.split("/");
  if (parts[0] === ".") throw new Error(PATH_ESCAPES);
  const normal = parts.filter((part) => part !== "" && part !== ".");
  if (normal.length === 0 || normal.some((part) => part === "..")) throw new Error(PATH_ESCAPES);
  return normal.join("/");
}

const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

/** The base64 crate's STANDARD engine: padded, no whitespace, canonical trailing bits. */
export function decodeBase64Strict(value: string): Uint8Array | null {
  if (!BASE64.test(value)) return null;
  let binary: string;
  try {
    binary = atob(value);
  } catch {
    return null;
  }
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  // Non-canonical trailing bits ("QQ==" vs "QR==") are an error in the crate.
  if (encodeBase64(bytes) !== value) return null;
  return bytes;
}

export function encodeBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(binary);
}

export const utf8Length = (value: string): number => new TextEncoder().encode(value).byteLength;

/** `bounded(value, max)`: the first `max` characters (Unicode scalar values). */
export function bounded(value: string, max: number): string {
  let out = "";
  let count = 0;
  for (const character of value) {
    if (count++ >= max) break;
    out += character;
  }
  return out;
}
