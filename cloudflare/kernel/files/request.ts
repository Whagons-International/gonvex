// `StorageRequest` (RT/storage.rs:82-100): `#[serde(default, deny_unknown_fields)]`
// over the host-call payload, with serde_json's error texts. The runtime
// builds serde_json with `preserve_order`, so the first bad key in the
// payload's own order is the one reported.

export interface StorageRequest {
  fileId: string;
  ttlMs: number;
  contentBase64: string;
  contentType: string;
  size: number;
  visibility: string;
  ownerId: string;
  expiresMs: number;
}

const FIELDS: Record<keyof StorageRequest, "string" | "i64"> = {
  fileId: "string", ttlMs: "i64", contentBase64: "string", contentType: "string",
  size: "i64", visibility: "string", ownerId: "string", expiresMs: "i64",
};
const EXPECTED = "`fileId`, `ttlMs`, `contentBase64`, `contentType`, `size`, `visibility`, `ownerId`, `expiresMs`";

export const emptyRequest = (): StorageRequest => ({ fileId: "", ttlMs: 0, contentBase64: "", contentType: "", size: 0, visibility: "", ownerId: "", expiresMs: 0 });

/** serde's `Unexpected` rendering of a JSON value. */
function unexpected(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return `boolean \`${value}\``;
  if (typeof value === "string") return `string ${JSON.stringify(value)}`;
  if (typeof value === "number") return Number.isInteger(value) ? `integer \`${value}\`` : `floating point \`${value}\``;
  if (Array.isArray(value)) return "sequence";
  return "map";
}

const I64_MAX = 9223372036854775807n;

export class StorageRequestError extends Error {}

/** Parses a payload, or throws `invalid storage payload: …`. `null`/`undefined` is the default request. */
export function parseStorageRequest(payload: unknown): StorageRequest {
  const request = emptyRequest();
  if (payload === null || payload === undefined) return request;
  const fail = (message: string): never => { throw new StorageRequestError(`invalid storage payload: ${message}`); };
  if (typeof payload !== "object" || Array.isArray(payload)) fail(`invalid type: ${unexpected(payload)}, expected struct StorageRequest`);
  for (const [key, value] of Object.entries(payload as Record<string, unknown>)) {
    if (value === undefined) continue; // JSON.stringify drops it before Rust sees the payload
    const kind = (FIELDS as Record<string, "string" | "i64" | undefined>)[key];
    if (!kind) fail(`unknown field \`${key}\`, expected one of ${EXPECTED}`);
    if (kind === "string") {
      if (typeof value !== "string") fail(`invalid type: ${unexpected(value)}, expected a string`);
      (request as unknown as Record<string, unknown>)[key] = value;
    } else {
      if (typeof value !== "number" || !Number.isInteger(value)) fail(`invalid type: ${unexpected(value)}, expected i64`);
      if (BigInt(value as number) > I64_MAX) fail(`invalid value: integer \`${value}\`, expected i64`);
      (request as unknown as Record<string, unknown>)[key] = value;
    }
  }
  return request;
}
