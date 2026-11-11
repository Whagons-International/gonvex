// The /storage/{*key} routes (RT/lib.rs:560-566, 653-885) on the router Worker.
//
//   GET  (and HEAD)  signed download: row lookup in the tenant object (0012 R13), then an R2 range GET, streamed
//   POST, PUT        signed single-use upload: ticket claim in the tenant object, then an R2 conditional PUT
//   OPTIONS          preflight: 204 with a valid upload signature, else 403
//
// Bytes never pass through the tenant object. Status codes, bodies and headers
// are Rust's; the differences are IB1's (README.md).
import { type FilesConfig, MAX_STORE_BYTES, MAX_UPLOAD_BYTES } from "../../kernel/files/config";
import { parseObjectKey } from "../../kernel/files/keys";
import { protectStorageResponse, storageCors } from "../../kernel/files/media";
import type { FilesTenantRpc } from "../../kernel/files/rpc";
import { type SignatureScope, verifyProxy } from "../../kernel/files/signing";
import { UPLOAD_CONSUMED } from "../../kernel/files/tenant-files";

export const STORAGE_PREFIX = "/storage/";

export interface StorageRouteOptions {
  bucket: R2Bucket | undefined;
  config: FilesConfig | undefined;
  /** The tenant object for a tenant id (`idFromName(tenantId)`). */
  tenant: (tenantId: string) => FilesTenantRpc;
  /** Unix seconds (tests pin it). */
  nowSeconds?: () => number;
}

const INVALID_SIGNATURE = "invalid or expired storage signature";

function text(status: number, body: string, cors = true): Response {
  const response = new Response(body, { status, headers: { "content-type": "text/plain; charset=utf-8" } });
  if (cors) storageCors(response.headers);
  return response;
}

function json(status: number, body: unknown): Response {
  const response = Response.json(body, { status });
  storageCors(response.headers);
  return response;
}

interface ProxyQuery {
  exp: number;
  sig: string;
  upload: number | undefined;
}

/** Rust's integer `FromStr` errors, as serde_urlencoded reports them. */
function parseInteger(value: string, min: bigint, max: bigint): number | string {
  if (value === "") return "cannot parse integer from empty string";
  const body = value[0] === "+" || (value[0] === "-" && min < 0n) ? value.slice(1) : value;
  if (body === "" || !/^[0-9]+$/.test(body)) return "invalid digit found in string";
  const parsed = BigInt(value[0] === "+" ? body : value);
  if (parsed > max) return "number too large to fit in target type";
  if (parsed < min) return "number too small to fit in target type";
  return Number(parsed);
}

/**
 * `Query<StorageProxyQuery>` (`exp: i64`, `sig: String`, `upload: Option<u8>`):
 * axum answers a bad query with 400 "Failed to deserialize query string: …",
 * before the handler, so without the storage CORS headers.
 */
function parseQuery(url: URL): ProxyQuery | Response {
  const seen = new Map<string, string>();
  for (const [name, value] of url.searchParams) {
    if (name !== "exp" && name !== "sig" && name !== "upload") continue;
    if (seen.has(name)) return text(400, `Failed to deserialize query string: duplicate field \`${name}\``, false);
    seen.set(name, value);
  }
  const fail = (message: string) => text(400, `Failed to deserialize query string: ${message}`, false);
  let exp: number | undefined;
  let upload: number | undefined;
  for (const [name, value] of seen) {
    if (name === "exp") {
      const parsed = parseInteger(value, -(2n ** 63n), 2n ** 63n - 1n);
      if (typeof parsed === "string") return fail(`exp: ${parsed}`);
      exp = parsed;
    } else if (name === "upload") {
      const parsed = parseInteger(value, 0n, 255n);
      if (typeof parsed === "string") return fail(`upload: ${parsed}`);
      upload = parsed;
    }
  }
  if (exp === undefined) return fail("missing field `exp`");
  if (!seen.has("sig")) return fail("missing field `sig`");
  return { exp, sig: seen.get("sig")!, upload };
}

/** axum's `Path<String>` percent-decodes the wildcard. */
function keyOf(url: URL): string | undefined {
  try {
    return decodeURIComponent(url.pathname.slice(STORAGE_PREFIX.length));
  } catch {
    return undefined;
  }
}

export async function handleStorageRequest(request: Request, options: StorageRouteOptions): Promise<Response> {
  const url = new URL(request.url);
  const key = keyOf(url);
  if (key === undefined) return text(400, "Invalid URL: Cannot parse percent-encoded path", false);
  const method = request.method.toUpperCase();
  if (method !== "GET" && method !== "HEAD" && method !== "POST" && method !== "PUT" && method !== "OPTIONS") {
    return new Response(null, { status: 405, headers: { allow: "GET,HEAD,POST,PUT,OPTIONS" } });
  }
  const query = parseQuery(url);
  if (query instanceof Response) return query;
  const now = options.nowSeconds?.() ?? Math.floor(Date.now() / 1000);
  if (!options.config || !options.bucket) {
    // Rust signs with an empty secret when storage is unconfigured and then fails at S3; nothing verifies here.
    return text(403, INVALID_SIGNATURE);
  }
  const config = options.config;
  if (method === "OPTIONS") {
    const valid = query.upload === 1 && await verifyProxy(config.secret, key, query.exp, query.sig, { kind: "upload" }, now);
    if (!valid) return text(403, INVALID_SIGNATURE);
    const response = new Response(null, { status: 204 });
    storageCors(response.headers);
    return response;
  }
  if (method === "POST" || method === "PUT") return upload(request, key, query, now, options.bucket, config, options);
  return download(request, key, query, now, options.bucket, config, options, method === "HEAD");
}

async function download(request: Request, key: string, query: ProxyQuery, now: number, bucket: R2Bucket, config: FilesConfig, options: StorageRouteOptions, head: boolean): Promise<Response> {
  if (query.upload !== undefined || query.exp <= now || query.sig.length === 0) return text(403, INVALID_SIGNATURE);
  const parsed = parseObjectKey(key);
  let target = null;
  try {
    target = parsed ? await options.tenant(parsed.tenant).filesDownloadTarget(key) : null;
  } catch (error) {
    return text(502, String((error as Error)?.message ?? error));
  }
  // IB1 / R13: a private file opens only with a URL minted for its owner.
  const scope: SignatureScope = target?.visibility === "private" ? { kind: "private", ownerId: target.ownerId } : { kind: "download" };
  if (!await verifyProxy(config.secret, key, query.exp, query.sig, scope, now)) return text(403, INVALID_SIGNATURE);
  // A deleted file: Rust's S3 GET is a 404 for it.
  if (!target) return text(404, "object not found");
  const range = request.headers.get("range");
  let object: R2ObjectBody | R2Object | null;
  try {
    const ranged = Boolean(range && range.trim());
    object = head && !ranged ? await bucket.head(key) : await bucket.get(key, ranged ? { range: request.headers } : {});
  } catch (error) {
    // An unsatisfiable range: S3 answers 416, which Rust does not forward (lib.rs:717).
    if (/range/i.test(String((error as Error)?.message))) return text(404, "object not found");
    return text(502, String((error as Error)?.message ?? error));
  }
  if (!object || (!head && !("body" in object))) return text(404, "object not found");
  const headers = new Headers();
  let status = 200;
  let length = object.size;
  const partial = object.range as { offset?: number; length?: number; suffix?: number } | undefined;
  if (range && range.trim() && partial) {
    const offset = partial.suffix !== undefined ? Math.max(object.size - partial.suffix, 0) : partial.offset ?? 0;
    length = partial.suffix !== undefined ? object.size - offset : partial.length ?? object.size - offset;
    status = 206;
    headers.set("content-range", `bytes ${offset}-${offset + length - 1}/${object.size}`);
  }
  headers.set("accept-ranges", "bytes");
  headers.set("content-length", String(length));
  if (object.httpMetadata?.contentType) headers.set("content-type", object.httpMetadata.contentType);
  headers.set("etag", object.httpEtag);
  headers.set("last-modified", object.uploaded.toUTCString());
  protectStorageResponse(headers);
  headers.set("cache-control", "private, max-age=300");
  storageCors(headers);
  if (head) {
    if ("body" in object) await (object as R2ObjectBody).body.cancel();
    return new Response(null, { status, headers });
  }
  return new Response((object as R2ObjectBody).body, { status, headers });
}

async function upload(request: Request, key: string, query: ProxyQuery, now: number, bucket: R2Bucket, config: FilesConfig, options: StorageRouteOptions): Promise<Response> {
  if (query.upload !== 1 || !await verifyProxy(config.secret, key, query.exp, query.sig, { kind: "upload" }, now)) return text(403, INVALID_SIGNATURE);
  const contentType = request.headers.get("content-type") ?? "";
  const declared = request.headers.get("content-length");
  const length = declared !== null && /^\d+$/.test(declared) ? Number(declared) : undefined;
  if (length !== undefined && length > MAX_UPLOAD_BYTES) return text(413, "upload too large");
  // A body of unknown length is buffered, so it is capped at the `store` limit (a 128 MB isolate); see README.
  let body: ReadableStream | ArrayBuffer | null = request.body;
  if (length === undefined) {
    const buffered = await readCapped(request.body, MAX_STORE_BYTES);
    if (!buffered) return text(413, "upload too large");
    body = buffered;
  }
  const parsed = parseObjectKey(key);
  if (!parsed) return json(409, { error: UPLOAD_CONSUMED });
  const tenant = options.tenant(parsed.tenant);
  let claim;
  try {
    claim = await tenant.filesClaimUpload(key);
  } catch (error) {
    return json(502, { error: String((error as Error)?.message ?? error) });
  }
  if (!claim.ok) return json(claim.error === UPLOAD_CONSUMED ? 409 : 502, { error: claim.error });
  let failure: string | undefined;
  try {
    // `If-None-Match: *` at the store, atomically with the write (storage.rs put_once).
    const put = await bucket.put(key, body, {
      onlyIf: new Headers({ "if-none-match": "*" }),
      ...(contentType ? { httpMetadata: { contentType } } : {}),
    });
    if (!put) failure = UPLOAD_CONSUMED;
  } catch (error) {
    failure = String((error as Error)?.message ?? error);
  }
  if (failure !== undefined) {
    await tenant.filesReleaseUpload(key, claim.consumedAt).catch(() => undefined);
    return json(failure === UPLOAD_CONSUMED ? 409 : 502, { error: failure });
  }
  return json(200, { storageId: key.split("/").pop() ?? "" });
}

async function readCapped(body: ReadableStream<Uint8Array> | null, limit: number): Promise<ArrayBuffer | undefined> {
  if (!body) return new ArrayBuffer(0);
  const chunks: Uint8Array[] = [];
  let total = 0;
  const reader = body.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > limit) {
      await reader.cancel();
      return undefined;
    }
    chunks.push(value);
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out.buffer;
}
