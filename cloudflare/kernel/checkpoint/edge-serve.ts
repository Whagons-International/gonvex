// Stateless Edge Worker checkpoint serving handler (Phase 3).
// Serves immutable WLP1 checkpoint pages and manifests directly from R2 after verifying the
// client's Ed25519 `wlt1.` session token at the edge:
//   - 0 Control DO hops
//   - 0 Tenant DO wakes
//   - 0 SQLite rows read
//   - 0 JSON.parse / JSON.stringify in the edge hot path (streams R2 body directly).

import type { EdgeAuthVerifier, EdgeSessionClaims } from "../edge-auth/index";
import {
  CHECKPOINT_CACHE_IMMUTABLE,
  CHECKPOINT_CACHE_LATEST,
  CHECKPOINT_CONTENT_TYPE_FRAMES,
  CHECKPOINT_CONTENT_TYPE_JSON,
  checkpointLatestKey,
  checkpointManifestKey,
  checkpointPageKey,
  isValidTenantSegment,
  type CheckpointBucket,
} from "./manifest";

export const DEFAULT_CHECKPOINT_BASE_PATH = "/sync/checkpoint";

export type ParsedCheckpointRoute =
  | { readonly kind: "latest"; readonly tenantId: string }
  | { readonly kind: "manifest"; readonly tenantId: string; readonly epoch: number; readonly seq: number }
  | {
      readonly kind: "page";
      readonly tenantId: string;
      readonly epoch: number;
      readonly seq: number;
      readonly tbl: number;
      readonly page: number;
    };

export interface ServeCheckpointOptions {
  readonly bucket: CheckpointBucket;
  readonly verifier: EdgeAuthVerifier;
  readonly basePath?: string;
  readonly nowSec?: number;
  /** Optional extra authorization hook (e.g. checking role or permission bits on `claims`). */
  readonly authorize?: (claims: EdgeSessionClaims, route: ParsedCheckpointRoute) => boolean;
}

function parseNonNegIntSegment(seg: string): number {
  if (seg.length === 0 || seg.length > 15) return -1;
  let n = 0;
  for (let i = 0; i < seg.length; i++) {
    const code = seg.charCodeAt(i);
    if (code < 48 || code > 57) return -1;
    n = n * 10 + (code - 48);
  }
  return Number.isSafeInteger(n) ? n : -1;
}

/**
 * Parses a checkpoint HTTP path under `basePath`:
 *   - `<basePath>/:tenantId/latest`
 *   - `<basePath>/:tenantId/:epoch/:seq/manifest`
 *   - `<basePath>/:tenantId/:epoch/:seq/:tbl/:page`
 */
export function parseCheckpointRoute(
  pathname: string,
  basePath = DEFAULT_CHECKPOINT_BASE_PATH,
): ParsedCheckpointRoute | null {
  const prefix = basePath.endsWith("/") ? basePath : `${basePath}/`;
  if (!pathname.startsWith(prefix)) return null;
  const rest = pathname.slice(prefix.length);
  if (rest.length === 0) return null;

  const parts = rest.split("/");
  const tenantId = parts[0]!;
  if (!isValidTenantSegment(tenantId)) return null;

  if (parts.length === 2 && parts[1] === "latest") {
    return { kind: "latest", tenantId };
  }

  if (parts.length === 4 && parts[3] === "manifest") {
    const epoch = parseNonNegIntSegment(parts[1]!);
    const seq = parseNonNegIntSegment(parts[2]!);
    if (epoch < 0 || seq < 0) return null;
    return { kind: "manifest", tenantId, epoch, seq };
  }

  if (parts.length === 5) {
    const epoch = parseNonNegIntSegment(parts[1]!);
    const seq = parseNonNegIntSegment(parts[2]!);
    const tbl = parseNonNegIntSegment(parts[3]!);
    const page = parseNonNegIntSegment(parts[4]!);
    if (epoch < 0 || seq < 0 || tbl < 0 || page < 0) return null;
    return { kind: "page", tenantId, epoch, seq, tbl, page };
  }

  return null;
}

function etagMatches(ifNoneMatch: string | null, etag: string): boolean {
  if (ifNoneMatch === null || etag.length === 0) return false;
  const trimmed = ifNoneMatch.trim();
  if (trimmed === "*") return true;
  if (trimmed === etag) return true;
  if (trimmed.length >= 2 && trimmed.charCodeAt(0) === 34 && trimmed.charCodeAt(trimmed.length - 1) === 34) {
    return trimmed.slice(1, -1) === etag;
  }
  return false;
}

function quoteEtag(etag: string): string {
  if (etag.length === 0) return "";
  if (etag.charCodeAt(0) === 34) return etag;
  return `"${etag}"`;
}

function pathnameOf(url: string): string {
  const schemeEnd = url.indexOf("://");
  const pathStart = schemeEnd >= 0 ? url.indexOf("/", schemeEnd + 3) : 0;
  if (pathStart < 0) return "/";
  const q = url.indexOf("?", pathStart);
  return q >= 0 ? url.slice(pathStart, q) : url.slice(pathStart);
}

/**
 * Stateless Edge Worker request handler for R2 WLP1 checkpoints.
 * Returns `null` when the request path does not match `basePath` so routers can fall through.
 */
export async function serveCheckpointRequest(
  request: Request,
  options: ServeCheckpointOptions,
): Promise<Response | null> {
  const basePath = options.basePath ?? DEFAULT_CHECKPOINT_BASE_PATH;
  const pathname = pathnameOf(request.url);
  const route = parseCheckpointRoute(pathname, basePath);
  if (route === null) return null;

  const method = request.method;
  if (method !== "GET" && method !== "HEAD") {
    return new Response("Method Not Allowed", {
      status: 405,
      headers: { Allow: "GET, HEAD" },
    });
  }

  const auth = await options.verifier.verifyRequest(request, options.nowSec);
  if (auth.kind !== "edge") {
    const reason = auth.kind === "rejected" ? auth.reason : "edge_token_required";
    return new Response(reason, {
      status: 401,
      headers: { "WWW-Authenticate": 'Bearer realm="gonvex-checkpoint"' },
    });
  }

  if (auth.claims.tenantId !== route.tenantId) {
    return new Response("forbidden_tenant", { status: 403 });
  }
  if (options.authorize !== undefined && !options.authorize(auth.claims, route)) {
    return new Response("forbidden", { status: 403 });
  }

  let key: string;
  let defaultContentType: string;
  let defaultCacheControl: string;
  if (route.kind === "latest") {
    key = checkpointLatestKey(route.tenantId);
    defaultContentType = CHECKPOINT_CONTENT_TYPE_JSON;
    defaultCacheControl = CHECKPOINT_CACHE_LATEST;
  } else if (route.kind === "manifest") {
    key = checkpointManifestKey(route.tenantId, route.epoch, route.seq);
    defaultContentType = CHECKPOINT_CONTENT_TYPE_JSON;
    defaultCacheControl = CHECKPOINT_CACHE_IMMUTABLE;
  } else {
    key = checkpointPageKey(route.tenantId, route.epoch, route.seq, route.tbl, route.page);
    defaultContentType = CHECKPOINT_CONTENT_TYPE_FRAMES;
    defaultCacheControl = CHECKPOINT_CACHE_IMMUTABLE;
  }

  const ifNoneMatch = request.headers.get("if-none-match");

  if (method === "HEAD") {
    const meta = await options.bucket.head(key);
    if (meta === null) return new Response("not_found", { status: 404 });
    const headers = new Headers();
    headers.set("Content-Type", defaultContentType);
    headers.set("Cache-Control", defaultCacheControl);
    headers.set("Content-Length", String(meta.size));
    headers.set("X-Gonvex-Checkpoint-Zero-DO", "1");
    if (meta.etag.length > 0) headers.set("ETag", quoteEtag(meta.etag));
    if (etagMatches(ifNoneMatch, meta.etag)) {
      return new Response(null, { status: 304, headers });
    }
    return new Response(null, { status: 200, headers });
  }

  const obj = await options.bucket.get(key);
  if (obj === null) return new Response("not_found", { status: 404 });

  const headers = new Headers();
  headers.set("Content-Type", obj.httpMetadata?.contentType ?? defaultContentType);
  headers.set("Cache-Control", obj.httpMetadata?.cacheControl ?? defaultCacheControl);
  headers.set("Content-Length", String(obj.size));
  headers.set("X-Gonvex-Checkpoint-Zero-DO", "1");
  if (obj.etag.length > 0) headers.set("ETag", quoteEtag(obj.etag));

  if (etagMatches(ifNoneMatch, obj.etag)) {
    return new Response(null, { status: 304, headers });
  }

  return new Response(obj.body, { status: 200, headers });
}
