/**
 * The router Worker's `/dev/*` and `/healthz` (CONTRACT D.0, D.1, D.2).
 *
 * - `dev_cors`: every `OPTIONS /dev/*` is 204 before routing; every `/dev/*`
 *   response gets the CORS headers of lib.rs:609-643.
 * - `/dev/sync`: the body (the Whagons manifest is ~45 MB) is streamed into R2
 *   untouched and only its key crosses RPC (32 MiB RPC cap). The control object
 *   runs the sync with the release builder; after it answers, the router puts
 *   the new Worker version live and deletes the body.
 * - Everything else under `/dev/` goes to the control object as is; the
 *   operator routes, dashboard, metrics and log sockets live there.
 * - `/_gonvex/k19/blobs/<key>`: the release builder's access to the release
 *   bucket, with the builder token (a Container or a local Node process cannot
 *   bind R2 itself).
 */
import type { OperatorControlObject } from "../../control/operator/object.ts";
import { HttpReleaseBuilder, incomingKey, type ReleaseBuilder, type RolloutMessage } from "../../control/operator/release.ts";
import type { SyncOutcome } from "../../control/operator/sync.ts";

export interface DevRouterEnv {
  CONTROL: DurableObjectNamespace<OperatorControlObject>;
  RELEASES: R2Bucket;
  ROLLOUT: Queue<RolloutMessage>;
  RELEASE_BUILDER?: Fetcher;
  GONVEX_RELEASE_BUILDER_URL?: string;
  GONVEX_RELEASE_BUILDER_TOKEN?: string;
}

/** The control object's name (one per deployment, as K06's test Worker names it). */
export const CONTROL_NAME = "control";

/** axum `DefaultBodyLimit::max(64 << 20)`. */
export const BODY_LIMIT = 64 * 1024 * 1024;

const CORS: Record<string, string> = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, PUT, PATCH, DELETE, OPTIONS",
  "access-control-allow-headers": "Authorization, Content-Type, X-Gonvex-Key, X-Gonvex-Project-Key, X-Gonvex-Project-Id, X-Gonvex-Tenant-Id",
  "access-control-max-age": "600",
};

function withCors(response: Response): Response {
  if (response.status === 101 || response.webSocket) return response;
  const out = new Response(response.body, response);
  for (const [name, value] of Object.entries(CORS)) out.headers.set(name, value);
  return out;
}

const plain = (status: number, text: string) => new Response(text, { status, headers: { "content-type": "text/plain; charset=utf-8" } });
const jsonError = (status: number, error: string) => Response.json({ error }, { status });

export function releaseBuilder(env: DevRouterEnv): ReleaseBuilder | undefined {
  if (env.RELEASE_BUILDER) {
    const fetcher = env.RELEASE_BUILDER;
    const token = env.GONVEX_RELEASE_BUILDER_TOKEN;
    return new HttpReleaseBuilder((path, body) =>
      fetcher.fetch(`https://release-builder${path}`, { method: "POST", headers: { "content-type": "application/json", ...(token ? { authorization: `Bearer ${token}` } : {}) }, body: JSON.stringify(body) }),
    );
  }
  if (env.GONVEX_RELEASE_BUILDER_URL) return HttpReleaseBuilder.forUrl(env.GONVEX_RELEASE_BUILDER_URL, env.GONVEX_RELEASE_BUILDER_TOKEN);
  return undefined;
}

/** Handles `/healthz`, `/dev/*` and the builder's blob route; `null` for any other path. */
export async function handleDevRequest(request: Request, env: DevRouterEnv, ctx: ExecutionContext): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname.startsWith("/_gonvex/k19/blobs/")) return handleBlob(request, env, url);
  const control = env.CONTROL.getByName(CONTROL_NAME);
  if (url.pathname === "/healthz") {
    try {
      return await control.fetch(request);
    } catch {
      return Response.json({ ok: false }, { status: 503, headers: { "cache-control": "no-store", vary: "Authorization" } });
    }
  }
  if (!url.pathname.startsWith("/dev/")) return null;
  if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
  if (url.pathname === "/dev/sync") {
    if (request.method !== "POST") return withCors(new Response(null, { status: 405, headers: { allow: "POST" } }));
    return withCors(await devSync(request, env, ctx));
  }
  const length = Number(request.headers.get("content-length") ?? "0");
  if (length > BODY_LIMIT) return withCors(plain(413, "Failed to buffer the request body: length limit exceeded"));
  if (request.headers.get("upgrade")?.toLowerCase() === "websocket" || request.method === "GET" || request.method === "HEAD") return withCors(await control.fetch(request));
  // Operator bodies are small JSON: buffer them so a route that answers before
  // reading (an authorization failure) never leaves a stream half-read.
  const body = await request.arrayBuffer();
  if (body.byteLength > BODY_LIMIT) return withCors(plain(413, "Failed to buffer the request body: length limit exceeded"));
  return withCors(await control.fetch(new Request(request, { body })));
}

/** `POST /dev/sync` (D.2.2; held open per 0012 R4). */
async function devSync(request: Request, env: DevRouterEnv, ctx: ExecutionContext): Promise<Response> {
  const type = (request.headers.get("content-type") ?? "").trim();
  if (!/^application\/(?:[^;]*\+)?json\s*(?:;|$)/i.test(type)) return plain(415, "Expected request with `Content-Type: application/json`");
  const length = request.headers.get("content-length");
  if (length !== null && Number(length) > BODY_LIMIT) return plain(413, "Failed to buffer the request body: length limit exceeded");
  const headerProject = (request.headers.get("x-gonvex-project-id") ?? "").trim();
  const authorization = request.headers.get("authorization");
  const key = (authorization?.startsWith("Bearer ") ? authorization.slice("Bearer ".length) : request.headers.get("x-gonvex-key") ?? "").trim();
  const control = env.CONTROL.getByName(CONTROL_NAME);
  // Kernel: refuse a wrong key before storing the body, when the header names
  // the project (the CLI always sends it). Rust parses first; see note 0036 §3.
  if (headerProject !== "" && !(await control.syncKeyAccepted(headerProject, key))) return jsonError(401, "invalid Gonvex sync key");

  const incoming = crypto.randomUUID();
  try {
    if (length !== null && request.body) {
      await env.RELEASES.put(incomingKey(incoming), request.body.pipeThrough(new FixedLengthStream(Number(length))));
    } else {
      const bytes = await request.arrayBuffer();
      if (bytes.byteLength > BODY_LIMIT) return plain(413, "Failed to buffer the request body: length limit exceeded");
      await env.RELEASES.put(incomingKey(incoming), bytes);
    }
  } catch (cause) {
    return jsonError(503, `the sync body could not be stored: ${(cause as Error).message}`);
  }
  try {
    const outcome = (await control.devSync({ incoming, headerProject, key })) as SyncOutcome;
    if (!outcome.ok) return outcome.plain ? plain(outcome.status, outcome.error) : jsonError(outcome.status, outcome.error);
    if (outcome.deploy) {
      const builder = releaseBuilder(env);
      const deployed = builder ? await builder.deploy(outcome.deploy) : { ok: false as const, error: "no release builder is configured" };
      // Rust answers 500 when the last step fails while the new code serves (D.2.2 item 12).
      if (!deployed.ok) return jsonError(500, deployed.error);
    }
    return Response.json(outcome.body);
  } finally {
    ctx.waitUntil(env.RELEASES.delete(incomingKey(incoming)).catch(() => undefined));
  }
}

function sameText(left: string, right: string): boolean {
  const a = new TextEncoder().encode(left);
  const b = new TextEncoder().encode(right);
  return a.byteLength === b.byteLength && crypto.subtle.timingSafeEqual(a, b);
}

const BLOB_PREFIXES = ["incoming/", "releases/", "plans/"];

/** The builder's blob access: GET/PUT/DELETE one key under the release prefixes. */
async function handleBlob(request: Request, env: DevRouterEnv, url: URL): Promise<Response> {
  const token = env.GONVEX_RELEASE_BUILDER_TOKEN;
  if (!token || !sameText(request.headers.get("authorization") ?? "", `Bearer ${token}`)) return new Response(null, { status: 404 });
  const key = decodeURIComponent(url.pathname.slice("/_gonvex/k19/blobs/".length));
  if (!BLOB_PREFIXES.some((prefix) => key.startsWith(prefix)) || key.includes("..")) return new Response(null, { status: 400 });
  switch (request.method) {
    case "GET": {
      const object = await env.RELEASES.get(key);
      return object ? new Response(object.body, { headers: { "content-length": String(object.size) } }) : new Response(null, { status: 404 });
    }
    case "PUT": {
      const length = request.headers.get("content-length");
      if (length !== null && request.body) await env.RELEASES.put(key, request.body.pipeThrough(new FixedLengthStream(Number(length))));
      else await env.RELEASES.put(key, await request.arrayBuffer());
      return new Response(null, { status: 204 });
    }
    case "DELETE":
      await env.RELEASES.delete(key);
      return new Response(null, { status: 204 });
    default:
      return new Response(null, { status: 405 });
  }
}
