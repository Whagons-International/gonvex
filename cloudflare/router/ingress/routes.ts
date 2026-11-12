/**
 * The Ingress Worker's routes (K18; CONTRACT D.1 "Webhooks and HTTP actions",
 * IB4; decisions 0002, 0043 and 0045).
 *
 * - `POST /webhooks/telnyx?tenant=<tenant>[&project=<project>]` →
 *   `calling.ingestWebhook {rawBody, signature, timestamp}`
 * - `POST /webhooks/telnyx/relay?tenant=…` → `relay.ingestTelnyxWebhook {rawBody, signature, timestamp}`
 * - `POST /webhooks/github?tenant=…` → `github.ingestWebhook {event, rawBody, signature}`
 * - `GET /share/task?token=&tenant=` and `GET /share/task-image?token=&tenant=`
 *
 * Answers to providers follow the adapter they replace (whagons-api-gateway
 * `webhooks.rs`): 200 handled, 401 unsigned or bad signature, 400 malformed,
 * 404 unknown tenant or project, 405 not POST, 413 too large, 429 over the
 * rate, 503 anything else (the provider retries). The body is
 * `{"ok":<status is 200>}`. Where the order of checks differs from the
 * gateway's, 0043 §2 lists it.
 *
 * Before any tenant lookup, a request draws from a token bucket keyed by
 * (project, selector), as the gateway's limiter is; a lookup that misses the
 * cache also draws from the project's lookup budget (directory.ts).
 */
import { BodyTooLarge, exactUtf8, readRawBody } from "./body.ts";
import type { ServiceGrant } from "../../control/src/types.ts";
import { ControlDirectory, IngressMisconfigured, LookupBudgetExceeded, type ControlAuthenticator, type IngressDirectory } from "./directory.ts";
import { LOOKUP_RATE, Limiter, parseRate, SHARE_RATE, WEBHOOK_RATE } from "./limits.ts";
import { ingressObjectName, type DeliveryOutcome, type DeliveryRequest, type IngressObjectEnv } from "./object.ts";
import {
  classifyFailure,
  GITHUB_REPLAY_MS,
  headerText,
  MAX_TENANT_BYTES,
  providerFor,
  TELNYX_WINDOW_SECONDS,
  telnyxEventId,
  telnyxFresh,
  telnyxSeconds,
  type Provider,
} from "./providers.ts";
import {
  appLink,
  EMPTY_IMAGE,
  isPreview,
  MAX_TOKEN_BYTES,
  PREVIEW_FUNCTION,
  renderNotFound,
  renderShareImage,
  renderSharePage,
  renderUnavailable,
  webLink,
  type ShareConfig,
} from "./share.ts";

/** The control object's name (one per deployment; K19 `CONTROL_NAME`). */
export const CONTROL_NAME = "control";

export interface IngressEnv extends IngressObjectEnv {
  /** K06's control object; the ingress calls only `authenticate`. */
  CONTROL: DurableObjectNamespace;
  INGRESS: DurableObjectNamespace;
  /** The ingress's `gvx_svc_` credential (a secret). Its digest is in `GONVEX_SERVICE_PRINCIPALS`. */
  GONVEX_INGRESS_SERVICE_TOKEN?: string;
  /** Projects the ingress serves, comma separated; the first is the default for `?project=`. */
  GONVEX_INGRESS_PROJECTS?: string;
  /** Control lookups (cache misses) per project and isolate, `<per second>/<burst>`; default 20/40. */
  GONVEX_INGRESS_LOOKUP_RATE?: string;
  GONVEX_SHARE_SITE_NAME?: string;
  GONVEX_SHARE_APP_SCHEME?: string;
  GONVEX_SHARE_WEB_URL?: string;
  GONVEX_SHARE_IMAGE_PROXY_URL?: string;
}

interface IngressDelivery {
  deliver(request: DeliveryRequest): Promise<DeliveryOutcome>;
}

let directory: { token: string; value: IngressDirectory } | undefined;
let selectors: Limiter | undefined;

function directoryFor(env: IngressEnv, token: string): IngressDirectory {
  if (!directory || directory.token !== token) {
    const control = () => env.CONTROL.getByName(CONTROL_NAME) as unknown as ControlAuthenticator;
    directory = { token, value: new ControlDirectory(control, token, { lookupRate: parseRate(env.GONVEX_INGRESS_LOOKUP_RATE, LOOKUP_RATE) }) };
  }
  return directory.value;
}

/** The per-isolate limiter keyed by (project, selector), checked before any tenant lookup. */
function selectorLimiter(env: IngressEnv): Limiter {
  selectors ??= new Limiter({
    webhook: parseRate(env.GONVEX_INGRESS_TENANT_RATE, WEBHOOK_RATE),
    share: parseRate(env.GONVEX_INGRESS_SHARE_RATE, SHARE_RATE),
  });
  return selectors;
}

/** Tests: forget cached tenant resolutions and rate buckets. */
export function resetIngressDirectory(): void {
  directory = undefined;
  selectors = undefined;
}

const projectsOf = (env: IngressEnv): string[] => (env.GONVEX_INGRESS_PROJECTS ?? "").split(",").map((p) => p.trim()).filter(Boolean);

function audit(fields: Record<string, unknown>): void {
  console.log(JSON.stringify({ target: "audit", ...fields }));
}

function reply(status: number, extra: Record<string, string> = {}): Response {
  const headers: Record<string, string> = { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", ...extra };
  if (status === 405) headers.allow = "POST";
  return new Response(JSON.stringify({ ok: status === 200 }), { status, headers });
}

const hex = (buffer: ArrayBuffer): string => Array.from(new Uint8Array(buffer), (byte) => byte.toString(16).padStart(2, "0")).join("");

/**
 * A delivery's identity: the provider, its signed headers and the body digest.
 * Ed25519 signatures are deterministic, so the body must be part of the key:
 * otherwise a captured (timestamp, signature) pair with another body would
 * be answered as a duplicate of the signed one.
 */
async function replayKey(provider: Provider, parts: string[], body: Uint8Array): Promise<string> {
  const digest = hex(await crypto.subtle.digest("SHA-256", body));
  return hex(await crypto.subtle.digest("SHA-256", new TextEncoder().encode([provider.id, ...parts, digest].join("\u0000"))));
}

/** Handles the ingress paths; `null` for any other path, so a router can try its other handlers. */
export async function handleIngressRequest(request: Request, env: IngressEnv): Promise<Response | null> {
  const url = new URL(request.url);
  const provider = providerFor(url.pathname);
  if (provider) return webhook(provider, request, env, url);
  if (url.pathname === "/share/task" || url.pathname === "/share/task-image") return share(request, env, url);
  return null;
}

type Target = { ok: true; project: string; tenant: string } | { ok: false; status: number };

/** `?tenant=` (required, at most 128 bytes) and `?project=` (one the ingress serves; default the first). */
function target(env: IngressEnv, url: URL): Target {
  const projects = projectsOf(env);
  const tenant = (url.searchParams.get("tenant") ?? "").trim();
  if (!tenant || new TextEncoder().encode(tenant).byteLength > MAX_TENANT_BYTES) return { ok: false, status: 400 };
  const project = (url.searchParams.get("project") ?? "").trim() || projects[0] || "";
  if (!projects.length) return { ok: false, status: 503 };
  if (!projects.includes(project)) return { ok: false, status: 404 };
  return { ok: true, project, tenant };
}

/** Rate limit on (project, selector), then the tenant lookup. */
async function resolve(env: IngressEnv, bucket: "webhook" | "share", project: string, tenant: string): Promise<{ grant: ServiceGrant } | { status: number; reason: string }> {
  const token = env.GONVEX_INGRESS_SERVICE_TOKEN?.trim();
  if (!token) return { status: 503, reason: "GONVEX_INGRESS_SERVICE_TOKEN is not set" };
  if (!selectorLimiter(env).take(bucket, Date.now(), `${project}\u0000${tenant}`)) return { status: 429, reason: "over the selector's rate" };
  try {
    const grant = await directoryFor(env, token).resolve(project, tenant);
    return grant ? { grant } : { status: 404, reason: "unknown tenant" };
  } catch (error) {
    if (error instanceof LookupBudgetExceeded) return { status: 429, reason: error.message };
    if (error instanceof IngressMisconfigured) console.error(JSON.stringify({ target: "ingress", error: error.message }));
    return { status: 503, reason: String((error as Error)?.message ?? error) };
  }
}

const deskFor = (env: IngressEnv, grant: ServiceGrant): IngressDelivery => env.INGRESS.getByName(ingressObjectName(grant)) as unknown as IngressDelivery;

async function webhook(provider: Provider, request: Request, env: IngressEnv, url: URL): Promise<Response> {
  const started = Date.now();
  let tenant = (url.searchParams.get("tenant") ?? "").trim();
  let event = "";
  let duplicate = false;
  const status = await (async (): Promise<number> => {
    if (request.method !== "POST") return 405;
    const where = target(env, url);
    if (!where.ok) return where.status;
    tenant = where.tenant;
    // Unsigned requests never reach a tenant, and their bodies are never read.
    const args: Record<string, unknown> = {};
    for (const [header, argument] of provider.headers) {
      const value = headerText(request.headers, header);
      if (value === null) return 401;
      args[argument] = value;
    }
    let bytes: Uint8Array;
    try {
      bytes = await readRawBody(request, provider.maxBodyBytes);
    } catch (error) {
      return error instanceof BodyTooLarge ? 413 : 400;
    }
    const rawBody = exactUtf8(bytes);
    if (rawBody === null) return 400;
    let replay: DeliveryRequest["replay"];
    if (provider.replay === "telnyx-timestamp") {
      const id = telnyxEventId(rawBody);
      if (id === null) return 400;
      event = id;
      const seconds = telnyxSeconds(String(args.timestamp));
      // A stale or future-dated signature would fail in the Action; refuse it here (replay window).
      if (seconds === null || !telnyxFresh(seconds, Date.now())) return 401;
      replay = { key: await replayKey(provider, [String(args.timestamp), String(args.signature)], bytes), untilMs: (seconds + TELNYX_WINDOW_SECONDS + 30) * 1000 };
    } else {
      event = headerText(request.headers, "x-github-delivery") ?? "";
      replay = { key: await replayKey(provider, [event, String(args.event), String(args.signature)], bytes), untilMs: Date.now() + GITHUB_REPLAY_MS };
    }
    args.rawBody = rawBody;
    const resolved = await resolve(env, "webhook", where.project, where.tenant);
    if (!("grant" in resolved)) return resolved.status;
    const outcome = await deskFor(env, resolved.grant).deliver({ v: 1, grant: resolved.grant, path: provider.function, args, bucket: "webhook", replay });
    if (outcome.kind === "limited") return 429;
    if (outcome.kind === "unavailable") {
      console.error(JSON.stringify({ target: "ingress", route: provider.route, project: where.project, tenant: resolved.grant.route.tenantId, error: outcome.reason }));
      return 503;
    }
    duplicate = outcome.duplicate === true;
    if (outcome.report.ok) return 200;
    switch (classifyFailure(provider, outcome.report.error)) {
      case "signature": return 401;
      case "tooLarge": return 413;
      case "unknownTenant": return 404;
      case "other":
        console.warn(JSON.stringify({ target: "ingress", route: provider.route, project: where.project, tenant: resolved.grant.route.tenantId, event, error: outcome.report.error.slice(0, 500) }));
        return 503;
    }
  })();
  audit({ route: provider.route, tenant, event, status, duplicate, latencyMs: Date.now() - started });
  return reply(status, status === 429 ? { "retry-after": "1" } : {});
}

function shareConfig(env: IngressEnv): ShareConfig {
  return {
    siteName: env.GONVEX_SHARE_SITE_NAME?.trim() || "Whagons",
    appScheme: env.GONVEX_SHARE_APP_SCHEME?.trim() || "whagons",
    webUrl: env.GONVEX_SHARE_WEB_URL?.trim() || undefined,
    imageProxyUrl: env.GONVEX_SHARE_IMAGE_PROXY_URL?.trim() || undefined,
  };
}

const PAGE_HEADERS = {
  "content-type": "text/html; charset=utf-8",
  "cache-control": "no-store",
  "referrer-policy": "no-referrer",
  "x-content-type-options": "nosniff",
  "x-robots-tag": "noindex",
};
const IMAGE_HEADERS = {
  "content-type": "image/svg+xml; charset=utf-8",
  "referrer-policy": "no-referrer",
  "x-content-type-options": "nosniff",
  "content-security-policy": "default-src 'none'; style-src 'unsafe-inline'",
};

const pageCsp = (nonce: string) =>
  `default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${nonce}'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`;

function nonce(): string {
  return btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(16))));
}

async function share(request: Request, env: IngressEnv, url: URL): Promise<Response> {
  const image = url.pathname === "/share/task-image";
  const config = shareConfig(env);
  const head = request.method === "HEAD";
  const respond = (status: number, body: string, headers: Record<string, string>) => new Response(head ? null : body, { status, headers });
  const notFound = () => image
    ? respond(404, EMPTY_IMAGE, { ...IMAGE_HEADERS, "cache-control": "no-store" })
    : respond(404, renderNotFound(config), { ...PAGE_HEADERS, "content-security-policy": pageCsp(nonce()) });
  const unavailable = (status: number) => image
    ? respond(status, EMPTY_IMAGE, { ...IMAGE_HEADERS, "cache-control": "no-store", "retry-after": "60" })
    : respond(status, renderUnavailable(config), { ...PAGE_HEADERS, "content-security-policy": pageCsp(nonce()), "retry-after": "60" });

  if (request.method !== "GET" && !head) return new Response(null, { status: 405, headers: { allow: "GET, HEAD" } });
  const token = (url.searchParams.get("token") ?? "").trim();
  if (!token || new TextEncoder().encode(token).byteLength > MAX_TOKEN_BYTES) return notFound();
  const where = target(env, url);
  if (!where.ok) return where.status === 503 ? unavailable(503) : notFound();
  const resolved = await resolve(env, "share", where.project, where.tenant);
  if (!("grant" in resolved)) return resolved.status === 404 ? notFound() : unavailable(resolved.status === 429 ? 429 : 503);
  const outcome = await deskFor(env, resolved.grant).deliver({ v: 1, grant: resolved.grant, path: PREVIEW_FUNCTION, args: { token }, bucket: "share" });
  if (outcome.kind === "limited") return unavailable(429);
  if (outcome.kind === "unavailable" || !outcome.report.ok) {
    const error = outcome.kind === "unavailable" ? outcome.reason : outcome.report.ok ? "" : outcome.report.error;
    console.warn(JSON.stringify({ target: "ingress", route: url.pathname, project: where.project, tenant: resolved.grant.route.tenantId, error: error.slice(0, 500) }));
    return unavailable(503);
  }
  const preview = outcome.report.value;
  if (!isPreview(preview)) return notFound();
  if (image) return respond(200, renderShareImage(preview, config), { ...IMAGE_HEADERS, "cache-control": "public, max-age=300" });

  const link = (path: string) => {
    const out = new URL(path, url.origin);
    out.searchParams.set("token", token);
    out.searchParams.set("tenant", where.tenant);
    if (url.searchParams.get("project")) out.searchParams.set("project", where.project);
    return out.toString();
  };
  const scriptNonce = nonce();
  const html = renderSharePage({
    preview,
    config,
    shareUrl: link("/share/task"),
    imageUrl: link("/share/task-image"),
    appUrl: appLink(config, token, where.tenant),
    webUrl: webLink(config, token, where.tenant),
    nonce: scriptNonce,
  });
  return respond(200, html, { ...PAGE_HEADERS, "content-security-policy": pageCsp(scriptNonce) });
}
