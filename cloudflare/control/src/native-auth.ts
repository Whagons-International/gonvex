/**
 * Browser-facing native OAuth routes (rust/crates/runtime/src/native_auth.rs;
 * contract C.3.5, D.1 "Native OAuth"). The router forwards `/auth/*` here.
 *
 * Status codes, JSON error bodies, redirects (307) and CORS headers follow the
 * reference. Request-shape failures follow axum's extractor rejections.
 */
import type { Ctx } from "./context";
import { verifyExternalToken, type ExternalAuthConfiguration } from "./external-auth";
import { decryptControlSecret } from "./secrets";
import {
  type AuthAccount,
  type SessionGrant,
  REFRESH_TOKEN_MS,
  issueSession,
  loadAuthAccount,
  refreshSession,
  resolveExternalAccount,
  revokeFamily,
  sessionResult,
  sessionResultFromDirectory,
} from "./sessions";
import { NOW_MS, blob, str } from "./sql";
import { constantTimeEqual, nativeSecureToken, sha256, sha256Hex } from "./util";

const TRANSACTION_MS = 10 * 60 * 1000;
const CODE_MS = 5 * 60 * 1000;

const CORS: Record<string, string> = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, OPTIONS",
  "access-control-allow-headers": "Authorization, Content-Type",
  "access-control-max-age": "600",
};

function withCors(response: Response): Response {
  const out = new Response(response.body, response);
  for (const [key, value] of Object.entries(CORS)) out.headers.set(key, value);
  return out;
}

const jsonResponse = (status: number, body: unknown): Response => Response.json(body, { status });
const error = (status: number, message: string): Response => jsonResponse(status, { error: message });
const text = (status: number, body: string): Response => new Response(body, { status, headers: { "content-type": "text/plain; charset=utf-8" } });
const redirect = (location: string): Response => new Response(null, { status: 307, headers: { location } });

class Rejection extends Error {
  constructor(readonly response: Response) {
    super("rejection");
  }
}

/** axum `Query<T>` with required string fields. */
function query(url: URL, required: string[]): Record<string, string> {
  const values: Record<string, string> = {};
  for (const [key, value] of url.searchParams) if (!(key in values)) values[key] = value;
  for (const field of required) {
    if (!(field in values)) throw new Rejection(text(400, `Failed to deserialize query string: missing field \`${field}\``));
  }
  return values;
}

/** axum `Json<T>`: content type, syntax, then missing required fields. */
async function jsonBody(request: Request, required: string[]): Promise<Record<string, unknown>> {
  const type = request.headers.get("content-type") ?? "";
  if (!/^application\/(?:[^;]+\+)?json\s*(?:;.*)?$/i.test(type.trim())) {
    throw new Rejection(text(415, "Expected request with `Content-Type: application/json`"));
  }
  const raw = await request.text();
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch (cause) {
    throw new Rejection(text(400, `Failed to parse the request body as JSON: ${cause instanceof Error ? cause.message : String(cause)}`));
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Rejection(text(422, "Failed to deserialize the JSON body into the target type: invalid type: expected a struct"));
  }
  for (const field of required) {
    if (!(field in (value as object))) throw new Rejection(text(422, `Failed to deserialize the JSON body into the target type: missing field \`${field}\``));
  }
  return value as Record<string, unknown>;
}

const stringField = (body: Record<string, unknown>, key: string): string => {
  const value = body[key];
  if (value === undefined || value === null) return "";
  if (typeof value !== "string") {
    throw new Rejection(text(422, `Failed to deserialize the JSON body into the target type: ${key}: invalid type: expected a string`));
  }
  return value;
};

const isLocal = (url: URL): boolean => ["localhost", "127.0.0.1", "::1"].includes(url.hostname);

/** `normalize_redirect` (native_auth.rs:773). */
export function normalizeRedirect(raw: string): string {
  if (Buffer.byteLength(raw) > 2048) throw new Error("redirect URI is too long");
  let parsed: URL;
  try {
    parsed = new URL(raw.trim());
  } catch {
    throw new Error("redirect URI must be absolute");
  }
  const local = isLocal(parsed);
  if (
    !parsed.hostname ||
    parsed.username ||
    parsed.password ||
    raw.trim().includes("#") ||
    (!local && parsed.protocol !== "https:") ||
    (local && parsed.protocol !== "http:" && parsed.protocol !== "https:")
  ) {
    throw new Error("redirect URI must use https without credentials or a fragment");
  }
  return parsed.toString();
}

/** `callback_uri` (native_auth.rs:749). */
function callbackUri(ctx: Ctx, provider: string): string {
  const base = ctx.config.authPublicUrl;
  if (!base) throw new Error("GONVEX_AUTH_URL is required");
  let parsed: URL;
  try {
    parsed = new URL(base);
  } catch {
    throw new Error("GONVEX_AUTH_URL is invalid");
  }
  const local = isLocal(parsed);
  if (!parsed.hostname || parsed.username || parsed.password || (!local && parsed.protocol !== "https:") || (local && parsed.protocol !== "http:" && parsed.protocol !== "https:")) {
    throw new Error("GONVEX_AUTH_URL is invalid");
  }
  return `${base.replace(/\/+$/, "")}/auth/${provider}/callback`;
}

const validPkce = (value: string): boolean => value.length >= 43 && value.length <= 128 && /^[A-Za-z0-9_-]+$/.test(value);
const pkceChallenge = (verifier: string): string => Buffer.from(sha256(verifier)).toString("base64url");

function tokenNonceMatches(token: string, expected: string): boolean {
  const payload = token.split(".")[1];
  if (payload === undefined || !/^[A-Za-z0-9_-]*$/.test(payload)) return false;
  try {
    const value = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as { nonce?: unknown };
    return typeof value.nonce === "string" && constantTimeEqual(value.nonce, expected);
  } catch {
    return false;
  }
}

/** `redirect_values`: appends form-encoded pairs to the existing query. */
function redirectValues(redirectUri: string, values: Array<[string, string]>): Response {
  let target: URL;
  try {
    target = new URL(redirectUri);
  } catch {
    return error(500, "invalid app redirect");
  }
  const appended = new URLSearchParams(values).toString();
  const existing = target.search.replace(/^\?/, "");
  target.search = existing ? `${existing}&${appended}` : appended;
  return redirect(target.toString());
}

const redirectError = (redirectUri: string, state: string, code: string) => redirectValues(redirectUri, [["error", code], ["state", state]]);

interface NativeProvider {
  provider: string;
  authorizeUrl: string;
  tokenUrl: string;
  issuer: string;
  clientId: string;
  audience: string;
  jwksUrl: string;
  clientSecret: string;
}

/** `native_provider_configuration` (native_auth.rs:600). */
function nativeProviderConfiguration(ctx: Ctx, project: string, provider: string, redirectUri: string): NativeProvider {
  const row = ctx.sql.get(
    `SELECT p.azure_tenant_id,p.client_id,p.client_secret_encrypted,p.issuer,p.audience,p.jwks_url
     FROM gonvex_auth_providers p
     JOIN gonvex_auth_redirect_uris redirect ON redirect.project_id=p.project_id AND redirect.provider=p.provider
     JOIN gonvex_runtime_projects project ON project.id=p.project_id
     WHERE p.project_id=$1 AND p.provider=$2 AND p.enabled=TRUE AND redirect.redirect_uri=$3
       AND COALESCE(NULLIF(project.auth_mode,''),'gonvex-native') IN ('gonvex-native','hybrid')`,
    project,
    provider,
    redirectUri,
  );
  if (!row) throw new Error("redirect URI is not registered for this project");
  const tenant = str(row.azure_tenant_id);
  let clientId = str(row.client_id);
  const encrypted = blob(row.client_secret_encrypted);
  if (provider === "google" && !clientId) clientId = ctx.config.googleClientId ?? "";
  let secret: string | null = null;
  if (encrypted) {
    try {
      secret = decryptControlSecret(ctx.config.controlSecret, encrypted);
    } catch {
      secret = null;
    }
  } else if (provider === "google") {
    secret = ctx.config.googleClientSecret ?? null;
  }
  if (!clientId || secret === null) throw new Error("authentication provider is not fully configured");
  let urls: [string, string, string, string];
  if (provider === "google") {
    urls = ["https://accounts.google.com/o/oauth2/v2/auth", "https://oauth2.googleapis.com/token", "https://accounts.google.com", "https://www.googleapis.com/oauth2/v3/certs"];
  } else if (provider === "microsoft" && tenant) {
    urls = [
      `https://login.microsoftonline.com/${tenant}/oauth2/v2.0/authorize`,
      `https://login.microsoftonline.com/${tenant}/oauth2/v2.0/token`,
      `https://login.microsoftonline.com/${tenant}/v2.0`,
      `https://login.microsoftonline.com/${tenant}/discovery/v2.0/keys`,
    ];
  } else if (provider === "apple") {
    urls = ["https://appleid.apple.com/auth/authorize", "https://appleid.apple.com/auth/token", "https://appleid.apple.com", "https://appleid.apple.com/auth/keys"];
  } else {
    throw new Error("authentication provider configuration is incomplete");
  }
  const nonempty = (value: string, fallback: string) => (value.trim() ? value : fallback);
  return {
    provider,
    authorizeUrl: urls[0],
    tokenUrl: urls[1],
    issuer: nonempty(str(row.issuer), urls[2]),
    clientId,
    audience: nonempty(str(row.audience), clientId),
    jwksUrl: nonempty(str(row.jwks_url), urls[3]),
    clientSecret: secret,
  };
}

const externalOf = (configuration: NativeProvider): ExternalAuthConfiguration => ({
  provider: configuration.provider,
  issuer: configuration.issuer,
  audience: configuration.audience,
  jwksUrl: configuration.jwksUrl,
  firebaseTenantId: "",
  signupMode: "personal",
  firebaseProjectId: "",
  firebaseAdminCredentials: null,
});

async function authConfig(ctx: Ctx, url: URL): Promise<Response> {
  const project = query(url, ["project"]).project!.trim();
  if (!project) return error(400, "project is required");
  const row = ctx.sql.get(
    "SELECT database_mode,COALESCE(NULLIF(auth_mode,''),'gonvex-native') AS auth_mode FROM gonvex_runtime_projects WHERE id=$1 AND status='active'",
    project,
  );
  if (!row) return error(404, "project is unavailable");
  const rows = ctx.sql.all("SELECT provider,signup_mode FROM gonvex_auth_providers WHERE project_id=$1 AND enabled=TRUE ORDER BY provider", project);
  const native = ["gonvex-native", "hybrid"].includes(str(row.auth_mode));
  return jsonResponse(200, {
    project,
    providers: native ? rows.map((r) => str(r.provider)).filter((p) => ["google", "microsoft", "apple"].includes(p)) : [],
    databaseMode: str(row.database_mode),
    signupMode: rows[0] ? str(rows[0].signup_mode) : "personal",
    accessTokenLifetimeSeconds: 15 * 60,
    refreshTokenLifetimeSeconds: 30 * 24 * 60 * 60,
  });
}

async function authorize(ctx: Ctx, provider: string, url: URL): Promise<Response> {
  // axum extracts the query before the handler runs: a malformed query is refused first.
  const q = query(url, ["project", "redirect_uri", "state", "code_challenge", "code_challenge_method"]);
  if (!["google", "microsoft", "apple"].includes(provider)) return error(404, "authentication provider is unsupported");
  const state = q.state!;
  if (state.length < 16 || state.length > 512 || q.code_challenge_method !== "S256" || !validPkce(q.code_challenge!)) {
    return error(400, "project, state, and PKCE S256 are required");
  }
  let redirectUri: string;
  try {
    redirectUri = normalizeRedirect(q.redirect_uri!);
  } catch (cause) {
    return error(400, (cause as Error).message);
  }
  let callback: string;
  try {
    callback = callbackUri(ctx, provider);
  } catch (cause) {
    return error(503, (cause as Error).message);
  }
  const project = q.project!.trim();
  const stateToken = nativeSecureToken("oauth");
  const nonce = nativeSecureToken("nonce");
  let configuration: NativeProvider;
  try {
    configuration = ctx.sql.tx(() => {
      const found = nativeProviderConfiguration(ctx, project, provider, redirectUri);
      ctx.sql.run(
        `INSERT INTO gonvex_auth_transactions(token_hash,project_id,redirect_uri,app_state,code_challenge,nonce,google_redirect_uri,provider,expires_at)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        sha256Hex(stateToken),
        project,
        redirectUri,
        state.trim(),
        q.code_challenge!.trim(),
        nonce,
        callback,
        provider,
        Date.now() + TRANSACTION_MS,
      );
      return found;
    });
  } catch (cause) {
    const message = (cause as Error).message;
    if (message.startsWith("redirect URI") || message.startsWith("authentication provider")) return error(400, message);
    return error(500, "could not start auth flow");
  }
  const target = new URL(configuration.authorizeUrl);
  const params: Array<[string, string]> = [
    ["client_id", configuration.clientId],
    ["redirect_uri", callback],
    ["response_type", "code"],
    ["scope", provider === "apple" ? "name email" : "openid email profile"],
    ["state", stateToken],
    ["nonce", nonce],
  ];
  if (provider === "microsoft") params.push(["response_mode", "query"]);
  else if (provider === "apple") params.push(["response_mode", "form_post"]);
  target.search = new URLSearchParams(params).toString();
  return redirect(target.toString());
}

async function exchangeProviderCode(ctx: Ctx, configuration: NativeProvider, code: string, redirectUri: string): Promise<string> {
  if (!code) throw new Error("code");
  const response = await ctx.fetch(configuration.tokenUrl, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      code,
      client_id: configuration.clientId,
      client_secret: configuration.clientSecret,
      redirect_uri: redirectUri,
      grant_type: "authorization_code",
    }).toString(),
    signal: AbortSignal.timeout(15_000),
  });
  if (response.status < 200 || response.status >= 300) throw new Error("status");
  const body = (await response.json()) as { id_token?: unknown };
  if (typeof body.id_token !== "string" || !body.id_token) throw new Error("id_token");
  return body.id_token;
}

async function finishCallback(ctx: Ctx, provider: string, values: Record<string, string>): Promise<Response> {
  const state = (values.state ?? "").trim();
  const row = ctx.sql.tx(() =>
    ctx.sql.get(
      `DELETE FROM gonvex_auth_transactions WHERE token_hash=$1 AND expires_at>${NOW_MS}
       RETURNING project_id,redirect_uri,app_state,code_challenge,nonce,google_redirect_uri,provider`,
      sha256Hex(state),
    ),
  );
  if (!row) return error(400, "invalid or expired OAuth state");
  const redirectUri = str(row.redirect_uri);
  const appState = str(row.app_state);
  if (str(row.provider) !== provider) return redirectError(redirectUri, appState, "provider_mismatch");
  if (values.error) return redirectError(redirectUri, appState, values.error);
  const project = str(row.project_id);
  let configuration: NativeProvider;
  try {
    configuration = nativeProviderConfiguration(ctx, project, provider, redirectUri);
  } catch {
    return redirectError(redirectUri, appState, "provider_not_configured");
  }
  let idToken: string;
  try {
    idToken = await exchangeProviderCode(ctx, configuration, (values.code ?? "").trim(), str(row.google_redirect_uri));
  } catch {
    return redirectError(redirectUri, appState, "provider_exchange_failed");
  }
  let identity;
  try {
    identity = await verifyExternalToken(ctx.config, ctx.fetch, externalOf(configuration), idToken);
  } catch {
    return redirectError(redirectUri, appState, "invalid_provider_identity");
  }
  if (!tokenNonceMatches(idToken, str(row.nonce))) return redirectError(redirectUri, appState, "invalid_provider_identity");
  if (provider === "microsoft" && identity.email) identity.emailVerified = true;
  let account: AuthAccount;
  try {
    account = ctx.sql.tx(() => resolveExternalAccount(ctx, project, identity));
  } catch {
    return redirectError(redirectUri, appState, "account_creation_failed");
  }
  const authCode = nativeSecureToken("authcode");
  try {
    ctx.sql.tx(() =>
      ctx.sql.run(
        "INSERT INTO gonvex_auth_codes(code_hash,project_id,account_id,redirect_uri,code_challenge,expires_at) VALUES($1,$2,$3,$4,$5,$6)",
        sha256Hex(authCode),
        project,
        account.id,
        redirectUri,
        str(row.code_challenge),
        Date.now() + CODE_MS,
      ),
    );
  } catch {
    return redirectError(redirectUri, appState, "code_creation_failed");
  }
  return redirectValues(redirectUri, [["code", authCode], ["state", appState]]);
}

/** `exchange_code` (native_auth.rs:497). Runs inside the writer transaction. */
function exchangeCode(ctx: Ctx, project: string, code: string, verifier: string, redirectUri: string): { grant: SessionGrant; account: AuthAccount } {
  if (!code || !validPkce(verifier)) throw new Error("invalid authorization code");
  const redirect = normalizeRedirect(redirectUri);
  const row = ctx.sql.get(
    `SELECT project_id,account_id,redirect_uri,code_challenge FROM gonvex_auth_codes
     WHERE code_hash=$1 AND used_at IS NULL AND expires_at>${NOW_MS}`,
    sha256Hex(code),
  );
  if (!row) throw new Error("invalid authorization code");
  if (str(row.project_id) !== project || str(row.redirect_uri) !== redirect || !constantTimeEqual(str(row.code_challenge), pkceChallenge(verifier))) {
    throw new Error("authorization code does not match this client");
  }
  ctx.sql.run(`UPDATE gonvex_auth_codes SET used_at=${NOW_MS} WHERE code_hash=$1`, sha256Hex(code));
  const accountId = str(row.account_id);
  const account = loadAuthAccount(ctx, project, accountId);
  const grant = issueSession(ctx, project, accountId, `family_${crypto.randomUUID()}`, Date.now() + REFRESH_TOKEN_MS);
  return { grant, account };
}

async function token(ctx: Ctx, request: Request): Promise<Response> {
  const body = await jsonBody(request, ["grantType", "project"]);
  const project = stringField(body, "project").trim();
  const grantType = stringField(body, "grantType");
  const fields = {
    code: stringField(body, "code"),
    codeVerifier: stringField(body, "codeVerifier"),
    redirectUri: stringField(body, "redirectUri"),
    refreshToken: stringField(body, "refreshToken"),
    tenant: stringField(body, "tenant"),
  };
  if (!project) return error(400, "project is required");
  if (grantType !== "authorization_code" && grantType !== "refresh_token") return error(400, "grantType must be authorization_code or refresh_token");
  type Outcome = { kind: "ok"; result: Record<string, unknown> } | { kind: "reuse" } | { kind: "fail" };
  let outcome: Outcome;
  try {
    outcome = ctx.sql.tx((): Outcome => {
      if (grantType === "authorization_code") {
        let exchanged;
        try {
          exchanged = exchangeCode(ctx, project, fields.code.trim(), fields.codeVerifier.trim(), fields.redirectUri.trim());
        } catch {
          // The reference returns before committing: nothing is written.
          throw new Rejection(error(401, "invalid or expired authentication grant"));
        }
        return { kind: "ok", result: sessionResult(exchanged.grant, exchanged.account, fields.tenant.trim()) };
      }
      let refreshed;
      try {
        refreshed = refreshSession(ctx, project, fields.refreshToken.trim());
      } catch {
        throw new Rejection(error(401, "invalid or expired authentication grant"));
      }
      if (refreshed.kind === "reuseRevoked") return { kind: "reuse" };
      return { kind: "ok", result: sessionResult(refreshed.grant, refreshed.account, "") };
    });
  } catch (cause) {
    if (cause instanceof Rejection) return cause.response;
    return error(503, "auth session service is unavailable");
  }
  if (outcome.kind === "reuse") return error(401, "invalid or expired authentication grant");
  if (outcome.kind === "fail") return error(401, "invalid or expired authentication grant");
  try {
    return jsonResponse(200, await sessionResultFromDirectory(ctx, project, outcome.result));
  } catch {
    return error(503, "tenant memberships are unavailable");
  }
}

async function logout(ctx: Ctx, request: Request): Promise<Response> {
  const body = await jsonBody(request, []);
  const refresh = stringField(body, "refreshToken");
  if (body.all !== undefined && typeof body.all !== "boolean") {
    throw new Rejection(text(422, "Failed to deserialize the JSON body into the target type: all: invalid type: expected a boolean"));
  }
  const all = body.all === true;
  const header = request.headers.get("authorization") ?? "";
  const access = (header.startsWith("Bearer ") ? header.slice(7) : "").trim();
  try {
    ctx.sql.tx(() => {
      const family = ctx.sql.value<string>(
        `SELECT family_id FROM gonvex_auth_sessions WHERE token_hash=$1
         UNION SELECT family_id FROM gonvex_auth_refresh_tokens WHERE token_hash=$2 LIMIT 1`,
        sha256Hex(access),
        sha256Hex(refresh),
      );
      if (family === undefined) return;
      if (all) {
        const owner = ctx.sql.get("SELECT project_id,account_id FROM gonvex_auth_sessions WHERE family_id=$1 LIMIT 1", family);
        if (owner) {
          ctx.sql.run(`UPDATE gonvex_auth_sessions SET revoked_at=COALESCE(revoked_at,${NOW_MS}) WHERE project_id=$1 AND account_id=$2`, str(owner.project_id), str(owner.account_id));
          ctx.sql.run(`UPDATE gonvex_auth_refresh_tokens SET revoked_at=COALESCE(revoked_at,${NOW_MS}) WHERE project_id=$1 AND account_id=$2`, str(owner.project_id), str(owner.account_id));
        }
      } else {
        revokeFamily(ctx, family);
      }
    });
  } catch {
    /* the reference answers ok whatever happens */
  }
  return jsonResponse(200, { ok: true });
}

/** Handles `/auth/*`. Returns null for paths that are not native OAuth routes. */
export async function handleNativeAuth(ctx: Ctx, request: Request): Promise<Response | null> {
  const url = new URL(request.url);
  const path = url.pathname;
  let response: Response | null = null;
  try {
    if (path === "/auth/config") {
      response = request.method === "GET" || request.method === "HEAD" ? await authConfig(ctx, url) : new Response(null, { status: 405 });
    } else if (path === "/auth/token" || path === "/auth/logout") {
      if (request.method === "OPTIONS") response = new Response(null, { status: 204 });
      else if (request.method === "POST") response = path === "/auth/token" ? await token(ctx, request) : await logout(ctx, request);
      else response = new Response(null, { status: 405 });
    } else {
      const match = /^\/auth\/([^/]+)\/(authorize|callback)$/.exec(path);
      if (!match) return null;
      const provider = decodeURIComponent(match[1]!);
      if (match[2] === "authorize") {
        response = request.method === "GET" || request.method === "HEAD" ? await authorize(ctx, provider, url) : new Response(null, { status: 405 });
      } else if (request.method === "GET" || request.method === "HEAD") {
        response = await finishCallback(ctx, provider, Object.fromEntries(url.searchParams));
      } else if (request.method === "POST") {
        const type = request.headers.get("content-type") ?? "";
        if (!/^application\/x-www-form-urlencoded/i.test(type)) {
          response = text(415, "Form requests must have `Content-Type: application/x-www-form-urlencoded`");
        } else {
          response = await finishCallback(ctx, provider, Object.fromEntries(new URLSearchParams(await request.text())));
        }
      } else {
        response = new Response(null, { status: 405 });
      }
    }
  } catch (cause) {
    if (!(cause instanceof Rejection)) throw cause;
    response = cause.response;
  }
  return withCors(response);
}
