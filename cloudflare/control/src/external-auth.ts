/**
 * External identity verification: Firebase and external OIDC
 * (rust/crates/runtime/src/external_auth.rs; contract C.3.4).
 *
 * RS256 only, with a `kid`; `exp`, `iat`, `sub`, `iss`, `aud` required with
 * 30 s leeway; `iat <= now + 60`. Firebase also needs `auth_time <= now + 60`
 * and a matching `firebase.tenant`, and with admin credentials configured
 * every exchange calls `accounts:lookup`.
 *
 * Adapted for Workers: keys come through Worker `fetch` (HTTPS, no
 * credentials, no `localhost`, no redirects, 5 s timeout); the reference's
 * resolve-and-pin to a public IP has no Workers equivalent, and Workers cannot
 * reach private networks. JWKS are cached per URL in isolate memory with the
 * same max-age rules. RSA verification and signing use WebCrypto.
 */
import type { ControlConfig } from "./context";
import { external } from "./errors";
import { sha256Hex } from "./util";
import type { VerifiedExternalIdentity } from "./sessions";

export interface ExternalAuthConfiguration {
  provider: string;
  issuer: string;
  audience: string;
  jwksUrl: string;
  firebaseTenantId: string;
  signupMode: string;
  firebaseProjectId: string;
  firebaseAdminCredentials: string | null;
}

interface Jwk {
  kid?: string;
  kty?: string;
  n?: string;
  e?: string;
  alg?: string;
  [key: string]: unknown;
}

const cache = new Map<string, { expiresAt: number; keys: Jwk[] }>();
/**
 * X14: Google OAuth access tokens of the Firebase admin service account, per isolate. Google issues them for ~1 h
 * and they only authorize `accounts:lookup`; fetching one per exchange cost a Google round trip (75-216 ms from the
 * edge, X14 report). Keyed by a digest of the token URL and the whole credentials JSON, so a rotated key never
 * reuses a token. The lookup itself (disabled / revoked check) still runs on every exchange.
 */
const accessTokens = new Map<string, { token: string; expiresAt: number }>();
const ACCESS_TOKEN_MARGIN_MS = 5 * 60_000;
const ACCESS_TOKEN_MAX_MS = 60 * 60_000;

/** X14: wall time of each part of one exchange's verification, for the dev diagnostics (no identifiers). */
export interface ExchangeTimings { jwksMs?: number; verifyMs?: number; accessTokenMs?: number; accessTokenCached?: boolean; lookupMs?: number }
const MIN_AGE = 30_000;
const MAX_AGE = 60 * 60_000;
const DEFAULT_AGE = 5 * 60_000;

function decodeSegment(segment: string): unknown {
  if (!/^[A-Za-z0-9_-]*$/.test(segment)) throw external.invalidToken();
  try {
    return JSON.parse(Buffer.from(segment, "base64url").toString("utf8"));
  } catch {
    throw external.invalidToken();
  }
}

function base64urlBytes(segment: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]*$/.test(segment)) throw external.invalidToken();
  return new Uint8Array(Buffer.from(segment, "base64url"));
}

/** `safe_client` scheme and host checks (external_auth.rs:456-498). */
function safeUrl(raw: string): URL {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw external.unsafeJwks();
  }
  if (url.protocol !== "https:" || url.username || url.password) throw external.unsafeJwks();
  const host = url.hostname.toLowerCase();
  if (!host || host === "localhost" || host.endsWith(".localhost")) throw external.unsafeJwks();
  return url;
}

export function cacheMaxAge(header: string): number | null {
  for (const part of header.split(",")) {
    const match = /^max-age=(\d+)$/.exec(part.trim());
    if (match) return Number(match[1]) * 1000;
  }
  return null;
}

async function jwks(config: ControlConfig, fetcher: typeof fetch, url: string, force: boolean): Promise<Jwk[]> {
  if (!force) {
    const cached = cache.get(url);
    if (cached && cached.expiresAt > Date.now()) return cached.keys;
  }
  const target = safeUrl(url);
  let response: Response;
  try {
    response = await fetcher(target.toString(), { redirect: "manual", signal: AbortSignal.timeout(5000) });
  } catch {
    throw external.keysUnavailable();
  }
  if (response.status >= 400) throw external.keysUnavailable();
  const cacheControl = response.headers.get("cache-control") ?? "";
  const noStore = cacheControl.split(",").some((part) => ["no-store", "no-cache"].includes(part.trim().toLowerCase()));
  const maxAge = noStore ? 0 : Math.min(MAX_AGE, Math.max(MIN_AGE, cacheMaxAge(cacheControl) ?? DEFAULT_AGE));
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw external.keysUnavailable();
  }
  const keys = (body as { keys?: unknown })?.keys;
  if (!Array.isArray(keys) || keys.some((key) => !key || typeof key !== "object" || typeof (key as Jwk).kty !== "string")) {
    throw external.keysUnavailable();
  }
  if (maxAge > 0) cache.set(url, { expiresAt: Date.now() + maxAge, keys: keys as Jwk[] });
  void config;
  return keys as Jwk[];
}

/** Clears the JWKS and access-token caches (tests). */
export const clearJwksCache = (): void => { cache.clear(); accessTokens.clear(); };

const numeric = (value: unknown): number | null => (typeof value === "number" && Number.isFinite(value) ? Math.trunc(value) : null);

/** jsonwebtoken `decode` with `Validation::new(RS256)` as configured in external_auth.rs:113-131. */
async function verifiedClaims(config: ControlConfig, fetcher: typeof fetch, configuration: ExternalAuthConfiguration, token: string, timings?: ExchangeTimings): Promise<Record<string, unknown>> {
  const parts = token.split(".");
  if (parts.length !== 3) throw external.invalidToken();
  const header = decodeSegment(parts[0]!) as Record<string, unknown> | null;
  if (!header || typeof header !== "object" || header.alg !== "RS256") throw external.invalidToken();
  const kid = header.kid;
  if (typeof kid !== "string") throw external.invalidToken();
  const keysAt = Date.now();
  let keys = await jwks(config, fetcher, configuration.jwksUrl, false);
  if (!keys.some((key) => key.kid === kid)) keys = await jwks(config, fetcher, configuration.jwksUrl, true);
  if (timings) timings.jwksMs = Date.now() - keysAt;
  const jwk = keys.find((key) => key.kid === kid);
  if (!jwk || jwk.kty !== "RSA" || typeof jwk.n !== "string" || typeof jwk.e !== "string") throw external.invalidToken();
  let key: CryptoKey;
  try {
    key = await crypto.subtle.importKey("jwk", { kty: "RSA", n: jwk.n, e: jwk.e, ext: true }, { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["verify"]);
  } catch {
    throw external.invalidToken();
  }
  const signed = new TextEncoder().encode(`${parts[0]}.${parts[1]}`);
  const valid = await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, base64urlBytes(parts[2]!), signed).catch(() => false);
  if (!valid) throw external.invalidToken();
  const claims = decodeSegment(parts[1]!) as Record<string, unknown> | null;
  if (!claims || typeof claims !== "object" || Array.isArray(claims)) throw external.invalidToken();
  for (const required of ["exp", "iat", "sub", "iss", "aud"]) if (!(required in claims)) throw external.invalidToken();
  const exp = numeric(claims.exp);
  if (exp === null || exp < 0) throw external.invalidToken();
  const now = Math.floor(Date.now() / 1000);
  if (exp < now - 30) throw external.expired();
  if (claims.iss !== configuration.issuer) throw external.wrongIssuer();
  const audiences = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
  if (!audiences.some((aud) => aud === configuration.audience)) throw external.wrongAudience();
  return claims;
}

/** The emulator gate (external_auth.rs:240-258). */
export function emulatorAllowed(configuration: ExternalAuthConfiguration, emulatorHost: string | undefined): boolean {
  if (!emulatorHost) return false;
  const host = emulatorHost.replace(/:\d+$/, "").replace(/^\[|\]$/g, "");
  const loopback = host === "127.0.0.1" || host === "::1" || host === "localhost";
  return (
    loopback &&
    configuration.provider === "firebase" &&
    configuration.firebaseProjectId.startsWith("demo-") &&
    configuration.audience === configuration.firebaseProjectId &&
    configuration.issuer === `https://securetoken.google.com/${configuration.firebaseProjectId}` &&
    configuration.firebaseAdminCredentials === null
  );
}

/** Unsigned emulator ID tokens (external_auth.rs:260-290). */
function emulatorClaims(configuration: ExternalAuthConfiguration, token: string): Record<string, unknown> {
  const parts = token.split(".");
  if (parts.length !== 3 || parts[2] !== "") throw external.invalidToken();
  const header = decodeSegment(parts[0]!) as Record<string, unknown> | null;
  if (!header || header.alg !== "none") throw external.invalidToken();
  const claims = decodeSegment(parts[1]!) as Record<string, unknown> | null;
  if (!claims || typeof claims !== "object") throw external.invalidToken();
  if (claims.iss !== configuration.issuer) throw external.wrongIssuer();
  if (claims.aud !== configuration.audience) throw external.wrongAudience();
  const exp = numeric(claims.exp);
  if (exp === null) throw external.invalidToken();
  if (exp <= Math.floor(Date.now() / 1000)) throw external.expired();
  return claims;
}

/** `identity_from_claims` (external_auth.rs:136-218). */
async function identityFromClaims(
  config: ControlConfig,
  fetcher: typeof fetch,
  configuration: ExternalAuthConfiguration,
  claims: Record<string, unknown>,
  timings?: ExchangeTimings,
  hooks?: ExternalVerifyHooks,
): Promise<VerifiedExternalIdentity> {
  const subject = typeof claims.sub === "string" ? claims.sub.trim() : "";
  if (!subject) throw external.missingSubject();
  const now = Math.floor(Date.now() / 1000);
  const issuedAt = numeric(claims.iat);
  if (issuedAt === null || !Number.isInteger(claims.iat)) throw external.invalidToken();
  if (issuedAt > now + 60) throw external.invalidToken();
  const firebase = claims.firebase && typeof claims.firebase === "object" && !Array.isArray(claims.firebase) ? (claims.firebase as Record<string, unknown>) : null;
  let authTime: number;
  if (configuration.provider === "firebase") {
    if (!Number.isInteger(claims.auth_time)) throw external.invalidToken();
    authTime = claims.auth_time as number;
    if (authTime > now + 60) throw external.invalidToken();
    const tenant = typeof firebase?.tenant === "string" ? firebase.tenant : "";
    if (tenant !== configuration.firebaseTenantId) throw external.firebaseTenantMismatch();
  } else {
    authTime = Number.isInteger(claims.auth_time) ? (claims.auth_time as number) : issuedAt;
  }
  const verified = claims.email_verified;
  const identity: VerifiedExternalIdentity = {
    provider: configuration.provider,
    issuer: configuration.issuer,
    subject,
    email: typeof claims.email === "string" ? claims.email.trim().toLowerCase() : "",
    emailVerified: verified === true || (typeof verified === "string" && verified.toLowerCase() === "true"),
    name: typeof claims.name === "string" ? claims.name.trim() : "",
    picture: typeof claims.picture === "string" ? claims.picture.trim() : "",
    signInProvider: typeof firebase?.sign_in_provider === "string" ? firebase.sign_in_provider : configuration.provider,
    authTime,
  };
  // X17h: the token itself is verified (signature, issuer, audience, expiry, tenant); the caller may start read-only
  // work for this identity while the provider's account check runs. Nothing it starts may grant anything on its own.
  hooks?.beforeAccountCheck?.(identity);
  if (configuration.firebaseAdminCredentials !== null) {
    await verifyFirebaseAccount(config, fetcher, configuration, configuration.firebaseAdminCredentials, identity, timings);
  }
  return identity;
}

function pemToDer(pem: string): Uint8Array {
  const body = pem.replace(/-----BEGIN [^-]+-----|-----END [^-]+-----|\s+/g, "");
  return new Uint8Array(Buffer.from(body, "base64"));
}

function lookup(fetcher: typeof fetch, url: string, accessToken: string, subject: string): Promise<Response> {
  return fetcher(url, {
    method: "POST",
    headers: { authorization: `Bearer ${accessToken}`, "content-type": "application/json" },
    body: JSON.stringify({ localId: [subject] }),
    redirect: "manual",
    signal: AbortSignal.timeout(8000),
  });
}

/** The service-account JWT exchanged for an access token (external_auth.rs:331-446), cached per isolate (X14). */
async function serviceAccountToken(fetcher: typeof fetch, tokenUrl: string, cacheKey: string, credentials: { client_email: string; private_key: string }): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  let assertion: string;
  try {
    const key = await crypto.subtle.importKey("pkcs8", pemToDer(credentials.private_key), { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["sign"]);
    const header = Buffer.from(JSON.stringify({ typ: "JWT", alg: "RS256" })).toString("base64url");
    const payload = Buffer.from(
      JSON.stringify({
        iss: credentials.client_email,
        scope: "https://www.googleapis.com/auth/identitytoolkit",
        aud: "https://oauth2.googleapis.com/token",
        iat: now,
        exp: now + 300,
      }),
    ).toString("base64url");
    const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(`${header}.${payload}`));
    assertion = `${header}.${payload}.${Buffer.from(signature).toString("base64url")}`;
  } catch {
    throw external.firebaseAccountUnavailable();
  }
  const form = new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion });
  try {
    const response = await fetcher(tokenUrl, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: form.toString(),
      redirect: "manual",
      signal: AbortSignal.timeout(8000),
    });
    if (response.status < 200 || response.status >= 300) throw external.firebaseAccountUnavailable();
    const body = (await response.json()) as { access_token?: unknown; expires_in?: unknown };
    if (typeof body.access_token !== "string" || !body.access_token) throw external.firebaseAccountUnavailable();
    const lifetime = typeof body.expires_in === "number" && Number.isFinite(body.expires_in) ? Math.min(body.expires_in * 1000, ACCESS_TOKEN_MAX_MS) : 0;
    if (lifetime > ACCESS_TOKEN_MARGIN_MS) accessTokens.set(cacheKey, { token: body.access_token, expiresAt: Date.now() + lifetime - ACCESS_TOKEN_MARGIN_MS });
    return body.access_token;
  } catch {
    throw external.firebaseAccountUnavailable();
  }
}

/** `verify_firebase_account` (external_auth.rs:331-446): service-account JWT, then `accounts:lookup`. */
async function verifyFirebaseAccount(
  config: ControlConfig,
  fetcher: typeof fetch,
  configuration: ExternalAuthConfiguration,
  credentialsJson: string,
  identity: VerifiedExternalIdentity,
  timings?: ExchangeTimings,
): Promise<void> {
  let credentials: { project_id?: unknown; client_email?: unknown; private_key?: unknown };
  try {
    credentials = JSON.parse(credentialsJson);
  } catch {
    throw external.firebaseAccountUnavailable();
  }
  if (
    typeof credentials.project_id !== "string" ||
    typeof credentials.client_email !== "string" ||
    typeof credentials.private_key !== "string" ||
    credentials.project_id !== configuration.firebaseProjectId ||
    !credentials.client_email.trim() ||
    !credentials.private_key.trim()
  ) {
    throw external.firebaseAccountUnavailable();
  }
  const tokenUrl = config.googleTokenUrl ?? "https://oauth2.googleapis.com/token";
  const cacheKey = sha256Hex(`${tokenUrl}\0${credentialsJson}`);
  const tokenAt = Date.now();
  const cachedToken = accessTokens.get(cacheKey);
  let accessToken = cachedToken && cachedToken.expiresAt > Date.now() ? cachedToken.token : undefined;
  const fromCache = accessToken !== undefined;
  if (timings) timings.accessTokenCached = fromCache;
  if (accessToken === undefined) accessToken = await serviceAccountToken(fetcher, tokenUrl, cacheKey, credentials as { client_email: string; private_key: string });
  if (timings) timings.accessTokenMs = Date.now() - tokenAt;
  const base = config.identityToolkitUrl ?? "https://identitytoolkit.googleapis.com";
  const lookupUrl = `${base}/v1/projects/${encodeURIComponent(configuration.firebaseProjectId).replace(/%20/g, "+")}/accounts:lookup`;
  const lookupAt = Date.now();
  let body: { users?: Array<Record<string, unknown>> };
  try {
    let response = await lookup(fetcher, lookupUrl, accessToken, identity.subject);
    if ((response.status === 401 || response.status === 403) && fromCache) {
      // A cached token Google no longer accepts: drop it and retry once with a fresh one.
      accessTokens.delete(cacheKey);
      accessToken = await serviceAccountToken(fetcher, tokenUrl, cacheKey, credentials as { client_email: string; private_key: string });
      response = await lookup(fetcher, lookupUrl, accessToken, identity.subject);
    }
    if (response.status < 200 || response.status >= 300) throw external.firebaseAccountUnavailable();
    body = (await response.json()) as typeof body;
  } catch {
    throw external.firebaseAccountUnavailable();
  }
  if (timings) timings.lookupMs = Date.now() - lookupAt;
  const user = Array.isArray(body.users) ? body.users.find((entry) => entry?.localId === identity.subject) : undefined;
  if (!user) throw external.firebaseAccountRevoked();
  if (user.disabled === true) throw external.firebaseAccountRevoked();
  const validSince = typeof user.validSince === "string" && /^[+-]?\d+$/.test(user.validSince) ? Number(user.validSince) : 0;
  if (identity.authTime < validSince) throw external.firebaseAccountRevoked();
}

/** `verify_external_token`. */
/** X17h: called once the token is verified and before the provider's account check (see `identityFromClaims`). */
export interface ExternalVerifyHooks { beforeAccountCheck?(identity: VerifiedExternalIdentity): void }

export async function verifyExternalToken(
  config: ControlConfig,
  fetcher: typeof fetch,
  configuration: ExternalAuthConfiguration,
  token: string,
  timings?: ExchangeTimings,
  hooks?: ExternalVerifyHooks,
): Promise<VerifiedExternalIdentity> {
  if (!token.trim()) throw external.invalidToken();
  const verifyAt = Date.now();
  const claims = config.authEmulator
    ? emulatorAllowed(configuration, config.authEmulator.host)
      ? emulatorClaims(configuration, token)
      : (() => {
          throw external.invalidToken();
        })()
    : await verifiedClaims(config, fetcher, configuration, token, timings);
  if (timings) timings.verifyMs = Date.now() - verifyAt;
  return identityFromClaims(config, fetcher, configuration, claims, timings, hooks);
}
