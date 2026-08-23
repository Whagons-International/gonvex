import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, useSyncExternalStore, type ButtonHTMLAttributes, type ReactNode } from "react";
import { GonvexClient, GonvexClientError, control, type ConnectionState, type FunctionReference, type LiveQueryResult, type ReplicaCollectionState, type ReplicaRow } from "@gonvex/client";
import type { JsonValue } from "@gonvex/protocol";

export { GonvexClientError, type ConnectionState } from "@gonvex/client";

const GonvexContext = createContext<GonvexClient | null>(null);
const GonvexAuthContext = createContext<AuthState>({ isLoading: false, isAuthenticated: true });

export function GonvexProvider(props: { client: GonvexClient; children: ReactNode }) {
  return <GonvexContext.Provider value={props.client}>{props.children}</GonvexContext.Provider>;
}

export type AuthState = {
  isLoading: boolean;
  isAuthenticated: boolean;
  /** Terminal runtime rejection after one forced token refresh attempt. */
  authError?: Error | null;
  fetchAccessToken?: (args: { forceRefreshToken: boolean }) => Promise<string | null>;
};

export type GonvexAuthAccount = {
  id: string;
  email?: string;
  emailVerified: boolean;
  name?: string;
  picture?: string;
  provider: "google" | string;
};

export type GonvexAuthTenant = {
  id: string;
  name: string;
  role: "owner" | "admin" | "member" | "viewer" | string;
  permissions?: Record<string, unknown>;
};

type GonvexAuthSession = {
  accessToken: string;
  expiresAt: number;
  refreshToken: string;
  refreshExpiresAt: number;
  account: GonvexAuthAccount;
  tenants: GonvexAuthTenant[];
  activeTenantId?: string;
};

type PKCEState = { state: string; verifier: string; redirectUri: string; returnTo: string; createdAt: number };

class GonvexAuthRequestError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "GonvexAuthRequestError";
    this.status = status;
  }
}

export type GonvexAuthValue = AuthState & {
  account: GonvexAuthAccount | null;
  tenants: GonvexAuthTenant[];
  activeTenant: GonvexAuthTenant | null;
  error: string | null;
  signIn: () => Promise<void>;
  signOut: (options?: { allDevices?: boolean }) => Promise<void>;
  setActiveTenant: (tenantId: string) => Promise<void>;
  refreshMemberships: () => Promise<GonvexAuthTenant[]>;
  createTenant: (name: string) => Promise<GonvexAuthTenant>;
  inviteMember: (tenantId: string, email: string, options?: { role?: GonvexAuthTenant["role"]; permissions?: Record<string, unknown> }) => Promise<void>;
  revokeInvitation: (tenantId: string, email: string) => Promise<void>;
};

export type GonvexAuthConfig = {
  runtimeUrl: string;
  projectId: string;
  callbackPath?: string;
};

const ManagedAuthContext = createContext<GonvexAuthValue | null>(null);

export function GonvexProviderWithAuth(props: {
  client: GonvexClient;
  children: ReactNode;
  useAuth: () => AuthState;
}) {
  const auth = props.useAuth();
  const [tokenReady, setTokenReady] = useState(false);
  const [clientAuthError, setClientAuthError] = useState<Error | null>(null);
  const authError = auth.authError ?? clientAuthError;

  useEffect(() => props.client.onAuthError((message) => {
    setClientAuthError(new Error(message || "Authentication failed"));
    setTokenReady(false);
  }), [props.client]);

  useEffect(() => {
    setTokenReady(false);
    if (auth.isLoading || !auth.isAuthenticated || !auth.fetchAccessToken) {
      if (!auth.isLoading && !auth.isAuthenticated) setClientAuthError(null);
      return;
    }
    const fetchAccessToken = auth.fetchAccessToken;
    let cancelled = false;
    void fetchAccessToken({ forceRefreshToken: false }).then(
      (token) => {
        if (!cancelled) {
          // Install the fetcher alongside the token so the client re-fetches
          // on reconnect and force-refreshes on auth.error itself, instead of
          // replaying this token verbatim after it expires.
          props.client.setAuth({ token: token ?? undefined, fetchToken: fetchAccessToken });
          setClientAuthError(null);
          setTokenReady(Boolean(token));
        }
      },
      (error) => {
        // A rejected token fetch must not hold the app at `null` forever. The
        // canonical case is an offline load whose identity provider needs the
        // network to refresh: the client may already hold a locally cached
        // token installed before mount, and local-first reads work without a
        // fresh one. Release the children and leave the client's existing auth
        // untouched; the next auth state change re-runs this effect.
        if (!cancelled) {
          console.warn("[gonvex] fetchAccessToken failed; continuing with existing client auth", error);
          setTokenReady(true);
        }
      },
    );
    return () => {
      cancelled = true;
    };
  }, [auth.isLoading, auth.isAuthenticated, auth.fetchAccessToken, props.client]);

  const authValue = useMemo<AuthState>(
    () => ({
      ...auth,
      authError,
      isLoading: !authError && (auth.isLoading || (auth.isAuthenticated && !tokenReady)),
      isAuthenticated: !authError && auth.isAuthenticated && tokenReady,
    }),
    [auth, authError, tokenReady],
  );

  const shouldHoldChildren = !authError && (auth.isLoading || (auth.isAuthenticated && !tokenReady));

  return (
    <GonvexAuthContext.Provider value={authValue}>
      <GonvexProvider client={props.client}>{shouldHoldChildren ? null : props.children}</GonvexProvider>
    </GonvexAuthContext.Provider>
  );
}

/**
 * Native Gonvex authentication. The runtime performs the one centrally
 * configured Google OAuth flow, while each app uses PKCE and receives a
 * project-scoped Gonvex session. No Firebase or Google SDK is loaded in the
 * browser.
 */
// Dedupe callback bootstrap across React StrictMode remounts so the OAuth
// code+PKCE exchange runs once. Without this, the first effect's finally
// clears sessionStorage PKCE before the remount can finish verification.
const authBootstrapPromises = new Map<string, Promise<GonvexAuthSession | null>>();

export function GonvexAuthProvider(props: GonvexAuthConfig & { client: GonvexClient; children: ReactNode }) {
  const runtimeUrl = props.runtimeUrl.replace(/\/+$/, "");
  const callbackPath = normalizeCallbackPath(props.callbackPath ?? "/");
  const storageKey = `gonvex-auth:${encodeURIComponent(runtimeUrl)}:${props.projectId}`;
  const pkceStorageKey = `${storageKey}:pkce`;
  const [session, setSession] = useState<GonvexAuthSession | null>(() => readAuthSession(storageKey));
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshRetryAt, setRefreshRetryAt] = useState(0);
  const sessionRef = useRef(session);
  const refreshRef = useRef<Promise<GonvexAuthSession | null> | null>(null);

  const installSession = useCallback((next: GonvexAuthSession | null, persist = true) => {
    sessionRef.current = next;
    if (next) {
      if (persist) safeLocalStorageSet(storageKey, JSON.stringify(next));
      props.client.setAuth({
        project: props.projectId, tenant: next.activeTenantId, token: next.accessToken,
        identity: { sub: next.account.id, iss: props.projectId },
      });
    } else {
      if (persist) safeLocalStorageRemove(storageKey);
      props.client.setAuth({ project: props.projectId, tenant: undefined, token: undefined, identity: undefined });
    }
    setSession(next);
  }, [props.client, props.projectId, storageKey]);

  useEffect(() => {
    let cancelled = false;
    let bootstrap = authBootstrapPromises.get(storageKey);
    if (!bootstrap) {
      bootstrap = bootstrapGonvexAuth({ callbackPath, client: props.client, pkceStorageKey, projectId: props.projectId, runtimeUrl, storageKey })
        .finally(() => {
          // Keep the resolved promise briefly so a StrictMode remount attaches
          // to the same result instead of re-running a spent OAuth code.
          window.setTimeout(() => {
            if (authBootstrapPromises.get(storageKey) === bootstrap) {
              authBootstrapPromises.delete(storageKey);
            }
          }, 0);
        });
      authBootstrapPromises.set(storageKey, bootstrap);
    }
    void bootstrap.then((next) => {
      if (!cancelled) installSession(next);
    }).catch((cause) => {
      if (!cancelled) {
        installSession(null);
        setError(cause instanceof Error ? cause.message : "Google sign-in failed.");
      }
    }).finally(() => {
      if (!cancelled) setIsLoading(false);
    });
    return () => { cancelled = true; };
  }, [callbackPath, installSession, pkceStorageKey, props.projectId, runtimeUrl, storageKey]);

  const refreshSession = useCallback(async (force = false) => {
    if (refreshRef.current) return refreshRef.current;
    let attemptedRefreshToken = "";
    const request = withBrowserAuthLock(`${storageKey}:refresh`, async () => {
      const current = readAuthSession(storageKey) ?? sessionRef.current;
      if (!current || current.refreshExpiresAt <= Date.now()) return null;
      if (!force && current.expiresAt > Date.now() + 60_000) return current;
      attemptedRefreshToken = current.refreshToken;
      const grant = await props.client.action(control.auth.refreshSession, { refreshToken: current.refreshToken });
      const next = sessionFromNativeGrant(grant, current);
      // Persist the rotated token before releasing the cross-tab lock. The
      // next waiter must never read and reuse the just-consumed refresh token.
      safeLocalStorageSet(storageKey, JSON.stringify(next));
      setRefreshRetryAt(0);
      setError(null);
      return next;
    }).then((next) => {
      if (next) {
        setRefreshRetryAt(0);
        setError(null);
      }
      installSession(next);
      return next;
    }).catch((cause) => {
      const latest = readAuthSession(storageKey);
      if (latest && attemptedRefreshToken && latest.refreshToken !== attemptedRefreshToken) {
        // Another tab completed the one permitted rotation while this request
        // was in flight. Adopt its winner instead of erasing shared auth state.
        installSession(latest);
        setRefreshRetryAt(0);
        setError(null);
        return latest;
      }
      if (isFatalRefreshError(cause)) {
        installSession(null);
        setRefreshRetryAt(0);
        setError(cause instanceof Error ? cause.message : "Your session expired. Please sign in again.");
        return null;
      }
      // Network failures, timeouts, rate limits, and server outages must not
      // destroy a valid refresh credential. Keep it and retry shortly.
      if (sessionRef.current) setRefreshRetryAt(Date.now() + 5_000);
      setError("Gonvex could not refresh your session. Retrying shortly…");
      return null;
    }).finally(() => {
      refreshRef.current = null;
    });
    refreshRef.current = request;
    return request;
  }, [installSession, props.client, storageKey]);

  useEffect(() => {
    if (!session) return;
    const scheduledAt = Math.max(session.expiresAt - 60_000, refreshRetryAt);
    const delay = Math.max(0, scheduledAt - Date.now());
    const timeout = window.setTimeout(() => { void refreshSession(); }, delay);
    return () => window.clearTimeout(timeout);
  }, [refreshRetryAt, refreshSession, session]);

  useEffect(() => {
    const onStorage = (event: StorageEvent) => {
      if (event.key !== storageKey) return;
      installSession(readAuthSession(storageKey), false);
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, [installSession, storageKey]);

  const signIn = useCallback(async () => {
    setError(null);
    const verifier = randomBase64Url(64);
    const state = randomBase64Url(32);
    const challengeBytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
    const challenge = bytesToBase64Url(new Uint8Array(challengeBytes));
    const redirectUri = new URL(callbackPath, window.location.origin).toString();
    const returnTo = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    safeSessionStorageSet(pkceStorageKey, JSON.stringify({ state, verifier, redirectUri, returnTo, createdAt: Date.now() }));
    const authorizeUrl = new URL(`${runtimeUrl}/auth/google/authorize`);
    authorizeUrl.searchParams.set("project", props.projectId);
    authorizeUrl.searchParams.set("redirect_uri", redirectUri);
    authorizeUrl.searchParams.set("state", state);
    authorizeUrl.searchParams.set("code_challenge", challenge);
    authorizeUrl.searchParams.set("code_challenge_method", "S256");
    window.location.assign(authorizeUrl.toString());
  }, [callbackPath, pkceStorageKey, props.projectId, runtimeUrl]);

  const signOut = useCallback(async (options?: { allDevices?: boolean }) => {
    const current = sessionRef.current;
    setError(null);
    if (current) {
      await props.client.reducer(control.auth.logout, { refreshToken: current.refreshToken, all: options?.allDevices === true }).catch(() => undefined);
    }
    installSession(null);
  }, [installSession, props.client]);

  const fetchAccessToken = useCallback(async (args: { forceRefreshToken: boolean }) => {
    const current = sessionRef.current;
    if (!current) return null;
    if (!args.forceRefreshToken && current.expiresAt > Date.now() + 60_000) return current.accessToken;
    return (await refreshSession(args.forceRefreshToken))?.accessToken ?? null;
  }, [refreshSession]);

  const setActiveTenant = useCallback(async (tenantId: string) => {
    const current = sessionRef.current;
    if (!current || !current.tenants.some((tenant) => tenant.id === tenantId)) {
      throw new Error(`Your account does not have access to tenant ${tenantId}.`);
    }
    installSession({ ...current, activeTenantId: tenantId });
  }, [installSession]);

  const refreshMemberships = useCallback(async () => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before loading tenant memberships.");
    const current = sessionRef.current!;
    const [account, tenants] = await Promise.all([
      props.client.query(control.accounts.me, {}),
      props.client.query(control.tenants.mine, {}),
    ]);
    const mappedAccount: GonvexAuthAccount = { id: account.id, email: account.email, emailVerified: true, name: account.name, picture: account.avatarUrl, provider: current.account.provider };
    const mappedTenants = tenants as GonvexAuthTenant[];
    const activeTenantId = mappedTenants.some((tenant) => tenant.id === current.activeTenantId) ? current.activeTenantId : mappedTenants[0]?.id;
    installSession({ ...current, account: mappedAccount, tenants: mappedTenants, activeTenantId });
    return mappedTenants;
  }, [fetchAccessToken, installSession, props.client]);

  const createTenant = useCallback(async (name: string) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before creating a tenant.");
    const tenant = await props.client.reducer(control.tenants.create, { name }) as GonvexAuthTenant;
    const current = sessionRef.current!;
    installSession({ ...current, tenants: [...current.tenants.filter((item) => item.id !== tenant.id), tenant], activeTenantId: tenant.id });
    return tenant;
  }, [fetchAccessToken, installSession, props.client]);

  const inviteMember = useCallback(async (tenantId: string, email: string, options?: { role?: GonvexAuthTenant["role"]; permissions?: Record<string, unknown> }) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before inviting a member.");
    if (tenantId !== sessionRef.current?.activeTenantId) throw new Error("Switch to the tenant before inviting a member.");
    await props.client.reducer(control.invitations.create, { email, role: options?.role ?? "member", permissions: (options?.permissions ?? {}) as Record<string, JsonValue> });
  }, [fetchAccessToken, props.client]);

  const revokeInvitation = useCallback(async (tenantId: string, email: string) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before revoking an invitation.");
    if (tenantId !== sessionRef.current?.activeTenantId) throw new Error("Switch to the tenant before revoking an invitation.");
    await props.client.reducer(control.invitations.revoke, { id: "", email });
  }, [fetchAccessToken, props.client]);

  const activeTenant = session?.tenants.find((tenant) => tenant.id === session.activeTenantId) ?? null;

  const authValue = useMemo<GonvexAuthValue>(() => ({
    isLoading,
    isAuthenticated: Boolean(session && session.refreshExpiresAt > Date.now()),
    fetchAccessToken,
    account: session?.account ?? null,
    tenants: session?.tenants ?? [],
    activeTenant,
    error,
    signIn,
    signOut,
    setActiveTenant,
    refreshMemberships,
    createTenant,
    inviteMember,
    revokeInvitation,
  }), [activeTenant, createTenant, error, fetchAccessToken, inviteMember, isLoading, refreshMemberships, revokeInvitation, session, setActiveTenant, signIn, signOut]);

  return (
    <ManagedAuthContext.Provider value={authValue}>
      <GonvexAuthContext.Provider value={authValue}>
        <GonvexProvider client={props.client}>{isLoading ? null : props.children}</GonvexProvider>
      </GonvexAuthContext.Provider>
    </ManagedAuthContext.Provider>
  );
}

export function useGonvexAuth(): GonvexAuthValue {
  const value = useContext(ManagedAuthContext);
  if (!value) throw new Error("GonvexAuthProvider is required");
  return value;
}

/** Read the auth state installed by either auth provider. */
export function useGonvexAuthState(): AuthState {
  return useContext(GonvexAuthContext);
}

export function GonvexGoogleAuthButton(props: ButtonHTMLAttributes<HTMLButtonElement> & { signOutLabel?: string }) {
  const { signOutLabel = "Sign out", children, disabled, onClick, ...buttonProps } = props;
  const auth = useGonvexAuth();
  const label = auth.isLoading ? "Loading…" : auth.isAuthenticated ? signOutLabel : children ?? "Continue with Google";
  return (
    <button
      {...buttonProps}
      disabled={disabled || auth.isLoading}
      onClick={(event) => {
        onClick?.(event);
        if (event.defaultPrevented) return;
        void (auth.isAuthenticated ? auth.signOut() : auth.signIn());
      }}
      type={buttonProps.type ?? "button"}
    >
      {!auth.isAuthenticated && !auth.isLoading ? <GoogleMark /> : null}
      <span>{label}</span>
    </button>
  );
}

export function createGonvexAuth(config: GonvexAuthConfig) {
  function ConfiguredGonvexAuthProvider(props: { client: GonvexClient; children: ReactNode }) {
    return <GonvexAuthProvider {...config} {...props} />;
  }
  return {
    GonvexAuthProvider: ConfiguredGonvexAuthProvider,
    GoogleSignInButton: GonvexGoogleAuthButton,
    useGonvexAuth,
  };
}

function GoogleMark() {
  return (
    <svg aria-hidden="true" height="18" viewBox="0 0 18 18" width="18">
      <path fill="#4285F4" d="M17.64 9.2c0-.64-.06-1.25-.16-1.84H9v3.48h4.84a4.14 4.14 0 0 1-1.8 2.72v2.26h2.9c1.7-1.56 2.7-3.86 2.7-6.62Z" />
      <path fill="#34A853" d="M9 18c2.43 0 4.47-.8 5.96-2.18l-2.9-2.26c-.8.54-1.84.86-3.06.86-2.35 0-4.34-1.58-5.05-3.72H.96v2.34A9 9 0 0 0 9 18Z" />
      <path fill="#FBBC05" d="M3.95 10.7a5.41 5.41 0 0 1 0-3.4V4.96H.96a9 9 0 0 0 0 8.08l2.99-2.34Z" />
      <path fill="#EA4335" d="M9 3.58c1.32 0 2.5.45 3.44 1.35l2.58-2.58A8.62 8.62 0 0 0 9 0 9 9 0 0 0 .96 4.96L3.95 7.3C4.66 5.16 6.65 3.58 9 3.58Z" />
    </svg>
  );
}

async function bootstrapGonvexAuth(options: {
  callbackPath: string;
  client: GonvexClient;
  pkceStorageKey: string;
  projectId: string;
  runtimeUrl: string;
  storageKey: string;
}): Promise<GonvexAuthSession | null> {
  const current = readAuthSession(options.storageKey);
  const url = new URL(window.location.href);
  const onCallbackPath = url.pathname === options.callbackPath;
  const code = onCallbackPath ? url.searchParams.get("code") : null;
  const returnedState = onCallbackPath ? url.searchParams.get("state") : null;
  const callbackError = onCallbackPath ? url.searchParams.get("error") : null;
  if (!code && !callbackError) {
    if (!current) return null;
    if (current.expiresAt > Date.now() + 30_000) return current;
    try {
      return await withBrowserAuthLock(`${options.storageKey}:refresh`, async () => {
        const latest = readAuthSession(options.storageKey) ?? current;
        if (latest.expiresAt > Date.now() + 30_000) return latest;
        if (latest.refreshExpiresAt <= Date.now()) throw new GonvexAuthRequestError("Your session expired. Please sign in again.", 401);
        options.client.setAuth({ project: options.projectId, token: latest.accessToken });
        const grant = await options.client.action(control.auth.refreshSession, { refreshToken: latest.refreshToken });
        const next = sessionFromNativeGrant(grant, latest);
        safeLocalStorageSet(options.storageKey, JSON.stringify(next));
        return next;
      });
    } catch (cause) {
      if (!isFatalRefreshError(cause)) return readAuthSession(options.storageKey) ?? current;
      throw cause;
    }
  }

  const pkce = readPKCE(options.pkceStorageKey);
  // Prefer surfacing the runtime/Google error when PKCE is missing (e.g. after
  // a StrictMode remount or a second tab), rather than always saying "verified".
  if (callbackError) {
    const messages: Record<string, string> = {
      access_denied: "Google sign-in was cancelled.",
      invitation_required: "This app is invite-only. Ask an administrator to invite your verified Google email.",
      verified_google_email_required: "Google must provide a verified email address for this app.",
      membership_setup_failed: "Your account was verified, but its workspace could not be prepared. Please try again.",
      google_exchange_failed: "Google rejected the sign-in code exchange. Check GONVEX_GOOGLE_CLIENT_ID/SECRET and the broker callback URI.",
      invalid_google_identity: "Google identity verification failed. Please try again.",
      account_creation_failed: "Your Google account could not be linked. Please try again.",
      code_creation_failed: "Gonvex could not finish creating a sign-in code. Please try again.",
    };
    safeSessionStorageRemove(options.pkceStorageKey);
    clearAuthCallbackParams(url, pkce?.returnTo);
    throw new Error(messages[callbackError] ?? `Google sign-in failed (${callbackError}). Please try again.`);
  }
  if (!pkce || !returnedState || returnedState !== pkce.state || Date.now() - pkce.createdAt > 10 * 60 * 1000) {
    clearAuthCallbackParams(url, pkce?.returnTo);
    throw new Error("The Google sign-in response could not be verified. Please try again.");
  }
  // Consume PKCE only after validation so a concurrent remount still sees it.
  safeSessionStorageRemove(options.pkceStorageKey);
  clearAuthCallbackParams(url, pkce.returnTo);
  const session = await requestGonvexAuthToken(options.runtimeUrl, {
    grantType: "authorization_code", project: options.projectId, code,
    codeVerifier: pkce.verifier, redirectUri: pkce.redirectUri,
  });
  // Persist before returning so a StrictMode remount can recover the session
  // even if the first effect was cancelled before installSession.
  safeLocalStorageSet(options.storageKey, JSON.stringify(session));
  return session;
}

function sessionFromNativeGrant(value: JsonValue, previous: GonvexAuthSession): GonvexAuthSession {
  if (!value || typeof value !== "object" || Array.isArray(value) || !isGonvexAuthSession(value as Partial<GonvexAuthSession>)) {
    throw new GonvexAuthRequestError("Gonvex returned an invalid native session.", 502);
  }
  const session = value as GonvexAuthSession;
  const activeTenantId = session.tenants.some((tenant) => tenant.id === previous.activeTenantId)
    ? previous.activeTenantId
    : session.activeTenantId;
  return { ...session, activeTenantId };
}

async function requestGonvexAuthToken(runtimeUrl: string, body: Record<string, unknown>): Promise<GonvexAuthSession> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 15_000);
  try {
    const response = await fetch(`${runtimeUrl}/auth/token`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal: controller.signal,
    });
    const payload = await response.json().catch(() => ({})) as Partial<GonvexAuthSession> & { error?: string };
    if (!response.ok || !isGonvexAuthSession(payload)) {
      throw new GonvexAuthRequestError(payload.error ?? "Gonvex could not finish sign-in.", response.ok ? 502 : response.status);
    }
    return payload;
  } catch (cause) {
    if (controller.signal.aborted) throw new Error("Gonvex did not finish sign-in in time. Please try again.");
    throw cause;
  } finally {
    window.clearTimeout(timeout);
  }
}

function isFatalRefreshError(cause: unknown) {
  return cause instanceof GonvexAuthRequestError && (cause.status === 400 || cause.status === 401 || cause.status === 403);
}

function isGonvexAuthSession(value: Partial<GonvexAuthSession>): value is GonvexAuthSession {
  return Boolean(
    value.accessToken && value.expiresAt && value.refreshToken && value.refreshExpiresAt
    && value.account?.id && Array.isArray(value.tenants),
  );
}

async function withBrowserAuthLock<T>(name: string, action: () => Promise<T>): Promise<T> {
  const locks = typeof navigator === "undefined"
    ? undefined
    : (navigator as Navigator & { locks?: { request: <R>(name: string, callback: () => Promise<R>) => Promise<R> } }).locks;
  return locks ? locks.request(name, action) : withLocalStorageAuthLock(name, action);
}

async function withLocalStorageAuthLock<T>(name: string, action: () => Promise<T>): Promise<T> {
  const key = `${name}:lease`;
  const owner = randomBase64Url(16);
  const deadline = Date.now() + 25_000;
  while (Date.now() < deadline) {
    try {
      const current = JSON.parse(localStorage.getItem(key) ?? "null") as { owner?: string; expiresAt?: number } | null;
      if (!current?.owner || Number(current.expiresAt) <= Date.now()) {
        localStorage.setItem(key, JSON.stringify({ owner, expiresAt: Date.now() + 20_000 }));
        const claimed = JSON.parse(localStorage.getItem(key) ?? "null") as { owner?: string } | null;
        if (claimed?.owner === owner) {
          try {
            return await action();
          } finally {
            const latest = JSON.parse(localStorage.getItem(key) ?? "null") as { owner?: string } | null;
            if (latest?.owner === owner) localStorage.removeItem(key);
          }
        }
      }
    } catch {
      return action();
    }
    await new Promise((resolve) => setTimeout(resolve, 30 + Math.floor(Math.random() * 40)));
  }
  throw new Error("Another tab is refreshing this session. Please try again.");
}

function safeLocalStorageSet(key: string, value: string) {
  try { localStorage.setItem(key, value); } catch { /* storage can be unavailable in hardened browsers */ }
}

function safeLocalStorageRemove(key: string) {
  try { localStorage.removeItem(key); } catch { /* storage can be unavailable in hardened browsers */ }
}

function safeSessionStorageSet(key: string, value: string) {
  try { sessionStorage.setItem(key, value); } catch { /* reported by the missing-state check after redirect */ }
}

function safeSessionStorageRemove(key: string) {
  try { sessionStorage.removeItem(key); } catch { /* nothing else to clean */ }
}

function normalizeCallbackPath(value: string) {
  const path = value.trim();
  if (!path.startsWith("/") || path.startsWith("//") || path.includes("?") || path.includes("#")) {
    throw new Error("Gonvex auth callbackPath must be an absolute pathname");
  }
  return path;
}

function readAuthSession(key: string): GonvexAuthSession | null {
  if (typeof window === "undefined") return null;
  try {
    const parsed = JSON.parse(localStorage.getItem(key) ?? "null") as GonvexAuthSession | null;
    if (!parsed?.accessToken || !parsed.refreshToken || !parsed.account?.id || !Array.isArray(parsed.tenants) || parsed.refreshExpiresAt <= Date.now()) {
      safeLocalStorageRemove(key);
      return null;
    }
    return parsed;
  } catch {
    safeLocalStorageRemove(key);
    return null;
  }
}

function readPKCE(key: string): PKCEState | null {
  try {
    const parsed = JSON.parse(sessionStorage.getItem(key) ?? "null") as PKCEState | null;
    if (!parsed?.state || !parsed.verifier || !parsed.redirectUri || !parsed.returnTo || !parsed.createdAt) return null;
    return parsed;
  } catch {
    return null;
  }
}

function clearAuthCallbackParams(url: URL, returnTo?: string) {
  url.searchParams.delete("code");
  url.searchParams.delete("state");
  url.searchParams.delete("error");
  const safeReturnTo = returnTo?.startsWith("/") && !returnTo.startsWith("//") ? returnTo : url.toString();
  history.replaceState({}, "", safeReturnTo);
}

function randomBase64Url(byteLength: number) {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return bytesToBase64Url(bytes);
}

function bytesToBase64Url(bytes: Uint8Array) {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export type QueryStatus = "skip" | "loading" | "success" | "error" | "timeout";

export type UseQueryResultOptions = {
  /**
   * Soft "pending too long" signal for the live subscription. When no result
   * or error has arrived within this window the status becomes `timeout`
   * (the subscription stays alive; `retry()` re-requests). Default 15s.
   * `0` disables.
   */
  timeoutMs?: number;
  /**
   * Keep showing the last successful data (with `isStale: true`) while the
   * query is erroring, timing out, or disconnected. Default true.
   */
  keepPreviousData?: boolean;
};

export type UseQueryResult<T> = {
  data: T | undefined;
  status: QueryStatus;
  error: Error | null;
  isLoading: boolean;
  isError: boolean;
  isSuccess: boolean;
  /** True while showing last good data during an error/timeout/reconnect. */
  isStale: boolean;
  /** Re-request the query from the server (drops error/timeout state). */
  retry: () => void;
};

const DEFAULT_LIVE_QUERY_SLOW_MS = 15_000;

type QueryResultState<T> = {
  data: T | undefined;
  status: QueryStatus;
  error: Error | null;
  isStale: boolean;
};

/** One-shot Query hook with explicit loading/error/timeout status and retry. */
export function useQueryResult<T extends JsonValue = JsonValue>(
  ref: FunctionReference,
  args: JsonValue | "skip" = {},
  options: UseQueryResultOptions = {},
): UseQueryResult<T> {
  const client = useGonvexClient();
  const path = ref.path;
  const kind = ref.kind;
  const optimisticKey = JSON.stringify(ref.optimistic ?? null);
  const argsKey = JSON.stringify(args);
  const keepPreviousData = options.keepPreviousData !== false;
  const timeoutMs = options.timeoutMs ?? DEFAULT_LIVE_QUERY_SLOW_MS;
  const [requestGeneration, setRequestGeneration] = useState(0);
  const [state, setState] = useState<QueryResultState<T>>({
    data: undefined,
    status: args === "skip" ? "skip" : "loading",
    error: null,
    isStale: false,
  });
  const slowTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const clearSlowTimer = useCallback(() => {
    if (slowTimerRef.current !== null) {
      clearTimeout(slowTimerRef.current);
      slowTimerRef.current = null;
    }
  }, []);

  const startSlowTimer = useCallback(() => {
    clearSlowTimer();
    if (timeoutMs <= 0) return;
    slowTimerRef.current = setTimeout(() => {
      slowTimerRef.current = null;
      setState((previous) => {
        if (previous.status !== "loading") return previous;
        return { ...previous, status: "timeout", isStale: previous.data !== undefined };
      });
    }, timeoutMs);
  }, [clearSlowTimer, timeoutMs]);

  useEffect(() => {
    if (args === "skip") {
      setState({ data: undefined, status: "skip", error: null, isStale: false });
      return;
    }
    setState((previous) => ({
      data: keepPreviousData ? previous.data : undefined,
      status: "loading",
      error: null,
      isStale: keepPreviousData && previous.data !== undefined,
    }));
    startSlowTimer();

    let active = true;
    void client.query<T>(ref, args).then(
      (data) => {
        if (!active) return;
        clearSlowTimer();
        setState({ data, status: "success", error: null, isStale: false });
      },
      (failure) => {
        if (!active) return;
        clearSlowTimer();
        const error = failure instanceof Error
          ? failure
          : new GonvexClientError(String(failure), { code: "server", path, operation: "query" });
        setState((previous) => ({
          data: keepPreviousData ? previous.data : undefined,
          status: "error",
          error,
          isStale: keepPreviousData && previous.data !== undefined,
        }));
      },
    );
    return () => {
      active = false;
      clearSlowTimer();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, kind, path, optimisticKey, argsKey, keepPreviousData, requestGeneration, startSlowTimer, clearSlowTimer]);

  const retry = useCallback(() => {
    if (args === "skip") return;
    setRequestGeneration((generation) => generation + 1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [argsKey]);

  return {
    data: state.data,
    status: state.status,
    error: state.error,
    isLoading: state.status === "loading",
    isError: state.status === "error" || state.status === "timeout",
    isSuccess: state.status === "success",
    isStale: state.isStale,
    retry,
  };
}

export function useLiveQuery<T extends JsonValue = JsonValue>(ref: FunctionReference, args: JsonValue | "skip" = {}): T | undefined {
  const client = useGonvexClient();
  if (ref.delivery !== "live" || !ref.live?.plan) {
    throw new Error(`useLiveQuery requires a structured Live Query reference: ${ref.path}`);
  }
  const path = ref.path;
  const kind = ref.kind;
  const optimisticKey = JSON.stringify(ref.optimistic ?? null);
  const argsKey = JSON.stringify(args);
  const liveKey = JSON.stringify(ref.live ?? null);
  const liveWatch = useMemo(
    () => args === "skip" ? undefined : client.watchLiveQuery<T>(ref, args),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [client, kind, path, optimisticKey, argsKey, liveKey],
  );
  const liveResult = useSyncExternalStore(
    useCallback((notify) => liveWatch?.onUpdate(notify) ?? (() => undefined), [liveWatch]),
    useCallback(() => liveWatch?.localLiveQueryResult(), [liveWatch]),
    () => undefined,
  );

  return liveResult;
}

/** Read one normalized entity from the single Gonvex Local Replica. */
export function useEntity<T extends ReplicaRow = ReplicaRow>(entity: string, id: string): T | undefined {
  const client = useGonvexClient();
  useSyncExternalStore(
    useCallback((notify) => client.localReplica.subscribe(notify), [client]),
    useCallback(() => client.localReplica.version(), [client]),
    () => 0,
  );
  return client.localReplica.entity<T>(entity, id);
}

/** Resolve an ordered entity batch with one Local Replica subscription. */
export function useReplicaEntities<T extends ReplicaRow = ReplicaRow>(entity: string, ids: readonly string[]): Array<T | undefined> {
  const client = useGonvexClient();
  const idsKey = JSON.stringify(ids);
  const version = useSyncExternalStore(
    useCallback((notify) => client.localReplica.subscribe(notify), [client]),
    useCallback(() => client.localReplica.version(), [client]),
    () => 0,
  );
  return useMemo(() => client.replicaEntities<T>(entity, ids), [client, entity, idsKey, version]);
}

/** Read a persisted Live Query window without opening another server subscription. */
export function useRetainedLiveQuery<T extends ReplicaRow = ReplicaRow>(
  signatureOrReference: string | FunctionReference,
  args: JsonValue = {},
): LiveQueryResult<T> {
  const client = useGonvexClient();
  const argsKey = JSON.stringify(args);
  const signature = typeof signatureOrReference === "string"
    ? signatureOrReference
    : client.replicaSignature(signatureOrReference, args);
  const version = useSyncExternalStore(
    useCallback((notify) => client.localReplica.subscribe(notify), [client]),
    useCallback(() => client.localReplica.version(), [client]),
    () => 0,
  );
  return useMemo(() => client.retainedLiveQuery<T>(signature), [client, signature, argsKey, version]);
}

/** Structured Live Query state backed by normalized Local Replica entities. */
export function useLiveQueryState<T extends ReplicaRow = ReplicaRow>(
  ref: FunctionReference,
  args: JsonValue | "skip" = {},
): LiveQueryResult<T> {
  const client = useGonvexClient();
  const argsKey = JSON.stringify(args);
  const signature = args === "skip" ? "" : client.replicaSignature(ref, args);
  useEffect(() => {
    if (args === "skip") return;
    return client.subscribeLiveQuery(ref, args, () => undefined);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, ref.kind, ref.path, argsKey]);
  useSyncExternalStore(
    useCallback((notify) => client.localReplica.subscribe(notify), [client]),
    useCallback(() => client.localReplica.version(), [client]),
    () => 0,
  );
  if (args !== "skip" && client.localReplica.freshness() === "offline") {
    const offline = client.offlineLiveQuery<T>(ref, args);
    return {
      rows: offline.rows,
      ids: offline.rows.map((row) => String(row.id ?? row._id ?? "")).filter(Boolean),
      ...(offline.total === undefined ? {} : { total: offline.total }),
      ...(offline.offset === undefined ? {} : { offset: offline.offset }),
      ...(offline.limit === undefined ? {} : { limit: offline.limit }),
      source: "cache",
      completeness: offline.completeness,
      freshness: "offline",
      supported: offline.supported,
      ...(offline.unsupportedOperator ? { unsupportedOperator: offline.unsupportedOperator } : {}),
    };
  }
  return signature
    ? client.localReplica.liveQuery<T>(signature)
    : { rows: [], ids: [], source: "cache", completeness: "partial", freshness: client.localReplica.freshness() };
}

/** Execute a read-only Query once. Queries never subscribe or rerun. */
export function useQuery<T extends JsonValue = JsonValue>(ref: FunctionReference, args: JsonValue | "skip" = {}): T | undefined {
  const client = useGonvexClient();
  const [result, setResult] = useState<T>();
  const [error, setError] = useState<Error | null>(null);
  const argsKey = JSON.stringify(args);

  useEffect(() => {
    let active = true;
    if (args === "skip") {
      setResult(undefined);
      setError(null);
      return () => { active = false; };
    }
    setResult(undefined);
    setError(null);
    void client.query<T>(ref, args).then(
      (value) => { if (active) setResult(value); },
      (failure) => { if (active) setError(failure instanceof Error ? failure : new Error(String(failure))); },
    );
    return () => { active = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, ref.kind, ref.path, argsKey]);

  if (error) throw error;
  return result;
}

export function useReplicaCollection<T extends JsonValue = JsonValue>(
  ref: FunctionReference,
  args: JsonValue | "skip" = {},
): T[] | undefined {
  const client = useGonvexClient();
  const path = ref.path;
  const kind = ref.kind;
  const optimisticKey = JSON.stringify(ref.optimistic ?? null);
  const argsKey = JSON.stringify(args);
  const watch = useMemo(
    () => args === "skip" ? undefined : client.watchReplica<T>(ref, args),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [client, kind, path, optimisticKey, argsKey],
  );
  return useSyncExternalStore(
    useCallback((onStoreChange) => watch?.onUpdate(onStoreChange) ?? (() => undefined), [watch]),
    useCallback(() => watch?.localReplicaResult(), [watch]),
    () => undefined,
  );
}

/** Replica rows plus authoritative completeness, truncation, and freshness metadata. */
export function useReplicaCollectionState<T extends ReplicaRow = ReplicaRow>(
  ref: FunctionReference,
  args: JsonValue | "skip" = {},
): ReplicaCollectionState<T> | undefined {
  const client = useGonvexClient();
  const path = ref.path;
  const argsKey = JSON.stringify(args);
  const watch = useMemo(
    () => args === "skip" ? undefined : client.watchReplica<T>(ref, args),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [client, ref.kind, path, argsKey],
  );
  return useSyncExternalStore(
    useCallback((onStoreChange) => watch?.onUpdate(onStoreChange) ?? (() => undefined), [watch]),
    useCallback(() => watch?.localReplicaState() as ReplicaCollectionState<T> | undefined, [watch]),
    () => undefined,
  );
}

export function useReplicaSelector<T extends JsonValue = JsonValue, Selected = unknown>(
  ref: FunctionReference,
  args: JsonValue | "skip",
  selector: (rows: T[]) => Selected,
  isEqual: (left: Selected, right: Selected) => boolean = Object.is,
): Selected | undefined {
  const client = useGonvexClient();
  const path = ref.path;
  const kind = ref.kind;
  const optimisticKey = JSON.stringify(ref.optimistic ?? null);
  const argsKey = JSON.stringify(args);
  const selectorRef = useRef(selector);
  const equalityRef = useRef(isEqual);
  selectorRef.current = selector;
  equalityRef.current = isEqual;
  const watch = useMemo(
    () => args === "skip" ? undefined : client.watchReplica<T>(ref, args),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [client, kind, path, optimisticKey, argsKey],
  );
  const selectedRef = useRef<{ initialized: boolean; value: Selected | undefined }>({
    initialized: false,
    value: undefined,
  });
  useEffect(() => {
    selectedRef.current = { initialized: false, value: undefined };
  }, [watch]);
  const getSnapshot = useCallback(() => {
    const rows = watch?.localReplicaResult();
    const next = rows === undefined ? undefined : selectorRef.current(rows);
    if (
      !selectedRef.current.initialized
      || next === undefined
      || selectedRef.current.value === undefined
      || !equalityRef.current(selectedRef.current.value, next)
    ) {
      selectedRef.current = { initialized: true, value: next };
    }
    return selectedRef.current.value;
  }, [watch]);
  const subscribe = useCallback((onStoreChange: () => void) => {
    if (!watch) return () => undefined;
    return watch.onUpdate(() => {
      const previous = selectedRef.current.value;
      const rows = watch.localReplicaResult();
      const next = rows === undefined ? undefined : selectorRef.current(rows);
      if (
        selectedRef.current.initialized
        && previous !== undefined
        && next !== undefined
        && equalityRef.current(previous, next)
      ) return;
      selectedRef.current = { initialized: true, value: next };
      onStoreChange();
    });
  }, [watch]);
  return useSyncExternalStore(subscribe, getSnapshot, () => undefined);
}

export type UseReducerOptions = {
  /** Per-call timeout override forwarded to the client. `0` disables. */
  timeoutMs?: number;
};

export function useReducer(ref: FunctionReference, options: UseReducerOptions = {}) {
  const client = useGonvexClient();
  return (args: JsonValue = {}) => client.reducer(ref, args, options);
}

export function useAction(ref: FunctionReference, options: UseReducerOptions = {}) {
  const client = useGonvexClient();
  return (args: JsonValue = {}) => client.action(ref, args, options);
}

const FALLBACK_CONNECTION_STATE: ConnectionState = {
  isWebSocketConnected: false,
  hasEverConnected: false,
  connectionCount: 0,
  connectionRetries: 0,
  hasInflightRequests: false,
  inflightReducers: 0,
  inflightActions: 0,
  inflightOneShotQueries: 0,
};

export function useGonvexConnectionState(): ConnectionState {
  const client = useGonvexClient();
  const [state, setState] = useState<ConnectionState>(() => (
    typeof client.connectionState === "function" ? client.connectionState() : FALLBACK_CONNECTION_STATE
  ));

  useEffect(() => {
    if (typeof client.subscribeToConnectionState !== "function") return;
    setState(client.connectionState());
    return client.subscribeToConnectionState(setState);
  }, [client]);

  return state;
}

export function useGonvexClient() {
  const client = useContext(GonvexContext);
  if (!client) throw new Error("GonvexProvider is required");
  return client;
}

function isRecord(value: JsonValue): value is { [key: string]: JsonValue } {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export { useSignalValue } from "./useSignal.js";
