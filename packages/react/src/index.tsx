import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, useSyncExternalStore, type ButtonHTMLAttributes, type ReactNode } from "react";
import { GonvexClient, GonvexClientError, control, type ConnectionState, type ControlImpersonation, type ControlInvitationAcceptance, type ControlInvitationListItem, type ControlTenant, type ControlToken, type FunctionReference, type GonvexExternalAuthAdapter, type LiveQueryResult, type ReplicaCollectionSubscriptionState, type ReplicaRow } from "@gonvex/client";
import type { JsonValue } from "@gonvex/protocol";

export { GonvexClientError, type ConnectionState } from "@gonvex/client";
export { createFirebaseAuthAdapter, type GonvexExternalAuthAdapter, type GonvexExternalIdentityHint, type GonvexFirebaseAuthAdapterOptions } from "@gonvex/client";

const GonvexContext = createContext<GonvexClient | null>(null);
const GonvexAuthContext = createContext<AuthState>({ isLoading: false, isAuthenticated: true });

export function GonvexProvider(props: { client: GonvexClient; children: ReactNode }) {
  return <GonvexContext.Provider value={props.client}>{props.children}</GonvexContext.Provider>;
}

export type AuthState = {
  isLoading: boolean;
  isAuthenticated: boolean;
  /** Freshness of the canonical Gonvex session during provider rotation. */
  sessionState?: "loading" | "current" | "reconnecting" | "degraded" | "signedOut";
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
  provider: "password" | "google" | "microsoft" | "apple" | string;
};

export type GonvexAuthTenant = {
  id: string;
  name: string;
  role: "owner" | "admin" | "member" | "viewer" | string;
  permissions?: Record<string, unknown>;
  domain: string;
  timezone: string;
  description: string;
  profile: JsonValue;
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

export type GonvexAuthProviderName = "google" | "microsoft" | "apple";
export type GonvexDeveloperModeState = {
  active: boolean;
  tenantId?: string;
  grantId?: string;
  expiresAt?: string;
};
type PKCEState = { state: string; verifier: string; redirectUri: string; returnTo: string; provider: GonvexAuthProviderName; createdAt: number };

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
  signIn: (provider?: GonvexAuthProviderName) => Promise<void>;
  signInWithProvider: (provider: GonvexAuthProviderName) => Promise<void>;
  signInWithPassword: (email: string, password: string) => Promise<void>;
  signOut: (options?: { allDevices?: boolean }) => Promise<void>;
  setActiveTenant: (tenantId: string) => Promise<void>;
  refreshMemberships: () => Promise<GonvexAuthTenant[]>;
  createTenant: (name: string, options?: { domain?: string }) => Promise<GonvexAuthTenant>;
  /** Copies the canonical session into a trusted sibling origin before navigation. */
  handoffSessionTo: (targetUrl: string) => Promise<void>;
  inviteMember: (tenantId: string, email: string, options?: { role?: GonvexAuthTenant["role"]; permissions?: Record<string, unknown>; teamIds?: string[]; allowedAuthProviders?: string[]; payload?: JsonValue }) => Promise<ControlToken>;
  acceptInvitation: (token: string) => Promise<ControlInvitationAcceptance>;
  revokeInvitation: (tenantId: string, email: string) => Promise<void>;
  developerMode: GonvexDeveloperModeState;
  enterDeveloperMode: (tenantId: string) => Promise<void>;
  exitDeveloperMode: () => Promise<void>;
};

export type GonvexAuthConfig = {
  runtimeUrl: string;
  projectId: string;
  callbackPath?: string;
  /**
   * Tenant routing scope for the first authentication exchange. Pass `null`
   * to explicitly select account/landlord scope; omit it to restore the
   * persisted active tenant when possible.
   */
  initialTenantId?: string | null;
  /** Visible content while the external identity provider initializes. */
  loadingFallback?: ReactNode;
  /** Trusted identity adapter such as createFirebaseAuthAdapter(). */
  externalAuth?: GonvexExternalAuthAdapter;
  /** Secure browser-to-browser handoff for apps routed across sibling subdomains. */
  crossOriginHandoff?: {
    allowedOriginSuffix: string;
    receiverPath?: string;
    /**
     * Load the receiver with POST when a service worker may serve a stale
     * cached application shell for GET navigations. The POST body is empty;
     * the canonical session still travels only through the origin-checked
     * postMessage exchange.
     */
    receiverMethod?: "get" | "post";
    timeoutMs?: number;
  };
};

const SESSION_HANDOFF_READY = "gonvex.sessionHandoff.ready";
const SESSION_HANDOFF_OFFER = "gonvex.sessionHandoff.offer";
const SESSION_HANDOFF_ACCEPTED = "gonvex.sessionHandoff.accepted";

function normalizedOriginSuffix(value: string): string {
  return value.trim().toLowerCase().replace(/^\.+/, "").replace(/\.+$/, "");
}

function originMatchesSuffix(origin: string, suffix: string): boolean {
  try {
    const hostname = new URL(origin).hostname.toLowerCase();
    const normalized = normalizedOriginSuffix(suffix);
    return Boolean(normalized) && (hostname === normalized || hostname.endsWith(`.${normalized}`));
  } catch {
    return false;
  }
}

function handoffNonce(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(24));
  return bytesToBase64Url(bytes);
}

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

/** Native Gonvex authentication with password or a configured OAuth provider. */
// Dedupe callback bootstrap across React StrictMode remounts so the OAuth
// code+PKCE exchange runs once. Without this, the first effect's finally
// clears sessionStorage PKCE before the remount can finish verification.
const authBootstrapPromises = new Map<string, Promise<GonvexAuthSession | null>>();

export function GonvexAuthProvider(props: GonvexAuthConfig & { client: GonvexClient; children: ReactNode }) {
  const runtimeUrl = props.runtimeUrl.replace(/\/+$/, "");
  const hasExplicitInitialTenant = props.initialTenantId !== undefined;
  const initialTenantId = props.initialTenantId?.trim() || undefined;
  const callbackPath = normalizeCallbackPath(props.callbackPath ?? "/");
  const storageKey = `gonvex-auth:${encodeURIComponent(runtimeUrl)}:${props.projectId}`;
  const pkceStorageKey = `${storageKey}:pkce`;
  const initialAuthRef = useRef<{
    session: GonvexAuthSession | null;
    warmSession: GonvexAuthSession | null;
  } | null>(null);
  if (!initialAuthRef.current) {
    const persisted = readAuthSession(storageKey);
    initialAuthRef.current = {
      session: persisted,
      warmSession: props.externalAuth
        ? reusableExternalAuthSession(
            persisted,
            initialTenantId,
            props.externalAuth.provider,
            hasExplicitInitialTenant,
          )
        : null,
    };
  }
  const initialAuth = initialAuthRef.current;
  // The session exposed to React must be the same scoped session installed on
  // the client. On an explicit landlord origin, warmSession intentionally
  // clears a persisted tenant selection. Keeping the unscoped persisted value
  // here makes the tenant-directory effect see a different auth scope and skip
  // its authoritative Control Query, leaving deleted tenants in the UI.
  const initialSession = initialAuth.warmSession ?? initialAuth.session;
  const [session, setSession] = useState<GonvexAuthSession | null>(initialSession);
  const [isLoading, setIsLoading] = useState(!initialAuth.warmSession);
  const [sessionState, setSessionState] = useState<AuthState["sessionState"]>(
    initialAuth.warmSession ? "reconnecting" : "loading",
  );
  const [error, setError] = useState<string | null>(null);
  const [refreshRetryAt, setRefreshRetryAt] = useState(0);
  const [canonicalRefreshRequested, setCanonicalRefreshRequested] = useState(false);
  const [developerMode, setDeveloperMode] = useState<GonvexDeveloperModeState>({ active: false });
  const sessionRef = useRef(session);
  const refreshRef = useRef<Promise<GonvexAuthSession | null> | null>(null);
  const developerModeRef = useRef<(GonvexDeveloperModeState & { active: true; originalTenantId?: string }) | null>(null);
  // A persisted session is only a candidate until bootstrap/external auth has
  // installed its complete project + tenant + account scope on the client.
  // Never let the token-refresh effect authenticate an account-only socket in
  // that gap: Replica Collections require an accepted tenant scope.
  const installedClientScopeRef = useRef<string | null>(
    initialAuth.warmSession
      ? authSessionScope(props.projectId, initialAuth.warmSession)
      : null,
  );
  const installedWarmSessionRef = useRef(false);
  if (initialAuth.warmSession && !installedWarmSessionRef.current) {
    installedWarmSessionRef.current = true;
    installClientSession(props.client, props.projectId, initialAuth.warmSession);
  }

  const installSession = useCallback((
    next: GonvexAuthSession | null,
    persist = true,
    installClientAuth = true,
  ) => {
    sessionRef.current = next;
    if (next) {
      if (persist) safeLocalStorageSet(storageKey, JSON.stringify(next));
      if (!developerModeRef.current && installClientAuth) {
        installClientSession(props.client, props.projectId, next);
      }
      if (!developerModeRef.current) {
        installedClientScopeRef.current = authSessionScope(props.projectId, next);
      }
    } else {
      if (persist) safeLocalStorageRemove(storageKey);
      if (!developerModeRef.current && installClientAuth) {
        props.client.setAuth({ project: props.projectId, tenant: undefined, token: undefined, identity: undefined });
      }
      if (!developerModeRef.current) installedClientScopeRef.current = null;
    }
    setSession(next);
  }, [props.client, props.projectId, storageKey]);

  useEffect(() => {
    const handoff = props.crossOriginHandoff;
    if (!handoff || window.parent === window) return;
    const hash = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    const nonce = hash.get("gonvexSessionHandoff");
    if (!nonce) return;
    let parentOrigin = "";
    try {
      parentOrigin = new URL(document.referrer).origin;
    } catch {
      return;
    }
    if (!originMatchesSuffix(parentOrigin, handoff.allowedOriginSuffix)) return;

    const onMessage = (event: MessageEvent) => {
      if (event.source !== window.parent || event.origin !== parentOrigin) return;
      const message = event.data as {
        type?: string;
        nonce?: string;
        projectId?: string;
        runtimeUrl?: string;
        targetOrigin?: string;
        session?: GonvexAuthSession;
      };
      if (
        message.type !== SESSION_HANDOFF_OFFER
        || message.nonce !== nonce
        || message.projectId !== props.projectId
        || message.runtimeUrl !== runtimeUrl
        || message.targetOrigin !== window.location.origin
      ) return;
      const candidate = message.session ?? null;
      const reusable = reusableExternalAuthSession(
        candidate,
        initialTenantId,
        props.externalAuth?.provider ?? candidate?.account.provider ?? "",
        hasExplicitInitialTenant,
      );
      if (!reusable) return;
      installSession(reusable);
      setSessionState("current");
      setIsLoading(false);
      window.parent.postMessage({ type: SESSION_HANDOFF_ACCEPTED, nonce }, parentOrigin);
    };
    window.addEventListener("message", onMessage);
    window.parent.postMessage({ type: SESSION_HANDOFF_READY, nonce }, parentOrigin);
    return () => window.removeEventListener("message", onMessage);
  }, [hasExplicitInitialTenant, initialTenantId, installSession, props.crossOriginHandoff, props.externalAuth?.provider, props.projectId, runtimeUrl]);

  const restoreAccountSession = useCallback(() => {
    const developer = developerModeRef.current;
    if (!developer) return;
    developerModeRef.current = null;
    setDeveloperMode({ active: false });
    const current = sessionRef.current;
    const activeTenantId = current?.tenants.some((tenant) => tenant.id === developer.originalTenantId)
      ? developer.originalTenantId
      : current?.activeTenantId;
    installSession(current ? { ...current, activeTenantId } : null);
  }, [installSession]);

  useEffect(() => props.client.onAuthError((message) => {
    if (!developerModeRef.current) return;
    restoreAccountSession();
    setError(message || "Developer mode ended because its authorization is no longer valid.");
  }), [props.client, restoreAccountSession]);

  useEffect(() => {
    if (!developerMode.active || !developerMode.expiresAt) return;
    const delay = Math.max(0, Date.parse(developerMode.expiresAt) - Date.now());
    const timeout = window.setTimeout(() => restoreAccountSession(), delay);
    return () => window.clearTimeout(timeout);
  }, [developerMode.active, developerMode.expiresAt, restoreAccountSession]);

  useEffect(() => {
    if (props.externalAuth) return;
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
      if (!cancelled) {
        installSession(next);
        setSessionState(next ? "current" : "signedOut");
      }
    }).catch((cause) => {
      if (!cancelled) {
        installSession(null);
        setSessionState("signedOut");
        setError(cause instanceof Error ? cause.message : "Sign-in failed.");
      }
    }).finally(() => {
      if (!cancelled) setIsLoading(false);
    });
    return () => { cancelled = true; };
  }, [callbackPath, installSession, pkceStorageKey, props.externalAuth, props.projectId, runtimeUrl, storageKey]);

  useEffect(() => {
    const adapter = props.externalAuth;
    if (!adapter) return;
    let cancelled = false;
    let generation = 0;
    // A Firebase ID token is never persisted. The canonical Gonvex session is
    // installed only after the trusted host verifies it and resolves Account.
    const unsubscribe = adapter.onIdTokenChanged((identity) => {
      const currentGeneration = ++generation;
      if (!identity) {
        // Gonvex owns the canonical application session after the external
        // provider proves identity. Firebase can report null transiently while
        // a new tab hydrates its IndexedDB state; that is not an application
        // sign-out command and must not erase the shared Gonvex session.
        // Explicit signOut() clears both systems, and its localStorage removal
        // propagates to every tab through the storage listener below.
        const canonical = readAuthSession(storageKey) ?? sessionRef.current;
        const reusableCanonical = reusableExternalAuthSession(
          canonical,
          initialTenantId,
          adapter.provider,
          hasExplicitInitialTenant,
        );
        if (reusableCanonical) {
          setCanonicalRefreshRequested(false);
          if (!sameInstalledAuthSession(sessionRef.current, reusableCanonical)) {
            installSession(reusableCanonical, false);
          }
          setSessionState("current");
          setIsLoading(false);
        } else if (canonical) {
          // Firebase persistence is origin-scoped, while the canonical Gonvex
          // session is deliberately handed across tenant subdomains. Refresh
          // that canonical session directly instead of waiting forever for a
          // Firebase user that may never exist on this sibling origin.
          sessionRef.current = canonical;
          setSession(canonical);
          setSessionState("loading");
          setIsLoading(true);
          setCanonicalRefreshRequested(true);
        } else {
          setCanonicalRefreshRequested(false);
          installSession(null, false);
          setSessionState("signedOut");
          setIsLoading(false);
        }
        setError(null);
        return;
      }
      setCanonicalRefreshRequested(false);
      // A valid canonical Gonvex session remains usable while Firebase rotates
      // its identity token. Keep the application mounted and perform that
      // exchange in the background; unmounting the entire tree here destroys
      // open dialogs and other local UI state in every existing tab.
      const canonicalSession = readAuthSession(storageKey) ?? sessionRef.current;
      const reusableCanonicalSession = reusableExternalAuthSession(
        canonicalSession,
        initialTenantId,
        adapter.provider,
        hasExplicitInitialTenant,
      );
      if (reusableCanonicalSession) {
        // A reload already has a valid canonical Gonvex session. Install its
        // complete tenant scope before releasing application children; the
        // Firebase exchange below rotates it in the background. Otherwise the
        // new document can issue private Control Queries on the constructor's
        // unauthenticated socket while this exchange is still running.
        installSession(reusableCanonicalSession, false);
        setSessionState("reconnecting");
      } else {
        setSessionState("loading");
      }
      setIsLoading(!reusableCanonicalSession);
      let attemptedRefreshToken = "";
      void withBrowserAuthLock(`${storageKey}:external-session`, async () => {
        const token = await adapter.getIdToken(false);
        if (!token) throw new Error("The external identity provider did not return an ID token.");
        // Another tab can rotate the canonical Gonvex session while this tab
        // is waiting for the cross-tab exchange lock. Read the shared winner
        // inside the lock instead of replaying this tab's stale in-memory
        // refresh token.
        const storedCurrent = readAuthSession(storageKey) ?? sessionRef.current;
        const current = scopeSessionForInitialTenant(
          storedCurrent,
          initialTenantId,
          hasExplicitInitialTenant,
        ) ?? storedCurrent;
        attemptedRefreshToken = current?.refreshToken ?? "";
        if (!current) {
          props.client.setAuth({
            project: props.projectId, tenant: undefined, token: undefined,
            identity: { sub: identity.uid, iss: identity.issuer ?? adapter.provider },
          });
        }
        const tenantId = hasExplicitInitialTenant
          ? initialTenantId
          : current?.activeTenantId;
        const grant = await props.client.action(control.auth.exchangeExternalToken, {
          provider: adapter.provider,
          token,
          ...(tenantId ? { tenantId } : {}),
          ...(attemptedRefreshToken ? { previousRefreshToken: attemptedRefreshToken } : {}),
        });
        return sessionFromNativeGrant(grant, current ?? undefined);
      }).then((next) => {
        if (cancelled || currentGeneration !== generation) return;
        // A user can select a different tenant while the provider exchange is
        // still in flight. The returned access token is account-wide, but its
        // activeTenantId reflects the scope captured when the exchange began.
        // Preserve the latest locally accepted tenant selection so a stale
        // background exchange cannot bounce a cross-origin handoff back to the
        // previously active tenant.
        installSession(preserveActiveTenant(next, sessionRef.current ?? undefined));
        setSessionState("current");
        setError(null);
      }).catch((cause) => {
        if (cancelled || currentGeneration !== generation) return;
        const latest = readAuthSession(storageKey);
        if (latest && attemptedRefreshToken && latest.refreshToken !== attemptedRefreshToken) {
          // A competing tab completed the single-use rotation first. Its
          // persisted session is authoritative; a replay rejection in this
          // tab must not clear authentication for every open tab.
          installSession(latest);
          setSessionState("current");
          setError(null);
          return;
        }
        const canonical = latest ?? sessionRef.current ?? canonicalSession;
        if (canonical && !isFatalExternalExchangeError(cause)) {
          // The already-issued Gonvex session remains authoritative. A host,
          // database, or transport failure while rotating the provider token
          // must not turn a healthy signed-in application into a sign-in page.
          installSession(canonical, false);
          setSessionState("degraded");
          setError(cause instanceof Error ? cause.message : "External session rotation failed.");
          return;
        }
        installSession(null);
        setSessionState("signedOut");
        setError(cause instanceof Error ? cause.message : "External sign-in failed.");
      }).finally(() => {
        if (!cancelled && currentGeneration === generation) setIsLoading(false);
      });
    });
    return () => {
      cancelled = true;
      generation += 1;
      unsubscribe();
    };
  }, [hasExplicitInitialTenant, initialTenantId, installSession, props.client, props.externalAuth, props.projectId, storageKey]);

  const refreshSession = useCallback(async (force = false) => {
    if (refreshRef.current) return refreshRef.current;
    let attemptedRefreshToken = "";
    const lockName = props.externalAuth
      ? `${storageKey}:external-session`
      : `${storageKey}:refresh`;
    const request = withBrowserAuthLock(lockName, async () => {
      const storedCurrent = readAuthSession(storageKey) ?? sessionRef.current;
      const current = scopeSessionForInitialTenant(
        storedCurrent,
        initialTenantId,
        hasExplicitInitialTenant,
      ) ?? storedCurrent;
      if (!current) return null;
      if (!force && current.expiresAt > Date.now() + 60_000) return current;
      if (current.refreshExpiresAt <= Date.now()) return null;
      if (props.externalAuth) {
        if (current.account.provider !== props.externalAuth.provider) return null;
        attemptedRefreshToken = current.refreshToken;
        const token = await props.externalAuth.getIdToken(force);
        const grant = token
          ? await props.client.action(control.auth.exchangeExternalToken, {
              provider: props.externalAuth.provider,
              token,
              ...(current.activeTenantId ? { tenantId: current.activeTenantId } : {}),
              previousRefreshToken: current.refreshToken,
            })
          : await props.client.action(control.auth.refreshSession, {
              refreshToken: current.refreshToken,
            });
        const refreshed = sessionFromNativeGrant(grant, current);
        const next = scopeSessionForInitialTenant(
          refreshed,
          initialTenantId,
          hasExplicitInitialTenant,
        );
        if (!next) return null;
        safeLocalStorageSet(storageKey, JSON.stringify(next));
        return next;
      }
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
        setSessionState("current");
      } else {
        setSessionState("signedOut");
      }
      installSession(next);
      setIsLoading(false);
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
        setSessionState("signedOut");
        setIsLoading(false);
        setError(cause instanceof Error ? cause.message : "Your session expired. Please sign in again.");
        return null;
      }
      // Network failures, timeouts, rate limits, and server outages must not
      // destroy a valid refresh credential. Keep it and retry shortly.
      if (sessionRef.current) setRefreshRetryAt(Date.now() + 5_000);
      setSessionState("degraded");
      setIsLoading(false);
      setError("Gonvex could not refresh your session. Retrying shortly…");
      return null;
    }).finally(() => {
      refreshRef.current = null;
    });
    refreshRef.current = request;
    return request;
  }, [hasExplicitInitialTenant, initialTenantId, installSession, props.client, props.externalAuth, storageKey]);

  useEffect(() => {
    if (!canonicalRefreshRequested) return;
    let cancelled = false;
    void refreshSession().finally(() => {
      if (!cancelled) setCanonicalRefreshRequested(false);
    });
    return () => {
      cancelled = true;
    };
  }, [canonicalRefreshRequested, refreshSession]);

  useEffect(() => {
    if (!session) return;
    const scheduledAt = Math.max(session.expiresAt - 60_000, refreshRetryAt);
    const delay = Math.max(0, scheduledAt - Date.now());
    const timeout = window.setTimeout(() => { void refreshSession(); }, delay);
    return () => window.clearTimeout(timeout);
  }, [refreshRetryAt, refreshSession, session]);

  // Keep the account tenant directory authoritative without a reducer+manual
  // refetch pair. The live Control Plane Query resumes on reconnect.
  useEffect(() => {
    const currentSession = sessionRef.current;
    if (!currentSession) return;
    const sessionScope = `${props.projectId}\u0000${currentSession.activeTenantId ?? ""}\u0000${currentSession.account.id}`;
    // A localStorage session has not been authenticated merely because it was
    // parsed. Starting a private Control Query before the complete scope is
    // installed can race the provider bootstrap and receive a legitimate
    // "authentication is required" response from the control plane.
    if (installedClientScopeRef.current !== sessionScope) return;
    const watch = props.client.watchControlQuery<ControlTenant[]>(control.tenants.mine, {});
    return watch.onUpdate(() => {
      const tenants = watch.getSnapshot().result as GonvexAuthTenant[] | undefined;
      const current = sessionRef.current;
      if (!current || !tenants) return;
      // A live directory update is not a tenant-selection intent. In
      // particular, tenants.create refreshes this query before its reducer
      // result/control watermark. Auto-selecting the first newly visible
      // tenant here changes client auth scope and cancels the still-pending
      // create call. Preserve landlord scope until createTenant or
      // setActiveTenant explicitly selects the tenant. An already-selected
      // tenant may still fall back when its membership is revoked.
      const activeTenantId = current.activeTenantId === undefined
        ? undefined
        : tenants.some((tenant) => tenant.id === current.activeTenantId)
          ? current.activeTenantId
          : tenants[0]?.id;
      if (JSON.stringify(current.tenants) === JSON.stringify(tenants) && current.activeTenantId === activeTenantId) return;
      installSession({ ...current, tenants, activeTenantId });
    });
  }, [installSession, props.client, props.projectId, session]);

  useEffect(() => {
    const onStorage = (event: StorageEvent) => {
      if (event.key !== storageKey) return;
      installSession(readAuthSession(storageKey), false);
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, [installSession, storageKey]);

  const signInWithProvider = useCallback(async (provider: GonvexAuthProviderName) => {
    if (props.externalAuth) throw new Error("Provider sign-in is owned by the configured external authentication adapter.");
    setError(null);
    const verifier = randomBase64Url(64);
    const state = randomBase64Url(32);
    const challengeBytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
    const challenge = bytesToBase64Url(new Uint8Array(challengeBytes));
    const redirectUri = new URL(callbackPath, window.location.origin).toString();
    const returnTo = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    safeSessionStorageSet(pkceStorageKey, JSON.stringify({ state, verifier, redirectUri, returnTo, provider, createdAt: Date.now() }));
    const authorizeUrl = new URL(`${runtimeUrl}/auth/${provider}/authorize`);
    authorizeUrl.searchParams.set("project", props.projectId);
    authorizeUrl.searchParams.set("redirect_uri", redirectUri);
    authorizeUrl.searchParams.set("state", state);
    authorizeUrl.searchParams.set("code_challenge", challenge);
    authorizeUrl.searchParams.set("code_challenge_method", "S256");
    window.location.assign(authorizeUrl.toString());
  }, [callbackPath, pkceStorageKey, props.externalAuth, props.projectId, runtimeUrl]);

  const signIn = useCallback((provider: GonvexAuthProviderName = "google") => signInWithProvider(provider), [signInWithProvider]);

  const signInWithPassword = useCallback(async (email: string, password: string) => {
    if (props.externalAuth) throw new Error("Password sign-in is owned by the configured external authentication adapter.");
    setError(null);
    const grant = await props.client.action(control.auth.passwordLogin, { email, password });
    const next = sessionFromNativeGrant(grant, sessionRef.current ?? undefined);
    installSession(next);
  }, [installSession, props.client, props.externalAuth]);

  const signOut = useCallback(async (options?: { allDevices?: boolean }) => {
    const current = sessionRef.current;
    setError(null);
    // Start remote revocation while the authenticated socket is still
    // available, but never leave the browser signed in while that request is
    // queued behind other realtime work. Local and external-provider logout
    // are the user-visible security boundary.
    const revokeSession = current
      ? props.client.reducer(control.auth.logout, { refreshToken: current.refreshToken, all: options?.allDevices === true }).catch(() => undefined)
      : Promise.resolve();
    installSession(null);
    const signOutExternal = props.externalAuth?.signOut?.() ?? Promise.resolve();
    await Promise.all([revokeSession, signOutExternal]);
  }, [installSession, props.client, props.externalAuth]);

  const fetchAccessToken = useCallback(async (args: { forceRefreshToken: boolean }) => {
    const current = sessionRef.current;
    if (!current) return null;
    if (!args.forceRefreshToken && current.expiresAt > Date.now() + 60_000) return current.accessToken;
    return (await refreshSession(args.forceRefreshToken))?.accessToken ?? null;
  }, [refreshSession]);

  useEffect(() => {
    if (!session || developerModeRef.current) return;
    const sessionScope = `${props.projectId}\u0000${session.activeTenantId ?? ""}\u0000${session.account.id}`;
    if (installedClientScopeRef.current !== sessionScope) return;
    // installSession() and setActiveTenant() exclusively own the authenticated
    // project/tenant scope. This effect only attaches the refresh callback.
    // Re-sending the rendered session's tenant here can race an awaited tenant
    // switch and replace it with the previous scope.
    props.client.setAuth({
      token: session.accessToken,
      fetchToken: fetchAccessToken,
    });
  }, [developerMode.active, fetchAccessToken, props.client, props.projectId, session]);

  const setActiveTenant = useCallback(async (tenantId: string) => {
    if (developerModeRef.current) throw new Error("Exit developer mode before switching tenants.");
    const current = sessionRef.current;
    if (!current || !current.tenants.some((tenant) => tenant.id === tenantId)) {
      throw new Error(`Your account does not have access to tenant ${tenantId}.`);
    }
    const next = { ...current, activeTenantId: tenantId };
    await props.client.authenticate({
      project: props.projectId,
      tenant: tenantId,
      token: current.accessToken,
      fetchToken: fetchAccessToken,
      identity: { sub: current.account.id, iss: props.projectId },
    });
    // Publish the new tenant only after the runtime accepted it and the client
    // activated its authoritative Local Replica scope. This keeps React hooks
    // from observing a tenant that is not usable yet.
    installSession(next, true, false);
  }, [fetchAccessToken, installSession, props.client, props.projectId]);

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

  const createTenant = useCallback(async (name: string, options?: { domain?: string }) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before creating a tenant.");
    const tenant = await props.client.reducer(control.tenants.create, {
      name,
      ...(options?.domain ? { domain: options.domain } : {}),
    }) as GonvexAuthTenant;
    const current = sessionRef.current!;
    installSession({ ...current, tenants: [...current.tenants.filter((item) => item.id !== tenant.id), tenant], activeTenantId: tenant.id });
    return tenant;
  }, [fetchAccessToken, installSession, props.client]);

  const handoffSessionTo = useCallback(async (targetUrl: string) => {
    const handoff = props.crossOriginHandoff;
    const current = sessionRef.current;
    if (!handoff || !current) {
      throw new Error("Cross-origin session handoff is not configured for this authenticated session.");
    }
    const target = new URL(targetUrl, window.location.href);
    if (
      target.protocol !== window.location.protocol
      || !originMatchesSuffix(window.location.origin, handoff.allowedOriginSuffix)
      || !originMatchesSuffix(target.origin, handoff.allowedOriginSuffix)
    ) {
      throw new Error("Cross-origin session handoff target is outside the configured origin suffix.");
    }
    const nonce = handoffNonce();
    const receiver = new URL(handoff.receiverPath ?? "/auth/session-handoff", target.origin);
    receiver.hash = new URLSearchParams({ gonvexSessionHandoff: nonce }).toString();
    const frame = document.createElement("iframe");
    frame.hidden = true;
    frame.setAttribute("aria-hidden", "true");
    const receiverMethod = handoff.receiverMethod ?? "get";
    let receiverForm: HTMLFormElement | null = null;
    if (receiverMethod === "get") {
      frame.src = receiver.toString();
    } else {
      const frameName = `gonvex-session-handoff-${nonce}`;
      frame.name = frameName;
      receiverForm = document.createElement("form");
      receiverForm.hidden = true;
      receiverForm.method = "post";
      receiverForm.action = receiver.toString();
      receiverForm.target = frameName;
    }

    await new Promise<void>((resolve, reject) => {
      const timeout = window.setTimeout(() => {
        cleanup();
        reject(new Error("Cross-origin session handoff timed out before the destination accepted it."));
      }, Math.max(1_000, handoff.timeoutMs ?? 10_000));
      const cleanup = () => {
        window.clearTimeout(timeout);
        window.removeEventListener("message", onMessage);
        receiverForm?.remove();
        frame.remove();
      };
      const onMessage = (event: MessageEvent) => {
        if (event.source !== frame.contentWindow || event.origin !== target.origin) return;
        const message = event.data as { type?: string; nonce?: string };
        if (message.nonce !== nonce) return;
        if (message.type === SESSION_HANDOFF_READY) {
          frame.contentWindow?.postMessage({
            type: SESSION_HANDOFF_OFFER,
            nonce,
            projectId: props.projectId,
            runtimeUrl,
            targetOrigin: target.origin,
            session: current,
          }, target.origin);
        } else if (message.type === SESSION_HANDOFF_ACCEPTED) {
          cleanup();
          resolve();
        }
      };
      window.addEventListener("message", onMessage);
      document.body.append(frame);
      if (receiverForm) {
        document.body.append(receiverForm);
        receiverForm.submit();
      }
    });
  }, [props.crossOriginHandoff, props.projectId, runtimeUrl]);

  const inviteMember = useCallback(async (tenantId: string, email: string, options?: { role?: GonvexAuthTenant["role"]; permissions?: Record<string, unknown>; teamIds?: string[]; allowedAuthProviders?: string[]; payload?: JsonValue }) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before inviting a member.");
    if (tenantId !== sessionRef.current?.activeTenantId) throw new Error("Switch to the tenant before inviting a member.");
    return props.client.reducer(control.invitations.create, { email, role: options?.role ?? "member", permissions: (options?.permissions ?? {}) as Record<string, JsonValue>, teamIds: options?.teamIds ?? [], allowedAuthProviders: options?.allowedAuthProviders ?? [], payload: options?.payload ?? {} });
  }, [fetchAccessToken, props.client]);

  const acceptInvitation = useCallback(async (token: string) => {
    const accessToken = await fetchAccessToken({ forceRefreshToken: false });
    if (!accessToken) throw new Error("Sign in before accepting an invitation.");
    return props.client.reducer(control.invitations.accept, { token });
  }, [fetchAccessToken, props.client]);

  const revokeInvitation = useCallback(async (tenantId: string, email: string) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before revoking an invitation.");
    if (tenantId !== sessionRef.current?.activeTenantId) throw new Error("Switch to the tenant before revoking an invitation.");
    await props.client.reducer(control.invitations.revoke, { id: "", email });
  }, [fetchAccessToken, props.client]);

  const enterDeveloperMode = useCallback(async (tenantId: string) => {
    const current = sessionRef.current;
    if (!current) throw new Error("Sign in before entering developer mode.");
    if (developerModeRef.current) throw new Error("Exit developer mode before entering another tenant.");
    setError(null);
    const grant = await props.client.reducer(control.developer.enter, { tenantId }) as ControlImpersonation;
    const expiresAt = String(grant.expiresAt);
    if (!grant.id || !grant.token || !Number.isFinite(Date.parse(expiresAt))) {
      throw new Error("Gonvex returned an invalid developer grant.");
    }
    try {
      await props.client.authenticate({
        project: props.projectId,
        tenant: tenantId,
        token: grant.token,
        fetchToken: undefined,
        identity: { sub: current.account.id, iss: props.projectId },
      });
    } catch (cause) {
      props.client.setAuth({
        project: props.projectId,
        tenant: current.activeTenantId,
        token: current.accessToken,
        fetchToken: undefined,
        identity: { sub: current.account.id, iss: props.projectId },
      });
      throw cause;
    }
    const next = { active: true as const, tenantId, grantId: grant.id, expiresAt, originalTenantId: current.activeTenantId };
    developerModeRef.current = next;
    setDeveloperMode({ active: true, tenantId, grantId: grant.id, expiresAt });
  }, [props.client, props.projectId]);

  const exitDeveloperMode = useCallback(async () => {
    const developer = developerModeRef.current;
    if (!developer?.grantId) return;
    setError(null);
    // Remain in developer mode if revocation fails. Restoring first would leave
    // an active grant detached from the provider's state.
    await props.client.reducer(control.developer.exit, { grantId: developer.grantId });
    restoreAccountSession();
  }, [props.client, restoreAccountSession]);

  const visibleTenantId = developerMode.active ? developerMode.tenantId : session?.activeTenantId;
  const activeTenant = session?.tenants.find((tenant) => tenant.id === visibleTenantId) ?? null;

  const authValue = useMemo<GonvexAuthValue>(() => ({
    isLoading,
    isAuthenticated: Boolean(
      session
      && session.expiresAt > Date.now()
      && session.refreshExpiresAt > Date.now()
    ),
    sessionState,
    fetchAccessToken,
    account: session?.account ?? null,
    tenants: session?.tenants ?? [],
    activeTenant,
    error,
    signIn,
    signInWithProvider,
    signInWithPassword,
    signOut,
    setActiveTenant,
    refreshMemberships,
        createTenant,
        handoffSessionTo,
    inviteMember,
    acceptInvitation,
    revokeInvitation,
    developerMode,
    enterDeveloperMode,
    exitDeveloperMode,
    }), [acceptInvitation, activeTenant, createTenant, developerMode, enterDeveloperMode, error, exitDeveloperMode, fetchAccessToken, handoffSessionTo, inviteMember, isLoading, refreshMemberships, revokeInvitation, session, sessionState, setActiveTenant, signIn, signInWithPassword, signInWithProvider, signOut]);

  return (
    <ManagedAuthContext.Provider value={authValue}>
      <GonvexAuthContext.Provider value={authValue}>
        <GonvexProvider client={props.client}>
          {isLoading ? (props.loadingFallback ?? null) : props.children}
        </GonvexProvider>
      </GonvexAuthContext.Provider>
    </ManagedAuthContext.Provider>
  );
}

export function useGonvexAuth(): GonvexAuthValue {
  const value = useContext(ManagedAuthContext);
  if (!value) throw new Error("GonvexAuthProvider is required");
  return value;
}

/** Subscribed profile for the active tenant, reconciled by GonvexAuthProvider. */
export function useCurrentTenantProfile(): GonvexAuthTenant | null {
  return useGonvexAuth().activeTenant;
}

/** Live tenant-admin invitation list; reducer changes reconcile automatically. */
export function useInvitationList(): ControlInvitationListItem[] | undefined {
  return useControlQuery<ControlInvitationListItem[]>(control.invitations.list, {});
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
  const provider = pkce?.provider ?? "google";
  const providerLabel = provider[0]!.toUpperCase() + provider.slice(1);
  // Surface the runtime error even when another tab already consumed PKCE.
  if (callbackError) {
    const messages: Record<string, string> = {
      access_denied: `${providerLabel} sign-in was cancelled.`,
      invitation_required: `This app is invite-only. Ask an administrator to invite your verified ${providerLabel} email.`,
      verified_google_email_required: "Google must provide a verified email address for this app.",
      verified_microsoft_email_required: "Microsoft must provide a verified email address for this app.",
      membership_setup_failed: "Your account was verified, but its workspace could not be prepared. Please try again.",
      google_exchange_failed: "Google rejected the sign-in code exchange. Check GONVEX_GOOGLE_CLIENT_ID/SECRET and the broker callback URI.",
      microsoft_exchange_failed: "Microsoft rejected the sign-in code exchange. Check the project's Microsoft realm configuration.",
      apple_exchange_failed: "Apple rejected the sign-in code exchange. Check the project's Apple realm configuration.",
      invalid_google_identity: "Google identity verification failed. Please try again.",
      invalid_microsoft_identity: "Microsoft identity verification failed. Please try again.",
      invalid_apple_identity: "Apple identity verification failed. Please try again.",
      microsoft_not_configured: "Microsoft sign-in is not configured for this project.",
      apple_not_configured: "Apple sign-in is not configured for this project.",
      account_creation_failed: `Your ${providerLabel} account could not be linked. Please try again.`,
      code_creation_failed: "Gonvex could not finish creating a sign-in code. Please try again.",
    };
    safeSessionStorageRemove(options.pkceStorageKey);
    clearAuthCallbackParams(url, pkce?.returnTo);
    throw new Error(messages[callbackError] ?? `${providerLabel} sign-in failed (${callbackError}). Please try again.`);
  }
  if (!pkce || !returnedState || returnedState !== pkce.state || Date.now() - pkce.createdAt > 10 * 60 * 1000) {
    clearAuthCallbackParams(url, pkce?.returnTo);
    throw new Error(`The ${providerLabel} sign-in response could not be verified. Please try again.`);
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

function sessionFromNativeGrant(value: JsonValue, previous?: GonvexAuthSession): GonvexAuthSession {
  if (!value || typeof value !== "object" || Array.isArray(value) || !isGonvexAuthSession(value as Partial<GonvexAuthSession>)) {
    throw new GonvexAuthRequestError("Gonvex returned an invalid native session.", 502);
  }
  const session = value as GonvexAuthSession;
  return preserveActiveTenant(session, previous);
}

function preserveActiveTenant(
  session: GonvexAuthSession,
  previous?: GonvexAuthSession,
): GonvexAuthSession {
  const activeTenantId = previous
    && previous.account.id === session.account.id
    && session.tenants.some((tenant) => tenant.id === previous.activeTenantId)
    ? previous.activeTenantId
    : session.activeTenantId;
  return activeTenantId === session.activeTenantId
    ? session
    : { ...session, activeTenantId };
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
  if (cause instanceof GonvexAuthRequestError) {
    return cause.status === 400 || cause.status === 401 || cause.status === 403;
  }
  if (!(cause instanceof GonvexClientError)) return false;
  if (cause.code === "auth") return true;
  if (cause.code !== "server") return false;
  return /invalid or expired refresh token|refresh token (?:reuse detected|was already rotated)|login was revoked/i.test(cause.message);
}

function isFatalExternalExchangeError(cause: unknown) {
  if (cause instanceof GonvexAuthRequestError) {
    return cause.status === 400 || cause.status === 401 || cause.status === 403;
  }
  if (!(cause instanceof GonvexClientError)) return false;
  if (cause.code === "auth") return true;
  if (cause.code !== "server") return false;
  return /external identity provider is disabled|project auth mode .* does not allow provider|external identity token (?:is invalid|issuer is invalid|audience is invalid|is expired|subject is missing)|firebase tenant does not match this auth realm|firebase account is disabled or its sessions were revoked|account signup requires an active invitation|verified invited email is required|verified email matches more than one account|account is unavailable|user is disabled|user is revoked/i.test(cause.message);
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

const WARM_SESSION_ACCESS_SAFETY_MS = 30_000;

function authSessionScope(projectId: string, session: GonvexAuthSession) {
  return `${projectId}\u0000${session.activeTenantId ?? ""}\u0000${session.account.id}`;
}

function installClientSession(
  client: GonvexClient,
  projectId: string,
  session: GonvexAuthSession,
) {
  client.setAuth({
    project: projectId,
    tenant: session.activeTenantId,
    token: session.accessToken,
    identity: { sub: session.account.id, iss: projectId },
  });
}

function sameInstalledAuthSession(
  current: GonvexAuthSession | null,
  next: GonvexAuthSession,
) {
  return current?.accessToken === next.accessToken
    && current.refreshToken === next.refreshToken
    && current.activeTenantId === next.activeTenantId
    && current.account.id === next.account.id;
}

/**
 * A provider-backed session may reveal its cached Replica before the external
 * provider hydrates only when the exact stored account and tenant scope still
 * has a current access token. A refresh credential alone is not authorization
 * to expose cached tenant data.
 */
function reusableExternalAuthSession(
  session: GonvexAuthSession | null,
  initialTenantId: string | undefined,
  provider: string,
  hasExplicitInitialTenant = initialTenantId !== undefined,
  now = Date.now(),
): GonvexAuthSession | null {
  if (!session || session.account.provider !== provider) return null;
  if (!Number.isFinite(session.expiresAt) || session.expiresAt <= now + WARM_SESSION_ACCESS_SAFETY_MS) {
    return null;
  }
  const scoped = scopeSessionForInitialTenant(session, initialTenantId, hasExplicitInitialTenant);
  if (!scoped) return null;
  if (
    hasExplicitInitialTenant
    && initialTenantId !== undefined
    && !scoped.tenants.some((tenant) => (
      tenant.id === scoped.activeTenantId
      && (tenant.id === initialTenantId || tenant.domain.toLowerCase() === initialTenantId.toLowerCase())
    ))
  ) return null;
  if (
    scoped.activeTenantId !== undefined
    && !scoped.tenants.some((tenant) => tenant.id === scoped.activeTenantId)
  ) {
    return null;
  }
  return scoped;
}

/** Restrict a persisted session to the tenant context selected by the host. */
function scopeSessionForInitialTenant(
  session: GonvexAuthSession | null,
  initialTenantId: string | undefined,
  hasExplicitInitialTenant: boolean,
): GonvexAuthSession | null {
  if (!session || !hasExplicitInitialTenant) return session;
  // The landlord is account-scoped. Reuse the valid token and tenant directory,
  // but never carry a tenant Replica scope onto that origin.
  if (initialTenantId === undefined) {
    return session.activeTenantId === undefined
      ? session
      : { ...session, activeTenantId: undefined };
  }
  const tenant = session.tenants.find((candidate) => (
    candidate.id === initialTenantId
    || candidate.domain.toLowerCase() === initialTenantId.toLowerCase()
  ));
  if (!tenant) return null;
  return session.activeTenantId === tenant.id
    ? session
    : { ...session, activeTenantId: tenant.id };
}

function readAuthSession(key: string): GonvexAuthSession | null {
  if (typeof window === "undefined") return null;
  try {
    const parsed = JSON.parse(localStorage.getItem(key) ?? "null") as GonvexAuthSession | null;
    if (
      !parsed?.accessToken
      || !Number.isFinite(parsed.expiresAt)
      || !parsed.refreshToken
      || !Number.isFinite(parsed.refreshExpiresAt)
      || !parsed.account?.id
      || !Array.isArray(parsed.tenants)
      || parsed.refreshExpiresAt <= Date.now()
    ) {
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

function useSessionScopeGeneration(client: GonvexClient): number {
  const [generation, setGeneration] = useState(0);
  useEffect(() => {
    const unsubscribe = client.onSessionScopeChange(() => {
      setGeneration((current) => current + 1);
    });
    return () => { unsubscribe(); };
  }, [client]);
  return generation;
}

function isSupersededQuery(error: unknown): error is GonvexClientError {
  return error instanceof GonvexClientError && error.code === "superseded";
}

/** One-shot Query hook with explicit loading/error/timeout status and retry. */
export function useQueryResult<T extends JsonValue = JsonValue>(
  ref: FunctionReference,
  args: JsonValue | "skip" = {},
  options: UseQueryResultOptions = {},
): UseQueryResult<T> {
  const client = useGonvexClient();
  const sessionScopeGeneration = useSessionScopeGeneration(client);
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
        if (isSupersededQuery(failure)) return;
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
  }, [client, kind, path, optimisticKey, argsKey, keepPreviousData, requestGeneration, sessionScopeGeneration, startSlowTimer, clearSlowTimer]);

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

/** Subscribe to a host-owned Control Plane Query on the existing Gonvex connection. */
export function useControlQuery<T extends JsonValue = JsonValue>(ref: FunctionReference, args: JsonValue | "skip" = {}): T | undefined {
  const client = useGonvexClient();
  const argsKey = JSON.stringify(args);
  const watch = useMemo(
    () => args === "skip" ? undefined : client.watchControlQuery<T>(ref, args),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [client, ref.kind, ref.path, argsKey],
  );
  const snapshot = useSyncExternalStore(
    useCallback((notify) => watch?.onUpdate(notify) ?? (() => undefined), [watch]),
    useCallback(() => watch?.getSnapshot(), [watch]),
    () => undefined,
  );
  return snapshot?.result;
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
  const sessionScopeGeneration = useSessionScopeGeneration(client);
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
      (failure) => {
        if (active && !isSupersededQuery(failure)) {
          setError(failure instanceof Error ? failure : new Error(String(failure)));
        }
      },
    );
    return () => { active = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, ref.kind, ref.path, argsKey, sessionScopeGeneration]);

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

/** Replica rows plus exact per-collection authority, completeness, truncation, and freshness. */
export function useReplicaCollectionState<T extends ReplicaRow = ReplicaRow>(
  ref: FunctionReference,
  args: JsonValue | "skip" = {},
): ReplicaCollectionSubscriptionState<T> | undefined {
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
    useCallback(() => watch?.localReplicaState() as ReplicaCollectionSubscriptionState<T> | undefined, [watch]),
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
  const selectedRef = useRef<{ initialized: boolean; rows: T[] | undefined; value: Selected | undefined }>({
    initialized: false,
    rows: undefined,
    value: undefined,
  });
  useEffect(() => {
    selectedRef.current = { initialized: false, rows: undefined, value: undefined };
  }, [watch]);
  const getSnapshot = useCallback(() => {
    const rows = watch?.localReplicaResult();
    if (selectedRef.current.initialized && selectedRef.current.rows === rows) {
      return selectedRef.current.value;
    }
    const next = rows === undefined ? undefined : selectorRef.current(rows);
    if (
      !selectedRef.current.initialized
      || next === undefined
      || selectedRef.current.value === undefined
      || !equalityRef.current(selectedRef.current.value, next)
    ) {
      selectedRef.current = { initialized: true, rows, value: next };
    } else {
      selectedRef.current = { ...selectedRef.current, rows };
    }
    return selectedRef.current.value;
  }, [watch]);
  const subscribe = useCallback((onStoreChange: () => void) => {
    if (!watch) return () => undefined;
    return watch.onUpdate(() => {
      const previous = selectedRef.current.value;
      const rows = watch.localReplicaResult();
      if (selectedRef.current.initialized && selectedRef.current.rows === rows) return;
      const next = rows === undefined ? undefined : selectorRef.current(rows);
      if (
        selectedRef.current.initialized
        && previous !== undefined
        && next !== undefined
        && equalityRef.current(previous, next)
      ) {
        selectedRef.current = { ...selectedRef.current, rows };
        return;
      }
      selectedRef.current = { initialized: true, rows, value: next };
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
  const refRef = useRef(ref);
  refRef.current = ref;
  const timeoutMs = options.timeoutMs;
  return useCallback(
    (args: JsonValue = {}) => client.reducer(refRef.current, args, timeoutMs === undefined ? {} : { timeoutMs }),
    [client, timeoutMs],
  );
}

export function useAction(ref: FunctionReference, options: UseReducerOptions = {}) {
  const client = useGonvexClient();
  const refRef = useRef(ref);
  refRef.current = ref;
  const timeoutMs = options.timeoutMs;
  return useCallback(
    (args: JsonValue = {}) => client.action(refRef.current, args, timeoutMs === undefined ? {} : { timeoutMs }),
    [client, timeoutMs],
  );
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
