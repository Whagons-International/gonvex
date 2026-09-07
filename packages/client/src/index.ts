import { DeliveryDiagnostics } from "./delivery-diagnostics.js";
import type {
  BrowserTelemetryInfo,
  ClientMessage,
  JsonValue,
  MessageTrace,
  QueryCacheDirective,
  ServerCapabilities,
  ServerMessage,
  SubscriptionRevision,
  SyncCursor,
  SyncOpenRequest,
} from "@gonvex/protocol";
import {
  createQueryCacheStore,
  defaultQueryCacheReadTimeoutMs,
  type QueryCacheOptions,
  type QueryCacheStatus,
  type QueryCacheStore,
} from "./query-cache.js";
import {
  createSyncStore,
  syncHashesDigest,
  syncRowsHashes,
  type SyncStore,
  type SyncStoreOptions,
} from "./sync-store.js";
import { GonvexErrorReporter, type ErrorReporterOptions } from "./error-reporter.js";
import {
  OptimisticOverlay,
  optimisticPatchesFromReference,
  type OptimisticMutationDefinition,
  type OptimisticPatch,
  type OptimisticProjection,
  type Row,
} from "./optimistic.js";
import {
  createMutationOutbox,
  type MutationOutbox,
  type OutboxStore,
} from "./outbox.js";
export * from "./cache.js";
export * from "./cache-coordinator.js";
export * from "./browser-cache.js";
export * from "./browser-cache-client.js";
export * from "./browser-cache-shared-worker.js";
export * from "./browser-capabilities.js";
export * from "./persistent-cache.js";
export * from "./query-cache.js";
export * from "./sync-store.js";
export * from "./error-reporter.js";
export * from "./optimistic.js";
export * from "./outbox.js";
export * from "./kv-stores.js";
export * from "./signals.js";
export type { QueryCacheDirective } from "@gonvex/protocol";

type SubscriptionHandler = (message: ServerMessage) => void;
export type SyncReadyMessage = Extract<ServerMessage, { type: "sync.ready" }> & {
  /** True when the server cut this collection at its row or byte budget. */
  truncated?: boolean;
};
export type SyncMessage =
  | Extract<ServerMessage, {
    type:
      | "sync.snapshot"
      | "sync.delta"
      | "sync.needHashes"
      | "sync.syncing"
      | "sync.reset"
      | "sync.error";
  }>
  | SyncReadyMessage;
export type SyncSubscriptionHandler = (message: SyncMessage) => void;
type WatchUpdateHandler = () => void;
type TelemetryHandler = (event: GonvexTelemetryEvent) => void;
type ConnectionStateHandler = (state: ConnectionState) => void;
type QuerySubscription = {
  id: string;
  key: string;
  path: string;
  projection?: OptimisticProjection;
  args: JsonValue;
  listeners: Set<SubscriptionHandler>;
  unsubscribeTimer?: ReturnType<typeof setTimeout>;
  lastMessage?: ServerMessage;
  serverSettled: boolean;
  cacheReadGeneration?: number;
  cacheReadPromise?: Promise<void>;
  cacheReadFallbackTimer?: ReturnType<typeof setTimeout>;
  cachedRevision?: string;
  socketGeneration?: number;
  lastRevision?: SubscriptionRevision;
  revisionSocketGeneration?: number;
};
type SyncSubscription = {
  id: string;
  key: string;
  path: string;
  entity: string;
  args: JsonValue;
  listeners: Set<SyncSubscriptionHandler>;
  unsubscribeTimer?: ReturnType<typeof setTimeout>;
  rows: JsonValue[];
  cursor?: SyncCursor;
  // Keep the newest cursor seen in the current epoch even while an integrity
  // reset clears `cursor` to force a fresh snapshot. Without this floor, a
  // delayed pre-reset snapshot can be accepted during the reopen and become
  // current again.
  cursorFloor?: SyncCursor;
  retiredEpochs: Set<string>;
  keyField: string;
  mode?: "eager" | "progressive";
  truncated?: boolean;
  orderBy?: string;
  orderDirection?: "asc" | "desc";
  maxRows?: number;
  maxBytes?: number;
  lastMessage?: SyncMessage;
  cacheReadGeneration?: number;
  socketGeneration?: number;
  opening: boolean;
  persistence: Promise<void>;
  retryTimer?: ReturnType<typeof setTimeout>;
  retryAttempt: number;
  isUpToDate: boolean;
  hashes: Record<string, string>;
  integrityDigest?: string;
  integrityRows?: JsonValue[];
  integrityEpoch?: string;
  forceFullIntegrity: boolean;
  verificationGeneration: number;
  watermarkPersistTimer?: ReturnType<typeof setTimeout>;
  /**
   * The exact rows array last written to (or read from) the sync store. When
   * a later persist carries the same array, only the cursor has moved and the
   * stored rows can be left untouched.
   */
  persistedRows?: JsonValue[];
};

function syncCursorIsStale(subscription: SyncSubscription, cursor: SyncCursor) {
  if (subscription.retiredEpochs.has(cursor.epoch)) return true;
  const floor = subscription.cursorFloor;
  return floor?.epoch === cursor.epoch && cursor.revision < floor.revision;
}

function raiseSyncCursorFloor(subscription: SyncSubscription, cursor: SyncCursor) {
  if (subscription.cursorFloor && subscription.cursorFloor.epoch !== cursor.epoch) {
    subscription.retiredEpochs.add(subscription.cursorFloor.epoch);
  }
  if (
    !subscription.cursorFloor
    || subscription.cursorFloor.epoch !== cursor.epoch
    || cursor.revision > subscription.cursorFloor.revision
  ) {
    subscription.cursorFloor = cursor;
  }
}
type OneShotQuery = {
  id: string;
  path: string;
  args: JsonValue;
  reject: (error: Error) => void;
  socketGeneration?: number;
  timeoutTimer?: ReturnType<typeof setTimeout>;
};
type PendingCall = {
  id: string;
  kind: "mutation" | "action";
  path: string;
  reject: (error: Error) => void;
  timeoutTimer?: ReturnType<typeof setTimeout>;
};

export type FunctionReference = {
  kind: string;
  path: string;
  optimistic?: {
    projection?: OptimisticProjection;
    mutation?: OptimisticMutationDefinition;
  };
};

export type GonvexClientErrorCode = "server" | "timeout" | "disconnected" | "closed" | "auth";

/**
 * Typed error for every rejected Gonvex operation. `code` distinguishes
 * server-side failures from transport-level ones so apps can decide whether
 * a retry is safe:
 *
 * - `server`: the runtime executed the function and returned an error.
 * - `timeout`: no response arrived within the operation timeout. For
 *   mutations/actions the write may or may not have been applied.
 * - `disconnected`: the socket dropped while the operation was pending.
 *   Mutations/actions fail closed unless a mutation opted into the outbox.
 * - `closed`: the client was explicitly closed.
 * - `auth`: authentication was rejected.
 */
export class GonvexClientError extends Error {
  readonly code: GonvexClientErrorCode;
  readonly path?: string;
  readonly operation?: "query" | "mutation" | "action";

  constructor(message: string, options: { code: GonvexClientErrorCode; path?: string; operation?: "query" | "mutation" | "action" }) {
    super(message);
    this.name = "GonvexClientError";
    this.code = options.code;
    this.path = options.path;
    this.operation = options.operation;
  }
}

export type ConnectionState = {
  isWebSocketConnected: boolean;
  hasEverConnected: boolean;
  connectionCount: number;
  connectionRetries: number;
  hasInflightRequests: boolean;
  inflightMutations: number;
  inflightActions: number;
  inflightOneShotQueries: number;
};

export type GonvexTimeoutOptions = {
  /** One-shot `client.query()` timeout. Default 20s. `0` disables. */
  queryTimeoutMs?: number;
  /** `client.mutation()` timeout. Default 20s. `0` disables. */
  mutationTimeoutMs?: number;
  /** `client.action()` timeout. Default 60s. `0` disables. */
  actionTimeoutMs?: number;
};

export const DEFAULT_QUERY_TIMEOUT_MS = 20_000;
export const DEFAULT_MUTATION_TIMEOUT_MS = 20_000;
export const DEFAULT_ACTION_TIMEOUT_MS = 60_000;

export type CallOptions = {
  /** Per-call override of the operation timeout. `0` disables. */
  timeoutMs?: number;
  /** Ordered row changes to expose until the mutation settles. */
  optimistic?: OptimisticPatch[];
  /** Queue transport failures durably instead of rejecting. Default `reject`. */
  offline?: "queue" | "reject";
};

/** Returned when an offline mutation has been accepted by the local outbox. */
export type QueuedMutationOutcome = {
  status: "queued";
  mutationId: string;
};

export type GonvexAuthTokenFetcher = (args: {
  /** True when the server just rejected the current token — bypass any cache. */
  forceRefreshToken: boolean;
}) => Promise<string | null | undefined>;

export type GonvexClientAuth = {
  project?: string;
  token?: string;
  tenant?: string;
  telemetry?: boolean;
  /**
   * Async source of the auth token, mirroring Convex's `fetchToken` contract.
   * When installed, the client re-fetches before every auth send — on first
   * connect, on every reconnect, and once more with `forceRefreshToken: true`
   * when the server rejects the current token — so a socket that outlives a
   * short-lived JWT (e.g. an ~1h Firebase ID token) reauthenticates with a
   * live credential instead of replaying the expired one. A `token` passed in
   * the same `setAuth` call is trusted and sent as-is; resolving `null` signs
   * the session out; a rejected fetch keeps the currently installed token so
   * an offline start is not signed out.
   */
  fetchToken?: GonvexAuthTokenFetcher;
  /**
   * Non-secret identity hint ({@link https://datatracker.ietf.org/doc/html/rfc7519 JWT}
   * `sub` and `iss` claims) that stands in for a token when deriving the local
   * cache identity. Lets a cold start with no usable token — e.g. an offline
   * tab whose identity provider cannot refresh — recover the warm query-cache
   * directive and serve cached reads. Never sent to the server; a parseable
   * token always takes precedence, and both must derive the same key for the
   * same user (persist the claims of the last token you installed).
   */
  identity?: { sub: string; iss?: string };
};

export type GonvexClientOptions = GonvexClientAuth & {
  queryCache?: false | QueryCacheOptions;
  /**
   * Keep listenerless live queries subscribed for this long so route
   * backtracking can reuse their current result without WebSocket churn.
   * Defaults to 250ms; set a longer bounded window for local-first apps.
   */
  querySubscriptionRetentionMs?: number;
  /**
   * Keep listenerless durable syncs open briefly across React remounts.
   * Defaults to 250ms, preventing close/open/snapshot churn in StrictMode.
   */
  syncSubscriptionRetentionMs?: number;
  sync?: false | SyncStoreOptions;
  /**
   * Durable mutation queue settings. Every replay keeps its original
   * idempotency key, making an accidental cross-tab double-send server-safe.
   * Runtimes without IndexedDB inject `store` to keep queued mutations
   * durable; queue semantics always stay in the SDK.
   */
  outbox?: { databaseName?: string; enabled?: boolean; store?: OutboxStore };
  errorReporting?: false | Omit<ErrorReporterOptions, "endpoint" | "project" | "tenant">;
  timeouts?: GonvexTimeoutOptions;
};

export type GonvexTelemetryEvent = {
  type: "mutation" | "action" | "query";
  id: string;
  path: string;
  reason?: "initial" | "invalidate" | "recover";
  outcome: "ok" | "error";
  error?: string;
  clientSentAtMs?: number;
  clientReceivedAtMs: number;
  clientDurationMs?: number;
  serverTrace?: MessageTrace;
  device?: BrowserTelemetryInfo;
};

// Small collections can send their row hashes immediately and repair in one
// round trip. Everything else resumes with one 64-byte digest and only sends
// the hash map when the server proves that something actually differs
// (sync.needHashes) — the server verifies digest-only resumes with zero row
// data on the unchanged path, so a reload uploads bytes, not hash maps.
const compactSyncIntegrityThreshold = 16;
// Must match the runtime's per-frame sync.openMany admission limit. Keeping
// this client-side prevents one oversized page from stranding every sync in a
// batch behind a frame-level rejection.
const maxSyncBatchOpens = 256;
// A wedged IndexedDB (observed in Chrome: open() never fires any event, so no
// rejection ever reaches the store's error handling) must degrade the warm
// start into a cold open — never into a permanently empty screen. Reads
// normally settle in a few milliseconds.
const syncStoreReadTimeoutMs = 1_000;
// Watermarks can arrive for every tenant revision. Bound cursor-only IndexedDB
// writes per collection while keeping the in-memory resume cursor immediate.
const syncWatermarkPersistDelayMs = 1_000;

export class GonvexClient {
  private socket: WebSocket | undefined;
  private readonly handlers = new Map<string, SubscriptionHandler>();
  private readonly querySubscriptions = new Map<string, QuerySubscription>();
  private readonly syncSubscriptions = new Map<string, SyncSubscription>();
  private readonly oneShotQueries = new Map<string, OneShotQuery>();
  private readonly telemetryHandlers = new Set<TelemetryHandler>();
  private readonly pendingMessages: ClientMessage[] = [];
  private readonly pendingSyncOpens = new Set<SyncSubscription>();
  private readonly pendingQuerySubscribes = new Set<QuerySubscription>();
  private readonly syncPersistence = new Map<string, Promise<void>>();
  private syncOpenFlushTimer: ReturnType<typeof setTimeout> | undefined;
  private querySubscribeFlushTimer: ReturnType<typeof setTimeout> | undefined;
  private serverCapabilities: ServerCapabilities = {};
  private auth: GonvexClientAuth = {};
  private authInFlight = false;
  private authWatchdogTimer: ReturnType<typeof setTimeout> | undefined;
  // Monotonic guard for async token fetches: a resolve whose generation is no
  // longer current was superseded (newer setAuth, watchdog re-issue, or a
  // reconnect's own fetch) and must be discarded.
  private authFetchGeneration = 0;
  // At most one forced refresh per rejection cycle; cleared when auth settles
  // or a fresh send cycle starts, so a bad token can't refresh-loop forever.
  private authRetriedAfterError = false;
  private readonly authErrorHandlers = new Set<(error: string) => void>();
  private telemetryEnabled = false;
  private readonly deliveryDiagnostics = new DeliveryDiagnostics();
  private readonly queryCache: QueryCacheStore | undefined;
  private readonly queryCacheWaitForScope: boolean;
  private readonly queryCacheReadTimeoutMs: number;
  private readonly querySubscriptionRetentionMs: number;
  private readonly syncSubscriptionRetentionMs: number;
  private readonly syncStore: SyncStore | undefined;
  private readonly mutationOutbox: MutationOutbox;
  private readonly overlay = new OptimisticOverlay();
  private readonly optimisticMutationIds = new Set<string>();
  private readonly optimisticOutboxEntryIds = new Map<string, number>();
  private outboxReady: Promise<void>;
  private outboxScope = "";
  private outboxScopeGeneration = 0;
  private readonly outboxEphemeralScope = randomID();
  private readonly unsubscribeOutbox: () => void;
  private readonly unsubscribeOverlay: () => void;
  private drainingOutbox = false;
  private outboxDrainTimer: ReturnType<typeof setTimeout> | undefined;
  private queryCacheDirective: QueryCacheDirective | undefined;
  private queryCacheGeneration = 0;
  // Sync collections live under a visibility-only scope that survives query
  // cache rotations (deploys); their warm reads are guarded separately.
  private syncScopeGeneration = 0;
  private queryCacheNegotiatedSocketGeneration: number | undefined;
  private syncIdentityGeneration = 0;
  private readonly sessionScopeHandlers = new Set<() => void>();
  private readonly errorReporter: GonvexErrorReporter | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectAttempt = 0;
  private socketGeneration = 0;
  private manuallyClosed = false;
  private readonly pendingCalls = new Map<string, PendingCall>();
  private readonly connectionStateHandlers = new Set<ConnectionStateHandler>();
  private isWebSocketConnected = false;
  private hasEverConnected = false;
  private connectionCount = 0;
  private readonly timeouts: Required<GonvexTimeoutOptions>;

  constructor(private readonly url: string, options: GonvexClientOptions = {}) {
    this.auth = authFromOptions(options);
    this.telemetryEnabled = options.telemetry === true;
    this.queryCache = createQueryCacheStore(options.queryCache);
    this.queryCacheWaitForScope = options.queryCache !== undefined && options.queryCache !== false;
    this.queryCacheReadTimeoutMs = queryCacheReadTimeout(
      options.queryCache === false ? undefined : options.queryCache?.readTimeoutMs,
    );
    this.querySubscriptionRetentionMs = normalizeQuerySubscriptionRetentionMs(
      options.querySubscriptionRetentionMs,
    );
    this.syncSubscriptionRetentionMs = normalizeQuerySubscriptionRetentionMs(
      options.syncSubscriptionRetentionMs,
    );
    this.syncStore = createSyncStore(options.sync);
    this.mutationOutbox = createMutationOutbox(options.outbox);
    this.unsubscribeOutbox = this.mutationOutbox.subscribe(() => {
      void this.drainOutbox();
    });
    this.unsubscribeOverlay = this.overlay.subscribe((entity) => {
      this.emitOptimisticEntity(entity);
    });
    // Defer the first restore by one microtask. Apps commonly construct the
    // client and immediately install a cached token/identity; waiting lets the
    // durable queue select that authenticated scope instead of briefly
    // restoring an anonymous user's entries.
    this.outboxReady = Promise.resolve().then(() => this.activateOutboxScope());
    this.timeouts = {
      queryTimeoutMs: options.timeouts?.queryTimeoutMs ?? DEFAULT_QUERY_TIMEOUT_MS,
      mutationTimeoutMs: options.timeouts?.mutationTimeoutMs ?? DEFAULT_MUTATION_TIMEOUT_MS,
      actionTimeoutMs: options.timeouts?.actionTimeoutMs ?? DEFAULT_ACTION_TIMEOUT_MS,
    };
    if (options.errorReporting && options.project) {
      this.errorReporter = new GonvexErrorReporter({ endpoint: url, project: options.project, tenant: options.tenant, ...options.errorReporting });
    }
    this.recoverWarmSyncDirective();
  }

  /** The client's materialized optimistic state for pending-row indicators. */
  get optimisticOverlay(): OptimisticOverlay {
    return this.overlay;
  }

  /** Number of mutations waiting for a definitive server result. */
  async outboxCount(): Promise<number> {
    await this.outboxReady;
    return this.mutationOutbox.count(this.outboxScope);
  }

  connectionState(): ConnectionState {
    const inflightMutations = countPendingCalls(this.pendingCalls, "mutation");
    const inflightActions = countPendingCalls(this.pendingCalls, "action");
    const inflightOneShotQueries = this.oneShotQueries.size;
    return {
      isWebSocketConnected: this.isWebSocketConnected,
      hasEverConnected: this.hasEverConnected,
      connectionCount: this.connectionCount,
      connectionRetries: this.reconnectAttempt,
      hasInflightRequests: inflightMutations + inflightActions + inflightOneShotQueries > 0,
      inflightMutations,
      inflightActions,
      inflightOneShotQueries,
    };
  }

  /** Metadata advertised by the runtime in its latest session.ready frame. */
  serverInfo(): Readonly<ServerCapabilities> {
    return { ...this.serverCapabilities };
  }

  subscribeToConnectionState(handler: ConnectionStateHandler): () => void {
    this.connectionStateHandlers.add(handler);
    return () => {
      this.connectionStateHandlers.delete(handler);
    };
  }

  private notifyConnectionState() {
    if (this.connectionStateHandlers.size === 0) return;
    const state = this.connectionState();
    for (const handler of Array.from(this.connectionStateHandlers)) {
      handler(state);
    }
  }

  setAuth(auth: GonvexClientAuth) {
    this.applyAuth(auth);
    // The caller owns auth now: a token fetch still in flight from the
    // previous installation must not clobber this one when it resolves.
    this.authFetchGeneration += 1;
    if (this.socket?.readyState === WebSocket.OPEN) {
      // A token supplied in this very call was just minted by the caller —
      // send it as-is instead of paying another fetch round trip.
      this.sendAuth(true, { useFetcher: !hasOwn(auth, "token") });
    }
  }

  /**
   * Subscribe to unrecoverable auth rejections: the server refused the
   * credentials and, when a token fetcher is installed, a force-refreshed
   * token did not fix it. Lets apps route to sign-in instead of silently
   * degrading to an unauthenticated session.
   */
  onAuthError(handler: (error: string) => void): () => void {
    this.authErrorHandlers.add(handler);
    return () => {
      this.authErrorHandlers.delete(handler);
    };
  }

  private applyAuth(auth: GonvexClientAuth) {
    const nextAuth = { ...this.auth, ...auth };
    const tokenScopeChanged = hasOwn(auth, "token")
      && auth.token !== this.auth.token
      && !sameAuthTokenIdentity(this.auth, nextAuth);
    const scopeMayChange = tokenScopeChanged
      || (hasOwn(auth, "tenant") && auth.tenant !== this.auth.tenant)
      || (hasOwn(auth, "project") && auth.project !== this.auth.project)
      // An identity hint that changes the derived key must recover (or drop)
      // the warm directive just like a token change would. Same-key updates —
      // e.g. installing the hint after its token is already live — are inert.
      || (hasOwn(auth, "identity") && !sameAuthTokenIdentity(this.auth, nextAuth));
    if (scopeMayChange) {
      this.resetQueryCacheScope();
    }
    this.auth = nextAuth;
    if (scopeMayChange) {
      void this.activateOutboxScope();
      this.recoverWarmSyncDirective();
    }
    if (auth.tenant !== undefined) this.errorReporter?.setTenant(auth.tenant);
    if (auth.project !== undefined) this.errorReporter?.setProject(auth.project);
    if (auth.telemetry !== undefined) {
      this.telemetryEnabled = auth.telemetry === true;
      if (!this.telemetryEnabled) this.deliveryDiagnostics.close();
    }
  }

  connect() {
    if (this.socket && this.socket.readyState <= WebSocket.OPEN) return;

    const isReconnect = this.socket !== undefined;
    this.manuallyClosed = false;
    const generation = ++this.socketGeneration;
    const socket = new WebSocket(this.url);
    this.socket = socket;
    socket.addEventListener("open", () => {
      if (this.socket !== socket) return;
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = undefined;
      }
      this.reconnectAttempt = 0;
      this.isWebSocketConnected = true;
      this.hasEverConnected = true;
      this.connectionCount += 1;
      this.sendAuth(false);
      if (isReconnect) this.resubscribeQueries(generation);
      void this.drainOutbox();
      this.notifyConnectionState();
    });
    socket.addEventListener("close", () => {
      if (this.socket !== socket || this.manuallyClosed) return;
      this.isWebSocketConnected = false;
      this.markSyncSubscriptionsOutOfDate();
      this.authInFlight = false;
      if (this.authWatchdogTimer) {
        clearTimeout(this.authWatchdogTimer);
        this.authWatchdogTimer = undefined;
      }
      // A subscription queued for the old socket is superseded by the complete
      // resubscribe below. Queued mutations/actions are rejected below, so
      // drop them too — flushing them after reconnect would fire writes whose
      // callers already saw a rejection.
      this.pendingMessages.length = 0;
      // Mutations/actions must fail closed on transport loss: silently
      // replaying a non-idempotent write after reconnect is unsafe, and
      // leaving the promise pending hangs the caller forever.
      this.rejectPendingCalls((call) => new GonvexClientError(
        `Connection lost while waiting for ${call.kind} ${call.path}. The operation may or may not have been applied.`,
        { code: "disconnected", path: call.path, operation: call.kind },
      ));
      this.scheduleReconnect();
      this.notifyConnectionState();
    });
    socket.addEventListener("message", (event) => {
      if (this.socket !== socket) return;
      if (this.telemetryEnabled) this.deliveryDiagnostics.begin(event.timeStamp);
      let message: ServerMessage;
      try {
        message = JSON.parse(String(event.data)) as ServerMessage;
        if (this.telemetryEnabled) this.deliveryDiagnostics.decodedMessage();
      } catch {
        return;
      }
      if (message.type === "session.ready") {
        this.serverCapabilities = message.capabilities ?? {};
        if (!this.auth.token && !this.auth.tenant) {
          this.installQueryCacheDirective(message.queryCache);
          this.queryCacheNegotiatedSocketGeneration = this.socketGeneration;
          this.resumeQuerySubscriptions();
        }
        return;
      }
      if (message.type === "session.scope") {
        this.installQueryCacheDirective(message.queryCache);
        this.queryCacheNegotiatedSocketGeneration = this.socketGeneration;
        this.resumeQuerySubscriptions();
        return;
      }
      if (message.type === "auth.result" || message.type === "auth.error") {
        this.authInFlight = false;
        if (this.authWatchdogTimer) {
          clearTimeout(this.authWatchdogTimer);
          this.authWatchdogTimer = undefined;
        }
        if (message.type === "auth.result") {
          this.authRetriedAfterError = false;
          this.installQueryCacheDirective(queryCacheDirectiveFromAuthResult(message.result));
          this.queryCacheNegotiatedSocketGeneration = this.socketGeneration;
          this.resumeQuerySubscriptions();
        } else {
          const fetcher = this.auth.fetchToken;
          if (fetcher && !this.authRetriedAfterError) {
            // The installed token was rejected — typically expired while the
            // socket was down. Force-refresh through the fetcher and retry
            // once before treating the rejection as final.
            this.authRetriedAfterError = true;
            this.authInFlight = true;
            this.armAuthWatchdog();
            void this.refreshRejectedAuth(fetcher, this.auth.token, message.error);
            return;
          }
          this.authRetriedAfterError = false;
          this.resetQueryCacheScope();
          this.notifyAuthError(message.error);
        }
        this.flushPendingMessages();
      }
      if (message.type === "sync.readyMany") {
        for (const ready of message.ready) {
          const readyMessage = { type: "sync.ready", ...ready } as SyncReadyMessage;
          this.handlers.get(ready.id)?.(readyMessage);
        }
        return;
      }
      if (message.type === "sync.watermark") {
        if (this.serverCapabilities.syncWatermark === 1) {
          this.handleSyncWatermark(message.revision);
        }
        return;
      }
			if (message.type === "query.fanout") {
				const { ids, queryType, ...shared } = message;
				for (const id of ids) {
					this.handlers.get(id)?.({ ...shared, type: queryType, id } as ServerMessage);
				}
				return;
			}
			if (message.type === "query.batch") {
				for (const nested of message.messages) {
					if (nested.type === "query.fanout") {
						const { ids, queryType, ...shared } = nested;
						for (const id of ids) {
							this.handlers.get(id)?.({ ...shared, type: queryType, id } as ServerMessage);
						}
						continue;
					}
					if (nested.type !== "query.batch") {
						const nestedID = "id" in nested ? nested.id : "system";
						this.handlers.get(nestedID)?.(nested);
					}
				}
				return;
			}
      const id = "id" in message ? message.id : "system";
      this.handlers.get(id)?.(message);
    });
  }

  close() {
    this.deliveryDiagnostics.close();
    this.manuallyClosed = true;
    if (isEphemeralOutboxScope(this.outboxScope)) {
      void this.mutationOutbox.clear(this.outboxScope);
    }
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    for (const query of this.oneShotQueries.values()) {
      if (query.timeoutTimer) clearTimeout(query.timeoutTimer);
      query.reject(new GonvexClientError("Gonvex client was closed", { code: "closed", path: query.path, operation: "query" }));
    }
    this.oneShotQueries.clear();
    this.rejectPendingCalls((call) => new GonvexClientError(
      `Gonvex client was closed while waiting for ${call.kind} ${call.path}`,
      { code: "closed", path: call.path, operation: call.kind },
    ));
    for (const subscription of this.syncSubscriptions.values()) {
      this.clearSyncRetry(subscription);
      if (subscription.unsubscribeTimer) clearTimeout(subscription.unsubscribeTimer);
      if (subscription.watermarkPersistTimer) {
        clearTimeout(subscription.watermarkPersistTimer);
        subscription.watermarkPersistTimer = undefined;
        this.persistSyncSnapshot(subscription, true);
      }
    }
    if (this.syncOpenFlushTimer) {
      clearTimeout(this.syncOpenFlushTimer);
      this.syncOpenFlushTimer = undefined;
    }
    this.pendingSyncOpens.clear();
    if (this.querySubscribeFlushTimer) {
      clearTimeout(this.querySubscribeFlushTimer);
      this.querySubscribeFlushTimer = undefined;
    }
    this.pendingQuerySubscribes.clear();
    if (this.outboxDrainTimer) {
      clearTimeout(this.outboxDrainTimer);
      this.outboxDrainTimer = undefined;
    }
    this.unsubscribeOutbox();
    this.unsubscribeOverlay();
    for (const subscription of this.querySubscriptions.values()) {
      if (subscription.cacheReadFallbackTimer) clearTimeout(subscription.cacheReadFallbackTimer);
    }
    this.handlers.clear();
    this.querySubscriptions.clear();
    this.syncSubscriptions.clear();
    this.sessionScopeHandlers.clear();
    this.authErrorHandlers.clear();
    // Invalidate any token fetch still in flight so its resolve can't touch
    // the closed client's caches.
    this.authFetchGeneration += 1;
    this.queryCacheGeneration += 1;
    this.queryCacheDirective = undefined;
    this.queryCache?.close();
    this.syncStore?.close();
    this.errorReporter?.close();
    const socket = this.socket;
    this.socket = undefined;
    this.isWebSocketConnected = false;
    this.notifyConnectionState();
    this.connectionStateHandlers.clear();
    if (!socket) return;
    socket.close();
  }

  private rejectPendingCalls(makeError: (call: PendingCall) => GonvexClientError) {
    if (this.pendingCalls.size === 0) return;
    const calls = Array.from(this.pendingCalls.values());
    this.pendingCalls.clear();
    for (const call of calls) {
      if (call.timeoutTimer) clearTimeout(call.timeoutTimer);
      this.handlers.delete(call.id);
      call.reject(makeError(call));
    }
  }

  onTelemetry(handler: TelemetryHandler) {
    this.telemetryHandlers.add(handler);
    return () => this.telemetryHandlers.delete(handler);
  }

  onSessionScopeChange(handler: () => void) {
    this.sessionScopeHandlers.add(handler);
    return () => this.sessionScopeHandlers.delete(handler);
  }

  async clearQueryCache(options: { allScopes?: boolean } = {}) {
    if (!this.queryCache) return;
    const scope = options.allScopes ? undefined : this.queryCacheDirective?.scope;
    if (!options.allScopes && !scope) return;
    await this.queryCache.clear(scope);
  }

  getQueryCacheStatus(): QueryCacheStatus {
    return this.queryCache?.status() ?? {
      enabled: false,
      readsEnabled: false,
      writesEnabled: false,
      reason: "disabled-by-client",
    };
  }

  subscribeQuery(ref: FunctionReference, args: JsonValue = {}, onMessage: SubscriptionHandler) {
    this.connect();
    const key = querySubscriptionKey(ref, args);
    const existing = this.querySubscriptions.get(key);
    if (existing) {
      const wasOrphaned = existing.listeners.size === 0;
      if (existing.unsubscribeTimer) {
        clearTimeout(existing.unsubscribeTimer);
        existing.unsubscribeTimer = undefined;
      }
      existing.listeners.add(onMessage);
      this.startQueryCacheRead(existing);
      // Replay the latest result/error to this late joiner. Coalesced subscriptions
      // share a single server subscription, so the server only sends `initial` once —
      // to the first subscriber. Without this replay, components that mount after the
      // initial result arrives (e.g. a dialog opened later) would never receive data
      // until the next server-side invalidation. Replaying here (not via the shared
      // handler) keeps the cached value flowing without emitting extra telemetry/traffic.
      const cached = existing.lastMessage;
      if (wasOrphaned && cached?.type === "query.error") {
        // A React error boundary can unmount and remount a failed query while the
        // unsubscribe grace timer is still active. Replaying that terminal error
        // traps the boundary even if the runtime was updated in the meantime.
        // Treat the remount as a fresh attempt while continuing to coalesce active
        // subscribers normally.
        existing.lastMessage = undefined;
        existing.serverSettled = false;
        existing.socketGeneration = undefined;
        this.sendSubscription(existing);
      } else if (cached) {
        queueMicrotask(() => {
          if (existing.listeners.has(onMessage)) onMessage(this.materializeQueryMessage(existing, cached));
        });
      }
      return () => this.unsubscribeQueryListener(key, onMessage);
    }

    const subscription: QuerySubscription = {
      id: randomID(),
      key,
      path: ref.path,
      projection: ref.optimistic?.projection,
      args,
      listeners: new Set([onMessage]),
      serverSettled: false,
    };
    if (subscription.projection) {
      this.overlay.expectSource(subscription.key, subscription.projection.entity);
    }
    this.querySubscriptions.set(key, subscription);
    this.handlers.set(subscription.id, (message) => {
      const normalized = this.normalizeSubscriptionMessage(subscription, message);
      if (!normalized) return;
      message = normalized;
      if (message.type === "query.result") {
        if (message.cacheScope && message.cacheScope !== this.queryCacheDirective?.scope) {
          return;
        }
        subscription.serverSettled = true;
        subscription.lastMessage = message;
        this.recordTelemetry({
          type: "query",
          id: message.id,
          path: subscription.path,
          reason: message.reason,
          outcome: "ok",
          clientReceivedAtMs: nowMs(),
          serverTrace: message.trace,
        });
      }
      if (message.type === "query.error") {
        subscription.serverSettled = true;
        subscription.lastMessage = message;
        this.recordTelemetry({
          type: "query",
          id: message.id,
          path: subscription.path,
          outcome: "error",
          error: message.error,
          clientReceivedAtMs: nowMs(),
        });
      }
      const outgoing = this.materializeQueryMessage(subscription, message);
      for (const listener of Array.from(subscription.listeners)) {
        listener(outgoing);
      }
      if (message.type === "query.result") {
        this.acknowledgeOptimisticSource(subscription.key, message.mutationIds);
        this.acknowledgeOptimisticQuerySnapshot(subscription, message.result);
      }
      if (message.type === "query.result") {
        this.persistQueryResult(subscription, message);
      } else if (message.type === "query.error") {
        this.deleteCachedQuery(subscription);
      }
    });
    this.sendSubscription(subscription);
    this.startQueryCacheRead(subscription);

    return () => this.unsubscribeQueryListener(key, onMessage);
  }

  private normalizeSubscriptionMessage(subscription: QuerySubscription, message: ServerMessage): ServerMessage | undefined {
    if (message.type === "query.progress") {
      if (subscription.lastMessage?.type !== "query.result") {
        // A progress frame only confirms that an advertised cache revision is
        // current. If the in-memory snapshot is gone, accepting it would leave
        // listeners permanently without a value.
        this.requestSubscriptionSnapshot(subscription);
        return undefined;
      }
      if (!this.acceptRevision(subscription, message.throughRevision)) return undefined;
      subscription.lastRevision = message.throughRevision;
      subscription.revisionSocketGeneration = this.socketGeneration;
      subscription.serverSettled = true;
      this.acknowledgeOptimisticSource(subscription.key, message.mutationIds);
      // Progress advances freshness without waking React/query listeners.
      return undefined;
    }
    if (message.type === "query.patch") {
      if (!sameRevision(message.baseRevision, subscription.lastRevision)) {
        // A missing base is never guessed through. Re-subscribing causes the
        // runtime to replay an authoritative shared snapshot.
        this.requestSubscriptionSnapshot(subscription);
        return undefined;
      }
      if (!this.acceptRevision(subscription, message.subscriptionRevision)) return undefined;
      const previous = subscription.lastMessage;
      if (previous?.type !== "query.result" || !Array.isArray(previous.result)) {
        this.requestSubscriptionSnapshot(subscription);
        return undefined;
      }
      const result = applyKeyedPatch(previous.result, message);
      if (!result) {
        this.requestSubscriptionSnapshot(subscription);
        return undefined;
      }
      subscription.lastRevision = message.subscriptionRevision;
      subscription.revisionSocketGeneration = this.socketGeneration;
      return {
        type: "query.result",
        id: message.id,
        path: message.path,
        result,
        reason: message.reason,
        trace: message.trace,
        cacheScope: message.cacheScope,
        cacheRevision: message.cacheRevision,
        subscriptionRevision: message.subscriptionRevision,
        mutationIds: message.mutationIds,
      };
    }
		if (message.type === "query.pagePatch") {
			if (!sameRevision(message.baseRevision, subscription.lastRevision)) {
				this.requestSubscriptionSnapshot(subscription);
				return undefined;
			}
			if (!this.acceptRevision(subscription, message.subscriptionRevision)) return undefined;
			const previous = subscription.lastMessage;
			if (previous?.type !== "query.result" || !isJsonRecord(previous.result) || !Array.isArray(previous.result.page)) {
				this.requestSubscriptionSnapshot(subscription);
				return undefined;
			}
			const page = applyKeyedPatch(previous.result.page, message);
			if (!page) {
				this.requestSubscriptionSnapshot(subscription);
				return undefined;
			}
			const metadata = isJsonRecord(message.result) ? message.result : {};
			subscription.lastRevision = message.subscriptionRevision;
			subscription.revisionSocketGeneration = this.socketGeneration;
			return { ...message, type: "query.result", result: { ...previous.result, ...metadata, page }, mutationIds: message.mutationIds };
		}
		if (message.type === "query.objectPatch") {
			if (!sameRevision(message.baseRevision, subscription.lastRevision)) {
				this.requestSubscriptionSnapshot(subscription);
				return undefined;
			}
			if (!this.acceptRevision(subscription, message.subscriptionRevision)) return undefined;
			const previous = subscription.lastMessage;
			if (previous?.type !== "query.result" || !isJsonRecord(previous.result)) {
				this.requestSubscriptionSnapshot(subscription);
				return undefined;
			}
			const result: Record<string, JsonValue> = { ...previous.result };
			for (const [key, patch] of Object.entries(message.collections)) {
				const collection = result[key];
				if (!Array.isArray(collection)) {
					this.requestSubscriptionSnapshot(subscription);
					return undefined;
				}
				const patched = applyKeyedPatch(collection, patch);
				if (!patched) {
					this.requestSubscriptionSnapshot(subscription);
					return undefined;
				}
				result[key] = patched;
			}
			subscription.lastRevision = message.subscriptionRevision;
			subscription.revisionSocketGeneration = this.socketGeneration;
			return { ...message, type: "query.result", result, mutationIds: message.mutationIds };
		}
    if (message.type === "query.result" && message.subscriptionRevision) {
      if (!this.acceptRevision(subscription, message.subscriptionRevision)) return undefined;
      subscription.lastRevision = message.subscriptionRevision;
      subscription.revisionSocketGeneration = this.socketGeneration;
    }
    return message;
  }

  private acceptRevision(subscription: QuerySubscription, next: SubscriptionRevision) {
    const previous = subscription.lastRevision;
    if (!previous) return true;
    if (next.epoch === previous.epoch) return next.sequence > previous.sequence;
    // Epochs are opaque runtime-start IDs. A different epoch is accepted only
    // after the socket generation changes; otherwise a delayed old-epoch frame
    // could overwrite a result already accepted on the same connection.
    return subscription.revisionSocketGeneration !== this.socketGeneration;
  }

  watchQuery<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}) {
    let latest: T | undefined;
    let latestError: Error | undefined;
    const updateHandlers = new Set<WatchUpdateHandler>();

    const unsubscribe = this.subscribeQuery(ref, args, (message) => {
      if (message.type === "query.result") {
        latest = message.result as T;
        latestError = undefined;
        for (const handler of updateHandlers) handler();
      }
      if (message.type === "query.error") {
        latestError = new Error(message.error);
        for (const handler of updateHandlers) handler();
      }
    });
    const unsubscribeScope = this.onSessionScopeChange(() => {
      latest = undefined;
      latestError = undefined;
      for (const handler of updateHandlers) handler();
    });

    return {
      localQueryResult() {
        if (latestError) throw latestError;
        return latest;
      },
      onUpdate(handler: WatchUpdateHandler) {
        updateHandlers.add(handler);
        return () => {
          updateHandlers.delete(handler);
          if (updateHandlers.size === 0) {
            unsubscribe();
            unsubscribeScope();
          }
        };
      },
    };
  }

  subscribeSync(ref: FunctionReference, args: JsonValue = {}, onMessage: SyncSubscriptionHandler) {
    this.connect();
    const key = querySubscriptionKey(ref, args);
    const existing = this.syncSubscriptions.get(key);
    if (existing) {
      if (existing.unsubscribeTimer) {
        clearTimeout(existing.unsubscribeTimer);
        existing.unsubscribeTimer = undefined;
      }
      existing.listeners.add(onMessage);
      if (existing.lastMessage) {
        queueMicrotask(() => {
          if (existing.listeners.has(onMessage) && existing.lastMessage) {
            onMessage(this.materializeSyncMessage(existing, existing.lastMessage));
          }
        });
      }
      return () => this.unsubscribeSyncListener(key, onMessage);
    }

    const subscription: SyncSubscription = {
      id: randomID(),
      key,
      path: ref.path,
      entity: ref.optimistic?.projection?.entity ?? ref.path,
      args,
      listeners: new Set([onMessage]),
      rows: [],
      keyField: "id",
      opening: false,
      persistence: Promise.resolve(),
      retryAttempt: 0,
      isUpToDate: false,
      hashes: {},
      forceFullIntegrity: false,
      verificationGeneration: 0,
      retiredEpochs: new Set(),
    };
    this.overlay.expectSource(subscription.key, subscription.entity);
    this.syncSubscriptions.set(key, subscription);
    this.handlers.set(subscription.id, (message) => this.handleSyncMessage(subscription, message as SyncMessage));
    this.startSync(subscription);
    return () => this.unsubscribeSyncListener(key, onMessage);
  }

  watchSync<T extends JsonValue = JsonValue>(ref: FunctionReference, args: JsonValue = {}) {
    let latest: T[] | undefined;
    let latestError: Error | undefined;
    const thisClient = this;
    const key = querySubscriptionKey(ref, args);
    const updateHandlers = new Set<WatchUpdateHandler>();
    const notify = () => {
      for (const handler of updateHandlers) handler();
    };
    const unsubscribe = this.subscribeSync(ref, args, (message) => {
      if (message.type === "sync.snapshot") {
        latest = message.result as T[];
        latestError = undefined;
        notify();
      } else if (message.type === "sync.ready") {
        latestError = undefined;
        notify();
      } else if (message.type === "sync.syncing" || message.type === "sync.reset") {
        notify();
      } else if (message.type === "sync.error") {
        latestError = new Error(message.error);
        notify();
      }
    });
    const unsubscribeScope = this.onSessionScopeChange(() => {
      latest = undefined;
      latestError = undefined;
      notify();
    });
    return {
      localSyncResult() {
        if (latestError) throw latestError;
        return latest;
      },
      status() {
        return {
          isLoading: latest === undefined,
          isUpToDate: thisClient.syncSubscriptions.get(key)?.isUpToDate === true,
        };
      },
      onUpdate(handler: WatchUpdateHandler) {
        updateHandlers.add(handler);
        return () => {
          updateHandlers.delete(handler);
          if (updateHandlers.size === 0) {
            unsubscribe();
            unsubscribeScope();
          }
        };
      },
    };
  }

  private handleSyncMessage(subscription: SyncSubscription, message: SyncMessage) {
    if (message.type === "sync.snapshot") {
      // Snapshots are only valid responses to an outstanding sync.open. Live
      // subscriptions advance through deltas; accepting an unsolicited or
      // delayed snapshot could roll a verified collection back to old rows.
      if (!subscription.opening) return;
      if (syncCursorIsStale(subscription, message.cursor)) return;
      if (subscription.cursor
        && message.cursor.epoch === subscription.cursor.epoch
        && message.cursor.revision < subscription.cursor.revision) return;
      this.clearSyncRetry(subscription, true);
      subscription.verificationGeneration += 1;
      subscription.isUpToDate = false;
      subscription.opening = false;
      subscription.cursor = message.cursor;
      raiseSyncCursorFloor(subscription, message.cursor);
      subscription.keyField = message.key;
      subscription.mode = message.mode;
      subscription.truncated = undefined;
      subscription.orderBy = message.orderBy;
      subscription.orderDirection = message.orderDirection;
      subscription.maxRows = message.maxRows;
      subscription.maxBytes = message.maxBytes;
      subscription.rows = boundSyncRows(
        message.result,
        message.key,
        message.maxRows,
        message.maxBytes,
        message.orderBy,
        message.orderDirection,
      );
      subscription.hashes = { ...(message.hashes ?? {}) };
      subscription.integrityDigest = undefined;
      subscription.integrityRows = undefined;
      subscription.integrityEpoch = undefined;
      const snapshot: SyncMessage = { ...message, result: subscription.rows };
      subscription.lastMessage = snapshot;
      this.emitSyncMessage(subscription, snapshot);
      this.persistSyncSnapshot(subscription);
      return;
    }
    if (message.type === "sync.delta") {
      if (syncCursorIsStale(subscription, message.cursor)) return;
      if (subscription.cursor && (
        message.cursor.epoch !== subscription.cursor.epoch
        || message.cursor.revision < subscription.cursor.revision
        || (
          message.cursor.revision === subscription.cursor.revision
          && !message.digest
        )
      )) return;
      this.clearSyncRetry(subscription, true);
      subscription.verificationGeneration += 1;
      subscription.isUpToDate = false;
      subscription.cursor = message.cursor;
      raiseSyncCursorFloor(subscription, message.cursor);
      subscription.rows = applySyncDelta(
        subscription.rows,
        subscription.keyField,
        message.upserts ?? [],
        message.deleted ?? [],
        subscription.maxRows,
        subscription.maxBytes,
        subscription.orderBy,
        subscription.orderDirection,
      );
      for (const key of message.deleted ?? []) delete subscription.hashes[key];
      Object.assign(subscription.hashes, message.hashes ?? {});
      subscription.integrityDigest = undefined;
      subscription.integrityRows = undefined;
      subscription.integrityEpoch = undefined;
      const snapshot: SyncMessage = {
        type: "sync.snapshot",
        id: subscription.id,
        path: subscription.path,
        result: subscription.rows,
        cursor: message.cursor,
        key: subscription.keyField,
        mode: subscription.mode,
        orderBy: subscription.orderBy,
        orderDirection: subscription.orderDirection,
        maxRows: subscription.maxRows,
        maxBytes: subscription.maxBytes,
      };
      subscription.lastMessage = snapshot;
      this.emitSyncMessage(subscription, snapshot);
      this.acknowledgeOptimisticSource(subscription.key, message.mutationIds);
      this.persistSyncDelta(subscription, message.upserts ?? [], message.deleted ?? []);
      return;
    }
    if (message.type === "sync.reset") {
      this.clearSyncRetry(subscription, true);
      if (subscription.watermarkPersistTimer) {
        clearTimeout(subscription.watermarkPersistTimer);
        subscription.watermarkPersistTimer = undefined;
      }
      subscription.verificationGeneration += 1;
      subscription.isUpToDate = false;
      subscription.cursor = undefined;
      subscription.truncated = undefined;
      subscription.rows = [];
      subscription.persistedRows = undefined;
      subscription.hashes = {};
      subscription.integrityDigest = undefined;
      subscription.integrityRows = undefined;
      subscription.integrityEpoch = undefined;
      subscription.forceFullIntegrity = false;
      subscription.lastMessage = undefined;
      subscription.opening = false;
      const directive = this.queryCacheDirective;
      const store = this.syncStore;
      if (directive && store) {
        const scope = syncPersistenceScope(directive);
        this.enqueueSyncPersistence(subscription, scope, () => store.delete(
          scope,
          subscription.path,
          subscription.args,
        ));
      }
      this.emitSyncMessage(subscription, message);
      queueMicrotask(() => this.sendSyncOpen(subscription));
      return;
    }
    if (message.type === "sync.syncing") {
      subscription.verificationGeneration += 1;
      subscription.isUpToDate = false;
      this.emitSyncMessage(subscription, message);
      return;
    }
    if (message.type === "sync.needHashes") {
      subscription.verificationGeneration += 1;
      subscription.isUpToDate = false;
      subscription.opening = false;
      subscription.forceFullIntegrity = true;
      this.emitSyncMessage(subscription, {
        type: "sync.syncing",
        id: subscription.id,
        path: subscription.path,
        reason: "integrity-reconciling",
      });
      queueMicrotask(() => this.sendSyncOpen(subscription));
      return;
    }
    if (message.type === "sync.ready") {
      if (!subscription.cursor || (
        message.cursor.epoch !== subscription.cursor.epoch
        || message.cursor.revision < subscription.cursor.revision
        || syncCursorIsStale(subscription, message.cursor)
      )) return;
      const generation = ++subscription.verificationGeneration;
      if (!message.digest && this.serverCapabilities.syncIntegrity === 1) {
        this.handleSyncMessage(subscription, {
          type: "sync.reset",
          id: subscription.id,
          path: subscription.path,
          reason: "integrity-missing",
        });
        return;
      }
      if (
        !subscription.forceFullIntegrity
        && subscription.integrityRows === subscription.rows
        && subscription.integrityDigest
        && subscription.integrityEpoch === subscription.cursor.epoch
      ) {
        if (message.digest && subscription.integrityDigest !== message.digest) {
          // Re-hash once before treating the server/memo disagreement as an
          // integrity failure. The memo may be stale even though row identity
          // says the collection has not changed.
        } else {
          this.acceptSyncReady(subscription, message, subscription.integrityDigest);
          return;
        }
      }
      void syncRowsHashes(subscription.rows, subscription.keyField).then((hashes) => (
        syncHashesDigest(hashes).then((digest) => ({ digest, hashes }))
      )).then(({ digest, hashes }) => {
          if (
            generation !== subscription.verificationGeneration
            || this.syncSubscriptions.get(subscription.key) !== subscription
          ) return;
          if (message.digest && digest !== message.digest) {
            this.handleSyncMessage(subscription, {
              type: "sync.reset",
              id: subscription.id,
              path: subscription.path,
              reason: "integrity-mismatch",
            });
            return;
          }
          subscription.hashes = hashes;
          subscription.integrityDigest = digest;
          subscription.integrityRows = subscription.rows;
          this.acceptSyncReady(subscription, message, digest);
        }).catch(() => {
          if (generation !== subscription.verificationGeneration) return;
          this.handleSyncMessage(subscription, {
            type: "sync.reset",
            id: subscription.id,
            path: subscription.path,
            reason: "integrity-mismatch",
          });
        });
      return;
    }
    if (message.type === "sync.error") {
      subscription.verificationGeneration += 1;
      subscription.isUpToDate = false;
      subscription.opening = false;
      this.scheduleSyncRetry(subscription);
    }
    this.emitSyncMessage(subscription, message);
  }

  private acceptSyncReady(
    subscription: SyncSubscription,
    message: SyncReadyMessage,
    verifiedDigest = message.digest,
  ) {
    this.clearSyncRetry(subscription, true);
    subscription.isUpToDate = true;
    subscription.opening = false;
    subscription.cursor = message.cursor;
    raiseSyncCursorFloor(subscription, message.cursor);
    subscription.mode = message.mode ?? subscription.mode;
    subscription.truncated = message.truncated;
    subscription.integrityDigest = verifiedDigest;
    subscription.integrityRows = subscription.rows;
    subscription.integrityEpoch = message.cursor.epoch;
    subscription.forceFullIntegrity = false;
    this.persistSyncSnapshot(subscription);
    // Every emitted ready frame is self-describing: when a legacy runtime
    // omitted the digest, the locally verified one is stamped in so consumers
    // observe one contract regardless of the peer's protocol generation.
    this.emitSyncMessage(
      subscription,
      message.digest === verifiedDigest ? message : { ...message, digest: verifiedDigest },
    );
  }

  private handleSyncWatermark(revision: number) {
    if (!Number.isSafeInteger(revision) || revision < 0) return;
    for (const subscription of this.syncSubscriptions.values()) {
      const cursor = subscription.cursor;
      if (
        !cursor
        || cursor.revision >= revision
        || !subscription.isUpToDate
        || subscription.opening
        || subscription.forceFullIntegrity
        || subscription.integrityRows !== subscription.rows
        || !subscription.integrityDigest
        || subscription.integrityEpoch !== cursor.epoch
      ) continue;
      subscription.cursor = { ...cursor, revision };
      raiseSyncCursorFloor(subscription, subscription.cursor);
      this.scheduleSyncWatermarkPersistence(subscription);
    }
  }

  private scheduleSyncWatermarkPersistence(subscription: SyncSubscription) {
    if (subscription.watermarkPersistTimer) return;
    subscription.watermarkPersistTimer = setTimeout(() => {
      subscription.watermarkPersistTimer = undefined;
      if (this.syncSubscriptions.get(subscription.key) !== subscription) return;
      this.persistSyncSnapshot(subscription, true);
    }, syncWatermarkPersistDelayMs);
  }

  private emitSyncMessage(subscription: SyncSubscription, message: SyncMessage) {
    const outgoing = this.materializeSyncMessage(subscription, message);
    for (const listener of Array.from(subscription.listeners)) listener(outgoing);
    if (message.type === "sync.snapshot") {
      const settled = this.overlay.acknowledgeMatching(
        subscription.key,
        subscription.entity,
        message.result as unknown as readonly Row[],
        message.key,
      );
      for (const mutationId of settled) void this.ackOptimisticMutation(mutationId);
    }
  }

  private materializeSyncMessage(subscription: SyncSubscription, message: SyncMessage): SyncMessage {
    if (message.type !== "sync.snapshot") return message;
    return {
      ...message,
      result: this.overlay.apply(
        subscription.key,
        subscription.entity,
        message.result as unknown as readonly Row[],
        message.key,
      ) as unknown as JsonValue[],
    };
  }

  private materializeQueryMessage(subscription: QuerySubscription, message: ServerMessage): ServerMessage {
    const projection = subscription.projection;
    if (!projection || message.type !== "query.result") return message;
    const projected = rowsAtPath(message.result, projection.resultPath);
    if (!projected) return message;
    const materialized = this.overlay.apply(
      subscription.key,
      projection.entity,
      projected.rows as unknown as readonly Row[],
      projection.key,
    ) as unknown as JsonValue[];
    return {
      ...message,
      result: replaceRowsAtPath(message.result, projection.resultPath, materialized, projected.scalar),
    };
  }

  private emitOptimisticEntity(entity: string) {
    for (const subscription of this.syncSubscriptions.values()) {
      if (subscription.entity !== entity) continue;
      this.overlay.expectSource(subscription.key, entity);
      if (subscription.lastMessage?.type !== "sync.snapshot") continue;
      this.emitSyncMessage(subscription, subscription.lastMessage);
    }
    for (const subscription of this.querySubscriptions.values()) {
      if (subscription.projection?.entity !== entity) continue;
      this.overlay.expectSource(subscription.key, entity);
      if (subscription.lastMessage?.type !== "query.result") continue;
      const outgoing = this.materializeQueryMessage(subscription, subscription.lastMessage);
      for (const listener of Array.from(subscription.listeners)) listener(outgoing);
      this.acknowledgeOptimisticQuerySnapshot(subscription, subscription.lastMessage.result);
    }
  }

  private acknowledgeOptimisticSource(source: string, mutationIds: readonly string[] | undefined) {
    const settled = this.overlay.acknowledge(source, mutationIds);
    for (const mutationId of settled) void this.ackOptimisticMutation(mutationId);
  }

  private acknowledgeOptimisticQuerySnapshot(subscription: QuerySubscription, result: JsonValue) {
    const projection = subscription.projection;
    if (!projection) return;
    const projected = rowsAtPath(result, projection.resultPath);
    if (!projected) return;
    const settled = this.overlay.acknowledgeMatching(
      subscription.key,
      projection.entity,
      projected.rows as unknown as readonly Row[],
      projection.key,
    );
    for (const mutationId of settled) void this.ackOptimisticMutation(mutationId);
  }

  private markSyncSubscriptionsOutOfDate() {
    for (const subscription of this.syncSubscriptions.values()) {
      const wasUpToDate = subscription.isUpToDate;
      subscription.verificationGeneration += 1;
      subscription.isUpToDate = false;
      if (!wasUpToDate) continue;
      this.emitSyncMessage(subscription, {
        type: "sync.syncing",
        id: subscription.id,
        path: subscription.path,
        reason: "disconnected",
      });
    }
  }

  private startSync(subscription: SyncSubscription) {
    const directive = this.queryCacheDirective;
    const store = this.syncStore;
    if (!directive) return;
    if (!store) {
      this.sendSyncOpen(subscription);
      return;
    }
    const scope = syncPersistenceScope(directive);
    const generation = this.syncScopeGeneration;
    if (subscription.cacheReadGeneration === generation) return;
    subscription.cacheReadGeneration = generation;
    // The warm read is an optimization with a deadline. If IndexedDB never
    // answers (a wedged Chrome origin store emits no event at all, so no
    // rejection ever fires), open cold after the timeout: a full snapshot
    // beats a permanently empty screen, and a late read result is discarded.
    let cacheReadSettled = false;
    const cacheReadTimer = setTimeout(() => {
      if (cacheReadSettled) return;
      cacheReadSettled = true;
      if (
        this.syncSubscriptions.get(subscription.key) !== subscription
        || this.syncScopeGeneration !== generation
      ) return;
      this.sendSyncOpen(subscription);
    }, syncStoreReadTimeoutMs);
    void store.load(scope, subscription.path, subscription.args).then((cached) => {
      clearTimeout(cacheReadTimer);
      if (cacheReadSettled) return;
      cacheReadSettled = true;
      const currentDirective = this.queryCacheDirective;
      if (
        this.syncSubscriptions.get(subscription.key) !== subscription
        || this.syncScopeGeneration !== generation
        || !currentDirective
        || syncPersistenceScope(currentDirective) !== scope
      ) return;
      if (cached) {
        subscription.isUpToDate = false;
        subscription.rows = cached.rows;
        // These rows came out of the store, so the store already holds them:
        // the ready that follows this resume must not rewrite them.
        subscription.persistedRows = cached.rows;
        subscription.cursor = cached.cursor;
        raiseSyncCursorFloor(subscription, cached.cursor);
        subscription.keyField = cached.keyField;
        subscription.mode = cached.mode;
        subscription.truncated = cached.truncated;
        subscription.orderBy = cached.orderBy;
        subscription.orderDirection = cached.orderDirection;
        subscription.maxRows = cached.maxRows;
        subscription.maxBytes = cached.maxBytes;
        // Stored hash metadata is never trusted. sendSyncOpen hashes these
        // actual materialized rows before advertising a cursor, which allows a
        // corrupt row to be repaired by delta without a full cache reset.
        subscription.hashes = {};
        subscription.integrityDigest = undefined;
        subscription.integrityRows = undefined;
        subscription.integrityEpoch = undefined;
        const message: SyncMessage = {
          type: "sync.snapshot",
          id: subscription.id,
          path: subscription.path,
          result: cached.rows,
          cursor: cached.cursor,
          key: cached.keyField,
          mode: cached.mode,
          orderBy: cached.orderBy,
          orderDirection: cached.orderDirection,
          maxRows: cached.maxRows,
          maxBytes: cached.maxBytes,
        };
        subscription.lastMessage = message;
        this.emitSyncMessage(subscription, message);
      }
      this.sendSyncOpen(subscription);
    }).catch(() => {
      clearTimeout(cacheReadTimer);
      if (cacheReadSettled) return;
      cacheReadSettled = true;
      this.sendSyncOpen(subscription);
    });
  }

  private sendSyncOpen(subscription: SyncSubscription) {
    if (subscription.listeners.size === 0 || subscription.opening) return;
    if (subscription.cursor && subscription.integrityRows !== subscription.rows) {
      subscription.opening = true;
      const rows = subscription.rows;
      const keyField = subscription.keyField;
      const socketGeneration = this.socketGeneration;
      void syncRowsHashes(rows, keyField).then((hashes) => (
        syncHashesDigest(hashes).then((digest) => ({ hashes, digest }))
      )).then(({ hashes, digest }) => {
        if (
          this.socketGeneration !== socketGeneration
          || this.syncSubscriptions.get(subscription.key) !== subscription
          || subscription.listeners.size === 0
          || subscription.rows !== rows
          || subscription.keyField !== keyField
        ) return;
        subscription.hashes = hashes;
        subscription.integrityDigest = digest;
        subscription.integrityRows = rows;
        subscription.integrityEpoch = subscription.cursor?.epoch;
        subscription.opening = false;
        this.sendSyncOpen(subscription);
      }).catch(() => {
        if (
          this.socketGeneration !== socketGeneration
          || this.syncSubscriptions.get(subscription.key) !== subscription
          || subscription.rows !== rows
        ) return;
        subscription.opening = false;
        this.handleSyncMessage(subscription, {
          type: "sync.reset",
          id: subscription.id,
          path: subscription.path,
          reason: "integrity-mismatch",
        });
      });
      return;
    }
    subscription.opening = true;
    subscription.socketGeneration = this.socketGeneration;
    const open = this.syncOpenRequest(subscription);
    if (this.serverCapabilities.syncBatch === 1) {
      this.pendingSyncOpens.add(subscription);
      if (!this.syncOpenFlushTimer) {
        this.syncOpenFlushTimer = setTimeout(() => this.flushSyncOpens(), 0);
      }
      return;
    }
    this.send({ type: "sync.open", ...open });
  }

  private syncOpenRequest(subscription: SyncSubscription): SyncOpenRequest {
    const fullIntegrity = subscription.cursor !== undefined && (
      subscription.forceFullIntegrity
      || !subscription.integrityDigest
      || subscription.rows.length <= compactSyncIntegrityThreshold
    );
    const keys = fullIntegrity
      ? subscription.rows.map((row) => syncRowKey(row, subscription.keyField)).filter(Boolean)
      : undefined;
    return {
      id: subscription.id,
      path: subscription.path,
      args: subscription.args,
      cursor: subscription.cursor,
      keys,
      hashes: fullIntegrity && Object.keys(subscription.hashes).length > 0
        ? subscription.hashes
        : undefined,
      digest: subscription.cursor ? subscription.integrityDigest : undefined,
      fullIntegrity: fullIntegrity || undefined,
    };
  }

  private flushSyncOpens() {
    this.syncOpenFlushTimer = undefined;
    const subscriptions = Array.from(this.pendingSyncOpens);
    this.pendingSyncOpens.clear();
    const opens = subscriptions
      .filter((subscription) => (
        subscription.opening
        && subscription.listeners.size > 0
        && this.syncSubscriptions.get(subscription.key) === subscription
      ))
      .map((subscription) => this.syncOpenRequest(subscription));
    for (let offset = 0; offset < opens.length; offset += maxSyncBatchOpens) {
      this.send({ type: "sync.openMany", opens: opens.slice(offset, offset + maxSyncBatchOpens) });
    }
  }

  private unsubscribeSyncListener(key: string, listener: SyncSubscriptionHandler) {
    const subscription = this.syncSubscriptions.get(key);
    if (!subscription) return;
    subscription.listeners.delete(listener);
    if (subscription.listeners.size > 0 || subscription.unsubscribeTimer) return;
    subscription.unsubscribeTimer = setTimeout(() => {
      const latest = this.syncSubscriptions.get(key);
      if (!latest || latest.listeners.size > 0) return;
      latest.unsubscribeTimer = undefined;
      this.clearSyncRetry(latest);
      this.pendingSyncOpens.delete(latest);
      this.syncSubscriptions.delete(key);
      for (const mutationId of this.overlay.removeSource(key)) void this.ackOptimisticMutation(mutationId);
      this.handlers.delete(latest.id);
      this.send({ type: "sync.close", id: latest.id });
    }, this.syncSubscriptionRetentionMs);
  }

  private persistSyncSnapshot(subscription: SyncSubscription, fromWatermark = false) {
    if (!fromWatermark && subscription.watermarkPersistTimer) {
      clearTimeout(subscription.watermarkPersistTimer);
      subscription.watermarkPersistTimer = undefined;
    }
    const directive = this.queryCacheDirective;
    const store = this.syncStore;
    if (!directive || !store || !subscription.cursor) return;
    const scope = syncPersistenceScope(directive);
    // sync.ready arrives for every collection on every reload, almost always
    // with the rows the store already holds. Persist the advancing cursor but
    // leave the rows alone unless they actually changed.
    const rowsUnchanged = subscription.persistedRows === subscription.rows;
    const value = {
      rows: subscription.rows,
      cursor: subscription.cursor,
      keyField: subscription.keyField,
      mode: subscription.mode,
      truncated: subscription.truncated,
      orderBy: subscription.orderBy,
      orderDirection: subscription.orderDirection,
      maxRows: subscription.maxRows,
      maxBytes: subscription.maxBytes,
      hashes: { ...subscription.hashes },
      rowsUnchanged,
    };
    subscription.persistedRows = subscription.rows;
    this.enqueueSyncPersistence(
      subscription,
      scope,
      () => store.replace(scope, subscription.path, subscription.args, value),
    );
  }

  private persistSyncDelta(subscription: SyncSubscription, upserts: JsonValue[], deleted: string[]) {
    if (subscription.watermarkPersistTimer) {
      clearTimeout(subscription.watermarkPersistTimer);
      subscription.watermarkPersistTimer = undefined;
    }
    const directive = this.queryCacheDirective;
    const store = this.syncStore;
    if (!directive || !store || !subscription.cursor) return;
    const scope = syncPersistenceScope(directive);
    const value = {
      cursor: subscription.cursor,
      keyField: subscription.keyField,
      mode: subscription.mode,
      truncated: subscription.truncated,
      orderBy: subscription.orderBy,
      orderDirection: subscription.orderDirection,
      upserts,
      deleted,
      maxRows: subscription.maxRows,
      maxBytes: subscription.maxBytes,
      hashes: { ...subscription.hashes },
    };
    // The delta brings the stored rows to exactly these in-memory rows, so the
    // sync.ready that closes this batch must not rewrite the whole collection.
    subscription.persistedRows = subscription.rows;
    this.enqueueSyncPersistence(
      subscription,
      scope,
      () => store.applyDelta(scope, subscription.path, subscription.args, value),
    );
  }

  private activateOutboxScope(): Promise<void> {
    const scope = mutationOutboxScope(this.url, this.auth, this.outboxEphemeralScope);
    if (scope === this.outboxScope) return this.outboxReady ?? Promise.resolve();

    const previousScope = this.outboxScope;
    const generation = ++this.outboxScopeGeneration;
    // Pending state from the previous authenticated identity must disappear
    // from every live projection immediately. Its durable rows remain scoped
    // in IndexedDB and can be resumed only if that identity returns.
    for (const mutationId of this.optimisticMutationIds) this.overlay.reject(mutationId);
    this.optimisticMutationIds.clear();
    this.optimisticOutboxEntryIds.clear();
    if (isEphemeralOutboxScope(previousScope)) {
      void this.mutationOutbox.clear(previousScope);
    }
    this.outboxScope = scope;
    const ready = this.restoreOutbox(scope, generation);
    this.outboxReady = ready;
    return ready;
  }

  private async restoreOutbox(scope: string, generation: number) {
    const entries = await this.mutationOutbox.loadAll(scope);
    if (
      this.manuallyClosed
      || generation !== this.outboxScopeGeneration
      || scope !== this.outboxScope
    ) return;
    for (const entry of entries) {
      if (entry.state === "committed" && (entry.patches?.length ?? 0) === 0) {
        await this.mutationOutbox.ack(entry.id);
        continue;
      }
      this.optimisticOutboxEntryIds.set(entry.idempotencyKey, entry.id);
      this.addOptimisticMutation(entry.idempotencyKey, entry.patches ?? [], entry.state === "committed");
    }
    const nextAttemptAt = Math.min(
      ...entries
        .filter((entry) => entry.state === "pending")
        .map((entry) => entry.nextAttemptAt),
    );
    if (Number.isFinite(nextAttemptAt) && nextAttemptAt > Date.now()) {
      this.scheduleOutboxDrain(nextAttemptAt - Date.now());
    }
    // If this scope was installed after the socket authenticated, no reconnect
    // or new enqueue may occur to wake the queue. The await inside drainOutbox
    // yields until this restore promise resolves, then safely resumes it.
    void this.drainOutbox();
  }

  private addOptimisticMutation(mutationId: string, patches: OptimisticPatch[], accepted = false) {
    if (patches.length === 0 || this.optimisticMutationIds.has(mutationId)) return;
    this.optimisticMutationIds.add(mutationId);
    this.overlay.add(mutationId, patches, { accepted });
  }

  private async settleOptimisticMutation(mutationId: string) {
    await Promise.all(
      this.overlay.accept(mutationId).map((settledId) => this.ackOptimisticMutation(settledId)),
    );
  }

  private async rejectOptimisticMutation(mutationId: string, knownEntryId?: number) {
    this.optimisticMutationIds.delete(mutationId);
    this.overlay.reject(mutationId);
    await this.ackOptimisticMutation(mutationId, knownEntryId);
  }

  private async ackOptimisticMutation(mutationId: string, knownEntryId?: number) {
    const entryId = knownEntryId ?? this.optimisticOutboxEntryIds.get(mutationId);
    this.optimisticOutboxEntryIds.delete(mutationId);
    this.optimisticMutationIds.delete(mutationId);
    if (entryId !== undefined) await this.mutationOutbox.ack(entryId);
  }

  private async drainOutbox() {
    await this.outboxReady;
    if (
      this.drainingOutbox
      || this.manuallyClosed
      || !this.socket
      || this.socket.readyState !== WebSocket.OPEN
    ) return;
    const drainScope = this.outboxScope;
    this.drainingOutbox = true;
    try {
      while (!this.manuallyClosed && this.socket?.readyState === WebSocket.OPEN) {
        const scope = this.outboxScope;
        const entry = await this.mutationOutbox.nextReady(scope, Date.now());
        if (!entry) return;
        if (scope !== this.outboxScope) return;
        await this.mutationOutbox.markInflight(entry.id);
        if (scope !== this.outboxScope) return;
        try {
          await this.call(
            "mutation",
            { kind: "mutation", path: entry.path },
            entry.args as JsonValue,
            this.timeouts.mutationTimeoutMs,
            entry.idempotencyKey,
            entry.idempotencyKey,
          );
          await this.mutationOutbox.markCommitted(entry.id);
          if ((entry.patches?.length ?? 0) > 0) {
            await this.settleOptimisticMutation(entry.idempotencyKey);
          } else {
            await this.ackOptimisticMutation(entry.idempotencyKey, entry.id);
          }
        } catch (error) {
          if (error instanceof GonvexClientError && error.code === "server") {
            await this.rejectOptimisticMutation(entry.idempotencyKey, entry.id);
            continue;
          }
          await this.mutationOutbox.fail(entry.id, mutationErrorMessage(error));
          this.scheduleOutboxDrain(Math.min(30_000, 1_000 * (2 ** (entry.attempts + 1))));
          return;
        }
      }
    } finally {
      this.drainingOutbox = false;
      if (!this.manuallyClosed && drainScope !== this.outboxScope) {
        void this.drainOutbox();
      }
    }
  }

  private scheduleOutboxDrain(delay: number) {
    if (this.manuallyClosed) return;
    if (this.outboxDrainTimer) clearTimeout(this.outboxDrainTimer);
    this.outboxDrainTimer = setTimeout(() => {
      this.outboxDrainTimer = undefined;
      void this.drainOutbox();
    }, delay);
  }

  mutation<T = JsonValue>(
    ref: FunctionReference,
    args: JsonValue,
    options: CallOptions & { offline: "queue" },
  ): Promise<T | QueuedMutationOutcome>;
  mutation<T = JsonValue>(ref: FunctionReference, args?: JsonValue, options?: CallOptions): Promise<T>;
  mutation<T = JsonValue>(
    ref: FunctionReference,
    args: JsonValue = {},
    options: CallOptions = {},
  ): Promise<T | QueuedMutationOutcome> {
    const mutationId = randomID();
    const patches = options.optimistic
      ?? optimisticPatchesFromReference(ref.optimistic?.mutation, args);
    if (patches.length === 0 && options.offline !== "queue") {
      return this.call<T>(
        "mutation",
        ref,
        args,
        options.timeoutMs ?? this.timeouts.mutationTimeoutMs,
        mutationId,
      );
    }
    return this.runOptimisticMutation<T>(ref, args, options, mutationId, patches);
  }

  private async runOptimisticMutation<T>(
    ref: FunctionReference,
    args: JsonValue,
    options: CallOptions,
    mutationId: string,
    patches: OptimisticPatch[],
  ): Promise<T | QueuedMutationOutcome> {
    // The startup recovery transaction converts abandoned inflight entries to
    // pending. Finish it before inserting a brand-new direct send, otherwise
    // recovery can mistake that live entry for a crashed mutation and race the
    // direct call through the background drain.
    await this.outboxReady;
    if (this.manuallyClosed) {
      throw new GonvexClientError(
        `Gonvex client was closed before mutation ${ref.path} could be sent.`,
        { code: "closed", path: ref.path, operation: "mutation" },
      );
    }
    const scope = this.outboxScope;
    const entry = await this.mutationOutbox.enqueue({
      scope,
      path: ref.path,
      args,
      idempotencyKey: mutationId,
      entityKeys: patches.map((patch) => `${patch.entity ?? patch.collection ?? ""}:${patch.rowId}`),
      patches,
      state: "inflight",
    });
    if (this.manuallyClosed) {
      await this.mutationOutbox.ack(entry.id);
      throw new GonvexClientError(
        `Gonvex client was closed before mutation ${ref.path} could be sent.`,
        { code: "closed", path: ref.path, operation: "mutation" },
      );
    }
    if (scope !== this.outboxScope) {
      await this.mutationOutbox.ack(entry.id);
      throw new GonvexClientError(
        `Authentication changed before mutation ${ref.path} could be sent.`,
        { code: "disconnected", path: ref.path, operation: "mutation" },
      );
    }
    this.optimisticOutboxEntryIds.set(mutationId, entry.id);
    this.addOptimisticMutation(mutationId, patches);
    try {
      // The direct send is outbox-managed: a crash here replays the entry
      // with the same idempotency key, so the server must dedupe it.
      const result = await this.call<T>(
        "mutation",
        ref,
        args,
        options.timeoutMs ?? this.timeouts.mutationTimeoutMs,
        mutationId,
        mutationId,
      );
      await this.mutationOutbox.markCommitted(entry.id);
      if (patches.length > 0) {
        await this.settleOptimisticMutation(mutationId);
      } else {
        await this.ackOptimisticMutation(mutationId, entry.id);
      }
      return result;
    } catch (error: unknown) {
      if (isQueueableMutationError(error) && options.offline === "queue") {
        await this.mutationOutbox.fail(entry.id, mutationErrorMessage(error));
        return { status: "queued", mutationId };
      }
      await this.rejectOptimisticMutation(mutationId, entry.id);
      throw error;
    }
  }

  action<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}, options: CallOptions = {}): Promise<T> {
    return this.call<T>("action", ref, args, options.timeoutMs ?? this.timeouts.actionTimeoutMs);
  }

  query<T = JsonValue>(ref: FunctionReference, args: JsonValue = {}, options: CallOptions = {}): Promise<T> {
    this.connect();
    const id = randomID();
    const timeoutMs = options.timeoutMs ?? this.timeouts.queryTimeoutMs;
    return new Promise<T>((resolve, reject) => {
      const query: OneShotQuery = { id, path: ref.path, args, reject };
      const settle = () => {
        if (query.timeoutTimer) clearTimeout(query.timeoutTimer);
        this.oneShotQueries.delete(id);
        this.handlers.delete(id);
        this.notifyConnectionState();
      };
      if (timeoutMs > 0) {
        query.timeoutTimer = setTimeout(() => {
          settle();
          this.send({ type: "query.unsubscribe", id });
          reject(new GonvexClientError(
            `Query ${ref.path} timed out after ${timeoutMs}ms`,
            { code: "timeout", path: ref.path, operation: "query" },
          ));
        }, timeoutMs);
      }
      this.oneShotQueries.set(id, query);
      this.handlers.set(id, (message) => {
        if (message.type === "query.result") {
          settle();
          this.recordTelemetry({
            type: "query",
            id: message.id,
            path: ref.path,
            reason: message.reason,
            outcome: "ok",
            clientReceivedAtMs: nowMs(),
            serverTrace: message.trace,
          });
          this.send({ type: "query.unsubscribe", id });
          resolve(message.result as T);
        }
        if (message.type === "query.error") {
          settle();
          this.recordTelemetry({
            type: "query",
            id: message.id,
            path: ref.path,
            outcome: "error",
            error: message.error,
            clientReceivedAtMs: nowMs(),
          });
          this.send({ type: "query.unsubscribe", id });
          reject(new GonvexClientError(message.error, { code: "server", path: ref.path, operation: "query" }));
        }
      });
      this.sendOneShotQuery(query);
      this.notifyConnectionState();
    });
  }

  /**
   * Force a live query subscription to re-request its result from the server,
   * e.g. after a `query.error` or when a subscriber gave up waiting. No-op if
   * nothing is subscribed to this query.
   */
  retryQuery(ref: FunctionReference, args: JsonValue = {}) {
    const subscription = this.querySubscriptions.get(querySubscriptionKey(ref, args));
    if (!subscription || subscription.listeners.size === 0) return;
    subscription.serverSettled = false;
    subscription.socketGeneration = undefined;
    this.connect();
    this.sendSubscription(subscription);
  }

  /**
   * Flush a queue of mutations in one `mutation.callMany` frame (queue order,
   * one websocket round trip). Each entry settles independently — a failed
   * call does not reject the batch — so offline queues can apply per-row
   * outcomes. Falls back to the standard per-mutation path when the runtime
   * lacks batching or when a call needs generated/explicit optimism or durable
   * offline queuing, so there is never a second mutation-state implementation.
   */
  async mutationMany<T = JsonValue>(
    calls: Array<{ ref: FunctionReference; args?: JsonValue }>,
    options: CallOptions = {},
  ): Promise<Array<{ status: "ok"; result: T } | { status: "error"; error: GonvexClientError }>> {
    if (calls.length === 0) return [];
    this.connect();
    const timeoutMs = options.timeoutMs ?? this.timeouts.mutationTimeoutMs;
    const settle = (promise: Promise<T>, path: string) => promise
      .then((result) => ({ status: "ok" as const, result }))
      .catch((error: unknown) => ({
        status: "error" as const,
        error: error instanceof GonvexClientError
          ? error
          : new GonvexClientError(String(error), { code: "server", path, operation: "mutation" }),
      }));
    const requiresStandardMutationPath = options.offline === "queue"
      || options.optimistic !== undefined
      || calls.some((call) => call.ref.optimistic?.mutation !== undefined);
    if (this.serverCapabilities.mutationBatch !== 1 || requiresStandardMutationPath) {
      const outcomes: Array<{ status: "ok"; result: T } | { status: "error"; error: GonvexClientError }> = [];
      for (const call of calls) {
        outcomes.push(await settle(this.mutation<T>(call.ref, call.args ?? {}, options), call.ref.path));
      }
      return outcomes;
    }
    const registered = calls.map((call) => {
      const entry = this.registerCall<T>("mutation", call.ref, call.args ?? {}, timeoutMs);
      return { ...entry, path: call.ref.path, args: call.args ?? {} };
    });
    for (let offset = 0; offset < registered.length; offset += maxSyncBatchOpens) {
      this.send({
        type: "mutation.callMany",
        calls: registered.slice(offset, offset + maxSyncBatchOpens).map((entry) => ({
          id: entry.id,
          path: entry.path,
          args: entry.args,
          trace: { clientSentAtMs: entry.clientSentAtMs },
        })),
      });
    }
    this.notifyConnectionState();
    return Promise.all(registered.map((entry) => settle(entry.promise, entry.path)));
  }

  private call<T>(
    kind: "mutation" | "action",
    ref: FunctionReference,
    args: JsonValue,
    timeoutMs: number,
    id?: string,
    idempotencyKey?: string,
  ): Promise<T> {
    this.connect();
    const entry = this.registerCall<T>(kind, ref, args, timeoutMs, id);
    if (kind === "mutation") {
      try { const w=(globalThis as any); if (w && w.__wsTapLog) w.__wsTapLog.push({ dir:"mut-args", type:"mutation.call", path: ref.path, argTenant: ((args as any)&&(args as any).tenantId)||null, authTenant: (this as any).auth?.tenant||null, authProject:(this as any).auth?.project||null, href: (w.location&&w.location.href)||null }); } catch(e){}
      this.send({
        type: "mutation.call",
        id: entry.id,
        path: ref.path,
        args,
        trace: { clientSentAtMs: entry.clientSentAtMs },
        ...(idempotencyKey ? { idempotencyKey } : {}),
      });
    } else {
      this.send({ type: "action.call", id: entry.id, path: ref.path, args, trace: { clientSentAtMs: entry.clientSentAtMs } });
    }
    this.notifyConnectionState();
    return entry.promise;
  }

  private registerCall<T>(kind: "mutation" | "action", ref: FunctionReference, args: JsonValue, timeoutMs: number, callId = randomID()): { id: string; clientSentAtMs: number; promise: Promise<T> } {
    const id = callId;
    const clientSentAtMs = nowMs();
    const promise = new Promise<T>((resolve, reject) => {
      const pending: PendingCall = { id, kind, path: ref.path, reject };
      const settle = () => {
        if (pending.timeoutTimer) clearTimeout(pending.timeoutTimer);
        this.pendingCalls.delete(id);
        this.handlers.delete(id);
        this.notifyConnectionState();
      };
      if (timeoutMs > 0) {
        pending.timeoutTimer = setTimeout(() => {
          settle();
          reject(new GonvexClientError(
            `${kind === "mutation" ? "Mutation" : "Action"} ${ref.path} timed out after ${timeoutMs}ms. The operation may or may not have been applied.`,
            { code: "timeout", path: ref.path, operation: kind },
          ));
        }, timeoutMs);
      }
      this.pendingCalls.set(id, pending);
      this.handlers.set(id, (message) => {
        if (kind === "mutation" && message.type === "mutation.result") {
          settle();
          this.emitTelemetryFromCall(kind, id, ref.path, "ok", clientSentAtMs, message.trace);
          resolve(message.result as T);
        }
        if (kind === "mutation" && message.type === "mutation.error") {
          settle();
          this.emitTelemetryFromCall(kind, id, ref.path, "error", clientSentAtMs, message.trace, message.error);
          reject(new GonvexClientError(message.error, { code: "server", path: ref.path, operation: kind }));
        }
        if (kind === "action" && message.type === "action.result") {
          settle();
          this.emitTelemetryFromCall(kind, id, ref.path, "ok", clientSentAtMs, message.trace);
          resolve(message.result as T);
        }
        if (kind === "action" && message.type === "action.error") {
          settle();
          this.emitTelemetryFromCall(kind, id, ref.path, "error", clientSentAtMs, message.trace, message.error);
          reject(new GonvexClientError(message.error, { code: "server", path: ref.path, operation: kind }));
        }
      });
    });
    return { id, clientSentAtMs, promise };
  }

  private unsubscribeQueryListener(key: string, listener: SubscriptionHandler) {
    const subscription = this.querySubscriptions.get(key);
    if (!subscription) return;
    subscription.listeners.delete(listener);
    if (subscription.listeners.size > 0 || subscription.unsubscribeTimer) return;

    // React can briefly unmount/remount the same hook during route transitions,
    // StrictMode, or error-boundary recovery. Holding the server subscription for
    // one tick prevents unsubscribe/subscribe ping-pong while still cleaning up
    // abandoned subscriptions promptly.
    subscription.unsubscribeTimer = setTimeout(() => {
      const latest = this.querySubscriptions.get(key);
      if (!latest || latest.listeners.size > 0) return;
      this.querySubscriptions.delete(key);
      for (const mutationId of this.overlay.removeSource(key)) void this.ackOptimisticMutation(mutationId);
      this.send({ type: "query.unsubscribe", id: latest.id });
      setTimeout(() => this.handlers.delete(latest.id), 500);
    }, this.querySubscriptionRetentionMs);
  }

  private sendSubscription(subscription: QuerySubscription) {
    if (subscription.listeners.size === 0) return;
    if (subscription.socketGeneration === this.socketGeneration) return;
    if (this.queryCache && this.queryCacheWaitForScope) {
      if (!this.queryCacheDirective) {
        if (this.queryCacheNegotiatedSocketGeneration !== this.socketGeneration) return;
      } else if (subscription.cacheReadGeneration !== this.queryCacheGeneration) {
        this.startQueryCacheRead(subscription);
        return;
      } else if (subscription.cacheReadPromise) {
        return;
      }
    }
    subscription.socketGeneration = this.socketGeneration;
    // Route reloads register dozens of live queries at once. Collapse the
    // burst into one batched frame per tick instead of one frame per query.
    if (this.serverCapabilities.queryBatch === 1) {
      this.pendingQuerySubscribes.add(subscription);
      if (!this.querySubscribeFlushTimer) {
        this.querySubscribeFlushTimer = setTimeout(() => this.flushQuerySubscribes(), 0);
      }
      return;
    }
    this.send({
      type: "query.subscribe",
      id: subscription.id,
      path: subscription.path,
      args: subscription.args,
      cacheRevision: subscription.cachedRevision,
    });
  }

  private flushQuerySubscribes() {
    this.querySubscribeFlushTimer = undefined;
    const subscriptions = Array.from(this.pendingQuerySubscribes);
    this.pendingQuerySubscribes.clear();
    const subscribes = subscriptions
      .filter((subscription) => (
        subscription.listeners.size > 0
        && subscription.socketGeneration === this.socketGeneration
        && this.querySubscriptions.get(subscription.key) === subscription
      ))
      .map((subscription) => ({
        id: subscription.id,
        path: subscription.path,
        args: subscription.args,
        cacheRevision: subscription.cachedRevision,
      }));
    for (let offset = 0; offset < subscribes.length; offset += maxSyncBatchOpens) {
      this.send({ type: "query.subscribeMany", subscribes: subscribes.slice(offset, offset + maxSyncBatchOpens) });
    }
  }

  private resumeQuerySubscriptions() {
    for (const subscription of this.querySubscriptions.values()) {
      if (subscription.listeners.size === 0) continue;
      this.sendSubscription(subscription);
    }
  }

  private enqueueSyncPersistence(
    subscription: SyncSubscription,
    scope: string,
    operation: () => Promise<void>,
  ) {
    const key = `${scope}\u0000${subscription.key}`;
    const previous = this.syncPersistence.get(key) ?? Promise.resolve();
    const pending = previous
      .catch(() => undefined)
      .then(operation)
      .catch(() => undefined);
    this.syncPersistence.set(key, pending);
    subscription.persistence = pending;
    void pending.finally(() => {
      if (this.syncPersistence.get(key) === pending) this.syncPersistence.delete(key);
    });
  }

  private scheduleSyncRetry(subscription: SyncSubscription) {
    if (
      this.manuallyClosed
      || subscription.retryTimer
      || subscription.listeners.size === 0
      || this.syncSubscriptions.get(subscription.key) !== subscription
    ) return;
    const delay = Math.min(250 * (2 ** subscription.retryAttempt), 5_000);
    subscription.retryAttempt += 1;
    subscription.retryTimer = setTimeout(() => {
      subscription.retryTimer = undefined;
      if (
        this.manuallyClosed
        || !this.isWebSocketConnected
        || subscription.listeners.size === 0
        || this.syncSubscriptions.get(subscription.key) !== subscription
      ) return;
      subscription.opening = false;
      this.sendSyncOpen(subscription);
    }, delay);
  }

  private clearSyncRetry(subscription: SyncSubscription, resetAttempt = false) {
    if (subscription.retryTimer) {
      clearTimeout(subscription.retryTimer);
      subscription.retryTimer = undefined;
    }
    if (resetAttempt) subscription.retryAttempt = 0;
  }

  private requestSubscriptionSnapshot(subscription: QuerySubscription) {
    // Do not advertise the cache revision while recovering. Otherwise the
    // runtime can answer with another progress frame instead of a snapshot.
    subscription.cachedRevision = undefined;
    subscription.serverSettled = false;
    subscription.socketGeneration = undefined;
    this.sendSubscription(subscription);
  }

  private sendOneShotQuery(query: OneShotQuery) {
    if (query.socketGeneration === this.socketGeneration) return;
    query.socketGeneration = this.socketGeneration;
    this.send({ type: "query.subscribe", id: query.id, path: query.path, args: query.args });
  }

  private resubscribeQueries(generation: number) {
    if (generation !== this.socketGeneration) return;
    for (const subscription of this.querySubscriptions.values()) {
      if (subscription.listeners.size === 0) continue;
      subscription.serverSettled = false;
      this.sendSubscription(subscription);
    }
    for (const query of this.oneShotQueries.values()) {
      this.sendOneShotQuery(query);
    }
    for (const subscription of this.syncSubscriptions.values()) {
      if (subscription.listeners.size === 0) continue;
      this.clearSyncRetry(subscription, true);
      subscription.opening = false;
      subscription.socketGeneration = undefined;
      this.sendSyncOpen(subscription);
    }
  }

  private scheduleReconnect() {
    if (this.manuallyClosed || this.reconnectTimer) return;
    const delay = Math.min(250 * (2 ** this.reconnectAttempt), 5_000);
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      if (!this.manuallyClosed) {
        this.connect();
        this.notifyConnectionState();
      }
    }, delay);
  }

  private installQueryCacheDirective(value: QueryCacheDirective | undefined) {
    if (!validQueryCacheDirective(value)) {
      if (this.queryCacheDirective) this.resetQueryCacheScope();
      return;
    }
    const previous = this.queryCacheDirective;
    const syncScopeChanged = previous !== undefined
      && syncPersistenceScope(previous) !== syncPersistenceScope(value);
    if (previous?.scope === value.scope && !syncScopeChanged) {
      this.queryCacheDirective = value;
      return;
    }
    if (previous) {
      // A deploy rotates the query-result scope (results depend on code), but
      // sync collections are keyed by visibility and survive it: their rows,
      // cursors, and in-flight warm reads stay valid and are verified by the
      // server's reconcile on the next open.
      this.resetQueryResultCacheState();
      if (syncScopeChanged) this.resetSyncCacheState();
    }
    this.queryCacheDirective = value;
    const identity = authIdentityKey(this.auth);
    if (identity) void this.syncStore?.saveDirective(identity, value).catch(() => undefined);
    for (const subscription of this.querySubscriptions.values()) {
      this.startQueryCacheRead(subscription);
    }
    for (const subscription of this.syncSubscriptions.values()) {
      this.startSync(subscription);
    }
  }

  private recoverWarmSyncDirective() {
    const store = this.syncStore;
    const identity = authIdentityKey(this.auth);
    const generation = ++this.syncIdentityGeneration;
    if (!store || !identity) return;
    // Same deadline as the warm collection reads: a hung IndexedDB must not
    // stall directive recovery — the server's auth.result supplies it anyway.
    const abandonTimer = setTimeout(() => {
      // Only invalidate this recovery — a newer setAuth may already own the
      // current generation.
      if (generation === this.syncIdentityGeneration) this.syncIdentityGeneration += 1;
    }, syncStoreReadTimeoutMs);
    void store.loadDirective(identity).then((directive) => {
      clearTimeout(abandonTimer);
      if (
        generation !== this.syncIdentityGeneration
        || authIdentityKey(this.auth) !== identity
        || this.queryCacheDirective
        || !validQueryCacheDirective(directive)
      ) return;
      this.installQueryCacheDirective(directive);
    }).catch(() => {
      clearTimeout(abandonTimer);
    });
  }

  private resetQueryCacheScope() {
    const hadScope = this.queryCacheDirective !== undefined;
    this.queryCacheDirective = undefined;
    this.resetQueryResultCacheState();
    this.resetSyncCacheState();
    if (hadScope || this.querySubscriptions.size > 0 || this.syncSubscriptions.size > 0) {
      for (const handler of this.sessionScopeHandlers) handler();
    }
  }

  private resetQueryResultCacheState() {
    this.queryCacheGeneration += 1;
    this.queryCacheNegotiatedSocketGeneration = undefined;
    for (const subscription of this.querySubscriptions.values()) {
      subscription.lastMessage = undefined;
      subscription.serverSettled = false;
      subscription.cacheReadGeneration = undefined;
      subscription.cacheReadPromise = undefined;
      if (subscription.cacheReadFallbackTimer) clearTimeout(subscription.cacheReadFallbackTimer);
      subscription.cacheReadFallbackTimer = undefined;
      subscription.cachedRevision = undefined;
    }
  }

  private resetSyncCacheState() {
    this.syncScopeGeneration += 1;
    for (const subscription of this.syncSubscriptions.values()) {
      this.clearSyncRetry(subscription, true);
      if (subscription.watermarkPersistTimer) {
        clearTimeout(subscription.watermarkPersistTimer);
        subscription.watermarkPersistTimer = undefined;
      }
      subscription.isUpToDate = false;
      subscription.rows = [];
      subscription.persistedRows = undefined;
      subscription.hashes = {};
      subscription.integrityDigest = undefined;
      subscription.integrityRows = undefined;
      subscription.integrityEpoch = undefined;
      subscription.forceFullIntegrity = false;
      subscription.cursor = undefined;
      subscription.cursorFloor = undefined;
      subscription.retiredEpochs.clear();
      subscription.lastMessage = undefined;
      subscription.cacheReadGeneration = undefined;
      subscription.opening = false;
      subscription.verificationGeneration += 1;
    }
  }

  private startQueryCacheRead(subscription: QuerySubscription) {
    const store = this.queryCache;
    const directive = this.queryCacheDirective;
    if (!store || !directive || subscription.serverSettled) return;
    const generation = this.queryCacheGeneration;
    if (subscription.cacheReadGeneration === generation) return;
    subscription.cacheReadGeneration = generation;
    const read = store.read(directive.scope, subscription.path, subscription.args, directive.maxAgeMs).then((cached) => {
      const current = this.querySubscriptions.get(subscription.key);
      if (
        current !== subscription
        || subscription.serverSettled
        || subscription.listeners.size === 0
        || this.queryCacheGeneration !== generation
        || this.queryCacheDirective?.scope !== directive.scope
      ) {
        return;
      }
      subscription.cachedRevision = cached?.revision;
      if (!cached) return;
      const message: ServerMessage = {
        type: "query.result",
        id: subscription.id,
        path: subscription.path,
        result: cached.result,
        reason: "initial",
        cacheScope: directive.scope,
        cacheRevision: cached.revision,
      };
      subscription.lastMessage = message;
      const outgoing = this.materializeQueryMessage(subscription, message);
      for (const listener of Array.from(subscription.listeners)) {
        listener(outgoing);
      }
      this.acknowledgeOptimisticQuerySnapshot(subscription, message.result);
    }).catch(() => {
      // Persistent cache failures never affect the server query path.
    }).finally(() => {
      if (subscription.cacheReadFallbackTimer) clearTimeout(subscription.cacheReadFallbackTimer);
      subscription.cacheReadFallbackTimer = undefined;
      if (subscription.cacheReadPromise === read) subscription.cacheReadPromise = undefined;
      if (
        this.querySubscriptions.get(subscription.key) === subscription
        && subscription.listeners.size > 0
        && this.queryCacheGeneration === generation
        && this.queryCacheDirective?.scope === directive.scope
      ) {
        this.sendSubscription(subscription);
      }
    });
    subscription.cacheReadPromise = read;
    subscription.cacheReadFallbackTimer = setTimeout(() => {
      if (subscription.cacheReadPromise !== read) return;
      subscription.cacheReadPromise = undefined;
      subscription.cacheReadFallbackTimer = undefined;
      this.sendSubscription(subscription);
    }, this.queryCacheReadTimeoutMs);
  }

  private persistQueryResult(subscription: QuerySubscription, message: Extract<ServerMessage, { type: "query.result" }>) {
    const store = this.queryCache;
    const directive = this.queryCacheDirective;
    if (
      !store
      || !directive
      || message.cacheScope !== directive.scope
      || !message.cacheRevision
    ) {
      return;
    }
    const generation = this.queryCacheGeneration;
    queueMicrotask(() => {
      if (this.queryCacheGeneration !== generation || this.queryCacheDirective?.scope !== directive.scope) return;
      void store.write({
        scope: directive.scope,
        path: subscription.path,
        args: subscription.args,
        result: message.result,
        revision: message.cacheRevision!,
        maxAgeMs: directive.maxAgeMs,
      }).catch(() => undefined);
    });
    subscription.cachedRevision = message.cacheRevision;
  }

  private deleteCachedQuery(subscription: QuerySubscription) {
    const store = this.queryCache;
    const directive = this.queryCacheDirective;
    if (!store || !directive) return;
    void store.delete(directive.scope, subscription.path, subscription.args).catch(() => undefined);
  }

  private emitTelemetryFromCall(
    kind: "mutation" | "action",
    id: string,
    path: string,
    outcome: "ok" | "error",
    clientSentAtMs: number,
    serverTrace: MessageTrace | undefined,
    error?: string,
  ) {
    const clientReceivedAtMs = nowMs();
    this.recordTelemetry({
      type: kind,
      id,
      path,
      outcome,
      error,
      clientSentAtMs,
      clientReceivedAtMs,
      clientDurationMs: clientReceivedAtMs - clientSentAtMs,
      serverTrace,
    });
  }

  private recordTelemetry(event: GonvexTelemetryEvent) {
    this.emitTelemetry(event);
    if (event.outcome === "error") {
      this.errorReporter?.captureException(new Error(event.error || `${event.type} failed`), {
        gonvexOperation: { type: event.type, path: event.path, operationId: event.id, reason: event.reason },
        serverTrace: event.serverTrace,
      });
    }
    if (this.telemetryEnabled) {
      this.reportTelemetry(event);
    }
  }

  private emitTelemetry(event: GonvexTelemetryEvent) {
    for (const handler of this.telemetryHandlers) {
      handler(event);
    }
  }

  private reportTelemetry(event: GonvexTelemetryEvent) {
    this.send({
      type: "telemetry.event",
      id: event.id,
      kind: event.type,
      path: event.path,
      reason: event.reason,
      outcome: event.outcome,
      error: event.error,
      clientSentAtMs: event.clientSentAtMs,
      clientReceivedAtMs: event.clientReceivedAtMs,
      clientDurationMs: event.clientDurationMs,
      trace: event.serverTrace,
      device: {
        ...(event.device ?? browserTelemetryInfo()),
        deliveryDiagnostics: this.deliveryDiagnostics.snapshot(this.socket?.bufferedAmount ?? 0),
      },
    });
  }

  private sendAuth(force: boolean, options: { useFetcher?: boolean } = {}) {
    if (!force && !this.auth.token && !this.auth.tenant && !this.auth.project && !this.auth.fetchToken) return;
    this.authInFlight = true;
    this.authRetriedAfterError = false;
    this.armAuthWatchdog();
    const fetcher = this.auth.fetchToken;
    if (fetcher && options.useFetcher !== false) {
      void this.fetchAndSendAuth(fetcher);
      return;
    }
    this.sendAuthFrame();
  }

  private sendAuthFrame() {
    this.sendNow({
      type: "auth",
      id: randomID(),
      token: this.auth.token,
      project: this.auth.project,
      tenant: this.auth.tenant,
      device: browserTelemetryInfo(),
		capabilities: { syncReadyMany: 1, syncWatermark: 1, queryPagePatch: 1, queryObjectPatch: 1, queryOrderDelta: 1, queryFanout: 1, queryResultBatch: 1 },
    });
  }

  // Tokens from a fetcher are typically short-lived while the socket (and any
  // disconnect gap) can span hours: replaying the token that was current at
  // setAuth time guarantees an auth.error after a long sleep. authInFlight is
  // already true here, so everything else queues behind the fetch exactly as
  // it queues behind the server's auth reply.
  private async fetchAndSendAuth(fetcher: GonvexAuthTokenFetcher) {
    const generation = ++this.authFetchGeneration;
    const socket = this.socket;
    let token: string | null | undefined;
    try {
      token = await fetcher({ forceRefreshToken: false });
    } catch {
      // A fetcher that cannot reach its identity provider (offline start)
      // must not sign the session out — fall back to the installed token.
      token = undefined;
    }
    if (generation !== this.authFetchGeneration || this.auth.fetchToken !== fetcher) return;
    if (typeof token === "string" && token) {
      this.applyAuth({ token });
    } else if (token === null) {
      // The fetcher is authoritative about sign-out.
      this.applyAuth({ token: undefined });
    }
    // A dead socket's close handler already reset authInFlight; the next
    // reconnect runs its own sendAuth, so this resolve has nothing to send.
    if (this.socket !== socket || socket?.readyState !== WebSocket.OPEN) return;
    this.sendAuthFrame();
  }

  private async refreshRejectedAuth(fetcher: GonvexAuthTokenFetcher, rejectedToken: string | undefined, error: string) {
    const generation = ++this.authFetchGeneration;
    const socket = this.socket;
    let token: string | null | undefined;
    try {
      token = await fetcher({ forceRefreshToken: true });
    } catch {
      token = undefined;
    }
    if (generation !== this.authFetchGeneration || this.auth.fetchToken !== fetcher) return;
    if (typeof token === "string" && token && token !== rejectedToken) {
      this.applyAuth({ token });
      if (this.socket === socket && socket?.readyState === WebSocket.OPEN) {
        this.sendAuthFrame();
      }
      return;
    }
    // No fresher credential exists (fetch failed, signed out, or the refresh
    // returned the very token the server just refused): surface the rejection
    // and degrade to the unauthenticated flow exactly like the no-fetcher path.
    this.authInFlight = false;
    if (this.authWatchdogTimer) {
      clearTimeout(this.authWatchdogTimer);
      this.authWatchdogTimer = undefined;
    }
    this.resetQueryCacheScope();
    this.notifyAuthError(error);
    this.flushPendingMessages();
  }

  private notifyAuthError(error: string) {
    for (const handler of Array.from(this.authErrorHandlers)) {
      handler(error);
    }
  }

  // A lost auth reply (e.g. the server swapped its app plugin and dropped
  // in-flight responses while the socket stayed up) used to leave
  // authInFlight stuck true forever: every later mutation/subscription
  // queued into pendingMessages and was never sent — no error, no timeout,
  // and the server never saw the call. Re-issue auth if no reply arrives.
  private armAuthWatchdog() {
    if (this.authWatchdogTimer) clearTimeout(this.authWatchdogTimer);
    this.authWatchdogTimer = setTimeout(() => {
      this.authWatchdogTimer = undefined;
      if (!this.authInFlight) return;
      if (this.socket?.readyState === WebSocket.OPEN) {
        this.sendAuth(true);
      } else {
        this.connect();
      }
    }, 10_000);
  }

  private send(message: ClientMessage) {
    if (this.authInFlight && message.type !== "auth" && message.type !== "telemetry.event") {
      this.pendingMessages.push(message);
      return;
    }
    this.sendNow(message);
  }

  private sendNow(message: ClientMessage) {
    const socket = this.socket;
    if (!socket || socket.readyState === WebSocket.CLOSING || socket.readyState === WebSocket.CLOSED) {
      // Never drop silently. A missing socket swallowed the message outright,
      // and an "open" listener on a closing/closed socket never fires — either
      // way the caller hung forever with the server never seeing the call.
      // Queue it (auth excepted: reconnect sends a fresh auth itself) and
      // reconnect; pendingMessages flush once auth settles, and the close
      // handler rejects pending calls so failures stay loud.
      if (message.type !== "auth") {
        this.pendingMessages.push(message);
      }
      this.connect();
      return;
    }
    if (socket.readyState === WebSocket.CONNECTING) {
      socket.addEventListener(
        "open",
        () => {
          if (message.type === "auth") {
            socket.send(JSON.stringify(message));
            return;
          }
          this.send(message);
        },
        { once: true },
      );
      return;
    }
    socket.send(JSON.stringify(message));
  }

  private flushPendingMessages() {
    const pending = this.pendingMessages.splice(0);
    for (const message of pending) {
      this.send(message);
    }
  }
}

function rowsAtPath(
  result: JsonValue,
  path: readonly string[],
): { rows: JsonValue[]; scalar: boolean } | undefined {
  let current: JsonValue | undefined = result;
  for (const segment of path) {
    if (!isJsonRecord(current)) return undefined;
    current = current[segment];
  }
  if (Array.isArray(current)) return { rows: current, scalar: false };
  if (isJsonRecord(current)) return { rows: [current], scalar: true };
  return undefined;
}

function replaceRowsAtPath(
  result: JsonValue,
  path: readonly string[],
  rows: JsonValue[],
  scalar: boolean,
): JsonValue {
  if (path.length === 0) return scalar ? (rows[0] ?? null) : rows;
  if (!isJsonRecord(result)) return result;
  const [head, ...tail] = path;
  const current = result[head!];
  return { ...result, [head!]: replaceRowsAtPath(current ?? null, tail, rows, scalar) };
}

function querySubscriptionKey(ref: FunctionReference, args: JsonValue) {
  return `${ref.path}\u0000${stableStringify(args)}`;
}

function countPendingCalls(calls: Map<string, PendingCall>, kind: "mutation" | "action") {
  let count = 0;
  for (const call of calls.values()) {
    if (call.kind === kind) count += 1;
  }
  return count;
}

function isQueueableMutationError(error: unknown) {
  return error instanceof GonvexClientError
    && (error.code === "disconnected" || error.code === "timeout");
}

function mutationErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function stableStringify(value: JsonValue): string {
  if (typeof value === "string") {
    return JSON.stringify(value)
      .replace(/\u2028/g, "\\u2028")
      .replace(/\u2029/g, "\\u2029");
  }
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const record = value as Record<string, JsonValue>;
  return `{${Object.keys(record)
    .sort(utf8KeyCompare)
    .map((key) => `${stableStringify(key)}:${stableStringify(record[key])}`)
    .join(",")}}`;
}

function utf8KeyCompare(left: string, right: string) {
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    if (leftBytes[index] !== rightBytes[index]) return leftBytes[index]! - rightBytes[index]!;
  }
  return leftBytes.length - rightBytes.length;
}

function sameRevision(left: SubscriptionRevision, right: SubscriptionRevision | undefined) {
  return !!right && left.epoch === right.epoch && left.sequence === right.sequence;
}

function boundSyncRows(
  rows: JsonValue[],
  keyField: string,
  maxRows?: number,
  maxBytes?: number,
  orderBy?: string,
  orderDirection?: "asc" | "desc",
) {
  const kept: JsonValue[] = [];
  const seen = new Set<string>();
  let bytes = 0;
  for (const row of sortClientSyncRows(rows, orderBy, orderDirection)) {
    const key = syncRowKey(row, keyField);
    if (!key || seen.has(key)) continue;
    const size = syncJSONSize(row);
    if (maxRows && kept.length >= maxRows) break;
    if (maxBytes && bytes + size > maxBytes) break;
    kept.push(row);
    seen.add(key);
    bytes += size;
  }
  return kept;
}

function applySyncDelta(
  current: JsonValue[],
  keyField: string,
  upserts: JsonValue[],
  deleted: string[],
  maxRows?: number,
  maxBytes?: number,
  orderBy?: string,
  orderDirection?: "asc" | "desc",
) {
  const deletedSet = new Set(deleted);
  const upsertKeys = new Set(upserts.map((row) => syncRowKey(row, keyField)).filter(Boolean));
  const remainder = current.filter((row) => {
    const key = syncRowKey(row, keyField);
    return key && !deletedSet.has(key) && !upsertKeys.has(key);
  });
  return boundSyncRows(
    [...upserts, ...remainder],
    keyField,
    maxRows,
    maxBytes,
    orderBy,
    orderDirection,
  );
}

function sortClientSyncRows(
  rows: JsonValue[],
  orderBy?: string,
  orderDirection?: "asc" | "desc",
) {
  if (!orderBy) return rows;
  const direction = orderDirection === "asc" ? 1 : -1;
  return [...rows].sort((left, right) => {
    const leftValue = syncOrderValue(left, orderBy);
    const rightValue = syncOrderValue(right, orderBy);
    if (leftValue === rightValue) return 0;
    if (leftValue === null) return 1;
    if (rightValue === null) return -1;
    return leftValue < rightValue ? -direction : direction;
  });
}

function syncOrderValue(value: JsonValue, orderBy: string): string | number | null {
  if (!value || Array.isArray(value) || typeof value !== "object") return null;
  const candidate = value[orderBy];
  return typeof candidate === "string" || typeof candidate === "number" ? candidate : null;
}

function syncRowKey(value: JsonValue, keyField: string) {
  if (!value || Array.isArray(value) || typeof value !== "object") return "";
  const key = value[keyField];
  return key === null || key === undefined ? "" : String(key);
}

function syncJSONSize(value: JsonValue) {
  return new TextEncoder().encode(stableStringify(value)).byteLength;
}

function applyKeyedPatch(
  previous: JsonValue[],
	patch: { inserted?: JsonValue[]; updated?: JsonValue[]; deleted?: string[]; order?: string[]; prepend?: string[]; append?: string[] },
): JsonValue[] | undefined {
  const rows = new Map<string, JsonValue>();
  for (const row of previous) {
	const id = queryPatchRowKey(row);
	if (!id || rows.has(id)) return undefined;
	rows.set(id, row);
  }
  for (const id of patch.deleted ?? []) rows.delete(id);
  for (const row of [...(patch.inserted ?? []), ...(patch.updated ?? [])]) {
	const id = queryPatchRowKey(row);
	if (!id) return undefined;
	rows.set(id, row);
  }
  if (patch.order) {
    if (patch.order.length !== rows.size) return undefined;
    const ordered: JsonValue[] = [];
    const seen = new Set<string>();
    for (const id of patch.order) {
      const row = rows.get(id);
      if (!row || seen.has(id)) return undefined;
      seen.add(id);
      ordered.push(row);
    }
    return ordered;
  }
	if (patch.prepend || patch.append) {
		const prefix = patch.prepend ?? [];
		const suffix = patch.append ?? [];
		const moved = new Set([...prefix, ...suffix]);
		if (moved.size !== prefix.length + suffix.length) return undefined;
		const ordered: JsonValue[] = [];
		for (const id of prefix) {
			const row = rows.get(id);
			if (!row) return undefined;
			ordered.push(row);
		}
		for (const [id, row] of rows) {
			if (!moved.has(id)) ordered.push(row);
		}
		for (const id of suffix) {
			const row = rows.get(id);
			if (!row) return undefined;
			ordered.push(row);
		}
		if (ordered.length !== rows.size) return undefined;
		return ordered;
	}
  return Array.from(rows.values());
}

function queryPatchRowKey(value: JsonValue): string {
  if (!isJsonRecord(value)) return "";
  const candidate = value._id ?? value.id;
  return typeof candidate === "string" || typeof candidate === "number" ? String(candidate) : "";
}

export class ConvexReactClient extends GonvexClient {
  constructor(url: string, options: GonvexClientOptions = {}) {
    super(toWebSocketURL(url, options.project), options);
  }
}

function authFromOptions(options: GonvexClientOptions): GonvexClientAuth {
  return {
    project: options.project,
    token: options.token,
    tenant: options.tenant,
    telemetry: options.telemetry,
    identity: options.identity,
    fetchToken: options.fetchToken,
  };
}

function normalizeQuerySubscriptionRetentionMs(value: number | undefined): number {
  if (value === undefined) return 250;
  if (!Number.isFinite(value)) return 250;
  return Math.max(0, Math.min(5 * 60_000, Math.floor(value)));
}

function authIdentityKey(auth: GonvexClientAuth) {
  if (!auth.tenant) return "";
  if (auth.token) return authIdentityKeyFromToken(auth);
  // Token-free fallback: an explicit identity hint carries the same claims a
  // token would supply, so both paths derive the same key for the same user.
  const hint = auth.identity;
  if (hint && typeof hint.sub === "string" && hint.sub.trim()) {
    return [auth.project ?? "", auth.tenant, hint.iss ?? "", hint.sub].join("\u0000");
  }
  return "";
}

function authIdentityKeyFromToken(auth: GonvexClientAuth) {
  if (!auth.token || !auth.tenant) return "";
  const parts = auth.token.split(".");
  if (parts.length < 2) return "";
  try {
    const encoded = parts[1]!.replace(/-/g, "+").replace(/_/g, "/");
    const padded = encoded.padEnd(Math.ceil(encoded.length / 4) * 4, "=");
    const payload = JSON.parse(globalThis.atob(padded)) as { sub?: unknown; iss?: unknown };
    if (typeof payload.sub !== "string" || !payload.sub.trim()) return "";
    return [
      auth.project ?? "",
      auth.tenant,
      typeof payload.iss === "string" ? payload.iss : "",
      payload.sub,
    ].join("\u0000");
  } catch {
    return "";
  }
}

function sameAuthTokenIdentity(left: GonvexClientAuth, right: GonvexClientAuth) {
  const leftIdentity = authIdentityKey(left);
  const rightIdentity = authIdentityKey(right);
  return leftIdentity !== "" && leftIdentity === rightIdentity;
}

function mutationOutboxScope(url: string, auth: GonvexClientAuth, ephemeralScope: string) {
  const identity = authIdentityKey(auth);
  if (identity) return ["identity", url, identity].join("\u0000");
  if (auth.token || auth.identity || auth.fetchToken) {
    // Opaque tokens (or credentials installed before tenant selection) do not
    // expose a stable user key. A per-client scope preserves current-session
    // queue semantics without ever restoring those rows under another user.
    return ["ephemeral-auth", url, ephemeralScope].join("\u0000");
  }
  // Anonymous/dev-auth clients still need a stable namespace, but it must be
  // isolated by deployment and tenant. Once an authenticated identity is
  // installed, applyAuth switches away from this scope before restoring or
  // sending its durable mutations.
  return ["anonymous", url, auth.project ?? "", auth.tenant ?? ""].join("\u0000");
}

function isEphemeralOutboxScope(scope: string) {
  return scope.startsWith("ephemeral-auth\u0000");
}

function queryCacheDirectiveFromAuthResult(result: JsonValue): QueryCacheDirective | undefined {
  if (!isJsonRecord(result)) return undefined;
  return validQueryCacheDirective(result.queryCache) ? result.queryCache : undefined;
}

function validQueryCacheDirective(value: unknown): value is QueryCacheDirective {
  if (!isJsonRecord(value)) return false;
  return value.protocolVersion === 1
    && typeof value.scope === "string"
    && value.scope.length >= 16
    && (value.syncScope === undefined
      || (typeof value.syncScope === "string" && value.syncScope.length >= 16))
    && typeof value.epoch === "string"
    && value.epoch.length >= 16
    && typeof value.maxAgeMs === "number"
    && Number.isFinite(value.maxAgeMs)
    && value.maxAgeMs > 0;
}

/**
 * The scope under which sync collections are persisted and resumed. Newer
 * runtimes send a visibility-only `syncScope` that survives deploys (the
 * authoritative reconcile on resume guarantees correctness across code
 * changes); older runtimes only send the bundle-epoch `scope`.
 */
function syncPersistenceScope(directive: QueryCacheDirective): string {
  return typeof directive.syncScope === "string" && directive.syncScope.length >= 16
    ? directive.syncScope
    : directive.scope;
}

function isJsonRecord(value: unknown): value is Record<string, JsonValue> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hasOwn<T extends object>(value: T, key: PropertyKey) {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function toWebSocketURL(url: string, project?: string) {
  const wsURL = url.startsWith("ws://") || url.startsWith("wss://")
    ? new URL(url)
    : new URL(`${url.replace(/^http:/, "ws:").replace(/^https:/, "wss:").replace(/\/$/, "")}/ws`);
  if (project && !wsURL.searchParams.has("project")) {
    wsURL.searchParams.set("project", project);
  }
  return wsURL.toString();
}

function randomID() {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (randomUUID) return randomUUID.call(globalThis.crypto);
  return `gonvex_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}

function nowMs() {
  const performanceValue = globalThis.performance;
  if (
    performanceValue
    && Number.isFinite(performanceValue.timeOrigin)
    && typeof performanceValue.now === "function"
  ) {
    return performanceValue.timeOrigin + performanceValue.now();
  }
  return Date.now();
}

function queryCacheReadTimeout(value: number | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return defaultQueryCacheReadTimeoutMs;
  }
  return value;
}

function browserTelemetryInfo(): BrowserTelemetryInfo | undefined {
  const navigatorValue = globalThis.navigator;
  if (!navigatorValue) return undefined;
  const userAgent = navigatorValue.userAgent || "";
  const connection = (navigatorValue as any).connection || (navigatorValue as any).mozConnection || (navigatorValue as any).webkitConnection;
  const viewportWidth = typeof globalThis.innerWidth === "number" ? globalThis.innerWidth : undefined;
  const viewportHeight = typeof globalThis.innerHeight === "number" ? globalThis.innerHeight : undefined;
  return {
    userAgent,
    ...parseBrowser(userAgent),
    deviceType: detectDeviceType(userAgent),
    platform: navigatorValue.platform || "",
    language: navigatorValue.language || "",
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "",
    viewportWidth,
    viewportHeight,
    hardwareConcurrency: navigatorValue.hardwareConcurrency,
    deviceMemory: typeof (navigatorValue as any).deviceMemory === "number" ? (navigatorValue as any).deviceMemory : undefined,
    touchPoints: navigatorValue.maxTouchPoints,
    connectionType: typeof connection?.type === "string" ? connection.type : undefined,
    effectiveConnectionType: typeof connection?.effectiveType === "string" ? connection.effectiveType : undefined,
  };
}

function parseBrowser(userAgent: string): Pick<BrowserTelemetryInfo, "browserName" | "browserVersion"> {
  const patterns: Array<[string, RegExp]> = [
    ["Edge", /Edg\/([0-9.]+)/],
    ["Chrome", /Chrome\/([0-9.]+)/],
    ["Firefox", /Firefox\/([0-9.]+)/],
    ["Safari", /Version\/([0-9.]+).*Safari/],
  ];
  for (const [browserName, pattern] of patterns) {
    const match = userAgent.match(pattern);
    if (match) return { browserName, browserVersion: match[1] };
  }
  return { browserName: "", browserVersion: "" };
}

function detectDeviceType(userAgent: string) {
  if (/ipad|tablet/i.test(userAgent)) return "tablet";
  if (/mobi|iphone|android/i.test(userAgent)) return "mobile";
  return "desktop";
}
