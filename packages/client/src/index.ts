import type {
  BrowserTelemetryInfo,
  ClientMessage,
  ExecutionScope,
  JsonValue,
  MessageTrace,
  ServerCapabilities,
  ServerMessage,
  SubscriptionRevision,
  ReplicaCursor,
  ReplicaDirective,
  ReplicaOpenRequest,
} from "@gonvex/protocol";
import { replicaHashesDigest, replicaRowsHashes } from "./replica-integrity.js";
import { GonvexErrorReporter, type ErrorReporterOptions } from "./error-reporter.js";
export { GonvexErrorReporter } from "./error-reporter.js";
export type { ErrorReporterOptions, ErrorEventPayload, ErrorContext, ErrorAccount } from "./error-reporter.js";
import {
  optimisticPatchesFromReference,
  type OptimisticPatch,
  type OptimisticTransactionDefinition,
} from "./optimistic.js";
import {
  createReducerOutbox,
  type ReducerOutbox,
  type OutboxStore,
} from "./outbox.js";
import {
  LocalReplica,
  MemoryLocalReplicaStorage,
  type LocalReplicaStorage,
  type LocalReplicaView,
  type ReplicaChange,
  type ReplicaFreshness,
  type ReplicaRow,
  type ReplicaScope,
  type ReplicaSnapshot,
  type ReplicaTransaction,
  type ReplicaWindow,
  type LiveQueryResult,
  type ReplicaCollectionState,
  type ReplicaCollectionSubscriptionState,
  type ReplicaCollectionPlan,
} from "./local-replica.js";
import { runOfflineLiveQuery, type LiveQueryPlan, type OfflineLiveQueryResult } from "./query-expression.js";
export * from "./error-reporter.js";
export * from "./optimistic.js";
export * from "./outbox.js";
export * from "./kv-stores.js";
export * from "./signals.js";
export * from "./external-auth.js";
// Keep the mutable LocalReplica implementation private to GonvexClient. The
// public package exposes only the read-only view plus storage/value types.
export {
  MemoryLocalReplicaStorage,
  type LocalReplicaStorage,
  type LocalReplicaView,
  type ReplicaChange,
  type ReplicaFreshness,
  type ReplicaRow,
  type ReplicaScope,
  type ReplicaSnapshot,
  type ReplicaTransaction,
  type ReplicaWindow,
  type LiveQueryResult,
  type ReplicaCollectionState,
  type ReplicaCollectionSubscriptionState,
  type ReplicaCollectionPlan,
} from "./local-replica.js";
export * from "./query-expression.js";
export * from "./indexeddb-replica.js";
export * from "./control.js";

function asReplicaRow(value: JsonValue | undefined): ReplicaRow | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as ReplicaRow
    : undefined;
}

function projectReplicaIntegrityRows(
  rows: readonly ReplicaRow[],
  columns: readonly string[] | undefined,
): ReplicaRow[] {
  if (!columns?.length) return rows.map((row) => ({ ...row }));
  return rows.map((row) => {
    const projected: ReplicaRow = {};
    for (const column of columns) {
      if (hasOwn(row, column)) projected[column] = row[column]!;
    }
    return projected;
  });
}

type SubscriptionHandler = (message: ServerMessage) => void;
export type ReplicaReadyMessage = Extract<ServerMessage, { type: "replica.ready" }> & {
  /** True when the server cut this collection at its row or byte budget. */
  truncated?: boolean;
};

function createLocalReplicaView(replica: LocalReplica): LocalReplicaView {
  return Object.freeze({
    cursor: () => replica.cursor(),
    freshness: () => replica.freshness(),
    version: () => replica.version(),
    subscribe: (listener) => replica.subscribe(listener),
    hasPendingCommand: (commandId) => replica.hasPendingCommand(commandId),
    getWindow: (signature) => replica.getWindow(signature),
    listWindows: () => replica.listWindows(),
    windowRows: <T extends ReplicaRow = ReplicaRow>(signature: string) => replica.windowRows<T>(signature),
    entity: <T extends ReplicaRow = ReplicaRow>(entity: string, id: string) => replica.entity<T>(entity, id),
    entityBatch: <T extends ReplicaRow = ReplicaRow>(entity: string, ids: readonly string[]) => replica.entityBatch<T>(entity, ids),
    entityRows: <T extends ReplicaRow = ReplicaRow>(entity: string) => replica.entityRows<T>(entity),
    entityCompleteness: (entity) => replica.entityCompleteness(entity),
    liveQuery: <T extends ReplicaRow = ReplicaRow>(signature: string) => replica.liveQuery<T>(signature),
    collectionState: <T extends ReplicaRow = ReplicaRow>(signature: string) => replica.collectionState<T>(signature),
    hasLiveQuery: (signature) => replica.hasLiveQuery(signature),
    snapshot: () => replica.snapshot(),
  } satisfies LocalReplicaView);
}
export type ReplicaMessage =
  | Extract<ServerMessage, {
    type:
      | "replica.snapshot"
      | "replica.delta"
      | "replica.needHashes"
      | "replica.syncing"
      | "replica.reset"
      | "replica.error";
  }>
  | ReplicaReadyMessage;
export type ReplicaSubscriptionHandler = (message: ReplicaMessage) => void;
type WatchUpdateHandler = () => void;
type TelemetryHandler = (event: GonvexTelemetryEvent) => void;
type ConnectionStateHandler = (state: ConnectionState) => void;
export type SupportCommand = { id: string; kind: string; payload: JsonValue };
type QuerySubscription = {
  id: string;
  key: string;
  path: string;
  live?: { entity: string; key: string; resultPath: string[] };
  args: JsonValue;
  listeners: Set<SubscriptionHandler>;
  unsubscribeTimer?: ReturnType<typeof setTimeout>;
  lastMessage?: ServerMessage;
  serverSettled: boolean;
  socketGeneration?: number;
  lastRevision?: SubscriptionRevision;
  revisionSocketGeneration?: number;
  scope?: ReplicaScope;
  executionScope: ExecutionScope;
};
type ReplicaSubscription = {
  id: string;
  key: string;
  path: string;
  entity: string;
  columns?: readonly string[];
  args: JsonValue;
  listeners: Set<ReplicaSubscriptionHandler>;
  unsubscribeTimer?: ReturnType<typeof setTimeout>;
  // Keep the newest cursor seen in the current epoch even while an integrity
  // reset clears `cursor` to force a fresh snapshot. Without this floor, a
  // delayed pre-reset snapshot can be accepted during the reopen and become
  // current again.
  cursorFloor?: ReplicaCursor;
  retiredEpochs: Set<string>;
  lastMessage?: ReplicaMessage;
  socketGeneration?: number;
  opening: boolean;
  retryTimer?: ReturnType<typeof setTimeout>;
  retryAttempt: number;
  isUpToDate: boolean;
  forceFullIntegrity: boolean;
  verificationGeneration: number;
  scope?: ReplicaScope;
};

function replicaCursorIsStale(subscription: ReplicaSubscription, cursor: ReplicaCursor) {
  if (subscription.retiredEpochs.has(cursor.epoch)) return true;
  const floor = subscription.cursorFloor;
  return floor?.epoch === cursor.epoch && cursor.revision < floor.revision;
}

function raiseReplicaCursorFloor(subscription: ReplicaSubscription, cursor: ReplicaCursor) {
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
  scope: ExecutionScope;
  authorization?: FunctionReference["authorization"];
  reject: (error: Error) => void;
  socketGeneration?: number;
  timeoutTimer?: ReturnType<typeof setTimeout>;
};
type PendingCall = {
  id: string;
  kind: "reducer" | "action";
  path: string;
  args: JsonValue;
  scope: ExecutionScope;
  authorization?: FunctionReference["authorization"];
  idempotencyKey?: string;
  socketGeneration?: number;
  reject: (error: Error) => void;
  timeoutTimer?: ReturnType<typeof setTimeout>;
  /**
   * New runtimes close each committed tenant revision with a Replica
   * watermark after every entity and membership frame for that revision.
   * Keep a successful Reducer, or an Action that invoked Reducers, pending
   * until that ordered boundary is applied locally. An awaited write-bearing
   * call cannot outrun its entity or collection membership changes.
   */
  committedRevision?: number;
  completeAfterReplicaWatermark?: () => void;
  /** Successful Control writes settle after the runtime refreshes this connection's Control Queries. */
  completeAfterControlWatermark?: () => void;
};

export type FunctionReference<Args extends JsonValue = JsonValue, Result extends JsonValue = JsonValue> = {
  kind: string;
  path: string;
  /** Core Control Plane functions share the same persistent connection. */
  scope?: ExecutionScope;
  authorization?: "public" | "account" | "tenantAdmin" | "developer" | "projectAdmin" | "internal";
  argsSchema?: JsonValue;
  resultSchema?: JsonValue;
  delivery?: "oneShot" | "live" | "replica";
  offline?: {
    mode: "forbidden" | "allowed" | "onlineOnly";
    conflict?: "reject" | "expectedVersion" | "merge";
    reason?: string;
  };
  live?: { entity: string; key: string; resultPath?: readonly string[]; plan: LiveQueryPlan };
  replica?: ReplicaCollectionPlan & {
    columns?: readonly string[];
    mode?: "eager" | "progressive";
    maxRows?: number;
    maxBytes?: number;
  };
  optimistic?: {
    transaction?: OptimisticTransactionDefinition;
  };
};

export type GonvexClientErrorCode = "server" | "timeout" | "disconnected" | "closed" | "auth" | "superseded";

/**
 * Typed error for every rejected Gonvex operation. `code` distinguishes
 * server-side failures from transport-level ones so apps can decide whether
 * a retry is safe:
 *
 * - `server`: the runtime executed the function and returned an error.
 * - `timeout`: no response arrived within the operation timeout. For
 *   reducers/actions the write may or may not have been applied.
 * - `disconnected`: the socket dropped while the operation was pending.
 *   Reducers/actions fail closed unless a reducer opted into the outbox.
 * - `closed`: the client was explicitly closed.
 * - `auth`: authentication was rejected.
 * - `superseded`: the operation belonged to an authentication scope that the
 *   caller replaced before the operation completed.
 */
export class GonvexClientError extends Error {
  readonly code: GonvexClientErrorCode;
  readonly path?: string;
  readonly operation?: "query" | "reducer" | "action";

  constructor(message: string, options: { code: GonvexClientErrorCode; path?: string; operation?: "query" | "reducer" | "action" }) {
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
  inflightReducers: number;
  inflightActions: number;
  inflightOneShotQueries: number;
};

export type GonvexTimeoutOptions = {
  /** One-shot `client.query()` timeout. Default 20s. `0` disables. */
  queryTimeoutMs?: number;
  /** `client.reducer()` timeout. Default 20s. `0` disables. */
  reducerTimeoutMs?: number;
  /** `client.action()` timeout. Default 60s. `0` disables. */
  actionTimeoutMs?: number;
};

export const DEFAULT_QUERY_TIMEOUT_MS = 20_000;
export const DEFAULT_REDUCER_TIMEOUT_MS = 20_000;
export const DEFAULT_ACTION_TIMEOUT_MS = 60_000;

export type CallOptions = {
  /** Per-call override of the operation timeout. `0` disables. */
  timeoutMs?: number;
  /** Ordered row changes to expose until the reducer settles. */
  optimistic?: OptimisticPatch[];
  /** Queue transport failures durably instead of rejecting. Default `reject`. */
  offline?: "queue" | "reject";
};

/** Returned when an offline reducer has been accepted by the local outbox. */
export type QueuedReducerOutcome = {
  status: "queued";
  reducerId: string;
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
   * Async source of the auth token.
   * When installed, the client re-fetches before every auth send — on first
   * connect, on every reconnect, and once more with `forceRefreshToken: true`
   * when the server rejects the current token — so a socket that outlives a
   * short-lived Gonvex app session reauthenticates with a
   * live credential instead of replaying the expired one. A `token` passed in
   * the same `setAuth` call is trusted and sent as-is; resolving `null` signs
   * the session out; a rejected fetch keeps the currently installed token so
   * an offline start is not signed out.
   */
  fetchToken?: GonvexAuthTokenFetcher;
  /** Non-secret identity hint used to isolate local-replica persistence. */
  identity?: { sub: string; iss?: string };
};

export type GonvexClientOptions = GonvexClientAuth & {
  /**
   * Keep listenerless live queries subscribed for this long so route
   * backtracking can reuse their current result without WebSocket churn.
   * Defaults to 250ms; set a longer bounded window for local-first apps.
   */
  querySubscriptionRetentionMs?: number;
  /**
   * Keep listenerless Replica Collections open briefly across React remounts.
   * Defaults to 250ms, preventing close/open/snapshot churn in StrictMode.
   */
  replicaSubscriptionRetentionMs?: number;
  /**
   * Durable reducer queue settings. Every replay keeps its original
   * idempotency key, making an accidental cross-tab double-send server-safe.
   * Runtimes without IndexedDB inject `store` to keep queued reducers
   * durable; queue semantics always stay in the SDK.
   */
  outbox?: { databaseName?: string; enabled?: boolean; store?: OutboxStore };
  /** Transactional normalized store used by Replica Collections and Live Queries. */
  localReplica?: { storage?: LocalReplicaStorage };
  errorReporting?: false | Omit<ErrorReporterOptions, "transport" | "project" | "tenant">;
  timeouts?: GonvexTimeoutOptions;
};

export type GonvexTelemetryEvent = {
  type: "reducer" | "action" | "query";
  id: string;
  path: string;
  reason?: "initial" | "change" | "recover";
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
// (replica.needHashes) — the server verifies digest-only resumes with zero row
// data on the unchanged path, so a reload uploads bytes, not hash maps.
// Must match the runtime's per-frame replica.openMany admission limit. Keeping
// this client-side prevents one oversized page from stranding every replica in a
// batch behind a frame-level rejection.
const maxReplicaBatchOpens = 256;
// A wedged IndexedDB (observed in Chrome: open() never fires any event, so no
// rejection ever reaches the store's error handling) must degrade the warm
// start into a cold open — never into a permanently empty screen. Reads
// normally settle in a few milliseconds.

export class GonvexClient {
  private socket: WebSocket | undefined;
  private readonly handlers = new Map<string, SubscriptionHandler>();
  private readonly querySubscriptions = new Map<string, QuerySubscription>();
  private readonly replicaSubscriptions = new Map<string, ReplicaSubscription>();
  private readonly oneShotQueries = new Map<string, OneShotQuery>();
  private readonly telemetryHandlers = new Set<TelemetryHandler>();
  private readonly pendingMessages: ClientMessage[] = [];
  private readonly pendingReplicaOpens = new Set<ReplicaSubscription>();
  private readonly pendingQuerySubscribes = new Set<QuerySubscription>();
  private replicaOpenFlushTimer: ReturnType<typeof setTimeout> | undefined;
  private querySubscribeFlushTimer: ReturnType<typeof setTimeout> | undefined;
  private serverCapabilities: ServerCapabilities = {};
  private activeArtifactHashValue = "";
  private auth: GonvexClientAuth = {};
  private authInFlight = false;
  // Only the newest auth frame may change socket authorization. The runtime
  // can finish an earlier project-only frame after a newer tenant frame; its
  // response is obsolete even though both travelled on the same WebSocket.
  private latestAuthFrameId: string | undefined;
  private activeAuthFrameId: string | undefined;
  private authResendRequired = false;
  private authWatchdogTimer: ReturnType<typeof setTimeout> | undefined;
  // Monotonic guard for async token fetches: a resolve whose generation is no
  // longer current was superseded (newer setAuth, watchdog re-issue, or a
  // reconnect's own fetch) and must be discarded.
  private authFetchGeneration = 0;
  // At most one forced refresh per rejection cycle; cleared when auth settles
  // or a fresh send cycle starts, so a bad token can't refresh-loop forever.
  private authRetriedAfterError = false;
  private readonly authErrorHandlers = new Set<(error: string) => void>();
  private managedAuthAttempt: {
    ids: Set<string>;
    resolve: () => void;
    reject: (error: Error) => void;
  } | undefined;
  private telemetryEnabled = false;
  private readonly querySubscriptionRetentionMs: number;
  private readonly replicaSubscriptionRetentionMs: number;
  private readonly reducerOutbox: ReducerOutbox;
  private readonly replica: LocalReplica;
  private readonly replicaView: LocalReplicaView;
  private readonly optimisticReducerIds = new Set<string>();
  /** Reducers currently owned by the foreground send path, never by recovery. */
  private readonly directOutboxReducerIds = new Set<string>();
  private readonly optimisticOutboxEntryIds = new Map<string, number>();
  private outboxReady: Promise<void>;
  private outboxScope = "";
  private outboxScopeGeneration = 0;
  private readonly outboxEphemeralScope = randomID();
  private replicaScope: ReplicaScope = "";
  private hasAuthoritativeReplicaScope = false;
  private replicaReady: Promise<void> = Promise.resolve();
  private replicaFrames: Promise<void> = Promise.resolve();
  private processedReplicaWatermarkRevision = 0;
  private readonly pendingReplicaTransactions: Array<Extract<ServerMessage, { type: "replica.transaction" }>> = [];
  private readonly unsubscribeOutbox: () => void;
  private readonly unsubscribeBrowserOnline: (() => void) | undefined;
  private drainingOutbox = false;
  private outboxDrainTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly sessionScopeHandlers = new Set<() => void>();
  private readonly errorReporter: GonvexErrorReporter | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectAttempt = 0;
  private socketGeneration = 0;
  private authenticatedSocketGeneration: number | undefined;
  private manuallyClosed = false;
  private readonly pendingCalls = new Map<string, PendingCall>();
  private readonly connectionStateHandlers = new Set<ConnectionStateHandler>();
  private readonly supportCommandHandlers = new Set<(command: SupportCommand) => void>();
  private isWebSocketConnected = false;
  private hasEverConnected = false;
  private connectionCount = 0;
  private readonly timeouts: Required<GonvexTimeoutOptions>;

  constructor(private readonly url: string, options: GonvexClientOptions = {}) {
    this.auth = authFromOptions(options);
    this.telemetryEnabled = options.telemetry === true;
    this.querySubscriptionRetentionMs = normalizeQuerySubscriptionRetentionMs(
      options.querySubscriptionRetentionMs,
    );
    this.replicaSubscriptionRetentionMs = normalizeQuerySubscriptionRetentionMs(
      options.replicaSubscriptionRetentionMs,
    );
    this.reducerOutbox = createReducerOutbox(options.outbox);
    this.replica = new LocalReplica(options.localReplica?.storage);
    this.replicaView = createLocalReplicaView(this.replica);
    this.unsubscribeOutbox = this.reducerOutbox.subscribe(() => {
      void this.drainOutbox();
    });
    if (typeof globalThis.addEventListener === "function") {
      const onBrowserOnline = () => {
        if (this.manuallyClosed) return;
        this.connect();
        void this.drainOutbox();
      };
      globalThis.addEventListener("online", onBrowserOnline);
      this.unsubscribeBrowserOnline = () => globalThis.removeEventListener("online", onBrowserOnline);
    }
    // Select the initial identity scope synchronously so subscriptions created
    // immediately after the client cannot capture an empty placeholder scope.
    // A same-tick setAuth supersedes this activation by generation before its
    // snapshot can publish, so anonymous rows still cannot flash on screen.
    const initialScope = reducerOutboxScope(this.url, this.auth, this.outboxEphemeralScope);
    this.outboxScope = initialScope;
    this.replicaScope = ["awaiting-server-scope", this.url, this.outboxEphemeralScope].join("\u0000");
    this.outboxScopeGeneration += 1;
    this.replicaReady = this.replica.activateScope(this.replicaScope, true);
    this.outboxReady = Promise.resolve();
    this.timeouts = {
      queryTimeoutMs: options.timeouts?.queryTimeoutMs ?? DEFAULT_QUERY_TIMEOUT_MS,
      reducerTimeoutMs: options.timeouts?.reducerTimeoutMs ?? DEFAULT_REDUCER_TIMEOUT_MS,
      actionTimeoutMs: options.timeouts?.actionTimeoutMs ?? DEFAULT_ACTION_TIMEOUT_MS,
    };
    if (options.errorReporting && options.project) {
      this.errorReporter = new GonvexErrorReporter({
        project: options.project,
        tenant: options.tenant,
        ...options.errorReporting,
        transport: (type, payload) => this.sendNativeError(type, payload),
      });
    }
  }

  /** The single normalized authoritative + optimistic application data store. */
  get localReplica(): LocalReplicaView {
    return this.replicaView;
  }

  replicaSignature(ref: FunctionReference, args: JsonValue = {}) {
    return querySubscriptionKey(ref, args);
  }

  /** Read one persisted Live Query membership without starting another transport subscription. */
  retainedLiveQuery<T extends ReplicaRow = ReplicaRow>(signature: string): LiveQueryResult<T> {
    return this.replica.liveQuery<T>(signature);
  }

  /** Resolve an ordered ID batch from one normalized entity store. */
  replicaEntities<T extends ReplicaRow = ReplicaRow>(entity: string, ids: readonly string[]): Array<T | undefined> {
    return this.replica.entityBatch<T>(entity, ids);
  }

  /** Read rows and server-owned completeness for a persisted Replica Collection. */
  replicaCollectionState<T extends ReplicaRow = ReplicaRow>(ref: FunctionReference, args: JsonValue = {}): ReplicaCollectionState<T> {
    return this.replica.collectionState<T>(this.replicaSignature(ref, args));
  }

  /** Run the generated Live Query plan over the bounded normalized cache. */
  offlineLiveQuery<T extends ReplicaRow = ReplicaRow>(
    ref: FunctionReference,
    args: JsonValue = {},
  ): OfflineLiveQueryResult<T> {
    if (!ref.live?.plan) {
      return { rows: [], completeness: "partial", supported: false, unsupportedOperator: "missingPlan" };
    }
    const queryArgs = isJsonRecord(args) ? args : {};
    return runOfflineLiveQuery(
      this.replica.entityRows<T>(ref.live.entity),
      ref.live.plan,
      queryArgs,
      this.replica.entityCompleteness(ref.live.entity),
    );
  }

  /** Number of reducers waiting for a definitive server result. */
  async outboxCount(): Promise<number> {
    await this.outboxReady;
    return this.reducerOutbox.count(this.outboxScope);
  }

  connectionState(): ConnectionState {
    const inflightReducers = countPendingCalls(this.pendingCalls, "reducer");
    const inflightActions = countPendingCalls(this.pendingCalls, "action");
    const inflightOneShotQueries = this.oneShotQueries.size;
    return {
      isWebSocketConnected: this.isWebSocketConnected,
      hasEverConnected: this.hasEverConnected,
      connectionCount: this.connectionCount,
      connectionRetries: this.reconnectAttempt,
      hasInflightRequests: inflightReducers + inflightActions + inflightOneShotQueries > 0,
      inflightReducers,
      inflightActions,
      inflightOneShotQueries,
    };
  }

  /** Metadata advertised by the runtime in its latest session.ready frame. */
  serverInfo(): Readonly<ServerCapabilities> {
    return { ...this.serverCapabilities };
  }

  /** Hash of the atomically active TypeScript module generation. */
  activeArtifactHash(): string {
    return this.activeArtifactHashValue;
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
    const nextAuth = { ...this.auth, ...auth };
    const changesAuthFrame = this.auth.project !== nextAuth.project
      || this.auth.tenant !== nextAuth.tenant
      || this.auth.token !== nextAuth.token
      || this.auth.identity?.sub !== nextAuth.identity?.sub
      || this.auth.identity?.iss !== nextAuth.identity?.iss;
    const needsFetcherAuth = !nextAuth.token
      && nextAuth.fetchToken !== undefined
      && nextAuth.fetchToken !== this.auth.fetchToken;
    if (!changesAuthFrame && !needsFetcherAuth) {
      // Local auth metadata (most commonly React installing a refresh
      // callback for an already installed token) does not require another
      // wire auth frame. Duplicating it can race the response to the current
      // frame and has no authorization effect.
      this.applyAuth(auth);
      this.authFetchGeneration += 1;
      return;
    }
    this.cancelManagedAuthAttempt("Authentication was replaced by a newer session.");
    this.applyAuth(auth);
    // The caller owns auth now: a token fetch still in flight from the
    // previous installation must not clobber this one when it resolves.
    this.authFetchGeneration += 1;
    if (this.socket?.readyState === WebSocket.OPEN) {
      if (this.authInFlight && this.latestAuthFrameId) {
        // Keep server-side connection auth transitions serialized. Sending a
        // second frame concurrently allows an older, slower project-only auth
        // operation to overwrite a newer accepted tenant session.
        this.authResendRequired = true;
        return;
      }
      // A token supplied in this very call was just minted by the caller —
      // send it as-is instead of paying another fetch round trip.
      this.sendAuth(true, { useFetcher: !hasOwn(auth, "token") });
    }
  }

  /**
   * Atomically install credentials and wait for the runtime to accept them.
   * This is used by provider-owned, memory-only authentication transitions
   * such as developer mode. Applications should normally use their auth
   * provider rather than calling this method directly.
   */
  authenticate(auth: GonvexClientAuth): Promise<void> {
    this.cancelManagedAuthAttempt("Authentication was replaced by a newer session.");
    this.applyAuth(auth);
    this.authFetchGeneration += 1;
    const promise = new Promise<void>((resolve, reject) => {
      this.managedAuthAttempt = { ids: new Set(), resolve, reject };
    });
    if (this.socket?.readyState === WebSocket.OPEN) {
      if (this.authInFlight && this.latestAuthFrameId) {
        this.authResendRequired = true;
      } else {
        this.sendAuth(true, { useFetcher: !hasOwn(auth, "token") });
      }
    } else {
      this.connect();
    }
    return promise;
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
      this.pendingMessages.length = 0;
      this.pendingReplicaTransactions.length = 0;
      this.rejectPendingCalls((call) => new GonvexClientError(
        `Authentication scope changed while waiting for ${call.kind} ${call.path}`,
        { code: "superseded", path: call.path, operation: call.kind },
      ));
      for (const query of this.oneShotQueries.values()) {
        if (query.timeoutTimer) clearTimeout(query.timeoutTimer);
        this.handlers.delete(query.id);
        query.reject(new GonvexClientError(
          `Authentication scope changed while waiting for Query ${query.path}`,
          { code: "superseded", path: query.path, operation: "query" },
        ));
      }
      this.oneShotQueries.clear();
      // Authentication is changing the project, tenant, or actor. Immediately
      // leave the previous authoritative Replica scope so hooks mounted by the
      // next React render cannot send tenant reads against the old server
      // session. The accepted auth result installs the next scope below.
      this.quarantineReplicaScope();
    }
    this.auth = nextAuth;
    if (scopeMayChange) {
      void this.activateOutboxScope();
    }
    if (auth.tenant !== undefined) this.errorReporter?.setTenant(auth.tenant);
    if (auth.project !== undefined) this.errorReporter?.setProject(auth.project);
    if (auth.telemetry !== undefined) {
      this.telemetryEnabled = auth.telemetry === true;
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
      this.replica.setFreshness("verifying");
      this.hasEverConnected = true;
      this.connectionCount += 1;
      this.errorReporter?.connectionRestored?.();
      this.sendAuth(false);
      if (isReconnect) this.resubscribeQueries(generation);
      void this.drainOutbox();
      this.notifyConnectionState();
    });
    socket.addEventListener("close", () => {
      if (this.socket !== socket || this.manuallyClosed) return;
      this.isWebSocketConnected = false;
      this.replica.setFreshness("offline");
      this.markReplicaSubscriptionsOutOfDate();
      this.authInFlight = false;
      this.authResendRequired = false;
      this.latestAuthFrameId = undefined;
      this.activeAuthFrameId = undefined;
      if (this.authWatchdogTimer) {
        clearTimeout(this.authWatchdogTimer);
        this.authWatchdogTimer = undefined;
      }
      // A subscription queued for the old socket is superseded by the complete
      // resubscribe below. Queued reducers/actions are rejected below, so
      // drop them too — flushing them after reconnect would fire writes whose
      // callers already saw a rejection.
      this.pendingMessages.length = 0;
      this.pendingReplicaTransactions.length = 0;
      // Reducers/actions must fail closed on transport loss: silently
      // replaying a non-idempotent write after reconnect is unsafe, and
      // leaving the promise pending hangs the caller forever.
      this.rejectPendingCalls((call) => new GonvexClientError(
        `Connection lost while waiting for ${call.kind} ${call.path}. The operation may or may not have been applied.`,
        { code: "disconnected", path: call.path, operation: call.kind },
      ), (call) => call.scope !== "control");
      this.scheduleReconnect();
      this.notifyConnectionState();
    });
    socket.addEventListener("message", (event) => {
      if (this.socket !== socket) return;
      let message: ServerMessage;
      try {
        message = JSON.parse(String(event.data)) as ServerMessage;
      } catch {
        return;
      }
      if (message.type === "session.ready") {
        this.serverCapabilities = message.capabilities ?? {};
        if (!message.replica) {
          if (this.hasControlPlaneWork() || (!!this.auth.token && !this.auth.tenant)) {
            this.flushPendingMessages();
          } else {
            this.rejectMissingReplicaDirective();
          }
          return;
        }
        const ready = this.activateReplicaDirective(message.replica);
        if (!this.auth.token && !this.auth.tenant) {
          void ready
            .then(() => {
              this.resumeQuerySubscriptions();
              this.resumeReplicaSubscriptions();
            })
            .catch((error) => this.rejectReplicaDirective(error));
        } else {
          void ready.catch((error) => this.rejectReplicaDirective(error));
        }
        return;
      }
      if (message.type === "system.reload") {
        if (typeof message.artifactHash === "string") {
          this.activeArtifactHashValue = message.artifactHash;
        }
        this.resumeQuerySubscriptions(true);
        this.resumeReplicaSubscriptions();
        return;
      }
      if (message.type === "auth.result" || message.type === "auth.error") {
        if (
          message.type === "auth.error"
          && (message.id === "membership-changed" || message.id === "session-expired")
        ) {
          // These are unsolicited connection-scope events, not replies to an
          // auth frame. The runtime has already discarded every tenant
          // subscription. Rotate their routing IDs immediately so delayed
          // Replica errors from that discarded scope cannot reach current
          // React listeners.
          this.pendingReplicaTransactions.length = 0;
          this.quarantineReplicaScope();
          this.activeAuthFrameId = undefined;

          // If credentials are already being revalidated, that queued auth
          // frame will install the new authoritative scope. Starting another
          // transition here would recreate the out-of-order auth race.
          if (this.latestAuthFrameId) return;

          if (this.authWatchdogTimer) {
            clearTimeout(this.authWatchdogTimer);
            this.authWatchdogTimer = undefined;
          }
          this.authInFlight = false;
          if (message.id === "membership-changed") {
            this.sendAuth(true, { useFetcher: false });
            return;
          }

          const fetcher = this.auth.fetchToken;
          if (fetcher) {
            this.authInFlight = true;
            this.authRetriedAfterError = true;
            this.armAuthWatchdog();
            void this.refreshRejectedAuth(fetcher, this.auth.token, message.error);
          } else {
            this.notifyAuthError(message.error);
          }
          return;
        }
        // Auth work may complete out of order inside the runtime. An obsolete
        // response must never settle the current transition, reopen queries,
        // or downgrade an accepted tenant session to an older control-only
        // scope.
        const settlesLatestFrame = message.id === this.latestAuthFrameId;
        const rejectsActiveFrame = message.type === "auth.error"
          && this.latestAuthFrameId === undefined
          && message.id === this.activeAuthFrameId;
        if (!settlesLatestFrame && !rejectsActiveFrame) return;
        if (settlesLatestFrame) this.latestAuthFrameId = undefined;
        if (settlesLatestFrame && this.authResendRequired) {
          // The settled frame represented credentials that have already been
          // superseded locally. Do not publish or flush anything from that
          // intermediate server scope; authenticate the newest state now.
          this.authResendRequired = false;
          if (this.authWatchdogTimer) {
            clearTimeout(this.authWatchdogTimer);
            this.authWatchdogTimer = undefined;
          }
          this.sendAuth(true);
          return;
        }
        if (message.type === "auth.result") this.activeAuthFrameId = message.id;
        else this.activeAuthFrameId = undefined;
        if (this.authWatchdogTimer) {
          clearTimeout(this.authWatchdogTimer);
          this.authWatchdogTimer = undefined;
        }
        if (message.type === "auth.result") {
          const reauthenticatedSameSocket = this.authenticatedSocketGeneration === this.socketGeneration;
          this.authenticatedSocketGeneration = this.socketGeneration;
          this.activeArtifactHashValue = artifactHashFromAuthResult(message.result) ?? this.activeArtifactHashValue;
          const developerSessionToken = developerSessionTokenFromAuthResult(message.result);
          if (developerSessionToken) {
            // The activation token is single-use. Keep its rotating successor
            // only in process memory and use it for the next reconnect.
            this.auth = { ...this.auth, token: developerSessionToken, fetchToken: undefined };
          }
          this.authRetriedAfterError = false;
          const directive = replicaDirectiveFromAuthResult(message.result);
          if (!directive) {
            this.authInFlight = false;
            if (!this.auth.tenant) {
              this.resumeQuerySubscriptions(reauthenticatedSameSocket);
              this.settleManagedAuthAttempt(message.id);
            } else {
              this.settleManagedAuthAttempt(message.id, "Runtime did not provide an authoritative Local Replica visibility scope");
              this.rejectMissingReplicaDirective();
            }
            // A reducer admitted while this auth frame was in flight may have
            // been durably queued because the socket was not yet usable. Wake
            // that queue once the successful auth result has made it usable.
            void this.drainOutbox();
            this.flushPendingMessages();
            return;
          }
          void this.activateReplicaDirective(directive)
            .then(() => this.activateOutboxScope())
            .then(() => {
              // The accepted server identity is not usable until its durable
              // Replica partition has been activated locally. Only now may
              // tenant calls and subscriptions leave the client.
              this.authInFlight = false;
              this.drainPendingReplicaTransactions();
              // Authentication is replaced in-place on the same socket during
              // normal token rotation. The runtime clears its subscription
              // maps for every accepted auth frame, so same-generation Live
              // Queries must be sent again just like Replica Collections.
              this.resumeQuerySubscriptions(reauthenticatedSameSocket);
              this.resumeReplicaSubscriptions();
              this.settleManagedAuthAttempt(message.id);
              // Same-scope reauthentication does not reload the outbox, so no
              // restore callback will wake a reducer queued during auth.
              void this.drainOutbox();
              this.flushPendingMessages();
            })
            .catch((error) => {
              this.authInFlight = false;
              this.pendingReplicaTransactions.length = 0;
              this.settleManagedAuthAttempt(message.id, error instanceof Error ? error.message : "Runtime returned an invalid Local Replica scope");
              this.rejectReplicaDirective(error);
              this.flushPendingMessages();
            });
          return;
        } else {
          this.authInFlight = false;
          this.pendingReplicaTransactions.length = 0;
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
          this.quarantineReplicaScope();
          this.settleManagedAuthAttempt(message.id, message.error);
          this.notifyAuthError(message.error);
        }
        this.flushPendingMessages();
      }
      if (message.type === "replica.readyMany") {
        for (const ready of message.ready) {
          const readyMessage = { type: "replica.ready", ...ready } as ReplicaReadyMessage;
          this.handlers.get(ready.id)?.(readyMessage);
        }
        return;
      }
      if (message.type === "replica.transaction") {
        // Replica frames carry no tenant/scope field. During auth renewal we
        // cannot safely attribute a late frame to either side of the switch.
        if (this.authInFlight) {
          this.pendingReplicaTransactions.push(message);
          return;
        }
        this.enqueueReplicaTransaction(message);
        return;
      }
      if (message.type === "replica.watermark") {
        if (this.serverCapabilities.replicaWatermark === 1) {
          // The runtime emits transactions and watermarks in one ordered
          // stream. Keep both on the same client queue so a watermark cannot
          // advance the cursor past a transaction that was received first but
          // has not finished applying to durable Local Replica storage yet.
          this.enqueueReplicaFrame(() => this.handleReplicaWatermark(message.revision));
        }
        return;
      }
      if (message.type === "support.command") {
        const result = message.result && typeof message.result === "object" && !Array.isArray(message.result)
          ? message.result as Record<string, JsonValue>
          : {};
        const command = { id: message.id, kind: String(result.kind ?? ""), payload: result.payload ?? null };
        for (const handler of this.supportCommandHandlers) handler(command);
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
    this.manuallyClosed = true;
    this.cancelManagedAuthAttempt("Gonvex client was closed during authentication.");
    if (isEphemeralOutboxScope(this.outboxScope)) {
      void this.reducerOutbox.clear(this.outboxScope);
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
    for (const subscription of this.replicaSubscriptions.values()) {
      this.clearReplicaRetry(subscription);
      if (subscription.unsubscribeTimer) clearTimeout(subscription.unsubscribeTimer);
    }
    if (this.replicaOpenFlushTimer) {
      clearTimeout(this.replicaOpenFlushTimer);
      this.replicaOpenFlushTimer = undefined;
    }
    this.pendingReplicaOpens.clear();
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
    this.unsubscribeBrowserOnline?.();
    this.handlers.clear();
    this.querySubscriptions.clear();
    this.replicaSubscriptions.clear();
    this.sessionScopeHandlers.clear();
    this.authErrorHandlers.clear();
    // Invalidate any token fetch still in flight so its resolve can't touch
    // the closed client's caches.
    this.authFetchGeneration += 1;
    this.errorReporter?.close();
    const socket = this.socket;
    this.socket = undefined;
    this.isWebSocketConnected = false;
    this.replica.setFreshness("offline");
    this.notifyConnectionState();
    this.connectionStateHandlers.clear();
    this.supportCommandHandlers.clear();
    if (!socket) return;
    socket.close();
  }

  private rejectPendingCalls(
    makeError: (call: PendingCall) => GonvexClientError,
    predicate: (call: PendingCall) => boolean = () => true,
  ) {
    if (this.pendingCalls.size === 0) return;
    const calls = Array.from(this.pendingCalls.values()).filter(predicate);
    for (const call of calls) {
      this.pendingCalls.delete(call.id);
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

  onSupportCommand(handler: (command: SupportCommand) => void) {
    this.supportCommandHandlers.add(handler);
    return () => this.supportCommandHandlers.delete(handler);
  }

  subscribeLiveQuery<Args extends JsonValue = JsonValue, Result extends JsonValue = JsonValue>(ref: FunctionReference<Args, Result>, args: Args = {} as Args, onMessage: SubscriptionHandler) {
    const isControlLiveQuery = ref.scope === "control" && ref.delivery === "live";
    if ((!ref.live?.plan || ref.delivery !== "live") && !isControlLiveQuery) {
      throw new GonvexClientError(`Query ${ref.path} is not a structured Live Query`, { code: "server", path: ref.path, operation: "query" });
    }
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
      // Replay the latest result/error to this late joiner. Coalesced subscriptions
      // share a single server subscription, so the server only sends `initial` once —
      // to the first subscriber. Without this replay, components that mount after the
      // initial result arrives (e.g. a dialog opened later) would never receive data
      // until the next committed change. Replaying here (not via the shared
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
      live: ref.live ? { ...ref.live, resultPath: [...(ref.live.resultPath ?? [])] } : undefined,
      args,
      listeners: new Set([onMessage]),
      serverSettled: false,
      scope: this.replicaScope,
      executionScope: ref.scope ?? "tenant",
    };
    this.querySubscriptions.set(key, subscription);
    this.handlers.set(subscription.id, (message) => {
      const scope = subscription.scope ?? this.replicaScope;
      void this.handleQueryMessage(subscription, message, scope).catch(() => this.replica.setFreshness("verifying"));
    });
    this.sendSubscription(subscription);

    return () => this.unsubscribeQueryListener(key, onMessage);
  }

  /** Watch an authorized host-owned Control Plane Query on the persistent connection. */
  watchControlQuery<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args) {
    if (ref.scope !== "control" || ref.kind !== "query" || ref.delivery !== "live") {
      throw new GonvexClientError(`Query ${ref.path} is not a live Control Plane Query`, { code: "server", path: ref.path, operation: "query" });
    }
    const listeners = new Set<WatchUpdateHandler>();
    let result: T | undefined;
    let error: Error | undefined;
    let version = 0;
    let snapshot: Readonly<{ result: T | undefined; version: number }> = { result, version };
    const notify = () => queueMicrotask(() => listeners.forEach((listener) => listener()));
    const unsubscribe = this.subscribeLiveQuery(ref, args, (message) => {
      if (message.type === "query.result") {
        result = message.result as T;
        error = undefined;
        version += 1;
        snapshot = { result, version };
        notify();
      } else if (message.type === "query.error") {
        error = new GonvexClientError(`Query ${ref.path} failed: ${message.error}`, { code: "server", path: ref.path, operation: "query" });
        version += 1;
        snapshot = { result, version };
        notify();
      }
    });
    return {
      getSnapshot() {
        if (error) throw error;
        return snapshot;
      },
      onUpdate(listener: WatchUpdateHandler) {
        listeners.add(listener);
        queueMicrotask(() => listeners.has(listener) && listener());
        return () => {
          listeners.delete(listener);
          if (listeners.size === 0) unsubscribe();
        };
      },
    };
  }

  private async handleQueryMessage(subscription: QuerySubscription, message: ServerMessage, scope = this.replicaScope) {
      if (scope !== this.replicaScope || (subscription.scope !== undefined && subscription.scope !== scope)) return;
      const normalized = this.normalizeSubscriptionMessage(subscription, message);
      if (!normalized) return;
      message = normalized;
      if (message.type === "query.result") {
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
      if (message.type === "query.result" && subscription.live) {
        // Local Replica is the source of truth for normalized Live Query
        // reads. Publish the query callback only after its entity/membership
        // window has been durably committed and atomically swapped.
        try {
          await this.materializeLiveQuery(subscription, message, scope);
        } catch {
          this.replica.setFreshness("verifying");
        }
        if (scope !== this.replicaScope || subscription.scope !== scope) return;
      }
      if (message.type === "query.result") {
        this.acknowledgeOptimisticSource(subscription.key, message.originCommandIds);
        this.acknowledgeOptimisticQuerySnapshot(subscription, message.result);
      }
      const outgoing = this.materializeQueryMessage(subscription, message);
      for (const listener of Array.from(subscription.listeners)) {
        listener(outgoing);
      }
      // One-shot results are transient. Live rows are persisted by LocalReplica.
  }

  private materializeLiveQuery(
    subscription: QuerySubscription,
    message: Extract<ServerMessage, { type: "query.result" }>,
    scope = this.replicaScope,
  ): Promise<void> {
    const live = subscription.live;
    if (!live) return Promise.resolve();
    const projected = rowsAtPath(message.result, live.resultPath);
    if (!projected) return Promise.resolve();
    const rows = projected.rows.filter((row): row is ReplicaRow => (
      row !== null && typeof row === "object" && !Array.isArray(row)
    ));
    return this.replica.materializeWindow({
      signature: subscription.key,
      kind: "live",
      entity: live.entity,
      key: live.key,
      rows,
      completeness: "complete",
      source: message.subscriptionRevision ? "server" : "cache",
      resultSkeleton: replaceRowsAtPath(message.result, live.resultPath, [], projected.scalar),
      resultPath: [...live.resultPath],
      scalar: projected.scalar,
      windowRevision: message.windowRevision,
      subscriptionRevision: message.subscriptionRevision,
      scope,
    });
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
      this.acknowledgeOptimisticSource(subscription.key, message.originCommandIds);
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
        replicaScope: message.replicaScope,
        windowRevision: message.windowRevision,
        subscriptionRevision: message.subscriptionRevision,
        originCommandIds: message.originCommandIds,
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
			return { ...message, type: "query.result", result: { ...previous.result, ...metadata, page }, originCommandIds: message.originCommandIds };
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
			return { ...message, type: "query.result", result, originCommandIds: message.originCommandIds };
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

  private subscribeReplicaTransport<Args extends JsonValue = JsonValue, Result extends JsonValue = JsonValue>(ref: FunctionReference<Args, Result>, args: Args = {} as Args, onMessage: ReplicaSubscriptionHandler) {
    this.connect();
    const key = querySubscriptionKey(ref, args);
    const existing = this.replicaSubscriptions.get(key);
    if (existing) {
      if (existing.unsubscribeTimer) {
        clearTimeout(existing.unsubscribeTimer);
        existing.unsubscribeTimer = undefined;
      }
      existing.listeners.add(onMessage);
      if (existing.lastMessage) {
        queueMicrotask(() => {
          if (existing.listeners.has(onMessage) && existing.lastMessage) {
            onMessage(this.materializeReplicaMessage(existing, existing.lastMessage));
          }
        });
      }
      return () => this.unsubscribeReplicaListener(key, onMessage);
    }

    const subscription: ReplicaSubscription = {
      id: randomID(),
      key,
      path: ref.path,
      entity: ref.replica?.table ?? ref.live?.entity ?? ref.path,
      columns: ref.replica?.columns,
      args,
      listeners: new Set([onMessage]),
      scope: this.replicaScope,
      opening: false,
      retryAttempt: 0,
      isUpToDate: false,
      forceFullIntegrity: false,
      verificationGeneration: 0,
      retiredEpochs: new Set(),
    };
    if (ref.replica) {
      this.replica.registerReplicaCollection(key, ref.replica, asReplicaRow(args) ?? {});
    }
    this.replicaSubscriptions.set(key, subscription);
    this.handlers.set(subscription.id, (message) => {
      const scope = subscription.scope ?? this.replicaScope;
      this.enqueueReplicaFrame(() => this.handleReplicaMessage(subscription, message as ReplicaMessage, scope));
    });
    this.startReplica(subscription);
    return () => this.unsubscribeReplicaListener(key, onMessage);
  }

  /** Subscribe to a bounded Replica Collection. */
  subscribeReplica<Args extends JsonValue = JsonValue, Result extends JsonValue = JsonValue>(ref: FunctionReference<Args, Result>, args: Args = {} as Args, onMessage: ReplicaSubscriptionHandler) {
    return this.subscribeReplicaTransport(ref, args, onMessage);
  }

  /** Watch a bounded Replica Collection through the normalized Local Replica. */
  watchReplica<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args) {
    const key = querySubscriptionKey(ref, args);
    const updateHandlers = new Set<WatchUpdateHandler>();
    let latestError: Error | undefined;
    let snapshotVersion = -1;
    let snapshotRows: T[] | undefined;
    let stateVersion = -1;
    let stateFreshness: ReplicaFreshness | undefined;
    let stateIsUpToDate: boolean | undefined;
    let snapshotState: ReplicaCollectionSubscriptionState | undefined;
    let releaseTimer: ReturnType<typeof setTimeout> | undefined;
    const notify = () => {
      for (const handler of updateHandlers) handler();
    };
    // Keep the ReplicaSubscription as transport/reconciliation state only. The
    // value returned by this watch always comes from normalized LocalReplica.
    const unsubscribeTransport = this.subscribeReplicaTransport(ref, args, (message) => {
      if (message.type === "replica.error") {
        latestError = new Error(message.error);
        notify();
      } else if (message.type === "replica.syncing" || message.type === "replica.reset") {
        latestError = undefined;
        notify();
      } else if (message.type === "replica.ready") {
        latestError = undefined;
        notify();
      } else if (message.type === "replica.snapshot") {
        latestError = undefined;
      }
    });
    const unsubscribeReplica = this.replica.subscribe(notify);
    const unsubscribeScope = this.onSessionScopeChange(() => {
      latestError = undefined;
      snapshotVersion = -1;
      snapshotRows = undefined;
      stateVersion = -1;
      stateFreshness = undefined;
      stateIsUpToDate = undefined;
      snapshotState = undefined;
      notify();
    });
    return {
      localReplicaResult: () => {
        if (latestError) throw latestError;
        if (!this.replica.hasLiveQuery(key)) return undefined;
        const version = this.replica.windowVersion(key);
        if (snapshotVersion === version) return snapshotRows;
        snapshotVersion = version;
        snapshotRows = this.replica.liveQuery(key).rows as unknown as T[];
        return snapshotRows;
      },
      localReplicaState: () => {
        if (latestError) throw latestError;
        if (!this.replica.hasLiveQuery(key)) return undefined;
        const version = this.replica.windowVersion(key);
        const freshness = this.replica.freshness();
        const isUpToDate = this.replicaSubscriptions.get(key)?.isUpToDate === true;
        if (
          stateVersion === version
          && stateFreshness === freshness
          && stateIsUpToDate === isUpToDate
        ) return snapshotState;
        stateVersion = version;
        stateFreshness = freshness;
        stateIsUpToDate = isUpToDate;
        const state = this.replica.collectionState(key);
        snapshotState = {
          ...state,
          isUpToDate,
          source: isUpToDate ? state.source : "cache",
          freshness: isUpToDate
            ? state.freshness
            : state.freshness === "offline" ? "offline" : "verifying",
        };
        return snapshotState;
      },
      status: () => ({
        isLoading: !this.replica.hasLiveQuery(key),
        isUpToDate: this.replicaSubscriptions.get(key)?.isUpToDate === true,
      }),
      onUpdate(handler: WatchUpdateHandler) {
        if (releaseTimer) {
          clearTimeout(releaseTimer);
          releaseTimer = undefined;
        }
        updateHandlers.add(handler);
        queueMicrotask(() => {
          if (updateHandlers.has(handler)) handler();
        });
        return () => {
          updateHandlers.delete(handler);
          if (updateHandlers.size > 0 || releaseTimer) return;
          releaseTimer = setTimeout(() => {
            releaseTimer = undefined;
            if (updateHandlers.size > 0) return;
            unsubscribeTransport();
            unsubscribeReplica();
            unsubscribeScope();
          }, 0);
        };
      },
    };
  }

  /**
   * Watch a structured Live Query through normalized LocalReplica rows. The
   * latest query result is retained only as the transport-shaped skeleton;
   * its row window is always rebuilt from LocalReplica membership/entities.
   */
  watchLiveQuery<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args) {
    const key = querySubscriptionKey(ref, args);
    const updateHandlers = new Set<WatchUpdateHandler>();
    let transportResult: JsonValue | undefined;
    let transportGeneration = 0;
    let snapshotToken = "";
    let snapshotResult: T | undefined;
    let latestError: Error | undefined;
    let notifyQueued = false;
    let releaseTimer: ReturnType<typeof setTimeout> | undefined;
    const notify = () => {
      if (notifyQueued) return;
      notifyQueued = true;
      queueMicrotask(() => {
        notifyQueued = false;
        for (const handler of updateHandlers) handler();
      });
    };
    const unsubscribeQuery = this.subscribeLiveQuery(ref, args, (message) => {
      if (message.type === "query.result") {
        transportResult = message.result;
        transportGeneration += 1;
        latestError = undefined;
        // LocalReplica has already published its atomic window swap before
        // this callback is emitted, so this is the single initial UI wake-up.
        notify();
      } else if (message.type === "query.error") {
        latestError = new GonvexClientError(`Query ${ref.path} failed: ${message.error}`, {
          code: "server", path: ref.path, operation: "query",
        });
        notify();
      }
    });
    // During the initial query result, LocalReplica notifies before the
    // transport-shaped skeleton is installed above. Suppress that empty
    // intermediate wake-up; later transactions notify directly from the
    // normalized store.
    const unsubscribeReplica = this.replica.subscribe(() => {
      if (transportResult !== undefined || this.replica.hasLiveQuery(key)) notify();
    });
    void this.replicaReady.then(() => notify());
    const unsubscribeScope = this.onSessionScopeChange(() => {
      transportResult = undefined;
      transportGeneration += 1;
      snapshotToken = "";
      snapshotResult = undefined;
      latestError = undefined;
      notify();
    });
    return {
      localLiveQueryResult: () => {
        if (latestError) throw latestError;
        if (!ref.live) return undefined;
        const offline = this.replica.freshness() === "offline" && ref.live.plan
          ? this.offlineLiveQuery(ref, args)
          : undefined;
        if (!this.replica.hasLiveQuery(key) && !offline?.supported) return undefined;
        const window = this.replica.getWindow(key);
        const skeleton = transportResult ?? window?.resultSkeleton;
        if (skeleton === undefined && (ref.live.resultPath?.length ?? 0) > 0) return undefined;
        const nextToken = `${this.replica.version()}:${transportGeneration}`;
        if (snapshotToken === nextToken) return snapshotResult;
        const materializedRows = offline?.supported
          ? offline.rows as unknown as JsonValue[]
          : this.replica.liveQuery(key).rows as unknown as JsonValue[];
        const base = skeleton ?? [];
        const projected = rowsAtPath(base, ref.live.resultPath ?? []);
        let nextResult = !projected
          ? base as T
          : replaceRowsAtPath(
            base,
            ref.live.resultPath ?? [],
            materializedRows,
            projected.scalar,
          ) as T;
        if (offline?.supported && projected) {
          nextResult = replaceOfflineLiveQueryMetadata(nextResult, ref.live.resultPath ?? [], offline) as T;
        }
        snapshotResult = nextResult;
        snapshotToken = nextToken;
        return snapshotResult;
      },
      onUpdate(handler: WatchUpdateHandler) {
        if (releaseTimer) {
          clearTimeout(releaseTimer);
          releaseTimer = undefined;
        }
        updateHandlers.add(handler);
        queueMicrotask(() => {
          if (updateHandlers.has(handler)) handler();
        });
        return () => {
          updateHandlers.delete(handler);
          if (updateHandlers.size > 0 || releaseTimer) return;
          releaseTimer = setTimeout(() => {
            releaseTimer = undefined;
            if (updateHandlers.size > 0) return;
            unsubscribeQuery();
            unsubscribeReplica();
            unsubscribeScope();
          }, 0);
        };
      },
    };
  }

  private async handleReplicaMessage(subscription: ReplicaSubscription, message: ReplicaMessage, scope = this.replicaScope) {
    if (scope !== this.replicaScope || (subscription.scope !== undefined && subscription.scope !== scope)) return;
    const current = () => this.replica.getWindow(subscription.key);
    if (message.type === "replica.snapshot") {
      if (!subscription.opening || replicaCursorIsStale(subscription, message.cursor)) return;
      this.clearReplicaRetry(subscription, true);
      subscription.opening = false;
      subscription.isUpToDate = false;
      raiseReplicaCursorFloor(subscription, message.cursor);
      const rows = boundReplicaRows(message.result, message.key, message.maxRows, message.maxBytes, message.orderBy, message.orderDirection);
      let hashes: Record<string, string> | undefined;
      if (message.hashes && message.digest) {
        const digest = await replicaHashesDigest(message.hashes);
        if (digest === message.digest) hashes = message.hashes;
      }
      const window = {
        signature: subscription.key,
        kind: "replica" as const,
        entity: subscription.entity,
        key: message.key,
        rows: rows.filter((row): row is ReplicaRow => asReplicaRow(row) !== undefined).map((row) => asReplicaRow(row)!),
        // New runtimes include authoritative integrity and truncation metadata
        // in bounded snapshots. Older runtimes remain verifying until ready.
        completeness: hashes && message.truncated !== true ? "complete" as const : "partial" as const,
        source: "server" as const,
        cursor: message.cursor,
        mode: message.mode,
        orderBy: message.orderBy,
        orderDirection: message.orderDirection,
        maxRows: message.maxRows,
        maxBytes: message.maxBytes,
        truncated: message.truncated,
        hashes,
        scope,
      };
      await this.replica.replaceWindow(window);
      const snapshot: ReplicaMessage = { ...message, result: this.replica.windowRows(subscription.key) };
      subscription.lastMessage = snapshot;
      this.emitReplicaMessage(subscription, snapshot, scope);
      return;
    }
    if (message.type === "replica.delta") {
      const prior = current();
      if (replicaCursorIsStale(subscription, message.cursor) || (prior?.cursor && message.cursor.revision < prior.cursor.revision)) return;
      this.clearReplicaRetry(subscription, true);
      raiseReplicaCursorFloor(subscription, message.cursor);
      await this.replica.applyWindowDelta({
        signature: subscription.key,
        kind: "replica",
        entity: subscription.entity,
        key: prior?.key ?? "id",
        upserts: (message.upserts ?? []).filter((row): row is ReplicaRow => asReplicaRow(row) !== undefined).map((row) => asReplicaRow(row)! ),
        deleted: message.deleted ?? [],
        completeness: prior?.completeness ?? "partial",
        source: "server",
        cursor: message.cursor,
        mode: prior?.mode,
        truncated: prior?.truncated,
        orderBy: prior?.orderBy,
        orderDirection: prior?.orderDirection,
        maxRows: prior?.maxRows,
        maxBytes: prior?.maxBytes,
        // A delta invalidates the prior full integrity map unless the server
        // supplied a complete replacement map with this frame.
        hashes: message.hashes,
      });
      const snapshot: ReplicaMessage = {
        type: "replica.snapshot", id: subscription.id, path: subscription.path,
        result: this.replica.windowRows(subscription.key), cursor: message.cursor,
        key: prior?.key ?? "id", mode: prior?.mode, orderBy: prior?.orderBy,
        orderDirection: prior?.orderDirection, maxRows: prior?.maxRows, maxBytes: prior?.maxBytes,
      };
      subscription.lastMessage = snapshot;
      this.acknowledgeOptimisticSource(subscription.key, message.originCommandIds);
      this.emitReplicaMessage(subscription, snapshot, scope);
      return;
    }
    if (message.type === "replica.reset") {
      this.clearReplicaRetry(subscription, true);
      subscription.isUpToDate = false;
      subscription.opening = false;
      subscription.cursorFloor = undefined;
      subscription.retiredEpochs.clear();
      subscription.lastMessage = undefined;
      await this.replica.removeWindow(subscription.key);
      this.emitReplicaMessage(subscription, message, scope);
      queueMicrotask(() => this.sendReplicaOpen(subscription));
      return;
    }
    if (message.type === "replica.syncing") {
      subscription.isUpToDate = false;
      this.emitReplicaMessage(subscription, message, scope);
      return;
    }
    if (message.type === "replica.needHashes") {
      subscription.isUpToDate = false;
      subscription.opening = false;
      this.emitReplicaMessage(subscription, { type: "replica.syncing", id: subscription.id, path: subscription.path, reason: "integrity-reconciling" }, scope);
      queueMicrotask(() => this.sendReplicaOpen(subscription));
      return;
    }
    if (message.type === "replica.ready") {
      const window = current();
      if (!window?.cursor || replicaCursorIsStale(subscription, message.cursor) || message.cursor.revision < window.cursor.revision) return;
      const hashesWereStored = window.hashes !== undefined;
      const hashes = window.hashes ?? await replicaRowsHashes(
        projectReplicaIntegrityRows(
          this.replica.committedWindowRows(subscription.key),
          subscription.columns,
        ),
        window.key,
      );
      const digest = await replicaHashesDigest(hashes);
      if (!message.digest || message.digest !== digest) {
        await this.handleReplicaMessage(subscription, { type: "replica.reset", id: subscription.id, path: subscription.path, reason: "integrity-mismatch" }, scope);
        return;
      }
      await this.acceptReplicaReady(subscription, message, hashes, hashesWereStored, scope);
      return;
    }
    if (message.type === "replica.error") {
      subscription.isUpToDate = false;
      subscription.opening = false;
      this.scheduleReplicaRetry(subscription);
    }
    this.emitReplicaMessage(subscription, message, scope);
  }

  private enqueueReplicaFrame(operation: () => Promise<void>) {
    const processing = this.replicaFrames.then(operation);
    this.replicaFrames = processing.catch(() => {
      this.replica.setFreshness("verifying");
    });
  }

  private enqueueReplicaTransaction(message: Extract<ServerMessage, { type: "replica.transaction" }>) {
    const scope = this.replicaScope;
    this.enqueueReplicaFrame(async () => {
      await this.replica.applyTransaction({
        cursor: message.cursor,
        originCommandId: message.originCommandId,
        provenance: message.provenance,
        changes: message.changes.map((change) => ({
          ...change,
          oldValue: asReplicaRow(change.oldValue),
          newValue: asReplicaRow(change.newValue),
        })),
      }, scope);
      if (message.originCommandId && !this.replica.hasPendingCommand(message.originCommandId)) {
        await this.ackOptimisticReducer(message.originCommandId);
      }
    });
  }

  private drainPendingReplicaTransactions() {
    const pending = this.pendingReplicaTransactions.splice(0);
    for (const message of pending) this.enqueueReplicaTransaction(message);
  }

  private async acceptReplicaReady(
    subscription: ReplicaSubscription,
    message: ReplicaReadyMessage,
    hashes: Record<string, string>,
    hashesWereStored: boolean,
    scope = this.replicaScope,
  ) {
    this.clearReplicaRetry(subscription, true);
    subscription.isUpToDate = true;
    subscription.opening = false;
    raiseReplicaCursorFloor(subscription, message.cursor);
    const window = this.replica.getWindow(subscription.key);
    if (window) {
      const completeness = message.truncated === true ? "partial" : "complete";
      const mode = message.mode ?? window.mode;
      const truncated = message.truncated ?? window.truncated;
      const readyAlreadyPersisted = hashesWereStored
        && window.source === "server"
        && window.cursor?.epoch === message.cursor.epoch
        && window.cursor.revision === message.cursor.revision
        && window.completeness === completeness
        && window.mode === mode
        && window.truncated === truncated;
      if (readyAlreadyPersisted) {
        // Connection-wide freshness remains a separate summary. Collection
        // state combines it with this subscription's isUpToDate bit, so one
        // ready window cannot make another hydrated cache authoritative.
        this.replica.setFreshness("current");
        this.emitReplicaMessage(subscription, message, scope);
        return;
      }
      await this.replica.replaceWindow({
        ...window, rows: this.replica.committedWindowRows(subscription.key), source: "server", cursor: message.cursor,
        completeness,
        mode, truncated,
        // Persist the exact integrity map verified above. Subsequent auth
        // rotations can then prove unchanged rows instead of requesting a full
        // upsert of every entity in every retained collection.
        hashes,
      });
    }
    this.replica.setFreshness("current");
    this.emitReplicaMessage(subscription, message, scope);
  }

  private async handleReplicaWatermark(revision: number) {
    if (!Number.isSafeInteger(revision) || revision < 0) return;
    const eligibleSignatures: string[] = [];
    for (const subscription of this.replicaSubscriptions.values()) {
      const cursor = this.replica.getWindow(subscription.key)?.cursor;
      if (
        !cursor
        || cursor.revision >= revision
        || !subscription.isUpToDate
        || subscription.opening
        || !this.replica.getWindow(subscription.key)?.hashes
      ) continue;
      eligibleSignatures.push(subscription.key);
    }
    await this.replica.advanceWatermark(revision, eligibleSignatures, this.replicaScope);
    this.processedReplicaWatermarkRevision = Math.max(
      this.processedReplicaWatermarkRevision,
      revision,
    );
    for (const pending of this.pendingCalls.values()) {
      if (
        (pending.kind !== "reducer" && pending.kind !== "action")
        || pending.committedRevision === undefined
        || pending.committedRevision > this.processedReplicaWatermarkRevision
      ) continue;
      const complete = pending.completeAfterReplicaWatermark;
      pending.completeAfterReplicaWatermark = undefined;
      complete?.();
    }
  }

  private emitReplicaMessage(subscription: ReplicaSubscription, message: ReplicaMessage, scope = this.replicaScope) {
    if (scope !== this.replicaScope || subscription.scope !== scope) return;
    const outgoing = this.materializeReplicaMessage(subscription, message);
    for (const listener of Array.from(subscription.listeners)) listener(outgoing);
  }

  private materializeReplicaMessage(subscription: ReplicaSubscription, message: ReplicaMessage): ReplicaMessage {
    if (message.type !== "replica.snapshot") return message;
    return {
      ...message,
      result: this.replica.windowRows(subscription.key) as unknown as JsonValue[],
    };
  }

  private materializeQueryMessage(subscription: QuerySubscription, message: ServerMessage): ServerMessage {
    return message;
  }

  private emitOptimisticEntity(entity: string) {
    void entity;
  }

  private acknowledgeOptimisticSource(source: string, originCommandIds: readonly string[] | undefined) {
    void source;
    for (const commandId of originCommandIds ?? []) this.replica.acknowledgeCommand(commandId);
  }

  private acknowledgeOptimisticQuerySnapshot(subscription: QuerySubscription, result: JsonValue) {
    void subscription;
    void result;
  }

  private markReplicaSubscriptionsOutOfDate() {
    for (const subscription of this.replicaSubscriptions.values()) {
      const wasUpToDate = subscription.isUpToDate;
      subscription.verificationGeneration += 1;
      subscription.isUpToDate = false;
      if (!wasUpToDate) continue;
      this.emitReplicaMessage(subscription, {
        type: "replica.syncing",
        id: subscription.id,
        path: subscription.path,
        reason: "disconnected",
      });
    }
  }

  private startReplica(subscription: ReplicaSubscription) {
    const cached = this.replica.getWindow(subscription.key);
    if (cached) {
      subscription.isUpToDate = false;
      const message: ReplicaMessage = {
        type: "replica.snapshot", id: subscription.id, path: subscription.path,
        result: this.replica.windowRows(subscription.key), cursor: cached.cursor ?? { epoch: "cache", revision: 0 },
        key: cached.key, mode: cached.mode, orderBy: cached.orderBy,
        orderDirection: cached.orderDirection, maxRows: cached.maxRows, maxBytes: cached.maxBytes,
      };
      subscription.lastMessage = message;
      this.emitReplicaMessage(subscription, message, this.replicaScope);
    }
    this.sendReplicaOpen(subscription);
  }

  private sendReplicaOpen(subscription: ReplicaSubscription) {
    // Replica rows are tenant-authorized state. A hook can mount while the
    // socket is connected but before session.ready/auth.result supplies the
    // authoritative visibility scope. Opening in that gap produces a correct
    // server rejection that is nevertheless transient and must not become a
    // fatal React snapshot error. The directive handlers resume every retained
    // subscription once the scope is active.
    if (this.authInFlight || !this.hasAuthoritativeReplicaScope || subscription.listeners.size === 0 || subscription.opening) return;
    const requestScope = this.replicaScope;
    subscription.scope = requestScope;
    subscription.opening = true;
    subscription.socketGeneration = this.socketGeneration;
    const open = this.replicaOpenRequest(subscription);
    if (this.serverCapabilities.replicaBatch === 1) {
      this.pendingReplicaOpens.add(subscription);
      if (!this.replicaOpenFlushTimer) {
        this.replicaOpenFlushTimer = setTimeout(() => this.flushReplicaOpens(), 0);
      }
      return;
    }
    this.send({ type: "replica.open", ...open });
  }

  private replicaOpenRequest(subscription: ReplicaSubscription): ReplicaOpenRequest {
    const window = this.replica.getWindow(subscription.key);
    const cursor = window?.cursor;
    const hashes = window?.hashes;
    const fullIntegrity = cursor !== undefined && hashes !== undefined;
    // Resume protocol state must describe only the committed server window.
    // windowRows() includes optimistic overlays for rendering and would cause
    // the server digest to disagree (and advertise uncommitted IDs as deletes).
    const keys = cursor ? [...(window?.ids ?? [])] : undefined;
    return {
      id: subscription.id,
      path: subscription.path,
      args: subscription.args,
      cursor,
      keys,
      hashes,
      digest: undefined,
      fullIntegrity: fullIntegrity || undefined,
    };
  }

  private flushReplicaOpens() {
    this.replicaOpenFlushTimer = undefined;
    const subscriptions = Array.from(this.pendingReplicaOpens);
    this.pendingReplicaOpens.clear();
    if (this.authInFlight || !this.hasAuthoritativeReplicaScope) {
      for (const subscription of subscriptions) subscription.opening = false;
      return;
    }
    const opens = subscriptions
      .filter((subscription) => (
        subscription.opening
        && subscription.listeners.size > 0
        && this.replicaSubscriptions.get(subscription.key) === subscription
      ))
      .map((subscription) => this.replicaOpenRequest(subscription));
    for (let offset = 0; offset < opens.length; offset += maxReplicaBatchOpens) {
      this.send({ type: "replica.openMany", opens: opens.slice(offset, offset + maxReplicaBatchOpens) });
    }
  }

  private unsubscribeReplicaListener(key: string, listener: ReplicaSubscriptionHandler) {
    const subscription = this.replicaSubscriptions.get(key);
    if (!subscription) return;
    subscription.listeners.delete(listener);
    if (subscription.listeners.size > 0 || subscription.unsubscribeTimer) return;
    subscription.unsubscribeTimer = setTimeout(() => {
      const latest = this.replicaSubscriptions.get(key);
      if (!latest || latest.listeners.size > 0) return;
      latest.unsubscribeTimer = undefined;
      this.clearReplicaRetry(latest);
      this.pendingReplicaOpens.delete(latest);
      this.replicaSubscriptions.delete(key);
      this.handlers.delete(latest.id);
      this.send({ type: "replica.close", id: latest.id });
    }, this.replicaSubscriptionRetentionMs);
  }

  private activateOutboxScope(): Promise<void> {
    const scope = reducerOutboxScope(this.url, this.auth, this.outboxEphemeralScope);
    if (scope === this.outboxScope) {
      return this.outboxReady ?? this.replicaReady;
    }

    const previousScope = this.outboxScope;
    const generation = ++this.outboxScopeGeneration;
    // Pending state from the previous authenticated identity must disappear
    // from every live projection immediately. Its durable rows remain scoped
    // in IndexedDB and can be resumed only if that identity returns.
    for (const reducerId of this.optimisticReducerIds) this.replica.rejectCommand(reducerId);
    this.optimisticReducerIds.clear();
    this.optimisticOutboxEntryIds.clear();
    if (isEphemeralOutboxScope(previousScope)) {
      void this.reducerOutbox.clear(previousScope);
    }
    this.outboxScope = scope;
    const ready = this.hasAuthoritativeReplicaScope
      ? this.restoreOutbox(scope, generation)
      : Promise.resolve();
    this.outboxReady = ready;
    return ready;
  }

  private async activateReplicaDirective(directive: ReplicaDirective): Promise<void> {
    if (
      directive.protocolVersion !== 1
      || !directive.visibilityScope.trim()
      || !directive.epoch.trim()
    ) {
      this.quarantineReplicaScope();
      throw new GonvexClientError("Runtime returned an invalid Local Replica scope", { code: "server", operation: "query" });
    }
    const scope = directive.visibilityScope.trim();
    if (this.hasAuthoritativeReplicaScope && this.replicaScope === scope) {
      await this.replicaReady;
      await this.outboxReady;
      return;
    }
    for (const reducerId of this.optimisticReducerIds) this.replica.rejectCommand(reducerId);
    this.optimisticReducerIds.clear();
    this.optimisticOutboxEntryIds.clear();
    this.resetReplicaScopeState();
    this.replicaScope = scope;
    this.hasAuthoritativeReplicaScope = true;
    this.replicaReady = this.replica.activateScope(scope);
    this.rotateSubscriptionScopes();
    const generation = this.outboxScopeGeneration;
    // Publish the recovery barrier before yielding to Replica storage. A
    // reducer may be invoked as soon as the auth result arrives, while the
    // session.ready scope activation is still hydrating. If outboxReady still
    // points at the old resolved promise, that reducer can enqueue an inflight
    // row which the concurrent recovery then mistakes for an abandoned call
    // and sends a second time with the same command ID.
    this.outboxReady = this.restoreOutbox(this.outboxScope, generation);
    await this.outboxReady;
  }

  private quarantineReplicaScope() {
    // Keep the durable prior identity scope intact for an authorized future
    // login, but make every synchronous selector fail closed immediately.
    // The random suffix prevents a denied scope from ever restoring rows.
    const scope = ["auth-denied", this.url, this.outboxEphemeralScope, randomID()].join("\u0000");
    for (const reducerId of this.optimisticReducerIds) this.replica.rejectCommand(reducerId);
    this.optimisticReducerIds.clear();
    this.optimisticOutboxEntryIds.clear();
    this.resetReplicaScopeState();
    this.replicaScope = scope;
    this.hasAuthoritativeReplicaScope = false;
    this.replicaReady = this.replica.activateScope(scope, true);
    this.rotateSubscriptionScopes();
  }

  private rejectMissingReplicaDirective() {
    this.quarantineReplicaScope();
    this.notifyAuthError("Runtime did not provide an authoritative Local Replica visibility scope");
  }

  private rejectReplicaDirective(error: unknown) {
    this.quarantineReplicaScope();
    this.notifyAuthError(error instanceof Error ? error.message : "Runtime returned an invalid Local Replica scope");
  }

  private async restoreOutbox(scope: string, generation: number) {
    await this.replicaReady;
    const entries = await this.reducerOutbox.loadAll(scope);
    if (
      this.manuallyClosed
      || generation !== this.outboxScopeGeneration
      || scope !== this.outboxScope
    ) return;
    for (const entry of entries) {
      if (entry.state === "committed" && (entry.patches?.length ?? 0) === 0) {
        await this.reducerOutbox.ack(entry.id);
        continue;
      }
      this.optimisticOutboxEntryIds.set(entry.idempotencyKey, entry.id);
      this.addOptimisticReducer(entry.idempotencyKey, entry.patches ?? [], entry.state === "committed");
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

  private addOptimisticReducer(reducerId: string, patches: OptimisticPatch[], accepted = false) {
    if (patches.length === 0 || this.optimisticReducerIds.has(reducerId)) return;
    this.optimisticReducerIds.add(reducerId);
    this.replica.applyOptimistic(reducerId, patches);
  }

  private async settleOptimisticReducer(reducerId: string) {
    await this.ackOptimisticReducer(reducerId);
  }

  private async rejectOptimisticReducer(reducerId: string, knownEntryId?: number) {
    this.optimisticReducerIds.delete(reducerId);
    this.replica.rejectCommand(reducerId);
    await this.ackOptimisticReducer(reducerId, knownEntryId);
  }

  private async ackOptimisticReducer(reducerId: string, knownEntryId?: number) {
    const entryId = knownEntryId ?? this.optimisticOutboxEntryIds.get(reducerId);
    this.optimisticOutboxEntryIds.delete(reducerId);
    this.optimisticReducerIds.delete(reducerId);
    if (entryId !== undefined) await this.reducerOutbox.ack(entryId);
  }

  private async drainOutbox() {
    await this.outboxReady;
    if (
      this.drainingOutbox
      || this.manuallyClosed
      || !this.canSendReducerNow()
    ) return;
    const drainScope = this.outboxScope;
    this.drainingOutbox = true;
    try {
      while (!this.manuallyClosed && this.socket?.readyState === WebSocket.OPEN) {
        const scope = this.outboxScope;
        const entry = await this.reducerOutbox.nextReady(scope, Date.now());
        if (!entry) return;
        if (scope !== this.outboxScope) return;
        if (this.directOutboxReducerIds.has(entry.idempotencyKey)) {
          // Scope recovery may observe an inflight row created by this live
          // process and reset it to pending under the assumption that a prior
          // process crashed. The foreground call still owns that command ID.
          // Restore the durable marker and wait for its real result instead of
          // registering a second response handler for the same command.
          await this.reducerOutbox.markInflight(entry.id);
          return;
        }
        if (!this.canSendReducerNow()) {
          await this.reducerOutbox.markPending(entry.id);
          return;
        }
        await this.reducerOutbox.markInflight(entry.id);
        if (scope !== this.outboxScope) return;
        if (!this.canSendReducerNow()) {
          await this.reducerOutbox.markPending(entry.id);
          return;
        }
        try {
          await this.call(
            "reducer",
            { kind: "reducer", path: entry.path },
            entry.args as JsonValue,
            this.timeouts.reducerTimeoutMs,
            entry.idempotencyKey,
            entry.idempotencyKey,
          );
          await this.reducerOutbox.markCommitted(entry.id);
          if ((entry.patches?.length ?? 0) > 0) {
            await this.settleOptimisticReducer(entry.idempotencyKey);
          } else {
            await this.ackOptimisticReducer(entry.idempotencyKey, entry.id);
          }
        } catch (error) {
          if (error instanceof GonvexClientError && error.code === "server") {
            await this.rejectOptimisticReducer(entry.idempotencyKey, entry.id);
            continue;
          }
          await this.reducerOutbox.fail(entry.id, reducerErrorMessage(error));
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

  reducer<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(
    ref: FunctionReference<Args, T>,
    args: Args,
    options: CallOptions & { offline: "queue" },
  ): Promise<T | QueuedReducerOutcome>;
  reducer<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args?: Args, options?: CallOptions): Promise<T>;
  reducer<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(
    ref: FunctionReference<Args, T>,
    args: Args = {} as Args,
    options: CallOptions = {},
  ): Promise<T | QueuedReducerOutcome> {
    if (options.offline === "queue" && ref.offline?.mode !== "allowed") {
      return Promise.reject(new GonvexClientError(
        `Reducer ${ref.path} does not allow offline queueing.`,
        { code: "disconnected", path: ref.path, operation: "reducer" },
      ));
    }
    const effectiveOptions: CallOptions = {
      ...options,
      offline: options.offline ?? (ref.offline?.mode === "allowed" ? "queue" : "reject"),
    };
    const reducerId = randomID();
    const patches = effectiveOptions.optimistic
      ?? optimisticPatchesFromReference(ref.optimistic?.transaction, args);
    if (patches.length === 0 && effectiveOptions.offline !== "queue") {
      return this.call<T>(
        "reducer",
        ref,
        args,
        effectiveOptions.timeoutMs ?? this.timeouts.reducerTimeoutMs,
        reducerId,
      );
    }
    return this.runOptimisticReducer<T>(ref, args, effectiveOptions, reducerId, patches);
  }

  private async runOptimisticReducer<T>(
    ref: FunctionReference,
    args: JsonValue,
    options: CallOptions,
    reducerId: string,
    patches: OptimisticPatch[],
  ): Promise<T | QueuedReducerOutcome> {
    // The startup recovery transaction converts abandoned inflight entries to
    // pending. Finish it before inserting a brand-new direct send, otherwise
    // recovery can mistake that live entry for a crashed reducer and race the
    // direct call through the background drain.
    await this.outboxReady;
    if (this.manuallyClosed) {
      throw new GonvexClientError(
        `Gonvex client was closed before reducer ${ref.path} could be sent.`,
        { code: "closed", path: ref.path, operation: "reducer" },
      );
    }
    this.directOutboxReducerIds.add(reducerId);
    let entryId: number | undefined;
    let entryAttempts = 0;
    try {
      const scope = this.outboxScope;
      const entry = await this.reducerOutbox.enqueue({
        scope,
        path: ref.path,
        args,
        idempotencyKey: reducerId,
        entityKeys: patches.map((patch) => `${patch.entity ?? patch.collection ?? ""}:${patch.rowId}`),
        patches,
        state: "inflight",
      });
      entryId = entry.id;
      entryAttempts = entry.attempts;
      if (this.manuallyClosed) {
        await this.reducerOutbox.ack(entry.id);
        throw new GonvexClientError(
          `Gonvex client was closed before reducer ${ref.path} could be sent.`,
          { code: "closed", path: ref.path, operation: "reducer" },
        );
      }
      if (scope !== this.outboxScope) {
        await this.reducerOutbox.ack(entry.id);
        throw new GonvexClientError(
          `Authentication changed before reducer ${ref.path} could be sent.`,
          { code: "disconnected", path: ref.path, operation: "reducer" },
        );
      }
      this.optimisticOutboxEntryIds.set(reducerId, entry.id);
      this.addOptimisticReducer(reducerId, patches);
      if (options.offline === "queue" && !this.canSendReducerNow()) {
        await this.reducerOutbox.markPending(entry.id);
        return { status: "queued", reducerId };
      }
      // The direct send is outbox-managed: a crash here replays the entry
      // with the same idempotency key, so the server must dedupe it.
      const result = await this.call<T>(
        "reducer",
        ref,
        args,
        options.timeoutMs ?? this.timeouts.reducerTimeoutMs,
        reducerId,
        reducerId,
      );
      await this.reducerOutbox.markCommitted(entry.id);
      if (patches.length > 0) {
        await this.settleOptimisticReducer(reducerId);
      } else {
        await this.ackOptimisticReducer(reducerId, entry.id);
      }
      return result;
    } catch (error: unknown) {
      if (isQueueableReducerError(error) && options.offline === "queue") {
        const queuedEntryId = this.optimisticOutboxEntryIds.get(reducerId) ?? entryId;
        if (queuedEntryId !== undefined) {
          await this.reducerOutbox.fail(queuedEntryId, reducerErrorMessage(error));
          // `fail` deliberately records backoff, but it does not own the
          // client's timer. The foreground queueable path must schedule the
          // next deterministic drain just like the background drain path.
          this.scheduleOutboxDrain(Math.min(30_000, 1_000 * (2 ** (entryAttempts + 1))));
        }
        return { status: "queued", reducerId };
      }
      await this.rejectOptimisticReducer(reducerId, entryId);
      throw error;
    } finally {
      this.directOutboxReducerIds.delete(reducerId);
      void this.drainOutbox();
    }
  }

  private canSendReducerNow(): boolean {
    if (globalThis.navigator?.onLine === false) return false;
    const socket = this.socket;
    if (!socket || socket.readyState !== WebSocket.OPEN || this.authInFlight) return false;
    const hasConfiguredAuth = Boolean(
      this.auth.project || this.auth.tenant || this.auth.token || this.auth.fetchToken,
    );
    return !hasConfiguredAuth || this.authenticatedSocketGeneration === this.socketGeneration;
  }

  action<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args, options: CallOptions = {}): Promise<T> {
    return this.call<T>("action", ref, args, options.timeoutMs ?? this.timeouts.actionTimeoutMs);
  }

  query<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(ref: FunctionReference<Args, T>, args: Args = {} as Args, options: CallOptions = {}): Promise<T> {
    this.connect();
    const id = randomID();
    const timeoutMs = options.timeoutMs ?? this.timeouts.queryTimeoutMs;
    return new Promise<T>((resolve, reject) => {
      const query: OneShotQuery = {
        id, path: ref.path, args, scope: ref.scope ?? "tenant",
        authorization: ref.authorization, reject,
      };
      const settle = () => {
        if (query.timeoutTimer) clearTimeout(query.timeoutTimer);
        this.oneShotQueries.delete(id);
        this.handlers.delete(id);
        this.notifyConnectionState();
      };
      if (timeoutMs > 0) {
        query.timeoutTimer = setTimeout(() => {
          settle();
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
          reject(new GonvexClientError(`Query ${ref.path} failed: ${message.error}`, { code: "server", path: ref.path, operation: "query" }));
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
  retryLiveQuery(ref: FunctionReference, args: JsonValue = {}) {
    const subscription = this.querySubscriptions.get(querySubscriptionKey(ref, args));
    if (!subscription || subscription.listeners.size === 0) return;
    subscription.serverSettled = false;
    subscription.socketGeneration = undefined;
    this.connect();
    this.sendSubscription(subscription);
  }

  /**
   * Flush a queue of reducers in one `reducer.callMany` frame (queue order,
   * one websocket round trip). Each entry settles independently — a failed
   * call does not reject the batch — so offline queues can apply per-row
   * outcomes. Falls back to the standard per-reducer path when the runtime
   * lacks batching or when a call needs generated/explicit optimism or durable
   * offline queuing, so there is never a second reducer-state implementation.
   */
  async reducerMany<T extends JsonValue = JsonValue>(
    calls: Array<{ ref: FunctionReference; args?: JsonValue }>,
    options: CallOptions = {},
  ): Promise<Array<{ status: "ok"; result: T | QueuedReducerOutcome } | { status: "error"; error: GonvexClientError }>> {
    if (calls.length === 0) return [];
    this.connect();
    const timeoutMs = options.timeoutMs ?? this.timeouts.reducerTimeoutMs;
    const settle = (promise: Promise<T | QueuedReducerOutcome>, path: string) => promise
      .then((result) => ({ status: "ok" as const, result }))
      .catch((error: unknown) => ({
        status: "error" as const,
        error: error instanceof GonvexClientError
          ? error
          : new GonvexClientError(String(error), { code: "server", path, operation: "reducer" }),
      }));
    const requiresStandardReducerPath = options.offline === "queue"
      || options.optimistic !== undefined
      || calls.some((call) => call.ref.optimistic?.transaction !== undefined || call.ref.scope === "control");
    if (this.serverCapabilities.reducerBatch !== 1 || requiresStandardReducerPath) {
      const outcomes: Array<{ status: "ok"; result: T | QueuedReducerOutcome } | { status: "error"; error: GonvexClientError }> = [];
      for (const call of calls) {
        outcomes.push(await settle(this.reducer<T>(call.ref, call.args ?? {}, options), call.ref.path));
      }
      return outcomes;
    }
    const registered = calls.map((call) => {
      const entry = this.registerCall<T>("reducer", call.ref, call.args ?? {}, timeoutMs);
      return { ...entry, path: call.ref.path, args: call.args ?? {} };
    });
    for (let offset = 0; offset < registered.length; offset += maxReplicaBatchOpens) {
      this.send({
        type: "reducer.callMany",
        calls: registered.slice(offset, offset + maxReplicaBatchOpens).map((entry) => ({
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
    kind: "reducer" | "action",
    ref: FunctionReference,
    args: JsonValue,
    timeoutMs: number,
    id?: string,
    idempotencyKey?: string,
  ): Promise<T> {
    this.connect();
    const callId = id ?? randomID();
    const effectiveIdempotencyKey = ref.scope === "control"
      ? (idempotencyKey ?? callId)
      : idempotencyKey;
    const entry = this.registerCall<T>(kind, ref, args, timeoutMs, callId, effectiveIdempotencyKey);
    if (kind === "reducer") {
      this.sendInvocation(ref, {
        type: "reducer.call",
        id: entry.id,
        path: ref.path,
        args,
        ...(ref.scope === "control" ? { scope: "control" as const } : {}),
        trace: { clientSentAtMs: entry.clientSentAtMs },
        ...(effectiveIdempotencyKey ? { idempotencyKey: effectiveIdempotencyKey } : {}),
      });
    } else {
      this.sendInvocation(ref, {
        type: "action.call", id: entry.id, path: ref.path, args,
        ...(ref.scope === "control" ? { scope: "control" as const } : {}),
        ...(effectiveIdempotencyKey ? { idempotencyKey: effectiveIdempotencyKey } : {}),
        trace: { clientSentAtMs: entry.clientSentAtMs },
      });
    }
    this.notifyConnectionState();
    return entry.promise;
  }

  private registerCall<T>(kind: "reducer" | "action", ref: FunctionReference, args: JsonValue, timeoutMs: number, callId = randomID(), idempotencyKey?: string): { id: string; clientSentAtMs: number; promise: Promise<T> } {
    const id = callId;
    const clientSentAtMs = nowMs();
    const promise = new Promise<T>((resolve, reject) => {
      const pending: PendingCall = {
        id,
        kind,
        path: ref.path,
        args,
        scope: ref.scope ?? "tenant",
        authorization: ref.authorization,
        idempotencyKey,
        socketGeneration: this.socketGeneration,
        reject,
      };
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
            `${kind === "reducer" ? "Reducer" : "Action"} ${ref.path} timed out after ${timeoutMs}ms. The operation may or may not have been applied.`,
            { code: "timeout", path: ref.path, operation: kind },
          ));
        }, timeoutMs);
      }
      this.pendingCalls.set(id, pending);
      this.handlers.set(id, (message) => {
        if (message.type === "control.watermark") {
          const complete = pending.completeAfterControlWatermark;
          pending.completeAfterControlWatermark = undefined;
          complete?.();
          return;
        }
        if (kind === "reducer" && message.type === "reducer.result") {
          this.replica.acknowledgeCommand(message.originCommandId, message.committedRevision);
          const complete = () => {
            settle();
            this.emitTelemetryFromCall(kind, id, ref.path, "ok", clientSentAtMs, message.trace);
            resolve(message.result as T);
          };
          const committedRevision = message.committedRevision;
          if (pending.scope === "control" && this.serverCapabilities.controlWatermark === 1) {
            pending.completeAfterControlWatermark = complete;
            return;
          }
          if (
            pending.scope === "tenant"
            && this.serverCapabilities.replicaWatermark === 1
            && typeof committedRevision === "number"
            && Number.isSafeInteger(committedRevision)
            && committedRevision > this.processedReplicaWatermarkRevision
          ) {
            pending.committedRevision = committedRevision;
            pending.completeAfterReplicaWatermark = complete;
            return;
          }
          complete();
        }
        if (kind === "reducer" && message.type === "reducer.error") {
          settle();
          this.emitTelemetryFromCall(kind, id, ref.path, "error", clientSentAtMs, message.trace, message.error);
          reject(new GonvexClientError(message.error, { code: "server", path: ref.path, operation: kind }));
        }
        if (kind === "action" && message.type === "action.result") {
          const complete = () => {
            settle();
            this.emitTelemetryFromCall(kind, id, ref.path, "ok", clientSentAtMs, message.trace);
            resolve(message.result as T);
          };
          const committedRevision = message.committedRevision;
          if (pending.scope === "control" && this.serverCapabilities.controlWatermark === 1) {
            pending.completeAfterControlWatermark = complete;
            return;
          }
          if (
            pending.scope === "tenant"
            && this.serverCapabilities.replicaWatermark === 1
            && typeof committedRevision === "number"
            && Number.isSafeInteger(committedRevision)
            && committedRevision > this.processedReplicaWatermarkRevision
          ) {
            pending.committedRevision = committedRevision;
            pending.completeAfterReplicaWatermark = complete;
            return;
          }
          complete();
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
      this.send({ type: "query.unsubscribe", id: latest.id });
      setTimeout(() => this.handlers.delete(latest.id), 500);
    }, this.querySubscriptionRetentionMs);
  }

  private sendSubscription(subscription: QuerySubscription) {
    if (subscription.listeners.size === 0) return;
    if (
      subscription.executionScope !== "control"
      && (this.authInFlight || (!!this.auth.tenant && !this.hasAuthoritativeReplicaScope))
    ) return;
    if (subscription.socketGeneration === this.socketGeneration) return;
    subscription.scope = this.replicaScope;
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
      ...(subscription.executionScope === "control" ? { scope: "control" as const } : {}),
      windowRevision: undefined,
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
        && !(
          subscription.executionScope !== "control"
          && (this.authInFlight || (!!this.auth.tenant && !this.hasAuthoritativeReplicaScope))
        )
      ))
      .map((subscription) => ({
        id: subscription.id,
        path: subscription.path,
        args: subscription.args,
        ...(subscription.executionScope === "control" ? { scope: "control" as const } : {}),
        windowRevision: undefined,
      }));
    for (let offset = 0; offset < subscribes.length; offset += maxReplicaBatchOpens) {
      this.send({ type: "query.subscribeMany", subscribes: subscribes.slice(offset, offset + maxReplicaBatchOpens) });
    }
  }

  private resumeQuerySubscriptions(force = false) {
    for (const subscription of this.querySubscriptions.values()) {
      if (subscription.listeners.size === 0) continue;
      if (force) subscription.socketGeneration = undefined;
      this.sendSubscription(subscription);
    }
  }

  private resumeReplicaSubscriptions() {
    for (const subscription of this.replicaSubscriptions.values()) {
      if (subscription.listeners.size === 0) continue;
      subscription.opening = false;
      subscription.socketGeneration = undefined;
      this.sendReplicaOpen(subscription);
    }
  }

  private scheduleReplicaRetry(subscription: ReplicaSubscription) {
    if (
      this.manuallyClosed
      || subscription.retryTimer
      || subscription.listeners.size === 0
      || this.replicaSubscriptions.get(subscription.key) !== subscription
    ) return;
    const delay = Math.min(250 * (2 ** subscription.retryAttempt), 5_000);
    subscription.retryAttempt += 1;
    subscription.retryTimer = setTimeout(() => {
      subscription.retryTimer = undefined;
      if (
        this.manuallyClosed
        || !this.isWebSocketConnected
        || subscription.listeners.size === 0
        || this.replicaSubscriptions.get(subscription.key) !== subscription
      ) return;
      subscription.opening = false;
      this.sendReplicaOpen(subscription);
    }, delay);
  }

  private clearReplicaRetry(subscription: ReplicaSubscription, resetAttempt = false) {
    if (subscription.retryTimer) {
      clearTimeout(subscription.retryTimer);
      subscription.retryTimer = undefined;
    }
    if (resetAttempt) subscription.retryAttempt = 0;
  }

  private requestSubscriptionSnapshot(subscription: QuerySubscription) {
    // Do not advertise the cache revision while recovering. Otherwise the
    // runtime can answer with another progress frame instead of a snapshot.
    subscription.serverSettled = false;
    subscription.socketGeneration = undefined;
    this.sendSubscription(subscription);
  }

  private sendOneShotQuery(query: OneShotQuery) {
    if (query.socketGeneration === this.socketGeneration) return;
    query.socketGeneration = this.socketGeneration;
    const message: ClientMessage = { type: "query.call", id: query.id, path: query.path, args: query.args, ...(query.scope === "control" ? { scope: "control" as const } : {}) };
    if (query.scope === "control" && query.authorization === "public" && this.authInFlight) this.sendNow(message);
    else this.send(message);
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
    for (const call of this.pendingCalls.values()) {
      if (call.scope === "control") this.sendPendingControlCall(call);
    }
    for (const subscription of this.replicaSubscriptions.values()) {
      if (subscription.listeners.size === 0) continue;
      this.clearReplicaRetry(subscription, true);
      subscription.opening = false;
      subscription.socketGeneration = undefined;
      this.sendReplicaOpen(subscription);
    }
  }

  private sendPendingControlCall(call: PendingCall) {
    if (call.socketGeneration === this.socketGeneration) return;
    call.socketGeneration = this.socketGeneration;
    const trace = { clientSentAtMs: nowMs() };
    if (call.kind === "reducer") {
      const message: ClientMessage = {
        type: "reducer.call",
        id: call.id,
        path: call.path,
        args: call.args,
        scope: "control",
        idempotencyKey: call.idempotencyKey ?? call.id,
        trace,
      };
      if (call.authorization === "public" && this.authInFlight) this.sendNow(message);
      else this.send(message);
      return;
    }
    const message: ClientMessage = {
      type: "action.call", id: call.id, path: call.path, args: call.args,
      scope: "control", idempotencyKey: call.idempotencyKey ?? call.id, trace,
    };
    if (call.authorization === "public" && this.authInFlight) this.sendNow(message);
    else this.send(message);
  }

  private sendInvocation(ref: FunctionReference, message: ClientMessage) {
    if (ref.scope === "control" && ref.authorization === "public" && this.authInFlight) {
      this.sendNow(message);
      return;
    }
    this.send(message);
  }

  private hasControlPlaneWork() {
    for (const query of this.oneShotQueries.values()) if (query.scope === "control") return true;
    for (const call of this.pendingCalls.values()) if (call.scope === "control") return true;
    return false;
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

  private resetReplicaScopeState() {
    this.processedReplicaWatermarkRevision = 0;
    for (const subscription of this.querySubscriptions.values()) {
      subscription.lastMessage = undefined;
      subscription.serverSettled = false;
    }
    for (const subscription of this.replicaSubscriptions.values()) {
      this.clearReplicaRetry(subscription, true);
      subscription.isUpToDate = false;
      subscription.cursorFloor = undefined;
      subscription.retiredEpochs.clear();
      subscription.lastMessage = undefined;
      subscription.opening = false;
      subscription.verificationGeneration += 1;
    }
    for (const handler of this.sessionScopeHandlers) handler();
  }

  /**
   * A subscription id is also the server's response routing key. Rotate it on
   * auth scope changes so a delayed frame from the previous tenant cannot be
   * delivered to a handler that now materializes into the new scope.
   */
  private rotateSubscriptionScopes() {
    for (const subscription of this.querySubscriptions.values()) {
      this.handlers.delete(subscription.id);
      this.pendingQuerySubscribes.delete(subscription);
      subscription.id = randomID();
      subscription.socketGeneration = undefined;
      subscription.scope = this.replicaScope;
      this.handlers.set(subscription.id, (message) => {
        const scope = subscription.scope ?? this.replicaScope;
        void this.handleQueryMessage(subscription, message, scope)
          .catch(() => this.replica.setFreshness("verifying"));
      });
    }
    for (const subscription of this.replicaSubscriptions.values()) {
      this.handlers.delete(subscription.id);
      this.pendingReplicaOpens.delete(subscription);
      subscription.id = randomID();
      subscription.socketGeneration = undefined;
      subscription.scope = this.replicaScope;
      this.handlers.set(subscription.id, (message) => {
        const scope = subscription.scope ?? this.replicaScope;
        // Snapshot/delta/ready frames for every Replica Collection must be
        // handled in wire order. In particular, `ready` verifies the entities
        // written by the preceding snapshot or delta. Running these handlers
        // concurrently after an auth-scope rotation lets `ready` inspect the
        // old window, falsely reset it for an integrity mismatch, and leave a
        // retained entity stale until another server change happens.
        this.enqueueReplicaFrame(() => this.handleReplicaMessage(
          subscription,
          message as ReplicaMessage,
          scope,
        ));
      });
    }
  }

  private emitTelemetryFromCall(
    kind: "reducer" | "action",
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
      device: event.device ?? browserTelemetryInfo(),
    });
  }

  /** Send a bounded native error-telemetry frame using authenticated connection attribution. */
  reportError(type: "register" | "envelope" | "heartbeat", payload: unknown): Promise<void> {
    return this.sendNativeError(type, payload);
  }

  private sendNativeError(type: "register" | "envelope" | "heartbeat", payload: unknown): Promise<void> {
    if (this.manuallyClosed) return Promise.reject(new Error("Gonvex client is closed"));
    this.connect();
    const id = randomID();
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.handlers.delete(id);
        reject(new Error("native error telemetry timed out"));
      }, 10_000);
      this.handlers.set(id, (message) => {
        if (message.type !== "error.ack") return;
        clearTimeout(timer);
        this.handlers.delete(id);
        if (message.error) reject(new Error(message.error));
        else resolve();
      });
      if (type === "register") {
        const registration = payload as { release?: string; environment?: string };
        this.send({ type: "error.register", id, release: registration.release, environment: registration.environment });
        return;
      }
      if (type === "heartbeat") {
        this.send({ type: "error.heartbeat", id });
        return;
      }
      const events = (payload as { events?: JsonValue[] }).events ?? [];
      this.send({ type: "error.envelope", id, events });
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
    const id = randomID();
    this.latestAuthFrameId = id;
    this.managedAuthAttempt?.ids.add(id);
    this.sendNow({
      type: "auth",
      id,
      token: this.auth.token,
      project: this.auth.project,
      tenant: this.auth.tenant,
      controlOnly: !this.auth.tenant,
      device: browserTelemetryInfo(),
		capabilities: { replicaReadyMany: 1, replicaWatermark: 1, queryPagePatch: 1, queryObjectPatch: 1, queryOrderDelta: 1, queryFanout: 1, queryResultBatch: 1 },
    });
  }

  private settleManagedAuthAttempt(id: string, error?: string) {
    const attempt = this.managedAuthAttempt;
    if (!attempt || !attempt.ids.has(id)) return;
    this.managedAuthAttempt = undefined;
    if (error) attempt.reject(new GonvexClientError(error, { code: "auth" }));
    else attempt.resolve();
  }

  private cancelManagedAuthAttempt(message: string) {
    const attempt = this.managedAuthAttempt;
    if (!attempt) return;
    this.managedAuthAttempt = undefined;
    attempt.reject(new GonvexClientError(message, { code: "auth" }));
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
    this.quarantineReplicaScope();
    this.notifyAuthError(error);
    this.flushPendingMessages();
  }

  private notifyAuthError(error: string) {
    for (const handler of Array.from(this.authErrorHandlers)) {
      handler(error);
    }
  }

  // A lost auth reply (for example, during a module-generation swap) that dropped
  // in-flight responses while the socket stayed up) used to leave
  // authInFlight stuck true forever: every later reducer/subscription
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

function replaceOfflineLiveQueryMetadata(
  result: JsonValue,
  path: readonly string[],
  offline: OfflineLiveQueryResult<unknown>,
): JsonValue {
  if (path.length === 0 || !isJsonRecord(result)) return result;
  const [head, ...tail] = path;
  if (tail.length === 0) {
    const next = { ...result };
    delete next.total;
    delete next.offset;
    delete next.limit;
    if (offline.total !== undefined) next.total = offline.total;
    if (offline.offset !== undefined) next.offset = offline.offset;
    if (offline.limit !== undefined) next.limit = offline.limit;
    return next;
  }
  const current = result[head!];
  return { ...result, [head!]: replaceOfflineLiveQueryMetadata(current ?? null, tail, offline) };
}

function querySubscriptionKey(ref: FunctionReference, args: JsonValue) {
  const contract = {
    scope: ref.scope ?? "tenant",
    delivery: ref.delivery ?? "oneShot",
    live: ref.live
      ? { entity: ref.live.entity, key: ref.live.key, resultPath: [...(ref.live.resultPath ?? [])], plan: ref.live.plan ?? null }
      : null,
  } as unknown as JsonValue;
  return `${ref.path}\u0000${stableStringify(args)}\u0000${stableStringify(contract)}`;
}

function countPendingCalls(calls: Map<string, PendingCall>, kind: "reducer" | "action") {
  let count = 0;
  for (const call of calls.values()) {
    if (call.kind === kind) count += 1;
  }
  return count;
}

function isQueueableReducerError(error: unknown) {
  return error instanceof GonvexClientError
    && (error.code === "disconnected" || error.code === "timeout");
}

function reducerErrorMessage(error: unknown) {
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

function boundReplicaRows(
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
  for (const row of sortReplicaRows(rows, orderBy, orderDirection)) {
    const key = replicaRowKeyValue(row, keyField);
    if (!key || seen.has(key)) continue;
    const size = replicaJSONSize(row);
    if (maxRows && kept.length >= maxRows) break;
    if (maxBytes && bytes + size > maxBytes) break;
    kept.push(row);
    seen.add(key);
    bytes += size;
  }
  return kept;
}

function applyReplicaDelta(
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
  const upsertKeys = new Set(upserts.map((row) => replicaRowKeyValue(row, keyField)).filter(Boolean));
  const remainder = current.filter((row) => {
    const key = replicaRowKeyValue(row, keyField);
    return key && !deletedSet.has(key) && !upsertKeys.has(key);
  });
  return boundReplicaRows(
    [...upserts, ...remainder],
    keyField,
    maxRows,
    maxBytes,
    orderBy,
    orderDirection,
  );
}

function sortReplicaRows(
  rows: JsonValue[],
  orderBy?: string,
  orderDirection?: "asc" | "desc",
) {
  if (!orderBy) return rows;
  const direction = orderDirection === "asc" ? 1 : -1;
  return [...rows].sort((left, right) => {
    const leftValue = replicaOrderValue(left, orderBy);
    const rightValue = replicaOrderValue(right, orderBy);
    if (leftValue === rightValue) return 0;
    if (leftValue === null) return 1;
    if (rightValue === null) return -1;
    return leftValue < rightValue ? -direction : direction;
  });
}

function replicaOrderValue(value: JsonValue, orderBy: string): string | number | null {
  if (!value || Array.isArray(value) || typeof value !== "object") return null;
  const candidate = value[orderBy];
  return typeof candidate === "string" || typeof candidate === "number" ? candidate : null;
}

function replicaRowKeyValue(value: JsonValue, keyField: string) {
  if (!value || Array.isArray(value) || typeof value !== "object") return "";
  const key = value[keyField];
  return key === null || key === undefined ? "" : String(key);
}

function replicaJSONSize(value: JsonValue) {
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
  if (auth.token) {
    const tokenIdentity = authIdentityKeyFromToken(auth);
    if (tokenIdentity) return tokenIdentity;
  }
  // Token-free fallback: an explicit identity hint carries the same claims a
  // token would supply, so both paths derive the same key for the same Account.
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

function reducerOutboxScope(url: string, auth: GonvexClientAuth, ephemeralScope: string) {
  const identity = authIdentityKey(auth);
  if (identity) return ["identity", url, identity].join("\u0000");
  if (auth.token || auth.identity || auth.fetchToken) {
    // Opaque tokens (or credentials installed before tenant selection) do not
    // expose a stable Account key. A per-client scope preserves current-session
    // queue semantics without ever restoring those rows under another Account.
    return ["ephemeral-auth", url, ephemeralScope].join("\u0000");
  }
  // Anonymous/dev-auth clients still need a stable namespace, but it must be
  // isolated by deployment and tenant. Once an authenticated identity is
  // installed, applyAuth switches away from this scope before restoring or
  // sending its durable reducers.
  return ["anonymous", url, auth.project ?? "", auth.tenant ?? ""].join("\u0000");
}

function isEphemeralOutboxScope(scope: string) {
  return scope.startsWith("ephemeral-auth\u0000");
}

function isJsonRecord(value: unknown): value is Record<string, JsonValue> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function replicaDirectiveFromAuthResult(result: JsonValue): ReplicaDirective | undefined {
  if (!isJsonRecord(result) || !isJsonRecord(result.replica)) return undefined;
  const directive = result.replica;
  if (
    directive.protocolVersion !== 1
    || typeof directive.scope !== "string"
    || typeof directive.visibilityScope !== "string"
    || typeof directive.epoch !== "string"
  ) return undefined;
  return {
    protocolVersion: 1,
    scope: directive.scope,
    visibilityScope: directive.visibilityScope,
    epoch: directive.epoch,
  };
}

function developerSessionTokenFromAuthResult(result: JsonValue): string | undefined {
  if (!isJsonRecord(result)) return undefined;
  const token = result.developerSessionToken;
  return typeof token === "string" && token.startsWith("gvx_dev_") ? token : undefined;
}

function artifactHashFromAuthResult(result: JsonValue): string | undefined {
  if (!isJsonRecord(result)) return undefined;
  const hash = result.artifactHash;
  return typeof hash === "string" && hash.length > 0 ? hash : undefined;
}

function hasOwn<T extends object>(value: T, key: PropertyKey) {
  return Object.prototype.hasOwnProperty.call(value, key);
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
