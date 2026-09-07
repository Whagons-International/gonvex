export type FunctionKind =
  | "query"
  | "mutation"
  | "action"
  | "http"
  | "internalMutation"
  | "liveGrid"
  | "sync";

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type FunctionManifestEntry = {
  kind: FunctionKind;
  handler: string;
  file: string;
  dependencies?: FunctionDependencies;
  sync?: SyncDefinition;
};

export type SyncDefinition = {
  table: string;
  key: string;
  columns: string[];
  equalFilters?: Record<string, string>;
  excludeWhenSet?: string[];
  visibilityTables?: string[];
  orderBy?: string;
  orderDirection?: "asc" | "desc";
  mode?: "eager" | "progressive";
  maxRows?: number;
  maxBytes?: number;
  retentionMs?: number;
};

export type SyncCursor = {
  epoch: string;
  revision: number;
};

export type FunctionDependencies = {
  reads?: Array<{ table: string; columns?: string[]; filters?: string[]; ordersBy?: string[]; windowed?: boolean; predicate?: string }>;
  writes?: Array<{ table: string; columns?: string[] }>;
  readsEphemeral?: boolean;
  writesEphemeral?: boolean;
  shareByPermissions?: boolean;
	shareByVisibility?: string;
	shareResultFrom?: string;
	shareResultField?: string;
};

export type SubscriptionRevision = { epoch: string; sequence: number };

export type MessageTrace = {
  clientSentAtMs?: number;
  serverReceivedAtMs?: number;
  serverMutationStartedAtMs?: number;
  serverMutationCommittedAtMs?: number;
  serverCompletedAtMs?: number;
  serverBroadcastScheduledAtMs?: number;
  serverChangeCommittedAtMs?: number;
  serverSubscriptionStartedAtMs?: number;
  serverSubscriptionSentAtMs?: number;
  serverDurationMs?: number;
  /** Actual socket-write start, after result batching and lock acquisition. */
  serverSocketWriteStartedAtMs?: number;
  /** Non-semantic top-level query performance metadata from result.perf. */
  queryPerf?: JsonValue;
};

export type BrowserTelemetryInfo = {
  deliveryDiagnostics?: {
    messageHandlerDelayMs?: number;
    messageDecodeMs: number;
    messageProcessingMs: number;
    longTaskSupported: boolean;
    longTaskCount: number;
    longTaskMaxMs: number;
    longTaskTotalMs: number;
    visibilityState?: string;
    online?: boolean;
    bufferedAmount: number;
  };
  userAgent?: string;
  browserName?: string;
  browserVersion?: string;
  deviceType?: string;
  platform?: string;
  language?: string;
  timezone?: string;
  viewportWidth?: number;
  viewportHeight?: number;
  hardwareConcurrency?: number;
  deviceMemory?: number;
  touchPoints?: number;
  connectionType?: string;
  effectiveConnectionType?: string;
};

export type QueryCacheDirective = {
  protocolVersion: 1;
  scope: string;
  /**
   * Visibility-only scope (project/tenant/user/permissions, no code epoch)
   * under which sync collections are persisted and resumed. Unlike `scope`,
   * it survives deploys; resume safety comes from the server's authoritative
   * reconcile. Absent on runtimes that predate deploy-stable sync scopes.
   */
  syncScope?: string;
  epoch: string;
  maxAgeMs: number;
};

export type ServerCapabilities = {
  /** WebSocket protocol generation implemented by this runtime. */
  protocolVersion?: number;
  /** Exact runtime build identifier, normally the deployed Git commit SHA. */
  runtimeVersion?: string;
  syncBatch?: 1;
  /** sync.ready frames always carry a collection integrity digest. */
  syncIntegrity?: 1;
  /** Server accepts `query.subscribeMany` batched subscription frames. */
  queryBatch?: 1;
	/** Server emits several independent query updates in one WebSocket frame. */
	queryResultBatch?: 1;
  /** Server accepts `mutation.callMany` batched offline-queue flushes. */
  mutationBatch?: 1;
  /** Server emits connection-level sync revision watermarks. */
  syncWatermark?: 1;
};

export type QuerySubscribeRequest = {
  id: string;
  path: string;
  args: JsonValue;
  cacheRevision?: string;
};

export type MutationCallRequest = {
  id: string;
  path: string;
  args: JsonValue;
  trace?: MessageTrace;
  /** Stable key for a replayable write; see the `mutation.call` message. */
  idempotencyKey?: string;
};

export type SyncOpenRequest = {
  id: string;
  path: string;
  args: JsonValue;
  cursor?: SyncCursor;
  keys?: string[];
  hashes?: Record<string, string>;
  digest?: string;
  fullIntegrity?: boolean;
};

export type SyncReady = {
  id: string;
  path?: string;
  cursor: SyncCursor;
  mode?: "eager" | "progressive";
  digest?: string;
  truncated?: boolean;
};

export type ClientCapabilities = {
  /** Client accepts coalesced `sync.readyMany` server frames. */
  syncReadyMany?: 1;
  /** Client accepts connection-level `sync.watermark` server frames. */
  syncWatermark?: 1;
	/** Client accepts keyed patches for object results with a `page` row array. */
	queryPagePatch?: 1;
	/** Client atomically applies keyed patches to named arrays in object results. */
	queryObjectPatch?: 1;
	/** Client applies compact front/back order deltas on keyed patches. */
	queryOrderDelta?: 1;
	/** Client accepts one query payload fanned out to multiple subscription IDs. */
	queryFanout?: 1;
	/** Client accepts several independent query updates in one WebSocket frame. */
	queryResultBatch?: 1;
};

export type KeyedCollectionPatch = {
	inserted?: JsonValue[];
	updated?: JsonValue[];
	deleted?: string[];
	order?: string[];
	prepend?: string[];
	append?: string[];
};

export type GonvexManifest = {
  project: string;
  generatedAt: string;
  functions: Record<string, FunctionManifestEntry>;
  schema: Record<string, JsonValue>;
};

export type ClientMessage =
  | { type: "auth"; id: string; token?: string; project?: string; tenant?: string; device?: BrowserTelemetryInfo; capabilities?: ClientCapabilities }
  | { type: "query.subscribe"; id: string; path: string; args: JsonValue; cacheRevision?: string }
  | { type: "query.unsubscribe"; id: string }
  | {
    type: "sync.open";
    id: string;
    path: string;
    args: JsonValue;
    cursor?: SyncCursor;
    keys?: string[];
    hashes?: Record<string, string>;
    digest?: string;
    fullIntegrity?: boolean;
  }
  | { type: "sync.openMany"; opens: SyncOpenRequest[] }
  | { type: "query.subscribeMany"; subscribes: QuerySubscribeRequest[] }
  | { type: "sync.close"; id: string }
  | {
    type: "mutation.call";
    id: string;
    path: string;
    args: JsonValue;
    trace?: MessageTrace;
    /**
     * Stable key for a replayable write from the client outbox. The runtime
     * executes the mutation once per key and serves the stored result to
     * every duplicate delivery.
     */
    idempotencyKey?: string;
  }
  | { type: "mutation.callMany"; calls: MutationCallRequest[] }
  | { type: "action.call"; id: string; path: string; args: JsonValue; trace?: MessageTrace }
  | {
    type: "telemetry.event";
    id: string;
    kind: "query" | "mutation" | "action";
    path: string;
    reason?: "initial" | "invalidate" | "recover" | "timeout";
    outcome: "ok" | "error";
    error?: string;
    clientSentAtMs?: number;
    clientReceivedAtMs: number;
    clientDurationMs?: number;
    trace?: MessageTrace;
    device?: BrowserTelemetryInfo;
  };

export type ServerMessage =
	| {
		type: "query.batch";
		messages: ServerMessage[];
	}
	| {
		type: "query.fanout";
		queryType: "query.result" | "query.progress" | "query.patch" | "query.pagePatch" | "query.objectPatch";
		ids: string[];
		path?: string;
		result?: JsonValue;
		reason?: "initial" | "invalidate" | "recover";
		trace?: MessageTrace;
		cacheScope?: string;
		cacheRevision?: string;
		subscriptionRevision?: SubscriptionRevision;
		baseRevision?: SubscriptionRevision;
		throughRevision?: SubscriptionRevision;
		inserted?: JsonValue[];
		updated?: JsonValue[];
		deleted?: string[];
		order?: string[];
		prepend?: string[];
		append?: string[];
		collections?: Record<string, KeyedCollectionPatch>;
		mutationIds?: string[];
	}
  | {
    type: "session.ready";
    project?: string;
    tenant?: string;
    queryCache?: QueryCacheDirective;
    capabilities?: ServerCapabilities;
  }
  | { type: "session.scope"; queryCache?: QueryCacheDirective }
  | { type: "auth.result"; id: string; result: JsonValue }
  | { type: "auth.error"; id: string; error: string }
  | {
    type: "query.result";
    id: string;
    path?: string;
    result: JsonValue;
    reason?: "initial" | "invalidate" | "recover";
    trace?: MessageTrace;
    cacheScope?: string;
    cacheRevision?: string;
    subscriptionRevision?: SubscriptionRevision;
    mutationIds?: string[];
  }
  | {
    type: "query.progress";
    id: string;
    path?: string;
    reason?: "initial" | "invalidate" | "recover";
    throughRevision: SubscriptionRevision;
    trace?: MessageTrace;
    mutationIds?: string[];
  }
  | {
    type: "query.patch";
    id: string;
    path?: string;
    reason?: "initial" | "invalidate" | "recover";
    baseRevision: SubscriptionRevision;
    subscriptionRevision: SubscriptionRevision;
    inserted?: JsonValue[];
    updated?: JsonValue[];
    deleted?: string[];
    order?: string[];
		prepend?: string[];
		append?: string[];
    cacheScope?: string;
    cacheRevision?: string;
    trace?: MessageTrace;
    mutationIds?: string[];
  }
	| {
		type: "query.pagePatch";
		id: string;
		path?: string;
		reason?: "initial" | "invalidate" | "recover";
		baseRevision: SubscriptionRevision;
		subscriptionRevision: SubscriptionRevision;
		result?: JsonValue;
		inserted?: JsonValue[];
		updated?: JsonValue[];
		deleted?: string[];
		order?: string[];
		prepend?: string[];
		append?: string[];
		cacheScope?: string;
		cacheRevision?: string;
		trace?: MessageTrace;
		mutationIds?: string[];
	}
	| {
		type: "query.objectPatch";
		id: string;
		path?: string;
		reason?: "initial" | "invalidate" | "recover";
		baseRevision: SubscriptionRevision;
		subscriptionRevision: SubscriptionRevision;
		collections: Record<string, KeyedCollectionPatch>;
		cacheScope?: string;
		cacheRevision?: string;
		trace?: MessageTrace;
		mutationIds?: string[];
	}
  | {
    type: "sync.snapshot";
    id: string;
    path?: string;
    result: JsonValue[];
    cursor: SyncCursor;
    key: string;
    orderBy?: string;
    orderDirection?: "asc" | "desc";
    mode?: "eager" | "progressive";
    maxRows?: number;
    maxBytes?: number;
    hashes?: Record<string, string>;
  }
  | {
    type: "sync.delta";
    id: string;
    path?: string;
    cursor: SyncCursor;
    upserts?: JsonValue[];
    deleted?: string[];
    mutationIds?: string[];
    hashes?: Record<string, string>;
    digest?: string;
  }
  | ({ type: "sync.ready"; digest?: string } & SyncReady)
  | { type: "sync.readyMany"; ready: SyncReady[] }
  | { type: "sync.watermark"; revision: number }
  | { type: "sync.needHashes"; id: string; path?: string }
  // Client-local status frame emitted when a formerly authoritative materialized
  // collection must be reconciled before it can be trusted again.
  | {
    type: "sync.syncing";
    id: string;
    path?: string;
    reason: "disconnected" | "reconciling" | "listener-reconnecting" | "integrity-reconciling";
  }
  | {
    type: "sync.reset";
    id: string;
    path?: string;
    reason: "cursor-expired" | "definition-changed" | "visibility-changed" | "integrity-mismatch" | "integrity-missing" | "recover";
  }
  | { type: "sync.error"; id: string; path?: string; error: string }
  | { type: "query.error"; id: string; path?: string; error: string }
  | { type: "mutation.result"; id: string; path?: string; result: JsonValue; trace?: MessageTrace }
  | { type: "mutation.error"; id: string; path?: string; error: string; trace?: MessageTrace }
  | { type: "action.result"; id: string; path?: string; result: JsonValue; trace?: MessageTrace }
  | { type: "action.error"; id: string; path?: string; error: string; trace?: MessageTrace }
  | { type: "system.reload"; reason: string };
