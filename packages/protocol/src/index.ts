export type FunctionKind = "query" | "reducer" | "action";
export type ExecutionScope = "tenant" | "control";

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

/** The recursive, language-neutral value schema used by TypeScript modules. */
export type ModuleSchema =
  | StringSchema
  | NumberSchema
  | BooleanSchema
  | NullSchema
  | AnySchema
  | IdSchema
  | LiteralSchema
  | ArraySchema
  | ObjectSchema
  | RecordSchema
  | OptionalSchema;

export type StringSchema = {
  kind: "string";
  format?: "email" | "uri" | "uuid" | "datetime";
  minLength?: number;
  maxLength?: number;
};
export type NumberSchema = { kind: "number"; integer?: boolean; minimum?: number; maximum?: number };
export type BooleanSchema = { kind: "boolean" };
export type NullSchema = { kind: "null" };
export type AnySchema = { kind: "any" };
export type IdSchema = { kind: "id"; entity: string };
export type LiteralSchema = { kind: "literal"; value: JsonValue };
export type ArraySchema = { kind: "array"; items: ModuleSchema };
export type ObjectSchema = { kind: "object"; fields: Record<string, ModuleSchema>; allowUnknown?: boolean };
export type RecordSchema = { kind: "record"; values: ModuleSchema };
export type OptionalSchema = { kind: "optional"; value: ModuleSchema };

export type FunctionManifestEntry = {
  kind: FunctionKind;
  handler: string;
  file: string;
  args?: ModuleSchema;
  result?: ModuleSchema;
  internal?: boolean;
  public?: boolean;
  delivery?: "oneShot" | "live" | "replica";
  dependencies?: FunctionDependencies;
  replica?: ReplicaCollectionDefinition;
  /** Reducer delivery policy declared by a TypeScript module. */
  offline?: JsonValue;
  /** Ordered atomic optimistic transaction declared by a TypeScript module. */
  optimistic?: JsonValue;
  interactive?: boolean;
  classification?: "interactive" | "system" | "internal";
  description?: string;
  agent?: {
    tags?: string[];
    confirmation?: "none" | "required" | "destructive";
  };
};

export type ReplicaCollectionDefinition = {
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

export type ReplicaCursor = {
  epoch: string;
  revision: number;
};

export type PublicInvocationProvenance = {
  rootCommandId: string;
  rootChannel?: "ui" | "agent" | "api" | "scheduler" | "system" | (string & {});
  channel: "ui" | "agent" | "api" | "scheduler" | "system" | (string & {});
  actorAccountId?: string;
  actorMemberId?: string;
  onBehalfOfMemberId?: string;
  agentExecutionId?: string;
};

export type ReplicaChange = {
  entity: string;
  id: string;
  operation: "insert" | "update" | "delete";
  /** Legacy optional image. Omitted when replicaNoOldValues is negotiated. */
  oldValue?: JsonValue;
  newValue?: JsonValue;
  changedColumns?: string[];
};

export type FunctionDependencies = {
	shareByPermissions?: boolean;
	liveQueryPlan?: LiveQueryPlan;
	nonOptimisticReason?: string;
	shareResultFrom?: string;
	shareResultField?: string;
};

export type LiveQueryPlan = {
  table: string;
  key: string;
  columns?: string[];
  resultPath?: string[];
  where?: LiveExpression;
  search?: { argument: string; columns: string[] };
  filters?: { argument: string; allowedColumns: string[]; allowedOperators: FilterOperator[]; columnTypes?: Record<string, "text" | "number"> };
  sort?: { columnArgument: string; directionArgument: string; defaultColumn: string; defaultDirection: "asc" | "desc"; allowedColumns: string[] };
  window?: { offsetArgument: string; limitArgument: string; defaultLimit: number; maxLimit: number; count?: "exact" };
  serverOnly?: boolean;
};

export type FilterOperator = "contains" | "notContains" | "equals" | "notEquals" | "startsWith" | "endsWith" | "empty" | "notEmpty" | "oneOf" | "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" | "inRange";

export type LiveExpression = {
  operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in" | "contains" | "containsInsensitive" | "range" | "and" | "or" | "not" | "server" | "inRelation" | "arrayContains";
  column?: string;
  value?: LiveValue;
  valueTo?: LiveValue;
  children?: LiveExpression[];
  relation?: { table: string; column: string; where?: LiveExpression };
};

export type LiveValue = { context?: "account.id" | "member.id" | "tenant.id"; argument?: string; literal?: JsonValue };

export type SubscriptionRevision = { epoch: string; sequence: number };

export type MessageTrace = {
  clientSentAtMs?: number;
  serverReceivedAtMs?: number;
  serverReducerStartedAtMs?: number;
  serverReducerCommittedAtMs?: number;
  serverCompletedAtMs?: number;
  serverBroadcastScheduledAtMs?: number;
  serverChangeCommittedAtMs?: number;
  serverSubscriptionStartedAtMs?: number;
  serverSubscriptionSentAtMs?: number;
  serverDurationMs?: number;
  /** Non-semantic top-level query performance metadata from result.perf. */
  queryPerf?: JsonValue;
};

export type BrowserTelemetryInfo = {
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

export type ReplicaDirective = {
  protocolVersion: 1;
  scope: string;
  /** Visibility-only scope for persistent, authoritatively reconciled rows. */
  visibilityScope: string;
  epoch: string;
};

export type ServerCapabilities = {
  /** Can omit old transaction row images when the client advertises version 1. */
  replicaNoOldValues?: 1;
  /** Connection-local field patches, decoded before applying logical messages. */
  replicaFieldPatches?: 1;
  replicaSnapshotPages?: 1;
  /** Accepts cursor + xor1 row-hash digest, with needHashes repair. */
  replicaResume?: 1;
  /** Accepts held windowRevision and confirms unchanged results with progress. */
  queryResume?: 1;
  preserveSubscriptions?: 1;
  /** Server accepts up to 50 performance events per frame, using auth.device by default. */
  telemetryBatch?: 1;
  /** WebSocket protocol generation implemented by this runtime. */
  protocolVersion?: number;
  /** Exact runtime build identifier, normally the deployed Git commit SHA. */
  runtimeVersion?: string;
  replicaBatch?: 1;
  /** replica.ready frames always carry a collection integrity digest. */
  replicaIntegrity?: 1;
  /** Server accepts `query.subscribeMany` batched subscription frames. */
  queryBatch?: 1;
	/** Server emits several independent query updates in one WebSocket frame. */
	queryResultBatch?: 1;
  /** Server accepts `reducer.callMany` batched command-outbox flushes. */
  reducerBatch?: 1;
  /** Server emits connection-level replica revision watermarks. */
  replicaWatermark?: 1;
  /** Server closes each successful Control write after refreshing this connection's Control subscriptions. */
  controlWatermark?: 1;
};

export type QuerySubscribeRequest = {
  id: string;
  path: string;
  args: JsonValue;
  scope?: ExecutionScope;
  windowRevision?: string;
};

export type ReducerCallRequest = {
  id: string;
  path: string;
  args: JsonValue;
  scope?: ExecutionScope;
  trace?: MessageTrace;
  /** Stable key for a replayable command; see the `reducer.call` message. */
  idempotencyKey?: string;
  artifactHash?: string;
  clientContract?: number;
  receiptPath?: string;
  intentEntropy?: string;
};

export type ReplicaOpenRequest = {
  id: string;
  path: string;
  args: JsonValue;
  cursor?: ReplicaCursor;
  keys?: string[];
  hashes?: Record<string, string>;
  /** Optional xor1: digest selects aggregate verification on replicaResume runtimes. */
  digest?: string;
  fullIntegrity?: boolean;
};

export type ReplicaReady = {
  id: string;
  path?: string;
  cursor: ReplicaCursor;
  mode?: "eager" | "progressive";
  digest: string;
  truncated?: boolean;
};

export type ClientCapabilities = {
  /** Applies transactions from its local base; does not require oldValue. */
  replicaNoOldValues?: 1;
  /** Connection-local field patches, decoded before applying logical messages. */
  replicaFieldPatches?: 1;
  /** Accepts atomically activated replica.snapshotPage frames. */
  replicaSnapshotPages?: 1;
  /** Honors auth.result subscriptionsPreserved on an in-place refresh. */
  preserveSubscriptions?: 1;
  /** Preserve selected tenant authentication on control-only sockets. */
  tenantControl?: 1;
  telemetryBatch?: 1;
  /** Client accepts coalesced `replica.readyMany` server frames. */
  replicaReadyMany?: 1;
  /** Client accepts connection-level `replica.watermark` server frames. */
  replicaWatermark?: 1;
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
  | { type: "auth"; clientContract?: number; id: string; token?: string; project?: string; tenant?: string; controlOnly?: boolean; public?: boolean; device?: BrowserTelemetryInfo; capabilities?: ClientCapabilities }
  | { type: "query.call"; id: string; path: string; args: JsonValue; scope?: ExecutionScope }
  | { type: "query.subscribe"; id: string; path: string; args: JsonValue; scope?: ExecutionScope; windowRevision?: string }
  | { type: "query.unsubscribe"; id: string }
  | {
    type: "replica.open";
    id: string;
    path: string;
    args: JsonValue;
    cursor?: ReplicaCursor;
    keys?: string[];
    hashes?: Record<string, string>;
    digest?: string;
    fullIntegrity?: boolean;
  }
  | { type: "replica.openMany"; opens: ReplicaOpenRequest[] }
  | { type: "query.subscribeMany"; subscribes: QuerySubscribeRequest[] }
  | { type: "replica.close"; id: string }
  | {
    type: "reducer.call";
    id: string;
    path: string;
    args: JsonValue;
    scope?: ExecutionScope;
    trace?: MessageTrace;
    /**
     * Stable key for a replayable command from the client outbox. The runtime
     * executes the reducer once per key and serves the stored result to
     * every duplicate delivery.
     */
    idempotencyKey?: string;
    artifactHash?: string;
  clientContract?: number;
  receiptPath?: string;
    intentEntropy?: string;
  }
  | { type: "reducer.callMany"; calls: ReducerCallRequest[] }
  | { type: "action.call"; id: string; path: string; args: JsonValue; scope?: ExecutionScope; idempotencyKey?: string; trace?: MessageTrace }
  | { type: "error.register"; id: string; release?: string; environment?: string }
  | { type: "error.envelope"; id: string; events: JsonValue[] }
  | { type: "error.heartbeat"; id: string }
  | { type: "telemetry.batch"; events: Array<Omit<Extract<ClientMessage, { type: "telemetry.event" }>, "type">> }
  | {
    type: "telemetry.event";
    id: string;
    kind: "query" | "reducer" | "action";
    path: string;
    reason?: "initial" | "change" | "recover";
    outcome: "ok" | "error";
    error?: string;
    clientSentAtMs?: number;
    clientReceivedAtMs: number;
    clientDurationMs?: number;
    trace?: MessageTrace;
    device?: BrowserTelemetryInfo;
  };

/**
 * Machine-readable disposition of a failed Reducer call.
 *
 * - `rejected`: the Reducer refused the intent (application throw, validation,
 *   permission, conflict). Retrying the same call cannot succeed.
 * - `transient`: infrastructure failed and the transaction rolled back. The
 *   same idempotency key may be retried.
 * - `update_required`: the client's Reducer artifact or contract is stale.
 * - `unauthenticated`: the connection has no usable tenant session. Keep the
 *   intent, re-authenticate, and retry.
 */
export type ReducerErrorClass = "rejected" | "transient" | "update_required" | "unauthenticated";

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
		reason?: "initial" | "change" | "recover";
		trace?: MessageTrace;
		replicaScope?: string;
		windowRevision?: string;
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
		originCommandIds?: string[];
	}
  | {
    type: "replica.transaction";
    cursor: ReplicaCursor;
    originCommandId?: string;
    provenance?: PublicInvocationProvenance;
    changes: ReplicaChange[];
  }
  | {
    type: "session.ready";
    project?: string;
    tenant?: string;
    replica?: ReplicaDirective;
    capabilities?: ServerCapabilities;
  }
  | { type: "auth.result"; id: string; result: JsonValue }
  | { type: "auth.error"; id: string; error: string; code?: string }
  | { type: "error.ack"; id: string; accepted?: number; fingerprints?: string[]; error?: string }
  | { type: "support.command"; id: string; result: JsonValue }
  | {
    type: "query.result";
    id: string;
    path?: string;
    result: JsonValue;
    reason?: "initial" | "change" | "recover";
    trace?: MessageTrace;
    replicaScope?: string;
    windowRevision?: string;
    subscriptionRevision?: SubscriptionRevision;
    originCommandIds?: string[];
  }
  | {
    type: "query.progress";
    id: string;
    path?: string;
    reason?: "initial" | "change" | "recover";
    throughRevision: SubscriptionRevision;
    trace?: MessageTrace;
    originCommandIds?: string[];
  }
  | {
    type: "query.patch";
    id: string;
    path?: string;
    reason?: "initial" | "change" | "recover";
    baseRevision: SubscriptionRevision;
    subscriptionRevision: SubscriptionRevision;
    inserted?: JsonValue[];
    updated?: JsonValue[];
    deleted?: string[];
    order?: string[];
		prepend?: string[];
		append?: string[];
    replicaScope?: string;
    windowRevision?: string;
    trace?: MessageTrace;
    originCommandIds?: string[];
  }
	| {
		type: "query.pagePatch";
		id: string;
		path?: string;
		reason?: "initial" | "change" | "recover";
		baseRevision: SubscriptionRevision;
		subscriptionRevision: SubscriptionRevision;
		result?: JsonValue;
		inserted?: JsonValue[];
		updated?: JsonValue[];
		deleted?: string[];
		order?: string[];
		prepend?: string[];
		append?: string[];
		replicaScope?: string;
		windowRevision?: string;
		trace?: MessageTrace;
		originCommandIds?: string[];
	}
	| {
		type: "query.objectPatch";
		id: string;
		path?: string;
		reason?: "initial" | "change" | "recover";
		baseRevision: SubscriptionRevision;
		subscriptionRevision: SubscriptionRevision;
		collections: Record<string, KeyedCollectionPatch>;
		replicaScope?: string;
		windowRevision?: string;
		trace?: MessageTrace;
		originCommandIds?: string[];
	}
  | {
    type: "replica.snapshot";
    id: string;
    path?: string;
    result: JsonValue[];
    cursor: ReplicaCursor;
    key: string;
    orderBy?: string;
    orderDirection?: "asc" | "desc";
    mode?: "eager" | "progressive";
    maxRows?: number;
    maxBytes?: number;
    hashes?: Record<string, string>;
    digest?: string;
    truncated?: boolean;
  }
  | {
    type: "replica.snapshotPage";
    snapshotId: string;
    scope: string;
    visibilityScope: string;
    sequence: number;
    last: boolean;
    id: string;
    path?: string;
    result: JsonValue[];
    cursor: ReplicaCursor;
    key: string;
    orderBy?: string;
    orderDirection?: "asc" | "desc";
    mode?: "eager" | "progressive";
    maxRows?: number;
    maxBytes?: number;
    digest?: string;
    truncated?: boolean;
  }
  | {
    type: "replica.delta";
    id: string;
    path?: string;
    cursor: ReplicaCursor;
    upserts?: JsonValue[];
    deleted?: string[];
    originCommandIds?: string[];
    hashes?: Record<string, string>;
    digest?: string;
  }
  | ({ type: "replica.ready" } & ReplicaReady)
  | { type: "replica.readyMany"; ready: ReplicaReady[] }
  | { type: "replica.watermark"; revision: number }
  | { type: "replica.needHashes"; id: string; path?: string }
  // Client-local status frame emitted when a formerly authoritative materialized
  // collection must be reconciled before it can be trusted again.
  | {
    type: "replica.syncing";
    id: string;
    path?: string;
    reason: "disconnected" | "reconciling" | "listener-reconnecting" | "integrity-reconciling";
  }
  | {
    type: "replica.reset";
    id: string;
    path?: string;
    reason: "cursor-expired" | "definition-changed" | "visibility-changed" | "integrity-mismatch" | "integrity-missing" | "recover";
  }
  | { type: "replica.error"; id: string; path?: string; error: string }
  | { type: "query.error"; id: string; path?: string; error: string }
  | { type: "control.watermark"; id: string }
  | { type: "reducer.result"; id: string; path?: string; result: JsonValue; originCommandId: string; committedRevision?: number; trace?: MessageTrace }
  | {
    type: "reducer.error"; id: string; path?: string; error: string; trace?: MessageTrace;
    /** Absent from runtimes before 0.5.2-staging.15; treat absence as a legacy rejection. */
    class?: ReducerErrorClass;
    /** True when the same call (same idempotency key) may succeed later. */
    retryable?: boolean;
  }
  | { type: "action.result"; id: string; path?: string; result: JsonValue; committedRevision?: number; trace?: MessageTrace }
  | { type: "action.error"; id: string; path?: string; error: string; trace?: MessageTrace }
  | { type: "client.updateRequired"; reason: string; contract: number }
  | { type: "system.reload"; reason: string; artifactHash?: string };

/** A transport envelope, never a durable replica mutation. Versions and sequence
 * are scoped to one successful auth on one WebSocket. Rows replace only object
 * newValue slots in transactions, or the complete upserts array in a delta. */
export type ReplicaRowPatchFrame = {
  type: "replica.rowPatch";
  sequence: number;
  message: Extract<ServerMessage, { type: "replica.transaction" | "replica.delta" }>;
  rows: Array<{
    index: number;
    version?: number;
    value?: Record<string, JsonValue>;
    base?: number;
    set?: Record<string, JsonValue>;
    unset?: string[];
  }>;
  evicted: number[];
};
