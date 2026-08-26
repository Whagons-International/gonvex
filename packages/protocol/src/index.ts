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
  filters?: { argument: string; allowedColumns: string[]; allowedOperators: FilterOperator[] };
  sort?: { columnArgument: string; directionArgument: string; defaultColumn: string; defaultDirection: "asc" | "desc"; allowedColumns: string[] };
  window?: { offsetArgument: string; limitArgument: string; defaultLimit: number; maxLimit: number; count?: "exact" };
  serverOnly?: boolean;
};

export type FilterOperator = "contains" | "notContains" | "equals" | "notEquals" | "startsWith" | "endsWith" | "empty" | "notEmpty" | "oneOf" | "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" | "inRange";

export type LiveExpression = {
  operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in" | "contains" | "containsInsensitive" | "range" | "and" | "or" | "not" | "server";
  column?: string;
  value?: LiveValue;
  valueTo?: LiveValue;
  children?: LiveExpression[];
};

export type LiveValue = { argument?: string; literal?: JsonValue };

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
};

export type ReplicaOpenRequest = {
  id: string;
  path: string;
  args: JsonValue;
  cursor?: ReplicaCursor;
  keys?: string[];
  hashes?: Record<string, string>;
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
  | { type: "auth"; id: string; token?: string; project?: string; tenant?: string; controlOnly?: boolean; device?: BrowserTelemetryInfo; capabilities?: ClientCapabilities }
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
  }
  | { type: "reducer.callMany"; calls: ReducerCallRequest[] }
  | { type: "action.call"; id: string; path: string; args: JsonValue; scope?: ExecutionScope; idempotencyKey?: string; trace?: MessageTrace }
  | { type: "error.register"; id: string; release?: string; environment?: string }
  | { type: "error.envelope"; id: string; events: JsonValue[] }
  | { type: "error.heartbeat"; id: string }
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
  | { type: "auth.error"; id: string; error: string }
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
  | { type: "reducer.result"; id: string; path?: string; result: JsonValue; originCommandId: string; committedRevision?: number; trace?: MessageTrace }
  | { type: "reducer.error"; id: string; path?: string; error: string; trace?: MessageTrace }
  | { type: "action.result"; id: string; path?: string; result: JsonValue; trace?: MessageTrace }
  | { type: "action.error"; id: string; path?: string; error: string; trace?: MessageTrace }
  | { type: "system.reload"; reason: string; artifactHash?: string };
