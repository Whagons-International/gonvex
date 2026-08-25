/** JSON values accepted by the module ABI. */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type JsonObject = { [key: string]: JsonValue };

export type ModuleLanguage = "typescript";
export type ModuleEngine = "v8";

export type PortableSchema =
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
  readonly kind: "string";
  readonly format?: "email" | "uri" | "uuid" | "datetime";
  readonly minLength?: number;
  readonly maxLength?: number;
};

export type NumberSchema = {
  readonly kind: "number";
  readonly integer?: boolean;
  readonly minimum?: number;
  readonly maximum?: number;
};

export type BooleanSchema = { readonly kind: "boolean" };
export type NullSchema = { readonly kind: "null" };
export type AnySchema = { readonly kind: "any" };
export type IdSchema = { readonly kind: "id"; readonly entity: string };
export type LiteralSchema = { readonly kind: "literal"; readonly value: JsonValue };
export type ArraySchema = { readonly kind: "array"; readonly items: PortableSchema };
export type ObjectSchema = {
  readonly kind: "object";
  readonly fields: Readonly<Record<string, PortableSchema>>;
  readonly allowUnknown?: boolean;
};
export type RecordSchema = { readonly kind: "record"; readonly values: PortableSchema };
export type OptionalSchema = { readonly kind: "optional"; readonly value: PortableSchema };

export type InferSchema<S extends PortableSchema> =
  S extends StringSchema ? string
    : S extends NumberSchema ? number
      : S extends BooleanSchema ? boolean
        : S extends NullSchema ? null
          : S extends AnySchema ? JsonValue
            : S extends IdSchema ? string
              : S extends LiteralSchema ? S["value"]
                : S extends ArraySchema ? InferSchema<S["items"]>[]
                  : S extends ObjectSchema ? {
                    [K in keyof S["fields"]]: S["fields"][K] extends OptionalSchema
                      ? InferSchema<S["fields"][K]["value"]> | undefined
                      : S["fields"][K] extends PortableSchema ? InferSchema<S["fields"][K]> : never;
                  }
                    : S extends RecordSchema ? Record<string, InferSchema<S["values"]>>
                      : S extends OptionalSchema ? InferSchema<S["value"]> | undefined
                        : never;

const freeze = <T>(value: T): T => Object.freeze(value);

/** Constructors for the language-neutral schema subset. */
export const schema = {
  string(options: Omit<StringSchema, "kind"> = {}): StringSchema {
    return freeze({ kind: "string", ...options });
  },
  email(): StringSchema {
    return freeze({ kind: "string", format: "email" });
  },
  uri(): StringSchema {
    return freeze({ kind: "string", format: "uri" });
  },
  uuid(): StringSchema {
    return freeze({ kind: "string", format: "uuid" });
  },
  datetime(): StringSchema {
    return freeze({ kind: "string", format: "datetime" });
  },
  number(options: Omit<NumberSchema, "kind"> = {}): NumberSchema {
    return freeze({ kind: "number", ...options });
  },
  integer(options: Omit<NumberSchema, "kind" | "integer"> = {}): NumberSchema {
    return freeze({ kind: "number", integer: true, ...options });
  },
  boolean(): BooleanSchema {
    return freeze({ kind: "boolean" });
  },
  null(): NullSchema {
    return freeze({ kind: "null" });
  },
  any(): AnySchema {
    return freeze({ kind: "any" });
  },
  id(entity: string): IdSchema {
    if (!entity.trim()) throw new Error("schema.id requires an entity name");
    return freeze({ kind: "id", entity });
  },
  literal(value: JsonValue): LiteralSchema {
    return freeze({ kind: "literal", value });
  },
  array(items: PortableSchema): ArraySchema {
    return freeze({ kind: "array", items });
  },
  object(fields: Record<string, PortableSchema>, options: Omit<ObjectSchema, "kind" | "fields"> = {}): ObjectSchema {
    return freeze({ kind: "object", fields: freeze({ ...fields }), ...options });
  },
  record(values: PortableSchema): RecordSchema {
    return freeze({ kind: "record", values });
  },
  optional(value: PortableSchema): OptionalSchema {
    return freeze({ kind: "optional", value });
  },
};

export type Account = {
  readonly id: string;
  readonly email?: string;
  readonly name?: string;
  readonly avatarUrl?: string;
};

export type Tenant = { readonly id: string; readonly name?: string };

export type Member = {
  readonly id: string;
  readonly accountId: string;
  readonly status?: "active" | "revoked" | "disabled" | (string & {});
  readonly role?: string;
  readonly displayName?: string;
  readonly permissions: Readonly<Record<string, JsonValue>> | null;
};

/** Authentication identity exposed to a module. */
export type AuthContext = {
  readonly auth: { readonly account: Account | null };
};

/** Tenant and tenant-local member identity, both nullable at the ABI boundary. */
export type TenantContext = { readonly tenant: Tenant | null; readonly member: Member | null };

export type ReadDB = {
  readonly query: <T = JsonValue>(statement: string, parameters?: readonly JsonValue[]) => Promise<readonly T[]>;
};

export type WriteDB = ReadDB & {
  readonly insert: <T = JsonValue>(table: string, row: JsonObject) => Promise<T>;
  readonly update: <T = JsonValue>(table: string, id: string, patch: JsonObject) => Promise<T>;
  readonly delete: (table: string, id: string) => Promise<void>;
};

/** Durable external work recorded in the Reducer's current transaction. */
export type ReducerActions = {
  readonly enqueue: (path: string, args: JsonValue) => Promise<string>;
};

/** One-shot work owned by the Gonvex scheduler. Timestamps are Unix milliseconds. */
export type Scheduler = {
  readonly runAfter: (delayMs: number, functionPath: string, args?: JsonValue) => Promise<string>;
  readonly runAt: (unixMs: number, functionPath: string, args?: JsonValue) => Promise<string>;
};

export type QueryContext = AuthContext & TenantContext & { readonly db: ReadDB; readonly now: number };

export type ReducerContext = AuthContext & TenantContext & {
  readonly db: WriteDB;
  readonly actions: ReducerActions;
  readonly scheduler: Scheduler;
  readonly now: number;
};

export type ActionStorage = {
  readonly generateUploadUrl: (options?: JsonObject) => Promise<JsonValue>;
  readonly getUrl: (fileId: string) => Promise<JsonValue>;
  readonly generateDownloadUrl: (fileId: string, ttlMs?: number) => Promise<JsonValue>;
  readonly getMetadata: (fileId: string) => Promise<JsonValue>;
  readonly delete: (fileId: string) => Promise<JsonValue>;
  readonly store: (contentBase64: string, options?: JsonObject) => Promise<JsonValue>;
  readonly call: (operation: string, payload?: JsonValue) => Promise<JsonValue>;
};

export type ActionToolBinding = {
  readonly kind: "query" | "reducer";
  readonly function: string;
};

export type ActionToolBindings = Readonly<Record<string, ActionToolBinding>>;

export type SandboxStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled" | "timedOut";

export type SandboxHandle = {
  readonly sandboxId: string;
  readonly expiresAt: number;
  readonly duckdb: boolean;
};

export type SandboxExecution = {
  readonly sandboxId: string;
  readonly executionId: string;
  readonly status: SandboxStatus;
};

export type SandboxExecutionStatus = SandboxExecution & {
  readonly startedAt?: number;
  readonly finishedAt?: number;
  readonly result?: JsonValue;
  readonly error?: string;
  readonly logs: readonly { readonly level: "log" | "warn" | "error"; readonly message: string }[];
};

export type ActionSandbox = {
  /** Create one caller-owned, tenant-scoped ephemeral TypeScript workspace. */
  readonly create: (options?: { readonly ttlMs?: number }) => Promise<SandboxHandle>;
  /** Start TypeScript code asynchronously. The code returns its JSON result with a top-level return statement. */
  readonly run: (sandboxId: string, options: { readonly code: string; readonly timeoutMs?: number }) => Promise<SandboxExecution>;
  readonly cancel: (sandboxId: string, executionId: string) => Promise<SandboxExecutionStatus>;
  readonly status: (sandboxId: string, executionId: string) => Promise<SandboxExecutionStatus>;
  readonly readFile: (sandboxId: string, path: string) => Promise<{ readonly contentBase64: string; readonly size: number }>;
  readonly writeFile: (sandboxId: string, path: string, contentBase64: string) => Promise<{ readonly path: string; readonly size: number }>;
  readonly readText: (sandboxId: string, path: string) => Promise<string>;
  readonly writeText: (sandboxId: string, path: string, content: string) => Promise<{ readonly path: string; readonly size: number }>;
  /** Ingest an authorized Gonvex storage file into DuckDB without placing its bytes in model context. */
  readonly importFile: (sandboxId: string, options: { readonly fileId: string; readonly filename: string }) => Promise<{
    readonly alias: string;
    readonly tables: readonly { readonly tableName: string; readonly rowCount: number; readonly columns: readonly string[] }[];
  }>;
};

export type SandboxCapability = {
  /** Bind a private DuckDB database into the TypeScript worker. */
  readonly duckdb?: true;
};

export type ActionCapabilities<Tools extends ActionToolBindings = ActionToolBindings> = {
  /** Exact URL origins this Action may call. No network access is granted when omitted. */
  readonly networkOrigins?: readonly string[];
  /** Exact project secret names copied into this invocation. No other environment values are exposed. */
  readonly secrets?: readonly string[];
  /** Named, statically bound Query and Reducer tools. Arbitrary function paths are never accepted. */
  readonly tools?: Tools;
  readonly scheduler?: true;
  readonly storage?: true;
  /** Run untrusted TypeScript in an out-of-process, tenant-scoped sandbox. Agent Actions only. */
  readonly sandbox?: SandboxCapability;
};

type ActionToolFunctions<Tools extends ActionToolBindings> = {
  readonly [Name in keyof Tools]: <Result = JsonValue>(args?: JsonValue) => Promise<Result>;
};

export type ActionContext<Capabilities extends ActionCapabilities = ActionCapabilities> = AuthContext & TenantContext & {
  readonly now: number;
} & (Capabilities extends { readonly networkOrigins: readonly string[] }
  ? { readonly fetch: (input: string | URL, init?: RequestInit) => Promise<Response> }
  : {})
  & (Capabilities extends { readonly secrets: readonly string[] }
    ? { readonly secrets: Readonly<Record<Capabilities["secrets"][number], string>> }
    : {})
  & (Capabilities extends { readonly tools: infer Tools extends ActionToolBindings }
    ? { readonly tools: ActionToolFunctions<Tools> }
    : {})
  & (Capabilities extends { readonly scheduler: true } ? { readonly scheduler: Scheduler } : {})
  & (Capabilities extends { readonly storage: true } ? { readonly storage: ActionStorage } : {})
  & (Capabilities extends { readonly sandbox: SandboxCapability } ? { readonly sandbox: ActionSandbox } : {});

export type Handler<Context, Args, Result> = (context: Context, args: Args) => Result | Promise<Result>;

export type OfflinePolicy =
  | { readonly mode: "forbidden" }
  | { readonly mode: "allowed"; readonly conflict?: "reject" | "expectedVersion" | "merge" }
  | { readonly mode: "onlineOnly"; readonly reason: string };

export type OptimisticID = string | readonly string[];
/** Resolve this value from Reducer arguments in the client Local Replica. */
export type OptimisticArgument = { readonly $arg: string | readonly string[] };
export type OptimisticValue = JsonValue | OptimisticArgument | readonly OptimisticValue[] | { readonly [key: string]: OptimisticValue };
export type OptimisticObject = { readonly [key: string]: OptimisticValue };
export type OptimisticEffect =
  | { readonly operation: "patch"; readonly entity: string; readonly id: OptimisticID; readonly fields: OptimisticObject }
  | { readonly operation: "upsert"; readonly entity: string; readonly id: OptimisticID; readonly value: OptimisticObject }
  | { readonly operation: "delete"; readonly entity: string; readonly id: OptimisticID };

export type OptimisticTransaction = {
  readonly effects: readonly OptimisticEffect[];
  readonly expectedRevision?: number;
};

export type QueryOptions<Args, Result> = {
  readonly args?: PortableSchema;
  readonly result?: PortableSchema;
  readonly delivery?: "oneShot" | "live" | "replica";
  readonly liveQueryPlan?: LiveQueryPlan;
  readonly replica?: ReplicaCollectionDefinition;
  /** Internal Queries are callable only through a declared Action tool. */
  readonly internal?: boolean;
  readonly run?: Handler<QueryContext, Args, Result>;
};

/** Declarative contract for a bounded, locally materialized query collection. */
export type ReplicaCollectionDefinition = {
  readonly table: string;
  readonly key: string;
  readonly columns: readonly string[];
  readonly equalFilters?: Readonly<Record<string, string>>;
  readonly excludeWhenSet?: readonly string[];
  readonly visibilityTables?: readonly string[];
  /** Assigned by the runtime; module authors do not set this. */
  readonly visibilityPlanHash?: string;
  readonly orderBy?: string;
  readonly orderDirection?: "asc" | "desc";
  /** `eager` is complete at initial delivery; `progressive` may fill incrementally. */
  readonly mode?: "eager" | "progressive";
  readonly maxRows?: number;
  readonly maxBytes?: number;
  readonly retentionMs?: number;
};

export type ReplicaCollectionOptions<Args, Result> = Omit<QueryOptions<Args, Result>, "delivery" | "replica"> & {
  readonly replica: ReplicaCollectionDefinition;
};

export type ReducerOptions<Args, Result> = {
  readonly args?: PortableSchema;
  readonly result?: PortableSchema;
  readonly offline: OfflinePolicy;
  /** Set false for reducers that are not invoked directly by an interactive client. */
  readonly interactive?: boolean;
  readonly optimistic?: OptimisticTransaction;
  /** Required exception for a public interactive reducer that cannot predict a safe local transaction. */
  readonly nonOptimisticReason?: string;
  readonly internal?: boolean;
  readonly run?: Handler<ReducerContext, Args, Result>;
};

export type InternalReducerOptions<Args, Result> = Omit<ReducerOptions<Args, Result>, "offline" | "interactive" | "internal"> & {
  readonly offline?: OfflinePolicy;
};

export type ActionOptions<Args, Result, Capabilities extends ActionCapabilities = ActionCapabilities> = {
  /** Optional explicit public path used by static module artifact extraction. */
  readonly name?: string;
  readonly args?: PortableSchema;
  readonly result?: PortableSchema;
  /** Agent Actions are disabled unless the runtime operator explicitly enables them. */
  readonly profile?: "standard" | "agent";
  readonly capabilities?: Capabilities;
  readonly run?: Handler<ActionContext<Capabilities>, Args, Result>;
};

export type LiveQueryValue = { readonly argument?: string; readonly literal?: JsonValue };
export type FilterOperator = "contains" | "notContains" | "equals" | "notEquals" | "startsWith" | "endsWith" | "empty" | "notEmpty" | "oneOf" | "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" | "inRange";
export type LiveQueryExpression = {
  readonly operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "range" | "in" | "contains" | "containsInsensitive" | "and" | "or" | "not" | "server";
  readonly column?: string;
  readonly value?: LiveQueryValue;
  readonly valueTo?: LiveQueryValue;
  readonly children?: readonly LiveQueryExpression[];
};

export type LiveQueryPlan = {
  readonly table: string;
  readonly key: string;
  readonly columns?: readonly string[];
  readonly resultPath?: readonly string[];
  readonly where?: LiveQueryExpression;
  readonly search?: { readonly argument: string; readonly columns: readonly string[] };
  readonly filters?: { readonly argument: string; readonly allowedColumns: readonly string[]; readonly allowedOperators: readonly FilterOperator[] };
  readonly sort?: { readonly columnArgument?: string; readonly directionArgument?: string; readonly defaultColumn: string; readonly defaultDirection: "asc" | "desc"; readonly allowedColumns: readonly string[] };
  readonly window?: {
    readonly offsetArgument: string;
    readonly limitArgument: string;
    readonly defaultLimit: number;
    readonly maxLimit: number;
    /** Request exact total-count metadata alongside a shaped result window. */
    readonly count?: "exact";
  };
  readonly serverOnly?: boolean;
};

export type VisibilityOperator = "public" | "permission" | "role" | "eqContext" | "inSet" | "and" | "or" | "not";
export type VisibilityContextKey = "account.id" | "member.id" | "tenant.id";

export type VisibilityPlan = {
  readonly table: string;
  readonly key: string;
  readonly sets: Readonly<Record<string, VisibilitySet>>;
  readonly where: VisibilityExpression;
};

export type VisibilitySet = {
  readonly table: string;
  /** Logical SQL name. Required when the same physical table occurs twice. */
  readonly alias?: string;
  readonly select: string;
  /** Logical alias that owns `select`; defaults to the base occurrence. */
  readonly selectFrom?: string;
  readonly joins: readonly VisibilityJoin[];
  readonly where: readonly VisibilityConstraint[];
};

export type VisibilityJoin = {
  readonly table: string;
  /** Logical SQL name. Required when the same physical table occurs twice. */
  readonly alias?: string;
  /** Logical occurrence joined on the left; defaults to the previous occurrence. */
  readonly leftAlias?: string;
  readonly leftColumn: string;
  readonly rightColumn: string;
};

export type VisibilityConstraint = {
  readonly table: string;
  readonly column: string;
  readonly context: VisibilityContextKey;
};

export type VisibilityExpression = {
  readonly operator: VisibilityOperator;
  readonly column?: string;
  readonly context?: VisibilityContextKey;
  readonly set?: string;
  readonly value?: string;
  readonly children?: readonly VisibilityExpression[];
};

export type ModuleFunctionKind = "query" | "reducer" | "action";

export type CronScope = "project" | "tenant";
export type CronSchedule =
  | { readonly intervalMs: number; readonly expression?: never }
  | { readonly expression: string; readonly intervalMs?: never };
export type CronSpec = Readonly<{
  name: string;
  function: string;
  args?: JsonValue;
  scope: CronScope;
} & CronSchedule>;
export type CronOptions = Omit<CronSpec, "scope">;

export type ModuleFunctionManifest = {
  readonly path: string;
  readonly kind: ModuleFunctionKind;
  readonly args?: PortableSchema;
  readonly result?: PortableSchema;
  readonly internal?: boolean;
  readonly delivery?: "oneShot" | "live" | "replica";
  readonly liveQueryPlan?: LiveQueryPlan;
  readonly replica?: ReplicaCollectionDefinition;
  readonly offline?: OfflinePolicy;
  readonly interactive?: boolean;
  readonly optimistic?: OptimisticTransaction;
  readonly nonOptimisticReason?: string;
  readonly actionProfile?: "standard" | "agent";
  readonly actionCapabilities?: ActionCapabilities;
};

export type ModuleManifest = {
  readonly format: "gonvex.module.v1";
  readonly name: string;
  readonly version: string;
  readonly language: ModuleLanguage;
  readonly engine: ModuleEngine;
  readonly functions: Readonly<Record<string, ModuleFunctionManifest>>;
  readonly crons?: readonly CronSpec[];
  readonly schema?: PortableSchema;
  readonly artifact?: { readonly hash: string; readonly mediaType: string; readonly entrypoint: string };
  readonly visibility?: Readonly<Record<string, VisibilityPlan>>;
  readonly invitationAcceptanceReducer?: string;
};

/** Declare the host-invoked internal Reducer that applies invitation payloads. */
export function invitationAcceptance(reducerPath: string): Readonly<{ reducer: string }> {
  const reducer = normalizePath(reducerPath);
  return freeze({ reducer });
}

export type ModuleArtifact = {
  readonly manifest: ModuleManifest;
  readonly bytes: Uint8Array;
};

export type ModuleFunctionHandler =
  | Handler<QueryContext, unknown, unknown>
  | Handler<ReducerContext, unknown, unknown>
  | Handler<ActionContext, unknown, unknown>;

/** A manifest entry together with the executable function retained by the host. */
export type RuntimeFunctionRegistration = {
  readonly path: string;
  readonly kind: ModuleFunctionKind;
  readonly definition: ModuleFunctionManifest;
  readonly handler?: ModuleFunctionHandler;
};

export type ModuleRuntimeRegistration = {
  readonly path: string;
  readonly kind: ModuleFunctionKind;
  readonly definition: ModuleFunctionManifest;
};

/** Deterministic, handler-free payload consumed by a module host during loading. */
export type ModuleRuntimeRegistrationPayload = {
  readonly format: "gonvex.module.runtime.v1";
  readonly manifest: ModuleManifest;
  readonly registrations: readonly ModuleRuntimeRegistration[];
};

export type QueryInvocation<Args = unknown> = {
  readonly path: string;
  readonly kind: "query";
  readonly context: QueryContext;
  readonly args: Args;
};

export type ReducerInvocation<Args = unknown> = {
  readonly path: string;
  readonly kind: "reducer";
  readonly context: ReducerContext;
  readonly args: Args;
};

export type ActionInvocation<Args = unknown> = {
  readonly path: string;
  readonly kind: "action";
  readonly context: ActionContext;
  readonly args: Args;
};

export type ModuleInvocation<Args = unknown> =
  | QueryInvocation<Args>
  | ReducerInvocation<Args>
  | ActionInvocation<Args>;

type AnyFunctionOptions = QueryOptions<unknown, unknown> | ReducerOptions<unknown, unknown> | ActionOptions<unknown, unknown>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const normalizePath = (path: string): string => {
  const normalized = path.trim();
  if (!normalized) throw new Error("module function path is required");
  if (normalized === "control" || normalized.startsWith("control.")) {
    throw new Error(`module function path ${JSON.stringify(normalized)} uses the host-reserved Control Plane namespace`);
  }
  return normalized;
};

const validateOfflinePolicy: (value: unknown, path: string) => asserts value is OfflinePolicy = (value, path) => {
  if (!isRecord(value) || (value.mode !== "forbidden" && value.mode !== "allowed" && value.mode !== "onlineOnly")) {
    throw new Error(`reducer ${path} must declare a valid offline policy`);
  }
  if (value.mode === "onlineOnly" && (typeof value.reason !== "string" || !value.reason.trim())) {
    throw new Error(`reducer ${path} onlineOnly policy requires a reason`);
  }
  if (value.mode === "allowed" && value.conflict !== undefined &&
    value.conflict !== "reject" && value.conflict !== "expectedVersion" && value.conflict !== "merge") {
    throw new Error(`reducer ${path} has an invalid offline conflict policy`);
  }
};

const validateOptimisticTransaction: (value: unknown, path: string) => asserts value is OptimisticTransaction = (value, path) => {
  if (!isRecord(value) || !Array.isArray(value.effects) || value.effects.length === 0) {
    throw new Error(`reducer ${path} optimistic metadata must contain a non-empty effects array`);
  }
  if (value.expectedRevision !== undefined &&
    (typeof value.expectedRevision !== "number" || !Number.isSafeInteger(value.expectedRevision) || value.expectedRevision < 0)) {
    throw new Error(`reducer ${path} optimistic expectedRevision must be a non-negative integer`);
  }
  for (const effect of value.effects) {
    if (!isRecord(effect) || (effect.operation !== "patch" && effect.operation !== "upsert" && effect.operation !== "delete")) {
      throw new Error(`reducer ${path} has an invalid optimistic effect`);
    }
    if (typeof effect.entity !== "string" || !effect.entity.trim()) {
      throw new Error(`reducer ${path} optimistic effects require an entity`);
    }
    if ((typeof effect.id === "string" && !effect.id.trim()) ||
      (typeof effect.id !== "string" &&
      (!Array.isArray(effect.id) || effect.id.length === 0 || effect.id.some((part) => typeof part !== "string" || !part.trim())))) {
      throw new Error(`reducer ${path} optimistic effects require a string id or id references`);
    }
    if ((effect.operation === "patch" || effect.operation === "upsert") && !isRecord(effect.operation === "patch" ? effect.fields : effect.value)) {
      throw new Error(`reducer ${path} optimistic ${effect.operation} effects require an object value`);
    }
  }
};

const validateReplicaCollection: (value: unknown, path: string) => asserts value is ReplicaCollectionDefinition = (value, path) => {
  if (!isRecord(value) || typeof value.table !== "string" || !value.table.trim() ||
    typeof value.key !== "string" || !value.key.trim() || !Array.isArray(value.columns) ||
    value.columns.some((column) => typeof column !== "string" || !column.trim())) {
    throw new Error(`replica collection ${path} requires a table, key, and columns`);
  }
  for (const field of ["maxRows", "maxBytes", "retentionMs"] as const) {
    const budget = value[field];
    if (budget !== undefined && (typeof budget !== "number" || !Number.isSafeInteger(budget) || budget <= 0)) {
      throw new Error(`replica collection ${path} ${field} must be a positive integer`);
    }
  }
  if (value.mode !== undefined && value.mode !== "eager" && value.mode !== "progressive") {
    throw new Error(`replica collection ${path} has an invalid completeness mode`);
  }
  if (value.orderDirection !== undefined && value.orderDirection !== "asc" && value.orderDirection !== "desc") {
    throw new Error(`replica collection ${path} has an invalid order direction`);
  }
};

const validateActionCapabilities = (profile: "standard" | "agent", value: ActionCapabilities | undefined, path: string): void => {
  if (value === undefined) return;
  if (!isRecord(value)) throw new Error(`action ${path} capabilities must be an object`);
  const allowed = new Set(["networkOrigins", "secrets", "tools", "scheduler", "storage", "sandbox"]);
  for (const field of Object.keys(value)) {
    if (!allowed.has(field)) throw new Error(`action ${path} capabilities has unsupported field ${field}`);
  }
  if (value.networkOrigins !== undefined) {
    if (!Array.isArray(value.networkOrigins) || value.networkOrigins.length === 0) {
      throw new Error(`action ${path} networkOrigins must be a non-empty array`);
    }
    const seen = new Set<string>();
    for (const origin of value.networkOrigins) {
      if (typeof origin !== "string") throw new Error(`action ${path} networkOrigins must contain strings`);
      let parsed: URL;
      try { parsed = new URL(origin); } catch { throw new Error(`action ${path} network origin ${JSON.stringify(origin)} is invalid`); }
      if ((parsed.protocol !== "https:" && parsed.protocol !== "http:") || parsed.origin !== origin || parsed.username || parsed.password) {
        throw new Error(`action ${path} network origin ${JSON.stringify(origin)} must be an exact HTTP(S) origin`);
      }
      if (seen.has(origin)) throw new Error(`action ${path} declares duplicate network origin ${origin}`);
      seen.add(origin);
    }
  }
  if (value.secrets !== undefined) {
    if (!Array.isArray(value.secrets) || value.secrets.some((name) => typeof name !== "string" || !/^[A-Z][A-Z0-9_]*$/.test(name))) {
      throw new Error(`action ${path} secrets must be uppercase environment names`);
    }
  }
  if (value.tools !== undefined) {
    if (profile !== "agent") throw new Error(`action ${path} tools require profile "agent"`);
    if (!isRecord(value.tools) || Object.keys(value.tools).length === 0) {
      throw new Error(`agent action ${path} tools must be a non-empty object`);
    }
    for (const [name, binding] of Object.entries(value.tools)) {
      if (!/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name) || !isRecord(binding) ||
        (binding.kind !== "query" && binding.kind !== "reducer") || typeof binding.function !== "string" || !binding.function.trim()) {
        throw new Error(`agent action ${path} has an invalid tool binding ${JSON.stringify(name)}`);
      }
    }
  }
  if (value.scheduler !== undefined && value.scheduler !== true) throw new Error(`action ${path} scheduler must be true when declared`);
  if (value.storage !== undefined && value.storage !== true) throw new Error(`action ${path} storage must be true when declared`);
  if (value.sandbox !== undefined) {
    if (profile !== "agent") throw new Error(`action ${path} sandbox requires profile "agent"`);
    if (!isRecord(value.sandbox)) throw new Error(`action ${path} sandbox must be an object`);
    for (const field of Object.keys(value.sandbox)) {
      if (field !== "duckdb") throw new Error(`action ${path} sandbox has unsupported field ${field}`);
    }
    if (value.sandbox.duckdb !== undefined && value.sandbox.duckdb !== true) {
      throw new Error(`action ${path} sandbox.duckdb must be true when declared`);
    }
  }
};

const validateStructuredQueryPlan: (value: unknown, path: string) => asserts value is LiveQueryPlan = (value, path) => {
  if (!isRecord(value) || typeof value.table !== "string" || !value.table.trim() ||
    typeof value.key !== "string" || !value.key.trim() || !Array.isArray(value.columns) ||
    value.columns.length === 0 || value.columns.some((column) => typeof column !== "string" || !column.trim())) {
    throw new Error(`one-shot query ${path} requires a structured live query plan with a table, key, and columns`);
  }
  if (!value.columns.includes(value.key)) {
    throw new Error(`one-shot query ${path} live query plan columns must include its key`);
  }
  if (value.filters !== undefined) {
    if (!isRecord(value.filters) || typeof value.filters.argument !== "string" || !value.filters.argument.trim() ||
      !Array.isArray(value.filters.allowedColumns) || value.filters.allowedColumns.length === 0 ||
      value.filters.allowedColumns.some((column) => typeof column !== "string" || !column.trim()) ||
      !Array.isArray(value.filters.allowedOperators) || value.filters.allowedOperators.length === 0) {
      throw new Error(`structured query plan ${path} filters must declare an argument, allowed columns, and allowed operators`);
    }
    const operators = new Set<FilterOperator>(["contains", "notContains", "equals", "notEquals", "startsWith", "endsWith", "empty", "notEmpty", "oneOf", "lessThan", "lessThanOrEqual", "greaterThan", "greaterThanOrEqual", "inRange"]);
    if (value.filters.allowedOperators.some((operator) => typeof operator !== "string" || !operators.has(operator as FilterOperator))) {
      throw new Error(`structured query plan ${path} filters contains an unsupported operator`);
    }
  }
};

const visibilityOperators = new Set<VisibilityOperator>(["public", "permission", "role", "eqContext", "inSet", "and", "or", "not"]);
const visibilityContexts = new Set<VisibilityContextKey>(["account.id", "member.id", "tenant.id"]);

const validateExactObject: (
  value: unknown,
  path: string,
  fields: readonly string[],
) => asserts value is Record<string, unknown> = (value, path, fields) => {
  if (!isRecord(value)) throw new Error(`${path} must be an object`);
  const allowed = new Set(fields);
  const unexpected = Object.keys(value).find((field) => !allowed.has(field));
  if (unexpected !== undefined) throw new Error(`${path} has unsupported field ${unexpected}`);
};

const requireVisibilityString = (value: unknown, path: string): string => {
  if (typeof value !== "string" || !value.trim()) throw new Error(`${path} must be a non-empty string`);
  return value;
};

const validateVisibilityContext = (value: unknown, path: string): VisibilityContextKey => {
  if (typeof value !== "string" || !visibilityContexts.has(value as VisibilityContextKey)) {
    throw new Error(`${path} must be account.id, member.id, or tenant.id`);
  }
  return value as VisibilityContextKey;
};

type VisibilityExpressionValidator = (
  value: unknown,
  path: string,
  sets: Readonly<Record<string, VisibilitySet>>,
  ancestors?: ReadonlySet<object>,
) => asserts value is VisibilityExpression;

const validateVisibilityExpression: VisibilityExpressionValidator = (
  value,
  path,
  sets,
  ancestors = new Set(),
) => {
  validateExactObject(value, path, ["operator", "column", "context", "set", "value", "children"]);
  if (ancestors.has(value)) throw new Error(`${path} contains a cycle`);
  const operator = value.operator;
  if (typeof operator !== "string" || !visibilityOperators.has(operator as VisibilityOperator)) {
    throw new Error(`${path}.operator is unsupported`);
  }
  const nested = new Set(ancestors);
  nested.add(value);

  switch (operator as VisibilityOperator) {
    case "public":
      validateExactObject(value, path, ["operator"]);
      return;
    case "permission":
    case "role":
      validateExactObject(value, path, ["operator", "value"]);
      requireVisibilityString(value.value, `${path}.value`);
      return;
    case "eqContext":
      validateExactObject(value, path, ["operator", "column", "context"]);
      requireVisibilityString(value.column, `${path}.column`);
      validateVisibilityContext(value.context, `${path}.context`);
      return;
    case "inSet": {
      validateExactObject(value, path, ["operator", "column", "set"]);
      requireVisibilityString(value.column, `${path}.column`);
      const set = requireVisibilityString(value.set, `${path}.set`);
      if (!Object.prototype.hasOwnProperty.call(sets, set)) throw new Error(`${path}.set references unknown visibility set ${set}`);
      return;
    }
    case "and":
    case "or":
      validateExactObject(value, path, ["operator", "children"]);
      if (!Array.isArray(value.children) || value.children.length === 0) {
        throw new Error(`${path}.${operator} requires children`);
      }
      value.children.forEach((child, index) => validateVisibilityExpression(child, `${path}.children[${index}]`, sets, nested));
      return;
    case "not":
      validateExactObject(value, path, ["operator", "children"]);
      if (!Array.isArray(value.children) || value.children.length !== 1) {
        throw new Error(`${path}.not requires exactly one child`);
      }
      validateVisibilityExpression(value.children[0], `${path}.children[0]`, sets, nested);
      return;
  }
};

const validateVisibilityPlan: (value: unknown, path: string) => asserts value is VisibilityPlan = (value, path) => {
  validateExactObject(value, path, ["table", "key", "sets", "where"]);
  requireVisibilityString(value.table, `${path}.table`);
  requireVisibilityString(value.key, `${path}.key`);
  if (!isRecord(value.sets)) throw new Error(`${path}.sets must be an object`);

  for (const [name, candidate] of Object.entries(value.sets)) {
    requireVisibilityString(name, `${path}.sets key`);
    const setPath = `${path}.sets.${name}`;
    validateExactObject(candidate, setPath, ["table", "alias", "select", "selectFrom", "joins", "where"]);
    requireVisibilityString(candidate.table, `${setPath}.table`);
    if (candidate.alias !== undefined) requireVisibilityString(candidate.alias, `${setPath}.alias`);
    requireVisibilityString(candidate.select, `${setPath}.select`);
    if (candidate.selectFrom !== undefined) requireVisibilityString(candidate.selectFrom, `${setPath}.selectFrom`);
    if (!Array.isArray(candidate.joins)) throw new Error(`${setPath}.joins must be an array`);
    const joins = candidate.joins as unknown as VisibilityJoin[];
    joins.forEach((join, index) => {
      const joinPath = `${setPath}.joins[${index}]`;
      validateExactObject(join, joinPath, ["table", "alias", "leftAlias", "leftColumn", "rightColumn"]);
      requireVisibilityString(join.table, `${joinPath}.table`);
      if (join.alias !== undefined) requireVisibilityString(join.alias, `${joinPath}.alias`);
      if (join.leftAlias !== undefined) requireVisibilityString(join.leftAlias, `${joinPath}.leftAlias`);
      requireVisibilityString(join.leftColumn, `${joinPath}.leftColumn`);
      requireVisibilityString(join.rightColumn, `${joinPath}.rightColumn`);
    });
    const occurrences = [
      { table: candidate.table as string, alias: (candidate.alias as string | undefined) ?? candidate.table as string, explicit: candidate.alias !== undefined },
      ...joins.map((join) => ({ table: join.table, alias: join.alias ?? join.table, explicit: join.alias !== undefined })),
    ];
    const aliases = new Set<string>();
    const tableCounts = new Map<string, number>();
    for (const occurrence of occurrences) tableCounts.set(occurrence.table, (tableCounts.get(occurrence.table) ?? 0) + 1);
    occurrences.forEach((occurrence, index) => {
      if ((tableCounts.get(occurrence.table) ?? 0) > 1 && !occurrence.explicit) {
        throw new Error(`${setPath} repeats table ${occurrence.table}; every occurrence requires an explicit alias`);
      }
      if (aliases.has(occurrence.alias)) throw new Error(`${setPath} repeats logical alias ${occurrence.alias}`);
      if (index > 0) {
        const leftAlias = joins[index - 1]!.leftAlias;
        if (leftAlias !== undefined && !aliases.has(leftAlias)) throw new Error(`${setPath}.joins[${index - 1}].leftAlias must reference an earlier occurrence`);
      }
      aliases.add(occurrence.alias);
    });
    const selectFrom = (candidate.selectFrom as string | undefined) ?? occurrences[0]!.alias;
    if (!aliases.has(selectFrom)) throw new Error(`${setPath}.selectFrom references unknown alias ${selectFrom}`);
    if (!Array.isArray(candidate.where)) throw new Error(`${setPath}.where must be an array`);
    candidate.where.forEach((constraint, index) => {
      const constraintPath = `${setPath}.where[${index}]`;
      validateExactObject(constraint, constraintPath, ["table", "column", "context"]);
      requireVisibilityString(constraint.table, `${constraintPath}.table`);
      if (!aliases.has(constraint.table as string)) throw new Error(`${constraintPath}.table references unknown alias ${constraint.table}`);
      requireVisibilityString(constraint.column, `${constraintPath}.column`);
      validateVisibilityContext(constraint.context, `${constraintPath}.context`);
    });
  }

  validateVisibilityExpression(value.where, `${path}.where`, value.sets as Record<string, VisibilitySet>);
};

const freezeVisibilityExpression = (expression: VisibilityExpression): VisibilityExpression =>
  freeze({
    ...expression,
    children: expression.children === undefined
      ? undefined
      : freeze(expression.children.map(freezeVisibilityExpression)),
  });

const freezeVisibilityPlan = (plan: VisibilityPlan): VisibilityPlan => {
  const sets: Record<string, VisibilitySet> = {};
  for (const name of Object.keys(plan.sets).sort()) {
    const set = plan.sets[name]!;
    sets[name] = freeze({
      ...set,
      joins: freeze(set.joins.map((join) => freeze({ ...join }))),
      where: freeze(set.where.map((constraint) => freeze({ ...constraint }))),
    });
  }
  return freeze({
    ...plan,
    sets: freeze(sets),
    where: freezeVisibilityExpression(plan.where),
  });
};

/** Declare and validate one source table's language-neutral visibility plan. */
export function visibility(options: VisibilityPlan): VisibilityPlan {
  validateVisibilityPlan(options, "visibility");
  return freezeVisibilityPlan(options);
}

const stableValue = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(stableValue);
  if (isRecord(value)) {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) sorted[key] = stableValue(value[key]);
    return sorted;
  }
  return value;
};

/** JSON serialization with recursively sorted object keys for reproducible artifacts. */
export const stableJsonStringify = (value: unknown, space?: number): string =>
  JSON.stringify(stableValue(value), null, space);

const validateCron = (options: CronSpec): CronSpec => {
  const name = options.name.trim();
  const functionPath = options.function.trim();
  if (!name) throw new Error("cron name is required");
  if (!functionPath) throw new Error(`cron ${JSON.stringify(name)} requires a function path`);
  const hasInterval = options.intervalMs !== undefined;
  const hasExpression = options.expression !== undefined;
  if (hasInterval === hasExpression) {
    throw new Error(`cron ${JSON.stringify(name)} requires exactly one of intervalMs or expression`);
  }
  if (hasInterval && (!Number.isSafeInteger(options.intervalMs) || options.intervalMs <= 0)) {
    throw new Error(`cron ${JSON.stringify(name)} intervalMs must be a positive safe integer`);
  }
  const expression = options.expression?.trim();
  if (hasExpression && !expression) throw new Error(`cron ${JSON.stringify(name)} expression must be non-empty`);
  return freeze({
    name,
    function: functionPath,
    scope: options.scope,
    ...(options.args === undefined ? {} : { args: options.args }),
    ...(hasInterval ? { intervalMs: options.intervalMs } : { expression: expression! }),
  } as CronSpec);
};

/** Declare a project-wide recurring Reducer or Action. */
export function cron(options: CronOptions): CronSpec {
  return validateCron({ ...options, scope: "project" } as CronSpec);
}

/** Declare a recurring Reducer or Action once for every tenant. */
export function tenantCron(options: CronOptions): CronSpec {
  return validateCron({ ...options, scope: "tenant" } as CronSpec);
}

export class ModuleManifestCollector {
  private readonly entries = new Map<string, ModuleFunctionManifest>();
  private readonly visibilityEntries = new Map<string, VisibilityPlan>();
  private readonly cronEntries = new Map<string, CronSpec>();
  private readonly metadata: Omit<ModuleManifest, "functions" | "visibility" | "crons">;

  constructor(metadata: Omit<ModuleManifest, "functions">) {
    const { visibility: initialVisibility, crons: initialCrons, ...baseMetadata } = metadata;
    this.metadata = baseMetadata;
    for (const sourceTable of Object.keys(initialVisibility ?? {}).sort()) {
      const plan = initialVisibility![sourceTable]!;
      if (sourceTable !== plan.table) {
        throw new Error(`visibility map key ${sourceTable} does not match source table ${plan.table}`);
      }
      this.registerVisibility(plan);
    }
    for (const spec of initialCrons ?? []) this.registerCron(spec);
  }

  register(path: string, entry: Omit<ModuleFunctionManifest, "path">): ModuleFunctionManifest {
    const normalized = normalizePath(path);
    if (this.entries.has(normalized)) throw new Error(`duplicate module function: ${normalized}`);
    const result = freeze({ path: normalized, ...entry });
    this.entries.set(normalized, result);
    return result;
  }

  registerVisibility(options: VisibilityPlan): VisibilityPlan {
    const plan = visibility(options);
    if (this.visibilityEntries.has(plan.table)) throw new Error(`duplicate visibility plan: ${plan.table}`);
    this.visibilityEntries.set(plan.table, plan);
    return plan;
  }

  registerCron(options: CronSpec): CronSpec {
    const spec = validateCron(options);
    if (this.cronEntries.has(spec.name)) throw new Error(`duplicate cron: ${spec.name}`);
    this.cronEntries.set(spec.name, spec);
    return spec;
  }

  manifest(): ModuleManifest {
    const functions: Record<string, ModuleFunctionManifest> = {};
    for (const path of [...this.entries.keys()].sort()) functions[path] = this.entries.get(path)!;
    const visibilityPlans: Record<string, VisibilityPlan> = {};
    for (const table of [...this.visibilityEntries.keys()].sort()) visibilityPlans[table] = this.visibilityEntries.get(table)!;
    const crons = [...this.cronEntries.values()].sort((left, right) => left.name.localeCompare(right.name));
    for (const spec of crons) {
      const target = this.entries.get(spec.function);
      if (!target) throw new Error(`cron ${JSON.stringify(spec.name)} targets unknown function ${JSON.stringify(spec.function)}`);
      if (target.kind === "query") throw new Error(`cron ${JSON.stringify(spec.name)} must target a reducer or action`);
    }
    for (const [path, definition] of this.entries) {
      for (const [name, binding] of Object.entries(definition.actionCapabilities?.tools ?? {})) {
        const target = this.entries.get(binding.function);
        if (!target) throw new Error(`action ${JSON.stringify(path)} tool ${JSON.stringify(name)} targets unknown function ${JSON.stringify(binding.function)}`);
        if (target.kind !== binding.kind) throw new Error(`action ${JSON.stringify(path)} tool ${JSON.stringify(name)} kind does not match ${JSON.stringify(binding.function)}`);
        if (binding.kind === "query" && (!target.internal || target.delivery !== "oneShot")) throw new Error(`action ${JSON.stringify(path)} tool ${JSON.stringify(name)} must target an internal one-shot Query`);
        if (binding.kind === "reducer" && target.internal) throw new Error(`action ${JSON.stringify(path)} tool ${JSON.stringify(name)} must target a public business-intent Reducer`);
      }
    }
    return freeze({
      ...this.metadata,
      functions: freeze(functions),
      crons: crons.length === 0 ? undefined : freeze(crons),
      visibility: this.visibilityEntries.size === 0 ? undefined : freeze(visibilityPlans),
    });
  }

  serialize(space?: number): string {
    return stableJsonStringify(this.manifest(), space);
  }
}

export type RegisteredFunction<Args, Result> = {
  readonly path: string;
  readonly kind: ModuleFunctionKind;
  readonly definition: ModuleFunctionManifest;
  readonly handler?: Handler<QueryContext, Args, Result> | Handler<ReducerContext, Args, Result> | Handler<ActionContext, Args, Result>;
};

/**
 * Executable definition returned by the top-level declaration helpers.
 *
 * The module loader uses the exported binding itself as the declaration and
 * invokes `handler` when a V8 request arrives. `options` retains the
 * declarative input so hosts can project it into a manifest without needing
 * to evaluate TypeScript source again.
 */
export type ModuleDefinition<Kind extends ModuleFunctionKind, Options> = {
  readonly kind: Kind;
  readonly internal?: boolean;
  readonly delivery?: "oneShot" | "live" | "replica";
  readonly liveQueryPlan?: LiveQueryPlan;
  readonly replica?: ReplicaCollectionDefinition;
  readonly options: Readonly<Options>;
  readonly handler?: Options extends { readonly run?: infer HandlerType } ? HandlerType : never;
};

export type QueryDefinition<Args, Result> = ModuleDefinition<"query", QueryOptions<Args, Result>>;
export type ReducerDefinition<Args, Result> = ModuleDefinition<"reducer", ReducerOptions<Args, Result>>;
export type ActionDefinition<Args, Result, Capabilities extends ActionCapabilities = ActionCapabilities> = ModuleDefinition<"action", ActionOptions<Args, Result, Capabilities>>;

const executableOptions = <T extends object>(options: T): Readonly<T> => freeze({ ...options });

const queryDefinition = <Args, Result>(
  options: QueryOptions<Args, Result>,
  deliveryOverride?: "live" | "replica",
): QueryDefinition<Args, Result> => {
  const liveQueryPlan = options.liveQueryPlan;
  const replica = options.replica;
  // `query()` is always a one-shot declaration. Live and replica delivery are
  // selected only by the dedicated helpers below; a structured plan does not
  // silently change the execution mode.
  const delivery = deliveryOverride ?? options.delivery ?? "oneShot";
  if (delivery === "live" && !liveQueryPlan) throw new Error("live query requires a live query plan");
  if (delivery === "oneShot") {
    if (!liveQueryPlan) throw new Error("one-shot query requires a structured live query plan");
    validateStructuredQueryPlan(liveQueryPlan, "<export>");
  }
  if (delivery === "replica") {
    if (!replica) throw new Error("replica collection requires a replica definition");
    validateReplicaCollection(replica, "<export>");
  }
  return freeze({
    kind: "query",
    internal: options.internal,
    delivery,
    liveQueryPlan,
    replica,
    options: executableOptions(options),
    handler: options.run,
  });
};

/** Declare an executable one-shot, live, or replica query export. */
export function query<Args = JsonValue, Result = JsonValue>(options: QueryOptions<Args, Result> = {}): QueryDefinition<Args, Result> {
  return queryDefinition(options);
}

/** Declare a one-shot Query that is unreachable from clients and may be bound to an Action tool. */
export function internalQuery<Args = JsonValue, Result = JsonValue>(options: Omit<QueryOptions<Args, Result>, "internal" | "delivery" | "replica"> = {}): QueryDefinition<Args, Result> {
  return queryDefinition({ ...options, internal: true, delivery: "oneShot" });
}

/** Declare an executable live query export with a structured live plan. */
export function liveQuery<Args = JsonValue, Result = JsonValue>(options: Omit<QueryOptions<Args, Result>, "delivery"> = {}): QueryDefinition<Args, Result> {
  return queryDefinition({ ...options, delivery: "live" }, "live");
}

/** Declare an executable bounded replica collection export. */
export function replicaCollection<Args = JsonValue, Result = JsonValue>(options: ReplicaCollectionOptions<Args, Result>): QueryDefinition<Args, Result> {
  return queryDefinition({ ...options, delivery: "replica", replica: options.replica }, "replica");
}

const reducerDefinition = <Args, Result>(
  options: ReducerOptions<Args, Result>,
  internal = false,
): ReducerDefinition<Args, Result> => {
  validateOfflinePolicy(options.offline, "<export>");
  if (options.optimistic !== undefined) validateOptimisticTransaction(options.optimistic, "<export>");
  if (options.interactive === false && options.optimistic !== undefined) {
    throw new Error("non-interactive reducer <export> cannot declare optimistic metadata");
  }
  if (options.interactive !== false && options.optimistic === undefined && !options.nonOptimisticReason?.trim()) {
    throw new Error("interactive reducer <export> requires an optimistic transaction or nonOptimisticReason");
  }
  return freeze({
    kind: "reducer",
    internal: internal || options.internal,
    options: executableOptions(options),
    handler: options.run,
  });
};

/** Declare an executable public reducer export. */
export function reducer<Args = JsonValue, Result = JsonValue>(options: ReducerOptions<Args, Result>): ReducerDefinition<Args, Result> {
  return reducerDefinition(options);
}

/** Declare an executable non-interactive internal reducer export. */
export function internalReducer<Args = JsonValue, Result = JsonValue>(options: InternalReducerOptions<Args, Result>): ReducerDefinition<Args, Result> {
  return reducerDefinition({
    ...options,
    offline: options.offline ?? { mode: "forbidden" },
    interactive: false,
    internal: true,
  }, true);
}

/** Declare an executable action export. */
export function action<Args = JsonValue, Result = JsonValue, const Capabilities extends ActionCapabilities = ActionCapabilities>(options: ActionOptions<Args, Result, Capabilities> = {}): ActionDefinition<Args, Result, Capabilities> {
  validateActionCapabilities(options.profile ?? "standard", options.capabilities, options.name?.trim() || "<export>");
  return freeze({ kind: "action", options: executableOptions(options), handler: options.run });
}

export class ModuleBuilder {
  readonly manifestCollector: ModuleManifestCollector;
  private readonly runtimeEntries = new Map<string, RuntimeFunctionRegistration>();

  constructor(metadata: { name: string; version: string; language?: ModuleLanguage; engine?: ModuleEngine; schema?: PortableSchema; artifact?: ModuleManifest["artifact"]; visibility?: Readonly<Record<string, VisibilityPlan>>; crons?: readonly CronSpec[] }) {
    this.manifestCollector = new ModuleManifestCollector({
      format: "gonvex.module.v1",
      name: metadata.name,
      version: metadata.version,
      language: metadata.language ?? "typescript",
      engine: metadata.engine ?? "v8",
      schema: metadata.schema,
      artifact: metadata.artifact,
      visibility: metadata.visibility,
      crons: metadata.crons,
    });
  }

  visibility(options: VisibilityPlan): VisibilityPlan {
    return this.manifestCollector.registerVisibility(options);
  }

  cron(options: CronOptions): CronSpec {
    return this.manifestCollector.registerCron(cron(options));
  }

  tenantCron(options: CronOptions): CronSpec {
    return this.manifestCollector.registerCron(tenantCron(options));
  }

  query<Args = JsonValue, Result = JsonValue>(path: string, options: QueryOptions<Args, Result> = {}): RegisteredFunction<Args, Result> {
    const liveQueryPlan = options.liveQueryPlan;
    const replica = options.replica;
    // `ModuleBuilder.query()` follows the same contract as the static artifact
    // parser: a plan describes the SQL source, not the delivery mode.
    const delivery = options.delivery ?? "oneShot";
    if (delivery === "live" && !liveQueryPlan) throw new Error(`live query ${normalizePath(path)} requires a live query plan`);
    if (delivery === "oneShot") {
      if (!liveQueryPlan) throw new Error(`one-shot query ${normalizePath(path)} requires a structured live query plan`);
      validateStructuredQueryPlan(liveQueryPlan, normalizePath(path));
    }
    if (delivery === "replica") {
      if (!replica) throw new Error(`replica collection ${normalizePath(path)} requires a replica definition`);
      validateReplicaCollection(replica, normalizePath(path));
    }
    const definition = this.manifestCollector.register(path, {
      kind: "query",
      args: options.args,
      result: options.result,
      delivery,
      liveQueryPlan,
      replica,
      internal: options.internal,
    });
    const registration = freeze({ path: definition.path, kind: definition.kind, definition, handler: options.run as RegisteredFunction<Args, Result>["handler"] });
    this.runtimeEntries.set(definition.path, registration as RuntimeFunctionRegistration);
    return registration;
  }

  /** Register a Query delivered as a live, structured query stream. */
  liveQuery<Args = JsonValue, Result = JsonValue>(path: string, options: Omit<QueryOptions<Args, Result>, "delivery"> = {}): RegisteredFunction<Args, Result> {
    return this.query(path, { ...options, delivery: "live" });
  }

  /** Register a Query delivered as a bounded local replica collection. */
  replicaCollection<Args = JsonValue, Result = JsonValue>(path: string, options: ReplicaCollectionOptions<Args, Result>): RegisteredFunction<Args, Result> {
    return this.query(path, { ...options, delivery: "replica", replica: options.replica });
  }

  reducer<Args = JsonValue, Result = JsonValue>(path: string, options: ReducerOptions<Args, Result>): RegisteredFunction<Args, Result> {
    const normalized = normalizePath(path);
    validateOfflinePolicy(options.offline, normalized);
    if (options.optimistic !== undefined) validateOptimisticTransaction(options.optimistic, normalized);
    if (options.interactive === false && options.optimistic !== undefined) {
      throw new Error(`non-interactive reducer ${normalized} cannot declare optimistic metadata`);
    }
    if (options.interactive !== false && options.optimistic === undefined && !options.nonOptimisticReason?.trim()) {
      throw new Error(`interactive reducer ${normalized} requires an optimistic transaction or nonOptimisticReason`);
    }
    const definition = this.manifestCollector.register(path, {
      kind: "reducer",
      args: options.args,
      result: options.result,
      offline: options.offline,
      interactive: options.interactive ?? true,
      internal: options.internal,
      optimistic: options.optimistic,
      nonOptimisticReason: options.nonOptimisticReason?.trim() || undefined,
    });
    const registration = freeze({ path: definition.path, kind: definition.kind, definition, handler: options.run as RegisteredFunction<Args, Result>["handler"] });
    this.runtimeEntries.set(definition.path, registration as RuntimeFunctionRegistration);
    return registration;
  }

  /** Register a non-public Reducer while retaining kind `reducer` in the manifest. */
  internalReducer<Args = JsonValue, Result = JsonValue>(path: string, options: InternalReducerOptions<Args, Result>): RegisteredFunction<Args, Result> {
    return this.reducer(path, {
      ...options,
      offline: options.offline ?? { mode: "forbidden" },
      interactive: false,
      internal: true,
    });
  }

  action<Args = JsonValue, Result = JsonValue, const Capabilities extends ActionCapabilities = ActionCapabilities>(path: string, options: ActionOptions<Args, Result, Capabilities> = {}): RegisteredFunction<Args, Result> {
    const profile = options.profile ?? "standard";
    validateActionCapabilities(profile, options.capabilities, normalizePath(path));
    const definition = this.manifestCollector.register(path, {
      kind: "action",
      args: options.args,
      result: options.result,
      actionProfile: profile,
      actionCapabilities: options.capabilities,
    });
    const registration = freeze({ path: definition.path, kind: definition.kind, definition, handler: options.run as RegisteredFunction<Args, Result>["handler"] });
    this.runtimeEntries.set(definition.path, registration as RuntimeFunctionRegistration);
    return registration;
  }

  manifest(): ModuleManifest {
    return this.manifestCollector.manifest();
  }

  serialize(space?: number): string {
    return this.manifestCollector.serialize(space);
  }

  /** Executable registrations sorted by path for deterministic host loading. */
  runtimeRegistrations(): readonly RuntimeFunctionRegistration[] {
    return Object.freeze([...this.runtimeEntries.values()].sort((a, b) => a.path.localeCompare(b.path)));
  }

  runtimePayload(): ModuleRuntimeRegistrationPayload {
    const registrations = this.runtimeRegistrations().map(({ path, kind, definition }) => ({ path, kind, definition }));
    return freeze({
      format: "gonvex.module.runtime.v1",
      manifest: this.manifest(),
      registrations: freeze(registrations),
    });
  }

  serializeRuntimePayload(space?: number): string {
    return stableJsonStringify(this.runtimePayload(), space);
  }

  createRuntimeRegistry(): ModuleRuntimeRegistry {
    return new ModuleRuntimeRegistry(this);
  }
}

/**
 * Host-side executable registry. It is deliberately unaware of V8,
 * Postgres, or network transport; an engine supplies the capability-bearing
 * context and this registry only selects and invokes the registered handler.
 */
export class ModuleRuntimeRegistry {
  private readonly entries = new Map<string, RuntimeFunctionRegistration>();
  private readonly baseManifest: ModuleManifest;

  constructor(source: ModuleBuilder) {
    this.baseManifest = source.manifest();
    for (const registration of source.runtimeRegistrations()) this.register(registration);
  }

  register(registration: RuntimeFunctionRegistration): void {
    const path = normalizePath(registration.path);
    if (path !== registration.definition.path) {
      throw new Error(`runtime registration path does not match its manifest: ${path}`);
    }
    if (registration.kind !== registration.definition.kind) {
      throw new Error(`runtime registration kind does not match its manifest: ${path}`);
    }
    if (registration.kind === "reducer") {
      validateOfflinePolicy(registration.definition.offline, path);
      if (registration.definition.optimistic !== undefined) {
        validateOptimisticTransaction(registration.definition.optimistic, path);
      }
      if (registration.definition.interactive !== false && registration.definition.optimistic === undefined && !registration.definition.nonOptimisticReason?.trim()) {
        throw new Error(`interactive reducer ${path} requires an optimistic transaction or nonOptimisticReason`);
      }
    }
    if (registration.kind === "query" && (registration.definition.delivery ?? "oneShot") === "oneShot") {
      if (!registration.definition.liveQueryPlan) throw new Error(`one-shot query ${path} requires a structured live query plan`);
      validateStructuredQueryPlan(registration.definition.liveQueryPlan, path);
    }
    if (registration.definition.delivery === "replica") {
      if (!registration.definition.replica) throw new Error(`replica query ${path} requires a replica definition`);
      validateReplicaCollection(registration.definition.replica, path);
    }
    if (this.entries.has(path)) throw new Error(`duplicate runtime registration: ${path}`);
    this.entries.set(path, freeze({ ...registration, path }));
  }

  has(path: string, kind?: ModuleFunctionKind): boolean {
    const registration = this.entries.get(normalizePath(path));
    return registration !== undefined && (kind === undefined || registration.kind === kind);
  }

  registration(path: string): RuntimeFunctionRegistration | undefined {
    return this.entries.get(normalizePath(path));
  }

  registrations(): readonly RuntimeFunctionRegistration[] {
    return Object.freeze([...this.entries.values()].sort((a, b) => a.path.localeCompare(b.path)));
  }

  manifest(): ModuleManifest {
    const functions: Record<string, ModuleFunctionManifest> = {};
    for (const registration of this.registrations()) functions[registration.path] = registration.definition;
    return freeze({
      format: this.baseManifest.format,
      name: this.baseManifest.name,
      version: this.baseManifest.version,
      language: this.baseManifest.language,
      engine: this.baseManifest.engine,
      schema: this.baseManifest.schema,
      artifact: this.baseManifest.artifact,
      crons: this.baseManifest.crons,
      visibility: this.baseManifest.visibility,
      functions: freeze(functions),
    });
  }

  registrationPayload(): ModuleRuntimeRegistrationPayload {
    return freeze({
      format: "gonvex.module.runtime.v1",
      manifest: this.manifest(),
      registrations: freeze(this.registrations().map(({ path, kind, definition }) => ({ path, kind, definition }))),
    });
  }

  serializeRegistrationPayload(space?: number): string {
    return stableJsonStringify(this.registrationPayload(), space);
  }

  async query<Args, Result>(path: string, context: QueryContext, args: Args): Promise<Result> {
    return this.dispatch({ path, kind: "query", context, args }) as Promise<Result>;
  }

  async reducer<Args, Result>(path: string, context: ReducerContext, args: Args): Promise<Result> {
    return this.dispatch({ path, kind: "reducer", context, args }) as Promise<Result>;
  }

  async action<Args, Result>(path: string, context: ActionContext, args: Args): Promise<Result> {
    return this.dispatch({ path, kind: "action", context, args }) as Promise<Result>;
  }

  async dispatch<Args = unknown>(invocation: ModuleInvocation<Args>): Promise<unknown> {
    const path = normalizePath(invocation.path);
    const registration = this.entries.get(path);
    if (!registration) throw new Error(`unknown module function: ${path}`);
    if (registration.kind !== invocation.kind) {
      throw new Error(`module function ${path} is ${registration.kind}, not ${invocation.kind}`);
    }
    if (!registration.handler) {
      throw new Error(`module function ${path} has no executable handler`);
    }

    switch (invocation.kind) {
      case "query":
        return (registration.handler as Handler<QueryContext, Args, unknown>)(invocation.context, invocation.args);
      case "reducer":
        return (registration.handler as Handler<ReducerContext, Args, unknown>)(invocation.context, invocation.args);
      case "action":
        return (registration.handler as Handler<ActionContext, Args, unknown>)(invocation.context, invocation.args);
    }
  }
}

export function createModule(metadata: ConstructorParameters<typeof ModuleBuilder>[0]): ModuleBuilder {
  return new ModuleBuilder(metadata);
}

/** Type-only helper for APIs that accept any builder options. */
export type ModuleFunctionOptions = AnyFunctionOptions;
