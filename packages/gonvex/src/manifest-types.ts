// Manifest shapes for the language-neutral TypeScript module pipeline.

export type FunctionKind = "query" | "reducer" | "action";

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type FunctionEntry = {
  kind: FunctionKind;
  handler: string;
  file: string;
  /** Language-neutral argument metadata from a TypeScript module artifact. */
  args?: ModuleSchema;
  /** Language-neutral result metadata from a TypeScript module artifact. */
  result?: ModuleSchema;
  internal?: boolean;
  delivery?: "oneShot" | "live" | "replica";
  dependencies?: FunctionDependencies;
  replica?: ReplicaCollectionDefinition;
  /** Reducer delivery policy declared by a TypeScript module. */
  offline?: JsonValue;
  /** Ordered atomic optimistic transaction declared by a TypeScript module. */
  optimistic?: JsonValue;
  actionProfile?: "standard" | "agent";
  actionCapabilities?: ActionCapabilities;
};

export type ActionToolBinding = { kind: "query" | "reducer"; function: string };
export type ActionCapabilities = {
  networkOrigins?: string[];
  secrets?: string[];
  tools?: Record<string, ActionToolBinding>;
  scheduler?: true;
  storage?: true;
  sandbox?: { duckdb?: true };
};

export type ReplicaCollectionDefinition = {
  table: string;
  key: string;
  columns: string[];
  equalFilters?: Record<string, string>;
  excludeWhenSet?: string[];
  visibilityTables?: string[];
  visibilityPlanHash?: string;
  orderBy?: string;
  orderDirection?: "asc" | "desc";
  mode?: "eager" | "progressive";
  maxRows?: number;
  maxBytes?: number;
};

export type FunctionDependencies = {
  shareByPermissions?: boolean;
  shareResultFrom?: string;
  shareResultField?: string;
  liveQueryPlan?: LiveQueryPlan;
  nonOptimisticReason?: string;
};

export type LiveQueryPlan = {
  table: string;
  key: string;
  columns?: string[];
  resultPath?: string[];
  where?: LiveExpression;
  search?: { argument: string; columns: string[] };
  filters?: { argument: string; allowedColumns: string[]; allowedOperators: FilterOperator[] };
  sort?: { columnArgument?: string; directionArgument?: string; defaultColumn: string; defaultDirection: "asc" | "desc"; allowedColumns: string[] };
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

export type LiveValue = { argument?: string; literal?: unknown };

export type Column = {
  type: string;
  nullable: boolean;
  primaryKey: boolean;
};

export type Table = {
  columns: Record<string, Column>;
  indexes: Record<string, { columns: string[]; unique: boolean; kind?: string }>;
};

export type SchemaDefinition = {
  tables: Record<string, Table>;
  controlPlaneTables: Record<string, Table>;
  tenantTables: Record<string, Table>;
};

export type VisibilityPlan = {
  table: string;
  key: string;
  sets: Record<string, VisibilitySet>;
  where: VisibilityExpression;
};

export type VisibilitySet = {
  table: string;
  select: string;
  joins: VisibilityJoin[];
  where: VisibilityConstraint[];
};

export type VisibilityJoin = {
  table: string;
  leftColumn: string;
  rightColumn: string;
};

export type VisibilityConstraint = {
  table: string;
  column: string;
  context: VisibilityContextKey;
};

export type VisibilityOperator = "public" | "permission" | "role" | "eqContext" | "inSet" | "and" | "or" | "not";
export type VisibilityContextKey = "account.id" | "member.id" | "tenant.id";

export type VisibilityExpression = {
  operator: VisibilityOperator;
  column?: string;
  context?: VisibilityContextKey;
  set?: string;
  value?: string;
  children?: VisibilityExpression[];
};

export type Manifest = {
  project: string;
  generatedAt: string;
  functions: Record<string, FunctionEntry>;
  schema: SchemaDefinition;
  module: ModuleArtifact;
  visibility?: Record<string, VisibilityPlan>;
};

export type ModuleLanguage = "typescript";

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

export type ModuleFunction = {
  kind: FunctionKind;
  /** Symbol the runtime invokes inside the module. */
  handler: string;
  /** Project-relative POSIX path, matching a key in `ModuleArtifact.files`. */
  file: string;
  /** Exported binding when the function is defined as an export. */
  export?: string;
  args: ModuleSchema;
  result: ModuleSchema;
  dependencies?: FunctionDependencies;
  internal?: boolean;
  delivery?: "oneShot" | "live" | "replica";
  replica?: ReplicaCollectionDefinition;
  // Declarative metadata is emitted into the signed artifact contract and
  // validated again by the runtime before module activation.
  offline?: JsonValue;
  optimistic?: JsonValue;
  actionProfile?: "standard" | "agent";
  actionCapabilities?: ActionCapabilities;
};

/** Self-contained JavaScript generated by the CLI for the module runtime. */
export type ModuleJavaScript = {
  /** Project-relative POSIX path written by the module bundler. */
  path: string;
  hash: string;
  /** base64-encoded bundle contents. */
  code: string;
  /** base64-encoded source map when bundle generation enables one. */
  sourceMap?: string;
};

export type ModuleArtifact = {
  language: ModuleLanguage;
  /** Artifact format generation; bumped when the layout changes. */
  generation: number;
  /** Deterministic content hash over the generation, language, and payload. */
  hash: string;
  /** Project-relative POSIX path of the module entrypoint. */
  entrypoint: string;
  functions: Record<string, ModuleFunction>;
  /** Project-relative POSIX path to base64 contents, in sorted key order. */
  files: Record<string, string>;
  javascript?: ModuleJavaScript;
  visibility: Record<string, VisibilityPlan>;
  crons?: ModuleCron[];
  invitationAcceptanceReducer?: string;
};

export type ModuleCron = {
  name: string;
  function: string;
  args?: JsonValue;
  scope: "project" | "tenant";
  intervalMs?: number;
  expression?: string;
};
