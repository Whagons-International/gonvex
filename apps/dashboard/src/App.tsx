import {
  CompactSelection,
  DataEditor,
  GridCellKind,
  type GridCell,
  type GridColumn,
  type CustomCell,
  type CustomRenderer,
  type DrawHeaderCallback,
  type Item,
  type Rectangle,
  type GridSelection,
  type EditableGridCell,
  type HeaderClickedEventArgs,
  type CellClickedEventArgs,
  type GridMouseEventArgs,
  type SpriteMap,
  type Theme,
} from "@glideapps/glide-data-grid";
import "@glideapps/glide-data-grid/dist/index.css";
import type { IconDefinition } from "@fortawesome/fontawesome-svg-core";
import {
  faAngleRight,
  faBan,
  faBolt,
  faBroom,
  faBroomBall,
  faBuilding,
  faCartFlatbedSuitcase,
  faCartShopping,
  faCircle,
  faCopy,
  faDownload,
  faFilter,
  faHeart,
  faMagnifyingGlass,
  faPause,
  faStar,
  faTags,
  faTrash,
  faTree,
  faWrench,
} from "@fortawesome/free-solid-svg-icons";
import { GonvexClient } from "@gonvex/client";
import type { GonvexAuthValue } from "@gonvex/react";
import { Avatar, Button, Calendar, Card, Checkbox, Chip, DateField, DatePicker, ListBox, NumberField, SearchField, Select, Separator } from "@heroui/react";
import { parseDate, type DateValue } from "@internationalized/date";
import { Background, Controls, MarkerType, MiniMap, ReactFlow, applyNodeChanges, type Edge, type Node, type NodeChange } from "@xyflow/react";
import { useCallback, useEffect, useMemo, useRef, useState, type ChangeEvent, type Dispatch, type FormEvent, type MutableRefObject, type ReactNode, type SetStateAction } from "react";
import type { JsonValue } from "@gonvex/protocol";
import { api } from "../gonvex/_generated/api";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type PageID = "overview" | "functions" | "data" | "test" | "logs" | "errors" | "files" | "schedules" | "realtime" | "settings";

type GridRow = string[];

type Page = {
  id: PageID;
  label: string;
  eyebrow: string;
  title: string;
  description: string;
};

type DataTableInfo = {
  name: string;
  columns: string[];
  rowCount: number;
};

type DataRowsResponse = {
  table: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total?: number;
  offset?: number;
  limit?: number;
};

type TenantTarget = {
  relationshipId?: string;
  id: string;
  projectId: string;
  name: string;
  database: string;
  status: string;
  description: string;
  provisioned?: boolean;
  runtimeCreated?: boolean;
};

type FunctionInfo = {
  name: string;
  kind: string;
  realtime: string;
  source: string;
  status: string;
};

type ManifestResponse = {
  functions?: Record<string, {
    kind: string;
    handler: string;
    file: string;
  }>;
  schema?: ManifestSchema;
};

type ManifestTable = {
  columns?: Record<string, unknown>;
};

type ManifestSchema = {
  landlordTables?: Record<string, ManifestTable>;
  tenantTables?: Record<string, ManifestTable>;
  tables?: Record<string, ManifestTable>;
};

type RuntimeFunctionMetricPoint = {
  time: string;
  calls: number;
  errors: number;
  averageDurationMs: number;
};

type RuntimeFunctionMetrics = {
  kind: string;
  calls: number;
  errors: number;
  averageDurationMs: number;
  lastDurationMs: number;
  lastCalledAt?: string;
  series: RuntimeFunctionMetricPoint[];
};

type RuntimeCacheMetricPoint = {
  time: string;
  hits: number;
  misses: number;
  bypasses: number;
  hitRate: number;
};

type RuntimeCacheMetrics = {
  hits: number;
  misses: number;
  bypasses: number;
  requests: number;
  hitRate: number;
  series: RuntimeCacheMetricPoint[];
};

export type RuntimeLogEntry = {
  time: string;
  executionId?: string;
  operationId?: string;
  startedAt?: string;
  completedAt?: string;
  project?: string;
  tenant?: string;
  userId?: string;
  userEmail?: string;
  connectionId?: string;
  browser?: string;
  deviceType?: string;
  platform?: string;
  path: string;
  kind: string;
  outcome: string;
  durationMs: number;
  error?: string;
  cache?: string;
  source?: string;
  reason?: string;
  request?: unknown;
  requestSizeBytes?: number;
  resultCount?: number;
};

type RuntimeRunningMetrics = {
  current: Record<string, number>;
  total: number;
  series: { time: string; query: number; mutation: number; action: number }[];
};

type RuntimeWebSocketMetrics = {
  connections: number;
  subscriptions: number;
  users?: number;
  details?: RuntimeWebSocketConnection[];
};

type RuntimeWebSocketConnection = {
  id: string;
  project: string;
  tenant: string;
  userId?: string;
  userEmail?: string;
  authenticated: boolean;
  connectedAt: string;
  lastActiveAt: string;
  lastActivity: string;
  lastPath?: string;
  browser?: string;
  deviceType?: string;
  platform?: string;
  connectionType?: string;
  subscriptions: string[];
};

export type RuntimeDatabaseMetrics = {
  pools: number;
  openConnections: number;
  inUse: number;
  idle: number;
  maxOpenConnections: number;
  waitCount: number;
  waitDurationMs: number;
  series: Array<{
    time: string;
    openConnections: number;
    inUse: number;
    idle: number;
    waitCount: number;
    waitDurationMs: number;
  }>;
};

type RuntimeSchedulerCron = {
  name: string;
  project?: string;
  tenant?: string;
  function: string;
  schedule: string;
  nextRun?: string;
  lastRun?: string;
  status?: string;
  runs: number;
  failures: number;
};

type RuntimeSchedulerCronGroup = RuntimeSchedulerCron & {
  key: string;
  registrations: RuntimeSchedulerCron[];
};

type RuntimeSchedulerRun = {
  time: string;
  project?: string;
  function: string;
  cron?: string;
  outcome: string;
  lagMs: number;
  durationMs: number;
  error?: string;
};

type RuntimeSchedulerPoint = {
  time: string;
  completed: number;
  failed: number;
  avgLagMs: number;
  maxRunning: number;
};

type RuntimeSchedulerMetrics = {
  running: number;
  queued: number;
  scheduled: number;
  completed: number;
  failed: number;
  lagMs: number;
  crons: RuntimeSchedulerCron[];
  recent: RuntimeSchedulerRun[];
  series: RuntimeSchedulerPoint[];
};

type RuntimeMetricsResponse = {
  generatedAt?: string;
  functions: Record<string, RuntimeFunctionMetrics>;
  cache: RuntimeCacheMetrics;
  running?: RuntimeRunningMetrics;
  websocket?: RuntimeWebSocketMetrics;
  database?: RuntimeDatabaseMetrics;
  resources?: {
    memoryBytes: number;
    memoryPerClientBytes: number;
    cpuPercent: number;
    cpuPerClientPercent: number;
    bytesReceived: number;
    bytesSent: number;
    bytesPerClient: number;
  };
  propagation?: {
    targetMs: number;
    samples: number;
    averageMs: number;
    maxMs: number;
    series: Array<{
      time: string;
      samples: number;
      averageMs: number;
      p95Ms: number;
      maxMs: number;
      serverSamples: number;
      serverAverageMs: number;
      serverMaxMs: number;
    }>;
  };
  load?: {
    sampleIntervalSeconds: number;
    series: Array<{
      time: string;
      connections: number;
      users: number;
      subscriptions: number;
      cpuPercent: number;
      memoryBytes: number;
    }>;
  };
  scheduler?: RuntimeSchedulerMetrics | null;
  logs: RuntimeLogEntry[];
};

type FunctionStat = {
  label: string;
  value: string;
  tone: "blue" | "red" | "orange";
  series: number[];
};

type FileInfo = {
  id: string;
  size: string;
  contentType: string;
  uploadedAt: string;
  objectUrl?: string;
  source: "runtime" | "local";
};

type ThemeMode = "dark" | "light";

type ActionHandler = (message: string) => void;

type SelectOption = {
  value: string;
  label: string;
  description?: string;
};

type DashboardSession = {
  email: string;
  name: string;
  avatarUrl?: string;
  provider?: "dev" | "gonvex" | "google";
  role?: "admin" | "user";
  accessToken?: string;
  expiresAt?: number;
};

type ProjectTarget = {
  id: string;
  name: string;
  environment: string;
  runtimeUrl: string;
  database: string;
  databaseMode?: DatabaseMode;
  storageBucket: string;
  status: "local" | "preview" | "offline";
  description: string;
  provisioned?: boolean;
  runtimeCreated?: boolean;
  testTab?: boolean;
  errorTrackingEnabled?: boolean;
  ownerEmail?: string;
  sharedWith?: string[];
  role?: "owner" | "admin" | "dev" | "viewer";
};

type DashboardUser = {
  email: string;
  name: string;
  role: "admin" | "user";
};

type AccountAccessToken = {
  id: string;
  name: string;
  prefix: string;
  permissions: string[];
  createdAt: string;
  expiresAt?: string;
  lastUsedAt?: string;
  revokedAt?: string;
};

type CreatedAccountAccessToken = {
  token: AccountAccessToken;
  accessToken: string;
};

type ProjectMember = {
  email: string;
  name: string;
  role: "owner" | "admin" | "dev" | "viewer";
};

type ProjectInvitation = {
  id: string;
  projectId: string;
  email: string;
  role: "owner" | "admin" | "dev" | "viewer";
  expiresAt: string;
  accepted: boolean;
};

type DashboardNotification = {
  id: string;
  type: string;
  title: string;
  body: string;
  projectId?: string;
  read: boolean;
  createdAt: string;
};

type EnvVariable = {
  name: string;
  value?: string;
  masked?: string;
  source: string;
  sensitive?: boolean;
  updatedAt?: string;
};

/** Serialize project env rows to a copyable .env body (KEY=VALUE per line). */
function envVariablesToDotEnv(variables: EnvVariable[]): string {
  return variables
    .slice()
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((variable) => {
      let value = variable.value ?? variable.masked ?? "";
      // Bulk save uses a line-based parser; quote + escape when the value would
      // break KEY=VALUE on a single line.
      if (/[\n\r"\\]/.test(value) || value !== value.trim()) {
        value = `"${value
          .replace(/\\/g, "\\\\")
          .replace(/"/g, '\\"')
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n")}"`;
      }
      return `${variable.name}=${value}`;
    })
    .join("\n");
}

type CreatedProject = {
  project: ProjectTarget;
  databaseMode: DatabaseMode;
  projectKey: string;
};

type RuntimeCreatedProject = Omit<CreatedProject, "databaseMode">;

type DatabaseMode = "single" | "multiTenant";

type DataViewMode = "rows" | "erd";

type SortDirection = "asc" | "desc";

type SortState<T extends string> = {
  key: T;
  direction: SortDirection;
};

type TestSortState = {
  key: string;
  direction: SortDirection | "default";
};

type TestTaskGridArgs = {
  offset: number;
  limit: number;
  columns: string[];
  count: "false" | "estimate";
  search?: string;
  sort?: string;
  direction?: SortDirection;
  filters?: DataFilter[];
  cursorCreatedAt?: string;
  cursorId?: string;
};

type FileSortKey = "id" | "size" | "contentType" | "uploadedAt";

type DataFilter = {
  id: string;
  column: string;
  operator: "contains" | "notContains" | "equals" | "notEquals" | "startsWith" | "endsWith" | "empty" | "notEmpty" | "oneOf" | "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" | "inRange";
  value: string;
  valueTo?: string;
};

type TestColumnFilter = {
  operator: Exclude<DataFilter["operator"], "oneOf">;
  value: string;
  valueTo?: string;
  selectedValues: string[];
};

type TestFilterKind = "text" | "set" | "date" | "number";

type TestTaskColumn = GridColumn & {
  filterKind?: TestFilterKind;
  dataColumn?: string;
  sortable?: boolean;
};

type TestFilterMenu = {
  columnIndex: number;
  column: string;
  bounds: Rectangle;
};

type AssigneeMenu = {
  rowIndex: number;
  bounds: Rectangle;
};

type AssigneeProfileMenu = {
  name: string;
  avatarUrl?: string;
  bounds: Rectangle;
};

const emptyFilterSelectionValue = "__gonvex_no_filter_values_selected__";

type InsertRowResponse = {
  table: string;
  row: Record<string, unknown>;
};

type DeleteRowsResponse = {
  table: string;
  ids: string[];
  deleted: number;
};

type UpdateRowResponse = {
  table: string;
  row: Record<string, unknown>;
};

type CreateTenantResponse = {
  tenant: TenantTarget;
};

type RandomizeTasksResponse = {
  updated: number;
  requested: number;
  durationMs: number;
};

const pages: Page[] = [
  {
    id: "overview",
    label: "Overview",
    eyebrow: "Runtime pulse",
    title: "Realtime control room",
    description: "Watch the local project runtime, generated bindings, schema sync, and grid surface from one place.",
  },
  {
    id: "functions",
    label: "Functions",
    eyebrow: "Function manifest",
    title: "Live backend surface",
    description: "Queries, mutations, storage helpers, and LiveGrid registrations extracted from app-local Go files.",
  },
  {
    id: "data",
    label: "Data",
    eyebrow: "Schema sync",
    title: "Postgres project schema",
    description: "Tables, columns, and indexes defined in gonvex/schema.go and applied safely by the Gonvex Runtime.",
  },
  {
    id: "test",
    label: "Test",
    eyebrow: "Task table lab",
    title: "Whagons-style Glide table",
    description: "Temporary sandbox for rendering task rows with custom Glide Data Grid cells inspired by the Whagons workspace table.",
  },
  {
    id: "logs",
    label: "Logs",
    eyebrow: "Runtime stream",
    title: "Runtime logs",
    description: "Function execution, cache, and runtime activity emitted by the local Gonvex Runtime.",
  },
  {
    id: "errors",
    label: "Errors",
    eyebrow: "Issue intelligence",
    title: "User error inbox",
    description: "Grouped frontend failures enriched with tenant, release, user, and device impact.",
  },
  {
    id: "files",
    label: "Files",
    eyebrow: "Object storage",
    title: "MinIO upload lab",
    description: "Storage routes and bucket configuration for S3-compatible upload testing.",
  },
  {
    id: "schedules",
    label: "Schedules",
    eyebrow: "Cron scheduler",
    title: "Scheduled jobs",
    description: "Registered crons, their next run, and recent scheduled function executions.",
  },
  {
    id: "realtime",
    label: "Realtime",
    eyebrow: "Live presence",
    title: "Connection activity",
    description: "See who is online, where every connection is pointed, and what the project is doing now.",
  },
  {
    id: "settings",
    label: "Settings",
    eyebrow: "Project config",
    title: "Local runtime settings",
    description: "Environment and runtime target configuration used by gonvex dev.",
  },
];

const functionColumns: GridColumn[] = [
  { title: "Function", id: "function", width: 180 },
  { title: "Kind", id: "kind", width: 120 },
  { title: "Realtime", id: "realtime", width: 120 },
  { title: "Source", id: "source", width: 220 },
  { title: "Status", id: "status", width: 160 },
];

const functionRows: GridRow[] = [
  ["tasks.list", "query", "live", "gonvex/tasks.go", "ready"],
  ["tasks.create", "mutation", "invalidates", "gonvex/tasks.go", "ready"],
  ["tasks.randomizeStatusPriority", "mutation", "coalesced", "gonvex/tasks.go", "ready"],
  ["files.createUploadUrl", "mutation", "storage", "gonvex/files.go", "ready"],
  ["files.getUrl", "query", "storage", "gonvex/files.go", "ready"],
  ["files.getMetadata", "query", "storage", "gonvex/files.go", "ready"],
  ["files.delete", "mutation", "storage", "gonvex/files.go", "ready"],
  ["tasks.grid", "liveGrid", "patch stream", "gonvex/tasks.go", "ready"],
];

const functions: FunctionInfo[] = functionRows.map(([name, kind, realtime, source, status]) => ({
  name,
  kind,
  realtime,
  source,
  status,
}));

function realtimeLabel(kind: string): string {
  switch (kind) {
    case "query":
      return "live";
    case "mutation":
      return "invalidates";
    case "liveGrid":
      return "patch stream";
    case "action":
      return "async";
    case "http":
      return "route";
    case "internalMutation":
      return "internal";
    default:
      return "registered";
  }
}

function manifestFunctionsToRows(payload: ManifestResponse): FunctionInfo[] {
  return Object.entries(payload.functions ?? {})
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([name, entry]) => ({
      name,
      kind: entry.kind,
      realtime: realtimeLabel(entry.kind),
      source: entry.file || entry.handler,
      status: "ready",
    }));
}

function functionInfosToRows(items: FunctionInfo[]): GridRow[] {
  return items.map((item) => [item.name, item.kind, item.realtime, item.source, item.status]);
}

function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return "0 ms";
  if (ms < 10) return `${ms.toFixed(1)} ms`;
  return `${Math.round(ms).toLocaleString()} ms`;
}

function formatPercent(value: number): string {
  if (!Number.isFinite(value)) return "n/a";
  return `${Math.round(value * 100)}%`;
}

function buildFunctionStats(metrics: RuntimeFunctionMetrics | undefined, cache: RuntimeCacheMetrics | undefined): FunctionStat[] {
  const functionSeries = metrics?.series ?? [];
  const cacheSeries = cache?.series ?? [];
  const hasCacheRequests = (cache?.requests ?? 0) > 0;
  return [
    {
      label: "Function Calls",
      value: String(metrics?.calls ?? 0),
      tone: "blue",
      series: functionSeries.map((point) => point.calls),
    },
    {
      label: "Errors",
      value: String(metrics?.errors ?? 0),
      tone: "red",
      series: functionSeries.map((point) => point.errors),
    },
    {
      label: "Execution Time",
      value: formatDuration(metrics?.averageDurationMs ?? 0),
      tone: "orange",
      series: functionSeries.map((point) => point.averageDurationMs),
    },
    {
      label: "Cache Hit Rate",
      value: hasCacheRequests ? formatPercent(cache?.hitRate ?? 0) : "n/a",
      tone: "blue",
      series: cacheSeries.map((point) => point.hitRate * 100),
    },
  ];
}

function formatLogTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatLogDateTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function logEntryText(entry: RuntimeLogEntry): string {
  const source = runtimeLogSourceSummary(entry);
  return [
    entry.time,
    entry.executionId ?? "",
    entry.operationId ?? "",
    entry.tenant ?? "",
    entry.userId ?? "",
    entry.userEmail ?? "",
    entry.connectionId ?? "",
    entry.browser ?? "",
    entry.path,
    entry.kind,
    entry.outcome,
    entry.cache ?? "",
    entry.source ?? "",
    entry.reason ?? "",
    source.tableLabel,
    entry.error ?? "",
    entry.request ? JSON.stringify(entry.request) : "",
    formatDuration(entry.durationMs),
  ].join(" ").toLowerCase();
}

type RuntimeLogSourceSummary = {
  key: string;
  group: "redis" | "database" | "websocket" | "unknown";
  label: string;
  tableLabel: string;
  detail: string;
};

export function runtimeLogSourceSummary(entry: RuntimeLogEntry): RuntimeLogSourceSummary {
  const source = entry.source?.toLowerCase();
  const cache = entry.cache?.toLowerCase();
  if (source === "redis" || cache === "hit") {
    return { key: "redis-hit", group: "redis", label: "Redis", tableLabel: "Redis hit", detail: "Redis hit; database skipped" };
  }
  if (source === "database" || cache === "miss" || cache === "bypass" || cache === "error") {
    if (cache === "miss") return { key: "database-miss", group: "database", label: "Database", tableLabel: "DB miss", detail: "Redis miss; database executed" };
    if (cache === "error") return { key: "database-error", group: "database", label: "Database", tableLabel: "DB fallback", detail: "Redis error; database fallback" };
    if (cache === "bypass") return { key: "database-bypass", group: "database", label: "Database", tableLabel: "DB off", detail: "Redis not checked; database executed" };
    return { key: "database", group: "database", label: "Database", tableLabel: "DB", detail: "Database executed" };
  }
  if (source === "websocket") {
    return { key: "websocket", group: "websocket", label: "WebSocket", tableLabel: "WebSocket", detail: "Durable sync protocol" };
  }
  return {
    key: "unknown",
    group: "unknown",
    label: "Not tracked",
    tableLabel: "—",
    detail: entry.kind === "query" ? "Source not captured by this runtime" : "Not a cached query",
  };
}

function runtimeLogKey(entry: RuntimeLogEntry): string {
  return entry.executionId ?? [entry.time, entry.path, entry.kind, entry.durationMs].join("|");
}

export function runtimeLogsForCopy(logs: RuntimeLogEntry[], selectedIDs: ReadonlySet<string>): RuntimeLogEntry[] {
  if (selectedIDs.size === 0) return logs;
  return logs.filter((entry) => selectedIDs.has(runtimeLogKey(entry)));
}

function storedLogColumnWidths(): Record<string, number> {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(logsColumnWidthsKey) ?? "{}") as Record<string, number>;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

const schemaColumns: GridColumn[] = [
  { title: "Table", id: "table", width: 130 },
  { title: "Column", id: "column", width: 150 },
  { title: "Type", id: "type", width: 120 },
  { title: "Nullable", id: "nullable", width: 110 },
  { title: "Index", id: "index", width: 220 },
];

const schemaRows: GridRow[] = [
  ["tasks", "id", "id", "no", "primary key"],
  ["tasks", "title", "string", "no", ""],
  ["tasks", "status", "string", "no", "tasks_by_status"],
  ["tasks", "description", "text", "yes", ""],
  ["tasks", "priority", "string", "yes", "tasks_by_priority"],
  ["tasks", "assignee", "string", "yes", "tasks_by_assignee"],
  ["tasks", "project", "string", "yes", "tasks_by_project"],
  ["tasks", "label", "string", "yes", ""],
  ["tasks", "due_at", "time", "yes", "tasks_by_due_at"],
  ["tasks", "completed", "bool", "yes", ""],
  ["tasks", "estimate_minutes", "int", "yes", ""],
  ["tasks", "progress", "int", "yes", ""],
  ["tasks", "created_at", "time", "no", "tasks_by_created_at"],
  ["tasks", "updated_at", "time", "yes", ""],
  ["files", "id", "id", "no", "primary key"],
  ["files", "key", "string", "no", "files_by_key unique"],
  ["files", "content_type", "string", "yes", ""],
  ["files", "size", "int64", "yes", ""],
  ["files", "created_at", "time", "no", ""],
];

const dataFilterOperators: SelectOption[] = [
  { value: "contains", label: "contains" },
  { value: "equals", label: "equals" },
  { value: "notEquals", label: "does not equal" },
  { value: "startsWith", label: "starts with" },
  { value: "endsWith", label: "ends with" },
  { value: "empty", label: "is empty" },
  { value: "notEmpty", label: "is not empty" },
];
const dataFilterOperatorSet = new Set<string>([
  ...dataFilterOperators.map((operator) => operator.value),
  "notContains",
  "oneOf",
  "lessThan",
  "lessThanOrEqual",
  "greaterThan",
  "greaterThanOrEqual",
  "inRange",
]);

const emptyColumns: string[] = [];

const envRuntimeURL = String(import.meta.env.VITE_GONVEX_URL ?? import.meta.env.VITE_GONVEX_RUNTIME_URL ?? "").trim();
const envProjectID = String(import.meta.env.VITE_GONVEX_PROJECT_ID ?? "").trim();
const envProjects = String(import.meta.env.VITE_GONVEX_PROJECTS ?? "").trim();
const truthyEnvValues = new Set(["1", "true", "yes", "on"]);
const falsyEnvValues = new Set(["0", "false", "no", "off"]);
const dashboardAuthEnabled = optionalEnvBoolean(import.meta.env.VITE_GONVEX_AUTH_ENABLED)
  ?? (import.meta.env.PROD || import.meta.env.MODE === "test");
const dashboardAllowedEmails = parseEmailAllowlist(import.meta.env.VITE_GONVEX_ALLOWED_EMAILS);
const dashboardAllowUnlistedEmails = optionalEnvBoolean(import.meta.env.VITE_GONVEX_ALLOW_UNLISTED_EMAILS)
  ?? (!import.meta.env.PROD || import.meta.env.MODE === "test");
const dashboardEmailLoginEnabled = optionalEnvBoolean(import.meta.env.VITE_GONVEX_EMAIL_LOGIN_ENABLED)
  ?? (dashboardAllowUnlistedEmails || dashboardAllowedEmails.length > 0);
const dashboardPasswordLoginEnabled = optionalEnvBoolean(import.meta.env.VITE_GONVEX_PASSWORD_LOGIN_ENABLED)
  ?? import.meta.env.PROD;
const dashboardNativeAuthProjectID = String(import.meta.env.VITE_GONVEX_DASHBOARD_AUTH_PROJECT_ID ?? "").trim();
const dashboardGoogleLoginEnabled = googleLoginEnabled(
  import.meta.env.VITE_GONVEX_GOOGLE_LOGIN_ENABLED,
  Boolean(dashboardNativeAuthProjectID),
);
const runtimeBaseURL = import.meta.env.MODE === "test" ? "" : trimTrailingSlash(envRuntimeURL || "http://localhost:8080");

const projectTargets: ProjectTarget[] = loadProjectTargets();

const dashboardSessionKey = "gonvex-dashboard-session";
const dashboardProjectKey = "gonvex-dashboard-project";
const dashboardProjectsKey = "gonvex-dashboard-created-projects";
const dashboardDatabaseModesKey = "gonvex-dashboard-database-modes";
const dashboardDetectedTenantsKey = "gonvex-dashboard-detected-tenants";
const dashboardHideTestTenantsKey = "gonvex-dashboard-hide-test-tenants";
const logsColumnWidthsKey = "gonvex-logs-column-widths";
const landlordDataSourceID = "__gonvex_landlord__";
const dataDatabaseQueryParam = "db";
const dataTableQueryParam = "table";
const dataSearchQueryParam = "q";
const dataTableSearchQueryParam = "tables";
const dataSortQueryParam = "sort";
const dataSortDirectionQueryParam = "dir";
const dataFiltersQueryParam = "filters";
const dataViewQueryParam = "view";
const erdLayoutStoragePrefix = "gonvex-erd-layout";

const dataPageSize = 300;
const testTaskPageSize = 50;
const rowFetchPadding = 80;
const dataRowFetchStride = 150;
const testRowFetchStride = 50;
const testRowFetchBuffer = 20;
const scrollFetchDebounceMs = 400;
const scrollSettleMs = 80;
const maxFrontendCachedRows = 100_000;

function offsetForVisibleRange(row: number, height: number, stride: number, pageSize: number): number {
  const visibleStart = Math.max(0, row);
  const visibleCount = Math.max(1, Math.ceil(height));
  const visibleEnd = visibleStart + visibleCount;
  const paddedOffset = Math.max(0, Math.floor(Math.max(0, visibleStart - rowFetchPadding) / stride) * stride);
  if (paddedOffset + pageSize >= visibleEnd) return paddedOffset;
  const visibleOffset = Math.max(0, Math.floor(visibleStart / stride) * stride);
  if (visibleOffset + pageSize >= visibleEnd) return visibleOffset;
  return Math.max(0, visibleEnd - pageSize);
}

function visibleRowsCached<T>(rowCache: Record<number, T>, startRow: number, height: number): boolean {
  const visibleCount = Math.max(1, Math.ceil(height));
  for (let index = 0; index < visibleCount; index += 1) {
    if (!rowCache[startRow + index]) return false;
  }
  return true;
}

function liveWindowCoversRange(offset: number, pageSize: number, startRow: number, height: number): boolean {
  const visibleStart = Math.max(0, Math.floor(startRow));
  const visibleEnd = visibleStart + Math.max(1, Math.ceil(height));
  return offset <= visibleStart && offset + pageSize >= visibleEnd;
}

type ScrollFetchPending = { startRow: number; height: number };
type ScrollFetchTimers = { debounceTimer: number | null; stopTimer: number | null };

function commitRowFetch(
  offset: number,
  priority: boolean,
  pendingScrollRef: MutableRefObject<ScrollFetchPending>,
  rowCacheRef: MutableRefObject<Record<number, Record<string, unknown>>>,
  requestedOffsetRef: MutableRefObject<number>,
  inFlightOffsetRef: MutableRefObject<number | null>,
  setRequestedOffset: (offset: number) => void,
  setFetchNonce: Dispatch<SetStateAction<number>>,
) {
  const pending = pendingScrollRef.current;
  if (!priority && visibleRowsCached(rowCacheRef.current, pending.startRow, pending.height)) return;
  if (!priority && requestedOffsetRef.current === offset && inFlightOffsetRef.current === offset) return;
  if (requestedOffsetRef.current === offset) {
    setFetchNonce((nonce) => nonce + 1);
    return;
  }
  setRequestedOffset(offset);
}

function scheduleScrollLiveQuery(
  pendingScrollRef: MutableRefObject<ScrollFetchPending>,
  fetchTimersRef: MutableRefObject<ScrollFetchTimers>,
  liveOffsetRef: MutableRefObject<number>,
  setLiveOffset: (offset: number) => void,
  startRow: number,
  height: number,
  pageSize: number,
) {
  pendingScrollRef.current = { startRow, height };
  const timers = fetchTimersRef.current;
  if (timers.stopTimer !== null) {
    window.clearTimeout(timers.stopTimer);
    timers.stopTimer = null;
  }
  if (timers.debounceTimer !== null) {
    window.clearTimeout(timers.debounceTimer);
    timers.debounceTimer = null;
  }

  if (liveWindowCoversRange(liveOffsetRef.current, pageSize, startRow, height)) return;

  const visibleStart = Math.max(0, Math.floor(startRow));
  const nextOffset = Math.max(0, visibleStart - testRowFetchBuffer);
  if (liveOffsetRef.current !== nextOffset) setLiveOffset(nextOffset);
}

function scheduleScrollRowFetch(
  pendingScrollRef: MutableRefObject<ScrollFetchPending>,
  fetchTimersRef: MutableRefObject<ScrollFetchTimers>,
  rowCacheRef: MutableRefObject<Record<number, Record<string, unknown>>>,
  requestedOffsetRef: MutableRefObject<number>,
  inFlightOffsetRef: MutableRefObject<number | null>,
  setRequestedOffset: (offset: number) => void,
  setFetchNonce: Dispatch<SetStateAction<number>>,
  startRow: number,
  height: number,
  stride: number,
  pageSize: number,
) {
  pendingScrollRef.current = { startRow, height };
  const timers = fetchTimersRef.current;
  const nextOffset = offsetForVisibleRange(startRow, height, stride, pageSize);

  if (timers.stopTimer !== null) {
    window.clearTimeout(timers.stopTimer);
  }
  timers.stopTimer = window.setTimeout(() => {
    timers.stopTimer = null;
    if (timers.debounceTimer !== null) {
      window.clearTimeout(timers.debounceTimer);
      timers.debounceTimer = null;
    }
    commitRowFetch(
      nextOffset,
      true,
      pendingScrollRef,
      rowCacheRef,
      requestedOffsetRef,
      inFlightOffsetRef,
      setRequestedOffset,
      setFetchNonce,
    );
  }, scrollSettleMs);

  if (visibleRowsCached(rowCacheRef.current, startRow, height)) return;

  if (timers.debounceTimer !== null) {
    window.clearTimeout(timers.debounceTimer);
  }
  timers.debounceTimer = window.setTimeout(() => {
    timers.debounceTimer = null;
    commitRowFetch(
      nextOffset,
      false,
      pendingScrollRef,
      rowCacheRef,
      requestedOffsetRef,
      inFlightOffsetRef,
      setRequestedOffset,
      setFetchNonce,
    );
  }, scrollFetchDebounceMs);
}

function clearScrollRowFetch(fetchTimersRef: MutableRefObject<ScrollFetchTimers>) {
  const timers = fetchTimersRef.current;
  if (timers.debounceTimer !== null) {
    window.clearTimeout(timers.debounceTimer);
    timers.debounceTimer = null;
  }
  if (timers.stopTimer !== null) {
    window.clearTimeout(timers.stopTimer);
    timers.stopTimer = null;
  }
}

function mergeRowsIntoCache<T>(current: Record<number, T>, rows: T[], offset: number, maxRows = maxFrontendCachedRows) {
  const next = { ...current };
  rows.forEach((row, index) => {
    next[offset + index] = row;
  });

  const keys = Object.keys(next);
  if (keys.length <= maxRows) return next;

  const center = offset + Math.floor(rows.length / 2);
  let left = 0;
  let right = keys.length - 1;
  let toDelete = keys.length - maxRows;
  while (toDelete > 0 && left <= right) {
    const leftKey = Number(keys[left]);
    const rightKey = Number(keys[right]);
    if (Math.abs(leftKey - center) > Math.abs(rightKey - center)) {
      delete next[leftKey];
      left += 1;
    } else {
      delete next[rightKey];
      right -= 1;
    }
    toDelete -= 1;
  }

  return next;
}

function replaceRowsInCache<T>(current: Record<number, T>, rows: T[], offset: number, limit: number) {
  const next = { ...current };
  for (let index = offset; index < offset + limit; index += 1) {
    delete next[index];
  }
  return mergeRowsIntoCache(next, rows, offset);
}

const devScript = "gonvex dev -- vite";
const localDeveloperSession: DashboardSession = {
  email: "local@gonvex.dev",
  name: "Local Developer",
  provider: "dev",
  role: "admin",
};

function optionalEnvBoolean(value: string | undefined): boolean | null {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized) return null;
  if (truthyEnvValues.has(normalized)) return true;
  if (falsyEnvValues.has(normalized)) return false;
  return null;
}

export function normalizeDashboardEmail(value: string): string {
  return value.trim().toLowerCase();
}

export function parseEmailAllowlist(value: string | undefined): string[] {
  const seen = new Set<string>();
  for (const part of String(value ?? "").split(/[,\n;]/)) {
    const email = normalizeDashboardEmail(part);
    if (email) seen.add(email);
  }
  return [...seen];
}

export function dashboardEmailAllowed(email: string, allowlist: readonly string[], allowUnlisted: boolean): boolean {
  const normalized = normalizeDashboardEmail(email);
  if (!normalized) return false;
  if (allowlist.length === 0) return allowUnlisted;
  return allowlist.includes(normalized);
}

export function googleLoginEnabled(value: string | undefined, hasNativeAuthConfig: boolean): boolean {
  // Native Google only works when a dashboard auth project is configured and
  // the operator explicitly enables it.
  if (!hasNativeAuthConfig) return false;
  return optionalEnvBoolean(value) ?? false;
}

async function createDashboardPasswordSession(email: string, password: string): Promise<DashboardSession> {
  const response = await fetch("/api/dashboard/login", {
    body: JSON.stringify({ email, password }),
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  const payload = await response.json().catch(() => ({})) as { error?: string; session?: DashboardSession };
  if (!response.ok || !payload.session) {
    throw new Error(payload.error ?? "Unable to sign in.");
  }
  return payload.session;
}

async function destroyDashboardPasswordSession(): Promise<void> {
  await fetch("/api/dashboard/logout", {
    credentials: "include",
    method: "POST",
  }).catch(() => undefined);
}

async function fetchDashboardSession(): Promise<DashboardSession | null> {
  const response = await fetch("/api/dashboard/session", { credentials: "include" });
  if (response.status === 401) return null;
  const payload = await response.json().catch(() => ({})) as { session?: DashboardSession };
  if (!response.ok || !payload.session) return null;
  return payload.session;
}

export function shouldRestoreDashboardCookieSession(session: DashboardSession | null): boolean {
  // Native Google auth is stored and refreshed by GonvexAuthProvider, not by
  // the dashboard server's HttpOnly password-session cookie. Calling the
  // cookie endpoint for a valid native session produces a misleading 401 on
  // every page load even though its bearer token is healthy.
  return session?.provider !== "google";
}

async function validateNativeDashboardSession(accessToken: string, user: NonNullable<GonvexAuthValue["user"]>): Promise<DashboardSession> {
  const response = await fetch(`${runtimeBaseURL}/dev/auth/me`, {
    headers: { authorization: `Bearer ${accessToken}` },
  });
  const payload = await response.json().catch(() => ({})) as {
    account?: { email?: string; name?: string; role?: "admin" | "user" };
    error?: string;
  };
  if (!response.ok || !payload.account?.email) {
    throw new Error(payload.error ?? "This Google account is not authorized to use the Gonvex dashboard.");
  }
  return {
    accessToken,
    avatarUrl: user.picture,
    email: payload.account.email,
    name: payload.account.name || user.name || payload.account.email,
    provider: "google",
    role: payload.account.role,
  };
}

function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
}

function loadProjectTargets(): ProjectTarget[] {
  if (envProjects) {
    try {
      const parsed = JSON.parse(envProjects) as ProjectTarget[];
      const valid = parsed.filter((project) => project.id && project.name && project.runtimeUrl);
      if (valid.length > 0) return valid.map(normalizeProjectTarget);
    } catch {
      // Fall through to the single-project env shape used by the template.
    }
  }

  // In a hosted, multi-user deployment (dashboard auth enabled) the project
  // list comes from the runtime API, already scoped per signed-in user by
  // canAccessProject. Seeding a bundled placeholder here would surface an
  // ownerless phantom project to every user — e.g. a stale VITE_GONVEX_PROJECT_ID
  // baked into the build showing up as "<id> project" for invited members who
  // aren't actually on it. Only seed the template project in the local-dev/test
  // flow where there is no runtime-backed project list.
  if (dashboardAuthEnabled && import.meta.env.MODE !== "test") return [];

  return [normalizeProjectTarget({
    id: envProjectID || "app",
    name: envProjectID ? `${envProjectID} project` : "Dashboard Lab",
    environment: "local dev",
    runtimeUrl: runtimeBaseURL,
    database: String(import.meta.env.VITE_GONVEX_DATABASE ?? "gonvex_dev"),
    storageBucket: String(import.meta.env.VITE_GONVEX_STORAGE_BUCKET ?? "gonvex-dev"),
    status: "local",
    description: "Project target loaded from apps/dashboard/.env.local.",
    testTab: false,
  })];
}

function normalizeProjectTarget(project: ProjectTarget): ProjectTarget {
  return {
    ...project,
    databaseMode: project.databaseMode === "multiTenant" ? "multiTenant" : project.databaseMode === "single" ? "single" : undefined,
    provisioned: project.provisioned ?? true,
    runtimeUrl: trimTrailingSlash(project.runtimeUrl),
    testTab: project.testTab ?? false,
    errorTrackingEnabled: project.errorTrackingEnabled ?? false,
  };
}

function defaultProjectID(): string {
  return envProjectID || "app";
}

function defaultProjectName(): string {
  return envProjectID ? `${envProjectID} project` : "Dashboard Lab";
}

function defaultProjectDatabase(): string {
  return String(import.meta.env.VITE_GONVEX_DATABASE ?? "gonvex_dev");
}

function isDefaultProjectTarget(project: ProjectTarget): boolean {
  return project.id === defaultProjectID()
    && project.name === defaultProjectName()
    && project.database === defaultProjectDatabase()
    && project.runtimeCreated !== true;
}

function mergeRuntimeProjects(current: ProjectTarget[], runtimeProjects: ProjectTarget[]): ProjectTarget[] {
  if (runtimeProjects.length === 0) return current;
  const runtimeIDs = new Set(runtimeProjects.map((project) => project.id));
  const runtimeDatabases = new Set(runtimeProjects.map((project) => project.database));
  const keepCurrent = current.filter((project) => {
    if (runtimeIDs.has(project.id)) return false;
    if (isDefaultProjectTarget(project) && runtimeDatabases.has(project.database)) return false;
    return true;
  });
  return [...keepCurrent, ...runtimeProjects];
}

function preferredRuntimeProject(current: ProjectTarget | null, runtimeProjects: ProjectTarget[]): ProjectTarget | null {
  if (runtimeProjects.length === 0) return null;
  if (current) {
    const byID = runtimeProjects.find((project) => project.id === current.id);
    if (byID) return byID;
    const byDatabase = runtimeProjects.find((project) => project.database === current.database);
    if (byDatabase) return byDatabase;
  }
  return runtimeProjects.find((project) => project.database === defaultProjectDatabase()) ?? runtimeProjects[0] ?? null;
}

function projectIsProvisioned(project: ProjectTarget): boolean {
  return project.provisioned !== false;
}

function runtimeURLForProject(project: ProjectTarget): string {
  return trimTrailingSlash(project.runtimeUrl || runtimeBaseURL);
}

function runtimeWebSocketURL(project: ProjectTarget, path: string): string {
  const baseURL = runtimeURLForProject(project);
  if (!baseURL) return "";
  const url = new URL(path, `${baseURL}/`);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

function runtimeHeaders(project: ProjectTarget, headers?: HeadersInit, tenantID = ""): Headers {
  const next = new Headers(headers);
  next.set("x-gonvex-project-id", project.id);
  if (tenantID) next.set("x-gonvex-tenant-id", tenantID);
  const token = dashboardAccessToken();
  if (token) next.set("authorization", `Bearer ${token}`);
  return next;
}

function dashboardAuthHeaders(headers?: HeadersInit): Headers {
  const next = new Headers(headers);
  const token = dashboardAccessToken();
  if (token) next.set("authorization", `Bearer ${token}`);
  return next;
}

function dashboardAccessToken(): string {
  if (typeof window === "undefined") return "";
  try {
    const parsed = JSON.parse(window.localStorage.getItem(dashboardSessionKey) ?? "{}") as Partial<DashboardSession>;
    return String(parsed.accessToken ?? "");
  } catch {
    return "";
  }
}

export function dashboardMetricsWebSocketProtocols(token: string): string[] {
  const normalized = token.trim();
  return normalized ? [`gonvex-dashboard-auth.${normalized}`] : [];
}

function useRuntimeMetrics(project: ProjectTarget, enabled = true) {
  const [metrics, setMetrics] = useState<RuntimeMetricsResponse | null>(null);
  const [reachable, setReachable] = useState(true);

  useEffect(() => {
    if (!enabled) {
      setMetrics(null);
      setReachable(false);
      return;
    }
    const wsURL = runtimeWebSocketURL(project, "/dev/metrics/stream");
    const baseURL = runtimeURLForProject(project);
    if (!wsURL || !baseURL || typeof WebSocket === "undefined") {
      setMetrics(null);
      setReachable(false);
      return;
    }

    let cancelled = false;
    let fallbackStarted = false;
    let receivedMetrics = false;
    const loadFallbackOnce = () => {
      if (fallbackStarted || cancelled || receivedMetrics) return;
      fallbackStarted = true;
      fetch(`${baseURL}/dev/metrics`, { headers: runtimeHeaders(project) })
        .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
        .then((payload: RuntimeMetricsResponse) => {
          if (cancelled) return;
          setMetrics(payload);
          setReachable(true);
        })
        .catch(() => {
          if (!cancelled) setReachable(false);
        });
    };

    const url = new URL(wsURL);
    url.searchParams.set("project", project.id);
    const token = dashboardAccessToken();
    const protocols = dashboardMetricsWebSocketProtocols(token);
    const socket = protocols.length > 0
      ? new WebSocket(url.toString(), protocols)
      : new WebSocket(url.toString());
    const fallbackTimer = window.setTimeout(loadFallbackOnce, 2000);
    socket.addEventListener("open", () => {
      window.clearTimeout(fallbackTimer);
      if (!cancelled) setReachable(true);
    });
    socket.addEventListener("message", (event) => {
      try {
        const payload = JSON.parse(String(event.data)) as { type?: string; metrics?: RuntimeMetricsResponse };
        if (!cancelled && payload.type === "metrics" && payload.metrics) {
          receivedMetrics = true;
          setMetrics(payload.metrics);
          setReachable(true);
        }
      } catch {
        // Ignore malformed stream frames.
      }
    });
    socket.addEventListener("error", loadFallbackOnce);
    socket.addEventListener("close", loadFallbackOnce);
    return () => {
      cancelled = true;
      window.clearTimeout(fallbackTimer);
      socket.close();
    };
  }, [enabled, project]);

  return { metrics, reachable, setMetrics };
}

const gonvexClients = new Map<string, GonvexClient>();

function gonvexClientForProject(project: ProjectTarget, tenantID = ""): GonvexClient | null {
  const baseURL = runtimeURLForProject(project);
  if (!baseURL) return null;
  const url = new URL(`${baseURL.replace(/^http/, "ws")}/ws`);
  url.searchParams.set("project", project.id);
  if (tenantID) url.searchParams.set("tenant", tenantID);
  const key = url.toString();
  let client = gonvexClients.get(key);
  if (!client) {
    client = new GonvexClient(key, { telemetry: false });
    gonvexClients.set(key, client);
  }
  return client;
}

function projectFromRuntime(project: Partial<ProjectTarget> & { id: string; name: string }): ProjectTarget {
  return normalizeProjectTarget({
    id: project.id,
    name: project.name,
    environment: project.environment ?? "local dev",
    runtimeUrl: project.runtimeUrl ?? runtimeBaseURL,
    database: project.database ?? `${project.id.replace(/-/g, "_")}_dev`,
    databaseMode: project.databaseMode === "multiTenant" ? "multiTenant" : "single",
    storageBucket: project.storageBucket ?? `${project.id}-dev`,
    status: project.status ?? "local",
    description: project.description ?? "Runtime-created project database.",
    provisioned: project.provisioned ?? true,
    runtimeCreated: project.runtimeCreated ?? true,
    testTab: project.testTab ?? false,
    errorTrackingEnabled: project.errorTrackingEnabled ?? false,
    ownerEmail: project.ownerEmail,
    sharedWith: project.sharedWith,
    role: project.role as ProjectTarget["role"],
  });
}

async function fetchRuntimeProjects(): Promise<ProjectTarget[]> {
  const urls = runtimeBaseURL ? [`${runtimeBaseURL}/dev/projects`, "/dev/projects"] : ["/dev/projects"];
  let lastError = "runtime projects unavailable";
  for (const url of urls) {
    try {
      const response = await fetch(url, { headers: dashboardAuthHeaders() });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({} as { error?: string }));
        lastError = payload.error ?? response.statusText ?? `HTTP ${response.status}`;
        continue;
      }
      const contentType = response.headers.get("content-type") ?? "";
      if (!contentType.includes("application/json")) {
        lastError = response.url.includes("/login") ? "dashboard sign-in is required" : "runtime returned a non-JSON response";
        continue;
      }
      const payload = await response.json() as { projects?: Array<Partial<ProjectTarget> & { id: string; name: string }> };
      return (payload.projects ?? []).map(projectFromRuntime);
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
  }
  throw new Error(lastError);
}

async function createRuntimeProject(name: string, databaseMode: DatabaseMode): Promise<RuntimeCreatedProject> {
  const response = await fetch(`${runtimeBaseURL}/dev/projects`, {
    body: JSON.stringify({ name, databaseMode, testTab: false }),
    headers: dashboardAuthHeaders({ "content-type": "application/json" }),
    method: "POST",
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({} as { error?: string }));
    throw new Error(payload.error ?? response.statusText);
  }
  const payload = await response.json() as { project: Partial<ProjectTarget> & { id: string; name: string }; projectKey?: string };
  return {
    project: projectFromRuntime(payload.project),
    projectKey: payload.projectKey ?? "",
  };
}

async function updateRuntimeProject(project: ProjectTarget, update: { name?: string; databaseMode?: DatabaseMode }): Promise<ProjectTarget> {
  const baseURL = runtimeURLForProject(project);
  if (!baseURL) return { ...project, ...update };
  const response = await fetch(`${baseURL}/dev/projects/${encodeURIComponent(project.id)}`, {
    body: JSON.stringify(update),
    headers: runtimeHeaders(project, { "content-type": "application/json" }),
    method: "PATCH",
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({} as { error?: string }));
    throw new Error(payload.error ?? response.statusText);
  }
  const payload = await response.json() as { project?: Partial<ProjectTarget> & { id: string; name: string } };
  return payload.project ? projectFromRuntime(payload.project) : { ...project, ...update };
}

async function deleteRuntimeProject(projectID: string): Promise<void> {
  const response = await fetch(`${runtimeBaseURL}/dev/projects/${encodeURIComponent(projectID)}`, { headers: dashboardAuthHeaders(), method: "DELETE" });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({} as { error?: string }));
    throw new Error(payload.error ?? response.statusText);
  }
}

async function createDashboardUser(email: string, name: string, password: string, role: DashboardUser["role"]): Promise<DashboardUser> {
  const response = await fetch(`${runtimeBaseURL}/dev/auth/users`, {
    body: JSON.stringify({ email, name, password, role }),
    headers: dashboardAuthHeaders({ "content-type": "application/json" }),
    method: "POST",
  });
  const payload = await response.json().catch(() => ({} as { error?: string; user?: DashboardUser }));
  if (!response.ok || !payload.user) throw new Error(payload.error ?? response.statusText);
  return payload.user;
}

async function fetchAccountAccessTokens(): Promise<AccountAccessToken[]> {
  const response = await fetch(`${runtimeBaseURL}/dev/auth/tokens`, { headers: dashboardAuthHeaders() });
  const payload = await response.json().catch(() => ({} as { error?: string; tokens?: AccountAccessToken[] }));
  if (!response.ok) throw new Error(payload.error ?? response.statusText);
  return payload.tokens ?? [];
}

async function createGlobalAdminAccessToken(name: string, expiresAt?: string): Promise<CreatedAccountAccessToken> {
  const response = await fetch(`${runtimeBaseURL}/dev/auth/tokens`, {
    body: JSON.stringify({
      name,
      permissions: ["projects:*", "admin:projects"],
      ...(expiresAt ? { expiresAt } : {}),
    }),
    headers: dashboardAuthHeaders({ "content-type": "application/json" }),
    method: "POST",
  });
  const payload = await response.json().catch(() => ({} as { error?: string }));
  if (!response.ok || !(payload as CreatedAccountAccessToken).accessToken) {
    throw new Error((payload as { error?: string }).error ?? response.statusText);
  }
  return payload as CreatedAccountAccessToken;
}

async function revokeAccountAccessToken(tokenID: string): Promise<void> {
  const response = await fetch(`${runtimeBaseURL}/dev/auth/tokens/${encodeURIComponent(tokenID)}`, {
    headers: dashboardAuthHeaders(),
    method: "DELETE",
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({} as { error?: string }));
    throw new Error(payload.error ?? response.statusText);
  }
}

async function fetchDashboardNotifications(): Promise<{ notifications: DashboardNotification[]; unread: number }> {
  const response = await fetch(`${runtimeBaseURL}/dev/auth/notifications`, { headers: dashboardAuthHeaders() });
  const payload = await response.json().catch(() => ({} as { error?: string; notifications?: DashboardNotification[]; unread?: number }));
  if (!response.ok) throw new Error(payload.error ?? response.statusText);
  return { notifications: payload.notifications ?? [], unread: payload.unread ?? 0 };
}

async function markDashboardNotificationsRead(ids?: string[]): Promise<void> {
  const response = await fetch(`${runtimeBaseURL}/dev/auth/notifications/read`, {
    body: JSON.stringify(ids && ids.length > 0 ? { ids } : {}),
    headers: dashboardAuthHeaders({ "content-type": "application/json" }),
    method: "POST",
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({} as { error?: string }));
    throw new Error(payload.error ?? response.statusText);
  }
}

function formatNotificationTime(iso: string): string {
  const time = Date.parse(iso);
  if (Number.isNaN(time)) return "";
  const diffMs = Date.now() - time;
  const minutes = Math.round(diffMs / 60000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  if (days < 7) return `${days}d ago`;
  return new Date(time).toLocaleDateString();
}

function NotificationBell() {
  const [open, setOpen] = useState(false);
  const [notifications, setNotifications] = useState<DashboardNotification[]>([]);
  const [unread, setUnread] = useState(0);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const refresh = useCallback(async () => {
    try {
      const payload = await fetchDashboardNotifications();
      setNotifications(payload.notifications);
      setUnread(payload.unread);
    } catch {
      // Notifications are non-critical; ignore transient fetch failures.
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 30000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event: PointerEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as globalThis.Node)) {
        setOpen(false);
      }
    };
    window.addEventListener("pointerdown", onPointerDown);
    return () => window.removeEventListener("pointerdown", onPointerDown);
  }, [open]);

  const toggle = async () => {
    const next = !open;
    setOpen(next);
    if (next && unread > 0) {
      setUnread(0);
      setNotifications((current) => current.map((item) => ({ ...item, read: true })));
      try {
        await markDashboardNotificationsRead();
      } catch {
        // Ignore; the badge will re-sync on the next poll.
      }
    }
  };

  return (
    <div className="notification-bell" ref={containerRef}>
      <button
        aria-label={unread > 0 ? `Notifications, ${unread} unread` : "Notifications"}
        className="notification-bell-button"
        onClick={() => void toggle()}
        type="button"
      >
        <svg aria-hidden="true" fill="none" height="18" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" viewBox="0 0 24 24" width="18">
          <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
          <path d="M13.73 21a2 2 0 0 1-3.46 0" />
        </svg>
        {unread > 0 ? <span className="notification-bell-badge">{unread > 9 ? "9+" : unread}</span> : null}
      </button>
      {open ? (
        <div className="notification-panel" role="dialog" aria-label="Notifications">
          <div className="notification-panel-header">Notifications</div>
          {notifications.length === 0 ? (
            <div className="notification-empty">You're all caught up.</div>
          ) : (
            <ul className="notification-list">
              {notifications.map((item) => (
                <li className={item.read ? "notification-item" : "notification-item notification-item--unread"} key={item.id}>
                  <div className="notification-item-title">{item.title}</div>
                  {item.body ? <div className="notification-item-body">{item.body}</div> : null}
                  <div className="notification-item-time">{formatNotificationTime(item.createdAt)}</div>
                </li>
              ))}
            </ul>
          )}
        </div>
      ) : null}
    </div>
  );
}

async function fetchProjectMembers(project: ProjectTarget): Promise<{ members: ProjectMember[]; invitations: ProjectInvitation[] }> {
  const response = await fetch(`${runtimeURLForProject(project)}/dev/projects/${encodeURIComponent(project.id)}/members`, {
    headers: runtimeHeaders(project),
  });
  const payload = await response.json().catch(() => ({} as { error?: string; members?: ProjectMember[]; invitations?: ProjectInvitation[] }));
  if (!response.ok) throw new Error(payload.error ?? response.statusText);
  return { members: payload.members ?? [], invitations: payload.invitations ?? [] };
}

async function inviteProjectMember(project: ProjectTarget, email: string, role: ProjectMember["role"]): Promise<ProjectInvitation> {
  const response = await fetch(`${runtimeURLForProject(project)}/dev/projects/${encodeURIComponent(project.id)}/invitations`, {
    body: JSON.stringify({ email, role }),
    headers: runtimeHeaders(project, { "content-type": "application/json" }),
    method: "POST",
  });
  const payload = await response.json().catch(() => ({} as { error?: string; invitation?: ProjectInvitation }));
  if (!response.ok || !payload.invitation) throw new Error(payload.error ?? response.statusText);
  return payload.invitation;
}

const darkGridTheme: Partial<Theme> = {
  accentColor: "#e5482f",
  accentFg: "#ffffff",
  accentLight: "#3a2320",
  bgCell: "#221d1c",
  bgCellMedium: "#292322",
  bgHeader: "#302927",
  bgHeaderHasFocus: "#3a302d",
  bgHeaderHovered: "#3a302d",
  borderColor: "#433936",
  cellHorizontalPadding: 12,
  cellVerticalPadding: 8,
  fgIconHeader: "#f5efec",
  fontFamily: "Geist, ui-sans-serif, system-ui, sans-serif",
  headerIconSize: 16,
  headerFontStyle: "600 12px",
  lineHeight: 1.45,
  markerFontStyle: "11px",
  roundingRadius: 6,
  textDark: "#f5efec",
  textHeader: "#d8ceca",
  textHeaderSelected: "#ffffff",
  textLight: "#8f837e",
  textMedium: "#c8beb9",
};

const lightGridTheme: Partial<Theme> = {
  accentColor: "#d93f45",
  accentFg: "#ffffff",
  accentLight: "#f4d2d0",
  bgCell: "#f8f5f3",
  bgCellMedium: "#eee8e5",
  bgHeader: "#ebe5e2",
  bgHeaderHasFocus: "#e2d9d5",
  bgHeaderHovered: "#e2d9d5",
  borderColor: "#d9d0cc",
  cellHorizontalPadding: 12,
  cellVerticalPadding: 8,
  fgIconHeader: "#211c1b",
  fontFamily: "Geist, ui-sans-serif, system-ui, sans-serif",
  headerIconSize: 16,
  headerFontStyle: "600 12px",
  lineHeight: 1.45,
  markerFontStyle: "11px",
  roundingRadius: 6,
  textDark: "#211c1b",
  textHeader: "#5e5551",
  textHeaderSelected: "#211c1b",
  textLight: "#867b76",
  textMedium: "#4c4440",
};

function gridThemeFor(mode: ThemeMode): Partial<Theme> {
  return mode === "dark" ? darkGridTheme : lightGridTheme;
}

function emptyGridSelection(): GridSelection {
  return {
    columns: CompactSelection.empty(),
    rows: CompactSelection.empty(),
  };
}

const glideHeaderIcons: SpriteMap = {
  listFilter: ({ fgColor }) => `
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="${fgColor}" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round">
      <path d="M5 6h14"/>
      <path d="M8 12h8"/>
      <path d="M11 18h2"/>
    </svg>
  `,
};

function withHeaderFilterIcon(column: GridColumn): GridColumn {
  return {
    ...column,
    hasMenu: column.hasMenu ?? true,
    menuIcon: column.menuIcon ?? "listFilter",
  };
}

function isHeaderMenuPoint(x: number, y: number, bounds: Rectangle): boolean {
  const menuSize = 34;
  const menuWidth = Math.min(menuSize, bounds.width);
  const menuHeight = bounds.height;
  const menuX = bounds.width - menuWidth;
  const menuY = 0;

  return x >= menuX && x <= menuX + menuWidth && y >= menuY && y <= menuY + menuHeight;
}

function isHeaderMenuIconClick(event: HeaderClickedEventArgs): boolean {
  return isHeaderMenuPoint(event.localEventX, event.localEventY, event.bounds);
}

function createCellGetter(rows: GridRow[]) {
  return ([column, row]: Item): GridCell => ({
    kind: GridCellKind.Text,
    allowOverlay: false,
    displayData: rows[row]?.[column] ?? "",
    data: rows[row]?.[column] ?? "",
  });
}

function createCachedCellGetter(columns: string[], rowCache: Record<number, Record<string, unknown>>) {
  return ([column, row]: Item): GridCell => {
    const value = rowCache[row]?.[columns[column]];
    const displayData = value === undefined ? "" : formatCellValue(value);
    return {
      kind: GridCellKind.Text,
      allowOverlay: false,
      displayData,
      data: displayData,
    };
  };
}

type WhTaskCellData = {
  kind: "taskName" | "statusPill" | "priorityPill" | "dateStack" | "progressBar" | "flag" | "assignees" | "selection" | "notes" | "config" | "form";
  primary: string;
  secondary?: string;
  color?: string;
  textColor?: string;
  muted?: boolean;
  progress?: number;
  tagNames?: string[];
  tagColors?: string[];
  attachmentCount?: number;
  viewCount?: number;
  categoryIcon?: string;
  categoryColor?: string;
  hovered?: boolean;
  selected?: boolean;
  selectedAt?: number;
  statusAction?: string;
  statusIcon?: string;
  workingAnimation?: string;
  initial?: boolean;
  names?: string[];
  avatarUrls?: string[];
  options?: string[];
  optionAvatarUrls?: Record<string, string>;
};

function whTaskCell(data: WhTaskCellData): CustomCell<WhTaskCellData> {
  return {
    kind: GridCellKind.Custom,
    allowOverlay: data.kind === "assignees",
    copyData: [data.primary, data.secondary].filter(Boolean).join(" "),
    data,
  };
}

function drawRoundRect(ctx: CanvasRenderingContext2D, x: number, y: number, width: number, height: number, radius: number) {
  const r = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + width, y, x + width, y + height, r);
  ctx.arcTo(x + width, y + height, x, y + height, r);
  ctx.arcTo(x, y + height, x, y, r);
  ctx.arcTo(x, y, x + width, y, r);
  ctx.closePath();
}

function canvasEllipsize(ctx: CanvasRenderingContext2D, text: string, maxWidth: number): string {
  if (ctx.measureText(text).width <= maxWidth) return text;
  let next = text;
  while (next.length > 1 && ctx.measureText(`${next}...`).width > maxWidth) {
    next = next.slice(0, -1);
  }
  return `${next}...`;
}

function avatarColorForName(name: string): string {
  const colors = ["#5b8def", "#14b8a6", "#f97316", "#8b5cf6", "#ef4444", "#16a34a", "#d97706", "#0891b2"];
  let hash = 0;
  for (let index = 0; index < name.length; index += 1) hash = Math.imul(31, hash) + name.charCodeAt(index) | 0;
  return colors[Math.abs(hash) % colors.length];
}

function hexToRgb(value: string): { r: number; g: number; b: number } | null {
  const hex = value.trim().replace(/^#/, "");
  const normalized = hex.length === 3 ? hex.split("").map((char) => char + char).join("") : hex;
  if (!/^[0-9a-f]{6}$/i.test(normalized)) return null;
  return {
    r: Number.parseInt(normalized.slice(0, 2), 16),
    g: Number.parseInt(normalized.slice(2, 4), 16),
    b: Number.parseInt(normalized.slice(4, 6), 16),
  };
}

function alphaColor(color: string, alpha: number): string {
  const rgb = hexToRgb(color);
  return rgb ? `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${alpha})` : color;
}

function readableTextColor(color: string): string {
  const rgb = hexToRgb(color);
  if (!rgb) return "#ffffff";
  const luminance = (0.299 * rgb.r + 0.587 * rgb.g + 0.114 * rgb.b) / 255;
  return luminance > 0.62 ? "#111827" : "#ffffff";
}

const fontAwesomeCanvasIcons: Record<string, IconDefinition> = {
  "angle-right": faAngleRight,
  ban: faBan,
  bolt: faBolt,
  broom: faBroom,
  "broom-ball": faBroomBall,
  building: faBuilding,
  "cart-flatbed-suitcase": faCartFlatbedSuitcase,
  "cart-shopping": faCartShopping,
  circle: faCircle,
  filter: faFilter,
  heart: faHeart,
  "magnifying-glass": faMagnifyingGlass,
  pause: faPause,
  star: faStar,
  tags: faTags,
  tree: faTree,
  wrench: faWrench,
};

function fontAwesomeIconKey(iconClass: string | undefined): string {
  const icon = (iconClass ?? "").toLowerCase();
  const match = icon.match(/fa-([a-z0-9-]+)/g)?.at(-1);
  return match?.replace(/^fa-/, "") ?? icon.replace(/^fa-/, "").trim();
}

function drawFontAwesomeIcon(ctx: CanvasRenderingContext2D, iconClass: string | undefined, x: number, y: number, size: number, color: string): boolean {
  const icon = fontAwesomeCanvasIcons[fontAwesomeIconKey(iconClass)];
  if (!icon) return false;
  const [width, height, , , rawPath] = icon.icon;
  const path = Array.isArray(rawPath) ? rawPath.join(" ") : rawPath;
  const scale = size / Math.max(width, height);
  const drawWidth = width * scale;
  const drawHeight = height * scale;
  ctx.save();
  ctx.translate(x + (size - drawWidth) / 2, y + (size - drawHeight) / 2);
  ctx.scale(scale, scale);
  ctx.fillStyle = color;
  ctx.fill(new Path2D(path));
  ctx.restore();
  return true;
}

function drawCenteredIconText(ctx: CanvasRenderingContext2D, rect: Rectangle, label: string, theme: Theme, muted = false) {
  ctx.fillStyle = muted ? theme.textLight : theme.textMedium;
  ctx.font = "650 12px " + theme.fontFamily;
  const width = ctx.measureText(label).width;
  ctx.fillText(label, rect.x + Math.round((rect.width - width) / 2), rect.y + rect.height / 2, rect.width - 10);
}

function drawMiniLucide(ctx: CanvasRenderingContext2D, kind: "eye" | "paperclip" | "tags" | "wrench" | "plus", x: number, y: number, size: number) {
  ctx.save();
  ctx.translate(x, y);
  ctx.scale(size / 24, size / 24);
  ctx.lineWidth = 2;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  if (kind === "eye") {
    ctx.stroke(new Path2D("M2.5 12C4 8.5 7.5 5.5 12 5.5S20 8.5 21.5 12c-1.5 3.5-5 6.5-9.5 6.5S4 15.5 2.5 12z"));
    ctx.beginPath();
    ctx.arc(12, 12, 3, 0, Math.PI * 2);
    ctx.stroke();
  } else if (kind === "paperclip") {
    ctx.stroke(new Path2D("m21.44 11.05-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"));
  } else if (kind === "tags") {
    ctx.stroke(new Path2D("M20.59 13.41 12 22l-8.59-8.59A2 2 0 0 1 2.83 12V4a2 2 0 0 1 2-2h8a2 2 0 0 1 1.41.59l6.35 6.35a2 2 0 0 1 0 2.82Z"));
    ctx.beginPath();
    ctx.arc(7.5, 7.5, 1, 0, Math.PI * 2);
    ctx.stroke();
  } else if (kind === "wrench") {
    ctx.stroke(new Path2D("M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.1-3.1a6 6 0 0 1-7.9 7.9l-6 6a2.1 2.1 0 0 1-3-3l6-6a6 6 0 0 1 7.9-7.9l-3.1 3.1Z"));
  } else {
    ctx.beginPath();
    ctx.moveTo(12, 5);
    ctx.lineTo(12, 19);
    ctx.moveTo(5, 12);
    ctx.lineTo(19, 12);
    ctx.stroke();
  }
  ctx.restore();
}

function drawCategoryBadge(ctx: CanvasRenderingContext2D, x: number, y: number, color: string, icon: string | undefined) {
  drawRoundRect(ctx, x, y, 24, 24, 12);
  ctx.fillStyle = color || "#6b7280";
  ctx.fill();
  if (drawFontAwesomeIcon(ctx, icon, x + 6, y + 6, 12, "#ffffff")) return;
  ctx.strokeStyle = "#ffffff";
  ctx.lineWidth = 1.6;
  if ((icon ?? "").toLowerCase().includes("broom")) {
    ctx.beginPath();
    ctx.moveTo(x + 8, y + 16);
    ctx.lineTo(x + 16, y + 8);
    ctx.moveTo(x + 7, y + 17);
    ctx.lineTo(x + 11, y + 21);
    ctx.lineTo(x + 13, y + 18);
    ctx.stroke();
  } else if ((icon ?? "").toLowerCase().includes("tag")) {
    drawMiniLucide(ctx, "tags", x + 6, y + 6, 12);
  } else {
    drawMiniLucide(ctx, "wrench", x + 6, y + 6, 12);
  }
}

function statusSemantic(data: WhTaskCellData): "working" | "pending" | "done" | "initial" | "paused" | "cancelled" {
  const action = (data.statusAction ?? "").toLowerCase();
  const name = data.primary.toLowerCase();
  if (action === "working" || action === "in_progress" || name.includes("progress") || name.includes("progreso")) return "working";
  if (action === "done" || action === "completed" || action === "finished" || name.includes("done") || name.includes("listo") || name.includes("limpio")) return "done";
  if (action === "cancelled" || action === "canceled" || name.includes("cancel")) return "cancelled";
  if (action === "paused" || name.includes("paus")) return "paused";
  if (data.initial || action === "initial" || name.includes("pendiente") || name.includes("nuevo")) return "initial";
  return "pending";
}

function drawConfiguredStatusIcon(ctx: CanvasRenderingContext2D, icon: string | undefined, x: number, y: number, size: number): boolean {
  if (drawFontAwesomeIcon(ctx, icon, x, y, size, String(ctx.fillStyle))) return true;
  const normalized = (icon ?? "").toLowerCase();
  ctx.save();
  ctx.translate(x, y);
  ctx.lineWidth = 2;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  if (normalized.includes("angle-right") || normalized.includes("chevron-right")) {
    ctx.beginPath();
    ctx.moveTo(size * 0.38, size * 0.25);
    ctx.lineTo(size * 0.62, size * 0.5);
    ctx.lineTo(size * 0.38, size * 0.75);
    ctx.stroke();
    ctx.restore();
    return true;
  }
  if (normalized.includes("circle")) {
    ctx.beginPath();
    ctx.arc(size / 2, size / 2, size * 0.28, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
    return true;
  }
  if (normalized.includes("pause")) {
    ctx.beginPath();
    ctx.moveTo(size * 0.36, size * 0.28);
    ctx.lineTo(size * 0.36, size * 0.72);
    ctx.moveTo(size * 0.64, size * 0.28);
    ctx.lineTo(size * 0.64, size * 0.72);
    ctx.stroke();
    ctx.restore();
    return true;
  }
  if (normalized.includes("ban")) {
    ctx.beginPath();
    ctx.arc(size / 2, size / 2, size * 0.36, 0, Math.PI * 2);
    ctx.moveTo(size * 0.25, size * 0.75);
    ctx.lineTo(size * 0.75, size * 0.25);
    ctx.stroke();
    ctx.restore();
    return true;
  }
  if (normalized.includes("broom")) {
    ctx.beginPath();
    ctx.moveTo(size * 0.28, size * 0.72);
    ctx.lineTo(size * 0.72, size * 0.28);
    ctx.moveTo(size * 0.22, size * 0.78);
    ctx.lineTo(size * 0.4, size * 0.95);
    ctx.lineTo(size * 0.5, size * 0.84);
    ctx.lineTo(size * 0.34, size * 0.68);
    ctx.stroke();
    ctx.restore();
    return true;
  }
  ctx.restore();
  return false;
}

function drawStatusFallbackIcon(ctx: CanvasRenderingContext2D, semantic: ReturnType<typeof statusSemantic>, x: number, y: number, size: number) {
  ctx.save();
  ctx.translate(x, y);
  ctx.lineWidth = 2;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  if (semantic === "done") {
    ctx.beginPath();
    ctx.moveTo(size * 0.22, size * 0.52);
    ctx.lineTo(size * 0.43, size * 0.72);
    ctx.lineTo(size * 0.8, size * 0.3);
    ctx.stroke();
  } else if (semantic === "paused") {
    ctx.beginPath();
    ctx.moveTo(size * 0.36, size * 0.28);
    ctx.lineTo(size * 0.36, size * 0.72);
    ctx.moveTo(size * 0.64, size * 0.28);
    ctx.lineTo(size * 0.64, size * 0.72);
    ctx.stroke();
  } else if (semantic === "cancelled") {
    ctx.beginPath();
    ctx.moveTo(size * 0.28, size * 0.28);
    ctx.lineTo(size * 0.72, size * 0.72);
    ctx.moveTo(size * 0.72, size * 0.28);
    ctx.lineTo(size * 0.28, size * 0.72);
    ctx.stroke();
  } else if (semantic === "initial") {
    ctx.beginPath();
    ctx.moveTo(size * 0.38, size * 0.25);
    ctx.lineTo(size * 0.62, size * 0.5);
    ctx.lineTo(size * 0.38, size * 0.75);
    ctx.stroke();
  } else {
    ctx.beginPath();
    ctx.arc(size / 2, size / 2, size * 0.31, 0, Math.PI * 2);
    ctx.stroke();
  }
  ctx.restore();
}

function drawWorkingStatusAnimation(ctx: CanvasRenderingContext2D, type: string | undefined, x: number, y: number, size: number, timeMs: number) {
  const time = timeMs / 1000;
  const normalized = (type || "spinner").toLowerCase();
  ctx.save();
  if (normalized === "bounce") {
    for (let index = 0; index < 3; index += 1) {
      const phase = Math.sin((time * 7.5) + index * 0.9);
      ctx.beginPath();
      ctx.arc(x + 3 + index * 4, y + size / 2 - Math.max(0, phase) * 3, 1.7, 0, Math.PI * 2);
      ctx.fill();
    }
  } else if (normalized === "pulse") {
    const scale = 0.75 + Math.sin(time * 5.2) * 0.18;
    ctx.globalAlpha = 0.7 + Math.sin(time * 5.2) * 0.25;
    ctx.beginPath();
    ctx.arc(x + size / 2, y + size / 2, size * 0.28 * scale, 0, Math.PI * 2);
    ctx.fill();
  } else if (normalized === "wave") {
    for (let index = 0; index < 4; index += 1) {
      const height = 3 + (Math.sin(time * 8 + index * 0.8) + 1) * 3;
      drawRoundRect(ctx, x + 1 + index * 3, y + size / 2 - height / 2, 2, height, 1);
      ctx.fill();
    }
  } else if (normalized === "orbit") {
    const angle = time * Math.PI * 2;
    ctx.globalAlpha = 0.45;
    ctx.beginPath();
    ctx.arc(x + size / 2, y + size / 2, 2.4, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalAlpha = 1;
    ctx.beginPath();
    ctx.arc(x + size / 2 + Math.cos(angle) * 4.5, y + size / 2 + Math.sin(angle) * 4.5, 2, 0, Math.PI * 2);
    ctx.fill();
  } else if (normalized === "ripple") {
    const phase = (time * 1.4) % 1;
    ctx.globalAlpha = 0.35;
    ctx.beginPath();
    ctx.arc(x + size / 2, y + size / 2, 2.2 + phase * 5, 0, Math.PI * 2);
    ctx.stroke();
    ctx.globalAlpha = 1;
    ctx.beginPath();
    ctx.arc(x + size / 2, y + size / 2, 2.4, 0, Math.PI * 2);
    ctx.fill();
  } else {
    const angle = time * Math.PI * 2.4;
    ctx.translate(x + size / 2, y + size / 2);
    ctx.rotate(angle);
    ctx.beginPath();
    ctx.arc(0, 0, size * 0.35, -0.2, Math.PI * 1.35);
    ctx.stroke();
  }
  ctx.restore();
}

const whTaskRenderer: CustomRenderer = {
  kind: GridCellKind.Custom,
  needsHover: true,
  isMatch: (cell): cell is CustomCell<WhTaskCellData> => {
    const data = cell.data as Partial<WhTaskCellData> | undefined;
    return data?.kind === "taskName" || data?.kind === "statusPill" || data?.kind === "priorityPill" || data?.kind === "dateStack" || data?.kind === "progressBar" || data?.kind === "flag" || data?.kind === "assignees" || data?.kind === "selection" || data?.kind === "notes" || data?.kind === "config" || data?.kind === "form";
  },
  draw: (args, cell) => {
    const { ctx, rect, theme, hoverAmount } = args;
    const data = cell.data as WhTaskCellData;
    ctx.save();
    ctx.textBaseline = "middle";
    if (data.kind === "taskName") {
      const compact = rect.width < 260 || rect.height < 58;
      const hasCategory = Boolean(data.categoryIcon || data.categoryColor);
      const left = rect.x + 14;
      const titleX = left + (hasCategory ? 34 : 0);
      if (hasCategory) {
        drawCategoryBadge(ctx, left, rect.y + Math.round((rect.height - 24) / 2), data.categoryColor || theme.accentColor, data.categoryIcon);
      }
      ctx.fillStyle = theme.textDark;
      ctx.font = "650 14px " + theme.fontFamily;
      const indicatorSpace = (data.tagNames?.length ? 20 : 0) + (data.viewCount ? 20 : 0) + (data.attachmentCount ? 20 : 0) + 8;
      const titleWidth = rect.width - (titleX - rect.x) - indicatorSpace - 12;
      const primary = canvasEllipsize(ctx, data.primary, titleWidth);
      const titleY = rect.y + (compact ? rect.height / 2 : 22);
      ctx.fillText(primary, titleX, titleY, titleWidth);
      if (hoverAmount > 0) {
        const underlineWidth = Math.min(ctx.measureText(primary).width, titleWidth);
        ctx.strokeStyle = "rgba(76, 68, 64, 0.38)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(titleX, titleY + 8);
        ctx.lineTo(titleX + underlineWidth, titleY + 8);
        ctx.stroke();
      }
      let indicatorX = titleX + Math.min(ctx.measureText(primary).width, titleWidth) + 10;
      ctx.strokeStyle = "#5DCAA5";
      ctx.fillStyle = "#5DCAA5";
      if (data.viewCount) {
        const chipX = indicatorX - 4;
        const chipY = titleY - 12;
        drawRoundRect(ctx, chipX, chipY, 20, 16, 8);
        ctx.fillStyle = alphaColor("#5DCAA5", hoverAmount > 0 ? 0.18 : 0.10);
        ctx.fill();
        ctx.strokeStyle = "#5DCAA5";
        drawMiniLucide(ctx, "eye", chipX + 4, chipY + 2, 12);
        indicatorX += 22;
      }
      ctx.strokeStyle = theme.textLight;
      if (data.attachmentCount) {
        const chipX = indicatorX - 4;
        const chipY = titleY - 12;
        drawRoundRect(ctx, chipX, chipY, 20, 16, 8);
        ctx.fillStyle = alphaColor("#ef4444", hoverAmount > 0 ? 0.18 : 0.10);
        ctx.fill();
        ctx.strokeStyle = "#ef4444";
        drawMiniLucide(ctx, "paperclip", chipX + 4, chipY + 2, 12);
        indicatorX += 22;
      }
      if (data.tagNames?.length) {
        ctx.strokeStyle = data.tagColors?.[0] || theme.accentColor;
        drawMiniLucide(ctx, "tags", indicatorX, titleY - 8, 14);
      }
      if (data.secondary && !compact) {
        ctx.fillStyle = theme.textMedium;
        ctx.font = "12px " + theme.fontFamily;
        const secondary = canvasEllipsize(ctx, data.secondary, rect.width - (titleX - rect.x) - 12);
        ctx.fillText(secondary, titleX, rect.y + 43, rect.width - (titleX - rect.x) - 12);
      }
      if (hoverAmount > 0 && !data.tagNames?.length && !compact) {
        ctx.strokeStyle = theme.accentColor;
        ctx.setLineDash([2, 2]);
        ctx.beginPath();
        ctx.arc(titleX + 9, rect.y + 43, 9, 0, Math.PI * 2);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.strokeStyle = theme.accentColor;
        drawMiniLucide(ctx, "plus", titleX + 3, rect.y + 37, 12);
      }
    } else if (data.kind === "statusPill" || data.kind === "priorityPill") {
      const label = data.primary || "None";
      ctx.font = "650 12px " + theme.fontFamily;
      const isStatus = data.kind === "statusPill";
      const hasIcon = isStatus;
      const iconSize = 12;
      const pillColor = data.color ?? theme.bgBubble;
      const width = Math.min(rect.width - 18, Math.max(68, ctx.measureText(label).width + (hasIcon ? 38 : 36)));
      const x = rect.x + 10;
      const y = rect.y + Math.round((rect.height - 20) / 2);
      drawRoundRect(ctx, x, y, width, 20, 10);
      ctx.fillStyle = isStatus ? pillColor : alphaColor(pillColor, 0.14);
      ctx.fill();
      if (hoverAmount > 0.05) {
        ctx.strokeStyle = isStatus ? alphaColor(readableTextColor(pillColor), 0.45) : alphaColor(pillColor, 0.55);
        ctx.lineWidth = 1.3;
        ctx.stroke();
      }
      const pillTextColor = data.textColor ?? (isStatus ? readableTextColor(pillColor) : pillColor);
      ctx.fillStyle = pillTextColor;
      ctx.strokeStyle = pillTextColor;
      if (hasIcon) {
        const iconX = x + 10;
        const iconY = y + 4;
        const semantic = statusSemantic(data);
        if (semantic === "working") {
          args.requestAnimationFrame();
          drawWorkingStatusAnimation(ctx, data.workingAnimation, iconX, iconY, iconSize, args.frameTime);
        } else if (!drawConfiguredStatusIcon(ctx, data.statusIcon, iconX, iconY, iconSize)) {
          drawStatusFallbackIcon(ctx, semantic, iconX, iconY, iconSize);
        }
      } else {
        ctx.beginPath();
        ctx.arc(x + 12, y + 10, 4.5, 0, Math.PI * 2);
        ctx.fillStyle = pillColor;
        ctx.fill();
        ctx.fillStyle = data.textColor ?? pillColor;
      }
      ctx.fillText(label, x + (hasIcon ? 27 : 24), y + 10, width - (hasIcon ? 34 : 30));
    } else if (data.kind === "assignees") {
      const names = (data.names ?? []).filter(Boolean).slice(0, 3);
      const remaining = Math.max(0, (data.names?.length ?? 0) - names.length);
      const avatarSize = 28;
      const directHovered = hoverAmount > 0.05;
      const rowHovered = Boolean(data.hovered);
      const overlap = directHovered ? 4 : 10;
      const startX = rect.x + 12;
      const centerY = rect.y + rect.height / 2;
      const drawAddCircle = (x: number, active: boolean, alpha = 1) => {
        const centerX = x + avatarSize / 2;
        const centerYLocal = centerY;
        ctx.save();
        ctx.globalAlpha *= alpha;
        ctx.beginPath();
        ctx.setLineDash([2.2, 2.2]);
        ctx.arc(centerX, centerYLocal, avatarSize / 2 - 1, 0, Math.PI * 2);
        ctx.strokeStyle = active ? theme.accentColor : theme.textLight;
        ctx.lineWidth = 1.3;
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = active ? theme.accentColor : theme.textLight;
        ctx.font = "700 14px " + theme.fontFamily;
        const plusWidth = ctx.measureText("+").width;
        ctx.fillText("+", centerX - plusWidth / 2, centerYLocal + 0.5, avatarSize);
        ctx.restore();
      };
      if (names.length === 0) {
        drawAddCircle(startX, directHovered || rowHovered);
        ctx.fillStyle = directHovered || rowHovered ? theme.textMedium : theme.textLight;
        ctx.font = "550 12px " + theme.fontFamily;
        ctx.fillText("Assign", startX + 36, centerY, rect.width - 48);
      } else if (rowHovered && !directHovered && remaining === 0) {
        drawAddCircle(startX + names.length * (avatarSize - overlap) - 8, false, 0.9);
      }
      names.forEach((name, index) => {
        const x = startX + index * (avatarSize - overlap);
        const y = centerY - avatarSize / 2;
        ctx.beginPath();
        ctx.arc(x + avatarSize / 2, y + avatarSize / 2, avatarSize / 2, 0, Math.PI * 2);
        const image = data.avatarUrls?.[index] ? args.imageLoader.loadOrGetImage(data.avatarUrls[index], args.col, args.row) : undefined;
        if (image) {
          ctx.save();
          ctx.clip();
          ctx.drawImage(image, x, y, avatarSize, avatarSize);
          ctx.restore();
        } else {
          ctx.fillStyle = avatarColorForName(name);
          ctx.fill();
        }
        ctx.lineWidth = 2;
        ctx.strokeStyle = directHovered ? theme.accentColor : theme.bgCell;
        ctx.stroke();
        if (directHovered) {
          ctx.beginPath();
          ctx.arc(x + avatarSize / 2, y + avatarSize / 2, avatarSize / 2 + 2.5, 0, Math.PI * 2);
          ctx.strokeStyle = "rgba(220, 59, 57, 0.22)";
          ctx.lineWidth = 2;
          ctx.stroke();
        }
        if (!image) {
          ctx.fillStyle = "#ffffff";
          ctx.font = "700 11px " + theme.fontFamily;
          const initials = name.split(/\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]?.toUpperCase()).join("") || "?";
          const textWidth = ctx.measureText(initials).width;
          ctx.fillText(initials, x + (avatarSize - textWidth) / 2, centerY + 0.5, avatarSize);
        }
      });
      if (remaining > 0) {
        const x = startX + names.length * (avatarSize - overlap);
        const y = centerY - avatarSize / 2;
        ctx.beginPath();
        ctx.arc(x + avatarSize / 2, y + avatarSize / 2, avatarSize / 2, 0, Math.PI * 2);
        ctx.fillStyle = theme.bgBubble;
        ctx.fill();
        ctx.lineWidth = 2;
        ctx.strokeStyle = theme.bgCell;
        ctx.stroke();
        ctx.fillStyle = theme.textMedium;
        ctx.font = "700 11px " + theme.fontFamily;
        ctx.fillText(`+${remaining}`, x + 7, centerY + 0.5, avatarSize - 8);
      } else if (names.length > 0 && directHovered) {
        drawAddCircle(startX + names.length * (avatarSize - overlap) + 4, true);
      }
    } else if (data.kind === "dateStack") {
      if (data.color && data.primary) {
        ctx.font = "650 12px " + theme.fontFamily;
        const width = Math.min(rect.width - 18, Math.max(56, ctx.measureText(data.primary).width + 18));
        const x = rect.x + 10;
        const y = rect.y + Math.round((rect.height - 24) / 2);
        drawRoundRect(ctx, x, y, width, 24, 6);
        ctx.fillStyle = data.color;
        ctx.fill();
        ctx.fillStyle = data.textColor ?? theme.textDark;
        ctx.fillText(data.primary, x + 9, y + 12, width - 16);
      } else if (data.primary) {
        ctx.fillStyle = data.muted ? theme.textLight : theme.textMedium;
        ctx.font = `${data.muted ? "400" : "550"} 12px ` + theme.fontFamily;
        ctx.fillText(data.primary, rect.x + 12, rect.y + rect.height / 2, rect.width - 24);
      }
      if (data.secondary) {
        ctx.fillStyle = data.muted ? theme.textLight : theme.textMedium;
        ctx.font = "11px " + theme.fontFamily;
        ctx.fillText(data.secondary, rect.x + 12, rect.y + 42, rect.width - 24);
      }
    } else if (data.kind === "progressBar") {
      const x = rect.x + 12;
      const y = rect.y + Math.round((rect.height - 10) / 2);
      const width = rect.width - 24;
      drawRoundRect(ctx, x, y, width, 10, 5);
      ctx.fillStyle = theme.bgCellMedium;
      ctx.fill();
      drawRoundRect(ctx, x, y, Math.max(6, width * Math.min(100, Math.max(0, data.progress ?? 0)) / 100), 10, 5);
      ctx.fillStyle = data.color ?? theme.accentColor;
      ctx.fill();
      ctx.fillStyle = theme.textMedium;
      ctx.font = "11px " + theme.fontFamily;
      ctx.fillText(`${data.progress ?? 0}%`, x, y + 24, width);
    } else if (data.kind === "flag") {
      const iconSize = 18;
      const x = rect.x + Math.round((rect.width - iconSize) / 2);
      const y = rect.y + Math.round((rect.height - iconSize) / 2);
      ctx.save();
      ctx.globalAlpha = data.color ? 1 : 0.38;
      ctx.translate(x, y);
      ctx.scale(iconSize / 24, iconSize / 24);
      ctx.strokeStyle = data.color || theme.textLight;
      ctx.lineWidth = 2;
      ctx.lineCap = "round";
      ctx.lineJoin = "round";
      ctx.stroke(new Path2D("M4 22V4a1 1 0 0 1 .4-.8A6 6 0 0 1 8 2c3 0 5 2 7.333 2q2 0 3.067-.8A1 1 0 0 1 20 4v10a1 1 0 0 1-.4.8A6 6 0 0 1 16 16c-3 0-5-2-8-2a6 6 0 0 0-4 1.528"));
      ctx.restore();
    } else if (data.kind === "selection") {
      const selectedProgress = data.selectedAt ? Math.min(1, Math.max(0, (Date.now() - data.selectedAt) / 180)) : data.selected ? 1 : 0;
      if (data.selected && selectedProgress < 1) args.requestAnimationFrame();
      if (data.selected) {
        ctx.globalAlpha = selectedProgress;
        ctx.fillStyle = theme.accentColor;
        ctx.fillRect(rect.x, rect.y, 3, rect.height);
        ctx.globalAlpha = 1;
      }
      ctx.beginPath();
      ctx.arc(rect.x + rect.width / 2, rect.y + rect.height / 2, 9, 0, Math.PI * 2);
      ctx.lineWidth = data.selected ? 5 : 1.6;
      ctx.strokeStyle = data.selected ? alphaColor(theme.accentColor, selectedProgress) : hoverAmount > 0 ? alphaColor(theme.accentColor, 0.55) : alphaColor(theme.textLight, 0.35);
      ctx.stroke();
    } else if (data.kind === "notes") {
      const count = Number(data.primary || 0);
      const iconSize = 18;
      const x = rect.x + Math.round((rect.width - iconSize) / 2);
      const y = rect.y + Math.round((rect.height - iconSize) / 2);
      if (hoverAmount > 0.05) {
        drawRoundRect(ctx, x - 7, y - 7, iconSize + 14, iconSize + 14, 7);
        ctx.fillStyle = alphaColor(count > 0 ? "#22c55e" : "#867b76", 0.10);
        ctx.fill();
      }
      ctx.save();
      ctx.translate(x, y);
      ctx.scale(iconSize / 24, iconSize / 24);
      ctx.strokeStyle = count > 0 ? "#22c55e" : theme.textLight;
      ctx.lineWidth = 2;
      ctx.lineCap = "round";
      ctx.lineJoin = "round";
      ctx.stroke(new Path2D("M22 17a2 2 0 0 1-2 2H6.828a2 2 0 0 0-1.414.586l-2.202 2.202A.71.71 0 0 1 2 21.286V5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2z"));
      ctx.restore();
      if (count > 0) {
        const badgeText = count > 99 ? "99+" : String(count);
        ctx.font = "700 9px " + theme.fontFamily;
        const badgeWidth = Math.max(15, ctx.measureText(badgeText).width + 7);
        const badgeX = x + iconSize - 5;
        const badgeY = y - 7;
        drawRoundRect(ctx, badgeX, badgeY, badgeWidth, 15, 7.5);
        ctx.fillStyle = "#22c55e";
        ctx.fill();
        ctx.strokeStyle = theme.bgCell;
        ctx.lineWidth = 1.5;
        ctx.stroke();
        ctx.fillStyle = "#ffffff";
        ctx.fillText(badgeText, badgeX + (badgeWidth - ctx.measureText(badgeText).width) / 2, badgeY + 7.5, badgeWidth - 4);
      }
    } else if (data.kind === "config") {
      if (!data.primary) {
        ctx.restore();
        return;
      }
      const x = rect.x + Math.round((rect.width - 18) / 2);
      const y = rect.y + Math.round((rect.height - 18) / 2);
      drawRoundRect(ctx, x, y, 18, 18, 5);
      ctx.strokeStyle = theme.textLight;
      ctx.lineWidth = 1.5;
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(x + 5, y + 9);
      ctx.lineTo(x + 8, y + 12);
      ctx.lineTo(x + 13, y + 6);
      ctx.strokeStyle = theme.accentColor;
      ctx.lineWidth = 1.8;
      ctx.stroke();
    } else if (data.kind === "form") {
      drawCenteredIconText(ctx, rect, data.primary || "-", theme, !data.primary);
    }
    ctx.restore();
  },
};

function columnsForDataTable(columns: string[]): GridColumn[] {
  return columns.map((column) => ({
    title: column,
    id: column,
    width: Math.max(150, Math.min(260, column.length * 14 + 80)),
  }));
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

const immutableDataColumns = new Set(["_id", "id", "tenantId", "tenant_id"]);

export function dataRowIdentity(row: Record<string, unknown> | undefined): string {
  if (!row) return "";
  return formatCellValue(row._id ?? row.id).trim();
}

export function dataEditorInputValue(value: unknown): string {
  if (value === undefined) return "";
  if (typeof value === "string") return value;
  const encoded = JSON.stringify(value);
  return encoded === undefined ? String(value) : encoded;
}

export function parseDataEditorValue(raw: string, original: unknown): unknown {
  if (typeof original === "string" || original === undefined) return raw;
  if (typeof original === "number") {
    const value = Number(raw.trim());
    if (!raw.trim() || !Number.isFinite(value)) throw new Error("Enter a valid number.");
    return value;
  }
  if (typeof original === "boolean") {
    const value = raw.trim().toLowerCase();
    if (value !== "true" && value !== "false") throw new Error('Enter either "true" or "false".');
    return value === "true";
  }
  if (original !== null && typeof original === "object") {
    try {
      return JSON.parse(raw);
    } catch {
      throw new Error("Enter valid JSON.");
    }
  }
  if (!raw.trim() || raw.trim() === "null") return null;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

function dataValueTypeLabel(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
}

function dataValuesEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  try {
    return JSON.stringify(left) === JSON.stringify(right);
  } catch {
    return false;
  }
}

function selectedRowIDs(selectedRows: ReadonlySet<number>, rowCache: Record<number, Record<string, unknown>>): string[] {
  const ids: string[] = [];
  const seen = new Set<string>();
  Array.from(selectedRows).sort((left, right) => left - right).forEach((index) => {
    const id = dataRowIdentity(rowCache[index]);
    if (!id || seen.has(id)) return;
    seen.add(id);
    ids.push(id);
  });
  return ids;
}

function saveERDNodePositions(key: string, nodes: Node<ERDNodeData>[]) {
  if (typeof window === "undefined") return;
  const positions = Object.fromEntries(nodes.map((node) => [node.id, node.position]));
  window.localStorage.setItem(key, JSON.stringify(positions));
}

function applySavedERDNodePositions(nodes: Node<ERDNodeData>[], key: string): Node<ERDNodeData>[] {
  if (typeof window === "undefined") return nodes;
  try {
    const parsed = JSON.parse(window.localStorage.getItem(key) ?? "{}") as Record<string, { x: number; y: number }>;
    return nodes.map((node) => {
      const position = parsed[node.id];
      return Number.isFinite(position?.x) && Number.isFinite(position?.y) ? { ...node, position } : node;
    });
  } catch {
    return nodes;
  }
}

function compareValues(left: unknown, right: unknown, direction: SortDirection): number {
  const leftValue = formatCellValue(left).toLowerCase();
  const rightValue = formatCellValue(right).toLowerCase();
  const comparison = leftValue.localeCompare(rightValue, undefined, { numeric: true, sensitivity: "base" });
  return direction === "asc" ? comparison : -comparison;
}

function nextSort<T extends string>(current: SortState<T> | null, key: T): SortState<T> | null {
  if (current?.key === key && current.direction === "asc") return { key, direction: "desc" };
  if (current?.key === key && current.direction === "desc") return null;
  return { key, direction: "asc" };
}

function titleWithSort(title: string, sort: SortState<string> | null, key: string): string {
  if (sort?.key !== key) return title;
  return `${title} ${sort.direction === "asc" ? "↑" : "↓"}`;
}

function titleWithTestSort(title: string, sort: TestSortState, key: string): string {
  if (sort.key !== key) return title;
  if (sort.direction === "default") return title;
  return `${title} ${sort.direction === "asc" ? "↑" : "↓"}`;
}

function nextTestSort(current: TestSortState, key: string): TestSortState {
  if (current.key !== key) return { key, direction: "asc" };
  if (current.direction === "default") return { key, direction: "asc" };
  if (current.direction === "asc") return { key, direction: "desc" };
  return { key, direction: "default" };
}

function testColumnFilterKind(column: string): TestFilterKind {
  return testTaskColumns.find((item) => String(item.id) === column)?.filterKind ?? "text";
}

function testColumnDataColumn(column: string): string {
  return testTaskColumns.find((item) => String(item.id) === column)?.dataColumn ?? testSortColumns[column] ?? column;
}

function emptyTestColumnFilter(kind: TestFilterKind = "text"): TestColumnFilter {
  return { operator: kind === "date" || kind === "number" ? "equals" : "contains", value: "", selectedValues: [] };
}

function isTestColumnFilterActive(filter: TestColumnFilter | undefined, kind: TestFilterKind = "text"): boolean {
  if (!filter) return false;
  if (kind === "set") return filter.selectedValues.length > 0;
  if (filter.operator === "empty" || filter.operator === "notEmpty") return true;
  if (filter.operator === "inRange") return filter.value.trim() !== "" || (filter.valueTo ?? "").trim() !== "";
  return filter.value.trim() !== "";
}

function testColumnFilterToDataFilter(column: string, filter: TestColumnFilter | undefined): DataFilter | null {
  const kind = testColumnFilterKind(column);
  if (!isTestColumnFilterActive(filter, kind)) return null;
  if (!filter) return null;
  const dataColumn = testColumnDataColumn(column);
  if (kind === "set" && filter.selectedValues.length > 0) {
    return { id: dataColumn, column: dataColumn, operator: "oneOf", value: JSON.stringify(filter.selectedValues) };
  }
  return { id: dataColumn, column: dataColumn, operator: filter.operator, value: filter.value.trim(), valueTo: filter.valueTo?.trim() };
}

function sortedUniqueColumnValues(rows: Record<number, Record<string, unknown>>, column: string): string[] {
  const dataColumn = testColumnDataColumn(column);
  return Array.from(new Set(Object.values(rows).map((row) => formatCellValue(row[dataColumn]))))
    .sort((left, right) => left.localeCompare(right, undefined, { numeric: true, sensitivity: "base" }));
}

function mergeSortedValues(left: string[], right: string[]): string[] {
  return Array.from(new Set([...left, ...right]))
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }));
}

function pickerDateValue(value: string | undefined): DateValue | null {
  const date = (value ?? "").trim().slice(0, 10);
  if (!date) return null;
  try {
    return parseDate(date);
  } catch {
    return null;
  }
}

function pickerDateString(value: DateValue | null): string {
  return value?.toString() ?? "";
}

function defaultValueForColumn(column: string): string {
  if (column.endsWith("_at")) return new Date().toISOString();
  return "";
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

type StorageFileEntry = {
  id: string;
  key: string;
  size: number;
  contentType?: string;
  uploadedAt: string;
  url?: string;
};

type StorageFilesResponse = {
  configured: boolean;
  bucket?: string;
  files: StorageFileEntry[];
};

function fileFromStorageObject(entry: StorageFileEntry): FileInfo {
  const uploaded = entry.uploadedAt ? new Date(entry.uploadedAt) : null;
  return {
    id: entry.id || "unknown-file",
    size: entry.size > 0 ? formatBytes(entry.size) : "—",
    contentType: entry.contentType || "—",
    uploadedAt: uploaded && !Number.isNaN(uploaded.getTime()) ? uploaded.toLocaleString() : entry.uploadedAt || "unknown",
    objectUrl: entry.url,
    source: "runtime",
  };
}

function sortFiles(files: FileInfo[], sort: SortState<FileSortKey> | null): FileInfo[] {
  if (!sort) return files;
  return [...files].sort((left, right) => compareValues(left[sort.key], right[sort.key], sort.direction));
}

function getPage(id: PageID) {
  return pages.find((page) => page.id === id) ?? pages[0];
}

function pagesForProject(project: ProjectTarget): Page[] {
  return pages.filter((page) => {
    if (page.id === "test") return project.testTab === true;
    return true;
  });
}

function pageAvailableForProject(project: ProjectTarget, pageID: PageID): boolean {
  if (pageID === "test") return project.testTab === true;
  return true;
}

function pageFromPath(pathname: string): PageID {
  const parts = pathname.replace(/^\/+/, "").split("/").filter(Boolean);
  const candidate = parts[0] === "projects" && parts[1] ? parts[2] || "overview" : parts[0] || "overview";
  return pages.some((page) => page.id === candidate) ? candidate as PageID : "overview";
}

function projectIDFromPath(pathname: string): string | null {
  const parts = pathname.replace(/^\/+/, "").split("/").filter(Boolean);
  return parts[0] === "projects" && parts[1] ? decodeURIComponent(parts[1]) : null;
}

function isProjectChooserPath(pathname: string): boolean {
  return pathname.replace(/\/+$/, "") === "/projects";
}

function storedProjectID(): string | null {
  if (typeof window === "undefined" || isProjectChooserPath(window.location.pathname)) return null;
  return projectIDFromPath(window.location.pathname) ?? window.localStorage.getItem(dashboardProjectKey);
}

function pathForProjectPage(projectID: string, id: PageID): string {
  return `/projects/${encodeURIComponent(projectID)}/${id}`;
}

function dataSourceFromURL(): string {
  if (typeof window === "undefined") return landlordDataSourceID;
  const raw = new URLSearchParams(window.location.search).get(dataDatabaseQueryParam)?.trim() ?? "";
  return raw && raw !== "landlord" ? raw : landlordDataSourceID;
}

function dataStateFromURL(): {
  filters: DataFilter[];
  rowSearch: string;
  sort: SortState<string> | null;
  sourceID: string;
  table: string;
  tableSearch: string;
  view: DataViewMode;
} {
  if (typeof window === "undefined") {
    return { filters: [], rowSearch: "", sort: null, sourceID: landlordDataSourceID, table: "", tableSearch: "", view: "rows" };
  }
  const params = new URLSearchParams(window.location.search);
  const sortKey = params.get(dataSortQueryParam)?.trim() ?? "";
  const direction = params.get(dataSortDirectionQueryParam) === "desc" ? "desc" : "asc";
  return {
    filters: parseDataFiltersParam(params.get(dataFiltersQueryParam) ?? ""),
    rowSearch: params.get(dataSearchQueryParam) ?? "",
    sort: sortKey ? { key: sortKey, direction } : null,
    sourceID: dataSourceFromURL(),
    table: params.get(dataTableQueryParam)?.trim() ?? "",
    tableSearch: params.get(dataTableSearchQueryParam) ?? "",
    view: params.get(dataViewQueryParam) === "erd" ? "erd" : "rows",
  };
}

function tenantDatabaseLabel(tenant: TenantTarget | null, fallback: string): string {
  // Prefer human display name. Physical Postgres DB ids are opaque UUIDv6 and
  // must not be the primary label in the Data tab dropdown.
  const name = tenant?.name?.trim();
  if (name) return name;
  const database = tenant?.database?.trim();
  if (database) return database;
  return fallback;
}

function tenantDisplayLabel(tenant: TenantTarget | null, fallback: string): string {
  return tenantDatabaseLabel(tenant, fallback);
}

function tenantDropdownOption(tenant: TenantTarget): { value: string; label: string; description?: string } {
  const label = tenantDatabaseLabel(tenant, tenant.id);
  const alias = tenant.database?.trim();
  // Only show a secondary line when the registry alias is a useful human slug
  // that differs from the display name — never lead with the physical DB id.
  const description =
    alias && alias !== label && !looksLikeOpaqueDatabaseId(alias) ? alias : undefined;
  return { value: tenant.id, label, description };
}

function looksLikeOpaqueDatabaseId(value: string): boolean {
  // UUIDs (with or without hyphens) and legacy <slug>_<project> physical names.
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) return true;
  if (/^[0-9a-f]{32}$/i.test(value)) return true;
  if (/_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}$/i.test(value)) return true;
  return false;
}

function parseDataFiltersParam(value: string): DataFilter[] {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value) as Partial<DataFilter>[];
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((filter, index) => ({
        id: String(filter.id ?? `url-${index}`),
        column: String(filter.column ?? ""),
        operator: dataFilterOperatorSet.has(String(filter.operator)) ? filter.operator as DataFilter["operator"] : "contains",
        value: String(filter.value ?? ""),
        ...(filter.valueTo === undefined ? {} : { valueTo: String(filter.valueTo) }),
      }))
      .filter((filter) => filter.column);
  } catch {
    return [];
  }
}

function encodeDataFiltersParam(filters: DataFilter[]): string {
  return JSON.stringify(filters.map((filter) => ({
    column: filter.column,
    operator: filter.operator,
    value: filter.value,
    ...(filter.valueTo === undefined ? {} : { valueTo: filter.valueTo }),
  })));
}

function setDataStateInURL(update: {
  filters?: DataFilter[];
  rowSearch?: string;
  sort?: SortState<string> | null;
  sourceID?: string;
  table?: string;
  tableSearch?: string;
  view?: DataViewMode;
}, replace = false) {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  if (update.sourceID !== undefined) {
    url.searchParams.set(dataDatabaseQueryParam, update.sourceID && update.sourceID !== landlordDataSourceID ? update.sourceID : "landlord");
  }
  if (update.table !== undefined) {
    if (update.table) url.searchParams.set(dataTableQueryParam, update.table);
    else url.searchParams.delete(dataTableQueryParam);
  }
  if (update.rowSearch !== undefined) {
    if (update.rowSearch.trim()) url.searchParams.set(dataSearchQueryParam, update.rowSearch.trim());
    else url.searchParams.delete(dataSearchQueryParam);
  }
  if (update.tableSearch !== undefined) {
    if (update.tableSearch.trim()) url.searchParams.set(dataTableSearchQueryParam, update.tableSearch.trim());
    else url.searchParams.delete(dataTableSearchQueryParam);
  }
  if (update.sort !== undefined) {
    if (update.sort) {
      url.searchParams.set(dataSortQueryParam, update.sort.key);
      url.searchParams.set(dataSortDirectionQueryParam, update.sort.direction);
    } else {
      url.searchParams.delete(dataSortQueryParam);
      url.searchParams.delete(dataSortDirectionQueryParam);
    }
  }
  if (update.filters !== undefined) {
    if (update.filters.length > 0) url.searchParams.set(dataFiltersQueryParam, encodeDataFiltersParam(update.filters));
    else url.searchParams.delete(dataFiltersQueryParam);
  }
  if (update.view !== undefined) {
    if (update.view === "erd") url.searchParams.set(dataViewQueryParam, "erd");
    else url.searchParams.delete(dataViewQueryParam);
  }
  const next = `${url.pathname}${url.search}${url.hash}`;
  const current = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  if (next === current) return;
  if (replace) window.history.replaceState(null, "", next);
  else window.history.pushState(null, "", next);
}

function setDataSourceInURL(sourceID: string, replace = false) {
  setDataStateInURL({ sourceID }, replace);
}

function storedTheme(): ThemeMode {
  if (typeof window === "undefined") return "dark";
  return window.localStorage.getItem("gonvex-theme") === "light" ? "light" : "dark";
}

function storedDatabaseModes(): Record<string, DatabaseMode> {
  if (typeof window === "undefined") return {};
  try {
    const parsed = JSON.parse(window.localStorage.getItem(dashboardDatabaseModesKey) ?? "{}") as Record<string, string>;
    return Object.fromEntries(
      Object.entries(parsed).filter(([, mode]) => mode === "single" || mode === "multiTenant"),
    ) as Record<string, DatabaseMode>;
  } catch {
    return {};
  }
}

function databaseModeForProject(modes: Record<string, DatabaseMode>, projectID: string, fallback: DatabaseMode = "single"): DatabaseMode {
  return modes[projectID] ?? fallback;
}

function storedHideTestTenants(): Record<string, boolean> {
  if (typeof window === "undefined") return {};
  try {
    const parsed = JSON.parse(window.localStorage.getItem(dashboardHideTestTenantsKey) ?? "{}") as Record<string, unknown>;
    return Object.fromEntries(Object.entries(parsed).filter(([, value]) => typeof value === "boolean")) as Record<string, boolean>;
  } catch {
    return {};
  }
}

function hideTestTenantsForProject(settings: Record<string, boolean>, projectID: string): boolean {
  return settings[projectID] ?? true;
}

function tenantLooksInternalOrTest(tenant: TenantTarget): boolean {
  const values = [tenant.id, tenant.name, tenant.database, tenant.description]
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  return values.some((value) => {
    const normalized = value.replace(/\s+/g, "-");
    return normalized === "test"
      || /^test\d+$/.test(normalized)
      || normalized === "telemetry"
      || normalized.endsWith("-telemetry")
      || normalized.endsWith("_telemetry")
      || normalized.startsWith("e2e-")
      || normalized.startsWith("e2e_")
      || normalized.includes("-e2e-")
      || normalized.includes("_e2e_");
  });
}

function tablesLookMultiTenant(tables: DataTableInfo[]): boolean {
  const names = new Set(tables.map((table) => table.name));
  return names.has("tenants") && (names.has("userTenantMap") || names.has("users"));
}

function useViewportHeight() {
  const [height, setHeight] = useState(() => window.innerHeight);

  useEffect(() => {
    const onResize = () => setHeight(window.innerHeight);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  return height;
}

function storedSession(): DashboardSession | null {
  if (typeof window === "undefined") return null;
  const raw = window.localStorage.getItem(dashboardSessionKey);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<DashboardSession>;
    if (!parsed.email || !parsed.name) return null;
    return {
      accessToken: parsed.accessToken,
      avatarUrl: parsed.avatarUrl,
      email: parsed.email,
      expiresAt: parsed.expiresAt,
      name: parsed.name,
      provider: parsed.provider,
      role: parsed.role,
    };
  } catch {
    return null;
  }
}

function storedCreatedProjects(): ProjectTarget[] {
  if (typeof window === "undefined") return [];
  const raw = window.localStorage.getItem(dashboardProjectsKey);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw) as ProjectTarget[];
    return parsed.filter((project) => project.id && project.name && project.runtimeUrl).map(normalizeProjectTarget);
  } catch {
    return [];
  }
}

function projectByID(projects: ProjectTarget[], id: string | null): ProjectTarget | null {
  if (!id) return null;
  return projects.find((project) => project.id === id) ?? null;
}

function visibleProjectsForSession(projects: ProjectTarget[], session: DashboardSession): ProjectTarget[] {
  const email = session.email.toLowerCase();
  return projects.filter((project) => {
    // Runtime projects are already access-scoped per user by the server
    // (canAccessProject covers ownership AND gonvex_project_members). The
    // /dev/projects response doesn't echo membership back as `sharedWith`, so
    // re-filtering on ownerEmail here would wrongly hide a project a member was
    // invited to but doesn't own. Trust the server-scoped list.
    if (project.runtimeCreated) return true;
    if (!project.ownerEmail) return true;
    if (project.ownerEmail.toLowerCase() === email) return true;
    return (project.sharedWith ?? []).some((share) => share === "*" || share.toLowerCase() === email);
  });
}

function projectIDFromName(name: string, existingProjects: ProjectTarget[]): string {
  const base = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "") || "project";
  const existing = new Set(existingProjects.map((project) => project.id));
  if (!existing.has(base)) return base;
  let index = 2;
  while (existing.has(`${base}-${index}`)) index += 1;
  return `${base}-${index}`;
}

function projectInitial(name: string): string {
  return (name.trim()[0] ?? "P").toUpperCase();
}

function displayNameFromEmail(email: string): string {
  const [name] = email.split("@");
  return name
    .split(/[._-]+/)
    .filter(Boolean)
    .map((part) => part[0].toUpperCase() + part.slice(1))
    .join(" ") || email;
}

function AppSelect(props: {
  ariaLabel: string;
  className?: string;
  isDisabled?: boolean;
  label?: string;
  onChange: (value: string) => void;
  options: SelectOption[];
  searchable?: boolean;
  searchPlaceholder?: string;
  selectedKey: string;
}) {
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const [popoverWidth, setPopoverWidth] = useState<number | undefined>();
  const [search, setSearch] = useState("");
  const visibleOptions = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!props.searchable || !query) return props.options;
    return props.options.filter((option) => [option.label, option.description ?? "", option.value].some((value) => value.toLowerCase().includes(query)));
  }, [props.options, props.searchable, search]);

  useEffect(() => {
    const trigger = triggerRef.current;
    if (!trigger) return;
    const updateWidth = () => setPopoverWidth(trigger.getBoundingClientRect().width);
    updateWidth();
    const observer = new ResizeObserver(updateWidth);
    observer.observe(trigger);
    return () => observer.disconnect();
  }, []);

  return (
    <div className={["app-select-field", props.className].filter(Boolean).join(" ")}>
      {props.label ? <span>{props.label}</span> : null}
      <Select
        aria-label={props.ariaLabel}
        className="app-select"
        isDisabled={props.isDisabled}
        onSelectionChange={(key) => {
          if (key !== null) props.onChange(String(key));
        }}
        selectedKey={props.selectedKey}
      >
        <Select.Trigger ref={triggerRef} className="app-select-trigger">
          <Select.Value />
          <Select.Indicator />
        </Select.Trigger>
        <Select.Popover className="app-select-popover" style={popoverWidth ? { width: popoverWidth } : undefined}>
          {props.searchable ? (
            <div className="app-select-search" onKeyDown={(event) => event.stopPropagation()}>
              <input
                aria-label={`Search ${props.ariaLabel}`}
                autoComplete="off"
                onChange={(event) => setSearch(event.target.value)}
                placeholder={props.searchPlaceholder ?? "Search..."}
                value={search}
              />
            </div>
          ) : null}
          <ListBox>
            {visibleOptions.map((option) => (
              <ListBox.Item id={option.value} key={option.value} textValue={option.label}>
                <span className="app-select-option-text">
                  <strong>{option.label}</strong>
                  {option.description ? <small>{option.description}</small> : null}
                </span>
                <ListBox.ItemIndicator />
              </ListBox.Item>
            ))}
            {visibleOptions.length === 0 ? (
              <ListBox.Item id="__gonvex_no_options__" key="__gonvex_no_options__" isDisabled textValue="No matching databases">
                <span className="app-select-option-text">
                  <strong>No matches</strong>
                  <small>Try another search</small>
                </span>
              </ListBox.Item>
            ) : null}
          </ListBox>
        </Select.Popover>
      </Select>
    </div>
  );
}

function ManifestGrid(props: {
  columns: GridColumn[];
  rows?: GridRow[];
  rowCount?: number;
  getCellContent?: (item: Item) => GridCell;
  customRenderers?: readonly CustomRenderer[];
  height?: number | string;
  rowHeight?: number;
  themeMode: ThemeMode;
  clearSelection?: boolean;
  clearSelectionKey?: number;
  disableSelection?: boolean;
  noGridLines?: boolean;
  zebraRows?: boolean;
  hideRowMarkers?: boolean;
  selectableRows?: boolean;
  activeFilterColumnIds?: readonly string[];
  onHeaderClick?: (column: number) => void;
  onHeaderMenuClick?: (column: number, bounds: Rectangle) => void;
  onCellActivated?: (cell: Item) => void;
  onCellEdited?: (cell: Item, newValue: EditableGridCell) => void;
  onCellClick?: (cell: Item, event: CellClickedEventArgs) => void;
  onItemHovered?: (event: GridMouseEventArgs) => void;
  hoveredRow?: number | null;
  selectedRows?: ReadonlySet<number>;
  overlay?: ReactNode;
  onColumnResize?: (column: GridColumn, newSize: number, columnIndex: number) => void;
  onVisibleRegionChanged?: (range: Rectangle) => void;
  onSelectionChange?: (selection: GridSelection) => void;
}) {
  const rows = props.rows ?? [];
  const [gridSelection, setGridSelection] = useState<GridSelection>(() => emptyGridSelection());
  const [hoveredHeaderMenuColumn, setHoveredHeaderMenuColumn] = useState<number | null>(null);
  const activeFilterColumnIds = new Set(props.activeFilterColumnIds ?? []);

  const drawHeader: DrawHeaderCallback = (args, draw) => {
    const columnId = String(args.column.id ?? "");
    const hasMenu = Boolean(args.column.hasMenu);
    const active = activeFilterColumnIds.has(columnId);
    const hovered = hasMenu && (hoveredHeaderMenuColumn === args.columnIndex || args.isHovered);
    const { ctx, menuBounds } = args;
    const iconSize = args.theme.headerIconSize;
    const iconX = Math.round(menuBounds.x + (menuBounds.width - iconSize) / 2);
    const iconY = Math.round(menuBounds.y + (menuBounds.height - iconSize) / 2);
    const hoverSize = iconSize + 8;
    const hoverX = Math.round(iconX - (hoverSize - iconSize) / 2);
    const hoverY = Math.round(iconY - (hoverSize - iconSize) / 2);

    if (hovered) {
      ctx.save();
      drawRoundRect(ctx, hoverX, hoverY, hoverSize, hoverSize, 6);
      ctx.fillStyle = "rgba(15, 23, 42, 0.18)";
      ctx.fill();
      ctx.restore();
    }

    draw();

    if (hasMenu && active && !args.isHovered) {
      args.spriteManager.drawSprite(args.column.menuIcon ?? "listFilter", "normal", ctx, iconX, iconY, iconSize, args.theme);
    }

    if (hasMenu && active) {
      ctx.save();
      ctx.beginPath();
      ctx.arc(iconX + iconSize - 2, iconY + 2, 2.5, 0, Math.PI * 2);
      ctx.fillStyle = args.theme.accentColor;
      ctx.shadowColor = "rgba(15, 23, 42, 0.35)";
      ctx.shadowBlur = 3;
      ctx.fill();
      ctx.restore();
    }
  };

  const handleMouseMove = (event: GridMouseEventArgs) => {
    props.onItemHovered?.(event);
    const nextColumn = event.kind === "header" && props.columns[event.location[0]]?.hasMenu
      ? event.location[0]
      : null;
    setHoveredHeaderMenuColumn((current) => current === nextColumn ? current : nextColumn);
  };

  useEffect(() => {
    if (props.clearSelection) {
      const empty = emptyGridSelection();
      setGridSelection(empty);
      props.onSelectionChange?.(empty);
    }
  }, [props.clearSelection]);

  useEffect(() => {
    if (props.clearSelectionKey === undefined) return;
    const empty = emptyGridSelection();
    setGridSelection(empty);
    props.onSelectionChange?.(empty);
  }, [props.clearSelectionKey]);

  useEffect(() => {
    if (!props.selectableRows || !props.selectedRows) return;
    let rows = CompactSelection.empty();
    for (const row of props.selectedRows) rows = rows.add(row);
    setGridSelection((current) => current.rows.equals(rows) ? current : { ...current, rows });
  }, [props.selectableRows, props.selectedRows]);

  const theme = props.noGridLines
    ? {
      ...gridThemeFor(props.themeMode),
      borderColor: "transparent",
      horizontalBorderColor: "transparent",
    }
    : gridThemeFor(props.themeMode);

  const cellGetter = props.getCellContent ?? createCellGetter(rows);

  return (
    <div className="grid-frame" data-testid="function-grid">
      <DataEditor
        columns={props.onHeaderMenuClick ? props.columns.map(withHeaderFilterIcon) : props.columns}
        customRenderers={props.customRenderers}
        getCellContent={cellGetter}
        getCellsForSelection={(rect) => {
          const out: GridCell[][] = [];
          for (let y = rect.y; y < rect.y + rect.height; y++) {
            const line: GridCell[] = [];
            for (let x = rect.x; x < rect.x + rect.width; x++) line.push(cellGetter([x, y]));
            out.push(line);
          }
          return out;
        }}
        gridSelection={gridSelection}
        drawHeader={drawHeader}
        headerIcons={glideHeaderIcons}
        headerHeight={38}
        height={props.height ?? 360}
        getRowThemeOverride={(row) => props.selectedRows?.has(row) ? {
          bgCell: props.themeMode === "dark" ? "#352a2f" : "#eee7f4",
          bgCellMedium: props.themeMode === "dark" ? "#3a2e34" : "#e6ddeb",
        } : row === props.hoveredRow ? {
          bgCell: props.themeMode === "dark" ? "#322928" : "#efe0de",
          bgCellMedium: props.themeMode === "dark" ? "#362d2b" : "#ead8d5",
        } : props.zebraRows && row % 2 === 1 ? {
          bgCell: props.themeMode === "dark" ? "#272120" : "#f6eeee",
          bgCellMedium: props.themeMode === "dark" ? "#2d2624" : "#efe4e2",
        } : undefined}
        maxColumnWidth={700}
        minColumnWidth={80}
        columnSelect={props.disableSelection ? "none" : "multi"}
        rangeSelect={props.disableSelection ? "none" : "rect"}
        rowSelect={props.disableSelection ? "none" : "multi"}
        onCellClicked={props.onCellClick}
        onCellActivated={props.onCellActivated}
        onColumnResize={props.onColumnResize}
        onCellEdited={props.onCellEdited}
        onGridSelectionChange={props.disableSelection ? () => {
          const empty = emptyGridSelection();
          setGridSelection(empty);
          props.onSelectionChange?.(empty);
        } : (selection) => {
          setGridSelection(selection);
          props.onSelectionChange?.(selection);
        }}
        onHeaderClicked={props.onHeaderClick ? (column, event) => {
          if (isHeaderMenuIconClick(event)) return;
          props.onHeaderClick?.(column);
        } : undefined}
        onHeaderMenuClick={props.onHeaderMenuClick}
        onItemHovered={handleMouseMove}
        onMouseMove={handleMouseMove}
        onVisibleRegionChanged={props.onVisibleRegionChanged ? (range) => props.onVisibleRegionChanged?.(range) : undefined}
        rowHeight={props.rowHeight ?? 40}
        rowMarkers={props.hideRowMarkers ? "none" : props.selectableRows ? "both" : "number"}
        rows={props.rowCount ?? rows.length}
        smoothScrollX
        smoothScrollY
        theme={theme}
        verticalBorder={!props.noGridLines}
        width="100%"
      />
      {props.overlay ? <div className="grid-overlay" role="status">{props.overlay}</div> : null}
    </div>
  );
}

export function App({ nativeAuth }: { nativeAuth?: GonvexAuthValue } = {}) {
  const [activePage, setActivePage] = useState<PageID>(() => pageFromPath(window.location.pathname));
  const [theme, setTheme] = useState<ThemeMode>(() => storedTheme());
  const [signedOut, setSignedOut] = useState(() => window.location.pathname === "/login");
  const [session, setSession] = useState<DashboardSession | null>(() => {
    if (window.location.pathname === "/login") return null;
    if (!dashboardAuthEnabled) return localDeveloperSession;
    return storedSession();
  });
  const [projects, setProjects] = useState<ProjectTarget[]>(() => projectTargets);
  const [activeProjectID, setActiveProjectID] = useState<string | null>(() => storedProjectID());
  const [databaseModes, setDatabaseModes] = useState<Record<string, DatabaseMode>>(() => storedDatabaseModes());
  const [, setDetectedTenantProjects] = useState<Record<string, boolean>>(() => {
    try {
      return JSON.parse(window.localStorage.getItem(dashboardDetectedTenantsKey) ?? "{}") as Record<string, boolean>;
    } catch {
      return {};
    }
  });
  const [hideTestTenants, setHideTestTenants] = useState<Record<string, boolean>>(() => storedHideTestTenants());
  const [actionMessage, setActionMessage] = useState("");
  const [nativeLoginError, setNativeLoginError] = useState("");
  const [validatedNativeAccessToken, setValidatedNativeAccessToken] = useState("");
  const [projectDiscoveryError, setProjectDiscoveryError] = useState("");
  const [projectDiscoveryLoading, setProjectDiscoveryLoading] = useState(false);
  const currentSession = session ?? localDeveloperSession;
  const visibleProjects = visibleProjectsForSession(projects, currentSession);
  const activeProject = projectByID(visibleProjects, activeProjectID);
  const activeProjectRef = useRef<ProjectTarget | null>(null);
  activeProjectRef.current = activeProject;
  const didAutoDiscoverProjects = useRef(false);
  const activePages = activeProject ? pagesForProject(activeProject) : pages;
  const page = activePages.find((item) => item.id === activePage) ?? activePages[0] ?? getPage("overview");
  const activeDatabaseMode = activeProject ? activeProject.databaseMode ?? databaseModeForProject(databaseModes, activeProject.id) : "single";
  const activeHideTestTenants = activeProject ? hideTestTenantsForProject(hideTestTenants, activeProject.id) : true;
  const themeLabel = theme === "dark" ? "Light mode" : "Dark mode";
  const toggleTheme = () => setTheme((current) => (current === "dark" ? "light" : "dark"));
  const reportAction: ActionHandler = (message) => setActionMessage(message);
  const loginRequired = dashboardAuthEnabled || signedOut;
  const canSignOut = dashboardAuthEnabled || dashboardPasswordLoginEnabled || Boolean(session?.accessToken) || session?.provider === "gonvex" || nativeAuth?.isAuthenticated === true;
  const nativeGoogleSessionReady = session?.provider !== "google"
    || !nativeAuth
    || (Boolean(session.accessToken) && session.accessToken === validatedNativeAccessToken);

  const login = (nextSession: DashboardSession) => {
    setSignedOut(false);
    setSession(nextSession);
    window.localStorage.setItem(dashboardSessionKey, JSON.stringify(nextSession));
    window.history.replaceState(null, "", "/projects");
  };

  const restoreSession = (nextSession: DashboardSession) => {
    setSignedOut(false);
    setSession(nextSession);
    window.localStorage.setItem(dashboardSessionKey, JSON.stringify(nextSession));
    if (window.location.pathname === "/login") window.history.replaceState(null, "", "/projects");
  };

  const loginWithPassword = async (email: string, password: string) => {
    login(await createDashboardPasswordSession(email, password));
  };

  const loginWithGoogle = async () => {
    if (!nativeAuth) throw new Error("Gonvex native Google sign-in is not configured for this dashboard.");
    setNativeLoginError("");
    await nativeAuth.signIn();
  };

  const logout = () => {
    void destroyDashboardPasswordSession();
    if (nativeAuth?.isAuthenticated) void nativeAuth.signOut();
    setSignedOut(true);
    setSession(null);
    setActiveProjectID(null);
    window.localStorage.removeItem(dashboardSessionKey);
    window.history.replaceState(null, "", "/login");
    reportAction("Signed out of the dashboard");
  };

  const showProjects = () => {
    setActiveProjectID(null);
    window.localStorage.removeItem(dashboardProjectKey);
    window.history.pushState(null, "", "/projects");
  };

  const openProject = (projectID: string) => {
    setActivePage("overview");
    setActiveProjectID(projectID);
    window.localStorage.setItem(dashboardProjectKey, projectID);
    window.history.pushState(null, "", pathForProjectPage(projectID, "overview"));
  };

  const switchProject = (projectID: string) => {
    const nextProject = projectByID(visibleProjects, projectID) ?? visibleProjects[0];
    if (!nextProject) return;
    const nextPage = pageAvailableForProject(nextProject, activePage) ? activePage : "overview";
    setActiveProjectID(nextProject.id);
    setActivePage(nextPage);
    window.localStorage.setItem(dashboardProjectKey, nextProject.id);
    window.history.pushState(null, "", pathForProjectPage(nextProject.id, nextPage));
    reportAction(`Switched to ${nextProject.name}`);
  };

  const createProject = async (name: string, databaseMode: DatabaseMode): Promise<CreatedProject> => {
    const createdProject = await createRuntimeProject(name, databaseMode);
    const ownedProject = { ...createdProject.project, databaseMode, ownerEmail: currentSession.email };
    setProjects((current) => {
      const next = [...current.filter((item) => item.id !== ownedProject.id), ownedProject];
      return next;
    });
    setDatabaseModes((current) => {
      const next = { ...current, [ownedProject.id]: databaseMode };
      window.localStorage.setItem(dashboardDatabaseModesKey, JSON.stringify(next));
      return next;
    });
    reportAction(`Created ${ownedProject.name}`);
    return { project: ownedProject, databaseMode, projectKey: createdProject.projectKey };
  };

  const createUser = async (email: string, name: string, password: string, role: DashboardUser["role"]): Promise<DashboardUser> => {
    const user = await createDashboardUser(email, name, password, role);
    reportAction(`Created ${user.email}`);
    return user;
  };

  const deleteProject = async (projectID: string) => {
    const project = projectByID(projects, projectID);
    if (!project?.runtimeCreated) return;
    await deleteRuntimeProject(projectID);
    setProjects((current) => current.filter((item) => item.id !== projectID));
    if (activeProjectID === projectID) {
      setActiveProjectID(null);
      window.localStorage.removeItem(dashboardProjectKey);
      window.history.pushState(null, "", "/projects");
    }
    reportAction(`Deleted ${project.name}`);
  };

  const navigatePage = (id: PageID) => {
    const projectID = activeProject?.id ?? activeProjectID;
    if (!projectID) return;
    setActivePage(id);
    const nextPath = pathForProjectPage(projectID, id);
    if (window.location.pathname !== nextPath) {
      window.history.pushState(null, "", nextPath);
    }
  };

  const updateProjectDatabaseMode = (mode: DatabaseMode) => {
    if (!activeProject) return;
    const previousProject = activeProject;
    const previousMode = activeDatabaseMode;
    setProjects((current) => current.map((project) => (
      project.id === activeProject.id ? { ...project, databaseMode: mode } : project
    )));
    setDatabaseModes((current) => {
      const next = { ...current, [activeProject.id]: mode };
      window.localStorage.setItem(dashboardDatabaseModesKey, JSON.stringify(next));
      return next;
    });
    reportAction(mode === "multiTenant" ? "Using landlord and tenant databases" : "Using a single project database");
    void updateRuntimeProject(previousProject, { databaseMode: mode })
      .then((savedProject) => {
        setProjects((current) => current.map((project) => (
          project.id === savedProject.id ? { ...project, ...savedProject } : project
        )));
      })
      .catch((error) => {
        setProjects((current) => current.map((project) => (
          project.id === previousProject.id ? previousProject : project
        )));
        setDatabaseModes((current) => {
          const next = { ...current, [previousProject.id]: previousMode };
          window.localStorage.setItem(dashboardDatabaseModesKey, JSON.stringify(next));
          return next;
        });
        reportAction(error instanceof Error ? error.message : "Could not save database structure");
      });
  };

  const updateProjectName = async (name: string) => {
    if (!activeProject) throw new Error("No active project selected");
    const nextName = name.trim();
    if (!nextName) throw new Error("Project name is required");
    if (nextName === activeProject.name) return;

    const previousProject = activeProject;
    setProjects((current) => current.map((project) => (
      project.id === previousProject.id ? { ...project, name: nextName } : project
    )));
    try {
      const savedProject = await updateRuntimeProject(previousProject, { name: nextName });
      setProjects((current) => current.map((project) => (
        project.id === savedProject.id ? { ...project, name: savedProject.name } : project
      )));
      reportAction(`Renamed project to ${savedProject.name}`);
    } catch (error) {
      setProjects((current) => current.map((project) => (
        project.id === previousProject.id && project.name === nextName
          ? { ...project, name: previousProject.name }
          : project
      )));
      reportAction(error instanceof Error ? error.message : "Could not rename project");
      throw error;
    }
  };

  const handleTenantsDetected = (projectID: string, hasTenants: boolean) => {
    setDetectedTenantProjects((current) => {
      if (current[projectID] === hasTenants) return current;
      const next = { ...current, [projectID]: hasTenants };
      try { window.localStorage.setItem(dashboardDetectedTenantsKey, JSON.stringify(next)); } catch { /* ignore */ }
      return next;
    });
  };

  const updateProjectHideTestTenants = (hidden: boolean) => {
    if (!activeProject) return;
    setHideTestTenants((current) => {
      const next = { ...current, [activeProject.id]: hidden };
      window.localStorage.setItem(dashboardHideTestTenantsKey, JSON.stringify(next));
      return next;
    });
    reportAction(hidden ? "Hiding test tenant databases" : "Showing all tenant databases");
  };

  const refreshRuntimeProjects = useCallback(async () => {
    setProjectDiscoveryLoading(true);
    setProjectDiscoveryError("");
    try {
      const runtimeProjects = await fetchRuntimeProjects();
      // Always apply the runtime list (including empty) so a successful
      // "no projects yet" response does not leave the chooser stuck on
      // "Checking runtime" with a stale/empty local seed.
      setProjects((current) => mergeRuntimeProjects(current, runtimeProjects));
      if (runtimeProjects.length === 0) return;
      const currentActiveProject = activeProjectRef.current;
      const requestedProjectID = projectIDFromPath(window.location.pathname);
      const requestedProject = requestedProjectID
        ? runtimeProjects.find((project) => project.id === requestedProjectID) ?? null
        : null;
      // On a cold deep-link reload the project list is initially empty, so
      // activeProjectRef cannot identify the UUID from the URL yet. Honor the
      // route before falling back to the default/first runtime project.
      const preferred = requestedProject ?? preferredRuntimeProject(currentActiveProject, runtimeProjects);
      if (!isProjectChooserPath(window.location.pathname)
        && preferred
        && currentActiveProject?.id !== preferred.id
        && (!currentActiveProject || isDefaultProjectTarget(currentActiveProject))) {
        setActiveProjectID(preferred.id);
        window.localStorage.setItem(dashboardProjectKey, preferred.id);
      }
    } catch (error) {
      setProjectDiscoveryError(error instanceof Error ? error.message : "Could not load runtime projects");
    } finally {
      setProjectDiscoveryLoading(false);
    }
  }, []);

  useEffect(() => {
    if (import.meta.env.MODE === "test") return;
    if (!dashboardAuthEnabled) return;
    if (!shouldRestoreDashboardCookieSession(session)) return;
    let cancelled = false;
    fetchDashboardSession()
      .then((nextSession) => {
        if (cancelled) return;
        if (!nextSession) {
          // Do not leave an expired localStorage session rendering read-only
          // streams while every authenticated dashboard action fails with 401.
          setSignedOut(true);
          setSession(null);
          setActiveProjectID(null);
          window.localStorage.removeItem(dashboardSessionKey);
          if (window.location.pathname !== "/login") window.history.replaceState(null, "", "/login");
          return;
        }
        restoreSession(nextSession);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!nativeAuth || nativeAuth.isLoading || !nativeAuth.isAuthenticated || !nativeAuth.user) return;
    let cancelled = false;
    const user = nativeAuth.user;
    setValidatedNativeAccessToken("");
    void nativeAuth.fetchAccessToken?.({ forceRefreshToken: false })
      .then(async (accessToken) => {
        if (!accessToken) throw new Error("The Gonvex Google session did not provide an access token.");
        const nextSession = await validateNativeDashboardSession(accessToken, user);
        if (cancelled) return;
        setNativeLoginError("");
        restoreSession(nextSession);
        setValidatedNativeAccessToken(accessToken);
      })
      .catch((error) => {
        if (cancelled) return;
        setNativeLoginError(error instanceof Error ? error.message : "Google sign-in failed.");
        setSignedOut(true);
        setSession(null);
        setActiveProjectID(null);
        window.localStorage.removeItem(dashboardSessionKey);
        if (window.location.pathname !== "/login") window.history.replaceState(null, "", "/login");
        void nativeAuth.signOut();
      });
    return () => { cancelled = true; };
  }, [nativeAuth]);

  useEffect(() => {
    if (!nativeAuth || nativeAuth.isLoading || nativeAuth.isAuthenticated || session?.provider !== "google") return;
    // The dashboard's display session mirrors the native rotating session. Do
    // not leave a stale Google dashboard session usable in the UI after the
    // native refresh token expires or is revoked.
    setSignedOut(true);
    setSession(null);
    setActiveProjectID(null);
    window.localStorage.removeItem(dashboardSessionKey);
    if (window.location.pathname !== "/login") window.history.replaceState(null, "", "/login");
  }, [nativeAuth, session?.provider]);

  useEffect(() => {
    const onPopState = () => {
      setActivePage(pageFromPath(window.location.pathname));
      setActiveProjectID(projectIDFromPath(window.location.pathname));
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    if (loginRequired && !session) {
      if (window.location.pathname !== "/login") window.history.replaceState(null, "", "/login");
      return;
    }
    if (!activeProject) {
      if (!projectIDFromPath(window.location.pathname) && window.location.pathname !== "/projects") window.history.replaceState(null, "", "/projects");
      return;
    }
    const nextPage = pageAvailableForProject(activeProject, activePage) ? activePage : "overview";
    if (nextPage !== activePage) {
      setActivePage(nextPage);
      window.history.replaceState(null, "", pathForProjectPage(activeProject.id, nextPage));
      return;
    }
    const currentPath = pathForProjectPage(activeProject.id, nextPage);
    if (window.location.pathname !== currentPath) {
      window.history.replaceState(null, "", currentPath);
    }
  }, [activePage, activeProject, loginRequired, session]);

  useEffect(() => {
    window.localStorage.setItem("gonvex-theme", theme);
    document.documentElement.dataset.theme = theme;
    document.documentElement.classList.toggle("dark", theme === "dark");
    document.documentElement.classList.toggle("light", theme === "light");
  }, [theme]);

  useEffect(() => {
    if (didAutoDiscoverProjects.current) return;
    if (loginRequired && !session) return;
    // A cached Google display session can contain an expired access token while
    // GonvexAuthProvider restores and refreshes the real native session. Wait
    // for /dev/auth/me to validate that fresh token before requesting projects,
    // otherwise the first navigation gets a one-shot 401 and only reload works.
    if (!nativeGoogleSessionReady) return;
    didAutoDiscoverProjects.current = true;
    void refreshRuntimeProjects();
  }, [loginRequired, nativeGoogleSessionReady, refreshRuntimeProjects, session]);

  useEffect(() => {
    if (!actionMessage) return;
    const timeout = window.setTimeout(() => setActionMessage(""), 2600);
    return () => window.clearTimeout(timeout);
  }, [actionMessage]);

  if (loginRequired && !session) {
    return (
      <LoginPage
        allowUnlistedEmails={dashboardAllowUnlistedEmails}
        allowedEmails={dashboardAllowedEmails}
        emailLoginEnabled={dashboardEmailLoginEnabled}
        googleAuthError={nativeLoginError || nativeAuth?.error || ""}
        googleLoginEnabled={dashboardGoogleLoginEnabled}
        onLogin={login}
        onGoogleLogin={loginWithGoogle}
        onPasswordLogin={loginWithPassword}
        onToggleTheme={toggleTheme}
        passwordLoginEnabled={dashboardPasswordLoginEnabled || signedOut}
        theme={theme}
        themeLabel={themeLabel}
      />
    );
  }

  if (!activeProject) {
    return (
      <ProjectsPage
        onCreateProject={createProject}
        onCreateUser={createUser}
        onDeleteProject={deleteProject}
        onLogout={logout}
        onOpenProject={openProject}
        onRefreshProjects={refreshRuntimeProjects}
        onToggleTheme={toggleTheme}
        authEnabled={canSignOut}
        discoveryError={projectDiscoveryError}
        discoveryLoading={projectDiscoveryLoading}
        projects={visibleProjects}
        session={currentSession}
        theme={theme}
        themeLabel={themeLabel}
      />
    );
  }

  return (
    <main className="app-shell" data-theme={theme}>
      <aside className="sidebar" aria-label="Dashboard navigation">
        <div className="brand-lockup" aria-label="Gonvex dashboard">
          <span className="brand-mark">G</span>
          <span className="brand-name">Gonvex</span>
        </div>

        <nav className="sidebar-nav" aria-label="Primary sections">
          {activePages.map((item) => (
            <Button
              key={item.id}
              className="nav-button"
              data-active={item.id === activePage ? "true" : undefined}
              onPress={() => navigatePage(item.id)}
              size="sm"
              variant={item.id === activePage ? "secondary" : "ghost"}
            >
              <span className="nav-dot" aria-hidden="true" />
              {item.label}
            </Button>
          ))}
        </nav>

        <div className="project-card" aria-label="Active project">
          <span>Project</span>
          <strong>{activeProject.name}</strong>
          <small>{activeProject.environment}</small>
        </div>

        <div className="sidebar-status">
          <Chip color="accent" size="sm" variant="soft">
            {activeProject.status}
          </Chip>
          <span>{currentSession.name}</span>
        </div>
      </aside>

      <section className="workspace" aria-labelledby={activePage === "files" ? "file-storage-title" : "app-title"}>
        {activePage === "files" ? null : (
          <header className="topbar">
            <div className="crumbs" aria-label="Breadcrumb">
              <span>Dashboard</span>
              <span>/</span>
              <strong>{page.label}</strong>
            </div>

            <div className="topbar-actions">
              <div className="project-switcher">
                <Select
                  aria-label="Active project"
                  className="project-switcher-select"
                  onSelectionChange={(key) => {
                    if (key) switchProject(String(key));
                  }}
                  selectedKey={activeProject.id}
                >
                  <Select.Trigger className="project-switcher-trigger">
                    <Select.Value />
                    <Select.Indicator />
                  </Select.Trigger>
                  <Select.Popover className="project-switcher-popover">
                    <ListBox>
                      {visibleProjects.map((project) => (
                        <ListBox.Item id={project.id} key={project.id} textValue={project.name}>
                          <span>{project.name}</span>
                          <ListBox.ItemIndicator />
                        </ListBox.Item>
                      ))}
                    </ListBox>
                  </Select.Popover>
                </Select>
              </div>
              <Button size="sm" variant="ghost" onPress={showProjects}>
                All projects
              </Button>
              <Chip color="success" size="sm" variant="soft">
                realtime on
              </Chip>
              <NotificationBell />
              <ThemeToggle themeLabel={themeLabel} onToggle={() => {
                toggleTheme();
                reportAction(`Switched to ${theme === "dark" ? "light" : "dark"} mode`);
              }} />
              <Button size="sm" variant="ghost" onPress={logout}>
                Sign out
              </Button>
            </div>
          </header>
        )}

        <div className={activePage === "files" ? "content-stack content-stack--flush" : activePage === "data" ? "content-stack content-stack--data" : "content-stack"}>
          {activePage === "overview" ? <OverviewPage project={activeProject} /> : null}
          {activePage === "functions" ? <FunctionsPage project={activeProject} themeMode={theme} onAction={reportAction} /> : null}
          {activePage === "data" ? (
            <DataPage
              databaseMode={activeDatabaseMode}
              hideTestTenants={activeHideTestTenants}
              project={activeProject}
              themeMode={theme}
              onAction={reportAction}
              onTenantsDetected={(hasTenants) => handleTenantsDetected(activeProject.id, hasTenants)}
            />
          ) : null}
          {activePage === "test" && pageAvailableForProject(activeProject, "test") ? (
            <TestPage project={activeProject} themeMode={theme} onAction={reportAction} />
          ) : null}
          {activePage === "logs" ? <LogsPage project={activeProject} themeMode={theme} onAction={reportAction} /> : null}
          {activePage === "errors" ? <ErrorsPage project={activeProject} /> : null}
          {activePage === "schedules" ? <SchedulesPage project={activeProject} /> : null}
          {activePage === "files" ? (
            <FilesPage
              project={activeProject}
              projects={visibleProjects}
              themeLabel={themeLabel}
              onProjectChange={switchProject}
              onToggleTheme={toggleTheme}
              onAction={reportAction}
            />
          ) : null}
          {activePage === "realtime" ? <RealtimePage project={activeProject} /> : null}
          {activePage === "settings" ? (
            <SettingsPage
              databaseMode={activeDatabaseMode}
              hideTestTenants={activeHideTestTenants}
              onDatabaseModeChange={updateProjectDatabaseMode}
              onHideTestTenantsChange={updateProjectHideTestTenants}
              onProjectNameChange={updateProjectName}
              project={activeProject}
            />
          ) : null}
        </div>
      </section>
      {actionMessage ? <div className="action-toast" role="status">{actionMessage}</div> : null}
    </main>
  );
}

function ThemeToggle(props: { className?: string; onToggle: () => void; themeLabel: string }) {
  const currentTheme = props.themeLabel === "Light mode" ? "dark" : "light";
  const label = `Switch to ${props.themeLabel.toLowerCase()}`;
  return (
    <button
      aria-label={label}
      aria-pressed={currentTheme === "dark"}
      className={`theme-toggle ${props.className ?? ""}`.trim()}
      data-current-theme={currentTheme}
      onClick={props.onToggle}
      title={label}
      type="button"
    >
      <svg aria-hidden="true" className="theme-toggle-icon theme-toggle-sun" fill="none" viewBox="0 0 24 24">
        <circle cx="12" cy="12" r="4" />
        <path d="M12 2v2M12 20v2M4.93 4.93l1.42 1.42M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.42-1.42M17.66 6.34l1.41-1.41" />
      </svg>
      <svg aria-hidden="true" className="theme-toggle-icon theme-toggle-moon" fill="none" viewBox="0 0 24 24">
        <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79Z" />
      </svg>
    </button>
  );
}

function LoginPage(props: {
  allowUnlistedEmails: boolean;
  allowedEmails: readonly string[];
  emailLoginEnabled: boolean;
  googleAuthError: string;
  googleLoginEnabled: boolean;
  onGoogleLogin: () => Promise<void>;
  onLogin: (session: DashboardSession) => void;
  onPasswordLogin: (email: string, password: string) => Promise<void>;
  onToggleTheme: () => void;
  passwordLoginEnabled: boolean;
  theme: ThemeMode;
  themeLabel: string;
}) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [authError, setAuthError] = useState("");
  const [googleLoading, setGoogleLoading] = useState(false);
  const [passwordLoading, setPasswordLoading] = useState(false);

  const signInWithGoogle = async () => {
    setAuthError("");
    setGoogleLoading(true);
    try {
      await props.onGoogleLogin();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Google sign-in failed.";
      setAuthError(message);
    } finally {
      setGoogleLoading(false);
    }
  };

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!props.emailLoginEnabled && !props.passwordLoginEnabled) return;
    const normalizedEmail = normalizeDashboardEmail(email);
    if (!normalizedEmail) return;
    if (props.passwordLoginEnabled) {
      if (!password) return;
      setAuthError("");
      setPasswordLoading(true);
      try {
        await props.onPasswordLogin(normalizedEmail, password);
      } catch (error) {
        setAuthError(error instanceof Error ? error.message : "Unable to sign in.");
      } finally {
        setPasswordLoading(false);
      }
      return;
    }
    if (!dashboardEmailAllowed(normalizedEmail, props.allowedEmails, props.allowUnlistedEmails)) {
      setAuthError("That email is not allowed for this Gonvex dashboard.");
      return;
    }
    setAuthError("");
    props.onLogin({ email: normalizedEmail, name: displayNameFromEmail(normalizedEmail), provider: "gonvex" });
  };

  return (
    <main className="login-shell" data-theme={props.theme}>
      <Card className="login-card" variant="default">
        <Card.Header className="login-card-header">
          <div className="brand-lockup login-brand" aria-label="Gonvex dashboard">
            <span className="brand-mark">G</span>
            <span className="brand-name">Gonvex</span>
          </div>
          <div>
            <Card.Title>Welcome back</Card.Title>
            <p>Sign in to continue to Gonvex.</p>
          </div>
        </Card.Header>
        <Card.Content>
          {props.googleLoginEnabled ? (
            <button className="google-login-button" disabled={googleLoading} onClick={signInWithGoogle} type="button">
              <svg aria-hidden="true" className="google-mark" viewBox="0 0 18 18">
                <path fill="#4285F4" d="M17.64 9.2c0-.64-.06-1.25-.16-1.84H9v3.48h4.84a4.14 4.14 0 0 1-1.8 2.72v2.26h2.9c1.7-1.56 2.7-3.86 2.7-6.62Z" />
                <path fill="#34A853" d="M9 18c2.43 0 4.47-.8 5.96-2.18l-2.9-2.26c-.8.54-1.84.86-3.06.86-2.35 0-4.34-1.58-5.05-3.72H.96v2.34A9 9 0 0 0 9 18Z" />
                <path fill="#FBBC05" d="M3.95 10.7a5.41 5.41 0 0 1 0-3.4V4.96H.96a9 9 0 0 0 0 8.08l2.99-2.34Z" />
                <path fill="#EA4335" d="M9 3.58c1.32 0 2.5.45 3.44 1.35l2.58-2.58A8.62 8.62 0 0 0 9 0 9 9 0 0 0 .96 4.96L3.95 7.3C4.66 5.16 6.65 3.58 9 3.58Z" />
              </svg>
              <span>{googleLoading ? "Opening Google..." : "Sign in with Google"}</span>
            </button>
          ) : null}
          {authError || props.googleAuthError ? <p className="login-error" role="alert">{authError || props.googleAuthError}</p> : null}
          {props.emailLoginEnabled || props.passwordLoginEnabled ? (
            <>
              {props.googleLoginEnabled ? <div className="login-divider"><span>or</span></div> : null}
              <form className="login-form" onSubmit={submit}>
                <label className="setting-field">
                  <span>Email</span>
                  <input
                    autoComplete="email"
                    className="table-search"
                    onChange={(event) => setEmail(event.target.value)}
                    placeholder="you@example.com"
                    type="email"
                    value={email}
                  />
                </label>
                {props.passwordLoginEnabled ? (
                  <label className="setting-field">
                    <span>Password</span>
                    <input
                      autoComplete="current-password"
                      className="table-search"
                      onChange={(event) => setPassword(event.target.value)}
                      placeholder="Dashboard password"
                      type="password"
                      value={password}
                    />
                  </label>
                ) : null}
                <div className="login-actions">
                  <Button type="submit" variant="secondary">
                    {passwordLoading ? "Signing in..." : "Continue"}
                  </Button>
                  <ThemeToggle themeLabel={props.themeLabel} onToggle={props.onToggleTheme} />
                </div>
              </form>
            </>
          ) : (
            <>
              {!props.googleLoginEnabled ? <p className="login-error" role="alert">No sign-in method is configured for this Gonvex dashboard.</p> : null}
              <div className="login-actions">
                <ThemeToggle themeLabel={props.themeLabel} onToggle={props.onToggleTheme} />
              </div>
            </>
          )}
        </Card.Content>
      </Card>
    </main>
  );
}

function ProjectsPage(props: {
  onCreateProject: (name: string, databaseMode: DatabaseMode) => Promise<CreatedProject>;
  onCreateUser: (email: string, name: string, password: string, role: DashboardUser["role"]) => Promise<DashboardUser>;
  onDeleteProject: (projectID: string) => Promise<void>;
  onLogout: () => void;
  onOpenProject: (projectID: string) => void;
  onRefreshProjects: () => Promise<void>;
  onToggleTheme: () => void;
  authEnabled: boolean;
  discoveryError: string;
  discoveryLoading: boolean;
  projects: ProjectTarget[];
  session: DashboardSession;
  theme: ThemeMode;
  themeLabel: string;
}) {
  const [name, setName] = useState("");
  const [databaseMode, setDatabaseMode] = useState<DatabaseMode>("single");
  const [createOpen, setCreateOpen] = useState(false);
  const [createError, setCreateError] = useState("");
  const [creating, setCreating] = useState(false);
  const [createdProject, setCreatedProject] = useState<CreatedProject | null>(null);
  const [deletingProjectID, setDeletingProjectID] = useState<string | null>(null);
  const [profileOpen, setProfileOpen] = useState(false);
  const [userOpen, setUserOpen] = useState(false);
  const [userEmail, setUserEmail] = useState("");
  const [userName, setUserName] = useState("");
  const [userPassword, setUserPassword] = useState("");
  const [userRole, setUserRole] = useState<DashboardUser["role"]>("user");
  const [userStatus, setUserStatus] = useState("");
  const [userSaving, setUserSaving] = useState(false);

  const closeCreateModal = () => {
    setCreateOpen(false);
    setCreateError("");
    setCreatedProject(null);
    setDatabaseMode("single");
  };

  const closeUserModal = () => {
    setUserOpen(false);
    setUserEmail("");
    setUserName("");
    setUserPassword("");
    setUserRole("user");
    setUserStatus("");
  };

  const createProject = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const cleanName = name.trim();
    if (!cleanName) return;
    setCreating(true);
    setCreateError("");
    try {
      const created = await props.onCreateProject(cleanName, databaseMode);
      setCreatedProject(created);
      setName("");
    } catch (error) {
      setCreateError(error instanceof Error ? error.message : "Could not create project");
    } finally {
      setCreating(false);
    }
  };

  const deleteProject = async (project: ProjectTarget) => {
    if (!project.runtimeCreated) return;
    setDeletingProjectID(project.id);
    try {
      await props.onDeleteProject(project.id);
    } finally {
      setDeletingProjectID(null);
    }
  };

  const createUser = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!userEmail.trim() || !userPassword) return;
    setUserSaving(true);
    setUserStatus("");
    try {
      await props.onCreateUser(userEmail, userName, userPassword, userRole);
      closeUserModal();
    } catch (error) {
      setUserStatus(error instanceof Error ? error.message : "Could not create user");
    } finally {
      setUserSaving(false);
    }
  };

  return (
    <main className="projects-shell" data-theme={props.theme}>
      <header className="projects-header">
        <div className="brand-lockup" aria-label="Gonvex dashboard">
          <span className="brand-mark">G</span>
          <span className="brand-name">Gonvex</span>
        </div>
        <div className="topbar-actions">
          <Button
            aria-label="Open account profile"
            className="projects-profile-trigger"
            onPress={() => setProfileOpen(true)}
            size="sm"
            variant="ghost"
          >
            <Avatar className="projects-profile-avatar">
              {props.session.avatarUrl ? <Avatar.Image alt="" src={props.session.avatarUrl} /> : null}
              <Avatar.Fallback>{profileInitials(props.session.name)}</Avatar.Fallback>
            </Avatar>
            <span>
              <strong>{props.session.name}</strong>
              <small>{props.session.role === "admin" ? "Administrator" : "Account"}</small>
            </span>
          </Button>
          {props.session.role === "admin" ? <Button size="sm" variant="secondary" onPress={() => setUserOpen(true)}>New user</Button> : null}
          <ThemeToggle themeLabel={props.themeLabel} onToggle={props.onToggleTheme} />
          {props.authEnabled ? <Button size="sm" variant="ghost" onPress={props.onLogout}>Sign out</Button> : null}
        </div>
      </header>

      <section className="projects-title">
        <p className="eyebrow">Projects</p>
        <h1>Choose a project</h1>
        <p className="lede">Each project can point to the same Postgres instance with a different database and its own generated code manifest.</p>
        <div className="projects-discovery">
          <Button size="sm" variant="secondary" isDisabled={props.discoveryLoading} onPress={() => { void props.onRefreshProjects(); }}>
            {props.discoveryLoading ? "Checking runtime" : "Refresh projects"}
          </Button>
          {props.discoveryError ? <p className="login-error" role="alert">Runtime projects unavailable: {props.discoveryError}</p> : null}
        </div>
      </section>

      <section className="projects-grid" aria-label="Projects">
        {props.projects.map((project) => (
          <Card className="project-tile" key={project.id} variant="default">
            <Card.Header className="project-tile-header">
              <div>
                <Card.Title>{project.name}</Card.Title>
                <p>{project.id}</p>
              </div>
              <Chip color={projectIsProvisioned(project) ? "accent" : "warning"} size="sm" variant="soft">
                {projectIsProvisioned(project) ? project.environment : "setup"}
              </Chip>
            </Card.Header>
            <Card.Content className="project-tile-content">
              <div className="project-glyph" aria-hidden="true">{projectInitial(project.name)}</div>
              <div className="project-tile-actions">
                <Button variant="primary" onPress={() => props.onOpenProject(project.id)}>
                  Open project
                </Button>
                {project.runtimeCreated ? (
                  <Button
                    isDisabled={deletingProjectID === project.id}
                    onPress={() => deleteProject(project)}
                    variant="ghost"
                  >
                    {deletingProjectID === project.id ? "Deleting" : "Delete"}
                  </Button>
                ) : null}
              </div>
            </Card.Content>
          </Card>
        ))}

        <button className="project-create-tile" onClick={() => setCreateOpen(true)} type="button">
          <span aria-hidden="true">+</span>
          <strong>Create project</strong>
        </button>
      </section>

      {profileOpen ? <AccountProfileDialog onClose={() => setProfileOpen(false)} session={props.session} /> : null}

      {userOpen ? (
        <div className="modal-backdrop" role="presentation" onMouseDown={closeUserModal}>
          <section
            aria-labelledby="create-user-title"
            className="document-modal project-create-modal"
            onMouseDown={(event) => event.stopPropagation()}
            role="dialog"
          >
            <header>
              <div>
                <h2 id="create-user-title">Create user</h2>
                <p>New users can sign in and create their own projects. They only see existing projects after an invite.</p>
              </div>
              <Button size="sm" variant="ghost" onPress={closeUserModal}>Close</Button>
            </header>
            <form className="project-modal-form" onSubmit={createUser}>
              <label className="setting-field">
                <span>Email</span>
                <input className="table-search" onChange={(event) => setUserEmail(event.target.value)} type="email" value={userEmail} />
              </label>
              <label className="setting-field">
                <span>Name</span>
                <input className="table-search" onChange={(event) => setUserName(event.target.value)} value={userName} />
              </label>
              <label className="setting-field">
                <span>Password</span>
                <input className="table-search" onChange={(event) => setUserPassword(event.target.value)} type="password" value={userPassword} />
              </label>
              <AppSelect
                ariaLabel="Dashboard role"
                className="setting-field"
                label="Dashboard role"
                selectedKey={userRole}
                onChange={(value) => setUserRole(value as DashboardUser["role"])}
                options={[
                  { value: "user", label: "User", description: "Can create own projects and access invited projects" },
                  { value: "admin", label: "Admin", description: "Can create users; project access still requires ownership or invite" },
                ]}
              />
              {userStatus ? <p className="project-modal-error">{userStatus}</p> : null}
              <footer>
                <Button variant="ghost" onPress={closeUserModal}>Cancel</Button>
                <Button isDisabled={userSaving} type="submit" variant="primary">{userSaving ? "Creating" : "Create user"}</Button>
              </footer>
            </form>
          </section>
        </div>
      ) : null}

      {createOpen ? (
        <div className="modal-backdrop" role="presentation" onMouseDown={closeCreateModal}>
          <section
            aria-labelledby="create-project-title"
            className="document-modal project-create-modal"
            onMouseDown={(event) => event.stopPropagation()}
            role="dialog"
          >
            <header>
              <div>
                <h2 id="create-project-title">{createdProject ? "Project key" : "Create project"}</h2>
                <p>{createdProject ? "Add this to the app that owns the Gonvex schema." : "Start a new local Gonvex project entry."}</p>
              </div>
              <Button size="sm" variant="ghost" onPress={closeCreateModal}>Close</Button>
            </header>
            {createdProject ? (
              <div className="project-modal-form">
                <div className="project-key-block">
                  <span>{createdProject.project.id}</span>
                  <output data-muted="true">
                    {createdProject.databaseMode === "multiTenant" ? "Landlord + tenant databases" : "Single project database"}
                  </output>
                  <textarea
                    readOnly
                    value={[
                      `GONVEX_RUNTIME_URL=${runtimeURLForProject(createdProject.project)}`,
                      `GONVEX_PROJECT_KEY=${createdProject.projectKey}`,
                      `VITE_GONVEX_WS_URL=${runtimeURLForProject(createdProject.project).replace(/^http/, "ws")}/ws`,
                    ].join("\n")}
                  />
                </div>
                <footer>
                  <Button variant="primary" onPress={closeCreateModal}>Done</Button>
                </footer>
              </div>
            ) : (
              <form className="project-modal-form" onSubmit={createProject}>
                <label className="setting-field">
                  <span>Name</span>
                  <input autoFocus className="table-search" onChange={(event) => setName(event.target.value)} value={name} />
                </label>
                <AppSelect
                  ariaLabel="Database structure"
                  className="setting-field"
                  label="Database structure"
                  selectedKey={databaseMode}
                  onChange={(value) => setDatabaseMode(value as DatabaseMode)}
                  options={[
                    { value: "single", label: "Single project database", description: "One database for project data" },
                    { value: "multiTenant", label: "Landlord + tenant databases", description: "Project DB plus tenant DBs" },
                  ]}
                />
                {createError ? <p className="project-modal-error">{createError}</p> : null}
                <footer>
                  <Button variant="ghost" onPress={closeCreateModal}>Cancel</Button>
                  <Button isDisabled={creating} type="submit" variant="primary">{creating ? "Creating" : "Create project"}</Button>
                </footer>
              </form>
            )}
          </section>
        </div>
      ) : null}
    </main>
  );
}

function profileInitials(name: string): string {
  return name.split(/\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]?.toUpperCase()).join("") || "?";
}

function tokenDateLabel(value?: string): string {
  if (!value) return "Never";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Unknown";
  return date.toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" });
}

function tokenStatus(token: AccountAccessToken): "active" | "expired" | "revoked" {
  if (token.revokedAt) return "revoked";
  if (token.expiresAt && Date.parse(token.expiresAt) <= Date.now()) return "expired";
  return "active";
}

function adminTokenExpiration(duration: string): string | undefined {
  const days = Number(duration);
  if (!Number.isFinite(days) || days <= 0) return undefined;
  return new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString();
}

function AccountProfileDialog(props: { session: DashboardSession; onClose: () => void }) {
  const isAdmin = props.session.role === "admin";
  const [tokens, setTokens] = useState<AccountAccessToken[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [tokenName, setTokenName] = useState("");
  const [duration, setDuration] = useState("90");
  const [creating, setCreating] = useState(false);
  const [revokingID, setRevokingID] = useState("");
  const [createdToken, setCreatedToken] = useState<CreatedAccountAccessToken | null>(null);
  const [copyStatus, setCopyStatus] = useState("");

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    void fetchAccountAccessTokens()
      .then((nextTokens) => {
        if (!cancelled) setTokens(nextTokens);
      })
      .catch((reason) => {
        if (!cancelled) setError(reason instanceof Error ? reason.message : "Could not load API keys");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, []);

  const createToken = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = tokenName.trim();
    if (!name || !isAdmin) return;
    setCreating(true);
    setError("");
    setCopyStatus("");
    try {
      const created = await createGlobalAdminAccessToken(name, adminTokenExpiration(duration));
      setCreatedToken(created);
      setTokens((current) => [created.token, ...current]);
      setTokenName("");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Could not create API key");
    } finally {
      setCreating(false);
    }
  };

  const revokeToken = async (tokenID: string) => {
    setRevokingID(tokenID);
    setError("");
    try {
      await revokeAccountAccessToken(tokenID);
      const revokedAt = new Date().toISOString();
      setTokens((current) => current.map((token) => token.id === tokenID ? { ...token, revokedAt } : token));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Could not revoke API key");
    } finally {
      setRevokingID("");
    }
  };

  const copyCreatedToken = async () => {
    if (!createdToken) return;
    try {
      await navigator.clipboard.writeText(createdToken.accessToken);
      setCopyStatus("Copied");
    } catch {
      setCopyStatus("Select and copy the key manually");
    }
  };

  return (
    <div className="modal-backdrop account-profile-backdrop" onMouseDown={props.onClose} role="presentation">
      <section
        aria-labelledby="account-profile-title"
        className="document-modal account-profile-modal"
        onMouseDown={(event) => event.stopPropagation()}
        role="dialog"
      >
        <header className="account-profile-header">
          <div className="account-profile-identity">
            <Avatar className="account-profile-avatar">
              {props.session.avatarUrl ? <Avatar.Image alt="" src={props.session.avatarUrl} /> : null}
              <Avatar.Fallback>{profileInitials(props.session.name)}</Avatar.Fallback>
            </Avatar>
            <div>
              <p className="eyebrow">Account control plane</p>
              <h2 id="account-profile-title">{props.session.name}</h2>
              <span>{props.session.email}</span>
            </div>
          </div>
          <Button size="sm" variant="ghost" onPress={props.onClose}>Close</Button>
        </header>

        <div className="account-profile-role-strip">
          <div>
            <span>Dashboard role</span>
            <strong>{isAdmin ? "Administrator" : "Member"}</strong>
          </div>
          <div>
            <span>Credential reach</span>
            <strong>{isAdmin ? "All projects" : "Assigned projects"}</strong>
          </div>
          <div>
            <span>Project creation</span>
            <strong>Allowed</strong>
          </div>
        </div>

        <section className="account-token-section" aria-labelledby="account-token-title">
          <div className="account-token-heading">
            <div>
              <p className="eyebrow">Automation</p>
              <h3 id="account-token-title">Admin API keys</h3>
              <p>Account-level credentials for provisioning and managing projects without a browser session.</p>
            </div>
            <span className="account-token-scope">runtime-wide</span>
          </div>

          {createdToken ? (
            <div className="account-token-reveal" role="status">
              <div>
                <strong>Copy this key now</strong>
                <span>For security, Gonvex stores only its hash and cannot show it again.</span>
              </div>
              <textarea aria-label="New admin API key" readOnly value={createdToken.accessToken} />
              <div className="account-token-reveal-actions">
                <span>{copyStatus}</span>
                <Button size="sm" variant="secondary" onPress={copyCreatedToken}>Copy key</Button>
                <Button size="sm" variant="ghost" onPress={() => setCreatedToken(null)}>I saved it</Button>
              </div>
            </div>
          ) : null}

          {isAdmin ? (
            <form className="account-token-create" onSubmit={createToken}>
              <label className="setting-field">
                <span>Key name</span>
                <input
                  aria-label="Key name"
                  className="table-search"
                  onChange={(event) => setTokenName(event.target.value)}
                  placeholder="Production provisioning"
                  value={tokenName}
                />
              </label>
              <AppSelect
                ariaLabel="Key expiration"
                className="setting-field"
                label="Expiration"
                onChange={setDuration}
                options={[
                  { value: "30", label: "30 days" },
                  { value: "90", label: "90 days" },
                  { value: "365", label: "1 year" },
                  { value: "never", label: "No expiration" },
                ]}
                selectedKey={duration}
              />
              <Button isDisabled={creating || !tokenName.trim()} type="submit" variant="primary">
                {creating ? "Creating" : "Create admin key"}
              </Button>
            </form>
          ) : (
            <p className="account-token-notice">Only dashboard administrators can issue runtime-wide API keys.</p>
          )}

          {error ? <p className="project-modal-error" role="alert">{error}</p> : null}

          <div className="account-token-list" aria-label="API keys">
            {loading ? <p className="account-token-empty">Loading API keys…</p> : null}
            {!loading && tokens.length === 0 ? (
              <p className="account-token-empty">No account API keys yet. Create one for CI, deployment tooling, or project provisioning.</p>
            ) : null}
            {tokens.map((token) => {
              const status = tokenStatus(token);
              return (
                <article className="account-token-row" data-status={status} key={token.id}>
                  <div className="account-token-mark" aria-hidden="true">⌁</div>
                  <div className="account-token-main">
                    <div>
                      <strong>{token.name}</strong>
                      <span className="account-token-state">{status}</span>
                    </div>
                    <code>{token.prefix}••••••••</code>
                    <small>
                      Created {tokenDateLabel(token.createdAt)} · Last used {token.lastUsedAt ? tokenDateLabel(token.lastUsedAt) : "never"} · Expires {tokenDateLabel(token.expiresAt)}
                    </small>
                  </div>
                  {status === "active" ? (
                    <Button isDisabled={revokingID === token.id} size="sm" variant="ghost" onPress={() => { void revokeToken(token.id); }}>
                      {revokingID === token.id ? "Revoking" : "Revoke"}
                    </Button>
                  ) : null}
                </article>
              );
            })}
          </div>
        </section>
      </section>
    </div>
  );
}

const HEALTH_COLORS = {
  calls: "#4f7cff",
  errors: "var(--danger)",
  cache: "var(--success)",
  latency: "var(--warning)",
  query: "#4f7cff",
  mutation: "var(--success)",
  action: "var(--warning)",
  lag: "var(--accent)",
  completed: "var(--success)",
  failed: "var(--danger)",
  databaseInUse: "#4f7cff",
  databaseIdle: "#8ea8d8",
  databaseWait: "var(--warning)",
};

const HEALTH_TOOLTIP_STYLE = {
  background: "var(--surface)",
  border: "1px solid var(--border)",
  borderRadius: "10px",
  fontSize: "12px",
  color: "var(--surface-foreground)",
  boxShadow: "0 10px 30px rgba(0, 0, 0, 0.18)",
};

const HEALTH_AXIS_TICK = { fontSize: 10, fill: "var(--muted)" };
const HEALTH_CHART_MARGIN = { top: 10, right: 12, left: 0, bottom: 0 };
const healthCompactNumber = new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 });

function formatCount(value: number): string {
  return healthCompactNumber.format(value);
}

function shortClockLabel(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

type OverviewCallsPoint = { label: string; calls: number; errors: number; failureRate: number; avgDurationMs: number };

function aggregateFunctionSeries(functions: Record<string, RuntimeFunctionMetrics>): OverviewCallsPoint[] {
  const series = Object.values(functions)
    .map((fn) => fn.series)
    .filter((points) => points.length > 0);
  if (series.length === 0) return [];
  const length = Math.max(...series.map((points) => points.length));
  const result: OverviewCallsPoint[] = [];
  for (let index = 0; index < length; index += 1) {
    let calls = 0;
    let errors = 0;
    let weightedDuration = 0;
    let time = "";
    for (const points of series) {
      const point = points[index];
      if (!point) continue;
      time = point.time;
      calls += point.calls;
      errors += point.errors;
      weightedDuration += point.averageDurationMs * point.calls;
    }
    result.push({
      label: shortClockLabel(time),
      calls,
      errors,
      failureRate: calls > 0 ? (errors / calls) * 100 : 0,
      avgDurationMs: calls > 0 ? weightedDuration / calls : 0,
    });
  }
  return result;
}

function HealthStat(props: { label: string; value: string; sub?: string; tone?: "default" | "danger" | "success" | "warning" }) {
  return (
    <Card className="health-stat" variant="default">
      <span className="health-stat-label">{props.label}</span>
      <strong className="health-stat-value" data-tone={props.tone ?? "default"}>{props.value}</strong>
      {props.sub ? <span className="health-stat-sub">{props.sub}</span> : null}
    </Card>
  );
}

function HealthChartCard(props: { title: string; value?: string; tone?: "default" | "danger" | "success" | "warning"; hint?: string; children: ReactNode }) {
  return (
    <Card className="health-card" variant="default">
      <div className="health-card-head">
        <span className="health-card-title">{props.title}</span>
        {props.value !== undefined ? (
          <strong className="health-card-value" data-tone={props.tone ?? "default"}>{props.value}</strong>
        ) : null}
      </div>
      <div className="health-chart">{props.children}</div>
      {props.hint ? <span className="health-card-hint">{props.hint}</span> : null}
    </Card>
  );
}

export function DatabaseHealthSection(props: { database: RuntimeDatabaseMetrics }) {
  const points = props.database.series.map((point) => ({
    ...point,
    label: shortClockLabel(point.time),
    "In use": point.inUse,
    Idle: point.idle,
    "Wait time": point.waitDurationMs,
  }));
  const hasWaits = props.database.waitCount > 0;
  const maxOpenLabel = props.database.maxOpenConnections > 0
    ? `${props.database.maxOpenConnections} max`
    : "elastic limit";

  return (
    <section className="health-section database-health" aria-label="Database load">
      <div className="health-section-head">
        <h3>Database load</h3>
        <span data-tone={hasWaits ? "warning" : "success"}>{hasWaits ? `${props.database.waitCount} waits` : "no connection waits"}</span>
      </div>

      <div className="database-pressure-strip">
        <div><span>Pools</span><strong>{props.database.pools}</strong></div>
        <div><span>Open</span><strong>{props.database.openConnections}</strong><small>{maxOpenLabel}</small></div>
        <div><span>Active</span><strong>{props.database.inUse} in use</strong></div>
        <div><span>Ready</span><strong>{props.database.idle} idle</strong></div>
        <div data-tone={hasWaits ? "warning" : undefined}><span>Pressure</span><strong>{props.database.waitCount} waits</strong><small>{formatDuration(props.database.waitDurationMs)} total</small></div>
      </div>

      {hasWaits ? (
        <p className="database-pressure-note" data-tone="warning">
          {props.database.waitCount} connections waited for a database slot. Compare these spikes with execution time to confirm pool contention.
        </p>
      ) : (
        <p className="database-pressure-note">No connection waits recorded. PostgreSQL is accepting work without queueing inside Gonvex.</p>
      )}

      <div className="health-grid">
        <HealthChartCard title="Connection occupancy" value={`${props.database.openConnections} open`} hint="physical PostgreSQL connections sampled every 2s">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={points} margin={HEALTH_CHART_MARGIN}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
              <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
              <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={28} allowDecimals={false} />
              <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} />
              <Legend wrapperStyle={{ fontSize: 11 }} iconType="plainline" />
              <Area type="stepAfter" dataKey="In use" stackId="connections" stroke={HEALTH_COLORS.databaseInUse} fill={HEALTH_COLORS.databaseInUse} fillOpacity={0.42} isAnimationActive={false} />
              <Area type="stepAfter" dataKey="Idle" stackId="connections" stroke={HEALTH_COLORS.databaseIdle} fill={HEALTH_COLORS.databaseIdle} fillOpacity={0.2} isAnimationActive={false} />
            </AreaChart>
          </ResponsiveContainer>
        </HealthChartCard>

        <HealthChartCard title="Pool wait time" value={formatDuration(props.database.waitDurationMs)} tone={hasWaits ? "warning" : "success"} hint="time spent waiting for a free application connection">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={points} margin={HEALTH_CHART_MARGIN}>
              <defs>
                <linearGradient id="databaseWaitFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={HEALTH_COLORS.databaseWait} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={HEALTH_COLORS.databaseWait} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
              <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
              <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={36} tickFormatter={(value) => `${Math.round(value)}`} />
              <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} formatter={(value) => formatDuration(Number(value))} />
              <Area type="monotone" dataKey="Wait time" stroke={HEALTH_COLORS.databaseWait} strokeWidth={2} fill="url(#databaseWaitFill)" isAnimationActive={false} />
            </AreaChart>
          </ResponsiveContainer>
        </HealthChartCard>
      </div>
    </section>
  );
}

function OverviewPage(props: { project: ProjectTarget }) {
  const { metrics, reachable } = useRuntimeMetrics(props.project);
  const [clearingCache, setClearingCache] = useState(false);
  const [cacheNotice, setCacheNotice] = useState("");

  const clearCache = async () => {
    if (!window.confirm(`Clear cached query and table results for ${props.project.name}? PostgreSQL data will not be changed.`)) return;
    setClearingCache(true);
    setCacheNotice("");
    try {
      const response = await fetch(`${runtimeURLForProject(props.project)}/dev/cache?project=${encodeURIComponent(props.project.id)}`, {
        headers: dashboardAuthHeaders(),
        method: "DELETE",
      });
      const payload = await response.json().catch(() => ({} as { cleared?: number; error?: string }));
      if (!response.ok) throw new Error(payload.error ?? response.statusText);
      setCacheNotice(`Cleared ${payload.cleared ?? 0} cached entries. Active queries will refill from PostgreSQL.`);
    } catch (error) {
      setCacheNotice(error instanceof Error ? error.message : "Could not clear the runtime cache");
    } finally {
      setClearingCache(false);
    }
  };

  const derived = useMemo(() => {
    const functions = metrics?.functions ?? {};
    const callsSeries = aggregateFunctionSeries(functions);
    let totalCalls = 0;
    let totalErrors = 0;
    let totalDuration = 0;
    for (const fn of Object.values(functions)) {
      totalCalls += fn.calls;
      totalErrors += fn.errors;
      totalDuration += fn.averageDurationMs * fn.calls;
    }
    const cacheSeries = (metrics?.cache?.series ?? []).map((point) => ({
      label: shortClockLabel(point.time),
      hitRate: point.hitRate * 100,
    }));
    const runningSeries = (metrics?.running?.series ?? []).map((point) => ({
      label: shortClockLabel(point.time),
      Queries: point.query,
      Mutations: point.mutation,
      Actions: point.action,
    }));
    const schedulerSeries = (metrics?.scheduler?.series ?? []).map((point) => ({
      label: shortClockLabel(point.time),
      lag: point.avgLagMs,
      Completed: point.completed,
      Failed: point.failed,
    }));
    const loadSeries = (metrics?.load?.series ?? []).map((point) => ({
      label: shortClockLabel(point.time),
      Connections: point.connections,
      Users: point.users,
      Subscriptions: point.subscriptions,
      cpu: point.cpuPercent,
      memory: point.memoryBytes,
    }));
    const propagationSeries = (metrics?.propagation?.series ?? []).map((point) => ({
      label: shortClockLabel(point.time),
      "Last user": point.maxMs,
      p95: point.p95Ms,
      Average: point.averageMs,
      "Server max": point.serverMaxMs,
    }));
    const propagationRecent = (metrics?.propagation?.series ?? []).filter((point) => point.samples > 0 || point.serverSamples > 0);
    const propagationNow = propagationRecent.length > 0 ? propagationRecent[propagationRecent.length - 1] : null;
    const topFunctions = Object.entries(functions)
      .map(([name, fn]) => ({ name, calls: fn.calls, errors: fn.errors }))
      .filter((fn) => fn.calls > 0)
      .sort((left, right) => right.calls - left.calls)
      .slice(0, 5);
    return {
      callsSeries,
      cacheSeries,
      runningSeries,
      schedulerSeries,
      loadSeries,
      propagationSeries,
      propagationNow,
      topFunctions,
      totalCalls,
      totalErrors,
      failureRate: totalCalls > 0 ? (totalErrors / totalCalls) * 100 : 0,
      avgDuration: totalCalls > 0 ? totalDuration / totalCalls : 0,
    };
  }, [metrics]);

  const cacheHitRate = (metrics?.cache?.hitRate ?? 0) * 100;
  const connections = metrics?.websocket?.connections ?? 0;
  const subscriptions = metrics?.websocket?.subscriptions ?? 0;
  const runningNow = metrics?.running?.total ?? 0;
  const scheduler = metrics?.scheduler ?? null;
  const lagMs = scheduler?.lagMs ?? 0;
  const queued = scheduler?.queued ?? 0;
  const crons = scheduler?.crons ?? [];
  const recentJobs = scheduler?.recent ?? [];
  const hasTraffic = derived.totalCalls > 0;
  const resources = metrics?.resources;

  const projectRows = [
    ["Runtime URL", runtimeURLForProject(props.project) || "not configured"],
    ["Database", props.project.database],
    ["Storage bucket", props.project.storageBucket],
  ];

  return (
    <div className="dashboard-layout dashboard-layout--wide">
      <section className="main-column">
        <div className="health-status-line">
          <span className={`health-pulse ${reachable ? "is-live" : "is-down"}`} aria-hidden="true" />
          <span>{reachable ? "Live · refreshing every 2s" : "Runtime unreachable — retrying"}</span>
          {!hasTraffic && reachable ? <span className="health-status-note">waiting for traffic…</span> : null}
          <Button size="sm" variant="outline" isDisabled={!reachable || clearingCache} onPress={clearCache}>
            {clearingCache ? "Clearing cache…" : "Clear cache"}
          </Button>
        </div>

        {cacheNotice ? <p className="database-pressure-note" role="status">{cacheNotice}</p> : null}

        <section className="health-stat-row" aria-label="Runtime summary">
          <HealthStat label="Function calls" value={formatCount(derived.totalCalls)} sub={`${formatCount(derived.totalErrors)} errors`} />
          <HealthStat label="Failure rate" value={`${derived.failureRate.toFixed(1)}%`} tone={derived.failureRate > 0 ? "danger" : "success"} />
          <HealthStat label="Cache hit rate" value={`${cacheHitRate.toFixed(0)}%`} tone="success" />
          <HealthStat label="Connections" value={String(connections)} sub={`${subscriptions} subscriptions`} />
          <HealthStat label="Running now" value={String(runningNow)} tone={runningNow > 0 ? "warning" : "default"} />
          <HealthStat label="Scheduler lag" value={formatDuration(lagMs)} sub={`${queued} queued`} tone={lagMs > 1000 ? "danger" : "default"} />
        </section>

        <section className="health-section" aria-label="Functions">
          <div className="health-section-head">
            <h3>Functions</h3>
            <span>last 12 min · 30s buckets</span>
          </div>
          <div className="health-grid">
            <HealthChartCard title="Function Calls" value={formatCount(derived.totalCalls)} hint="calls per 30s across all functions">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={derived.callsSeries} margin={HEALTH_CHART_MARGIN}>
                  <defs>
                    <linearGradient id="healthCallsFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={HEALTH_COLORS.calls} stopOpacity={0.35} />
                      <stop offset="100%" stopColor={HEALTH_COLORS.calls} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={30} allowDecimals={false} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} />
                  <Area type="monotone" dataKey="calls" name="Calls" stroke={HEALTH_COLORS.calls} strokeWidth={2} fill="url(#healthCallsFill)" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </HealthChartCard>

            <HealthChartCard title="Failure Rate" value={`${derived.failureRate.toFixed(1)}%`} tone={derived.failureRate > 0 ? "danger" : "default"} hint="errors ÷ calls per 30s">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={derived.callsSeries} margin={HEALTH_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={34} domain={[0, 100]} tickFormatter={(value) => `${value}%`} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} formatter={(value) => `${Number(value).toFixed(1)}%`} />
                  <Line type="monotone" dataKey="failureRate" name="Failure rate" stroke={HEALTH_COLORS.errors} strokeWidth={2} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </HealthChartCard>

            <HealthChartCard title="Cache Hit Rate" value={`${cacheHitRate.toFixed(0)}%`} tone="success" hint="Redis hits ÷ cache lookups">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={derived.cacheSeries} margin={HEALTH_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={34} domain={[0, 100]} tickFormatter={(value) => `${value}%`} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} formatter={(value) => `${Number(value).toFixed(0)}%`} />
                  <Line type="monotone" dataKey="hitRate" name="Hit rate" stroke={HEALTH_COLORS.cache} strokeWidth={2} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </HealthChartCard>

            <HealthChartCard title="Execution Time" value={formatDuration(derived.avgDuration)} tone="warning" hint="avg server duration per 30s">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={derived.callsSeries} margin={HEALTH_CHART_MARGIN}>
                  <defs>
                    <linearGradient id="healthLatencyFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={HEALTH_COLORS.latency} stopOpacity={0.3} />
                      <stop offset="100%" stopColor={HEALTH_COLORS.latency} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={36} tickFormatter={(value) => `${Math.round(value)}`} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} formatter={(value) => formatDuration(Number(value))} />
                  <Area type="monotone" dataKey="avgDurationMs" name="Avg duration" stroke={HEALTH_COLORS.latency} strokeWidth={2} fill="url(#healthLatencyFill)" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </HealthChartCard>
          </div>
        </section>

        {metrics?.database ? <DatabaseHealthSection database={metrics.database} /> : null}

        <section className="health-section" aria-label="Client resource usage">
          <div className="health-section-head">
            <h3>Resource usage per client</h3>
            <span>{connections > 0 ? `average across ${connections} active ${connections === 1 ? "client" : "clients"}` : "waiting for active clients"}</span>
          </div>
          <div className="client-resource-strip">
            <div><span>Memory / client</span><strong>{connections > 0 && resources ? formatBytes(resources.memoryPerClientBytes) : "—"}</strong><small>{resources ? `${formatBytes(resources.memoryBytes)} runtime RSS` : "runtime sample unavailable"}</small></div>
            <div><span>CPU / client</span><strong>{connections > 0 && resources ? `${resources.cpuPerClientPercent.toFixed(1)}%` : "—"}</strong><small>{resources ? `${resources.cpuPercent.toFixed(1)}% runtime process` : "runtime sample unavailable"}</small></div>
            <div><span>Bandwidth / client</span><strong>{connections > 0 && resources ? formatBytes(resources.bytesPerClient) : "—"}</strong><small>since each active connection opened</small></div>
            <div><span>Network traffic</span><strong>{resources ? formatBytes(resources.bytesReceived + resources.bytesSent) : "—"}</strong><small>{resources ? `↓ ${formatBytes(resources.bytesReceived)} · ↑ ${formatBytes(resources.bytesSent)}` : "runtime sample unavailable"}</small></div>
          </div>
          <p className="resource-usage-note">CPU and memory are runtime-process averages divided by active clients. Use them for capacity trends, not per-client billing attribution.</p>
        </section>

        <section className="health-section" aria-label="Update propagation">
          <div className="health-section-head">
            <h3>Update propagation</h3>
            <span>change committed → reflected in every subscribed browser · target {metrics?.propagation?.targetMs ?? 200}ms</span>
          </div>
          <div className="health-grid">
            <HealthChartCard
              title="Time to last user"
              value={derived.propagationNow ? formatDuration(derived.propagationNow.maxMs || derived.propagationNow.serverMaxMs) : "—"}
              tone={derived.propagationNow && (derived.propagationNow.maxMs || derived.propagationNow.serverMaxMs) > (metrics?.propagation?.targetMs ?? 200) ? "danger" : "success"}
              hint="max commit→browser-ack delay, measured on the server clock (skew-free upper bound); 'Server max' isolates backend fan-out from network/browser time"
            >
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={derived.propagationSeries} margin={HEALTH_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={44} tickFormatter={(value) => formatDuration(Number(value))} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} formatter={(value) => formatDuration(Number(value))} />
                  <Legend wrapperStyle={{ fontSize: 11 }} iconType="plainline" />
                  <ReferenceLine y={metrics?.propagation?.targetMs ?? 200} stroke="var(--success)" strokeDasharray="4 4" label={{ value: "target", fontSize: 10, fill: "var(--muted)", position: "insideTopRight" }} />
                  <Line type="monotone" dataKey="Last user" stroke={HEALTH_COLORS.errors} strokeWidth={2} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="p95" stroke={HEALTH_COLORS.latency} strokeWidth={2} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="Average" stroke={HEALTH_COLORS.query} strokeWidth={2} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="Server max" stroke={HEALTH_COLORS.mutation} strokeWidth={1.5} strokeDasharray="5 3" dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </HealthChartCard>
          </div>
          {metrics?.propagation && metrics.propagation.samples === 0 ? (
            <p className="resource-usage-note">No browser samples yet — clients must connect with <code>telemetry: true</code> to report when updates reach their GUI. The dashed server line still shows backend fan-out delay.</p>
          ) : null}
        </section>

        <section className="health-section" aria-label="Connected load">
          <div className="health-section-head">
            <h3>Connected load</h3>
            <span>last 3 h · 30s samples · all projects</span>
          </div>
          <div className="health-grid">
            <HealthChartCard title="Connections & Users" value={String(connections)} hint="live sockets and distinct users, sampled every 30s">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={derived.loadSeries} margin={HEALTH_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={30} allowDecimals={false} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} />
                  <Legend wrapperStyle={{ fontSize: 11 }} iconType="plainline" />
                  <Line type="monotone" dataKey="Connections" stroke={HEALTH_COLORS.query} strokeWidth={2} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="Users" stroke={HEALTH_COLORS.mutation} strokeWidth={2} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </HealthChartCard>

            <HealthChartCard title="Subscriptions" value={String(subscriptions)} hint="open query + sync subscriptions across all connections">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={derived.loadSeries} margin={HEALTH_CHART_MARGIN}>
                  <defs>
                    <linearGradient id="healthSubsFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={HEALTH_COLORS.calls} stopOpacity={0.35} />
                      <stop offset="100%" stopColor={HEALTH_COLORS.calls} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={40} allowDecimals={false} tickFormatter={(value) => healthCompactNumber.format(Number(value))} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} />
                  <Area type="monotone" dataKey="Subscriptions" stroke={HEALTH_COLORS.calls} strokeWidth={2} fill="url(#healthSubsFill)" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </HealthChartCard>

            <HealthChartCard title="CPU" value={resources ? `${resources.cpuPercent.toFixed(0)}%` : "—"} tone={resources && resources.cpuPercent > 100 ? "warning" : "default"} hint="runtime process CPU (100% = one core)">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={derived.loadSeries} margin={HEALTH_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={38} tickFormatter={(value) => `${Math.round(Number(value))}%`} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} formatter={(value) => `${Number(value).toFixed(1)}%`} />
                  <Line type="monotone" dataKey="cpu" name="CPU" stroke={HEALTH_COLORS.latency} strokeWidth={2} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </HealthChartCard>

            <HealthChartCard title="Memory" value={resources ? formatBytes(resources.memoryBytes) : "—"} hint="runtime resident set size">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={derived.loadSeries} margin={HEALTH_CHART_MARGIN}>
                  <defs>
                    <linearGradient id="healthMemoryFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={HEALTH_COLORS.action} stopOpacity={0.3} />
                      <stop offset="100%" stopColor={HEALTH_COLORS.action} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={48} tickFormatter={(value) => formatBytes(Number(value))} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} formatter={(value) => formatBytes(Number(value))} />
                  <Area type="monotone" dataKey="memory" name="Memory" stroke={HEALTH_COLORS.action} strokeWidth={2} fill="url(#healthMemoryFill)" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </HealthChartCard>
          </div>
        </section>

        <section className="health-section" aria-label="Concurrency and scheduler">
          <div className="health-section-head">
            <h3>Concurrency &amp; scheduler</h3>
            <span>{runningNow} running · {queued} queued</span>
          </div>
          <div className="health-grid">
            <HealthChartCard title="Running Functions" value={String(runningNow)} tone={runningNow > 0 ? "warning" : "default"} hint="peak concurrent executions by type">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={derived.runningSeries} margin={HEALTH_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={28} allowDecimals={false} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} />
                  <Legend wrapperStyle={{ fontSize: 11 }} iconType="plainline" />
                  <Line type="monotone" dataKey="Queries" stroke={HEALTH_COLORS.query} strokeWidth={2} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="Mutations" stroke={HEALTH_COLORS.mutation} strokeWidth={2} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="Actions" stroke={HEALTH_COLORS.action} strokeWidth={2} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </HealthChartCard>

            <HealthChartCard title="Scheduler Lag" value={formatDuration(lagMs)} tone={lagMs > 1000 ? "danger" : "default"} hint="delay between scheduled and actual start">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={derived.schedulerSeries} margin={HEALTH_CHART_MARGIN}>
                  <defs>
                    <linearGradient id="healthLagFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={HEALTH_COLORS.lag} stopOpacity={0.3} />
                      <stop offset="100%" stopColor={HEALTH_COLORS.lag} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={36} tickFormatter={(value) => `${Math.round(value)}`} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} formatter={(value) => formatDuration(Number(value))} />
                  <Area type="monotone" dataKey="lag" name="Lag" stroke={HEALTH_COLORS.lag} strokeWidth={2} fill="url(#healthLagFill)" isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </HealthChartCard>

            <HealthChartCard title="Job Throughput" value={`${scheduler?.completed ?? 0} done`} tone={scheduler && scheduler.failed > 0 ? "danger" : "success"} hint="completed vs failed scheduled jobs">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={derived.schedulerSeries} margin={HEALTH_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                  <XAxis dataKey="label" tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} minTickGap={32} />
                  <YAxis tick={HEALTH_AXIS_TICK} tickLine={false} axisLine={false} width={28} allowDecimals={false} />
                  <Tooltip contentStyle={HEALTH_TOOLTIP_STYLE} labelStyle={{ color: "var(--muted)" }} />
                  <Legend wrapperStyle={{ fontSize: 11 }} iconType="plainline" />
                  <Line type="monotone" dataKey="Completed" stroke={HEALTH_COLORS.completed} strokeWidth={2} dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="Failed" stroke={HEALTH_COLORS.failed} strokeWidth={2} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </HealthChartCard>
          </div>
        </section>
      </section>

      <aside className="right-rail" aria-label="Scheduler and project">
        <Card className="health-rail-card" variant="default">
          <div className="health-card-head">
            <span className="health-card-title">Scheduled jobs</span>
            <Chip color="default" size="sm" variant="secondary">{crons.length}</Chip>
          </div>
          {crons.length > 0 ? (
            <div className="health-cron-list">
              {crons.map((cron) => (
                <div className="health-cron-row" key={cron.name}>
                  <div className="health-cron-main">
                    <strong>{cron.name}</strong>
                    <code>{cron.function}</code>
                  </div>
                  <div className="health-cron-meta">
                    <span>{cron.schedule}</span>
                    <span>{cron.nextRun ? `next ${shortClockLabel(cron.nextRun)}` : "—"}</span>
                  </div>
                  <div className="health-cron-stats">
                    <span>{cron.runs} runs</span>
                    {cron.failures > 0 ? <span data-tone="danger">{cron.failures} failed</span> : null}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="empty-state">
              No crons registered. Add one with <code>app.Cron("cleanup", time.Minute, "system.cleanup", nil)</code> in your project, or schedule one-shot work with <code>ctx.Scheduler.RunAfter(...)</code>.
            </p>
          )}
        </Card>

        <Card className="health-rail-card" variant="default">
          <div className="health-card-head">
            <span className="health-card-title">Recent jobs</span>
            <Chip color="default" size="sm" variant="secondary">{recentJobs.length}</Chip>
          </div>
          {recentJobs.length > 0 ? (
            <div className="health-job-list">
              {recentJobs.slice(0, 8).map((job, index) => (
                <div className="health-job-row" key={`${job.time}:${job.function}:${index}`} data-outcome={job.outcome}>
                  <span>{shortClockLabel(job.time)}</span>
                  <code>{job.cron ? job.cron : job.function}</code>
                  <em>{job.error ? job.error : formatDuration(job.durationMs)}</em>
                </div>
              ))}
            </div>
          ) : (
            <p className="empty-state">No scheduled runs yet.</p>
          )}
        </Card>

        {derived.topFunctions.length > 0 ? (
          <Card className="health-rail-card" variant="default">
            <div className="health-card-head">
              <span className="health-card-title">Busiest functions</span>
            </div>
            <div className="health-top-list">
              {derived.topFunctions.map((fn) => (
                <div className="health-top-row" key={fn.name}>
                  <code>{fn.name}</code>
                  <span>{formatCount(fn.calls)} calls{fn.errors > 0 ? ` · ${fn.errors} err` : ""}</span>
                </div>
              ))}
            </div>
          </Card>
        ) : null}

        <ListCard title="Project target" rows={projectRows} />
      </aside>
    </div>
  );
}

function MiniChart(props: { series: number[]; tone: FunctionStat["tone"] }) {
  const values = props.series.length > 0 ? props.series : [0];
  const max = Math.max(...values, 1);
  const width = 100;
  const height = 100;
  const points = values.map((value, index) => {
    const x = values.length === 1 ? width : (index / (values.length - 1)) * width;
    const y = height - (Math.max(0, value) / max) * (height - 12) - 6;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(" ");

  return (
    <div className="mini-chart" data-tone={props.tone}>
      <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true">
        <polyline points={points} />
      </svg>
    </div>
  );
}

function FunctionsPage(props: { project: ProjectTarget; themeMode: ThemeMode; onAction: ActionHandler }) {
  const [search, setSearch] = useState("");
  const [runtimeFunctions, setRuntimeFunctions] = useState<FunctionInfo[]>([]);
  const { metrics: runtimeMetrics } = useRuntimeMetrics(props.project, projectIsProvisioned(props.project));
  const [selectedName, setSelectedName] = useState("");
  const [activeTab, setActiveTab] = useState<"statistics" | "logs">("statistics");
  const selectedFunction = runtimeFunctions.find((item) => item.name === selectedName) ?? runtimeFunctions[0] ?? null;
  const functionStats = buildFunctionStats(selectedFunction ? runtimeMetrics?.functions[selectedFunction.name] : undefined, runtimeMetrics?.cache);
  const recentLogs = (runtimeMetrics?.logs ?? []).slice(0, 20);
  const visibleFunctions = runtimeFunctions.filter((item) =>
    [item.name, item.kind, item.source].some((value) => value.toLowerCase().includes(search.toLowerCase())),
  );
  const runtimeFunctionRows = functionInfosToRows(runtimeFunctions);

  useEffect(() => {
    if (!runtimeBaseURL || !projectIsProvisioned(props.project)) {
      setRuntimeFunctions([]);
      setSelectedName("");
      return;
    }
    let cancelled = false;

    fetch(`${runtimeBaseURL}/dev/manifest`, { headers: runtimeHeaders(props.project) })
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
      .then((payload: ManifestResponse) => {
        if (cancelled) return;
        const nextFunctions = manifestFunctionsToRows(payload);
        setRuntimeFunctions(nextFunctions);
        setSelectedName((current) => (nextFunctions.some((item) => item.name === current) ? current : nextFunctions[0]?.name ?? ""));
      })
      .catch(() => {
        if (!cancelled) {
          setRuntimeFunctions([]);
          setSelectedName("");
        }
      });

    return () => {
      cancelled = true;
    };
  }, [props.project]);

  return (
    <div className="function-browser">
      <aside className="function-list-panel" aria-label="Functions">
        <div className="table-browser-heading">
          <span>Functions</span>
          <Chip color="default" size="sm" variant="secondary">
            {runtimeFunctions.length}
          </Chip>
        </div>
        <input
          className="table-search"
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search functions..."
          value={search}
          aria-label="Search functions"
        />
        <div className="function-list">
          {visibleFunctions.map((item) => (
            <Button
              key={item.name}
              className="function-button"
              data-active={item.name === selectedName ? "true" : undefined}
              onPress={() => setSelectedName(item.name)}
              variant={item.name === selectedName ? "secondary" : "ghost"}
            >
              <span className="function-kind" aria-hidden="true">
                {item.kind === "mutation" ? "fn" : "q"}
              </span>
              <span>{item.name}</span>
            </Button>
          ))}
        </div>
      </aside>

        <section className="function-detail-panel" aria-labelledby="function-detail-title">
        {selectedFunction ? (
          <header className="function-detail-header">
            <div>
              <div className="function-title-row">
                <h2 id="function-detail-title">{selectedFunction.name}</h2>
                <Chip color="warning" size="sm" variant="soft">
                  {selectedFunction.kind}
                </Chip>
              </div>
              <p>{selectedFunction.source}</p>
            </div>
            <Button size="sm" variant="primary" onPress={() => props.onAction(`${selectedFunction.name} queued for local execution MVP`)}>
              Run Function
            </Button>
          </header>
        ) : (
          <header className="function-detail-header">
            <div>
              <div className="function-title-row">
                <h2 id="function-detail-title">No functions</h2>
              </div>
              <p>This project has not pushed a function manifest yet.</p>
            </div>
          </header>
        )}

        <div className="function-tabs" role="tablist" aria-label="Function detail tabs">
          <Button size="sm" variant={activeTab === "statistics" ? "secondary" : "ghost"} onPress={() => setActiveTab("statistics")}>
            Statistics
          </Button>
          <Button size="sm" variant={activeTab === "logs" ? "secondary" : "ghost"} onPress={() => setActiveTab("logs")}>
            Logs
          </Button>
        </div>

        {activeTab === "statistics" ? (
          <div className="function-stat-grid" aria-label="Function statistics">
            {functionStats.map((stat) => (
              <Card className="function-stat-card" key={stat.label} variant="default">
                <Card.Header className="list-card-heading">
                  <Card.Title>{stat.label}</Card.Title>
                  <strong>{stat.value}</strong>
                </Card.Header>
                <Card.Content>
                  <MiniChart series={stat.series} tone={stat.tone} />
                </Card.Content>
              </Card>
            ))}
          </div>
        ) : (
          <Card className="function-log-card" variant="default">
            <Card.Content>
              {recentLogs.length > 0 ? recentLogs.map((entry, index) => (
                <div className="function-log-row" key={`${entry.time}:${entry.path}:${entry.outcome}:${index}`} data-outcome={entry.outcome}>
                  <span>{formatLogTime(entry.time)}</span>
                  <code>{entry.kind}</code>
                  <strong>{entry.outcome}</strong>
                  <code>{entry.path}</code>
                  <em>{entry.error ? entry.error : entry.cache ? `cache ${entry.cache}` : formatDuration(entry.durationMs)}</em>
                </div>
              )) : <p className="empty-state">No runtime logs yet.</p>}
            </Card.Content>
          </Card>
        )}

        <GridCard title="Registered functions" eyebrow="Generated API" chip="manifest.json">
          <ManifestGrid columns={functionColumns} rows={runtimeFunctionRows} height={260} themeMode={props.themeMode} />
        </GridCard>
      </section>
    </div>
  );
}

function DataPage(props: { databaseMode: DatabaseMode; hideTestTenants: boolean; project: ProjectTarget; themeMode: ThemeMode; onAction: ActionHandler; onTenantsDetected?: (hasTenants: boolean) => void }) {
  const [tables, setTables] = useState<DataTableInfo[]>([]);
  const [tenants, setTenants] = useState<TenantTarget[]>([]);
  const [selectedTenant, setSelectedTenant] = useState(() => dataSourceFromURL());
  const [selectedTable, setSelectedTable] = useState(() => dataStateFromURL().table);
  const [rowCache, setRowCache] = useState<Record<number, Record<string, unknown>>>({});
  const [requestedOffset, setRequestedOffset] = useState(0);
  const [visibleOffset, setVisibleOffset] = useState(0);
  const [matchingRows, setMatchingRows] = useState(0);
  const [tableSearch, setTableSearch] = useState(() => dataStateFromURL().tableSearch);
  const [rowSearchInput, setRowSearchInput] = useState(() => dataStateFromURL().rowSearch);
  const [rowSearch, setRowSearch] = useState(() => dataStateFromURL().rowSearch);
  const [rowSort, setRowSort] = useState<SortState<string> | null>(() => dataStateFromURL().sort);
  const [filters, setFilters] = useState<DataFilter[]>(() => dataStateFromURL().filters);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const [filterOpen, setFilterOpen] = useState(() => dataStateFromURL().filters.length > 0);
  const [addOpen, setAddOpen] = useState(false);
  const [addValues, setAddValues] = useState<Record<string, string>>({});
  const [addError, setAddError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [editingRowIndex, setEditingRowIndex] = useState<number | null>(null);
  const [editValues, setEditValues] = useState<Record<string, string>>({});
  const [editError, setEditError] = useState("");
  const [savingEdit, setSavingEdit] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [creatingTenant, setCreatingTenant] = useState(false);
  const [deletingTenant, setDeletingTenant] = useState(false);
  const [deleteTenantOpen, setDeleteTenantOpen] = useState(false);
  const [deleteTenantInput, setDeleteTenantInput] = useState("");
  const [erdFullscreen, setErdFullscreen] = useState(false);
  const [erdNodes, setErdNodes] = useState<Node<ERDNodeData>[]>([]);
  const [selectedERDTable, setSelectedERDTable] = useState("");
  const [viewMode, setViewMode] = useState<DataViewMode>(() => dataStateFromURL().view);
  const [selectedDataRows, setSelectedDataRows] = useState<Set<number>>(() => new Set());
  const [selectionClearKey, setSelectionClearKey] = useState(0);
  const [status, setStatus] = useState("Loading tables...");
  const [runtimeAvailable, setRuntimeAvailable] = useState(false);
  const [detectedMultiTenant, setDetectedMultiTenant] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [fetchNonce, setFetchNonce] = useState(0);
  const rowCacheRef = useRef(rowCache);
  const requestedOffsetRef = useRef(requestedOffset);
  const inFlightOffsetRef = useRef<number | null>(null);
  const fetchTimersRef = useRef<ScrollFetchTimers>({ debounceTimer: null, stopTimer: null });
  const pendingScrollRef = useRef<ScrollFetchPending>({ startRow: 0, height: 1 });
  rowCacheRef.current = rowCache;
  requestedOffsetRef.current = requestedOffset;
  const multiTenantMode = props.databaseMode === "multiTenant" || detectedMultiTenant;
  const currentTenantID = multiTenantMode && selectedTenant !== landlordDataSourceID ? selectedTenant : "";
  const activeTenant = tenants.find((tenant) => tenant.id === currentTenantID) ?? null;
  const visibleTenants = useMemo(
    () => props.hideTestTenants ? tenants.filter((tenant) => !tenantLooksInternalOrTest(tenant)) : tenants,
    [props.hideTestTenants, tenants],
  );
  const activeTenantDatabase = tenantDatabaseLabel(activeTenant, currentTenantID);
  const activeTenantDisplay = tenantDisplayLabel(activeTenant, currentTenantID);
  const activeTable = tables.find((table) => table.name === selectedTable) ?? null;
  const activeColumns = activeTable?.columns ?? emptyColumns;
  const filtersKey = JSON.stringify(filters.filter((filter) => activeColumns.includes(filter.column)));

  useEffect(() => {
    const state = dataStateFromURL();
    setSelectedTenant(props.databaseMode === "multiTenant" ? state.sourceID : landlordDataSourceID);
    setSelectedTable(state.table);
    setTableSearch(state.tableSearch);
    setRowSearchInput(state.rowSearch);
    setRowSearch(state.rowSearch);
    setRowSort(state.sort);
    setFilters(state.filters);
    setFilterOpen(state.filters.length > 0);
    setViewMode(state.view);
    setTenants([]);
    setDetectedMultiTenant(false);
  }, [props.databaseMode, props.project.id]);

  useEffect(() => {
    if (multiTenantMode) return;
    setSelectedTenant(landlordDataSourceID);
    setDataSourceInURL(landlordDataSourceID, true);
    setTenants([]);
    setSelectedTable("");
    setRowCache({});
    setRequestedOffset(0);
    setVisibleOffset(0);
  }, [multiTenantMode]);

  useEffect(() => {
    const onPopState = () => {
      const state = dataStateFromURL();
      setSelectedTenant(multiTenantMode ? state.sourceID : landlordDataSourceID);
      setSelectedTable(state.table);
      setTableSearch(state.tableSearch);
      setRowSearchInput(state.rowSearch);
      setRowSearch(state.rowSearch);
      setRowSort(state.sort);
      setFilters(state.filters);
      setFilterOpen(state.filters.length > 0);
      setViewMode(state.view);
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [multiTenantMode]);

  useEffect(() => {
    setDataStateInURL({
      filters,
      rowSearch,
      sort: rowSort,
      table: selectedTable,
      tableSearch,
      view: viewMode,
    }, true);
  }, [filters, rowSearch, rowSort, selectedTable, tableSearch, viewMode]);

  useEffect(() => {
    if (!erdFullscreen) return undefined;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setErdFullscreen(false);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [erdFullscreen]);

  useEffect(() => {
    let cancelled = false;
    const baseURL = runtimeURLForProject(props.project);
    if (!projectIsProvisioned(props.project) || !baseURL) {
      setTenants([]);
      return;
    }

    const params = new URLSearchParams({ project: props.project.id });
    fetch(`${baseURL}/dev/tenants?${params.toString()}`, { headers: runtimeHeaders(props.project) })
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
      .then((payload: { tenants?: TenantTarget[] }) => {
        if (cancelled) return;
        const nextTenants = payload.tenants ?? [];
        const selectableTenants = props.hideTestTenants ? nextTenants.filter((tenant) => !tenantLooksInternalOrTest(tenant)) : nextTenants;
        const hasTenants = nextTenants.length > 0;
        setTenants(nextTenants);
        if (hasTenants) setDetectedMultiTenant(true);
        props.onTenantsDetected?.(hasTenants);
        setSelectedTenant((current) => {
          const nextSource = current === landlordDataSourceID || selectableTenants.some((tenant) => tenant.id === current)
            ? current
            : landlordDataSourceID;
          if (nextSource !== current) setDataSourceInURL(nextSource, true);
          return nextSource;
        });
      })
      .catch(() => {
        if (!cancelled) setTenants([]);
      });

    return () => {
      cancelled = true;
    };
  }, [props.hideTestTenants, props.project]);

  useEffect(() => {
    if (!props.hideTestTenants || !currentTenantID) return;
    const selected = tenants.find((tenant) => tenant.id === currentTenantID);
    if (!selected || !tenantLooksInternalOrTest(selected)) return;
    setSelectedTenant(landlordDataSourceID);
    setDataSourceInURL(landlordDataSourceID, true);
    setSelectedTable("");
    setRowCache({});
    setRequestedOffset(0);
    setVisibleOffset(0);
  }, [currentTenantID, props.hideTestTenants, tenants]);

  useEffect(() => {
    let cancelled = false;
    const baseURL = runtimeURLForProject(props.project);
    if (!projectIsProvisioned(props.project)) {
      setTables([]);
      setSelectedTable("");
      setStatus("Project database is not configured yet");
      setRuntimeAvailable(false);
      return;
    }
    if (!baseURL) {
      setTables([]);
      setSelectedTable("");
      setStatus("Runtime offline");
      setRuntimeAvailable(false);
      return;
    }

    setStatus("Loading tables...");
    const params = new URLSearchParams();
    if (currentTenantID) params.set("tenant", currentTenantID);
    const tablesQuery = params.toString();
    const tablesURL = `${baseURL}/dev/data/tables${tablesQuery ? `?${tablesQuery}` : ""}`;
    fetch(tablesURL, { headers: runtimeHeaders(props.project, undefined, currentTenantID) })
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
      .then((payload: { tables: DataTableInfo[] }) => {
        if (cancelled) return;
        const nextTables = payload.tables ?? [];
        if (!currentTenantID && tablesLookMultiTenant(nextTables)) {
          setDetectedMultiTenant(true);
          props.onTenantsDetected?.(true);
        }
        setTables(nextTables);
        setStatus(currentTenantID ? `Viewing tenant database: ${activeTenantDisplay}` : "Viewing landlord / project database");
        setRuntimeAvailable(true);
        setSelectedTable((current) => (nextTables.some((table) => table.name === current) ? current : nextTables[0]?.name ?? ""));
      })
      .catch(() => {
        if (!cancelled) {
          setTables([]);
          setSelectedTable("");
          setStatus(currentTenantID ? `Tenant database unavailable: ${activeTenantDisplay}` : "Runtime offline");
          setRuntimeAvailable(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [activeTenantDisplay, currentTenantID, props.project, refreshKey]);

  useEffect(() => {
    setRequestedOffset(0);
    setVisibleOffset(0);
    setRowCache({});
    setEditingRowIndex(null);
    setEditValues({});
    setEditError("");
    setSelectedDataRows(new Set());
    setSelectionClearKey((key) => key + 1);
  }, [currentTenantID, filtersKey, props.project.id, rowSearch, rowSort, selectedTable]);

  useEffect(() => {
    const timeout = window.setTimeout(() => setRowSearch(rowSearchInput), 300);
    return () => window.clearTimeout(timeout);
  }, [rowSearchInput]);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    const baseURL = runtimeURLForProject(props.project);
    if (!activeTable || !projectIsProvisioned(props.project)) {
      setRowCache({});
      setMatchingRows(0);
      return;
    }
    if (!baseURL) {
      setRowCache({});
      setMatchingRows(0);
      setRuntimeAvailable(false);
      return;
    }
    const params = new URLSearchParams({
      offset: String(requestedOffset),
      limit: String(dataPageSize),
    });
    if (rowSearch.trim()) params.set("search", rowSearch.trim());
    if (rowSort) {
      params.set("sort", rowSort.key);
      params.set("direction", rowSort.direction);
    }
    if (filtersKey !== "[]") params.set("filters", filtersKey);
    if (currentTenantID) params.set("tenant", currentTenantID);

    inFlightOffsetRef.current = requestedOffset;

    fetch(`${baseURL}/dev/data/tables/${selectedTable}/rows?${params.toString()}`, {
      headers: runtimeHeaders(props.project, undefined, currentTenantID),
      signal: controller.signal,
    })
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
      .then((payload: DataRowsResponse) => {
        if (cancelled) return;
        const offset = payload.offset ?? requestedOffset;
        if (offset !== requestedOffsetRef.current) return;
        setRowCache((current) => mergeRowsIntoCache(current, payload.rows, offset));
        setMatchingRows(payload.total ?? (rowSearch.trim() || filtersKey !== "[]" ? payload.rows.length : activeTable.rowCount));
        setStatus(payload.total === undefined
          ? "Connected to Gonvex Runtime (restart needed for server sort)"
          : currentTenantID ? `Viewing tenant database: ${activeTenantDisplay}` : "Viewing landlord / project database");
        setRuntimeAvailable(true);
      })
      .catch(() => {
        if (controller.signal.aborted) return;
        if (!cancelled) {
          setRowCache({});
          setMatchingRows(0);
          setRuntimeAvailable(false);
        }
      })
      .finally(() => {
        if (!cancelled && inFlightOffsetRef.current === requestedOffset) {
          inFlightOffsetRef.current = null;
        }
      });

    return () => {
      cancelled = true;
      controller.abort();
      if (inFlightOffsetRef.current === requestedOffset) {
        inFlightOffsetRef.current = null;
      }
      clearScrollRowFetch(fetchTimersRef);
    };
  }, [activeTable, activeTenantDisplay, currentTenantID, filtersKey, props.project, refreshKey, requestedOffset, rowSearch, rowSort, fetchNonce, selectedTable]);

  useEffect(() => {
    setAddValues(Object.fromEntries(activeColumns.map((column) => [column, defaultValueForColumn(column)])));
    setAddError("");
  }, [activeColumns, selectedTable]);

  const visibleTables = tables.filter((table) => table.name.toLowerCase().includes(tableSearch.toLowerCase()));
  const dataColumns = columnsForDataTable(activeColumns).map((column) => ({
    ...column,
    width: columnWidths[String(column.id)] ?? ("width" in column ? column.width : 150),
    title: titleWithSort(column.title, rowSort, String(column.id)),
  }));
  const gridRowCount = activeTable ? (runtimeAvailable ? matchingRows : Object.keys(rowCache).length) : 0;
  const dataCellGetter = createCachedCellGetter(activeColumns, rowCache);
  const rowsSelectedForDelete = selectedRowIDs(selectedDataRows, rowCache);
  const selectedDataRowIndex = selectedDataRows.size === 1 ? selectedDataRows.values().next().value as number : null;
  const selectedDataRow = selectedDataRowIndex === null ? undefined : rowCache[selectedDataRowIndex];
  const selectedDataRowID = dataRowIdentity(selectedDataRow);
  const editingRow = editingRowIndex === null ? undefined : rowCache[editingRowIndex];
  const editingRowID = dataRowIdentity(editingRow);
  const erdGraph = useMemo(() => createERDGraph(tables), [tables]);
  const erdLayoutKey = `${erdLayoutStoragePrefix}:${props.project.id}:${currentTenantID || "landlord"}`;
  const selectedERDTableInfo = tables.find((table) => table.name === selectedERDTable) ?? null;
  const selectedERDRelations = erdGraph.edges.filter((edge) => edge.source === selectedERDTable || edge.target === selectedERDTable);
  const handleERDNodesChange = useCallback((changes: NodeChange<Node<ERDNodeData>>[]) => {
    setErdNodes((current) => {
      const next = applyNodeChanges(changes, current);
      saveERDNodePositions(erdLayoutKey, next);
      return next;
    });
  }, [erdLayoutKey]);

  useEffect(() => {
    setErdNodes(applySavedERDNodePositions(erdGraph.nodes, erdLayoutKey));
    setSelectedERDTable((current) => (current && tables.some((table) => table.name === current) ? current : ""));
  }, [erdGraph.nodes, erdLayoutKey, tables]);

  const openAddDocument = () => {
    if (!activeTable) {
      props.onAction("No runtime table is selected");
      return;
    }
    if (!projectIsProvisioned(props.project)) {
      props.onAction("Configure this project's database before inserting documents");
      return;
    }
    if (!runtimeAvailable) {
      props.onAction("Start Gonvex Runtime with `pnpm dev:runtime` before inserting documents");
      return;
    }
    setAddValues(Object.fromEntries(activeColumns.map((column) => [column, defaultValueForColumn(column)])));
    setAddError("");
    setAddOpen(true);
  };
  const openEditDocument = (rowIndex: number | null) => {
    if (rowIndex === null) {
      props.onAction("Select one loaded row to edit");
      return;
    }
    const row = rowCache[rowIndex];
    const rowID = dataRowIdentity(row);
    if (!row || !rowID) {
      props.onAction("This row has no supported _id or id value, so it cannot be edited");
      return;
    }
    setEditingRowIndex(rowIndex);
    setEditValues(Object.fromEntries(activeColumns.map((column) => [column, dataEditorInputValue(row[column])])));
    setEditError("");
  };
  const updateDocument = async () => {
    if (!activeTable || editingRowIndex === null || !editingRow || !editingRowID) {
      setEditError("The selected row is no longer loaded. Close this editor and select it again.");
      return;
    }
    if (!runtimeAvailable) {
      setEditError("Gonvex Runtime is offline. Refresh the page after it is available again.");
      return;
    }

    const patch: Record<string, unknown> = {};
    for (const column of activeColumns) {
      if (immutableDataColumns.has(column)) continue;
      try {
        const value = parseDataEditorValue(editValues[column] ?? "", editingRow[column]);
        if (!dataValuesEqual(value, editingRow[column])) patch[column] = value;
      } catch (error) {
        const message = error instanceof Error ? error.message : "Enter a valid value.";
        setEditError(`${column}: ${message}`);
        return;
      }
    }
    if (Object.keys(patch).length === 0) {
      setEditError("Change at least one editable value before saving.");
      return;
    }

    setSavingEdit(true);
    setEditError("");
    try {
      const params = new URLSearchParams();
      if (currentTenantID) params.set("tenant", currentTenantID);
      const query = params.toString();
      const response = await fetch(`${runtimeURLForProject(props.project)}/dev/data/tables/${encodeURIComponent(selectedTable)}/rows/${encodeURIComponent(editingRowID)}${query ? `?${query}` : ""}`, {
        method: "PATCH",
        headers: runtimeHeaders(props.project, { "content-type": "application/json" }, currentTenantID),
        body: JSON.stringify(patch),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({ error: response.statusText }));
        throw new Error(payload.error ?? response.statusText);
      }
      const payload = await response.json() as UpdateRowResponse;
      setRowCache((current) => ({ ...current, [editingRowIndex]: payload.row }));
      setEditingRowIndex(null);
      setEditValues({});
      setFetchNonce((key) => key + 1);
      props.onAction(`Updated ${activeTable.name} row ${editingRowID}`);
    } catch (error) {
      const message = error instanceof TypeError
        ? `Cannot reach Gonvex Runtime at ${runtimeURLForProject(props.project) || "the configured URL"}.`
        : error instanceof Error ? error.message : "Update failed";
      setEditError(message);
    } finally {
      setSavingEdit(false);
    }
  };
  const addFilter = () => {
    if (!activeTable) return;
    setFilters((current) => [
      ...current,
      { id: String(Date.now()), column: activeColumns[0] ?? "id", operator: "contains", value: "" },
    ]);
  };
  const insertDocument = async () => {
    if (!activeTable) {
      setAddError("No runtime table is selected.");
      return;
    }
    if (!projectIsProvisioned(props.project)) {
      setAddError("This project does not have a database configured yet.");
      return;
    }
    if (!runtimeAvailable) {
      setAddError("Gonvex Runtime is offline. Start it with `pnpm dev:runtime`, then refresh this page.");
      return;
    }
    setSubmitting(true);
    setAddError("");
    try {
      const params = new URLSearchParams();
      if (currentTenantID) params.set("tenant", currentTenantID);
      const query = params.toString();
      const response = await fetch(`${runtimeURLForProject(props.project)}/dev/data/tables/${selectedTable}/rows${query ? `?${query}` : ""}`, {
        method: "POST",
        headers: runtimeHeaders(props.project, { "content-type": "application/json" }, currentTenantID),
        body: JSON.stringify(addValues),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({ error: response.statusText }));
        throw new Error(payload.error ?? response.statusText);
      }
      const payload = (await response.json()) as InsertRowResponse;
      setRowCache((current) => mergeRowsIntoCache(current, [payload.row], 0));
      setTables((current) => current.map((table) => (
        table.name === activeTable.name ? { ...table, rowCount: table.rowCount + 1 } : table
      )));
      setMatchingRows((current) => current + 1);
      setAddOpen(false);
      props.onAction(`Inserted ${activeTable.name} row into the runtime database`);
    } catch (error) {
      const message = error instanceof TypeError
        ? `Cannot reach Gonvex Runtime at ${runtimeURLForProject(props.project) || "the configured URL"}. Start it with \`pnpm dev:runtime\`.`
        : error instanceof Error ? error.message : "Insert failed";
      setAddError(message);
    } finally {
      setSubmitting(false);
    }
  };
  const deleteSelectedRows = async () => {
    if (!activeTable || rowsSelectedForDelete.length === 0) {
      props.onAction("Select one or more loaded rows with an id to delete");
      return;
    }
    const confirmed = window.confirm(`Delete ${rowsSelectedForDelete.length} row${rowsSelectedForDelete.length === 1 ? "" : "s"} from ${activeTable.name}?`);
    if (!confirmed) return;

    setDeleting(true);
    try {
      const params = new URLSearchParams();
      if (currentTenantID) params.set("tenant", currentTenantID);
      const query = params.toString();
      const response = await fetch(`${runtimeURLForProject(props.project)}/dev/data/tables/${selectedTable}/rows${query ? `?${query}` : ""}`, {
        method: "DELETE",
        headers: runtimeHeaders(props.project, { "content-type": "application/json" }, currentTenantID),
        body: JSON.stringify({ ids: rowsSelectedForDelete }),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({ error: response.statusText }));
        throw new Error(payload.error ?? response.statusText);
      }
      const payload = await response.json() as DeleteRowsResponse;
      setSelectedDataRows(new Set());
      setSelectionClearKey((key) => key + 1);
      setRowCache({});
      setRequestedOffset(0);
      setVisibleOffset(0);
      setMatchingRows((current) => Math.max(0, current - payload.deleted));
      setTables((current) => current.map((table) => (
        table.name === activeTable.name ? { ...table, rowCount: Math.max(0, table.rowCount - payload.deleted) } : table
      )));
      setRefreshKey((key) => key + 1);
      props.onAction(`Deleted ${payload.deleted.toLocaleString()} ${activeTable.name} row${payload.deleted === 1 ? "" : "s"}`);
    } catch (error) {
      const message = error instanceof TypeError
        ? `Cannot reach Gonvex Runtime at ${runtimeURLForProject(props.project) || "the configured URL"}.`
        : error instanceof Error ? error.message : "Delete failed";
      props.onAction(message);
    } finally {
      setDeleting(false);
    }
  };
  const createTenantDatabase = async () => {
    const name = window.prompt("Tenant database name");
    const cleanName = name?.trim();
    if (!cleanName) return;
    const baseURL = runtimeURLForProject(props.project);
    if (!baseURL) {
      props.onAction("Runtime offline");
      return;
    }

    setCreatingTenant(true);
    try {
      const response = await fetch(`${baseURL}/dev/tenants`, {
        method: "POST",
        headers: runtimeHeaders(props.project, { "content-type": "application/json" }),
        body: JSON.stringify({ name: cleanName, projectId: props.project.id }),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({ error: response.statusText }));
        throw new Error(payload.error ?? response.statusText);
      }
      const payload = await response.json() as CreateTenantResponse;
      setTenants((current) => [...current.filter((tenant) => tenant.id !== payload.tenant.id), payload.tenant]);
      setSelectedTenant(payload.tenant.id);
      setDataSourceInURL(payload.tenant.id);
      setSelectedTable("");
      setRowCache({});
      setRequestedOffset(0);
      setVisibleOffset(0);
      setRefreshKey((key) => key + 1);
      props.onAction(`Created tenant database ${tenantDisplayLabel(payload.tenant, payload.tenant.id)}`);
    } catch (error) {
      const message = error instanceof TypeError
        ? `Cannot reach Gonvex Runtime at ${baseURL}.`
        : error instanceof Error ? error.message : "Could not create tenant database";
      props.onAction(message);
    } finally {
      setCreatingTenant(false);
    }
  };
  const deleteTenantDatabase = async () => {
    if (!currentTenantID || !activeTenant) {
      props.onAction("Select a tenant database to delete");
      return;
    }

    const baseURL = runtimeURLForProject(props.project);
    if (!baseURL) {
      props.onAction("Runtime offline");
      return;
    }

    setDeletingTenant(true);
    try {
      const params = new URLSearchParams({ project: props.project.id });
      const response = await fetch(`${baseURL}/dev/tenants/${encodeURIComponent(currentTenantID)}?${params.toString()}`, {
        method: "DELETE",
        headers: runtimeHeaders(props.project, undefined, currentTenantID),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({ error: response.statusText }));
        throw new Error(payload.error ?? response.statusText);
      }
      setTenants((current) => current.filter((tenant) => tenant.id !== currentTenantID));
      setSelectedTenant(landlordDataSourceID);
      setDataSourceInURL(landlordDataSourceID);
      setSelectedTable("");
      setRowCache({});
      setRequestedOffset(0);
      setVisibleOffset(0);
      setRefreshKey((key) => key + 1);
      setDeleteTenantOpen(false);
      setDeleteTenantInput("");
      props.onAction(`Deleted tenant database ${activeTenantDatabase}`);
    } catch (error) {
      const message = error instanceof TypeError
        ? `Cannot reach Gonvex Runtime at ${baseURL}.`
        : error instanceof Error ? error.message : "Could not delete tenant database";
      props.onAction(message);
    } finally {
      setDeletingTenant(false);
    }
  };

  return (
    <div className="data-browser">
      <aside className="table-browser" aria-label="Tables">
        <div className="table-browser-heading">
          <span>Tables</span>
          <Chip color="default" size="sm" variant="secondary">
            {tables.length}
          </Chip>
        </div>
        <input
          className="table-search"
          onChange={(event) => setTableSearch(event.target.value)}
          placeholder="Search tables..."
          value={tableSearch}
          aria-label="Search tables"
        />
        <div className="table-list">
          {visibleTables.map((table) => (
            <Button
              key={table.name}
              className="table-button"
              data-active={table.name === selectedTable ? "true" : undefined}
              onPress={() => setSelectedTable(table.name)}
              variant={table.name === selectedTable ? "secondary" : "ghost"}
            >
              <span>{table.name}</span>
              <small>{table.rowCount}</small>
            </Button>
          ))}
          {visibleTables.length === 0 ? <span className="empty-list-note">No matching tables</span> : null}
        </div>
        <Button className="schema-button" isDisabled={!activeTable} size="sm" variant="secondary" onPress={() => {
          if (activeTable) props.onAction(`${activeTable.name}: ${activeTable.columns.join(", ")}`);
        }}>
          Schema
        </Button>
      </aside>

      <section className="data-table-panel" aria-labelledby="data-table-title">
        <header className="data-table-toolbar">
          <div className="data-table-meta">
            <p className="eyebrow" title={status}>{status}</p>
            <div className="data-table-nameline">
              <h2 id="data-table-title">{activeTable?.name ?? "No tables"}</h2>
              <span className="row-count">
                {activeTable ? `${activeTable.rowCount} rows · ${activeTable.columns.length} columns` : "No schema pushed for this project"}
              </span>
            </div>
          </div>
          {multiTenantMode ? (
            <div className="data-source-group">
              <AppSelect
                ariaLabel="Active database"
                className="data-source-picker"
                searchable
                searchPlaceholder="Search databases..."
                selectedKey={selectedTenant}
                onChange={(value) => {
                  if (value === "__gonvex_no_options__") return;
                  setSelectedTenant(value);
                  setDataSourceInURL(value);
                  setSelectedTable("");
                  setRowCache({});
                  setRequestedOffset(0);
                  setVisibleOffset(0);
                  const tenant = tenants.find((item) => item.id === value) ?? null;
                  props.onAction(value === landlordDataSourceID ? "Viewing landlord / project database" : `Viewing tenant database ${tenantDisplayLabel(tenant, value)}`);
                }}
                options={[
                  { value: landlordDataSourceID, label: "Landlord" },
                  ...visibleTenants.map((tenant) => tenantDropdownOption(tenant)),
                ]}
              />
              <Button size="sm" variant="secondary" onPress={createTenantDatabase} isDisabled={creatingTenant || !runtimeAvailable}>
                {creatingTenant ? "Creating" : "+ Tenant DB"}
              </Button>
              {currentTenantID ? (
                <Button
                  className="tenant-delete-button"
                  size="sm"
                  variant="ghost"
                  onPress={() => { setDeleteTenantInput(""); setDeleteTenantOpen(true); }}
                  isDisabled={deletingTenant || !runtimeURLForProject(props.project)}
                >
                  {deletingTenant ? "Deleting…" : "Delete tenant"}
                </Button>
              ) : null}
            </div>
          ) : (
            <div className="data-source-group">
              <label className="setting-field data-source-picker">
                <span>Active database</span>
                <output>{props.project.database || "Project database"}</output>
              </label>
            </div>
          )}
          <div className="topbar-actions">
            <Button size="sm" variant="primary" onPress={openAddDocument} isDisabled={!runtimeAvailable || !activeTable}>
              + Add Document
            </Button>
          </div>
        </header>

        <div className="data-view-bar">
          <div className="data-view-tabs" role="tablist" aria-label="Data view">
            <Button size="sm" variant={viewMode === "rows" ? "secondary" : "ghost"} onPress={() => setViewMode("rows")}>
              Rows
            </Button>
            <Button size="sm" variant={viewMode === "erd" ? "secondary" : "ghost"} onPress={() => setViewMode("erd")}>
              ERD
            </Button>
          </div>
          {viewMode === "rows" ? (
            <div className="data-controls">
              <input
                className="table-search"
                onChange={(event) => setRowSearchInput(event.target.value)}
                placeholder="Search rows by substring..."
                value={rowSearchInput}
                aria-label="Search rows"
              />
              <span>{matchingRows} matching rows</span>
            </div>
          ) : null}
        </div>

        {viewMode === "rows" ? (
          <>
        {rowsSelectedForDelete.length > 0 ? (
          <div className="data-selection-bar" role="region" aria-label="Selected rows">
            <span className="data-selection-count">
              {rowsSelectedForDelete.length} row{rowsSelectedForDelete.length === 1 ? "" : "s"} selected
            </span>
            <div className="data-selection-actions">
              {selectedDataRows.size === 1 ? (
                <Button
                  size="sm"
                  variant="secondary"
                  onPress={() => openEditDocument(selectedDataRowIndex)}
                  isDisabled={!selectedDataRowID || !runtimeAvailable || !activeTable}
                >
                  Edit row
                </Button>
              ) : null}
              <Button size="sm" variant="ghost" onPress={() => { setSelectedDataRows(new Set()); setSelectionClearKey((key) => key + 1); }}>
                Clear
              </Button>
              <Button
                className="tenant-delete-button"
                size="sm"
                variant="ghost"
                onPress={deleteSelectedRows}
                isDisabled={deleting || !runtimeAvailable || !activeTable}
              >
                {deleting ? "Deleting…" : `Delete ${rowsSelectedForDelete.length} row${rowsSelectedForDelete.length === 1 ? "" : "s"}`}
              </Button>
            </div>
          </div>
        ) : null}

        <div className="data-grid-wrap">
          <ManifestGrid
            columns={dataColumns}
            getCellContent={dataCellGetter}
            height="100%"
            rowCount={gridRowCount}
            themeMode={props.themeMode}
            selectableRows
            clearSelectionKey={selectionClearKey}
            onCellActivated={([, row]) => openEditDocument(row)}
            onHeaderClick={(columnIndex) => {
              const column = activeColumns[columnIndex];
              if (column && activeTable) {
                setRowCache({});
                setRequestedOffset(0);
                setRowSort((current) => nextSort(current, column));
                props.onAction(`Sorting ${activeTable.name} by ${column}`);
              }
            }}
            onColumnResize={(column, newSize) => {
              setColumnWidths((current) => ({ ...current, [String(column.id)]: newSize }));
            }}
            onVisibleRegionChanged={(range) => {
              const startRow = Math.floor(range.y);
              const nextOffset = offsetForVisibleRange(startRow, range.height, dataRowFetchStride, dataPageSize);
              setVisibleOffset((current) => current === nextOffset ? current : nextOffset);
              scheduleScrollRowFetch(
                pendingScrollRef,
                fetchTimersRef,
                rowCacheRef,
                requestedOffsetRef,
                inFlightOffsetRef,
                setRequestedOffset,
                setFetchNonce,
                startRow,
                range.height,
                dataRowFetchStride,
                dataPageSize,
              );
            }}
            onSelectionChange={(selection) => setSelectedDataRows(new Set(selection.rows.toArray()))}
          />
          {gridRowCount === 0 ? (
            <div className="data-empty-state" role="status">
              <div className="empty-icon">▦</div>
              <strong>{activeTable ? (runtimeAvailable ? "This table is empty." : "Runtime is offline.") : "No tables yet."}</strong>
              <span>
                {!activeTable
                  ? "Connect a project and push its schema before tables appear here."
                  : runtimeAvailable
                  ? "Create a document or run a mutation to start storing data."
                  : "Start Gonvex Runtime with `pnpm dev:runtime` to read and insert real database rows."}
              </span>
              <Button size="sm" variant="primary" onPress={openAddDocument} isDisabled={!runtimeAvailable || !activeTable}>
                {runtimeAvailable ? "+ Add Document" : "Runtime Required"}
              </Button>
            </div>
          ) : null}
        </div>
          </>
        ) : (
          <div className={erdFullscreen ? "erd-panel erd-panel--fullscreen" : "erd-panel"}>
            {tables.length > 0 ? (
              <>
                <div className="erd-toolbar">
                  <div>
                    <strong>Entity diagram</strong>
                    <span>{tables.length} tables · {erdGraph.edges.length} inferred relationships</span>
                  </div>
                  <Button size="sm" variant="secondary" onPress={() => setErdFullscreen((current) => !current)}>
                    {erdFullscreen ? "Exit Fullscreen" : "Fullscreen"}
                  </Button>
                </div>
                <ReactFlow
                  key={erdFullscreen ? "erd-fullscreen" : "erd-inline"}
                  nodes={erdNodes}
                  edges={erdGraph.edges}
                  onNodesChange={handleERDNodesChange}
                  onNodeClick={(_, node) => setSelectedERDTable(node.id)}
                  onPaneClick={() => setSelectedERDTable("")}
                  fitView
                  fitViewOptions={{ padding: erdFullscreen ? 0.12 : 0.18 }}
                  minZoom={0.08}
                  maxZoom={1.8}
                  nodeDragThreshold={1}
                  nodesDraggable
                  proOptions={{ hideAttribution: true }}
                >
                  <Background gap={18} color="var(--separator)" />
                  <MiniMap pannable zoomable nodeStrokeWidth={3} />
                  <Controls showInteractive={false} />
                </ReactFlow>
                {selectedERDTableInfo ? (
                  <aside className="erd-detail-panel" aria-label={`${selectedERDTableInfo.name} table details`}>
                    <header>
                      <div>
                        <p className="eyebrow">Table</p>
                        <h3>{selectedERDTableInfo.name}</h3>
                      </div>
                      <Button size="sm" variant="ghost" onPress={() => setSelectedERDTable("")}>Close</Button>
                    </header>
                    <div className="erd-detail-stats">
                      <span>{selectedERDTableInfo.rowCount.toLocaleString()} rows</span>
                      <span>{selectedERDTableInfo.columns.length.toLocaleString()} columns</span>
                      <span>{selectedERDRelations.length.toLocaleString()} relations</span>
                    </div>
                    <section>
                      <strong>Relationships</strong>
                      {selectedERDRelations.length > 0 ? (
                        <div className="erd-relation-list">
                          {selectedERDRelations.map((edge) => (
                            <span key={edge.id}>
                              {edge.source === selectedERDTableInfo.name
                                ? `${erdEdgeColumn(edge)} -> ${edge.target}`
                                : `${edge.source} -> ${erdEdgeColumn(edge)}`}
                            </span>
                          ))}
                        </div>
                      ) : <p>No inferred relationships.</p>}
                    </section>
                    <section>
                      <strong>Columns</strong>
                      <div className="erd-detail-columns">
                        {selectedERDTableInfo.columns.map((column) => (
                          <span className={isLikelyRelationshipColumn(column) ? "erd-column erd-column--relation" : "erd-column"} key={column}>{column}</span>
                        ))}
                      </div>
                    </section>
                  </aside>
                ) : null}
              </>
            ) : (
              <div className="data-empty-state" role="status">
                <div className="empty-icon">-</div>
                <strong>No tables to diagram.</strong>
                <span>Select a tenant database or push a schema to render an ERD.</span>
              </div>
            )}
          </div>
        )}
      </section>
      {addOpen && activeTable ? (
        <div className="modal-backdrop" role="presentation" onMouseDown={() => setAddOpen(false)}>
          <section className="document-modal" role="dialog" aria-modal="true" aria-labelledby="add-document-title" onMouseDown={(event) => event.stopPropagation()}>
            <header>
              <div>
                <p className="eyebrow">Insert into {activeTable.name}</p>
                <h2 id="add-document-title">Add Document</h2>
              </div>
              <Button size="sm" variant="ghost" onPress={() => setAddOpen(false)}>Close</Button>
            </header>
            <div className="document-form">
              {activeColumns.map((column) => (
                <label key={column}>
                  <span>{column}{column === "id" ? " (auto)" : ""}</span>
                  <input
                    value={addValues[column] ?? ""}
                    onChange={(event) => setAddValues((current) => ({ ...current, [column]: event.target.value }))}
                    placeholder={column === "id" ? "Leave blank to auto-increment" : column.endsWith("_at") ? "ISO timestamp" : `Value for ${column}`}
                  />
                </label>
              ))}
            </div>
            {!runtimeAvailable ? (
              <div className="form-warning" role="status">
                Runtime is offline. This form inserts into the real database only when `pnpm dev:runtime` is running.
              </div>
            ) : null}
            {addError ? <div className="form-error" role="alert">{addError}</div> : null}
            <footer>
              <Button size="sm" variant="secondary" onPress={() => setAddOpen(false)}>Cancel</Button>
              <Button size="sm" variant="primary" onPress={insertDocument} isDisabled={submitting}>
                {submitting ? "Inserting..." : "Insert Document"}
              </Button>
            </footer>
          </section>
        </div>
      ) : null}
      {editingRowIndex !== null && activeTable && editingRow && editingRowID ? (
        <div className="modal-backdrop" role="presentation" onMouseDown={() => !savingEdit && setEditingRowIndex(null)}>
          <section className="document-modal edit-document-modal" role="dialog" aria-modal="true" aria-labelledby="edit-document-title" onMouseDown={(event) => event.stopPropagation()}>
            <header>
              <div>
                <p className="eyebrow">{activeTable.name} · {editingRowID}</p>
                <h2 id="edit-document-title">Edit document</h2>
              </div>
              <Button size="sm" variant="ghost" onPress={() => setEditingRowIndex(null)} isDisabled={savingEdit}>Close</Button>
            </header>
            <p className="edit-document-note">
              Values keep their current data type. Objects and arrays must be valid JSON.
            </p>
            <div className="document-form edit-document-form">
              {activeColumns.map((column, index) => {
                const original = editingRow[column];
                const immutable = immutableDataColumns.has(column);
                const complex = original !== null && typeof original === "object";
                const inputProps = {
                  "aria-label": `Edit ${column}`,
                  autoFocus: !immutable && activeColumns.slice(0, index).every((item) => immutableDataColumns.has(item)),
                  disabled: immutable,
                  onChange: (event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => setEditValues((current) => ({ ...current, [column]: event.target.value })),
                  value: editValues[column] ?? "",
                };
                return (
                  <label className={complex ? "edit-document-field edit-document-field--wide" : "edit-document-field"} key={column}>
                    <span className="edit-document-label">
                      <strong>{column}</strong>
                      <small>{immutable ? "identity · read only" : dataValueTypeLabel(original)}</small>
                    </span>
                    {complex ? <textarea {...inputProps} rows={5} /> : <input {...inputProps} />}
                  </label>
                );
              })}
            </div>
            {editError ? <div className="form-error" role="alert">{editError}</div> : null}
            <footer>
              <Button size="sm" variant="secondary" onPress={() => setEditingRowIndex(null)} isDisabled={savingEdit}>Cancel</Button>
              <Button size="sm" variant="primary" onPress={updateDocument} isDisabled={savingEdit}>
                {savingEdit ? "Saving changes…" : "Save changes"}
              </Button>
            </footer>
          </section>
        </div>
      ) : null}
      {deleteTenantOpen && currentTenantID ? (
        <div className="modal-backdrop" role="presentation" onMouseDown={() => !deletingTenant && setDeleteTenantOpen(false)}>
          <section className="document-modal delete-tenant-modal" role="dialog" aria-modal="true" aria-labelledby="delete-tenant-title" onMouseDown={(event) => event.stopPropagation()}>
            <header>
              <div>
                <p className="eyebrow">Destructive action</p>
                <h2 id="delete-tenant-title">Delete tenant database</h2>
              </div>
              <Button size="sm" variant="ghost" onPress={() => setDeleteTenantOpen(false)} isDisabled={deletingTenant}>Close</Button>
            </header>
            <div className="delete-tenant-body">
              <p>
                This permanently drops the tenant database <strong>{activeTenantDatabase}</strong> and removes its
                landlord references from <code>tenants</code>, <code>users</code>, and <code>userTenantMap</code>. This cannot be undone.
              </p>
              <label className="delete-tenant-field">
                <span>Type <strong>{activeTenantDatabase}</strong> to confirm</span>
                <input
                  value={deleteTenantInput}
                  onChange={(event) => setDeleteTenantInput(event.target.value)}
                  placeholder={activeTenantDatabase}
                  autoFocus
                  aria-label="Confirm tenant database name"
                />
              </label>
            </div>
            <footer>
              <Button size="sm" variant="secondary" onPress={() => setDeleteTenantOpen(false)} isDisabled={deletingTenant}>Cancel</Button>
              <Button
                className="tenant-delete-button"
                size="sm"
                variant="ghost"
                onPress={deleteTenantDatabase}
                isDisabled={deletingTenant || deleteTenantInput.trim() !== activeTenantDatabase}
              >
                {deletingTenant ? "Deleting…" : "Delete tenant"}
              </Button>
            </footer>
          </section>
        </div>
      ) : null}
    </div>
  );
}

type ERDNodeData = {
  label: ReactNode;
};

function createERDGraph(tables: DataTableInfo[]): { nodes: Node<ERDNodeData>[]; edges: Edge[] } {
  const sortedTables = [...tables].sort((left, right) => left.name.localeCompare(right.name));
  const edges = inferERDEdges(sortedTables);
  const positions = erdNodePositions(sortedTables, edges);
  const degree = erdDegreeByTable(sortedTables, edges);
  const nodes = sortedTables.map((table, index) => {
    const relations = degree.get(table.name) ?? 0;
    return {
      id: table.name,
      type: "default",
      position: positions[table.name] ?? erdGridPosition(index, sortedTables.length),
      className: relations > 0 ? "erd-table-node erd-table-node--connected" : "erd-table-node",
      width: erdNodeWidth,
      height: erdNodeHeight,
      data: {
        label: (
          <div className="erd-table-bubble">
            <strong>{table.name}</strong>
            <div>
              <span>{table.rowCount.toLocaleString()} rows</span>
              <span>{relations.toLocaleString()} links</span>
            </div>
          </div>
        ),
      },
    } satisfies Node<ERDNodeData>;
  });
  return { nodes, edges };
}

const erdNodeWidth = 178;
const erdNodeHeight = 74;

function erdDegreeByTable(tables: DataTableInfo[], edges: Edge[]): Map<string, number> {
  const degree = new Map<string, number>();
  tables.forEach((table) => degree.set(table.name, 0));
  edges.forEach((edge) => {
    degree.set(edge.source, (degree.get(edge.source) ?? 0) + 1);
    degree.set(edge.target, (degree.get(edge.target) ?? 0) + 1);
  });
  return degree;
}

function erdGridPosition(index: number, total: number): { x: number; y: number } {
  const columns = total > 20 ? 6 : total > 12 ? 5 : total > 6 ? 3 : 2;
  return {
    x: (index % columns) * 260,
    y: Math.floor(index / columns) * 180,
  };
}

function erdNodePositions(tables: DataTableInfo[], edges: Edge[]): Record<string, { x: number; y: number }> {
  if (tables.length === 0) return {};
  const degree = erdDegreeByTable(tables, edges);

  const sorted = [...tables].sort((left, right) => {
    const byDegree = (degree.get(right.name) ?? 0) - (degree.get(left.name) ?? 0);
    return byDegree !== 0 ? byDegree : left.name.localeCompare(right.name);
  });
  const centerCount = Math.min(4, Math.max(1, Math.ceil(Math.sqrt(tables.length) / 1.2)));
  const center = sorted.slice(0, centerCount);
  const rest = sorted.slice(centerCount);
  const positions: Record<string, { x: number; y: number }> = {};
  const centerSpacingX = 300;
  const centerStartX = -((center.length - 1) * centerSpacingX) / 2;
  center.forEach((table, index) => {
    positions[table.name] = { x: centerStartX + index * centerSpacingX, y: 0 };
  });

  const lanes = 4;
  const laneBuckets = Array.from({ length: lanes }, () => [] as DataTableInfo[]);
  rest.forEach((table, index) => laneBuckets[index % lanes].push(table));
  const laneConfig = [
    { x: -620, y: -280 },
    { x: 620, y: -280 },
    { x: -620, y: 260 },
    { x: 620, y: 260 },
  ];
  laneBuckets.forEach((bucket, laneIndex) => {
    const config = laneConfig[laneIndex];
    bucket.forEach((table, index) => {
      positions[table.name] = {
        x: config.x,
        y: config.y + index * 150,
      };
    });
  });
  return positions;
}

function inferERDEdges(tables: DataTableInfo[]): Edge[] {
  const tableNames = new Map(tables.map((table) => [normalizeRelationName(table.name), table.name]));
  const edges: Edge[] = [];
  const seen = new Set<string>();
  for (const table of tables) {
    for (const column of table.columns) {
      const target = relationTargetForColumn(column, tableNames);
      if (!target || target === table.name) continue;
      const id = `${table.name}:${column}:${target}`;
      if (seen.has(id)) continue;
      seen.add(id);
      edges.push({
        id,
        source: table.name,
        target,
        type: "smoothstep",
        animated: false,
        markerEnd: { type: MarkerType.ArrowClosed },
        className: "erd-edge",
        data: { column },
      });
    }
  }
  return edges;
}

function erdEdgeColumn(edge: Edge): string {
  const data = edge.data as { column?: unknown } | undefined;
  return typeof data?.column === "string" ? data.column : "id";
}

function relationTargetForColumn(column: string, tableNames: Map<string, string>): string | null {
  const normalized = normalizeRelationName(column);
  const rawBase = normalized.endsWith("ids")
    ? normalized.slice(0, -3)
    : normalized.endsWith("id")
    ? normalized.slice(0, -2)
    : "";
  const base = rawBase.replace(/[_-]+$/, "");
  if (!base || base === normalized) return null;

  const candidates = [
    base,
    `${base}s`,
    `${base}es`,
    base.endsWith("y") ? `${base.slice(0, -1)}ies` : "",
    `${base}map`,
  ].filter(Boolean);
  for (const candidate of candidates) {
    const target = tableNames.get(candidate);
    if (target) return target;
  }
  return null;
}

function normalizeRelationName(value: string): string {
  return value.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
}

function isLikelyRelationshipColumn(column: string): boolean {
  const normalized = normalizeRelationName(column);
  return normalized !== "id" && (normalized.endsWith("id") || normalized.endsWith("ids"));
}

const testTaskColumns: TestTaskColumn[] = [
  { title: "", id: "selection", width: 42, sortable: false },
  { title: "ID", id: "pg_id", width: 55, filterKind: "number" },
  { title: "", id: "flag_color", width: 50, sortable: true },
  { title: "Name", id: "name", width: 360, filterKind: "text" },
  { title: "Config", id: "config", width: 78, sortable: false },
  { title: "Form", id: "form_id", width: 72, sortable: false },
  { title: "", id: "notes", width: 60, sortable: false },
  { title: "Status", id: "status", width: 220, filterKind: "set", dataColumn: "status_name" },
  { title: "Priority", id: "priority", width: 120, filterKind: "set", dataColumn: "priority_name" },
  { title: "Assignee", id: "assignee", width: 140, filterKind: "set", dataColumn: "assignee_names" },
  { title: "Dates", id: "due_date", width: 140, filterKind: "date" },
  { title: "Location", id: "spot", width: 180, filterKind: "set", dataColumn: "spot_name" },
  { title: "Created", id: "created_at", width: 120, filterKind: "date" },
  { title: "Modified", id: "updated_at", width: 120, filterKind: "date" },
];

const testTaskDataColumns = [
  "id",
  "pg_id",
  "name",
  "title",
  "description",
  "form_id",
  "sla_id",
  "approval_id",
  "notes_count",
  "category_icon",
  "category_color",
  "category_name",
  "tag_names",
  "tag_colors",
  "attachment_count",
  "view_count",
  "status",
  "status_name",
  "status_color",
  "status_action",
  "status_icon",
  "status_working_animation",
  "status_initial",
  "priority",
  "priority_name",
  "priority_color",
  "assignee",
  "assignee_names",
  "assignee_ids",
  "assignee_avatar_urls",
  "all_user_names",
  "all_user_avatar_urls",
  "due_date",
  "due_at",
  "start_date",
  "spot_id",
  "spot_name",
  "workspace_name",
  "progress",
  "flag_color",
  "created_at",
  "updated_at",
];

const testSortColumns: Record<string, string> = {
  status: "status_name",
  priority: "priority_name",
  assignee: "assignee_names",
  spot: "spot_name",
};

function taskGridCursor(rowCache: Record<number, Record<string, unknown>>, offset: number): Pick<TestTaskGridArgs, "cursorCreatedAt" | "cursorId"> {
  if (offset <= 0) return {};
  const anchor = rowCache[offset - 1];
  if (!anchor) return {};
  const cursorId = formatCellValue(anchor.id);
  const cursorCreatedAt = formatCellValue(anchor.created_at);
  if (!cursorId || !cursorCreatedAt) return {};
  return { cursorCreatedAt, cursorId };
}

function testTaskGridArgs(
  offset: number,
  search: string,
  sort: TestSortState,
  filters: DataFilter[],
  rowCache: Record<number, Record<string, unknown>>,
): TestTaskGridArgs {
  const trimmedSearch = search.trim();
  return {
    offset,
    limit: testTaskPageSize,
    columns: testTaskDataColumns,
    count: trimmedSearch ? "false" : "estimate",
    ...taskGridCursor(rowCache, offset),
    ...(sort.direction !== "default" ? { sort: testSortColumns[sort.key] ?? sort.key, direction: sort.direction } : {}),
    ...(filters.length > 0 ? { filters } : {}),
    ...(trimmedSearch ? { search: trimmedSearch } : {}),
  };
}

function testTaskGridStateKey(search: string, sort: TestSortState, filters: DataFilter[]): string {
  return JSON.stringify({ search: search.trim(), sort, filters });
}

function taskGridSearchParams(args: TestTaskGridArgs): URLSearchParams {
  const params = new URLSearchParams({
    offset: String(args.offset),
    limit: String(args.limit),
    count: args.count,
    columns: args.columns.join(","),
  });
  if (args.search) params.set("search", args.search);
  if (args.sort) {
    params.set("sort", args.sort);
    if (args.direction) params.set("direction", args.direction);
  }
  if (args.filters?.length) params.set("filters", JSON.stringify(args.filters));
  if (args.cursorCreatedAt) params.set("cursorCreatedAt", args.cursorCreatedAt);
  if (args.cursorId) params.set("cursorId", args.cursorId);
  return params;
}

const statusColors: Record<string, string> = {
  new: "#dbeafe",
  in_progress: "#ccfbf1",
  waiting: "#fef3c7",
  review: "#ede9fe",
  done: "#dcfce7",
};

const priorityColors: Record<string, string> = {
  low: "#dbeafe",
  medium: "#fef3c7",
  high: "#fed7aa",
  urgent: "#fecaca",
};

const flagColors: Record<string, string> = {
  red: "#ef4444",
  orange: "#f97316",
  yellow: "#f59e0b",
  green: "#22c55e",
  blue: "#3b82f6",
  purple: "#8b5cf6",
};

function prettify(value: unknown): string {
  return formatCellValue(value).replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatShortDate(value: unknown): string {
  const raw = formatCellValue(value);
  if (!raw) return "No date";
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return raw;
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function formatWhagonsRelativeDate(value: unknown): { primary: string; secondary?: string; muted?: boolean; color?: string; textColor?: string } {
  const raw = formatCellValue(value);
  if (!raw) return { primary: "", muted: true };
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return { primary: raw };
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const target = new Date(date);
  target.setHours(0, 0, 0, 0);
  const days = Math.round((target.getTime() - today.getTime()) / 86_400_000);
  if (days < 0) return { primary: `${Math.abs(days)} ${Math.abs(days) === 1 ? "day" : "days"} overdue`, color: "rgba(239, 68, 68, 0.08)", textColor: "#dc2626" };
  if (days === 0) return { primary: "Today", color: "rgba(245, 158, 11, 0.10)", textColor: "#d97706" };
  return { primary: `in ${days} ${days === 1 ? "day" : "days"}`, muted: days > 3 };
}

function splitAssigneeNames(row: Record<string, unknown>): string[] {
  return formatCellValue(row.assignee_names || row.assignee)
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

function splitAssigneeAvatarUrls(row: Record<string, unknown>): string[] {
  return formatCellValue(row.assignee_avatar_urls)
    .split(",")
    .map((value) => value.trim());
}

function splitAllUserNames(row: Record<string, unknown>): string[] {
  return formatCellValue(row.all_user_names)
    .split("|")
    .map((value) => value.trim())
    .filter(Boolean);
}

function splitAllUserAvatarUrls(row: Record<string, unknown>): string[] {
  return formatCellValue(row.all_user_avatar_urls)
    .split("|")
    .map((value) => value.trim());
}

type AssigneeClickTarget = { kind: "add" } | { kind: "avatar"; index: number };

function assigneeClickTarget(row: Record<string, unknown>, localX: number, localY: number, cellHeight: number): AssigneeClickTarget | null {
  const names = splitAssigneeNames(row).slice(0, 3);
  const avatarSize = 28;
  const step = avatarSize - 4;
  const startX = 12;
  const centerY = cellHeight / 2;
  const withinCircle = (x: number) => {
    const dx = localX - (x + avatarSize / 2);
    const dy = localY - centerY;
    return Math.hypot(dx, dy) <= avatarSize / 2;
  };

  if (names.length === 0) return withinCircle(startX) ? { kind: "add" } : null;
  if ((splitAssigneeNames(row).length - names.length) === 0 && withinCircle(startX + names.length * step + 4)) {
    return { kind: "add" };
  }
  for (let index = names.length - 1; index >= 0; index -= 1) {
    if (withinCircle(startX + index * step)) return { kind: "avatar", index };
  }
  return null;
}

function createWhagonsTaskCellGetter(rowCache: Record<number, Record<string, unknown>>, hoveredRow: number | null, selectedRows: ReadonlySet<number>, selectedRowTimes: Readonly<Record<number, number>>) {
  const assigneeAvatarByName: Record<string, string> = {};
  for (const row of Object.values(rowCache)) {
    const names = [...splitAllUserNames(row), ...splitAssigneeNames(row)];
    const urls = [...splitAllUserAvatarUrls(row), ...splitAssigneeAvatarUrls(row)];
    names.forEach((name, index) => {
      if (urls[index] && !assigneeAvatarByName[name]) assigneeAvatarByName[name] = urls[index];
    });
  }
  const assigneeOptions = Array.from(new Set(Object.values(rowCache).flatMap((row) => [...splitAllUserNames(row), ...splitAssigneeNames(row)])))
    .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base" }));

  return ([column, rowIndex]: Item): GridCell => {
    const row = rowCache[rowIndex];
    const columnId = String(testTaskColumns[column]?.id ?? "");
    if (!row) return columnId === "name" ? whTaskCell({ kind: "taskName", primary: "Loading...", muted: true }) : { kind: GridCellKind.Text, allowOverlay: false, displayData: "", data: "" };
    switch (columnId) {
      case "selection":
        return whTaskCell({ kind: "selection", primary: "", hovered: hoveredRow === rowIndex, selected: selectedRows.has(rowIndex), selectedAt: selectedRowTimes[rowIndex] });
      case "pg_id":
        return { kind: GridCellKind.Text, allowOverlay: false, displayData: formatCellValue(row.pg_id || row.id), data: formatCellValue(row.pg_id || row.id) };
      case "flag_color": {
        const color = formatCellValue(row.flag_color);
        return whTaskCell({ kind: "flag", primary: color || "none", color: flagColors[color] });
      }
      case "name":
        return whTaskCell({
          kind: "taskName",
          primary: formatCellValue(row.name || row.title),
          secondary: formatCellValue(row.description),
          hovered: hoveredRow === rowIndex,
          categoryIcon: formatCellValue(row.category_icon),
          categoryColor: formatCellValue(row.category_color),
          tagNames: formatCellValue(row.tag_names).split("|").filter(Boolean),
          tagColors: formatCellValue(row.tag_colors).split("|").filter(Boolean),
          attachmentCount: Number(row.attachment_count ?? 0),
          viewCount: Number(row.view_count ?? 0),
        });
      case "config":
        return whTaskCell({ kind: "config", primary: formatCellValue(row.sla_id || row.approval_id) ? "SLA" : "" });
      case "form_id":
        return whTaskCell({ kind: "form", primary: formatCellValue(row.form_id) ? "Form" : "" });
      case "notes":
        return whTaskCell({ kind: "notes", primary: formatCellValue(row.notes_count || 0) });
      case "status": {
        const value = formatCellValue(row.status_name || row.status);
        const color = formatCellValue(row.status_color);
        return whTaskCell({
          kind: "statusPill",
          primary: value || "No Status",
          color: color || statusColors[formatCellValue(row.status)] || "#e5e7eb",
          statusAction: formatCellValue(row.status_action),
          statusIcon: formatCellValue(row.status_icon),
          workingAnimation: formatCellValue(row.status_working_animation),
          initial: row.status_initial === true || formatCellValue(row.status_initial) === "true",
        });
      }
      case "priority": {
        const value = formatCellValue(row.priority_name || row.priority);
        const color = formatCellValue(row.priority_color);
        return whTaskCell({ kind: "priorityPill", primary: value || "No Priority", color: color || priorityColors[formatCellValue(row.priority)] || "#e5e7eb" });
      }
      case "assignee":
        return whTaskCell({ kind: "assignees", primary: formatCellValue(row.assignee_names || row.assignee), hovered: hoveredRow === rowIndex, names: splitAssigneeNames(row), avatarUrls: splitAssigneeAvatarUrls(row), options: assigneeOptions, optionAvatarUrls: assigneeAvatarByName });
      case "due_date": {
        const date = formatWhagonsRelativeDate(row.due_date || row.due_at || row.start_date);
        return whTaskCell({ kind: "dateStack", primary: date.primary, secondary: date.secondary, muted: date.muted, color: date.color, textColor: date.textColor });
      }
      case "spot":
        return { kind: GridCellKind.Text, allowOverlay: false, displayData: formatCellValue(row.spot_name || row.spot_id), data: formatCellValue(row.spot_name || row.spot_id) };
      case "created_at":
      case "updated_at":
        return { kind: GridCellKind.Text, allowOverlay: false, displayData: formatShortDate(row[columnId]), data: formatShortDate(row[columnId]) };
      default:
        return { kind: GridCellKind.Text, allowOverlay: false, displayData: formatCellValue(row[columnId]), data: formatCellValue(row[columnId]) };
    }
  };
}

const testFilterOperators: Array<{ value: TestColumnFilter["operator"]; label: string }> = [
  { value: "contains", label: "Contains" },
  { value: "notContains", label: "Does not contain" },
  { value: "equals", label: "Equals" },
  { value: "notEquals", label: "Does not equal" },
  { value: "startsWith", label: "Starts with" },
  { value: "endsWith", label: "Ends with" },
  { value: "empty", label: "Is empty" },
  { value: "notEmpty", label: "Is not empty" },
];

const testComparableFilterOperators: Array<{ value: TestColumnFilter["operator"]; label: string }> = [
  { value: "equals", label: "Equals" },
  { value: "notEquals", label: "Does not equal" },
  { value: "lessThan", label: "Less than" },
  { value: "lessThanOrEqual", label: "Less than or equal" },
  { value: "greaterThan", label: "Greater than" },
  { value: "greaterThanOrEqual", label: "Greater than or equal" },
  { value: "inRange", label: "In range" },
  { value: "empty", label: "Is empty" },
  { value: "notEmpty", label: "Is not empty" },
];

function operatorsForFilterKind(kind: TestFilterKind): Array<{ value: TestColumnFilter["operator"]; label: string }> {
  return kind === "date" || kind === "number" ? testComparableFilterOperators : testFilterOperators;
}

function TestTaskFilterCard(props: {
  menu: TestFilterMenu;
  filter: TestColumnFilter;
  kind: TestFilterKind;
  values: string[];
  onChange: (filter: TestColumnFilter) => void;
  onClose: () => void;
}) {
  const width = 300;
  const left = Math.max(16, Math.min(window.innerWidth - width - 16, props.menu.bounds.x + props.menu.bounds.width - width));
  const top = Math.max(8, Math.min(window.innerHeight - 280, props.menu.bounds.y + props.menu.bounds.height + 4));
  const [miniFilter, setMiniFilter] = useState("");
  const filterValue = miniFilter.trim().toLowerCase();
  const visibleValues = props.values
    .filter((value) => !filterValue || value.toLowerCase().includes(filterValue))
    .slice(0, 40);
  const allSelected = props.filter.selectedValues.length === 0;
  const noneSelected = props.filter.selectedValues.includes(emptyFilterSelectionValue);
  const partiallySelected = !allSelected && !noneSelected;
  const isSetFilter = props.kind === "set";
  const valueDisabled = props.filter.operator === "empty" || props.filter.operator === "notEmpty";
  const operators = operatorsForFilterKind(props.kind);

  const updateOperator = (operator: TestColumnFilter["operator"]) => {
    props.onChange({ ...props.filter, operator, value: "", valueTo: undefined, selectedValues: [] });
  };

  const updateValue = (value: string, key: "value" | "valueTo" = "value") => {
    props.onChange({ ...props.filter, [key]: value, selectedValues: [] });
  };

  const toggleValue = (value: string, selected: boolean) => {
    const nextValues = noneSelected && selected
      ? [value]
      : allSelected && !selected
      ? props.values.filter((item) => item !== value)
      : selected
      ? [...props.filter.selectedValues.filter((item) => item !== emptyFilterSelectionValue), value]
      : props.filter.selectedValues.filter((item) => item !== value && item !== emptyFilterSelectionValue);
    props.onChange({ ...props.filter, selectedValues: Array.from(new Set(nextValues)) });
  };

  const toggleAllValues = () => {
    props.onChange({ ...props.filter, selectedValues: allSelected ? [emptyFilterSelectionValue] : [] });
  };

  const renderCheckbox = (label: string, isSelected: boolean, onChange: (selected: boolean) => void, isIndeterminate = false) => (
    <Checkbox.Root className="filter-checkbox" isIndeterminate={isIndeterminate} isSelected={isSelected} onChange={onChange}>
      <Checkbox.Control><Checkbox.Indicator /></Checkbox.Control>
      <Checkbox.Content>{label}</Checkbox.Content>
    </Checkbox.Root>
  );

  return (
    <div className="grid-filter-layer" onMouseDown={props.onClose} role="presentation">
      <Card className="grid-filter-card" style={{ left, top, width }} onMouseDown={(event) => event.stopPropagation()}>
        <Card.Content className="grid-filter-content">
          <Select
            aria-label="Filter condition"
            className="filter-card-select"
            fullWidth
            isDisabled={isSetFilter}
            onSelectionChange={(key) => updateOperator(String(key) as TestColumnFilter["operator"])}
            selectedKey={props.filter.operator}
          >
            <Select.Trigger>
              <Select.Value />
              <Select.Indicator />
            </Select.Trigger>
            <Select.Popover className="filter-card-select-popover">
              <ListBox>
                {operators.map((operator) => (
                  <ListBox.Item id={operator.value} key={operator.value} textValue={operator.label}>
                    <span>{operator.label}</span>
                    <ListBox.ItemIndicator />
                  </ListBox.Item>
                ))}
              </ListBox>
            </Select.Popover>
          </Select>

          {props.kind === "text" ? (
            <SearchField
              aria-label={`Filter ${props.menu.column}`}
              className="filter-card-search-field"
              fullWidth
              isDisabled={valueDisabled}
              onChange={(value) => updateValue(value)}
              value={props.filter.value}
            >
              <SearchField.Group>
                <SearchField.SearchIcon />
                <SearchField.Input autoFocus placeholder="Filter..." />
                <SearchField.ClearButton />
              </SearchField.Group>
            </SearchField>
          ) : null}

          {props.kind === "date" ? (
            <div className="filter-card-range-fields">
              <DatePicker
                aria-label={`Filter ${props.menu.column}`}
                autoFocus
                className="filter-card-date-picker"
                isDisabled={valueDisabled}
                onChange={(value) => updateValue(pickerDateString(value))}
                value={pickerDateValue(props.filter.value)}
              >
                <DateField.Group className="filter-card-date-group" fullWidth>
                  <DateField.Input>
                    {(segment) => <DateField.Segment segment={segment} />}
                  </DateField.Input>
                  <DateField.Suffix>
                    <DatePicker.Trigger>
                      <DatePicker.TriggerIndicator />
                    </DatePicker.Trigger>
                  </DateField.Suffix>
                </DateField.Group>
                <DatePicker.Popover className="filter-card-date-popover">
                  <Calendar aria-label={`Choose ${props.menu.column} date`}>
                    <Calendar.Header>
                      <Calendar.YearPickerTrigger>
                        <Calendar.YearPickerTriggerHeading />
                        <Calendar.YearPickerTriggerIndicator />
                      </Calendar.YearPickerTrigger>
                      <Calendar.NavButton slot="previous" />
                      <Calendar.NavButton slot="next" />
                    </Calendar.Header>
                    <Calendar.Grid>
                      <Calendar.GridHeader>{(day) => <Calendar.HeaderCell>{day}</Calendar.HeaderCell>}</Calendar.GridHeader>
                      <Calendar.GridBody>{(date) => <Calendar.Cell date={date} />}</Calendar.GridBody>
                    </Calendar.Grid>
                  </Calendar>
                </DatePicker.Popover>
              </DatePicker>
              {props.filter.operator === "inRange" ? (
                <DatePicker
                  aria-label={`Filter ${props.menu.column} to`}
                  className="filter-card-date-picker"
                  onChange={(value) => updateValue(pickerDateString(value), "valueTo")}
                  value={pickerDateValue(props.filter.valueTo)}
                >
                  <DateField.Group className="filter-card-date-group" fullWidth>
                    <DateField.Input>
                      {(segment) => <DateField.Segment segment={segment} />}
                    </DateField.Input>
                    <DateField.Suffix>
                      <DatePicker.Trigger>
                        <DatePicker.TriggerIndicator />
                      </DatePicker.Trigger>
                    </DateField.Suffix>
                  </DateField.Group>
                  <DatePicker.Popover className="filter-card-date-popover">
                    <Calendar aria-label={`Choose ${props.menu.column} end date`}>
                      <Calendar.Header>
                        <Calendar.YearPickerTrigger>
                          <Calendar.YearPickerTriggerHeading />
                          <Calendar.YearPickerTriggerIndicator />
                        </Calendar.YearPickerTrigger>
                        <Calendar.NavButton slot="previous" />
                        <Calendar.NavButton slot="next" />
                      </Calendar.Header>
                      <Calendar.Grid>
                        <Calendar.GridHeader>{(day) => <Calendar.HeaderCell>{day}</Calendar.HeaderCell>}</Calendar.GridHeader>
                        <Calendar.GridBody>{(date) => <Calendar.Cell date={date} />}</Calendar.GridBody>
                      </Calendar.Grid>
                    </Calendar>
                  </DatePicker.Popover>
                </DatePicker>
              ) : null}
            </div>
          ) : null}

          {props.kind === "number" ? (
            <div className="filter-card-range-fields">
              <NumberField aria-label={`Filter ${props.menu.column}`} className="filter-card-number-field" fullWidth isDisabled={valueDisabled} onChange={(value) => updateValue(Number.isFinite(value) ? String(value) : "")} value={props.filter.value === "" ? undefined : Number(props.filter.value)}>
                <NumberField.Group>
                  <NumberField.Input autoFocus placeholder="Value" />
                </NumberField.Group>
              </NumberField>
              {props.filter.operator === "inRange" ? (
                <NumberField aria-label={`Filter ${props.menu.column} to`} className="filter-card-number-field" fullWidth onChange={(value) => updateValue(Number.isFinite(value) ? String(value) : "", "valueTo")} value={!props.filter.valueTo ? undefined : Number(props.filter.valueTo)}>
                  <NumberField.Group>
                    <NumberField.Input placeholder="To" />
                  </NumberField.Group>
                </NumberField>
              ) : null}
            </div>
          ) : null}

          {isSetFilter ? (
            <>
              <SearchField aria-label={`Search ${props.menu.column} values`} className="filter-card-search-field" fullWidth onChange={setMiniFilter} value={miniFilter}>
                <SearchField.Group>
                  <SearchField.SearchIcon />
                  <SearchField.Input autoFocus placeholder="Search values..." />
                  <SearchField.ClearButton />
                </SearchField.Group>
              </SearchField>
              <div className="filter-value-list" role="group" aria-label={`${props.menu.column} values`}>
                {renderCheckbox("(Select All)", allSelected, toggleAllValues, partiallySelected)}
                {visibleValues.map((value) => (
                  <span key={value}>
                    {renderCheckbox(value || "(Blank)", !noneSelected && (allSelected || props.filter.selectedValues.includes(value)), (selected) => toggleValue(value, selected))}
                  </span>
                ))}
              </div>
            </>
          ) : null}
        </Card.Content>
      </Card>
    </div>
  );
}

function AssigneePickerCard(props: {
  menu: AssigneeMenu;
  names: string[];
  options: string[];
  avatarUrls: Record<string, string>;
  onApply: (names: string[]) => void;
  onClose: () => void;
}) {
  const width = 240;
  const left = Math.max(16, Math.min(window.innerWidth - width - 16, props.menu.bounds.x + 12));
  const top = Math.max(8, Math.min(window.innerHeight - 360, props.menu.bounds.y + 12));
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<string[]>(() => props.names);
  const options = props.options
    .filter((name) => !search.trim() || name.toLowerCase().includes(search.trim().toLowerCase()))
    .slice(0, 8);
  const toggle = (name: string, checked: boolean) => {
    setSelected((current) => checked ? Array.from(new Set([...current, name])) : current.filter((item) => item !== name));
  };

  return (
    <div className="grid-filter-layer" onMouseDown={props.onClose} role="presentation">
      <div className="assignee-editor assignee-editor--floating" style={{ left, top, width }} onMouseDown={(event) => event.stopPropagation()}>
        <SearchField aria-label="Search users" className="assignee-editor-search" fullWidth onChange={setSearch} value={search}>
          <SearchField.Group>
            <SearchField.SearchIcon />
            <SearchField.Input autoFocus placeholder="Search users..." />
            <SearchField.ClearButton />
          </SearchField.Group>
        </SearchField>
        <div className="assignee-editor-list">
          {options.map((name) => (
            <label className="assignee-editor-row" key={name}>
              <input checked={selected.includes(name)} onChange={(event) => toggle(name, event.currentTarget.checked)} type="checkbox" />
              <Avatar className="assignee-editor-avatar">
                {props.avatarUrls[name] ? <Avatar.Image alt={name} src={props.avatarUrls[name]} /> : null}
                <Avatar.Fallback>{name.split(/\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]?.toUpperCase()).join("") || "?"}</Avatar.Fallback>
              </Avatar>
              <span>{name}</span>
            </label>
          ))}
        </div>
        <div className="assignee-editor-footer">
          <span>{selected.length} selected</span>
          <button type="button" onClick={() => props.onApply(selected)}>Apply</button>
        </div>
      </div>
    </div>
  );
}

function AssigneeProfileCard(props: { menu: AssigneeProfileMenu; onClose: () => void }) {
  const width = 220;
  const left = Math.max(16, Math.min(window.innerWidth - width - 16, props.menu.bounds.x + 12));
  const top = Math.max(8, Math.min(window.innerHeight - 180, props.menu.bounds.y + 12));

  return (
    <div className="grid-filter-layer" onMouseDown={props.onClose} role="presentation">
      <div className="assignee-profile-card" style={{ left, top, width }} onMouseDown={(event) => event.stopPropagation()}>
        <Avatar className="assignee-profile-avatar">
          {props.menu.avatarUrl ? <Avatar.Image alt={props.menu.name} src={props.menu.avatarUrl} /> : null}
          <Avatar.Fallback>{props.menu.name.split(/\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]?.toUpperCase()).join("") || "?"}</Avatar.Fallback>
        </Avatar>
        <div>
          <strong>{props.menu.name}</strong>
          <span>Assigned user</span>
        </div>
      </div>
    </div>
  );
}

function TestPage(props: { project: ProjectTarget; themeMode: ThemeMode; onAction: ActionHandler }) {
  const [rowCache, setRowCache] = useState<Record<number, Record<string, unknown>>>({});
  const [visibleOffset, setVisibleOffset] = useState(0);
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<TestSortState>({ key: "name", direction: "default" });
  const [columnFilters, setColumnFilters] = useState<Record<string, TestColumnFilter>>({});
  const [filterValuesByColumn, setFilterValuesByColumn] = useState<Record<string, string[]>>({});
  const [filterMenu, setFilterMenu] = useState<TestFilterMenu | null>(null);
  const [assigneeMenu, setAssigneeMenu] = useState<AssigneeMenu | null>(null);
  const [assigneeProfileMenu, setAssigneeProfileMenu] = useState<AssigneeProfileMenu | null>(null);
  const [hoveredRow, setHoveredRow] = useState<number | null>(null);
  const [selectedRows, setSelectedRows] = useState<Set<number>>(() => new Set());
  const [selectedRowTimes, setSelectedRowTimes] = useState<Record<number, number>>({});
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const [total, setTotal] = useState(0);
  const [status, setStatus] = useState("Loading Whagons-style task rows...");
  const [queryLoading, setQueryLoading] = useState(true);
  const [randomizing, setRandomizing] = useState(false);
  const [liveOffset, setLiveOffset] = useState(0);
  const [tenants, setTenants] = useState<TenantTarget[]>([]);
  const [selectedTenant, setSelectedTenant] = useState<string>(landlordDataSourceID);
  const currentTenantID = selectedTenant !== landlordDataSourceID ? selectedTenant : "";
  const activeTenant = tenants.find((tenant) => tenant.id === currentTenantID) ?? null;
  const [client, setClient] = useState(() => projectIsProvisioned(props.project) ? gonvexClientForProject(props.project) : null);
  const viewportHeight = useViewportHeight();
  const gridHeight = Math.max(420, Math.min(620, viewportHeight - 396));
  const activeFilters = Object.entries(columnFilters)
    .map(([column, filter]) => testColumnFilterToDataFilter(column, filter))
    .filter((filter): filter is DataFilter => filter !== null);
  const activeFiltersKey = JSON.stringify(activeFilters);
  const activeFilterColumnIds = Object.entries(columnFilters)
    .filter(([column, filter]) => isTestColumnFilterActive(filter, testColumnFilterKind(column)))
    .map(([column]) => column);
  const filterColumnValues = filterMenu ? filterValuesByColumn[filterMenu.column] ?? [] : [];
  const assigneeAvatarByName: Record<string, string> = {};
  for (const row of Object.values(rowCache)) {
    const names = [...splitAllUserNames(row), ...splitAssigneeNames(row)];
    const urls = [...splitAllUserAvatarUrls(row), ...splitAssigneeAvatarUrls(row)];
    names.forEach((name, index) => {
      if (urls[index] && !assigneeAvatarByName[name]) assigneeAvatarByName[name] = urls[index];
    });
  }
  const assigneeOptions = Array.from(new Set(Object.values(rowCache).flatMap((row) => [...splitAllUserNames(row), ...splitAssigneeNames(row)])))
    .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base" }));
  const sortKeyRef = useRef(`${sort.key}:${sort.direction}`);
  const rowCacheRef = useRef(rowCache);
  const liveOffsetRef = useRef(liveOffset);
  const fetchTimersRef = useRef<ScrollFetchTimers>({ debounceTimer: null, stopTimer: null });
  const pendingScrollRef = useRef<ScrollFetchPending>({ startRow: 0, height: 1 });
  const gridStateKey = testTaskGridStateKey(search, sort, activeFilters);
  const gridStateKeyRef = useRef(gridStateKey);

  rowCacheRef.current = rowCache;
  liveOffsetRef.current = liveOffset;

  useEffect(() => {
    gridStateKeyRef.current = gridStateKey;
  }, [gridStateKey]);

  useEffect(() => {
    return () => clearScrollRowFetch(fetchTimersRef);
  }, []);

  useEffect(() => {
    const timeout = window.setTimeout(() => setSearch(searchInput), 300);
    return () => window.clearTimeout(timeout);
  }, [searchInput]);

  useEffect(() => {
    setSelectedTenant(landlordDataSourceID);
    let cancelled = false;
    const baseURL = runtimeURLForProject(props.project);
    if (!projectIsProvisioned(props.project) || !baseURL) {
      setTenants([]);
      return;
    }
    const params = new URLSearchParams({ project: props.project.id });
    fetch(`${baseURL}/dev/tenants?${params.toString()}`, { headers: runtimeHeaders(props.project) })
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
      .then((payload: { tenants?: TenantTarget[] }) => {
        if (cancelled) return;
        const nextTenants = payload.tenants ?? [];
        setTenants(nextTenants);
        // Default to the first real tenant so the lab shows live data instead of
        // the typically empty landlord database.
        const firstReal = nextTenants.find((tenant) => !tenantLooksInternalOrTest(tenant));
        if (firstReal) setSelectedTenant(firstReal.id);
      })
      .catch(() => {
        if (!cancelled) setTenants([]);
      });
    return () => {
      cancelled = true;
    };
  }, [props.project.id]);

  useEffect(() => {
    if (!projectIsProvisioned(props.project)) {
      setClient(null);
      setTotal(0);
      setQueryLoading(false);
      setStatus("Project database is not configured yet");
      return;
    }
    setClient(gonvexClientForProject(props.project, currentTenantID));
    setQueryLoading(true);
    const target = currentTenantID ? tenantDisplayLabel(activeTenant, currentTenantID) : props.project.name;
    setStatus(`Loading ${target} task rows...`);
  }, [props.project.id, props.project.name, currentTenantID]);

  useEffect(() => {
    clearScrollRowFetch(fetchTimersRef);
    setLiveOffset(0);
    setVisibleOffset(0);
    setRowCache({});
    setTotal(0);
    setQueryLoading(true);
  }, [props.project.id, currentTenantID, search, activeFiltersKey]);

  useEffect(() => {
    const nextSortKey = `${sort.key}:${sort.direction}`;
    if (sortKeyRef.current === nextSortKey) return;
    sortKeyRef.current = nextSortKey;
    clearScrollRowFetch(fetchTimersRef);
    setRowCache({});
    setVisibleOffset(0);
    setLiveOffset(0);
    setQueryLoading(true);
  }, [sort]);

  useEffect(() => {
    if (!client) return;
    let cancelled = false;
    client.connect();
    const subscribedOffset = liveOffset;
    const subscribedStateKey = gridStateKeyRef.current;
    const args = testTaskGridArgs(subscribedOffset, search, sort, activeFilters, rowCacheRef.current);
    if (Object.keys(rowCacheRef.current).length === 0) setQueryLoading(true);
    const unsubscribe = client.subscribeQuery(api["tasks.grid"], args as unknown as JsonValue, (message) => {
      if (cancelled) return;
      if (message.type === "query.error") {
        setQueryLoading(false);
        setStatus(message.error);
        return;
      }
      if (message.type !== "query.result") return;
      const payload = message.result as DataRowsResponse;
      const offset = payload.offset ?? subscribedOffset;
      if (offset !== subscribedOffset) return;
      if (liveOffsetRef.current !== subscribedOffset) return;
      if (gridStateKeyRef.current !== subscribedStateKey) return;
      setRowCache((current) => mergeRowsIntoCache(current, payload.rows, offset));
      setTotal(payload.total ?? payload.rows.length);
      setQueryLoading(false);
      setStatus(message.reason === "invalidate" ? "Live · updated" : "Live via Gonvex binding");
    });
    return () => {
      cancelled = true;
      unsubscribe();
    };
  }, [activeFiltersKey, client, gridStateKey, liveOffset, search, sort]);

  const updateColumnFilter = (column: string, filter: TestColumnFilter) => {
    setColumnFilters((current) => ({ ...current, [column]: filter }));
  };

  const randomizeVisibleTaskFields = async () => {
    if (!client || randomizing) return;
    setRandomizing(true);
    setStatus("Randomizing 3k task statuses/priorities...");
    try {
      const result = await client.mutation<RandomizeTasksResponse>(
        api["tasks.randomizeStatusPriority"],
        { count: 3000 } as unknown as JsonValue,
      );
      setStatus(`Randomized ${result.updated.toLocaleString()} tasks in ${result.durationMs.toLocaleString()}ms`);
      props.onAction(`Randomized ${result.updated.toLocaleString()} task rows`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Randomize mutation failed");
    } finally {
      setRandomizing(false);
    }
  };

  const showInitialQueryLoading = queryLoading && Object.keys(rowCache).length === 0;

  return (
    <section className="test-table-shell">
      <header className="test-table-toolbar">
        <div>
          <p className="eyebrow">{status}</p>
          <h2>Task rendering lab</h2>
          <span>{total} matching rows · Glide custom renderers · Whagons-like fields</span>
        </div>
        <div className="test-table-actions">
          <AppSelect
            ariaLabel="Active database"
            className="test-table-tenant"
            label="Database"
            searchable
            searchPlaceholder="Search databases..."
            selectedKey={selectedTenant}
            onChange={(value) => {
              setSelectedTenant(value);
              const tenant = tenants.find((item) => item.id === value) ?? null;
              props.onAction(value === landlordDataSourceID ? "Viewing landlord tasks" : `Viewing ${tenantDisplayLabel(tenant, value)} tasks`);
            }}
            options={[
              { value: landlordDataSourceID, label: "Landlord" },
              ...tenants.map((tenant) => tenantDropdownOption(tenant)),
            ]}
          />
          <Button size="sm" variant="secondary" onPress={randomizeVisibleTaskFields} isDisabled={randomizing}>
            {randomizing ? "Randomizing..." : "Randomize 3k"}
          </Button>
          <input
            className="table-search test-table-search"
            onChange={(event) => setSearchInput(event.target.value)}
            placeholder="Search task name/description..."
            value={searchInput}
            aria-label="Search test tasks"
          />
        </div>
      </header>
      <ManifestGrid
        key={gridStateKey}
        activeFilterColumnIds={activeFilterColumnIds}
        columns={testTaskColumns.map((column) => ({
          ...column,
          hasMenu: Boolean(column.filterKind),
          title: titleWithTestSort(column.title, sort, String(column.id)),
          width: columnWidths[String(column.id)] ?? ("width" in column ? column.width : 150),
        }))}
        customRenderers={[whTaskRenderer]}
        clearSelection={sort.direction === "default"}
        disableSelection
        noGridLines
        zebraRows
        hideRowMarkers
        hoveredRow={hoveredRow}
        selectedRows={selectedRows}
        overlay={showInitialQueryLoading ? (
          <div className="test-grid-loading">
            <span className="grid-loading-spinner" />
            <strong>Loading matching tasks...</strong>
            <span>{search.trim() ? `Searching "${search.trim()}"` : "Fetching task rows"}</span>
          </div>
        ) : null}
        getCellContent={createWhagonsTaskCellGetter(rowCache, hoveredRow, selectedRows, selectedRowTimes)}
        height="100%"
        rowCount={total}
        rowHeight={64}
        themeMode={props.themeMode}
        onCellClick={(cell, event) => {
          const columnId = String(testTaskColumns[cell[0]]?.id ?? "");
          if (columnId === "selection") {
            event.preventDefault();
            setSelectedRows((current) => {
              const next = new Set(current);
              if (next.has(cell[1])) {
                next.delete(cell[1]);
                setSelectedRowTimes((times) => {
                  const { [cell[1]]: _removed, ...rest } = times;
                  return rest;
                });
              } else {
                next.add(cell[1]);
                setSelectedRowTimes((times) => ({ ...times, [cell[1]]: Date.now() }));
              }
              return next;
            });
            setAssigneeMenu(null);
            setAssigneeProfileMenu(null);
            setFilterMenu(null);
            return;
          }
          if (columnId === "assignee") {
            event.preventDefault();
            setFilterMenu(null);
            const row = rowCache[cell[1]] ?? {};
            const bounds = {
              x: event.bounds.x + event.localEventX,
              y: event.bounds.y + event.localEventY,
              width: 0,
              height: 0,
            };
            const target = assigneeClickTarget(row, event.localEventX, event.localEventY, event.bounds.height);
            if (target?.kind === "add") {
              setAssigneeProfileMenu(null);
              setAssigneeMenu({ rowIndex: cell[1], bounds });
            } else if (target?.kind === "avatar") {
              const names = splitAssigneeNames(row);
              const avatarUrls = splitAssigneeAvatarUrls(row);
              setAssigneeMenu(null);
              setAssigneeProfileMenu({ name: names[target.index] ?? "User", avatarUrl: avatarUrls[target.index], bounds });
            } else {
              setAssigneeMenu(null);
              setAssigneeProfileMenu(null);
            }
          } else {
            setAssigneeMenu(null);
            setAssigneeProfileMenu(null);
          }
        }}
        onItemHovered={(event) => {
          const nextRow = event.kind === "cell" ? event.location[1] : null;
          setHoveredRow((current) => current === nextRow ? current : nextRow);
        }}
        onCellEdited={(cell, newValue) => {
          const columnId = String(testTaskColumns[cell[0]]?.id ?? "");
          if (columnId !== "assignee" || newValue.kind !== GridCellKind.Custom) return;
          const data = newValue.data as WhTaskCellData;
          setRowCache((current) => ({
            ...current,
            [cell[1]]: {
              ...current[cell[1]],
              assignee_names: (data.names ?? []).join(", "),
              assignee_avatar_urls: (data.avatarUrls ?? []).join(","),
            },
          }));
        }}
        onHeaderClick={(columnIndex) => {
          const columnConfig = testTaskColumns[columnIndex];
          const column = String(columnConfig?.id ?? "");
          if (!column || columnConfig?.sortable === false) return;
          setSort((current) => nextTestSort(current, column));
          props.onAction(`Test table sorting by ${column}`);
        }}
        onHeaderMenuClick={(columnIndex, bounds) => {
          const columnConfig = testTaskColumns[columnIndex];
          const column = String(columnConfig?.id ?? "");
          if (!column || !columnConfig?.filterKind) return;
          const values = sortedUniqueColumnValues(rowCache, column);
          if (values.length > 0) {
            setFilterValuesByColumn((current) => ({
              ...current,
              [column]: mergeSortedValues(current[column] ?? [], values),
            }));
          }
          setAssigneeMenu(null);
          setAssigneeProfileMenu(null);
          setFilterMenu({ columnIndex, column, bounds });
          props.onAction(`Opening ${column} filter`);
        }}
        onColumnResize={(column, newSize) => {
          setColumnWidths((current) => ({ ...current, [String(column.id)]: newSize }));
        }}
        onVisibleRegionChanged={(range) => {
          const startRow = Math.floor(range.y);
          const nextOffset = offsetForVisibleRange(startRow, range.height, testRowFetchStride, testTaskPageSize);
          setVisibleOffset((current) => current === nextOffset ? current : nextOffset);
          scheduleScrollLiveQuery(
            pendingScrollRef,
            fetchTimersRef,
            liveOffsetRef,
            setLiveOffset,
            startRow,
            range.height,
            testTaskPageSize,
          );
        }}
      />
      {filterMenu ? (
        <TestTaskFilterCard
          menu={filterMenu}
          filter={columnFilters[filterMenu.column] ?? emptyTestColumnFilter(testColumnFilterKind(filterMenu.column))}
          kind={testColumnFilterKind(filterMenu.column)}
          values={filterColumnValues}
          onChange={(filter) => updateColumnFilter(filterMenu.column, filter)}
          onClose={() => setFilterMenu(null)}
        />
      ) : null}
      {assigneeMenu ? (
        <AssigneePickerCard
          menu={assigneeMenu}
          names={splitAssigneeNames(rowCache[assigneeMenu.rowIndex] ?? {})}
          options={assigneeOptions}
          avatarUrls={assigneeAvatarByName}
          onApply={(names) => {
            setRowCache((current) => ({
              ...current,
              [assigneeMenu.rowIndex]: {
                ...current[assigneeMenu.rowIndex],
                assignee_names: names.join(", "),
                assignee_avatar_urls: names.map((name) => assigneeAvatarByName[name] ?? "").join(","),
              },
            }));
            setAssigneeMenu(null);
          }}
          onClose={() => setAssigneeMenu(null)}
        />
      ) : null}
      {assigneeProfileMenu ? (
        <AssigneeProfileCard menu={assigneeProfileMenu} onClose={() => setAssigneeProfileMenu(null)} />
      ) : null}
    </section>
  );
}

function FaIcon(props: { icon: IconDefinition; className?: string }) {
  const [width, height, , , pathData] = props.icon.icon;
  const path = Array.isArray(pathData) ? pathData[pathData.length - 1] : pathData;
  return (
    <svg
      className={props.className}
      viewBox={`0 0 ${width} ${height}`}
      width="1em"
      height="1em"
      fill="currentColor"
      aria-hidden="true"
      focusable="false"
    >
      <path d={path} />
    </svg>
  );
}

function FilesPage(props: {
  project: ProjectTarget;
  projects: ProjectTarget[];
  themeLabel: string;
  onProjectChange: (projectID: string) => void;
  onToggleTheme: () => void;
  onAction: ActionHandler;
}) {
  const [search, setSearch] = useState("");
  const [runtimeFiles, setRuntimeFiles] = useState<FileInfo[]>([]);
  const [localFiles, setLocalFiles] = useState<FileInfo[]>([]);
  const [status, setStatus] = useState("Loading runtime files...");
  const [fileSort, setFileSort] = useState<SortState<FileSortKey> | null>(null);
  const files = [...localFiles, ...runtimeFiles];
  const visibleFiles = sortFiles(files.filter((file) =>
    [file.id, file.contentType].some((value) => value.toLowerCase().includes(search.toLowerCase())),
  ), fileSort);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set());
  const selectedVisible = visibleFiles.filter((file) => selectedIds.has(file.id));
  const allVisibleSelected = visibleFiles.length > 0 && selectedVisible.length === visibleFiles.length;
  const someVisibleSelected = selectedVisible.length > 0 && !allVisibleSelected;

  const toggleFileSelected = (id: string) => setSelectedIds((current) => {
    const next = new Set(current);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    return next;
  });
  const toggleAllVisible = () => setSelectedIds((current) => {
    const next = new Set(current);
    if (visibleFiles.every((file) => next.has(file.id))) {
      visibleFiles.forEach((file) => next.delete(file.id));
    } else {
      visibleFiles.forEach((file) => next.add(file.id));
    }
    return next;
  });

  useEffect(() => {
    let cancelled = false;
    const baseURL = runtimeURLForProject(props.project);
    if (!baseURL) {
      setRuntimeFiles([]);
      setStatus("Runtime offline. Select files to preview local uploads.");
      return;
    }

    fetch(`${baseURL}/dev/storage/files`, { headers: runtimeHeaders(props.project) })
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
      .then((payload: StorageFilesResponse) => {
        if (cancelled) return;
        const rows = (payload.files ?? []).map(fileFromStorageObject);
        setRuntimeFiles(rows);
        if (!payload.configured) {
          setStatus("Object storage is not configured for this runtime.");
        } else if (rows.length > 0) {
          setStatus(`Connected — ${rows.length} file${rows.length === 1 ? "" : "s"} in ${payload.bucket ?? "storage"}`);
        } else {
          setStatus("Connected — no files in storage yet");
        }
      })
      .catch(() => {
        if (!cancelled) {
          setRuntimeFiles([]);
          setStatus("Could not load files — runtime unreachable.");
        }
      });

    return () => {
      cancelled = true;
    };
  }, [props.project]);

  const uploadLocalFiles = (fileList: FileList | null) => {
    const selected = Array.from(fileList ?? []);
    if (selected.length === 0) return;
    const uploadedAt = new Date().toLocaleString();
    const previews = selected.map((file) => ({
      id: `local:${file.name}`,
      size: formatBytes(file.size),
      contentType: file.type || "application/octet-stream",
      uploadedAt,
      objectUrl: URL.createObjectURL(file),
      source: "local" as const,
    }));
    setLocalFiles((current) => [...previews, ...current]);
    props.onAction(`Added ${selected.length} local file preview${selected.length === 1 ? "" : "s"}`);
  };

  const downloadFile = (file: FileInfo) => {
    if (!file.objectUrl) {
      props.onAction("Runtime download URLs are not implemented yet");
      return;
    }
    const link = document.createElement("a");
    link.href = file.objectUrl;
    link.download = file.id.replace(/^local:/, "");
    link.click();
  };

  const deleteFile = (file: FileInfo) => {
    if (file.objectUrl) URL.revokeObjectURL(file.objectUrl);
    if (file.source === "local") {
      setLocalFiles((current) => current.filter((item) => item.id !== file.id));
      props.onAction(`Removed ${file.id} from local previews`);
      return;
    }
    props.onAction("Runtime file deletion is not implemented yet");
  };

  const renderHeader = (key: FileSortKey, label: string) => (
    <button className="file-sort-button" onClick={() => setFileSort((current) => nextSort(current, key))}>
      {label}
      <span>{fileSort?.key === key ? (fileSort.direction === "asc" ? "↑" : "↓") : ""}</span>
    </button>
  );

  return (
    <section
      className="file-storage-page"
      aria-labelledby="file-storage-title"
      onDragOver={(event) => event.preventDefault()}
      onDrop={(event) => {
        event.preventDefault();
        uploadLocalFiles(event.dataTransfer.files);
      }}
    >
      <header className="file-storage-header">
        <div>
          <div className="file-storage-title-row">
            <h1 id="file-storage-title">File Storage</h1>
            <AppSelect
              ariaLabel="Project"
              className="project-select"
              selectedKey={props.project.id}
              onChange={props.onProjectChange}
              options={props.projects.map((project) => ({
                value: project.id,
                label: project.name,
                description: project.environment,
              }))}
            />
          </div>
          <p>Total Files {files.length}</p>
          <p>{status}</p>
        </div>
        <ThemeToggle className="file-theme-toggle" themeLabel={props.themeLabel} onToggle={() => {
          props.onToggleTheme();
          props.onAction(`Switched to ${props.themeLabel.toLowerCase().replace(" mode", "")}`);
        }} />
      </header>

      <div className="file-storage-toolbar">
        <div className="file-filter-group">
          <label className="file-search">
            <span aria-hidden="true">ID</span>
            <input
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Lookup by ID"
              value={search}
              aria-label="Lookup by ID"
            />
          </label>
          <Button
            className="uploaded-filter"
            size="sm"
            variant="secondary"
            onPress={() => setFileSort((current) => nextSort(current, "uploadedAt"))}
          >
            Uploaded at: <span>Any time</span>
          </Button>
        </div>

        <div className="upload-actions">
          <label className="upload-label">
            <input type="file" multiple onChange={(event) => uploadLocalFiles(event.target.files)} />
            Upload Files
          </label>
          <span>or drag files here</span>
        </div>
      </div>

      {selectedVisible.length > 0 ? (
        <div className="file-selection-bar">
          <span>{selectedVisible.length} selected</span>
          <div className="file-selection-actions">
            <Button size="sm" variant="secondary" onPress={() => selectedVisible.forEach((file) => downloadFile(file))}>Download</Button>
            <Button size="sm" variant="ghost" onPress={() => setSelectedIds(new Set())}>Clear</Button>
          </div>
        </div>
      ) : null}

      <div className="file-table-shell" role="table" aria-label="Stored files">
        <div className="file-grid-row file-grid-head" role="row">
          <div className="check-cell" role="columnheader">
            <input
              type="checkbox"
              className="file-checkbox"
              aria-label="Select all files"
              checked={allVisibleSelected}
              ref={(el) => { if (el) el.indeterminate = someVisibleSelected; }}
              onChange={toggleAllVisible}
            />
          </div>
          <div role="columnheader">{renderHeader("id", "ID")} <span className="help-dot">?</span></div>
          <div role="columnheader">{renderHeader("size", "Size")}</div>
          <div role="columnheader">{renderHeader("contentType", "Content type")}</div>
          <div role="columnheader">{renderHeader("uploadedAt", "Uploaded at")}</div>
          <div className="action-cell" role="columnheader"><Button size="sm" variant="secondary" aria-label="Column settings" onPress={() => props.onAction("All file columns are visible")}>Cols</Button></div>
        </div>
        {visibleFiles.map((file) => (
          <div className="file-grid-row" role="row" key={file.id} data-selected={selectedIds.has(file.id) ? "true" : undefined}>
            <div className="check-cell" role="cell">
              <input
                type="checkbox"
                className="file-checkbox"
                aria-label={`Select ${file.id}`}
                checked={selectedIds.has(file.id)}
                onChange={() => toggleFileSelected(file.id)}
              />
            </div>
            <div className="file-id-cell" role="cell"><code>{file.id}</code><button className="file-icon-button" aria-label={`Copy ${file.id}`} title="Copy ID" onClick={() => {
              void navigator.clipboard?.writeText(file.id);
              props.onAction(`Copied ${file.id}`);
            }}><FaIcon icon={faCopy} /></button></div>
            <div role="cell">{file.size}</div>
            <div role="cell">{file.contentType}</div>
            <div role="cell">{file.uploadedAt}</div>
            <div className="file-actions" role="cell">
              <button className="file-icon-button" aria-label={`Download ${file.id}`} title="Download" onClick={() => downloadFile(file)}><FaIcon icon={faDownload} /></button>
              <button className="file-icon-button file-icon-button--danger" aria-label={`Delete ${file.id}`} title="Delete" onClick={() => deleteFile(file)}><FaIcon icon={faTrash} /></button>
            </div>
          </div>
        ))}
        {visibleFiles.length === 0 ? <div className="file-empty-state">No files yet. Upload a file locally or start the runtime with rows in the files table.</div> : null}
      </div>
    </section>
  );
}

type LogBadgeCellData = {
  kind: "logBadge";
  label: string;
  tone: "neutral" | "query" | "mutation" | "action" | "success" | "error";
};

const logBadgeRenderer: CustomRenderer = {
  kind: GridCellKind.Custom,
  isMatch: (cell): cell is CustomCell<LogBadgeCellData> => (cell.data as Partial<LogBadgeCellData> | undefined)?.kind === "logBadge",
  draw: ({ ctx, rect, theme }, cell) => {
    const data = cell.data as LogBadgeCellData;
    const palette: Record<LogBadgeCellData["tone"], { background: string; foreground: string }> = {
      neutral: { background: "rgba(120, 120, 120, 0.13)", foreground: theme.textMedium },
      query: { background: "rgba(59, 130, 246, 0.15)", foreground: "#3b82f6" },
      mutation: { background: "rgba(168, 85, 247, 0.15)", foreground: "#a855f7" },
      action: { background: "rgba(234, 179, 8, 0.16)", foreground: "#b58100" },
      success: { background: "rgba(34, 197, 94, 0.15)", foreground: "#219653" },
      error: { background: "rgba(239, 68, 68, 0.16)", foreground: "#e5484d" },
    };
    const colors = palette[data.tone];
    ctx.save();
    ctx.font = `600 11px ${theme.fontFamily}`;
    const label = data.label || "unknown";
    const width = Math.min(rect.width - 14, Math.max(42, ctx.measureText(label).width + 18));
    const height = 20;
    const x = rect.x + 7;
    const y = rect.y + Math.round((rect.height - height) / 2);
    drawRoundRect(ctx, x, y, width, height, 10);
    ctx.fillStyle = colors.background;
    ctx.fill();
    ctx.fillStyle = colors.foreground;
    ctx.textBaseline = "middle";
    ctx.fillText(canvasEllipsize(ctx, label, width - 14), x + 9, y + height / 2, width - 14);
    ctx.restore();
    return true;
  },
};

function logBadgeCell(label: string, tone: LogBadgeCellData["tone"]): CustomCell<LogBadgeCellData> {
  return {
    kind: GridCellKind.Custom,
    allowOverlay: false,
    copyData: label,
    data: { kind: "logBadge", label, tone },
  };
}

function runtimeLogStart(entry: RuntimeLogEntry): string {
  if (entry.startedAt) return entry.startedAt;
  const completed = new Date(entry.completedAt ?? entry.time).getTime();
  return Number.isFinite(completed) ? new Date(completed - entry.durationMs).toISOString() : entry.time;
}

function runtimeLogRequestText(entry: RuntimeLogEntry): string {
  if (entry.request === undefined || entry.request === null) return "";
  try {
    return JSON.stringify(entry.request, null, 2);
  } catch {
    return String(entry.request);
  }
}

export function LogDetailsSheet(props: { entry: RuntimeLogEntry; onClose: () => void; onAction: ActionHandler }) {
  const [tab, setTab] = useState<"execution" | "request">("execution");
  const requestText = runtimeLogRequestText(props.entry);
  const executionID = props.entry.executionId ?? "Not captured by this runtime";
  const executionHeader = props.entry.executionId ?? "—";
  const source = runtimeLogSourceSummary(props.entry);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") props.onClose();
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [props.onClose]);

  const copyExecution = async () => {
    try {
      await navigator.clipboard.writeText(JSON.stringify(props.entry, null, 2));
      props.onAction("Copied execution details");
    } catch {
      props.onAction("Could not copy execution details");
    }
  };

  const fields: Array<[string, string]> = [
    ["Execution ID", executionID],
    ["Sync subscription ID", props.entry.operationId || "Not captured"],
    ["Function", props.entry.path || "runtime"],
    ["Type", props.entry.kind || "unknown"],
    ["Started", formatLogDateTime(runtimeLogStart(props.entry))],
    ["Completed", formatLogDateTime(props.entry.completedAt ?? props.entry.time)],
    ["Duration", formatDuration(props.entry.durationMs)],
    ["Project", props.entry.project ?? "Not captured"],
    ["Tenant", props.entry.tenant ?? "Not captured"],
    ["User", props.entry.userEmail || props.entry.userId || "Anonymous / not captured"],
    ["Connection", props.entry.connectionId || "Not captured"],
    ["Client", [props.entry.browser, props.entry.platform, props.entry.deviceType].filter(Boolean).join(" · ") || "Not captured"],
    ["Outcome", props.entry.outcome || "unknown"],
    ["Source", source.label],
    [source.group === "websocket" ? "Protocol" : "Cache", source.detail],
    ["Trigger", props.entry.reason || "Not captured"],
    ["Result rows", props.entry.resultCount === undefined ? "Not captured" : props.entry.resultCount.toLocaleString()],
    ["Request size", props.entry.requestSizeBytes ? `${props.entry.requestSizeBytes.toLocaleString()} B` : "Not captured"],
  ];

  return (
    <div className="log-sheet-backdrop" onMouseDown={(event) => { if (event.target === event.currentTarget) props.onClose(); }}>
      <aside className="log-detail-sheet" role="dialog" aria-modal="true" aria-label={`${props.entry.path} execution`}>
        <header className="log-sheet-header">
          <div className="log-sheet-title">
            <div className="log-sheet-kicker">
              <span className={`log-outcome-dot log-outcome-dot--${props.entry.outcome === "error" ? "error" : "ok"}`} />
              <code>{executionHeader}</code>
              <span>{formatLogDateTime(props.entry.time)}</span>
            </div>
            <h2>{props.entry.path || "Runtime execution"}</h2>
          </div>
          <div className="log-sheet-actions">
            <button type="button" onClick={copyExecution}>Copy</button>
            <button type="button" className="log-sheet-close" aria-label="Close execution details" onClick={props.onClose}>×</button>
          </div>
        </header>

        <nav className="log-sheet-tabs" aria-label="Execution detail sections">
          <button type="button" aria-pressed={tab === "execution"} onClick={() => setTab("execution")}>Execution</button>
          <button type="button" aria-pressed={tab === "request"} onClick={() => setTab("request")}>Request</button>
        </nav>

        <div className="log-sheet-body">
          {tab === "execution" ? (
            <>
              <dl className="log-detail-list">
                {fields.map(([label, value]) => (
                  <div key={label}><dt>{label}</dt><dd>{value}</dd></div>
                ))}
              </dl>
              {props.entry.error ? (
                <section className="log-error-detail" aria-label="Execution error">
                  <h3>Error</h3>
                  <pre>{props.entry.error}</pre>
                </section>
              ) : null}
            </>
          ) : (
            <section className="log-request-detail">
              <div><h3>Arguments</h3><span>{props.entry.requestSizeBytes ? `${props.entry.requestSizeBytes.toLocaleString()} bytes received` : "Captured arguments"}</span></div>
              {requestText ? <pre>{requestText}</pre> : <p>No request payload was captured for this execution.</p>}
            </section>
          )}
        </div>
      </aside>
    </div>
  );
}

function LogsPage(props: { project: ProjectTarget; themeMode: ThemeMode; onAction: ActionHandler }) {
  const { metrics, reachable, setMetrics } = useRuntimeMetrics(props.project, projectIsProvisioned(props.project));
  const [status, setStatus] = useState("Loading runtime logs...");
  const [search, setSearch] = useState("");
  const [outcome, setOutcome] = useState("all");
  const [kind, setKind] = useState("all");
  const [source, setSource] = useState("all");
  const [selectedLog, setSelectedLog] = useState<RuntimeLogEntry | null>(null);
  const [selectedLogIDs, setSelectedLogIDs] = useState<Set<string>>(() => new Set());
  const [selectionClearKey, setSelectionClearKey] = useState(0);
  const [hoveredLogRow, setHoveredLogRow] = useState<number | null>(null);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>(() => storedLogColumnWidths());
  const logs = (metrics?.logs ?? []).filter((entry) => entry.project === props.project.id);
  const outcomes = Array.from(new Set(logs.map((entry) => entry.outcome || "unknown"))).sort();
  const kinds = Array.from(new Set(logs.map((entry) => entry.kind || "unknown"))).sort();
  const sources = Array.from(new Set(logs.map((entry) => runtimeLogSourceSummary(entry).group))).sort();
  const visibleLogs = logs.filter((entry) => {
    const entryOutcome = entry.outcome || "unknown";
    const entryKind = entry.kind || "unknown";
    if (outcome !== "all" && entryOutcome !== outcome) return false;
    if (kind !== "all" && entryKind !== kind) return false;
    if (source !== "all" && runtimeLogSourceSummary(entry).group !== source) return false;
    return !search.trim() || logEntryText(entry).includes(search.trim().toLowerCase());
  });
  const selectedVisibleRows = useMemo(() => new Set(visibleLogs.flatMap((entry, index) => (
    selectedLogIDs.has(runtimeLogKey(entry)) ? [index] : []
  ))), [selectedLogIDs, visibleLogs]);
  const selectedLogCount = selectedLogIDs.size;

  const logColumnDefs: { title: string; id: string; width: number }[] = [
    { title: "Time", id: "time", width: 168 },
    { title: "ID", id: "id", width: 92 },
    { title: "Kind", id: "kind", width: 102 },
    { title: "Outcome", id: "outcome", width: 104 },
    { title: "Source", id: "source", width: 112 },
    { title: "Function", id: "path", width: 270 },
    { title: "Duration", id: "duration", width: 96 },
    { title: "Context", id: "detail", width: 300 },
  ];
  const logColumns: GridColumn[] = logColumnDefs.map((column) => ({
    ...column,
    width: columnWidths[column.id] ?? column.width,
  }));
  const logRows: GridRow[] = visibleLogs.map((entry) => [
    formatLogDateTime(entry.time),
    entry.executionId ? entry.executionId.slice(0, 8) : "—",
    entry.kind || "unknown",
    entry.outcome || "unknown",
    runtimeLogSourceSummary(entry).tableLabel,
    entry.path || "runtime",
    formatDuration(entry.durationMs),
    entry.error
      ? entry.error
      : [entry.reason, entry.connectionId, entry.browser, entry.tenant ? `tenant ${entry.tenant}` : "", entry.userEmail || entry.userId || ""].filter(Boolean).join(" · ") || "—",
  ]);

  const logErrorTextColor = props.themeMode === "dark" ? "#ff6b78" : "#d93f45";
  const logCellGetter = ([column, row]: Item): GridCell => {
    const value = logRows[row]?.[column] ?? "";
    const entry = visibleLogs[row];
    const isError = (entry?.outcome || "unknown") === "error";
    if (column === 2) {
      const entryKind = entry?.kind || "unknown";
      const tone = entryKind === "query" || entryKind === "mutation" || entryKind === "action" ? entryKind : "neutral";
      return logBadgeCell(value, tone);
    }
    if (column === 3) {
      return logBadgeCell(value, isError ? "error" : entry?.outcome === "ok" ? "success" : "neutral");
    }
    if (column === 4) {
      const sourceSummary = entry ? runtimeLogSourceSummary(entry) : null;
      return logBadgeCell(value, sourceSummary?.group === "redis" ? "success" : sourceSummary?.group === "database" ? "query" : "neutral");
    }
    return {
      kind: GridCellKind.Text,
      allowOverlay: false,
      displayData: value,
      data: value,
      ...(isError ? { themeOverride: { textDark: logErrorTextColor } } : {}),
    };
  };

  const persistColumnWidth = (id: string, newSize: number) => {
    setColumnWidths((current) => {
      const next = { ...current, [id]: newSize };
      try {
        window.localStorage.setItem(logsColumnWidthsKey, JSON.stringify(next));
      } catch {
        // Ignore storage failures (e.g. private mode); width still applies for this session.
      }
      return next;
    });
  };

  const copyLogs = async () => {
    const copySource = selectedLogCount > 0 ? logs : visibleLogs;
    const logsToCopy = runtimeLogsForCopy(copySource, selectedLogIDs);
    if (logsToCopy.length === 0) {
      props.onAction("No logs to copy");
      return;
    }
    const header = logColumnDefs.map((column) => column.title).join("\t");
    const body = logsToCopy.map((entry) => [
      formatLogDateTime(entry.time),
      entry.executionId ? entry.executionId.slice(0, 8) : "—",
      entry.kind || "unknown",
      entry.outcome || "unknown",
      runtimeLogSourceSummary(entry).tableLabel,
      entry.path || "runtime",
      formatDuration(entry.durationMs),
      entry.error
        ? entry.error
        : [entry.reason, entry.connectionId, entry.browser, entry.tenant ? `tenant ${entry.tenant}` : "", entry.userEmail || entry.userId || ""].filter(Boolean).join(" · ") || "—",
    ].join("\t")).join("\n");
    try {
      await navigator.clipboard.writeText(`${header}\n${body}`);
      props.onAction(`Copied ${logsToCopy.length} log ${logsToCopy.length === 1 ? "entry" : "entries"}`);
    } catch {
      props.onAction("Could not copy logs to clipboard");
    }
  };

  const clearLogs = async () => {
    const baseURL = runtimeURLForProject(props.project);
    if (!baseURL || !projectIsProvisioned(props.project)) {
      props.onAction("Runtime offline");
      return;
    }
    try {
      const response = await fetch(`${baseURL}/dev/logs`, {
        method: "DELETE",
        headers: runtimeHeaders(props.project),
      });
      if (!response.ok) throw new Error(response.statusText);
      const payload = (await response.json()) as { cleared?: number };
      const cleared = payload.cleared ?? 0;
      setMetrics((current) => current ? {
        ...current,
        logs: current.logs.filter((entry) => entry.project !== props.project.id),
      } : current);
      setSelectedLog(null);
      setSelectedLogIDs(new Set());
      setSelectionClearKey((key) => key + 1);
      setStatus(`Showing 0 runtime log entries for ${props.project.name}`);
      props.onAction(`Cleared ${cleared} runtime log ${cleared === 1 ? "entry" : "entries"}`);
    } catch {
      props.onAction("Could not clear runtime logs");
    }
  };

  useEffect(() => {
    if (!projectIsProvisioned(props.project) || !reachable) {
      setStatus("Runtime offline");
      return;
    }
    const projectLogCount = logs.length;
    setStatus(`Showing ${projectLogCount} runtime log ${projectLogCount === 1 ? "entry" : "entries"} for ${props.project.name}`);
  }, [logs.length, props.project, reachable]);

  useEffect(() => {
    setSelectedLog(null);
    setSelectedLogIDs(new Set());
    setSelectionClearKey((key) => key + 1);
  }, [props.project.id]);

  return (
    <div className="logs-shell">
      <section className="logs-toolbar" aria-label="Log filters">
        <input
          className="table-search logs-search"
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search logs..."
          value={search}
          aria-label="Search logs"
        />
        <AppSelect
          ariaLabel="Log outcome"
          label="Outcome"
          selectedKey={outcome}
          onChange={setOutcome}
          options={[
            { value: "all", label: "All" },
            ...outcomes.map((item) => ({ value: item, label: item })),
          ]}
        />
        <AppSelect
          ariaLabel="Log kind"
          label="Kind"
          selectedKey={kind}
          onChange={setKind}
          options={[
            { value: "all", label: "All" },
            ...kinds.map((item) => ({ value: item, label: item })),
          ]}
        />
        <AppSelect
          ariaLabel="Log source"
          label="Source"
          selectedKey={source}
          onChange={setSource}
          options={[
            { value: "all", label: "All" },
            ...sources.map((item) => ({
              value: item,
              label: item === "redis" ? "Redis" : item === "database" ? "Database" : item === "websocket" ? "WebSocket" : "Not tracked",
            })),
          ]}
        />
        <Button size="sm" variant="secondary" onPress={() => {
          setSearch("");
          setOutcome("all");
          setKind("all");
          setSource("all");
          props.onAction("Cleared log filters");
        }}>
          Clear
        </Button>
      </section>

      <section className="logs-panel" aria-label="Runtime logs">
        <header className="logs-panel-header">
          <div>
            <p className="eyebrow">{status}</p>
            <h2>{props.project.name} runtime logs</h2>
          </div>
          <div className="logs-panel-actions">
            {selectedLogCount > 0 ? (
              <Button size="sm" variant="ghost" onPress={() => { setSelectedLogIDs(new Set()); setSelectionClearKey((key) => key + 1); }}>
                Clear {selectedLogCount} selected
              </Button>
            ) : null}
            <Button size="sm" variant="secondary" onPress={copyLogs} isDisabled={visibleLogs.length === 0 && selectedLogCount === 0}>
              {selectedLogCount > 0 ? `Copy selected (${selectedLogCount})` : "Copy visible"}
            </Button>
            <Button size="sm" variant="secondary" onPress={clearLogs} isDisabled={logs.length === 0}>
              Clear logs
            </Button>
            <Chip color={metrics ? "success" : "warning"} size="sm" variant="soft">
              {metrics ? `${visibleLogs.length} visible` : "offline"}
            </Chip>
          </div>
        </header>
        <div className="logs-grid-wrap">
          <ManifestGrid
            columns={logColumns}
            getCellContent={logCellGetter}
            customRenderers={[logBadgeRenderer]}
            rowCount={logRows.length}
            height="100%"
            rowHeight={32}
            themeMode={props.themeMode}
            selectableRows
            clearSelectionKey={selectionClearKey}
            selectedRows={selectedVisibleRows}
            onColumnResize={(column, newSize) => persistColumnWidth(String(column.id), newSize)}
            onCellClick={([, row]) => setSelectedLog(visibleLogs[row] ?? null)}
            onSelectionChange={(selection) => setSelectedLogIDs(new Set(selection.rows.toArray().flatMap((row) => {
              const entry = visibleLogs[row];
              return entry ? [runtimeLogKey(entry)] : [];
            })))}
            onItemHovered={(event) => setHoveredLogRow(event.kind === "cell" ? event.location[1] : null)}
            hoveredRow={hoveredLogRow}
            zebraRows
            overlay={logRows.length === 0 ? (
              <div className="data-empty-state" role="status">
                <span>{metrics ? "No logs match the current filters." : "No runtime logs are available yet."}</span>
              </div>
            ) : null}
          />
        </div>
      </section>
      {selectedLog ? <LogDetailsSheet entry={selectedLog} onClose={() => setSelectedLog(null)} onAction={props.onAction} /> : null}
    </div>
  );
}

type DashboardErrorGroup = {
  fingerprint: string; title: string; culprit?: string; status: string; priority: string;
  count: number; firstSeen: string; lastSeen: string; tenants: Record<string, number>;
  // Optional: runtimes older than levels omit it, and those groups are errors.
  level?: "error" | "warning";
  users: Record<string, number>; devices: Record<string, number>; releases: Record<string, number>;
  environments: Record<string, number>; regression?: boolean; assignee?: string;
  latest: {
    timestamp: string; stack?: string; url?: string; userAgent?: string; release?: string; tenant?: string; environment?: string;
    user?: Record<string, unknown>; tags?: Record<string, string>; context?: Record<string, unknown>; breadcrumbs?: Array<Record<string, unknown>>;
  };
};

type ErrorTrackingRuntimeState = "checking" | "capturing" | "unavailable";

async function readErrorTrackingResponse<T>(
  response: Response,
  options: { action: string; runtimeURL: string; routeRequired?: boolean },
): Promise<T> {
  let payload: unknown = {};
  let rawBody = "";

  try {
    // Reading the body as text first lets us turn legacy runtime responses such
    // as `404 page not found` into a useful compatibility message. Calling
    // response.json() directly reports the confusing "non-whitespace" parser
    // error because the leading HTTP status happens to be a valid JSON number.
    if (typeof response.text === "function") {
      rawBody = await response.text();
      payload = rawBody.trim() ? JSON.parse(rawBody) : {};
    } else {
      // A small fallback keeps lightweight Response doubles useful in tests.
      payload = await response.json();
    }
  } catch {
    if (response.status === 404 && options.routeRequired) {
      throw new Error(`Error tracking is unavailable at ${options.runtimeURL}. Restart Gonvex with the current runtime build, then refresh this page.`);
    }
    const responseSummary = rawBody.trim().replace(/\s+/g, " ").slice(0, 160);
    const status = [response.status, response.statusText].filter(Boolean).join(" ");
    throw new Error(`${options.action} failed: the Gonvex runtime returned invalid JSON${status ? ` (${status})` : ""}${responseSummary ? ` — ${responseSummary}` : ""}.`);
  }

  if (!response.ok) {
    if (response.status === 404 && options.routeRequired) {
      throw new Error(`Error tracking is unavailable at ${options.runtimeURL}. Restart Gonvex with the current runtime build, then refresh this page.`);
    }
    const runtimeError = payload && typeof payload === "object" && "error" in payload && typeof payload.error === "string"
      ? payload.error
      : "";
    throw new Error(runtimeError || `${options.action} failed (${response.status}${response.statusText ? ` ${response.statusText}` : ""}).`);
  }

  return payload as T;
}

function ErrorsPage(props: { project: ProjectTarget }) {
  const [groups, setGroups] = useState<DashboardErrorGroup[]>([]);
  const [releases, setReleases] = useState<string[]>([]);
  const [release, setRelease] = useState("all");
  const [status, setStatus] = useState("unresolved");
  const [level, setLevel] = useState("all");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<string | null>(null);
  const [copied, setCopied] = useState<string | null>(null);
  const [runtimeState, setRuntimeState] = useState<ErrorTrackingRuntimeState>("checking");

  const load = useCallback(async () => {
    setLoading(true);
    setRuntimeState("checking");
    const runtimeURL = runtimeURLForProject(props.project);
    try {
      const params = new URLSearchParams();
      if (status !== "all") params.set("status", status);
      if (release !== "all") params.set("release", release);
      if (level !== "all") params.set("level", level);
      const response = await fetch(`${runtimeURL}/dev/errors/groups${params.size ? `?${params.toString()}` : ""}`, { headers: runtimeHeaders(props.project) });
      const payload = await readErrorTrackingResponse<{ groups?: DashboardErrorGroup[]; releases?: string[] }>(response, {
        action: "Loading error groups",
        routeRequired: true,
        runtimeURL,
      });
      const nextReleases = payload.releases ?? [];
      setGroups(payload.groups ?? []);
      setReleases(nextReleases);
      if (release !== "all" && !nextReleases.includes(release)) setRelease("all");
      setError("");
      setRuntimeState("capturing");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Could not load error groups");
      setRuntimeState("unavailable");
    }
    finally { setLoading(false); }
  }, [props.project, release, status, level]);

  useEffect(() => { void load(); }, [load]);

  const updateGroup = async (group: DashboardErrorGroup, update: Record<string, string>) => {
    const runtimeURL = runtimeURLForProject(props.project);
    try {
      const response = await fetch(`${runtimeURL}/dev/errors/groups/${group.fingerprint}`, { method: "PATCH", headers: runtimeHeaders(props.project, { "content-type": "application/json" }), body: JSON.stringify(update) });
      await readErrorTrackingResponse<DashboardErrorGroup>(response, { action: "Updating error group", runtimeURL });
      void load();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Could not update error group");
    }
  };

  const copyBrief = async (group: DashboardErrorGroup) => {
    const runtimeURL = runtimeURLForProject(props.project);
    try {
      const response = await fetch(`${runtimeURL}/dev/errors/groups/${group.fingerprint}/bug-report`, { headers: runtimeHeaders(props.project) });
      const payload = await readErrorTrackingResponse<{ markdown?: string }>(response, { action: "Creating agent brief", runtimeURL });
      if (!payload.markdown) throw new Error("The runtime returned an empty agent brief.");
      await navigator.clipboard.writeText(payload.markdown);
      setCopied(group.fingerprint);
      window.setTimeout(() => setCopied(null), 1800);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Could not create agent brief");
    }
  };

  const normalizedSearch = search.trim().toLowerCase();
  const visibleGroups = groups.filter((group) => !normalizedSearch || [group.title, group.culprit, group.fingerprint, ...Object.keys(group.tenants), ...Object.keys(group.releases)].some((value) => value?.toLowerCase().includes(normalizedSearch)));
  const impactedTenants = new Set(groups.flatMap((group) => Object.keys(group.tenants))).size;
  const occurrences = groups.reduce((total, group) => total + group.count, 0);
  const regressions = groups.filter((group) => group.regression).length;
  const releaseOptions: SelectOption[] = [
    { value: "all", label: "All releases", description: "Errors from every release" },
    ...releases.map((value, index) => ({
      value,
      label: value,
      description: index === 0 ? "Latest release" : undefined,
    })),
  ];
  const levelOptions: SelectOption[] = [
    { value: "all", label: "All levels", description: "Errors and warnings" },
    { value: "error", label: "Errors", description: "Something broke" },
    { value: "warning", label: "Warnings", description: "Handled, but worth knowing" },
  ];
  const runtimeStatus = runtimeState === "capturing"
    ? { label: "Capturing", detail: "Gonvex runtime" }
    : runtimeState === "checking"
      ? { label: "Checking", detail: "Verifying runtime" }
      : { label: "Unavailable", detail: "Update runtime" };

  return <section className="errors-inbox" aria-label="Error groups">
    <div className="errors-command-header">
      <div><p className="eyebrow">Incident intelligence</p><h2>Errors affecting real users</h2><p>Grouped by root cause, enriched with tenant, release, user, and machine context.</p></div>
      <div className="errors-live-indicator" data-state={runtimeState} aria-live="polite"><span aria-hidden="true" /><strong>{runtimeStatus.label}</strong><small>{runtimeStatus.detail}</small></div>
    </div>
    <div className="errors-stat-strip" aria-label="Error impact summary">
      <div><span>Groups</span><strong>{groups.length}</strong></div>
      <div><span>Occurrences</span><strong>{occurrences}</strong></div>
      <div><span>Tenants hit</span><strong>{impactedTenants}</strong></div>
      <div data-alert={regressions > 0 ? "true" : undefined}><span>Regressions</span><strong>{regressions}</strong></div>
    </div>
    <div className="errors-toolbar">
      <input className="errors-search" aria-label="Search error groups" placeholder="Search message, tenant, release, fingerprint…" value={search} onChange={(event) => setSearch(event.target.value)} />
      <div className="errors-toolbar-actions">
        <AppSelect
          ariaLabel="Error release"
          className="errors-release-filter"
          onChange={setRelease}
          options={releaseOptions}
          selectedKey={release}
        />
        <AppSelect
          ariaLabel="Error level"
          className="errors-level-filter"
          onChange={setLevel}
          options={levelOptions}
          selectedKey={level}
        />
        <Select aria-label="Error status" selectedKey={status} onSelectionChange={(key) => setStatus(String(key))}>
          <Select.Trigger><Select.Value /><Select.Indicator /></Select.Trigger>
          <Select.Popover><ListBox><ListBox.Item id="unresolved">Unresolved</ListBox.Item><ListBox.Item id="resolved">Resolved</ListBox.Item><ListBox.Item id="ignored">Ignored</ListBox.Item><ListBox.Item id="all">All groups</ListBox.Item></ListBox></Select.Popover>
        </Select>
        <Button size="sm" variant="secondary" onPress={() => void load()}>Refresh</Button>
      </div>
    </div>
    {error ? <p className="form-error" role="alert">{error}</p> : null}
    {loading ? <p>Loading error groups…</p> : null}
    {!loading && !error && visibleGroups.length === 0 ? <div className="errors-empty"><span className="errors-empty-mark">✓</span><strong>{groups.length ? "No matching groups" : release === "all" ? `No ${status === "all" ? "captured" : status} errors` : `No errors in ${release}`}</strong><span>{groups.length ? "Try a tenant, release, or part of the error message." : release === "all" ? "Captured browser and Gonvex operation failures will appear here automatically." : "Choose another release or refresh after new errors are captured."}</span></div> : null}
    <div className="error-group-list">
      {visibleGroups.map((group) => {
        const expanded = selected === group.fingerprint;
        const errorContext = group.latest.context && Object.keys(group.latest.context).length > 0
          ? JSON.stringify(group.latest.context, null, 2)
          : "";
        const errorContextLabel = group.latest.tags?.source === "runtime" ? "Execution context" : "Captured context";
        return <article className="error-group-card" data-expanded={expanded ? "true" : undefined} data-level={group.level ?? "error"} data-priority={group.priority} key={group.fingerprint}>
          <button className="error-group-summary" type="button" aria-expanded={expanded} onClick={() => setSelected(expanded ? null : group.fingerprint)}>
            <div className="error-group-main"><div className="error-group-title"><span className="error-priority">{group.priority}</span>{group.level === "warning" ? <span className="error-level">warning</span> : null}{group.regression ? <span className="error-regression">regression</span> : null}<strong>{group.title}</strong></div><code>{group.culprit || group.fingerprint}</code><span>Last seen {new Date(group.lastSeen).toLocaleString()} · first seen {new Date(group.firstSeen).toLocaleDateString()}</span></div>
            <div className="error-impact"><div><strong>{group.count}</strong><span>events</span></div><div><strong>{Object.keys(group.tenants).length}</strong><span>tenants</span></div><div><strong>{Object.keys(group.users).length}</strong><span>users</span></div><div><strong>{Object.keys(group.devices).length}</strong><span>machines</span></div></div>
            <span className="error-expand-mark" aria-hidden="true">{expanded ? "−" : "+"}</span>
          </button>
          {expanded ? <div className="error-group-detail">
            <div className="error-detail-primary">
              <div className="error-detail-heading"><div><span>Latest exception</span><strong>{group.latest.release || "Unknown release"}</strong></div><code>{group.fingerprint}</code></div>
              <pre>{group.latest.stack || group.title}</pre>
              <dl className="error-context-grid">
                <div><dt>Tenant</dt><dd>{group.latest.tenant || "—"}</dd></div><div><dt>Environment</dt><dd>{group.latest.environment || "—"}</dd></div>
                <div><dt>URL</dt><dd title={group.latest.url}>{group.latest.url || "—"}</dd></div><div><dt>User agent</dt><dd title={group.latest.userAgent}>{group.latest.userAgent || "—"}</dd></div>
              </dl>
              {errorContext ? <div className="error-event-context"><span>{errorContextLabel}</span><pre>{errorContext}</pre></div> : null}
              {group.latest.breadcrumbs?.length ? <div className="error-breadcrumbs"><span>Recent breadcrumbs</span>{group.latest.breadcrumbs.slice(-5).map((crumb, index) => <code key={index}>{String(crumb.category ?? "event")} · {String(crumb.message ?? "")}</code>)}</div> : null}
            </div>
            <aside className="error-detail-rail">
              <ErrorBreakdown title="Tenants" values={group.tenants} />
              <ErrorBreakdown title="Releases" values={group.releases} />
              <ErrorBreakdown title="Machines" values={group.devices} maskKeys />
              <div className="error-triage-actions"><span>Triage</span><div><Button size="sm" variant="secondary" onPress={() => void copyBrief(group)}>{copied === group.fingerprint ? "Brief copied" : "Copy agent brief"}</Button>{group.status === "resolved" ? <Button size="sm" variant="ghost" onPress={() => void updateGroup(group, { status: "unresolved" })}>Reopen</Button> : <Button size="sm" variant="ghost" onPress={() => void updateGroup(group, { status: "resolved" })}>Resolve</Button>}<Button size="sm" variant="ghost" onPress={() => void updateGroup(group, { status: "ignored" })}>Ignore</Button></div></div>
            </aside>
          </div> : null}
        </article>;
      })}
    </div>
  </section>;
}

function ErrorBreakdown(props: { title: string; values: Record<string, number>; maskKeys?: boolean }) {
  const entries = Object.entries(props.values).sort((a, b) => b[1] - a[1]).slice(0, 5);
  return <div className="error-breakdown"><span>{props.title}</span>{entries.length ? entries.map(([key, count]) => <div key={key}><code title={key}>{props.maskKeys ? `${key.slice(0, 8)}…` : key}</code><strong>{count}</strong></div>) : <small>No data</small>}</div>;
}

export function groupRuntimeSchedulerCrons(crons: RuntimeSchedulerCron[]): RuntimeSchedulerCronGroup[] {
  const groups = new Map<string, RuntimeSchedulerCronGroup>();
  for (const cron of crons) {
    const scope = cron.tenant ? "tenant" : "project";
    const key = JSON.stringify([cron.project ?? "", cron.name, cron.function, cron.schedule, scope]);
    const current = groups.get(key);
    if (!current) {
      groups.set(key, {
        ...cron,
        key,
        registrations: [cron],
      });
      continue;
    }
    current.registrations.push(cron);
    current.runs += cron.runs;
    current.failures += cron.failures;
    if (cron.nextRun && (!current.nextRun || new Date(cron.nextRun).getTime() < new Date(current.nextRun).getTime())) {
      current.nextRun = cron.nextRun;
    }
    if (cron.lastRun && (!current.lastRun || new Date(cron.lastRun).getTime() > new Date(current.lastRun).getTime())) {
      current.lastRun = cron.lastRun;
    }
  }
  return [...groups.values()].map((group) => ({
    ...group,
    registrations: [...group.registrations].sort((left, right) => (left.tenant ?? "").localeCompare(right.tenant ?? "")),
  }));
}

export function SchedulesPage(props: { project: ProjectTarget }) {
  const { metrics, reachable } = useRuntimeMetrics(props.project, projectIsProvisioned(props.project));
  const [status, setStatus] = useState("Loading scheduler...");

  useEffect(() => {
    if (!projectIsProvisioned(props.project) || !reachable) {
      setStatus("Runtime offline");
      return;
    }
    const nextGroups = groupRuntimeSchedulerCrons(metrics?.scheduler?.crons ?? []);
    const instanceCount = metrics?.scheduler?.crons.length ?? 0;
    setStatus(metrics?.scheduler ? `${nextGroups.length} cron definition${nextGroups.length === 1 ? "" : "s"} · ${instanceCount} registered instance${instanceCount === 1 ? "" : "s"}` : "No scheduler data");
  }, [metrics, props.project, reachable]);

  const scheduler = metrics?.scheduler ?? null;
  const crons = scheduler?.crons ?? [];
  const groupedCrons = groupRuntimeSchedulerCrons(crons);
  const recent = scheduler?.recent ?? [];
  const summary = [
    { label: "Definitions", value: String(groupedCrons.length) },
    { label: "Instances", value: String(crons.length) },
    { label: "Scheduled", value: String(scheduler?.scheduled ?? 0) },
    { label: "Running", value: String(scheduler?.running ?? 0) },
    { label: "Queued", value: String(scheduler?.queued ?? 0) },
    { label: "Completed", value: String(scheduler?.completed ?? 0) },
    { label: "Failed", value: String(scheduler?.failed ?? 0) },
    { label: "Lag", value: formatDuration(scheduler?.lagMs ?? 0) },
  ];

  return (
    <div className="logs-shell schedules-shell">
      <section className="schedules-summary" aria-label="Scheduler summary">
        {summary.map((item) => (
          <div className="schedules-stat" key={item.label}>
            <span className="schedules-stat-label">{item.label}</span>
            <strong className="schedules-stat-value">{item.value}</strong>
          </div>
        ))}
      </section>

      <section className="logs-panel logs-panel--crons" aria-label="Registered crons">
        <header className="logs-panel-header">
          <div>
            <p className="eyebrow">{status}</p>
            <h2>Registered crons</h2>
          </div>
          <Chip color={metrics ? "success" : "warning"} size="sm" variant="soft">{metrics ? `${groupedCrons.length}` : "offline"}</Chip>
        </header>
        <div className="sched-table sched-table--crons" role="table" aria-label="Cron jobs">
          <div className="sched-row sched-row--head" role="row">
            <span role="columnheader">Name</span>
            <span role="columnheader">Function</span>
            <span role="columnheader">Scope</span>
            <span role="columnheader">Schedule</span>
            <span role="columnheader">Next run</span>
            <span role="columnheader">Runs</span>
            <span role="columnheader">Failures</span>
          </div>
          {groupedCrons.map((cron) => (
            <div className="sched-row" role="row" key={cron.key} data-status={cron.failures > 0 ? "warn" : "ok"}>
              <strong role="cell">{cron.name}</strong>
              <code role="cell">{cron.function}</code>
              <span className="sched-scope-cell" role="cell">
                {cron.tenant ? (
                  <details className="sched-scope-disclosure">
                    <summary title="One isolated registration runs for each tenant">
                      {cron.registrations.length} tenant{cron.registrations.length === 1 ? "" : "s"}
                    </summary>
                    <span className="sched-scope-list">
                      {cron.registrations.map((registration) => (
                        <span className="sched-scope-instance" key={registration.tenant}>
                          <code title={registration.tenant}>{registration.tenant}</code>
                          <small data-failures={registration.failures > 0 ? "true" : undefined}>
                            {registration.runs} run{registration.runs === 1 ? "" : "s"}
                            {registration.failures > 0 ? ` · ${registration.failures} failed` : ""}
                          </small>
                        </span>
                      ))}
                    </span>
                  </details>
                ) : <span className="sched-project-scope">Project-wide</span>}
              </span>
              <code role="cell">{cron.schedule}</code>
              <span role="cell">{cron.nextRun ? formatLogDateTime(cron.nextRun) : "—"}</span>
              <span role="cell">{cron.runs}</span>
              <span role="cell">{cron.failures}</span>
            </div>
          ))}
          {crons.length === 0 ? (
            <div className="sched-empty" role="row">
              <span role="cell">{metrics ? "No crons registered. Add one with app.Cron(\"name\", interval, \"function\", nil) in your project." : "Runtime offline."}</span>
            </div>
          ) : null}
        </div>
      </section>

      <section className="logs-panel logs-panel--runs" aria-label="Recent scheduled runs">
        <header className="logs-panel-header">
          <div>
            <p className="eyebrow">Most recent first</p>
            <h2>Recent runs</h2>
          </div>
          <Chip color="default" size="sm" variant="secondary">{recent.length}</Chip>
        </header>
        <div className="sched-table sched-table--runs" role="table" aria-label="Recent scheduled runs">
          <div className="sched-row sched-row--head" role="row">
            <span role="columnheader">Time</span>
            <span role="columnheader">Function</span>
            <span role="columnheader">Outcome</span>
            <span role="columnheader">Lag</span>
            <span role="columnheader">Duration</span>
          </div>
          {recent.map((run, index) => (
            <div className="sched-row" role="row" key={`${run.time}:${run.function}:${index}`} data-outcome={run.outcome}>
              <span role="cell">{formatLogDateTime(run.time)}</span>
              <code role="cell">{run.function}</code>
              <strong role="cell">{run.error ? run.error : run.outcome}</strong>
              <span role="cell">{formatDuration(run.lagMs)}</span>
              <span role="cell">{formatDuration(run.durationMs)}</span>
            </div>
          ))}
          {recent.length === 0 ? (
            <div className="sched-empty" role="row">
              <span role="cell">{metrics ? "No scheduled runs yet." : "Runtime offline."}</span>
            </div>
          ) : null}
        </div>
      </section>
    </div>
  );
}

function realtimeRelativeTime(value: string): string {
  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) return "unknown";
  const elapsedSeconds = Math.max(0, Math.round((Date.now() - timestamp) / 1000));
  if (elapsedSeconds < 5) return "now";
  if (elapsedSeconds < 60) return `${elapsedSeconds}s ago`;
  const elapsedMinutes = Math.round(elapsedSeconds / 60);
  if (elapsedMinutes < 60) return `${elapsedMinutes}m ago`;
  return `${Math.round(elapsedMinutes / 60)}h ago`;
}

function realtimeActivityLabel(activity: string): string {
  const labels: Record<string, string> = {
    connected: "Connected",
    auth: "Authenticated",
    "query.subscribe": "Subscribed",
    "query.unsubscribe": "Unsubscribed",
    "mutation.call": "Called mutation",
    "action.call": "Called action",
    query: "Received query update",
    mutation: "Received mutation result",
    action: "Received action result",
  };
  return labels[activity] ?? activity.replaceAll(".", " ");
}

function realtimeInitials(value: string): string {
  const name = value.split("@")[0] ?? value;
  const parts = name.split(/[._\-\s]+/).filter(Boolean);
  return parts.slice(0, 2).map((part) => part[0]?.toUpperCase()).join("") || "?";
}

function realtimeDeviceLabel(connection: RuntimeWebSocketConnection): string {
  return [connection.browser, connection.platform, connection.deviceType, connection.connectionType].filter(Boolean).join(" · ") || "Client details pending";
}

type RealtimeUserGroup = {
  key: string;
  label: string;
  secondary: string;
  connections: RuntimeWebSocketConnection[];
  subscriptions: number;
  tenants: string[];
};

export function RealtimeDashboard(props: {
  metrics: RuntimeMetricsResponse | null;
  project: ProjectTarget;
  reachable: boolean;
}) {
  const [search, setSearch] = useState("");
  const websocket = props.metrics?.websocket;
  const details = websocket?.details ?? [];
  const normalizedSearch = search.trim().toLowerCase();
  const visibleConnections = normalizedSearch
    ? details.filter((connection) => [
      connection.id,
      connection.userEmail,
      connection.userId,
      connection.tenant,
      connection.browser,
      connection.platform,
      connection.lastPath,
      ...connection.subscriptions,
    ].some((value) => String(value ?? "").toLowerCase().includes(normalizedSearch)))
    : details;

  const users = useMemo(() => {
    const grouped = new Map<string, RealtimeUserGroup>();
    for (const connection of visibleConnections) {
      const key = connection.userId || "anonymous";
      const current = grouped.get(key) ?? {
        key,
        label: connection.userEmail || connection.userId || "Anonymous sessions",
        secondary: connection.userEmail && connection.userId ? connection.userId : connection.authenticated ? "Authenticated user" : "Not authenticated",
        connections: [],
        subscriptions: 0,
        tenants: [],
      };
      current.connections.push(connection);
      current.subscriptions += connection.subscriptions.length;
      if (connection.tenant && !current.tenants.includes(connection.tenant)) current.tenants.push(connection.tenant);
      grouped.set(key, current);
    }
    return [...grouped.values()].sort((left, right) => right.connections.length - left.connections.length || left.label.localeCompare(right.label));
  }, [visibleConnections]);

  const recentActivity = (props.metrics?.logs ?? []).slice(0, 18);
  const activitySince = Date.now() - 5 * 60 * 1000;
  const recentEventCount = (props.metrics?.logs ?? []).filter((entry) => new Date(entry.time).getTime() >= activitySince).length;
  const reportedConnections = websocket?.connections ?? 0;
  const reportedUsers = websocket?.users ?? users.length;
  const hasLegacySummaryOnly = reportedConnections > 0 && details.length === 0;

  return (
    <div className="realtime-shell">
      <header className="realtime-command-header">
        <div>
          <div className="realtime-kicker"><span className={`health-pulse ${props.reachable ? "is-live" : "is-down"}`} aria-hidden="true" /> Live presence</div>
          <h2>Who is connected right now?</h2>
          <p>Trace each browser session from a user to its tenant and active query subscriptions.</p>
        </div>
        <div className="realtime-refresh-state" data-state={props.reachable ? "live" : "offline"}>
          <strong>{props.reachable ? "Streaming" : "Runtime offline"}</strong>
          <span>{props.reachable ? "updates every 2 seconds" : "retrying automatically"}</span>
        </div>
      </header>

      <section className="realtime-stat-strip" aria-label="Realtime summary">
        <div><span>Users online</span><strong>{reportedUsers}</strong></div>
        <div><span>Connections</span><strong>{reportedConnections}</strong></div>
        <div><span>Subscriptions</span><strong>{websocket?.subscriptions ?? 0}</strong></div>
        <div><span>Events · 5 min</span><strong>{recentEventCount}</strong></div>
      </section>

      <div className="realtime-workspace">
        <main className="realtime-main">
          <section className="realtime-panel" aria-labelledby="online-users-title">
            <header className="realtime-panel-header">
              <div><span>Presence</span><h3 id="online-users-title">Online users</h3></div>
              <small>{users.length} {users.length === 1 ? "identity" : "identities"}</small>
            </header>
            <div className="realtime-user-list">
              {users.map((user) => (
                <article className="realtime-user" key={user.key}>
                  <div className="realtime-avatar" aria-hidden="true">{realtimeInitials(user.label)}</div>
                  <span className="realtime-online-dot" aria-label="Online" />
                  <div className="realtime-user-name"><strong>{user.label}</strong><code>{user.secondary}</code></div>
                  <div className="realtime-user-destinations"><span>Tenants</span><strong>{user.tenants.join(", ") || props.project.id}</strong></div>
                  <div className="realtime-user-count"><strong>{user.connections.length}</strong><span>{user.connections.length === 1 ? "connection" : "connections"}</span></div>
                  <div className="realtime-user-count"><strong>{user.subscriptions}</strong><span>subscriptions</span></div>
                </article>
              ))}
              {users.length === 0 ? (
                <div className="realtime-empty">
                  <span aria-hidden="true">○</span>
                  <strong>{normalizedSearch ? "No connections match that search" : "No users online"}</strong>
                  <p>{normalizedSearch ? "Try a user, tenant, browser, or function name." : "New WebSocket sessions will appear here as soon as a client connects."}</p>
                </div>
              ) : null}
            </div>
          </section>

          <section className="realtime-panel" aria-labelledby="connections-title">
            <header className="realtime-panel-header realtime-panel-header--search">
              <div><span>Connection paths</span><h3 id="connections-title">Every connection</h3></div>
              <label className="realtime-search">
                <input aria-label="Search connections" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search user, tenant, function…" type="search" />
              </label>
            </header>
            {hasLegacySummaryOnly ? (
              <div className="realtime-compatibility" role="status">
                This runtime reports {reportedConnections} live {reportedConnections === 1 ? "connection" : "connections"}, but it predates connection-level details. Restart it with this build to inspect destinations and users.
              </div>
            ) : null}
            <div className="realtime-connection-list">
              {visibleConnections.map((connection) => {
                const identity = connection.userEmail || connection.userId || "Anonymous";
                const subscriptionCounts = connection.subscriptions.reduce((counts, subscription) => {
                  counts.set(subscription, (counts.get(subscription) ?? 0) + 1);
                  return counts;
                }, new Map<string, number>());
                return (
                  <article className="realtime-connection" key={connection.id}>
                    <header>
                      <div><span className="realtime-socket-mark" aria-hidden="true" /><code>{connection.id}</code></div>
                      <span>{realtimeDeviceLabel(connection)}</span>
                      <time dateTime={connection.lastActiveAt}>{realtimeRelativeTime(connection.lastActiveAt)}</time>
                    </header>
                    <div className="realtime-route" aria-label={`${identity} connects to ${connection.tenant}`}>
                      <div><span>Identity</span><strong>{identity}</strong></div>
                      <b aria-hidden="true">→</b>
                      <div><span>Destination</span><strong>{connection.tenant || connection.project}</strong></div>
                      <b aria-hidden="true">→</b>
                      <div><span>Watching</span><strong>{connection.subscriptions.length} {connection.subscriptions.length === 1 ? "query" : "queries"}</strong></div>
                    </div>
                    <div className="realtime-connection-foot">
                      <details className="realtime-subscription-disclosure">
                        <summary aria-label={`Show ${connection.subscriptions.length} subscriptions for ${connection.id}`}>
                          <span>Subscriptions</span>
                          <strong>{connection.subscriptions.length}</strong>
                          <em>{connection.subscriptions.length === 0 ? "None active" : "View paths"}</em>
                        </summary>
                        {connection.subscriptions.length > 0 ? (
                          <div className="realtime-subscription-list">
                            {[...subscriptionCounts.entries()].map(([subscription, count]) => (
                              <div key={subscription}>
                                <code>{subscription}</code>
                                {count > 1 ? <span>×{count}</span> : null}
                              </div>
                            ))}
                          </div>
                        ) : null}
                      </details>
                      <span className="realtime-last-activity">{realtimeActivityLabel(connection.lastActivity)}{connection.lastPath ? ` · ${connection.lastPath}` : ""}</span>
                    </div>
                  </article>
                );
              })}
            </div>
          </section>
        </main>

        <aside className="realtime-activity" aria-labelledby="project-activity-title">
          <header className="realtime-panel-header">
            <div><span>Project pulse</span><h3 id="project-activity-title">Recent activity</h3></div>
            <small>{props.project.name}</small>
          </header>
          <div className="realtime-activity-list">
            {recentActivity.map((entry, index) => (
              <article className="realtime-event" key={`${entry.time}:${entry.executionId ?? entry.path}:${index}`} data-outcome={entry.outcome}>
                <span className="realtime-event-line" aria-hidden="true" />
                <div className="realtime-event-top"><strong>{entry.path}</strong><time dateTime={entry.time}>{realtimeRelativeTime(entry.time)}</time></div>
                <p><span>{entry.kind}</span>{entry.userEmail || entry.userId ? ` by ${entry.userEmail || entry.userId}` : ""}</p>
                <div><code>{entry.tenant || props.project.id}</code><span>{formatDuration(entry.durationMs)} · {entry.outcome}</span></div>
              </article>
            ))}
            {recentActivity.length === 0 ? (
              <div className="realtime-empty realtime-empty--activity">
                <span aria-hidden="true">↯</span><strong>No project activity yet</strong><p>Queries, mutations, and actions will stream into this timeline.</p>
              </div>
            ) : null}
          </div>
        </aside>
      </div>
    </div>
  );
}

function RealtimePage(props: { project: ProjectTarget }) {
  const { metrics, reachable } = useRuntimeMetrics(props.project, projectIsProvisioned(props.project));
  return <RealtimeDashboard metrics={metrics} project={props.project} reachable={reachable} />;
}

// Dashboard API routes (/api/dashboard/*) are only served by the production
// dashboard server. Under `vite`/`gonvex dev` they fall through to the SPA's
// index.html, so response.json() would otherwise throw a cryptic
// "Unexpected token '<'". Detect that and surface a clear message instead.
async function readDashboardJSON<T>(response: Response): Promise<T> {
  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) {
    throw new Error("This dashboard API endpoint isn't available in the local dev server yet.");
  }
  return await response.json() as T;
}

function SettingsPage(props: {
  databaseMode: DatabaseMode;
  hideTestTenants: boolean;
  onDatabaseModeChange: (mode: DatabaseMode) => void;
  onHideTestTenantsChange: (hidden: boolean) => void;
  onProjectNameChange: (name: string) => Promise<void>;
  project: ProjectTarget;
}) {
  const [activeSection, setActiveSection] = useState<"general" | "database" | "connection" | "environment" | "members" | "authentication">("general");
  const [projectName, setProjectName] = useState(props.project.name);
  const [projectNameStatus, setProjectNameStatus] = useState("");
  const [projectNameSaving, setProjectNameSaving] = useState(false);
  const [projectKey, setProjectKey] = useState("");
  const [projectKeyStatus, setProjectKeyStatus] = useState("");
  const [projectKeyLoading, setProjectKeyLoading] = useState(false);
  const [envVars, setEnvVars] = useState<EnvVariable[]>([]);
  const [envName, setEnvName] = useState("");
  const [envValue, setEnvValue] = useState("");
  const [envStatus, setEnvStatus] = useState("");
  const [envLoading, setEnvLoading] = useState(false);
  const [envSaving, setEnvSaving] = useState(false);
  const [envPasteMode, setEnvPasteMode] = useState(true);
  const [envPasteText, setEnvPasteText] = useState("");
  const envPasteDirtyRef = useRef(false);
  const [members, setMembers] = useState<ProjectMember[]>([]);
  const [invitations, setInvitations] = useState<ProjectInvitation[]>([]);
  const [memberStatus, setMemberStatus] = useState("");
  const [membersLoading, setMembersLoading] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<ProjectMember["role"]>("dev");
  const [inviteSaving, setInviteSaving] = useState(false);
  const dashboardURL = typeof window === "undefined"
    ? pathForProjectPage(props.project.id, "overview")
    : `${window.location.origin}${pathForProjectPage(props.project.id, "overview")}`;
  const runtimeURL = runtimeURLForProject(props.project) || "not configured";
  const projectEnvSnippet = projectKey
    ? [
      `GONVEX_RUNTIME_URL=${runtimeURL}`,
      `GONVEX_PROJECT_KEY=${projectKey}`,
      `VITE_GONVEX_WS_URL=${runtimeURL.replace(/^http/, "ws")}/ws`,
    ].join("\n")
    : "";

  useEffect(() => {
    setProjectName(props.project.name);
  }, [props.project.id, props.project.name]);

  useEffect(() => {
    setProjectNameStatus("");
    envPasteDirtyRef.current = false;
    setEnvPasteText("");
    setEnvVars([]);
  }, [props.project.id]);

  const saveProjectName = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = projectName.trim();
    if (!name) {
      setProjectNameStatus("Project name is required");
      return;
    }
    if (name === props.project.name) return;

    setProjectNameSaving(true);
    setProjectNameStatus("");
    try {
      await props.onProjectNameChange(name);
      setProjectName(name);
      setProjectNameStatus("Project name saved");
    } catch (error) {
      setProjectNameStatus(error instanceof Error ? error.message : "Could not rename project");
    } finally {
      setProjectNameSaving(false);
    }
  };

  const revealProjectKey = async () => {
    setProjectKeyLoading(true);
    setProjectKeyStatus("");
    try {
      // Call the runtime directly like every other project API — the old
      // /api/dashboard/projects/{id}/key path was never implemented in the
      // dashboard's node server, so reveal always got its literal 404 "not found".
      const baseURL = runtimeURLForProject(props.project);
      if (!baseURL) throw new Error("runtime URL is not configured for this project");
      const response = await fetch(`${baseURL}/dev/projects/${encodeURIComponent(props.project.id)}/key`, {
        headers: dashboardAuthHeaders(),
        method: "POST",
      });
      const payload = await response.json() as { projectKey?: string; error?: string };
      if (!response.ok || !payload.projectKey) throw new Error(payload.error ?? response.statusText);
      setProjectKey(payload.projectKey);
      setProjectKeyStatus("Project key revealed");
    } catch (error) {
      setProjectKeyStatus(error instanceof Error ? error.message : "Could not reveal project key");
    } finally {
      setProjectKeyLoading(false);
    }
  };

  const copyProjectKey = async () => {
    if (!projectEnvSnippet) return;
    await navigator.clipboard.writeText(projectEnvSnippet).catch(() => undefined);
    setProjectKeyStatus("Copied .env.local values");
  };

  const envEndpoint = () => {
    const baseURL = runtimeURLForProject(props.project);
    return baseURL ? `${baseURL}/dev/projects/${encodeURIComponent(props.project.id)}/env` : "";
  };

  const loadEnvVars = useCallback(async () => {
    const endpoint = envEndpoint();
    if (!endpoint) {
      setEnvVars([]);
      setEnvStatus("Runtime offline — start the Gonvex Runtime to manage env vars.");
      return;
    }
    setEnvLoading(true);
    setEnvStatus("");
    try {
      const response = await fetch(endpoint, { headers: runtimeHeaders(props.project) });
      const payload = await response.json() as { variables?: EnvVariable[]; error?: string };
      if (!response.ok) throw new Error(payload.error ?? response.statusText);
      const next = payload.variables ?? [];
      setEnvVars(next);
      // Keep the .env text editor in sync with the server set when the user
      // hasn't started a local edit yet (or after save / open of .env mode).
      if (!envPasteDirtyRef.current) {
        setEnvPasteText(envVariablesToDotEnv(next));
      }
    } catch (error) {
      setEnvStatus(error instanceof TypeError ? "Runtime offline" : error instanceof Error ? error.message : "Could not load environment variables");
    } finally {
      setEnvLoading(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.project.id]);

  useEffect(() => {
    if (activeSection === "environment") void loadEnvVars();
  }, [activeSection, loadEnvVars]);

  const loadMembers = useCallback(async () => {
    setMembersLoading(true);
    setMemberStatus("");
    try {
      const payload = await fetchProjectMembers(props.project);
      setMembers(payload.members);
      setInvitations(payload.invitations);
    } catch (error) {
      setMemberStatus(error instanceof Error ? error.message : "Could not load members");
    } finally {
      setMembersLoading(false);
    }
  }, [props.project]);

  useEffect(() => {
    if (activeSection === "members") void loadMembers();
  }, [activeSection, loadMembers]);

  const inviteMember = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!inviteEmail.trim()) return;
    setInviteSaving(true);
    setMemberStatus("");
    try {
      await inviteProjectMember(props.project, inviteEmail, inviteRole);
      setInviteEmail("");
      setInviteRole("dev");
      setMemberStatus("Invitation saved");
      await loadMembers();
    } catch (error) {
      setMemberStatus(error instanceof Error ? error.message : "Could not invite member");
    } finally {
      setInviteSaving(false);
    }
  };

  const saveEnvVar = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = envName.trim();
    if (!name) return;
    const endpoint = envEndpoint();
    if (!endpoint) {
      setEnvStatus("Runtime offline");
      return;
    }
    setEnvSaving(true);
    setEnvStatus("");
    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: runtimeHeaders(props.project, { "content-type": "application/json" }),
        body: JSON.stringify({ name, value: envValue }),
      });
      const payload = await response.json() as { error?: string };
      if (!response.ok) throw new Error(payload.error ?? response.statusText);
      setEnvName("");
      setEnvValue("");
      setEnvStatus(`${name} saved`);
      await loadEnvVars();
    } catch (error) {
      setEnvStatus(error instanceof TypeError ? "Runtime offline" : error instanceof Error ? error.message : "Could not save environment variable");
    } finally {
      setEnvSaving(false);
    }
  };

  const saveEnvBulk = async () => {
    const endpoint = envEndpoint();
    if (!endpoint) {
      setEnvStatus("Runtime offline");
      return;
    }
    setEnvSaving(true);
    setEnvStatus("");
    try {
      const response = await fetch(endpoint, {
        method: "PUT",
        headers: runtimeHeaders(props.project, { "content-type": "application/json" }),
        body: JSON.stringify({ content: envPasteText }),
      });
      const payload = await response.json() as { error?: string; count?: number };
      if (!response.ok) throw new Error(payload.error ?? response.statusText);
      setEnvStatus(`Saved ${payload.count ?? 0} variable${payload.count === 1 ? "" : "s"} from .env text`);
      envPasteDirtyRef.current = false;
      await loadEnvVars();
    } catch (error) {
      setEnvStatus(error instanceof TypeError ? "Runtime offline" : error instanceof Error ? error.message : "Could not save environment variables");
    } finally {
      setEnvSaving(false);
    }
  };

  const openEnvTextMode = () => {
    setEnvPasteMode(true);
    envPasteDirtyRef.current = false;
    setEnvPasteText(envVariablesToDotEnv(envVars));
  };

  const copyEnvText = async () => {
    const text = envPasteText.trim() ? envPasteText : envVariablesToDotEnv(envVars);
    if (!text) {
      setEnvStatus("No environment variables to copy");
      return;
    }
    await navigator.clipboard.writeText(text).catch(() => undefined);
    setEnvStatus("Copied .env text to clipboard");
  };

  const editEnvVar = (variable: EnvVariable) => {
    setEnvPasteMode(false);
    setEnvName(variable.name);
    setEnvValue(variable.value ?? "");
    setEnvStatus(variable.sensitive ? "Sensitive values stay masked. Paste a new value to replace it." : "");
  };

  const deleteEnvVar = async (name: string) => {
    const endpoint = envEndpoint();
    if (!endpoint) {
      setEnvStatus("Runtime offline");
      return;
    }
    setEnvSaving(true);
    setEnvStatus("");
    try {
      const response = await fetch(endpoint, {
        method: "DELETE",
        headers: runtimeHeaders(props.project, { "content-type": "application/json" }),
        body: JSON.stringify({ name }),
      });
      const payload = await response.json() as { error?: string };
      if (!response.ok) throw new Error(payload.error ?? response.statusText);
      setEnvStatus(`${name} deleted`);
      await loadEnvVars();
    } catch (error) {
      setEnvStatus(error instanceof TypeError ? "Runtime offline" : error instanceof Error ? error.message : "Could not delete environment variable");
    } finally {
      setEnvSaving(false);
    }
  };

  return (
    <div className="settings-shell">
      <aside className="settings-nav" aria-label="Settings sections">
        <Button
          data-active={activeSection === "general" ? "true" : undefined}
          onPress={() => setActiveSection("general")}
          variant={activeSection === "general" ? "secondary" : "ghost"}
        >
          General
        </Button>
        <Button
          data-active={activeSection === "database" ? "true" : undefined}
          onPress={() => setActiveSection("database")}
          variant={activeSection === "database" ? "secondary" : "ghost"}
        >
          Database
        </Button>
        <Button
          data-active={activeSection === "connection" ? "true" : undefined}
          onPress={() => setActiveSection("connection")}
          variant={activeSection === "connection" ? "secondary" : "ghost"}
        >
          Connection
        </Button>
        <Button
          data-active={activeSection === "environment" ? "true" : undefined}
          onPress={() => setActiveSection("environment")}
          variant={activeSection === "environment" ? "secondary" : "ghost"}
        >
          Environment Variables
        </Button>
        <Button
          data-active={activeSection === "members" ? "true" : undefined}
          onPress={() => setActiveSection("members")}
          variant={activeSection === "members" ? "secondary" : "ghost"}
        >
          Members
        </Button>
        <Button
          data-active={activeSection === "authentication" ? "true" : undefined}
          onPress={() => setActiveSection("authentication")}
          variant={activeSection === "authentication" ? "secondary" : "ghost"}
        >
          Authentication
        </Button>
      </aside>

      <section className="settings-panel" aria-live="polite">
        {activeSection === "general" ? (
          <SettingsCard title="General" description="Local deployment details used by gonvex dev and the runtime.">
            <form className="project-name-form" onSubmit={saveProjectName}>
              <label className="setting-field">
                <span>Project name</span>
                <input
                  autoComplete="off"
                  className="table-search"
                  onChange={(event) => setProjectName(event.target.value)}
                  value={projectName}
                />
              </label>
              <Button
                isDisabled={projectNameSaving || !projectName.trim() || projectName.trim() === props.project.name}
                type="submit"
                variant="primary"
              >
                {projectNameSaving ? "Saving" : "Save name"}
              </Button>
            </form>
            {projectNameStatus ? <p className="settings-note" role="status">{projectNameStatus}</p> : null}
            <div className="settings-grid">
              <SettingField label="Project ID" value={props.project.id} />
              <SettingField label="Runtime URL" value={runtimeURL} />
              <SettingField label="Dev script" value={devScript} />
              <SettingField label="Database" value={props.project.database} />
            </div>
          </SettingsCard>
        ) : null}

        {activeSection === "connection" ? (
          <SettingsCard title="Connection" description="Use these values to connect a local app/runtime to this project.">
            <div className="settings-grid">
              <SettingField label="Dashboard URL" value={dashboardURL} />
              <SettingField label="Runtime URL" value={runtimeURL} />
              <SettingField label="Project ID" value={props.project.id} />
              <SettingField label="Project header" value={`x-gonvex-project-id: ${props.project.id}`} />
              <SettingField label="App env" value={`VITE_GONVEX_PROJECT_ID=${props.project.id}`} />
              <SettingField label="Runtime env" value={`GONVEX_PROJECT=${props.project.id}`} />
            </div>
            <div className="project-key-block">
              <span>Project key</span>
              {projectKey ? (
                <textarea readOnly value={projectEnvSnippet} />
              ) : (
                <output data-muted="true">Hidden until revealed</output>
              )}
              <div className="project-tile-actions">
                <Button isDisabled={projectKeyLoading} onPress={revealProjectKey} variant="secondary">
                  {projectKeyLoading ? "Revealing" : projectKey ? "Refresh key" : "Reveal key"}
                </Button>
                {projectKey ? <Button onPress={copyProjectKey} variant="ghost">Copy env</Button> : null}
              </div>
              {projectKeyStatus ? <p className="settings-note">{projectKeyStatus}</p> : null}
            </div>
          </SettingsCard>
        ) : null}

        {activeSection === "database" ? (
          <SettingsCard title="Database Structure" description="Choose whether this project stores app data in one project database or splits it across landlord and tenant databases.">
            <div className="settings-grid">
              <AppSelect
                ariaLabel="Database structure"
                className="setting-field"
                label="Structure"
                selectedKey={props.databaseMode}
                onChange={(value) => props.onDatabaseModeChange(value as DatabaseMode)}
                options={[
                  { value: "single", label: "Single project database" },
                  { value: "multiTenant", label: "Landlord + tenant databases" },
                ]}
              />
              <SettingField
                label="Data browser"
                value={props.databaseMode === "multiTenant" ? "Shows landlord/project DB plus tenant DB selector" : "Shows project DB only"}
              />
              <label className="setting-field setting-checkbox-field">
                <span>Tenant selector</span>
                <Checkbox.Root
                  className="filter-checkbox setting-checkbox"
                  isSelected={props.hideTestTenants}
                  onChange={props.onHideTestTenantsChange}
                >
                  <Checkbox.Control><Checkbox.Indicator /></Checkbox.Control>
                  <Checkbox.Content>Hide test and internal tenant DBs</Checkbox.Content>
                </Checkbox.Root>
              </label>
              <SettingField label="Landlord DB" value={props.project.database} />
            </div>
            <p className="settings-note">
              Tenant databases are created and listed by the runtime. The Data tab sends the selected tenant id to the runtime when browsing rows.
            </p>
          </SettingsCard>
        ) : null}

        {activeSection === "environment" ? (
          <SettingsCard title="Environment Variables" description={`Project environment variables for ${props.project.id}.`}>
            <div className="env-mode-toggle" role="tablist" aria-label="Env editor mode">
              <Button size="sm" type="button" variant={envPasteMode ? "secondary" : "ghost"} onPress={openEnvTextMode}>
                .env text
              </Button>
              <Button size="sm" type="button" variant={envPasteMode ? "ghost" : "secondary"} onPress={() => setEnvPasteMode(false)}>
                Single variable
              </Button>
            </div>
            {envPasteMode ? (
              <div className="env-paste">
                <label className="setting-field env-value-field">
                  <span>Project .env — one <code>KEY=VALUE</code> per line (copy, edit, or paste a full file)</span>
                  <textarea
                    className="env-paste-textarea"
                    onChange={(event) => {
                      envPasteDirtyRef.current = true;
                      setEnvPasteText(event.target.value);
                    }}
                    placeholder={"# Loading… or paste KEY=VALUE lines\nGOOGLE_MAPS_API_KEY=...\nRESEND_KEY=..."}
                    spellCheck={false}
                    value={envPasteText}
                  />
                </label>
                <div className="env-actions">
                  <Button isDisabled={envSaving || !envPasteText.trim()} type="button" variant="primary" onPress={saveEnvBulk}>
                    {envSaving ? "Saving" : "Save .env"}
                  </Button>
                  <Button type="button" variant="secondary" onPress={copyEnvText}>
                    Copy
                  </Button>
                  <Button
                    isDisabled={envLoading}
                    type="button"
                    variant="ghost"
                    onPress={() => {
                      envPasteDirtyRef.current = false;
                      void loadEnvVars();
                    }}
                  >
                    Refresh
                  </Button>
                </div>
                <p className="settings-note">
                  Saving <strong>replaces</strong> the entire env set for this project with the text above.
                  Values are scoped to this Gonvex project and available to functions through the runtime context.
                </p>
              </div>
            ) : (
              <>
              <form className="env-form" onSubmit={saveEnvVar}>
                <label className="setting-field">
                  <span>Name</span>
                  <input
                    autoComplete="off"
                    onChange={(event) => setEnvName(event.target.value)}
                    placeholder="GOOGLE_MAPS_API_KEY"
                    value={envName}
                  />
                </label>
                <label className="setting-field env-value-field">
                  <span>Value</span>
                  <textarea
                    onChange={(event) => setEnvValue(event.target.value)}
                    placeholder="whagons-5"
                    spellCheck={false}
                    value={envValue}
                  />
                </label>
                <div className="env-actions">
                  <Button isDisabled={envSaving || !envName.trim()} type="submit" variant="primary">
                    {envSaving ? "Saving" : "Save Variable"}
                  </Button>
                  <Button isDisabled={envLoading} type="button" variant="ghost" onPress={loadEnvVars}>
                    Refresh
                  </Button>
                </div>
              </form>
            <p className="settings-note">
              These values are scoped to this Gonvex project and are available to functions through the runtime context.
            </p>
            <div className="env-table" role="table" aria-label="Environment variables">
              <div className="env-row env-row--head" role="row">
                <span role="columnheader">Name</span>
                <span role="columnheader">Value</span>
                <span role="columnheader">Source</span>
                <span role="columnheader">Actions</span>
              </div>
              {envVars.length === 0 ? (
                <div className="env-empty">{envLoading ? "Loading variables" : "No runtime variables found"}</div>
              ) : envVars.map((variable) => (
                <div className="env-row" role="row" key={variable.name}>
                  <code role="cell">{variable.name}</code>
                  <span role="cell">{variable.sensitive ? variable.masked : variable.value ?? variable.masked}</span>
                  <span role="cell">{variable.source}</span>
                  <div className="env-row-actions" role="cell">
                    <Button size="sm" type="button" variant="ghost" onPress={() => editEnvVar(variable)}>
                      Edit
                    </Button>
                    <Button className="env-delete-button" size="sm" type="button" variant="ghost" onPress={() => void deleteEnvVar(variable.name)}>
                      Delete
                    </Button>
                  </div>
                </div>
              ))}
            </div>
              </>
            )}
            {envStatus ? <p className="settings-note">{envStatus}</p> : null}
          </SettingsCard>
        ) : null}

        {activeSection === "members" ? (
          <SettingsCard title="Members" description="Invite users who may access this project. Users can sign in only after an admin creates their account.">
            <form className="env-form" onSubmit={inviteMember}>
              <label className="setting-field">
                <span>Email</span>
                <input
                  autoComplete="email"
                  className="table-search"
                  onChange={(event) => setInviteEmail(event.target.value)}
                  placeholder="teammate@example.com"
                  type="email"
                  value={inviteEmail}
                />
              </label>
              <AppSelect
                ariaLabel="Project role"
                className="setting-field"
                label="Project role"
                selectedKey={inviteRole}
                onChange={(value) => setInviteRole(value as ProjectMember["role"])}
                options={[
                  { value: "admin", label: "Admin", description: "Manage project settings and members" },
                  { value: "dev", label: "Developer", description: "Use project runtime tools" },
                  { value: "viewer", label: "Viewer", description: "Read-only dashboard access" },
                ]}
              />
              <div className="env-actions">
                <Button isDisabled={inviteSaving || !inviteEmail.trim()} type="submit" variant="primary">
                  {inviteSaving ? "Inviting" : "Invite member"}
                </Button>
                <Button isDisabled={membersLoading} type="button" variant="ghost" onPress={loadMembers}>
                  Refresh
                </Button>
              </div>
            </form>
            <div className="env-table" role="table" aria-label="Project members">
              <div className="env-row env-row--head" role="row">
                <span role="columnheader">Email</span>
                <span role="columnheader">Name</span>
                <span role="columnheader">Role</span>
                <span role="columnheader">State</span>
              </div>
              {members.length === 0 ? (
                <div className="env-empty">{membersLoading ? "Loading members" : "No members found"}</div>
              ) : members.map((member) => (
                <div className="env-row" role="row" key={member.email}>
                  <code role="cell">{member.email}</code>
                  <span role="cell">{member.name}</span>
                  <span role="cell">{member.role}</span>
                  <span role="cell">active</span>
                </div>
              ))}
              {invitations.filter((invitation) => !invitation.accepted).map((invitation) => (
                <div className="env-row" role="row" key={invitation.id}>
                  <code role="cell">{invitation.email}</code>
                  <span role="cell">Pending user</span>
                  <span role="cell">{invitation.role}</span>
                  <span role="cell">invited</span>
                </div>
              ))}
            </div>
            {memberStatus ? <p className="settings-note">{memberStatus}</p> : null}
          </SettingsCard>
        ) : null}

        {activeSection === "authentication" ? (
          <SettingsCard
            title="Authentication Configuration"
            description="These are the authentication providers configured for this deployment."
          >
            <div className="auth-provider-grid">
              <SettingField label="Broker callback" value={`${runtimeURLForProject(props.project)}/auth/google/callback`} />
              <SettingField label="Application ID" value={props.project.id} />
              <SettingField label="Provider" value="Google OpenID Connect" muted />
            </div>
            <Separator />
            <div className="auth-provider-grid auth-provider-grid--wide">
              <SettingField label="Access session" value="15 minutes" />
              <SettingField label="Refresh session" value="30 days, rotating" />
              <SettingField label="Browser flow" value="Authorization Code + PKCE" />
              <SettingField label="Identity validation" value="RS256, issuer, audience, nonce, verified email" />
            </div>
            <p className="settings-note">Use <code>gonvex auth status</code> or <code>gonvex auth doctor</code> for live configuration checks.</p>
          </SettingsCard>
        ) : null}
      </section>
    </div>
  );
}

function SettingsCard(props: { title: string; description: string; children: ReactNode }) {
  return (
    <Card className="settings-card" variant="default">
      <Card.Header className="settings-card-header">
        <Card.Title>{props.title}</Card.Title>
        <p>{props.description}</p>
      </Card.Header>
      <Card.Content className="settings-card-content">{props.children}</Card.Content>
    </Card>
  );
}

function SettingField(props: { label: string; value: string; muted?: boolean }) {
  return (
    <label className="setting-field">
      <span>{props.label}</span>
      <output data-muted={props.muted ? "true" : undefined}>{props.value}</output>
    </label>
  );
}

function GridCard(props: { title: string; eyebrow: string; chip: string; children: ReactNode }) {
  return (
    <Card className="grid-panel" variant="default" aria-labelledby="grid-title">
      <Card.Header className="panel-heading">
        <div>
          <p className="eyebrow">{props.eyebrow}</p>
          <Card.Title id="grid-title">{props.title}</Card.Title>
        </div>
        <Chip color="default" size="sm" variant="secondary">
          {props.chip}
        </Chip>
      </Card.Header>
      <Separator />
      <Card.Content className="grid-content">{props.children}</Card.Content>
    </Card>
  );
}

function ListCard(props: { title: string; rows: string[][]; large?: boolean }) {
  return (
    <Card className={props.large ? "list-card list-card--large" : "list-card"} variant="default">
      <Card.Header className="list-card-heading">
        <Card.Title>{props.title}</Card.Title>
      </Card.Header>
      <Card.Content>
        {props.rows.map(([label, value]) => (
          <div className="kv-row" key={label}>
            <span>{label}</span>
            <strong>{value}</strong>
          </div>
        ))}
      </Card.Content>
    </Card>
  );
}
