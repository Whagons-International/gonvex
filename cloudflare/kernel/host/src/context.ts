// The `ctx` object a handler receives, ported from
// rust/crates/module-runtime-v8/src/gonvex_bootstrap.js (createContext, BS:549-734).
// Capability separation is structural: a method the invocation was not granted
// is absent. Every method lowers to exactly the host-call payload the Rust
// bootstrap sends, so host handlers can be shared with any other placement.
import type { Capabilities, HostCallKind } from "./capabilities.js";
import type { FunctionKind } from "./globals.js";

export type HostPayload = { kind: HostCallKind } & Record<string, unknown>;

/** The ordered op the context calls (op_gonvex_host_call after JSON decoding). */
export type HostOp = (payload: HostPayload) => Promise<unknown>;

export class GonvexHostError extends Error {
  readonly status: "denied" | "failed";
  readonly code: string;
  constructor(message: string, status: "denied" | "failed") {
    super(message);
    this.name = status === "denied" ? "GonvexCapabilityError" : "GonvexHostError";
    this.status = status;
    this.code = String(message).includes("STALE_AGENT_CATALOG:") ? "STALE_AGENT_CATALOG" : status === "denied" ? "CAPABILITY_DENIED" : "HOST_CALL_FAILED";
  }
}

export interface AccountIdentity { id: string; email?: string | null; name?: string | null; avatarUrl?: string | null }
export interface MemberIdentity { id: string; accountId?: string; status?: string | null; role?: string | null; displayName?: string | null; permissions?: unknown }
export interface TenantIdentity { id: string; projectId?: string; name?: string | null }

export interface Identity {
  account?: AccountIdentity | null;
  member?: MemberIdentity | null;
  tenant?: TenantIdentity | null;
  developerSession?: Record<string, unknown> | null;
}

/** `InvocationInfo` (frozen on ctx.invocation). */
export interface InvocationInfo {
  channel?: string | null;
  rootChannel?: string | null;
  commandId?: string | null;
  rootCommandId?: string | null;
  artifactHash?: string | null;
  actorAccountId?: string | null;
  actorMemberId?: string | null;
  onBehalfOfMemberId?: string | null;
  delegation?: unknown;
  [key: string]: unknown;
}

/** What `createContext` needs: the request the Rust dispatcher receives as JSON. */
export interface ContextRequest {
  kind: FunctionKind;
  function: string;
  now: number;
  identity: Identity;
  invocation: InvocationInfo;
  intentEntropy?: string | undefined;
  invitationInviter?: { permissions?: Record<string, unknown> } & Record<string, unknown> | undefined;
  capabilities: Capabilities;
  actionTools?: readonly string[] | undefined;
  environment?: Readonly<Record<string, string>> | undefined;
}

const text = (name: string, value: unknown): string => {
  if (typeof value !== "string" || value.length === 0) throw new TypeError(`${name} must be a non-empty string`);
  return value;
};
const plainObject = (name: string, value: unknown): Record<string, unknown> => {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`${name} must be an object`);
  return value as Record<string, unknown>;
};
const rowKey = (value: unknown): string | number => {
  if (typeof value === "string" || typeof value === "number") return value;
  throw new TypeError("row id must be a string or a number");
};
const optional = (value: unknown) => (value === undefined ? null : value);
const nonNegativeInteger = (name: string, value: unknown): number => {
  if (!Number.isSafeInteger(value) || (value as number) < 0) throw new TypeError(`${name} must be a non-negative safe integer`);
  return value as number;
};
const parameterList = (parameters: unknown): unknown[] => {
  if (parameters === undefined || parameters === null) return [];
  if (!Array.isArray(parameters)) throw new TypeError("query parameters must be an array");
  return [...parameters];
};

type FetchInit = { method?: string; headers?: unknown; body?: unknown; signal?: AbortSignal | null };

function requestInit(input: unknown, init: FetchInit | undefined) {
  const url = typeof input === "string" ? input : String((input as { href?: unknown } | null)?.href ?? input ?? "");
  const options = init ?? {};
  options.signal?.throwIfAborted?.();
  const headers: Record<string, string> = {};
  const source = options.headers as { entries?: () => Iterable<[unknown, unknown]> } | Record<string, unknown> | undefined;
  if (source && typeof (source as { entries?: unknown }).entries === "function") {
    for (const [name, value] of (source as { entries: () => Iterable<[unknown, unknown]> }).entries()) headers[String(name).toLowerCase()] = String(value);
  } else if (source && typeof source === "object") {
    for (const [name, value] of Object.entries(source)) headers[String(name).toLowerCase()] = String(value);
  }
  let body: string | null = null;
  if (options.body !== undefined && options.body !== null) {
    if (typeof options.body === "string") body = options.body;
    else if (typeof options.body === "object") {
      body = JSON.stringify(options.body);
      if (headers["content-type"] === undefined) headers["content-type"] = "application/json";
    } else body = String(options.body);
  }
  return { url: text("fetch url", url), method: String(options.method ?? "GET").toUpperCase(), headers, body };
}

interface RawResponse { status?: number; statusText?: string; url?: string; headers?: Record<string, string>; body?: string; stream?: number }

function createResponse(raw: RawResponse | null | undefined, op: HostOp): Response {
  const encoder = new TextEncoder();
  const stream = raw?.stream;
  const body = typeof stream === "number"
    ? new ReadableStream<Uint8Array>({
      async pull(controller) {
        for (;;) {
          const next = (await op({ kind: "fetchRead", stream })) as { done?: boolean; chunk?: string } | null;
          if (next?.done) { controller.close(); return; }
          if (next?.chunk) { controller.enqueue(encoder.encode(next.chunk)); return; }
        }
      },
      cancel() { return op({ kind: "fetchCancel", stream }).then(() => undefined, () => undefined); },
    })
    : typeof raw?.body === "string" ? raw.body : "";
  // The Rust bootstrap has its own Response; workerd's refuses status 0 and
  // null-body statuses with a body, so those are carried on the object only.
  const status = raw?.status ?? 0;
  const nullBody = status === 101 || status === 204 || status === 205 || status === 304;
  const response = new Response(nullBody ? null : body, { status: status >= 200 && status <= 599 ? status : 200, statusText: raw?.statusText ?? "", headers: raw?.headers ?? {} });
  if (response.status !== status) Object.defineProperty(response, "status", { value: status });
  Object.defineProperty(response, "url", { value: raw?.url ?? "" });
  return response;
}

/** Builds the frozen `ctx` for one invocation. */
export function createContext(request: ContextRequest, op: HostOp): Readonly<Record<string, unknown>> {
  const granted = request.capabilities;
  const identity = request.identity ?? {};
  const account = identity.account ?? null;
  const context: Record<string, unknown> = {
    kind: request.kind,
    function: request.function,
    now: request.now,
    auth: Object.freeze({ account, developerSession: identity.developerSession ? Object.freeze({ ...identity.developerSession }) : null }),
    tenant: identity.tenant ?? null,
    member: identity.member ?? null,
    invocation: Object.freeze({ ...(request.invocation ?? {}) }),
  };
  if (request.kind === "reducer" && request.intentEntropy) context["intentEntropy"] = request.intentEntropy;
  if (request.kind === "reducer" && request.invitationInviter) {
    context["invitationInviter"] = Object.freeze({ ...request.invitationInviter, permissions: Object.freeze({ ...request.invitationInviter.permissions }) });
  }

  const db: Record<string, unknown> = {};
  const query = (statement: unknown, parameters?: unknown, catalog?: string) =>
    op({ kind: "dbQuery", statement: text("statement", statement), parameters: parameterList(parameters), ...(catalog ? { catalog } : {}) });
  if (granted.dbRead) db["query"] = (statement: unknown, parameters?: unknown) => query(statement, parameters);
  if (granted.dbWrite) {
    const records = new Map<string, Promise<unknown>>();
    let hasRecords: Promise<unknown> | undefined;
    // The two statements are the bootstrap's own (BS:575-585). They carry a
    // `catalog` hint so the database host may answer them from its catalog
    // cache; the result and the aborted-transaction rule are unchanged.
    db["recordColumns"] = (table: unknown) => {
      const name = text("table", table);
      if (!records.has(name)) records.set(name, (async () => {
        hasRecords ??= query("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_gonvex_record_views' AND type='table') AS present", [], "recordViewsPresent");
        const present = (await hasRecords) as { present?: unknown }[];
        if (!present[0]?.present) return undefined;
        const [record] = (await query("SELECT schema FROM _gonvex_record_views WHERE name=$1", [name], "recordViewSchema")) as { schema: string }[];
        return record ? (JSON.parse(record.schema) as { columns: unknown }).columns : undefined;
      })());
      return records.get(name);
    };
    db["insert"] = (table: unknown, row: unknown, allocation?: { generatedId?: unknown }) =>
      op({ kind: "dbInsert", table: text("table", table), row: plainObject("row", row), generatedId: allocation?.generatedId ?? null });
    db["update"] = (table: unknown, id: unknown, patch: unknown) => op({ kind: "dbUpdate", table: text("table", table), id: rowKey(id), patch: plainObject("patch", patch) });
    db["delete"] = (table: unknown, id: unknown) => op({ kind: "dbDelete", table: text("table", table), id: rowKey(id) });
    db["deleteMany"] = (table: unknown, ids: Iterable<unknown> | null | undefined) => op({ kind: "dbDeleteMany", table: text("table", table), ids: Array.from(ids ?? [], rowKey) });
  }
  if (granted.dbRead || granted.dbWrite) context["db"] = Object.freeze(db);
  if (granted.dbRead) {
    context["permissions"] = Object.freeze({
      has: (permission: unknown, scope?: unknown) => op({ kind: "permissionsHas", permission: text("permission", permission), scope: scope === undefined || scope === null ? "" : String(scope) }),
    });
  }
  if (granted.actionOutbox) {
    context["actions"] = Object.freeze({ enqueue: (name: unknown, args?: unknown) => op({ kind: "actionEnqueue", function: text("Action", name), args: optional(args) }) });
  }
  if (granted.actionTools) {
    const tools: Record<string, unknown> = {};
    for (const name of request.actionTools ?? []) tools[name] = (args?: unknown) => op({ kind: "toolInvoke", tool: name, args: optional(args) });
    context["tools"] = Object.freeze(tools);
  }
  if (granted.functions) {
    context["functions"] = Object.freeze({
      invoke: (input: unknown) => {
        const call = plainObject("function invocation", input);
        return op({ kind: "functionInvoke", path: text("function path", call["path"]), args: optional(call["args"]), artifactHash: text("artifact hash", call["artifactHash"]) });
      },
    });
  }
  if (granted.scheduler) {
    context["scheduler"] = Object.freeze({
      runAfter: (delayMs: unknown, name: unknown, args?: unknown) => op({ kind: "scheduleAfter", delayMs: nonNegativeInteger("scheduler delayMs", delayMs), function: text("scheduled function", name), args: optional(args) }),
      runAt: (atUnixMs: unknown, name: unknown, args?: unknown) => op({ kind: "scheduleAt", atUnixMs: nonNegativeInteger("scheduler atUnixMs", atUnixMs), function: text("scheduled function", name), args: optional(args) }),
      wakeAt: (atUnixMs: unknown, name: unknown, args?: unknown) => op({ kind: "scheduleAt", atUnixMs: nonNegativeInteger("scheduler atUnixMs", atUnixMs), function: text("scheduled function", name), args: optional(args), coalesce: true }),
    });
  }
  if (granted.network) {
    context["fetch"] = async (input: unknown, init?: FetchInit) => {
      const fetchRequest = requestInit(input, init);
      const signal = init?.signal;
      if (!signal) return createResponse((await op({ kind: "fetch", request: fetchRequest })) as RawResponse, op);
      let onAbort: (() => void) | undefined;
      const aborted = new Promise<never>((_, reject) => {
        onAbort = () => reject(signal.reason ?? new Error("This operation was aborted"));
        signal.addEventListener("abort", onAbort, { once: true });
      });
      try {
        return createResponse((await Promise.race([op({ kind: "fetch", request: fetchRequest }), aborted])) as RawResponse, op);
      } finally {
        if (onAbort) signal.removeEventListener("abort", onAbort);
      }
    };
  }
  if (granted.secrets) context["secrets"] = Object.freeze({ ...(request.environment ?? {}) });
  if (granted.storage) {
    const storage = (operation: string, payload: unknown) => op({ kind: "storage", operation, payload: optional(payload) });
    const metadata = { getMetadata: (fileId: unknown) => storage("getMetadata", { fileId: text("fileId", fileId) }) };
    context["storage"] = Object.freeze(request.kind === "reducer" ? metadata : {
      ...metadata,
      generateUploadUrl: (options?: unknown) => storage("generateUploadUrl", options ?? {}),
      getUrl: (fileId: unknown) => storage("getUrl", { fileId: text("fileId", fileId) }),
      generateDownloadUrl: (fileId: unknown, ttlMs?: unknown) => storage("generateDownloadUrl", { fileId: text("fileId", fileId), ttlMs: ttlMs ?? 0 }),
      delete: (fileId: unknown) => storage("delete", { fileId: text("fileId", fileId) }),
      store: (contentBase64: unknown, options?: Record<string, unknown>) => storage("store", { contentBase64: text("content", contentBase64), ...(options ?? {}) }),
      call: (operation: unknown, payload?: unknown) => storage(text("operation", operation), payload),
    });
  }
  if (granted.sandbox) {
    const sandbox = (operation: string, payload: unknown) => op({ kind: "sandbox", operation, payload: optional(payload) });
    const id = (value: unknown) => text("sandbox id", value);
    context["sandbox"] = Object.freeze({
      create: (options: unknown = {}) => sandbox("create", options),
      destroy: (sandboxId: unknown) => sandbox("destroy", { sandboxId: id(sandboxId) }),
      run: (sandboxId: unknown, options?: Record<string, unknown>) => sandbox("run", { sandboxId: id(sandboxId), ...(options ?? {}) }),
      cancel: (sandboxId: unknown, executionId: unknown) => sandbox("cancel", { sandboxId: id(sandboxId), executionId: text("execution id", executionId) }),
      status: (sandboxId: unknown, executionId: unknown) => sandbox("status", { sandboxId: id(sandboxId), executionId: text("execution id", executionId) }),
      readFile: (sandboxId: unknown, path: unknown) => sandbox("readFile", { sandboxId: id(sandboxId), path: text("sandbox path", path) }),
      writeFile: (sandboxId: unknown, path: unknown, contentBase64: unknown) => sandbox("writeFile", { sandboxId: id(sandboxId), path: text("sandbox path", path), contentBase64: text("sandbox file contents", contentBase64) }),
      readText: (sandboxId: unknown, path: unknown) => sandbox("readText", { sandboxId: id(sandboxId), path: text("sandbox path", path) }),
      writeText: (sandboxId: unknown, path: unknown, content: unknown) => sandbox("writeText", { sandboxId: id(sandboxId), path: text("sandbox path", path), content: String(content) }),
      importFile: (sandboxId: unknown, options?: Record<string, unknown>) => sandbox("importFile", { sandboxId: id(sandboxId), ...(options ?? {}) }),
      __worker: Object.freeze({
        readText: (path: unknown) => sandbox("worker.readText", { path: text("sandbox path", path) }),
        writeText: (path: unknown, content: unknown) => sandbox("worker.writeText", { path: text("sandbox path", path), content: String(content) }),
        listFiles: () => sandbox("worker.listFiles", {}),
        query: (statement: unknown, parameters?: unknown) => sandbox("worker.query", { statement: text("DuckDB statement", statement), parameters: parameterList(parameters) }),
        register: (name: unknown, rows: unknown) => sandbox("worker.register", { name: text("DuckDB table", name), rows }),
      }),
    });
  }
  return Object.freeze(context);
}

/** `resolveHandler` (BS:742-764): the export shapes a binding may take. */
export function resolveHandler(binding: unknown, functionPath: string): ((ctx: unknown, args: unknown) => unknown) | null {
  if (typeof binding === "function") return binding as (ctx: unknown, args: unknown) => unknown;
  if (binding !== null && typeof binding === "object") {
    const b = binding as Record<string, unknown>;
    if (typeof b["runtimeRegistrations"] === "function") {
      try {
        const registrations = (b["runtimeRegistrations"] as () => { path?: string; handler?: unknown }[])();
        const registration = registrations.find((candidate) => candidate && candidate.path === functionPath);
        if (registration && typeof registration.handler === "function") return registration.handler as (ctx: unknown, args: unknown) => unknown;
      } catch { /* fall through to the ordinary shapes */ }
    }
    if (typeof b["handler"] === "function") return b["handler"] as (ctx: unknown, args: unknown) => unknown;
    if (typeof b["run"] === "function") return b["run"] as (ctx: unknown, args: unknown) => unknown;
    const options = b["options"] as Record<string, unknown> | null | undefined;
    if (options !== null && typeof options === "object" && typeof options["run"] === "function") return options["run"] as (ctx: unknown, args: unknown) => unknown;
    if (typeof b["default"] === "function") return b["default"] as (ctx: unknown, args: unknown) => unknown;
  }
  return null;
}
