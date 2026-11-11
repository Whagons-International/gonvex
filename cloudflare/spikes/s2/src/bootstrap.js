// Module contract for Queries, Reducers and (for the runtime-contract vectors)
// Actions: a port of the parts of rust/crates/module-runtime-v8/src/
// gonvex_bootstrap.js that build `ctx` and dispatch one invocation. Context
// shapes, host-call payloads, error envelopes and result encoding follow that
// file. `hostCall(payload)` is the host: it answers `{ status, value, message }`
// exactly like `op_gonvex_host_call`, except that it returns the decoded value.
import { validateValue } from "./local-runtime/validation.js";
import { withInvocation } from "./runtime-globals.js";

/** Same per-kind ceiling as rust/crates/module-runtime/src/capabilities.rs. */
export const structuralCapabilities = {
  query: { dbRead: true },
  reducer: { dbRead: true, dbWrite: true, actionOutbox: true, scheduler: true, storage: true },
  action: { actionTools: true, scheduler: true, network: true, storage: true, sandbox: true, secrets: true, functions: true },
};

export class GonvexHostError extends Error {
  constructor(message, status) {
    super(message);
    this.name = status === "denied" ? "GonvexCapabilityError" : "GonvexHostError";
    this.status = status;
    this.code = String(message).includes("STALE_AGENT_CATALOG:") ? "STALE_AGENT_CATALOG" : status === "denied" ? "CAPABILITY_DENIED" : "HOST_CALL_FAILED";
  }
}

const text = (name, value) => { if (typeof value !== "string" || value.length === 0) throw new TypeError(`${name} must be a non-empty string`); return value; };
const plainObject = (name, value) => { if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError(`${name} must be an object`); return value; };
const rowKey = (value) => { if (typeof value === "string" || typeof value === "number") return value; throw new TypeError("row id must be a string or a number"); };
const optional = (value) => (value === undefined ? null : value);
const nonNegativeInteger = (name, value) => { if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${name} must be a non-negative safe integer`); return value; };
const parameterList = (parameters) => { if (parameters === undefined || parameters === null) return []; if (!Array.isArray(parameters)) throw new TypeError("query parameters must be an array"); return [...parameters]; };

function createContext(request, hostCall) {
  const granted = request.capabilities;
  const identity = request.identity ?? {};
  const call = async (payload) => {
    const outcome = await hostCall(payload);
    if (outcome.status !== "ok") throw new GonvexHostError(outcome.message, outcome.status);
    return outcome.value;
  };
  const context = {
    kind: request.kind,
    function: request.function,
    now: request.now,
    auth: Object.freeze({ account: identity.account ?? null, developerSession: identity.developerSession ? Object.freeze({ ...identity.developerSession }) : null }),
    tenant: identity.tenant ?? null,
    member: identity.member ?? null,
    invocation: Object.freeze({ ...(request.invocation ?? {}) }),
    ...(request.kind === "reducer" && request.intentEntropy ? { intentEntropy: request.intentEntropy } : {}),
  };
  const db = {};
  if (granted.dbRead) {
    db.query = (statement, parameters) => call({ kind: "dbQuery", statement: text("statement", statement), parameters: parameterList(parameters) });
  }
  if (granted.dbWrite) {
    const records = new Map();
    let hasRecords;
    db.recordColumns = (table) => {
      text("table", table);
      if (!records.has(table)) records.set(table, (async () => {
        hasRecords ??= db.query("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_gonvex_record_views' AND type='table') AS present");
        if (!(await hasRecords)[0]?.present) return undefined;
        const [record] = await db.query("SELECT schema FROM _gonvex_record_views WHERE name=$1", [table]);
        return record ? JSON.parse(record.schema).columns : undefined;
      })());
      return records.get(table);
    };
    db.insert = (table, row, allocation) => call({ kind: "dbInsert", table: text("table", table), row: plainObject("row", row), generatedId: allocation?.generatedId ?? null });
    db.update = (table, id, patch) => call({ kind: "dbUpdate", table: text("table", table), id: rowKey(id), patch: plainObject("patch", patch) });
    db.delete = (table, id) => call({ kind: "dbDelete", table: text("table", table), id: rowKey(id) });
    db.deleteMany = (table, ids) => call({ kind: "dbDeleteMany", table: text("table", table), ids: Array.from(ids ?? [], rowKey) });
  }
  if (granted.dbRead || granted.dbWrite) context.db = Object.freeze(db);
  if (granted.dbRead) {
    context.permissions = Object.freeze({ has: (permission, scope) => call({ kind: "permissionsHas", permission: text("permission", permission), scope: scope === undefined || scope === null ? "" : String(scope) }) });
  }
  if (granted.actionOutbox) {
    context.actions = Object.freeze({ enqueue: (name, args) => call({ kind: "actionEnqueue", function: text("Action", name), args: optional(args) }) });
  }
  if (granted.scheduler) {
    context.scheduler = Object.freeze({
      runAfter: (delayMs, name, args) => call({ kind: "scheduleAfter", delayMs: nonNegativeInteger("scheduler delayMs", delayMs), function: text("scheduled function", name), args: optional(args) }),
      runAt: (atUnixMs, name, args) => call({ kind: "scheduleAt", atUnixMs: nonNegativeInteger("scheduler atUnixMs", atUnixMs), function: text("scheduled function", name), args: optional(args) }),
      wakeAt: (atUnixMs, name, args) => call({ kind: "scheduleAt", atUnixMs: nonNegativeInteger("scheduler atUnixMs", atUnixMs), function: text("scheduled function", name), args: optional(args), coalesce: true }),
    });
  }
  if (granted.network) {
    // Actions only (runtime-contract vectors). The host checks the origin.
    context.fetch = async (input, init) => {
      const url = typeof input === "string" ? input : String(input?.href ?? input ?? "");
      return call({ kind: "fetch", request: { url: text("fetch url", url), method: String(init?.method ?? "GET").toUpperCase(), headers: {}, body: null } });
    };
  }
  if (granted.storage) {
    const storage = (operation, payload) => call({ kind: "storage", operation, payload: optional(payload) });
    context.storage = Object.freeze({ getMetadata: (fileId) => storage("getMetadata", { fileId: text("fileId", fileId) }) });
  }
  return Object.freeze(context);
}

/** Same binding shapes as gonvex_bootstrap.js resolveHandler. */
export function resolveHandler(binding, functionPath) {
  if (typeof binding === "function") return binding;
  if (binding !== null && typeof binding === "object") {
    if (typeof binding.runtimeRegistrations === "function") {
      try {
        const registration = binding.runtimeRegistrations().find((candidate) => candidate && candidate.path === functionPath);
        if (registration && typeof registration.handler === "function") return registration.handler;
      } catch { /* fall through */ }
    }
    if (typeof binding.handler === "function") return binding.handler;
    if (typeof binding.run === "function") return binding.run;
    if (binding.options !== null && typeof binding.options === "object" && typeof binding.options.run === "function") return binding.options.run;
    if (typeof binding.default === "function") return binding.default;
  }
  return null;
}

const format = (value) => { if (typeof value === "string") return value; if (value instanceof Error) return value.stack || `${value.name}: ${value.message}`; try { const encoded = JSON.stringify(value); return encoded === undefined ? String(value) : encoded; } catch { return String(value); } };
const failure = (kind, message, stack) => (stack ? { status: "error", kind, message, stack } : { status: "error", kind, message });

/**
 * One invocation. `request` carries kind, function, now, intentEntropy,
 * identity, invocation metadata, granted capabilities and maxResultBytes.
 * Returns `{ status: "ok", value: <JSON text> }` or an error envelope, like
 * the Rust dispatcher. `argsSchema` is validated first, as the Rust engine does.
 */
export function dispatch(binding, request, args, hostCall, argsSchema) {
  const granted = request.capabilities ?? {};
  const ceiling = structuralCapabilities[request.kind] ?? {};
  const capabilities = Object.fromEntries(Object.keys(ceiling).map((name) => [name, ceiling[name] && granted[name] !== false]));
  const effective = { ...request, capabilities };
  return withInvocation({ kind: request.kind, now: request.now, intentEntropy: request.intentEntropy }, async () => {
    const handler = resolveHandler(binding, request.function);
    if (handler === null) return failure("dispatch", `module export for ${request.function} is not a callable ${request.kind} handler`);
    try { validateValue(argsSchema, args); } catch (cause) { return failure("arguments", `invalid arguments for ${request.function}: ${cause.message}`); }
    let value;
    try {
      value = await handler(createContext(effective, hostCall), args);
    } catch (cause) {
      const message = cause instanceof Error ? `${cause.name}: ${cause.message}` : format(cause);
      return failure("handler", message, cause instanceof Error ? cause.stack : undefined);
    }
    let encoded;
    try {
      encoded = value === undefined ? "null" : JSON.stringify(value);
      if (encoded === undefined) encoded = "null";
    } catch (cause) {
      return failure("result", `${request.function} returned a value that cannot be encoded as JSON: ${format(cause)}`);
    }
    if (encoded.length > (request.maxResultBytes ?? 8 * 1024 * 1024)) return failure("resultSize", `${request.function} returned more than the ${request.maxResultBytes} byte result limit`);
    return { status: "ok", value: encoded };
  });
}
