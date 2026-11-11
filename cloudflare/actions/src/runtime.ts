// Runs one Action: the Cloudflare counterpart of
// `Runtime::execute_tenant_action_with_access` (rust/crates/runtime/src/execution.rs)
// and `ActionHostCalls` (action_calls.rs), on K04's module engine.
//
// Order of checks, as in Rust: generation -> function (kind, internal) ->
// public access -> argument schema -> command id and provenance -> deadline
// (15 min; none for agent Actions; 60 s for anonymous callers) -> capability
// declaration -> project secrets -> engine (K04: budgets, sticky denials,
// result caps) with this file's host behind every Action op.
//
// Every error text is Rust's `ExecutionError` Display, so `action.error`
// frames are the same on both backends (conformance scenario `actions`).
import {
  HostFailure,
  hostErrors,
  ModuleError,
  SchemaError,
  validateValue,
  codes,
  classifyWire,
  isWireError,
  type ErrorClass,
  type FunctionContract,
  type Generation,
  type HostPayload,
  type ModuleHost,
  type ModuleRegistry,
} from "../../kernel/host/src/index.js";
import { ConcurrencyLimit, NetworkHost, type Fetcher } from "./fetch.js";
import type { ControlPort, SandboxPort, SchedulerPort, StoragePort, TenantActionPort, TenantCallReport } from "./ports.js";
import {
  delegatedProvenance,
  initialActionProvenance,
  persisted,
  publicInfo,
  toolProvenance,
  type Provenance,
} from "./provenance.js";
import { identityOf, isAnonymous, type ActionSession } from "./session.js";

export const MAX_INVOCATION_DEPTH = 8;
export const ACTION_TIMEOUT_MS = 15 * 60_000;
export const PUBLIC_ACTION_TIMEOUT_MS = 60_000;
/** The module host's transport slack (MH/session.rs:38): the outer public deadline fires first. */
const TRANSPORT_SLACK_MS = 5_000;

/** An `ExecutionError`: its Display text and its client class. */
export class ExecutionFailure extends Error {
  constructor(message: string, readonly errorClass: ErrorClass = "rejected") { super(message); }
}

const quote = (value: string) => JSON.stringify(value);

/** `ExecutionError` constructors, with Rust's Display. */
export const executionErrors = {
  moduleMissing: (project: string) => new ExecutionFailure(`project ${quote(project)} has no active TypeScript module`, "transient"),
  functionMissing: (path: string) => new ExecutionFailure(`function ${quote(path)} is not registered`),
  wrongKind: (path: string, expected: string, actual: string) => new ExecutionFailure(`function ${quote(path)} is a ${actual}, not a ${expected}`),
  internal: (path: string) => new ExecutionFailure(`internal function ${quote(path)} cannot be called by a client`),
  hostCall: (message: string, transient = false) => new ExecutionFailure(`module database capability failed: ${message}`, transient ? "transient" : "rejected"),
  capabilityUnavailable: (path: string, capability: string) => new ExecutionFailure(`Action ${quote(path)} requires a capability the Rust runtime has not configured: ${capability}`),
  staleCatalog: (expected: string, active: string) => new ExecutionFailure(`STALE_AGENT_CATALOG: expected artifact ${quote(expected)}, active artifact is ${quote(active)}`, "update_required"),
  notInteractive: (path: string) => new ExecutionFailure(`function ${quote(path)} is not classified as interactive`),
  invalidArguments: (path: string, message: string) => new ExecutionFailure(`function ${quote(path)} arguments do not match its schema: ${message}`),
  invalidResult: (path: string, message: string) => new ExecutionFailure(`function ${quote(path)} result does not match its schema: ${message}`),
  depth: () => new ExecutionFailure("nested function invocation exceeded the maximum depth"),
  recursive: (path: string) => new ExecutionFailure(`recursive Action invocation detected for ${quote(path)}`),
};

/** `ExecutionError::ModuleHost(Remote)`. */
function moduleHostFailure(code: string, message: string, retryable: boolean): ExecutionFailure {
  return new ExecutionFailure(`TypeScript module host returned ${code}: ${message}`, classifyWire({ code: code as never, message, retryable }));
}

/** The declared `actionCapabilities` (action_calls.rs `ActionCapabilities`). */
export interface ActionCapabilities {
  networkOrigins: string[];
  tools: Record<string, { kind: string; function: string }>;
  scheduler: boolean;
  storage: boolean;
  sandbox: { duckdb?: boolean } | null;
  secrets: string[];
  functions: boolean;
}

/** `ActionHostCalls::new`: decodes the declaration; refuses `functions` outside the agent profile. */
export function declaredCapabilities(contract: FunctionContract, anonymous: boolean): ActionCapabilities {
  const raw = contract.metadata?.["actionCapabilities"];
  const out: ActionCapabilities = { networkOrigins: [], tools: {}, scheduler: false, storage: false, sandbox: null, secrets: [], functions: false };
  if (raw !== undefined && raw !== null) {
    if (typeof raw !== "object" || Array.isArray(raw)) throw executionErrors.hostCall("invalid Action capability declaration: invalid type: expected struct ActionCapabilities");
    const r = raw as Record<string, unknown>;
    if (Array.isArray(r.networkOrigins)) out.networkOrigins = r.networkOrigins.filter((origin): origin is string => typeof origin === "string");
    if (r.tools && typeof r.tools === "object") {
      for (const name of Object.keys(r.tools as object).sort()) {
        const binding = (r.tools as Record<string, { kind?: unknown; function?: unknown }>)[name];
        out.tools[name] = { kind: String(binding?.kind ?? ""), function: String(binding?.function ?? "") };
      }
    }
    out.scheduler = r.scheduler === true;
    out.storage = r.storage === true;
    out.sandbox = r.sandbox !== undefined && r.sandbox !== null ? (r.sandbox as { duckdb?: boolean }) : null;
    if (Array.isArray(r.secrets)) out.secrets = r.secrets.filter((name): name is string => typeof name === "string");
    out.functions = r.functions === true;
  }
  if (out.functions && profileOf(contract) !== "agent") throw executionErrors.hostCall("interactive function invocation requires an agent Action profile");
  if (anonymous) out.scheduler = false;
  return out;
}

export const profileOf = (contract: FunctionContract): "standard" | "agent" =>
  contract.metadata?.["actionProfile"] === "agent" ? "agent" : "standard";

/** `FunctionDefinition.interactive` and `.classification`, derived as RT/modules.rs derives them. */
export function interactivity(contract: FunctionContract): { interactive: boolean; classification: string } {
  const declared = contract.metadata?.["interactive"];
  const classification = contract.metadata?.["classification"];
  const internal = contract.internal === true;
  const interactive = typeof declared === "boolean" ? declared
    : classification === "interactive" ? true
    : classification === "system" || classification === "internal" ? false
    : !internal && contract.kind !== "action";
  return { interactive, classification: typeof classification === "string" ? classification : internal ? "internal" : interactive ? "interactive" : "system" };
}

const deliveryOf = (contract: FunctionContract): string => String(contract.metadata?.["delivery"] ?? "");

/** `require_function`. */
export function requireFunction(generation: Generation, path: string, kind: "query" | "reducer" | "action", allowInternal: boolean): FunctionContract {
  const contract = generation.engine.functions.get(path);
  if (!contract) throw executionErrors.functionMissing(path);
  if (contract.internal === true && !allowInternal) throw executionErrors.internal(path);
  if (contract.kind !== kind) throw executionErrors.wrongKind(path, kind, contract.kind);
  return contract;
}

/** `require_public_access`. */
export function requirePublicAccess(session: ActionSession, contract: FunctionContract, allowInternal: boolean): void {
  if (!isAnonymous(session)) return;
  const isPublic = contract.metadata?.["public"] === true;
  const replica = deliveryOf(contract) === "replica" || contract.metadata?.["replica"] !== undefined;
  if (!(isPublic || (allowInternal && contract.internal === true)) || replica || deliveryOf(contract) === "live") {
    throw executionErrors.hostCall(`authenticate with an active tenant before calling a non-public ${contract.kind}`);
  }
}

function validateArguments(contract: FunctionContract, args: unknown): void {
  try { validateValue(contract.argsSchema, args); } catch (error) {
    if (error instanceof SchemaError) throw executionErrors.invalidArguments(contract.path, error.message);
    throw error;
  }
}

export interface ActionRuntimeOptions {
  /** The project's module generations (the bundle ships in the Worker version, G0-2). Its moduleId is the project id. */
  registry: ModuleRegistry;
  /** Queries and Reducers the Action calls (tools, `ctx.functions`) run in the tenant object. */
  tenant: TenantActionPort;
  control?: ControlPort | undefined;
  scheduler?: SchedulerPort | undefined;
  storage?: StoragePort | undefined;
  sandbox?: SandboxPort | undefined;
  /** Outbound fetch (workerd's global by default; tests route the upstream). */
  fetcher?: Fetcher | undefined;
  clock?: (() => number) | undefined;
  randomUUID?: (() => string) | undefined;
  /** GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS. */
  publicActionTimeoutMs?: number | undefined;
  /** Action execution budget (GONVEX_MODULE_HOST_ACTION_EXECUTION_TIMEOUT_MS). */
  actionTimeoutMs?: number | undefined;
}

export interface ActionCall {
  session: ActionSession;
  path: string;
  args: unknown;
  /** Inherited provenance (an outbox row, a scheduled job, a `ctx.functions` parent). */
  provenance?: Provenance | undefined;
  allowInternal?: boolean | undefined;
}

export type ActionReport =
  | { ok: true; value: string; committedRevision?: number | undefined; stats: ActionStats }
  | { ok: false; error: string; class: ErrorClass; stats: ActionStats };

/** Numbers for one Action (and everything it called). */
export interface ActionStats {
  hostCalls: number;
  byKind: Record<string, number>;
  tenantCalls: number;
  fetches: number;
  subrequests: number;
  bytesFetched: number;
  streams: number;
}

const emptyStats = (): ActionStats => ({ hostCalls: 0, byKind: {}, tenantCalls: 0, fetches: 0, subrequests: 0, bytesFetched: 0, streams: 0 });

/** Shared by one root Action and every nested Action it runs. */
interface Execution {
  generation: Generation;
  committedRevision: number | undefined;
  stats: ActionStats;
}

export class ActionRuntime {
  private readonly clock: () => number;
  private readonly randomUUID: () => string;

  constructor(readonly options: ActionRuntimeOptions) {
    this.clock = options.clock ?? (() => Date.now());
    this.randomUUID = options.randomUUID ?? (() => crypto.randomUUID());
  }

  /** Runs one Action and answers its result or Rust's error text. Never throws for an Action failure. */
  async execute(call: ActionCall): Promise<ActionReport> {
    const stats = emptyStats();
    let lease;
    try {
      lease = this.options.registry.lease();
    } catch (error) {
      if (isWireError(error) && error.code === codes.MODULE_NOT_LOADED) {
        const failure = executionErrors.moduleMissing(call.session.projectId);
        return { ok: false, error: failure.message, class: failure.errorClass, stats };
      }
      throw error;
    }
    const execution: Execution = { generation: lease.generation, committedRevision: undefined, stats };
    try {
      const value = await this.run(execution, call);
      return { ok: true, value, committedRevision: execution.committedRevision, stats };
    } catch (error) {
      if (error instanceof ExecutionFailure) return { ok: false, error: error.message, class: error.errorClass, stats };
      throw error;
    } finally {
      lease.release();
    }
  }

  /** `execute_tenant_action_with_access_counted`. Throws `ExecutionFailure`. */
  private async run(execution: Execution, call: ActionCall): Promise<string> {
    const { generation } = execution;
    const { session, path } = call;
    const allowInternal = call.allowInternal === true;
    const contract = requireFunction(generation, path, "action", allowInternal);
    requirePublicAccess(session, contract, allowInternal);
    validateArguments(contract, call.args);
    const commandId = `action-${this.randomUUID()}`;
    const agent = profileOf(contract) === "agent";
    const anonymous = isAnonymous(session);
    const provenance = initialActionProvenance(session, call.provenance, commandId, generation.artifact.artifactHash, agent, this.randomUUID);
    if (!agent || anonymous) {
      provenance.deadlineUnixMs ??= this.clock() + (this.options.actionTimeoutMs ?? ACTION_TIMEOUT_MS);
      if (anonymous) provenance.deadlineUnixMs = Math.min(provenance.deadlineUnixMs, this.clock() + (this.options.publicActionTimeoutMs ?? PUBLIC_ACTION_TIMEOUT_MS));
    }
    if (!provenance.actionStack.includes(path)) provenance.actionStack.push(path);
    const capabilities = declaredCapabilities(contract, anonymous);
    const environment = await this.secrets(session, capabilities.secrets);

    const host = new ActionHost(this, execution, session, provenance, capabilities, this.options.fetcher);
    const deadline = provenance.deadlineUnixMs;
    const invoke = generation.engine.invoke(host, {
      function: path,
      kind: "action",
      args: call.args,
      context: {
        nowUnixMs: 0,
        identity: identityOf(session),
        invocation: publicInfo(provenance),
        capabilities: {
          network: capabilities.networkOrigins.length > 0,
          actionTools: Object.keys(capabilities.tools).length > 0,
          scheduler: capabilities.scheduler,
          storage: capabilities.storage,
          sandbox: capabilities.sandbox !== null,
          secrets: Object.keys(environment).length > 0,
          functions: capabilities.functions,
        },
        actionTools: Object.keys(capabilities.tools),
        environment,
        // An anonymous Action's outer deadline (below) fires before the engine's, as in Rust.
        deadlineUnixMs: deadline === null ? undefined : anonymous ? deadline + TRANSPORT_SLACK_MS : deadline,
        nestingDepth: provenance.depth,
      },
    });
    try {
      const result = anonymous && deadline !== null ? await this.race(invoke, deadline) : await invoke;
      return result.value;
    } catch (error) {
      if (error instanceof ExecutionFailure) throw error;
      if (error instanceof ModuleError) {
        const wire = error.wire(this.options.registry.moduleId, path);
        throw moduleHostFailure(wire.code, wire.message, wire.retryable);
      }
      throw error;
    } finally {
      host.close();
    }
  }

  private async race<T>(work: Promise<T>, deadline: number): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(executionErrors.hostCall("public Action execution deadline exceeded")), Math.max(0, deadline - this.clock()));
    });
    try {
      return await Promise.race([work, expired]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      work.catch(() => undefined);
    }
  }

  /** `load_action_secrets`: every declared name must be configured. */
  private async secrets(session: ActionSession, names: readonly string[]): Promise<Record<string, string>> {
    if (names.length === 0) return {};
    const control = this.options.control;
    if (!control) throw executionErrors.capabilityUnavailable(session.projectId, "project secrets require the Control Plane");
    let found: Record<string, string>;
    try {
      found = await control.projectSecrets(session.projectId, names);
    } catch (error) {
      throw executionErrors.hostCall(String((error as Error)?.message ?? error));
    }
    const missing = names.find((name) => typeof found[name] !== "string");
    if (missing !== undefined) throw executionErrors.capabilityUnavailable(session.projectId, `unconfigured project secret ${quote(missing)}`);
    const environment: Record<string, string> = {};
    for (const name of [...names].sort()) environment[name] = found[name]!;
    return environment;
  }

  /** A Query or Reducer in the tenant object, checked here first as `execute_tenant_*_with_access` checks it. */
  async tenantCall(execution: Execution, session: ActionSession, path: string, kind: "query" | "reducer", args: unknown, provenance: Provenance, allowInternal: boolean): Promise<unknown> {
    const contract = requireFunction(execution.generation, path, kind, allowInternal);
    requirePublicAccess(session, contract, allowInternal);
    validateArguments(contract, args);
    execution.stats.tenantCalls++;
    let report: TenantCallReport;
    try {
      report = await this.options.tenant.call({ v: 1, function: path, kind, args, session, provenance, allowInternal });
    } catch (error) {
      throw new ExecutionFailure(String((error as Error)?.message ?? error), "transient");
    }
    if (!report.ok) throw new ExecutionFailure(report.error, report.transient ? "transient" : "rejected");
    if (report.committedRevision !== undefined) execution.committedRevision = Math.max(execution.committedRevision ?? 0, report.committedRevision);
    return JSON.parse(report.value);
  }

  /** `invoke_tool`. */
  async invokeTool(execution: Execution, session: ActionSession, parent: Provenance, binding: { kind: string; function: string } | undefined, name: string, args: unknown): Promise<unknown> {
    if (parent.depth >= MAX_INVOCATION_DEPTH) throw executionErrors.depth();
    if (!binding) throw new ExecutionFailure(`Action tool ${quote(name)} is not declared`);
    const provenance = toolProvenance(session, parent, `agent-tool-${this.randomUUID()}`);
    switch (binding.kind) {
      case "query": return this.tenantCall(execution, session, binding.function, "query", args, provenance, true);
      case "reducer":
      case "internalReducer": return this.tenantCall(execution, session, binding.function, "reducer", args, provenance, binding.kind === "internalReducer");
      default: throw new ExecutionFailure(`Action tool ${quote(name)} has unsupported kind ${quote(binding.kind)}`);
    }
  }

  /** `invoke_interactive_function`. */
  async invokeFunction(execution: Execution, session: ActionSession, parent: Provenance, path: string, args: unknown, artifactHash: string): Promise<unknown> {
    const control = this.options.control;
    if (!control) throw executionErrors.moduleMissing(session.projectId);
    // Re-resolve the Member for every delegated call: a revoked Member loses access at once.
    let fresh: ActionSession;
    try {
      fresh = await control.tenantSessionForAccount(session.projectId, session.tenantId, session.account.id);
    } catch (error) {
      throw new ExecutionFailure(String((error as Error)?.message ?? error));
    }
    const generation = execution.generation;
    if (parent.depth >= MAX_INVOCATION_DEPTH) throw executionErrors.depth();
    const active = generation.artifact.artifactHash;
    if (artifactHash !== active) throw executionErrors.staleCatalog(artifactHash, active);
    const contract = generation.engine.functions.get(path);
    if (!contract) throw executionErrors.functionMissing(path);
    const { interactive, classification } = interactivity(contract);
    if (contract.internal === true || !interactive || classification !== "interactive") throw executionErrors.notInteractive(path);
    if (contract.kind === "action" && parent.actionStack.includes(path)) throw executionErrors.recursive(path);
    validateArguments(contract, args);
    const child = delegatedProvenance(fresh, parent, `agent-${this.randomUUID()}`, active, this.randomUUID);
    let result: unknown;
    if (contract.kind === "action") result = JSON.parse(await this.run(execution, { session: fresh, path, args, provenance: child }));
    else result = await this.tenantCall(execution, fresh, path, contract.kind, args, child, false);
    try { validateValue(contract.resultSchema, result); } catch (error) {
      if (error instanceof SchemaError) throw executionErrors.invalidResult(path, error.message);
      throw error;
    }
    return result;
  }

  /** `enqueue_scheduled` from an Action: checks the target, then K11 inserts the job. */
  async schedule(execution: Execution, session: ActionSession, provenance: Provenance, name: string, args: unknown, runAtUnixMs: number, coalesce: boolean): Promise<string> {
    const function_ = name.trim();
    const contract = execution.generation.engine.functions.get(function_);
    if (!contract) throw new Error(`scheduled function ${quote(name)} is not registered`);
    if (contract.kind !== "reducer" && contract.kind !== "action") throw new Error("only Reducers and Actions can be scheduled");
    const scheduler = this.options.scheduler;
    if (!scheduler) throw new Error("scheduler storage is unavailable");
    return scheduler.enqueue({ v: 1, id: `job_${this.randomUUID()}`, function: function_, args, runAtUnixMs, coalesce, session, provenance: persisted(provenance) });
  }

  now(): number { return this.clock(); }
}

/** The host behind every Action op of one invocation (`ActionHostCalls::handle`). */
class ActionHost implements ModuleHost {
  private readonly network: NetworkHost | undefined;

  constructor(
    private readonly runtime: ActionRuntime,
    private readonly execution: Execution,
    private readonly session: ActionSession,
    private readonly provenance: Provenance,
    private readonly capabilities: ActionCapabilities,
    fetcher: Fetcher | undefined,
  ) {
    if (capabilities.networkOrigins.length > 0) {
      this.network = new NetworkHost({ origins: capabilities.networkOrigins, deadlineUnixMs: provenance.deadlineUnixMs, fetcher, clock: () => runtime.now() });
    }
  }

  close(): void {
    if (this.network) {
      const { stats } = this.execution;
      stats.fetches += this.network.stats.requests;
      stats.subrequests += this.network.stats.subrequests;
      stats.bytesFetched += this.network.stats.bytes;
      stats.streams += this.network.stats.streams;
      this.network.closeAll();
    }
  }

  async call(payload: HostPayload, capability: string): Promise<unknown> {
    const stats = this.execution.stats;
    stats.hostCalls++;
    stats.byKind[payload.kind] = (stats.byKind[payload.kind] ?? 0) + 1;
    try {
      return await this.handle(payload);
    } catch (error) {
      if (error instanceof ConcurrencyLimit) throw new HostFailure(error.message);
      throw new HostFailure(hostErrors.wrapped(capability, String((error as Error)?.message ?? error)));
    }
  }

  private async handle(payload: HostPayload): Promise<unknown> {
    const { runtime, execution, session, provenance, capabilities } = this;
    switch (payload.kind) {
      case "toolInvoke": {
        const name = String(payload["tool"]);
        return runtime.invokeTool(execution, session, provenance, capabilities.tools[name], name, payload["args"] ?? null);
      }
      case "functionInvoke":
        if (!capabilities.functions) throw new Error("interactive function invocation is not declared for this Action");
        return runtime.invokeFunction(execution, session, provenance, String(payload["path"]), payload["args"] ?? null, String(payload["artifactHash"]));
      case "fetch":
      case "fetchRead":
      case "fetchCancel": {
        const network = this.network;
        if (!network) throw new Error("network access is not declared for this Action");
        if (payload.kind === "fetch") return network.fetch(payload["request"]);
        if (payload.kind === "fetchRead") return network.read(payload["stream"]);
        return network.cancel(payload["stream"]);
      }
      case "scheduleAfter":
      case "scheduleAt": {
        if (!capabilities.scheduler) throw new Error("scheduler access is not declared for this Action");
        let at: number;
        if (payload.kind === "scheduleAfter") {
          const delay = payload["delayMs"];
          if (!Number.isSafeInteger(delay) || (delay as number) < 0) throw new Error("scheduler delayMs is outside the supported range");
          // B.4: the server clock at call time, not ctx.now.
          at = runtime.now() + (delay as number);
        } else {
          at = payload["atUnixMs"] as number;
          if (!Number.isSafeInteger(at) || at < 0 || at > 8.64e15) throw new Error("scheduler atUnixMs is outside the supported range");
        }
        return runtime.schedule(execution, session, provenance, String(payload["function"] ?? ""), payload["args"] ?? null, at, payload["coalesce"] === true);
      }
      case "storage": {
        if (!capabilities.storage) throw new Error("storage access is not declared for this Action");
        const storage = runtime.options.storage;
        if (!storage) throw new Error("storage is not configured");
        return storage.call(String(payload["operation"] ?? ""), payload["payload"] ?? null, session);
      }
      case "sandbox": {
        if (capabilities.sandbox === null) throw new Error("sandbox access is not declared for this Action");
        const operation = String(payload["operation"] ?? "");
        if (operation === "importFile" && !capabilities.storage) throw new Error("sandbox importFile also requires the storage capability");
        const sandbox = runtime.options.sandbox;
        if (!sandbox) throw new Error("sandbox is disabled by runtime policy");
        return sandbox.call(operation, payload["payload"] ?? null, session, capabilities.sandbox.duckdb === true);
      }
      default:
        throw new Error("this Action capability is not implemented in the Rust runtime yet");
    }
  }
}
