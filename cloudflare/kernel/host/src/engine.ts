// One module generation's engine: the Cloudflare counterpart of
// V8ModuleEngine (V8/lib.rs execute, V8/isolate.rs host_call/decode) plus the
// bootstrap dispatcher (BS:766-807).
//
// Order of checks per invocation, as in Rust:
//   function lookup -> kind -> args JSON + args schema -> depth -> budget ->
//   dispatch (handler resolution, handler, JSON encoding, UTF-16 pre-check) ->
//   budget overrun -> sticky violation -> exact byte cap -> result schema.
import { allows, capabilityOf, effectiveCapabilities, type Capabilities, type HostCallKind } from "./capabilities.js";
import { createContext, GonvexHostError, resolveHandler, type ContextRequest, type HostPayload, type Identity, type InvocationInfo } from "./context.js";
import { ModuleError, hostErrors } from "./errors.js";
import { withInvocation, type FunctionKind } from "./globals.js";
import { SchemaError, validateSchemaDefinition, validateValue } from "./schema.js";

export const MAX_INVOCATION_DEPTH = 8;
export const MAX_DURABLE_HOPS = 32;

/** One function of a module generation (FunctionContract + manifest metadata). */
export interface FunctionContract {
  path: string;
  kind: FunctionKind;
  internal?: boolean;
  /** Source export name (fallback when the bundle has no export named by `path`). */
  export?: string;
  argsSchema: unknown;
  resultSchema: unknown;
  /** `actionProfile`, `actionCapabilities`, `interactive`, `public`, … */
  metadata?: Record<string, unknown>;
}

export interface EngineConfig {
  /** Q/R wall budget (GONVEX_MODULE_HOST_EXECUTION_TIMEOUT_MS), contract 10 s. */
  executionTimeoutMs: number;
  /** Action wall budget, contract 15 min. */
  actionTimeoutMs: number;
  /** Scheduled Q/R slice at depth 0 (GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS), 3 s. */
  scheduledSliceMs: number;
  /** Result cap, contract 8 MiB. */
  maxResultBytes: number;
  /**
   * Host calls per invocation (R25). The platform clock does not advance
   * during synchronous work, so on Cloudflare the wall budget cannot fire at a
   * host-call boundary; a counter can. Sized well above any real Reducer
   * (Whagons: at most 37 statements) and below what 10 s of host calls reach.
   * 0 disables it.
   */
  maxHostCalls: number;
  /** Kernel clock for budgets (never the module's pinned clock). */
  clock: () => number;
}

export const defaultEngineConfig: EngineConfig = Object.freeze({
  executionTimeoutMs: 10_000,
  actionTimeoutMs: 15 * 60_000,
  scheduledSliceMs: 3_000,
  maxResultBytes: 8 * 1024 * 1024,
  maxHostCalls: 100_000,
  clock: () => Date.now(),
});

/** Per-invocation input (InvocationContext). */
export interface InvocationContext {
  /** Host-stamped `ctx.now`; 0 means "stamp now" (V8/lib.rs:574-580). */
  nowUnixMs: number;
  intentEntropy?: string | undefined;
  invitationInviter?: ContextRequest["invitationInviter"];
  identity: Identity;
  invocation: InvocationInfo;
  /** Host grant; the engine intersects it with the kind. */
  capabilities: Partial<Capabilities>;
  actionTools?: readonly string[] | undefined;
  environment?: Readonly<Record<string, string>> | undefined;
  /** Absolute invocation deadline (unix ms), captured before queueing. */
  deadlineUnixMs?: number | undefined;
  lane?: "interactive" | "scheduled";
  nestingDepth?: number;
}

export interface Invocation {
  function: string;
  kind: FunctionKind;
  /** Arguments as a JSON value (`undefined` is not JSON). */
  args: unknown;
  context: InvocationContext;
}

export interface InvocationResult {
  /** The result as JSON text, exactly what goes on the wire. */
  value: string;
}

/** Thrown by a host handler: the op answers `{status:"failed",message}`. */
export class HostFailure extends Error {}

/**
 * The host behind the op. It answers one call (already capability-checked)
 * synchronously or with a promise; a thrown `HostFailure` (or any error) is a
 * failed envelope whose message the module sees after "host call failed: ".
 */
export interface ModuleHost {
  call(payload: HostPayload, capability: string): unknown;
}

/** Bundle namespace: export name -> binding. */
export type ModuleExports = Readonly<Record<string, unknown>>;

interface ActiveSlot {
  open: boolean;
  calls: number;
  violation: string | undefined;
  overrun: boolean;
  serial: Promise<unknown>;
}

export class ModuleEngine {
  readonly functions: ReadonlyMap<string, FunctionContract>;
  readonly config: EngineConfig;
  private readonly exports: ModuleExports;

  /**
   * Builds an engine for one generation. Every function needs valid args and
   * result schemas, and paths are unique (V8/lib.rs:261-296). Throws
   * `ModuleError` InvalidArtifact otherwise. An export that does not resolve
   * fails its calls at dispatch, as in Rust (`unresolved()` lists them).
   */
  constructor(contracts: readonly FunctionContract[], exports: ModuleExports, config: Partial<EngineConfig> = {}) {
    this.config = { ...defaultEngineConfig, ...config };
    this.exports = exports;
    const functions = new Map<string, FunctionContract>();
    for (const contract of contracts) {
      if (functions.has(contract.path)) throw new ModuleError("InvalidArtifact", `function ${contract.path} is declared more than once`);
      for (const [name, schema] of [["arguments", contract.argsSchema], ["result", contract.resultSchema]] as const) {
        if (schema === undefined || schema === null) throw new ModuleError("InvalidArtifact", `function ${contract.path} has no ${name} schema`);
        try { validateSchemaDefinition(schema); } catch (error) {
          throw new ModuleError("InvalidArtifact", `function ${contract.path} has an invalid ${name} schema: ${(error as Error).message}`);
        }
      }
      functions.set(contract.path, contract);
    }
    this.functions = functions;
  }

  /** Functions whose export does not resolve to a handler (load must refuse them). */
  unresolved(): string[] {
    return [...this.functions.values()].filter((contract) => resolveHandler(this.binding(contract), contract.path) === null).map((contract) => contract.path);
  }

  private binding(contract: FunctionContract): unknown {
    // Bundles export each function under its public path; `export` is the
    // source export name, used only for single-module test bundles.
    return this.exports[contract.path] ?? (contract.export === undefined ? undefined : this.exports[contract.export]);
  }

  private budget(contract: FunctionContract, context: InvocationContext, started: number): { deadline: number | undefined; timeoutMs: number | undefined; sliced: boolean } {
    const agent = contract.kind === "action" && contract.metadata?.["actionProfile"] === "agent";
    const data = contract.kind !== "action";
    const slice = data && context.lane === "scheduled" && (context.nestingDepth ?? 0) === 0 && this.config.scheduledSliceMs > 0 ? this.config.scheduledSliceMs : undefined;
    let ceiling: number | undefined;
    if (!agent) {
      ceiling = data ? this.config.executionTimeoutMs : this.config.actionTimeoutMs;
      if (slice !== undefined) ceiling = Math.min(slice, ceiling);
    }
    const remaining = context.deadlineUnixMs === undefined ? undefined : context.deadlineUnixMs - this.config.clock();
    if (remaining !== undefined && remaining <= 0) throw new ModuleError("BudgetExceeded", "invocation deadline elapsed while waiting for a module isolate");
    const timeoutMs = ceiling === undefined ? remaining : remaining === undefined ? ceiling : Math.min(ceiling, remaining);
    return { deadline: timeoutMs === undefined ? undefined : started + timeoutMs, timeoutMs, sliced: slice !== undefined && timeoutMs === slice };
  }

  /** Runs one invocation; resolves with the JSON result or rejects with a `ModuleError`. */
  async invoke(host: ModuleHost, invocation: Invocation): Promise<InvocationResult> {
    const contract = this.functions.get(invocation.function);
    if (!contract) throw new ModuleError("FunctionNotFound", invocation.function);
    if (contract.kind !== invocation.kind) throw new ModuleError("WrongFunctionKind", invocation.function);
    if (invocation.args === undefined) throw new ModuleError("InvalidArguments", "$: invalid JSON: EOF while parsing a value at line 1 column 0");
    try { validateValue(contract.argsSchema, invocation.args); } catch (error) {
      if (error instanceof SchemaError) throw new ModuleError("InvalidArguments", error.message);
      throw error;
    }
    const context = invocation.context;
    if ((context.nestingDepth ?? 0) >= MAX_INVOCATION_DEPTH) throw new ModuleError("Execution", "nested function invocation exceeded the maximum depth");
    const started = this.config.clock();
    const budget = this.budget(contract, context, started);
    const capabilities = effectiveCapabilities(contract.kind, context.capabilities);
    const now = context.nowUnixMs === 0 ? started : context.nowUnixMs;
    // Declared origins come from the contract (V8/isolate.rs:1262): present
    // `actionCapabilities` without `networkOrigins` means none are allowed.
    const declared = contract.metadata?.["actionCapabilities"] as { networkOrigins?: unknown } | undefined;
    const networkOrigins = declared === undefined || declared === null ? undefined
      : Array.isArray(declared.networkOrigins) ? declared.networkOrigins.filter((origin): origin is string => typeof origin === "string") : [];
    const slot: ActiveSlot = { open: true, calls: 0, violation: undefined, overrun: false, serial: Promise.resolve() };
    const overrunMessage = budget.sliced
      ? scheduledSliceMessage(contract, budget.timeoutMs ?? 0)
      : `module exceeded its ${budget.timeoutMs ?? 0} ms execution deadline`;

    const op = (payload: HostPayload): Promise<unknown> => {
      if (!slot.open) return Promise.reject(new GonvexHostError("no module invocation is active", "failed"));
      const capability = capabilityOf[payload.kind as HostCallKind];
      if (capability === undefined) return Promise.reject(new GonvexHostError(`unsupported host operation: unknown variant \`${String(payload.kind)}\``, "failed"));
      if (!allows(capabilities, capability)) {
        const message = hostErrors.capabilityDenied(capability);
        slot.violation ??= message;
        return Promise.reject(new GonvexHostError(message, "denied"));
      }
      if (payload.kind === "fetch" && networkOrigins !== undefined) {
        const origin = originOf((payload["request"] as { url?: unknown } | undefined)?.url);
        if (origin === undefined || !networkOrigins.includes(origin)) {
          const message = "fetch origin is not declared for this Action";
          slot.violation ??= message;
          return Promise.reject(new GonvexHostError(message, "denied"));
        }
      }
      if (payload.kind === "toolInvoke" && !(context.actionTools ?? []).includes(String(payload["tool"]))) {
        const message = `Action tool ${JSON.stringify(payload["tool"])} is not declared for this invocation`;
        slot.violation ??= message;
        return Promise.reject(new GonvexHostError(message, "denied"));
      }
      // Budgets at the host-call boundary. workerd cannot interrupt running
      // JavaScript; a module that keeps calling the host stops here. The
      // clock check works only where time advances (local workerd, after real
      // awaits); on the platform the call counter is what fires (R25).
      slot.calls++;
      if (this.config.maxHostCalls > 0 && slot.calls > this.config.maxHostCalls) {
        slot.overrun = true;
        return Promise.reject(new GonvexHostError(hostErrors.failed(overrunMessage), "failed"));
      }
      if (budget.deadline !== undefined && this.config.clock() > budget.deadline) {
        slot.overrun = true;
        return Promise.reject(new GonvexHostError(hostErrors.failed(overrunMessage), "failed"));
      }
      const run = async () => {
        if (!slot.open) throw new GonvexHostError("no module invocation is active", "failed");
        try {
          return await host.call(payload, capability);
        } catch (error) {
          throw new GonvexHostError(hostErrors.failed(String((error as Error)?.message ?? error)), "failed");
        }
      };
      if (payload.kind === "fetch") return run();
      // Every op except fetch is serialized per invocation (B.2a shared mechanics).
      const result = slot.serial.then(run, run);
      slot.serial = result.catch(() => undefined);
      return result;
    };

    const request: ContextRequest = {
      kind: contract.kind,
      function: contract.path,
      now,
      identity: context.identity ?? {},
      invocation: context.invocation ?? {},
      intentEntropy: context.intentEntropy,
      invitationInviter: context.invitationInviter,
      capabilities,
      actionTools: context.actionTools,
      environment: context.environment,
    };
    const binding = this.binding(contract);
    const dispatched = withInvocation({ kind: contract.kind, now, intentEntropy: context.intentEntropy }, () =>
      dispatch(binding, request, invocation.args, op, this.config.maxResultBytes));

    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<"timeout">((resolve) => {
      if (budget.timeoutMs !== undefined) timer = setTimeout(() => resolve("timeout"), Math.max(0, budget.timeoutMs));
    });
    let envelope: DispatchEnvelope | "timeout";
    try {
      envelope = await Promise.race([dispatched, timedOut]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      slot.open = false;
    }
    if (envelope === "timeout" || slot.overrun) throw new ModuleError("BudgetExceeded", overrunMessage);
    // A denial fails the invocation even when the module caught it.
    if (slot.violation !== undefined) throw new ModuleError("Execution", slot.violation);
    if (envelope.status === "error") {
      switch (envelope.kind) {
        case "dispatch": throw new ModuleError("InvalidArtifact", envelope.message);
        case "handler": throw new ModuleError("Execution", envelope.stack ?? envelope.message);
        case "result": throw new ModuleError("Execution", envelope.message);
        case "resultSize": throw new ModuleError("BudgetExceeded", envelope.message);
      }
    }
    const bytes = utf8Length(envelope.value);
    if (bytes > this.config.maxResultBytes) {
      throw new ModuleError("BudgetExceeded", `module result of ${bytes} bytes exceeds the ${this.config.maxResultBytes} byte limit`);
    }
    try { validateValue(contract.resultSchema, JSON.parse(envelope.value)); } catch (error) {
      if (error instanceof SchemaError) throw new ModuleError("InvalidResult", error.message);
      throw error;
    }
    return { value: envelope.value };
  }
}

type DispatchEnvelope =
  | { status: "ok"; value: string }
  | { status: "error"; kind: "dispatch" | "handler" | "result" | "resultSize"; message: string; stack?: string };

function format(value: unknown): string {
  if (typeof value === "string") return value;
  if (value instanceof Error) return value.stack || `${value.name}: ${value.message}`;
  try {
    const encoded = JSON.stringify(value);
    return encoded === undefined ? String(value) : encoded;
  } catch { return String(value); }
}

/** The bootstrap dispatcher (BS:766-807), minus JSON transport. */
async function dispatch(binding: unknown, request: ContextRequest, args: unknown, op: (payload: HostPayload) => Promise<unknown>, maxResultBytes: number): Promise<DispatchEnvelope> {
  const handler = resolveHandler(binding, request.function);
  if (handler === null) return { status: "error", kind: "dispatch", message: `module export for ${request.function} is not a callable ${request.kind} handler` };
  let value: unknown;
  try {
    // The module receives its own copy of the arguments, as after JSON decoding.
    value = await handler(createContext(request, op), structuredClone(args));
  } catch (cause) {
    const message = cause instanceof Error ? `${cause.name}: ${cause.message}` : format(cause);
    return cause instanceof Error && cause.stack ? { status: "error", kind: "handler", message, stack: cause.stack } : { status: "error", kind: "handler", message };
  }
  let encoded: string | undefined;
  try {
    encoded = value === undefined ? "null" : JSON.stringify(value);
    if (encoded === undefined) encoded = "null";
  } catch (cause) {
    return { status: "error", kind: "result", message: `${request.function} returned a value that cannot be encoded as JSON: ${format(cause)}` };
  }
  if (encoded.length > maxResultBytes) return { status: "error", kind: "resultSize", message: `${request.function} returned more than the ${maxResultBytes} byte result limit` };
  return { status: "ok", value: encoded };
}

function originOf(url: unknown): string | undefined {
  if (typeof url !== "string") return undefined;
  try {
    const parsed = new URL(url);
    return parsed.protocol === "http:" || parsed.protocol === "https:" ? parsed.origin : undefined;
  } catch { return undefined; }
}

function utf8Length(text: string): number {
  let bytes = 0;
  for (let index = 0; index < text.length; index++) {
    const code = text.charCodeAt(index);
    if (code < 0x80) bytes += 1;
    else if (code < 0x800) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff && index + 1 < text.length) { bytes += 4; index++; }
    else bytes += 3;
  }
  return bytes;
}

/** V8/lib.rs scheduled_slice_message. */
function scheduledSliceMessage(contract: FunctionContract, sliceMs: number): string {
  return `scheduled ${contract.kind === "query" ? "query" : "reducer"} ${contract.path} exceeded its ${sliceMs} ms time slice for scheduled work `
    + "(GONVEX_MODULE_HOST_SCHEDULED_SLICE_MS). A scheduled run holds the tenant "
    + "writer and the module isolate while it runs: process a bounded batch per "
    + "run and schedule the rest with ctx.scheduler.runAfter";
}
