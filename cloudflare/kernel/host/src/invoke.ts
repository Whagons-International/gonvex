// Runs Queries and Reducers in the tenant object: one generation lease, one
// storage transaction (K01), one database host and the engine, with Rust's
// commit rules (B.7):
//
// - commit only after the handler returned and its result passed the schema;
// - a statement failure the module caught without ROLLBACK TO refuses the
//   commit ("module database capability failed: current transaction is
//   aborted…", rejected);
// - the deferred-FK check runs before the platform's commit (G0-5);
// - a Query never commits.
import { databaseGrant } from "./capabilities.js";
import { CatalogCache } from "./catalog.js";
import type { Identity, InvocationInfo } from "./context.js";
import { ABORTED, DatabaseHost, type DatabaseHostStats, type FileMetadataSource, type PermissionIndex } from "./database-host.js";
import type { InvocationContext } from "./engine.js";
import { ModuleError, classifyPlatformError, classifyWire, codes, type ErrorClass, type WireCode } from "./errors.js";
import { isWireError, type ModuleRegistry } from "./generations.js";
import type { TenantStorage } from "./storage.js";

export interface TenantCall {
  function: string;
  kind: "query" | "reducer";
  args: unknown;
  identity: Identity;
  invocation: InvocationInfo;
  /** Host-stamped `ctx.now`; omitted or 0 stamps the object's clock at start. */
  now?: number;
  intentEntropy?: string;
  invitationInviter?: InvocationContext["invitationInviter"];
  /** Pinned generation (UNKNOWN_GENERATION if the object is behind). */
  generation?: number;
  deadlineUnixMs?: number;
  lane?: "interactive" | "scheduled";
  /** Session facts for the host grant (RT/execution.rs:585-588). */
  anonymous?: boolean;
}

export interface CallError {
  code: WireCode;
  /** The text Rust puts in the reducer/query error. */
  message: string;
  class: ErrorClass;
  retryable: boolean;
}

export interface CallReport {
  ok: boolean;
  /** JSON text of the result when `ok`. */
  value?: string;
  error?: CallError;
  committed: boolean;
  generation: number | undefined;
  stats: DatabaseHostStats & { commitCheckRowsRead: number };
}

/** Commands whose earlier run overran the platform CPU limit (R25). */
export interface Quarantine {
  has(commandId: string): boolean;
  add(commandId: string): void;
}

/** In-memory quarantine; the caller re-records a command after an object reset (probes/r25/FINDINGS.md). */
export class MemoryQuarantine implements Quarantine {
  private readonly ids = new Set<string>();
  has(commandId: string) { return this.ids.has(commandId); }
  add(commandId: string) { this.ids.add(commandId); }
}

/**
 * The texts a caller (gateway or Worker) sees on the tenant stub when an
 * invocation overran the CPU limit or queued behind one (probes/r25).
 * `durableObjectReset` / `overloaded` are set on the error as well.
 */
export function isPlatformOverrun(error: unknown): boolean {
  const e = error as { message?: unknown; durableObjectReset?: unknown; overloaded?: unknown } | null;
  const message = String(e?.message ?? error);
  return e?.durableObjectReset === true || e?.overloaded === true
    || /exceeded its CPU time limit and was reset|reset because its code was updated|is overloaded\. Requests queued for too long/.test(message);
}

/**
 * The error a CPU-overrun command gets, for the culprit and for every
 * quarantined retry: Rust's deadline overrun (BudgetExceeded, transient), so
 * clients see the same text as on Rust.
 */
export function deadlineOverrunError(moduleId: string, functionPath: string, timeoutMs = 10_000): CallError {
  const wire = new ModuleError("BudgetExceeded", `module exceeded its ${timeoutMs} ms execution deadline`).wire(moduleId, functionPath);
  return moduleHostError(wire.code, wire.message, wire.retryable);
}

export interface TenantModuleHostOptions {
  storage: TenantStorage;
  registry: ModuleRegistry;
  cache?: CatalogCache;
  permissions?: PermissionIndex;
  files?: FileMetadataSource;
  /** Overrun commands refused without running (defaults to an in-memory set). */
  quarantine?: Quarantine;
  /** Object clock; never the module's pinned clock. */
  clock?: () => number;
}

const emptyStats = (): CallReport["stats"] => ({ calls: 0, byKind: {}, statements: 0, rowsRead: 0, rowsWritten: 0, catalogStatements: 0, catalogRowsRead: 0, catalogHits: 0, savepoints: 0, transientFault: false, commitCheckRowsRead: 0 });

function moduleHostError(code: WireCode, message: string, retryable = false): CallError {
  // ExecutionError::ModuleHost(Remote) Display.
  const text = `TypeScript module host returned ${code}: ${message}`;
  return { code, message: text, class: classifyWire({ code, message, retryable }), retryable };
}

/** ExecutionError::HostCall: "module database capability failed: <message>". */
function hostCallError(message: string, transient: boolean): CallError {
  return { code: codes.HOST_CALL_FAILED, message: `module database capability failed: ${message}`, class: transient ? "transient" : "rejected", retryable: transient };
}

/** One per tenant object. */
export class TenantModuleHost {
  readonly cache: CatalogCache;
  readonly quarantine: Quarantine;
  private readonly clock: () => number;

  constructor(private readonly options: TenantModuleHostOptions) {
    this.cache = options.cache ?? new CatalogCache();
    this.clock = options.clock ?? (() => Date.now());
    this.quarantine = options.quarantine ?? new MemoryQuarantine();
    // A new generation may come with new migrations: drop every catalog.
    options.registry.onReload(() => this.cache.invalidate());
  }

  /** Migrations (K02) and any other kernel DDL call this after they commit. */
  schemaChanged(): void { this.cache.invalidate(); }

  async run(call: TenantCall): Promise<CallReport> {
    const registry = this.options.registry;
    const commandId = call.invocation.commandId;
    if (typeof commandId === "string" && this.quarantine.has(commandId)) {
      // A retry of a command that reset this object: answer as Rust's deadline overrun, without running it.
      const timeoutMs = registry.activeGeneration?.engine.config.executionTimeoutMs;
      return { ok: false, error: deadlineOverrunError(registry.moduleId, call.function, timeoutMs), committed: false, generation: registry.activeGeneration?.generation, stats: emptyStats() };
    }
    let lease;
    try {
      lease = registry.lease(call.generation);
    } catch (error) {
      if (isWireError(error)) return { ok: false, error: moduleHostError(error.code, error.message, error.retryable), committed: false, generation: undefined, stats: emptyStats() };
      throw error;
    }
    const generation = lease.generation;
    let host: DatabaseHost | undefined;
    try {
      const outcome = await this.options.storage.invocation(call.kind, async (tx) => {
        host = new DatabaseHost({
          mode: call.kind,
          tx,
          cache: this.cache,
          identity: call.identity,
          invocation: call.invocation,
          schedulable: (path) => generation.schedulable(path),
          permissions: this.options.permissions,
          files: this.options.files,
          declaredKey: (table) => generation.declaredKeys.get(table),
          wallClock: this.clock,
          randomUUID: () => crypto.randomUUID(),
          subtle: crypto.subtle,
        });
        try {
          const definition = generation.engine.functions.get(call.function);
          const grant = databaseGrant(call.kind, {
            anonymous: call.anonymous === true,
            public: definition?.metadata?.["public"] === true,
            internal: definition?.internal === true,
            // SDK default (functionManifestMetadata): non-internal Queries and Reducers are interactive.
            interactive: typeof definition?.metadata?.["interactive"] === "boolean" ? definition.metadata["interactive"] === true : definition?.internal !== true,
          });
          const result = await generation.engine.invoke(host, {
            function: call.function,
            kind: call.kind,
            args: call.args,
            context: {
              nowUnixMs: call.now ?? 0,
              intentEntropy: call.intentEntropy,
              invitationInviter: call.invitationInviter,
              identity: call.identity,
              invocation: call.invocation,
              capabilities: grant,
              deadlineUnixMs: call.deadlineUnixMs,
              lane: call.lane ?? "interactive",
              nestingDepth: 0,
            },
          });
          return { ok: true as const, value: result.value };
        } catch (error) {
          if (error instanceof ModuleError) {
            const wire = error.wire(registry.moduleId, call.function);
            return { ok: false as const, error: moduleHostError(wire.code, wire.message, wire.retryable) };
          }
          throw error;
        }
      }, (result) => result.ok && host?.aborted === undefined);
      const stats = { ...(host?.stats ?? emptyStats()), commitCheckRowsRead: outcome.commitCheckRowsRead };
      let { result } = outcome;
      if (result.ok && call.kind === "reducer" && host?.aborted !== undefined) {
        result = { ok: false, error: hostCallError(ABORTED, false) };
      } else if (result.ok && outcome.commitError) {
        result = { ok: false, error: hostCallError(outcome.commitError.message, stats.transientFault) };
      }
      return result.ok
        ? { ok: true, value: result.value, committed: outcome.committed, generation: generation.generation, stats }
        : { ok: false, error: result.error, committed: false, generation: generation.generation, stats };
    } catch (error) {
      // The storage transaction itself failed (platform error, object reset).
      const platform = classifyPlatformError(error);
      return {
        ok: false,
        error: { code: platform.code, message: platform.message, class: platform.class, retryable: platform.class === "transient" },
        committed: false,
        generation: generation.generation,
        stats: { ...(host?.stats ?? emptyStats()), commitCheckRowsRead: 0 },
      };
    } finally {
      host?.finish();
      lease.release();
    }
  }
}
