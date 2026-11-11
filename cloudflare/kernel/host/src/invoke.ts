// Runs Queries and Reducers in the tenant object: one generation lease, one
// K01 storage invocation (`TenantStorage.invoke`), one database host and the
// engine. K01 decides the commit with Rust's rules (B.7): only after the
// handler returned, never with a statement failure outstanding (commit
// refused with ABORTED), never past a deferred-FK violation (G0-5), never for
// a Query. K04 discards the transaction when the engine fails the call
// (handler error, budget, denial, invalid result) and maps every failure to
// Rust's client text and class.
import { databaseGrant } from "./capabilities.js";
import type { Identity, InvocationInfo } from "./context.js";
import { StorageError, type InvocationStats, type TenantStorage } from "../../storage/index";
import { DatabaseHost, type FileMetadataSource, type PermissionIndex } from "./database-host.js";
import type { InvocationContext } from "./engine.js";
import { ModuleError, classifyPlatformError, classifyWire, codes, type ErrorClass, type WireCode } from "./errors.js";
import { isWireError, type ModuleRegistry } from "./generations.js";

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
  stats: CallStats;
}

/** Host calls (K04) and K01's row counters for one invocation. */
export interface CallStats {
  hostCalls: number;
  byKind: Record<string, number>;
  /** All statements, rows read and written, as K01 counts them (catalog and FK-model reads included). */
  statements: number;
  rowsRead: number;
  rowsWritten: number;
  /** Catalog lookups among the totals (pragma_table_info, record views). */
  catalogStatements: number;
  catalogRowsRead: number;
  /** FK-model reads among the totals (once per schema generation). */
  schemaRowsRead: number;
  savepoints: number;
  transientFault: boolean;
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
  /** K01's storage layer for this tenant object; it owns the one catalog cache. */
  storage: TenantStorage;
  registry: ModuleRegistry;
  permissions?: PermissionIndex;
  files?: FileMetadataSource;
  /** Overrun commands refused without running (defaults to an in-memory set). */
  quarantine?: Quarantine;
  /** Object clock; never the module's pinned clock. */
  clock?: () => number;
}

const emptyStats = (): CallStats => ({ hostCalls: 0, byKind: {}, statements: 0, rowsRead: 0, rowsWritten: 0, catalogStatements: 0, catalogRowsRead: 0, schemaRowsRead: 0, savepoints: 0, transientFault: false });

function callStats(storage: InvocationStats | undefined, host: DatabaseHost | undefined): CallStats {
  const stats = emptyStats();
  if (host) { stats.hostCalls = host.stats.calls; stats.byKind = host.stats.byKind; stats.transientFault = host.stats.transientFault; }
  if (storage) {
    stats.statements = storage.statements;
    stats.rowsRead = storage.rowsRead;
    stats.rowsWritten = storage.rowsWritten;
    stats.catalogStatements = storage.catalog.statements;
    stats.catalogRowsRead = storage.catalog.rowsRead;
    stats.schemaRowsRead = storage.schema.rowsRead;
    stats.savepoints = storage.savepoints;
  }
  return stats;
}

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
  readonly quarantine: Quarantine;
  private readonly clock: () => number;

  constructor(private readonly options: TenantModuleHostOptions) {
    this.clock = options.clock ?? (() => Date.now());
    this.quarantine = options.quarantine ?? new MemoryQuarantine();
    // A new generation may come with new migrations: drop K01's catalogs.
    options.registry.onReload(() => options.storage.invalidateSchema());
  }

  /** Migrations (K02) and any other kernel DDL call this after they commit; K01 owns the cache. */
  schemaChanged(): void { this.options.storage.invalidateSchema(); }

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
      const outcome = await this.options.storage.invoke(call.kind, async (session) => {
        host = new DatabaseHost({
          mode: call.kind,
          session,
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
          // Rust's finish(false): the engine failed the call, nothing commits.
          session.discard();
          if (error instanceof ModuleError) {
            const wire = error.wire(registry.moduleId, call.function);
            return { ok: false as const, error: moduleHostError(wire.code, wire.message, wire.retryable) };
          }
          throw error;
        }
      });
      const stats = callStats(outcome.stats, host);
      if (!outcome.ok) {
        // K01 refused the commit (a caught SQL failure, a deferred FK) or the body threw.
        if (outcome.error instanceof StorageError) {
          return { ok: false, error: hostCallError(outcome.error.message, stats.transientFault), committed: false, generation: generation.generation, stats };
        }
        throw outcome.error;
      }
      const result = outcome.value;
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
        stats: callStats(undefined, host),
      };
    } finally {
      lease.release();
    }
  }
}
