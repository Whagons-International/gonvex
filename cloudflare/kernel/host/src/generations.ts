// Module generations in the tenant object (CONTRACT.md B.6; 0005 decided by
// 0016 G0-2).
//
// Stage 1 loads the Query/Reducer bundle as part of the deployed Worker
// version: no eval, no dynamic code. A `/dev/sync` uploads a new Worker
// version (K19); hibernated objects adopt it on wake and active objects
// restart with it. Inside one object lifetime the registry still keeps Rust's
// generation semantics, so that S5's Worker Loader (in-place reload without
// dropping sockets) is a loading change only:
//
// - `load` validates the artifact (schemas, duplicates) and stages it under a strictly newer generation
//   (GENERATION_CONFLICT otherwise);
// - `activate` swaps the active generation atomically; calls that hold a lease
//   finish on the generation they started on; the retired generation is
//   reaped when its leases drain (or after `drainMs`, default 30 s);
// - activation invalidates the catalog cache and emits a `ModuleReloaded`
//   event. The session layer (K05) turns it into `system.reload` or
//   `client.updateRequired` + close per socket (RT/lib.rs:2479-2508);
// - an invocation may pin a generation: a newer one than the active is
//   UNKNOWN_GENERATION (retryable, "host behind"); an older pin is served by
//   the active generation, as in Rust.
import { ModuleEngine, type EngineConfig, type FunctionContract, type ModuleExports } from "./engine.js";
import { codes, wireError, type WireError } from "./errors.js";

export interface ModuleArtifact {
  moduleId: string;
  generation: number;
  artifactHash: string;
  /** Client contract version (`client.updateRequired` when a client's differs). */
  clientContract?: number | undefined;
  functions: readonly FunctionContract[];
  exports: ModuleExports;
  /** Module schema (manifest `schema`) for declared primary keys. */
  schema?: unknown;
}

export interface ModuleReloaded {
  moduleId: string;
  generation: number;
  artifactHash: string;
  clientContract: number | undefined;
  retired: number | undefined;
  /** `{type:"system.reload", reason:"module generation changed", artifactHash}` (protocol/src/lib.rs:933-942). */
  frame: { type: "system.reload"; reason: "module generation changed"; artifactHash: string };
}

export class Generation {
  leases = 0;
  retired = false;
  private drained: (() => void) | undefined;
  readonly engine: ModuleEngine;
  readonly declaredKeys: ReadonlyMap<string, string>;

  constructor(readonly artifact: ModuleArtifact, config: Partial<EngineConfig>) {
    // Rust validates schemas and duplicates at load and evaluates the bundle
    // (the Worker version already did); an export that does not resolve
    // fails only its own calls, at dispatch (InvalidArtifact).
    this.engine = new ModuleEngine(artifact.functions, artifact.exports, config);
    this.declaredKeys = declaredKeys(artifact.schema);
  }

  get generation() { return this.artifact.generation; }

  /** Registered Reducer or Action (scheduler targets, RT/host_calls.rs schedulable). */
  schedulable(path: string): boolean {
    const kind = this.engine.functions.get(path)?.kind;
    return kind === "reducer" || kind === "action";
  }

  release(): void {
    this.leases--;
    if (this.retired && this.leases === 0) this.drained?.();
  }

  waitForDrain(timeoutMs: number): Promise<boolean> {
    if (this.leases === 0) return Promise.resolve(true);
    return new Promise((resolve) => {
      const timer = setTimeout(() => resolve(false), timeoutMs);
      this.drained = () => { clearTimeout(timer); resolve(true); };
    });
  }
}

/** A held generation: release it when the invocation ends. */
export interface GenerationLease {
  generation: Generation;
  release(): void;
}

/** host_calls.rs declared_table_key: one `primaryKey: true` column per table in `tables`/`tenantTables`/`controlPlaneTables`. */
function declaredKeys(schema: unknown): Map<string, string> {
  const out = new Map<string, string>();
  if (!schema || typeof schema !== "object") return out;
  for (const scope of ["controlPlaneTables", "tenantTables", "tables"]) {
    const tables = (schema as Record<string, unknown>)[scope];
    if (!tables || typeof tables !== "object") continue;
    for (const [table, definition] of Object.entries(tables as Record<string, unknown>)) {
      const columns = (definition as { columns?: Record<string, { primaryKey?: unknown }> } | null)?.columns;
      if (!columns || typeof columns !== "object") continue;
      const keys = Object.entries(columns).filter(([, column]) => column?.primaryKey === true).map(([name]) => name);
      if (keys.length === 1) out.set(table, keys[0] as string);
    }
  }
  return out;
}

export class ModuleRegistry {
  private active: Generation | undefined;
  private readonly staged = new Map<number, Generation>();
  private readonly retired = new Set<Generation>();
  private issued = 0;
  private readonly listeners = new Set<(event: ModuleReloaded) => void>();

  constructor(readonly moduleId: string, private readonly config: Partial<EngineConfig> = {}, readonly drainMs = 30_000) {}

  get activeGeneration(): Generation | undefined { return this.active; }

  /** Validates and stages a generation. Throws a `WireError`. */
  load(artifact: ModuleArtifact): Generation {
    if (artifact.moduleId !== this.moduleId) throw wireError(codes.BAD_REQUEST, `module ${artifact.moduleId} is not served by module ${this.moduleId}`);
    const floor = Math.max(this.issued, this.active?.generation ?? 0);
    if (!Number.isSafeInteger(artifact.generation) || artifact.generation <= floor) {
      throw wireError(codes.GENERATION_CONFLICT, `module ${this.moduleId} generation ${artifact.generation} is not newer than the active one`);
    }
    let generation: Generation;
    try {
      generation = new Generation(artifact, this.config);
    } catch (error) {
      throw wireError(codes.MODULE_LOAD_FAILED, `module ${this.moduleId} generation ${artifact.generation} failed to load: ${(error as Error).message}`);
    }
    this.issued = artifact.generation;
    this.staged.set(artifact.generation, generation);
    return generation;
  }

  /** Makes a staged generation active. Returns the reload event (also sent to listeners). */
  activate(generationNumber: number): ModuleReloaded {
    const next = this.staged.get(generationNumber);
    if (!next) throw wireError(codes.UNKNOWN_GENERATION, `module ${this.moduleId} generation ${generationNumber} was never loaded`);
    if (this.active && generationNumber <= this.active.generation) {
      throw wireError(codes.GENERATION_CONFLICT, `module ${this.moduleId} generation ${generationNumber} is not newer than the active one`);
    }
    this.staged.delete(generationNumber);
    for (const staged of this.staged.keys()) if (staged < generationNumber) this.staged.delete(staged);
    const previous = this.active;
    this.active = next;
    if (previous) {
      previous.retired = true;
      this.retired.add(previous);
      void previous.waitForDrain(this.drainMs).then(() => this.retired.delete(previous));
    }
    const event: ModuleReloaded = {
      moduleId: this.moduleId,
      generation: next.generation,
      artifactHash: next.artifact.artifactHash,
      clientContract: next.artifact.clientContract,
      retired: previous?.generation,
      frame: { type: "system.reload", reason: "module generation changed", artifactHash: next.artifact.artifactHash },
    };
    for (const listener of this.listeners) listener(event);
    return event;
  }

  /** `load` + `activate` in one step (a Worker version starting with its bundle). */
  install(artifact: ModuleArtifact): ModuleReloaded {
    this.load(artifact);
    return this.activate(artifact.generation);
  }

  onReload(listener: (event: ModuleReloaded) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /** Generations still draining after activation. */
  draining(): number[] { return [...this.retired].map((generation) => generation.generation); }

  /** Leases the active generation for one invocation. Throws a `WireError`. */
  lease(pinned?: number): GenerationLease {
    const active = this.active;
    if (!active) throw wireError(codes.MODULE_NOT_LOADED, `module ${this.moduleId} has no active generation`);
    if (pinned !== undefined && active.generation < pinned) {
      throw wireError(codes.UNKNOWN_GENERATION, `module ${this.moduleId} generation ${pinned} is not loaded; the host is on ${active.generation}`);
    }
    active.leases++;
    let released = false;
    return { generation: active, release: () => { if (!released) { released = true; active.release(); } } };
  }
}

export function isWireError(error: unknown): error is WireError {
  return typeof error === "object" && error !== null && typeof (error as WireError).code === "string" && typeof (error as WireError).message === "string" && typeof (error as WireError).retryable === "boolean";
}
