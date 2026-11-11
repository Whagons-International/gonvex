// One sandbox: Rust's `Workspace` (RT/sandbox.rs:39-57) as a Durable Object.
//
// - Files and execution records live in key-value storage (blobs.ts), never in
//   SQL, so module SQL in the analysis object cannot reach them.
// - The object's name binds the sandbox to its (project, tenant, account), and
//   the stored scope is checked again on every call: another tenant's or
//   account's call reaches a different, empty object and gets "sandbox was not
//   found".
// - The alarm deletes everything at `expiresAt` (the TTL).
// - A run executes module code in a Dynamic Worker with no network and no
//   bindings; its only capability is the `ExecutionHost` passed to it.
import { answer, unwrap, type Answer } from "./answer";
import { DurableObject, RpcTarget } from "cloudflare:workers";
import { deleteBlob, getBlob, putBlob } from "./blobs";
import type { ExecutionStatus, ImportResult, SandboxExecution, SandboxScope } from "./backend";
import { PLATFORM_ISOLATE_BYTES, type SandboxConfig } from "./config";
import { REGISTRY_NAME, RUNNER_COMPATIBILITY_DATE } from "./constants";
import type { SandboxEnv } from "./env";
import { NOT_FOUND, PATH_ESCAPES, bounded, workspacePath } from "./payload";
import { RUNNER_MODULE, compileProgram } from "./program";

interface Meta {
  sandboxId: string;
  scope: SandboxScope;
  duckdb: boolean;
  expiresAt: number;
  /** Bytes under `files/` (Rust's `directory_size(root/files)`). */
  fileBytes: number;
  /** The analysis database's size after its last write. */
  analysisBytes: number;
  imports: { alias: string; tables: unknown }[];
  executions: string[];
  /** The execution in progress and the object instance that runs it. */
  active: { executionId: string; instance: string } | null;
}

interface FileEntry { size: number; chunks: number }

interface ExecutionRecord {
  sandboxId: string;
  executionId: string;
  status: ExecutionStatus;
  startedAt: number;
  finishedAt: number | null;
  error: string;
  /** JSON `{result, logs}` in a blob, once the run has finished. */
  payloadBytes: number;
  payloadChunks: number;
}

type Outcome = { ok: true; result: unknown; logs: unknown } | { ok: false; error: string };

const META = "meta";
const FILE = "file:";
const DATA = "data:";
const EXEC = "exec:";
const OUTPUT = "output:";
const CANCELLED = "sandbox execution cancelled";
const ENOENT = "No such file or directory (os error 2)";
const ENOTDIR = "Not a directory (os error 20)";
const EISDIR = "Is a directory (os error 21)";
/** How often a queued run asks the registry for a slot. */
const ADMISSION_POLL_MS = 100;

export const scopeKey = (scope: SandboxScope): string => JSON.stringify([scope.project, scope.tenant, scope.account]);
export const workspaceName = (scope: SandboxScope, sandboxId: string): string => `${scopeKey(scope)}/${sandboxId}`;

const sameScope = (a: SandboxScope, b: SandboxScope) => a.project === b.project && a.tenant === b.tenant && a.account === b.account;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const deadlineText = (timeoutMs: number) => `module execution exceeded its budget: module exceeded its ${timeoutMs} ms execution deadline`;

/** Maps what the platform reports when it stops the Dynamic Worker to the V8 engine's texts. */
export function platformFailure(message: string, timeoutMs: number): string {
  if (/exceeded CPU time limit/i.test(message)) return deadlineText(timeoutMs);
  if (/exceeded memory limit/i.test(message)) {
    return `module execution exceeded its budget: module exceeded the ${PLATFORM_ISOLATE_BYTES} byte isolate heap limit`;
  }
  return `module execution failed: ${message}`;
}

export class SandboxWorkspace extends DurableObject<SandboxEnv> {
  /** Identifies this instance of the object, so a run left by a reset instance is recognised. */
  private readonly instance = crypto.randomUUID();
  private meta: Meta | null | undefined;
  private cancelActive: (() => void) | null = null;
  private readonly reservedAliases = new Set<string>();

  // ---- state -----------------------------------------------------------------------------

  private async load(): Promise<Meta | null> {
    if (this.meta === undefined) this.meta = (await this.ctx.storage.get<Meta>(META)) ?? null;
    const meta = this.meta;
    if (meta?.active && meta.active.instance !== this.instance) {
      // The instance that ran it is gone, which drops Rust's run future: `ExecutionOwnership`
      // marks the execution cancelled (RT/sandbox.rs:61-108).
      const record = await this.ctx.storage.get<ExecutionRecord>(EXEC + meta.active.executionId);
      if (record && record.status === "running") {
        await this.ctx.storage.put<ExecutionRecord>(EXEC + record.executionId, { ...record, status: "cancelled", finishedAt: Date.now(), error: CANCELLED });
      }
      meta.active = null;
      await this.save(meta);
    }
    return meta;
  }

  private async save(meta: Meta): Promise<void> {
    this.meta = meta;
    await this.ctx.storage.put(META, meta);
  }

  /** `owned_workspace`: the sandbox exists, belongs to `scope` and has not expired. */
  private async owned(scope: SandboxScope): Promise<Meta> {
    const meta = await this.load();
    if (!meta || !sameScope(meta.scope, scope) || meta.expiresAt <= Date.now()) throw new Error(NOT_FOUND);
    return meta;
  }

  private registry() {
    return this.env.SANDBOX_REGISTRY.getByName(REGISTRY_NAME);
  }

  private analysis(meta: Meta) {
    return this.env.SANDBOX_ANALYSIS.getByName(workspaceName(meta.scope, meta.sandboxId));
  }

  // ---- create, destroy, TTL --------------------------------------------------------------

  async create(request: { scope: SandboxScope; sandboxId: string; duckdb: boolean; expiresAt: number }): Promise<Answer<void>> {
    return answer(() => this.createNow(request));
  }

  private async createNow(request: { scope: SandboxScope; sandboxId: string; duckdb: boolean; expiresAt: number }): Promise<void> {
    if (await this.load()) throw new Error("sandbox id is already in use");
    await this.save({
      sandboxId: request.sandboxId,
      scope: request.scope,
      duckdb: request.duckdb,
      expiresAt: request.expiresAt,
      fileBytes: 0,
      analysisBytes: 0,
      imports: [],
      executions: [],
      active: null,
    });
    await this.ctx.storage.setAlarm(request.expiresAt);
  }

  async destroy(scope: SandboxScope): Promise<Answer<{ sandboxId: string; destroyed: true }>> {
    return answer(() => this.destroyNow(scope));
  }

  private async destroyNow(scope: SandboxScope): Promise<{ sandboxId: string; destroyed: true }> {
    const meta = await this.load();
    // Rust's prune drops an expired workspace unless it is running.
    if (!meta || !sameScope(meta.scope, scope) || (meta.expiresAt <= Date.now() && !meta.active)) throw new Error(NOT_FOUND);
    if (meta.active) throw new Error("cannot destroy a sandbox with an active execution");
    await this.wipe(meta);
    return { sandboxId: meta.sandboxId, destroyed: true };
  }

  override async alarm(): Promise<void> {
    const meta = await this.load();
    if (!meta) return;
    if (meta.expiresAt > Date.now()) {
      await this.ctx.storage.setAlarm(meta.expiresAt);
      return;
    }
    // Rust's prune keeps a running workspace until its run ends.
    if (meta.active) {
      await this.ctx.storage.setAlarm(Date.now() + 1_000);
      return;
    }
    await this.wipe(meta);
  }

  private async wipe(meta: Meta): Promise<void> {
    await this.analysis(meta).destroy();
    await this.registry().forget(meta.sandboxId);
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
    this.meta = null;
  }

  // ---- files -----------------------------------------------------------------------------

  /** A missing file, a directory or a file used as a directory, with the errno text Rust's `fs` gives. */
  private async locate(path: string): Promise<FileEntry> {
    const entry = await this.ctx.storage.get<FileEntry>(FILE + path);
    if (entry) return entry;
    const parts = path.split("/");
    for (let i = 1; i < parts.length; i++) {
      if (await this.ctx.storage.get(FILE + parts.slice(0, i).join("/"))) throw new Error(ENOTDIR);
    }
    if ((await this.ctx.storage.list({ prefix: `${FILE}${path}/`, limit: 1 })).size > 0) throw new Error(EISDIR);
    throw new Error(ENOENT);
  }

  private async readPath(path: string): Promise<Uint8Array> {
    const entry = await this.locate(path);
    return getBlob(this.ctx.storage, DATA + path, entry.chunks, entry.size);
  }

  /** Where a write may go: no parent is a file and the path is not a directory. Answers the old size. */
  private async writable(path: string): Promise<number> {
    const parts = path.split("/");
    for (let i = 1; i < parts.length; i++) {
      if (await this.ctx.storage.get(FILE + parts.slice(0, i).join("/"))) throw new Error(ENOTDIR);
    }
    const entry = await this.ctx.storage.get<FileEntry>(FILE + path);
    if (entry) return entry.size;
    if ((await this.ctx.storage.list({ prefix: `${FILE}${path}/`, limit: 1 })).size > 0) throw new Error(EISDIR);
    return 0;
  }

  private async writePath(meta: Meta, path: string, content: Uint8Array): Promise<void> {
    const old = await this.ctx.storage.get<FileEntry>(FILE + path);
    if (old) await deleteBlob(this.ctx.storage, DATA + path, old.chunks);
    const chunks = await putBlob(this.ctx.storage, DATA + path, content);
    await this.ctx.storage.put<FileEntry>(FILE + path, { size: content.byteLength, chunks });
    meta.fileBytes += content.byteLength - (old?.size ?? 0);
    await this.save(meta);
  }

  /** `read_file` (RT/sandbox.rs:515-537): the bytes; text decoding is the caller's. */
  async readFile(scope: SandboxScope, relative: string, config: SandboxConfig): Promise<Answer<Uint8Array>> {
    return answer(() => this.readFileNow(scope, relative, config));
  }

  private async readFileNow(scope: SandboxScope, relative: string, config: SandboxConfig): Promise<Uint8Array> {
    await this.owned(scope);
    const path = workspacePath(relative);
    const bytes = await this.readPath(path);
    if (bytes.byteLength > config.maxFileBytes) throw new Error("sandbox file exceeds runtime policy");
    return bytes;
  }

  /** `write_file` (RT/sandbox.rs:539-588), after the caller's size check. */
  async writeFile(scope: SandboxScope, relative: string, content: Uint8Array, config: SandboxConfig): Promise<Answer<number>> {
    return answer(() => this.writeFileNow(scope, relative, content, config));
  }

  private async writeFileNow(scope: SandboxScope, relative: string, content: Uint8Array, config: SandboxConfig): Promise<number> {
    const meta = await this.owned(scope);
    const path = workspacePath(relative);
    const prior = await this.writable(path);
    if (meta.fileBytes - prior + content.byteLength > config.maxWorkspaceBytes) throw new Error("sandbox workspace byte limit exceeded");
    await this.writePath(meta, path, content);
    return content.byteLength;
  }

  // ---- imports ---------------------------------------------------------------------------

  /** `import_file` (RT/sandbox.rs:590-626) after the caller's name and size checks. */
  async importFile(scope: SandboxScope, request: { filename: string; body: ReadableStream<Uint8Array>; size: number }, config: SandboxConfig): Promise<Answer<ImportResult>> {
    return answer(() => this.importFileNow(scope, request, config));
  }

  private async importFileNow(scope: SandboxScope, request: { filename: string; body: ReadableStream<Uint8Array>; size: number }, config: SandboxConfig): Promise<ImportResult> {
    let meta: Meta;
    try {
      meta = await this.owned(scope);
      if (!meta.duckdb) throw new Error("sandbox importFile requires capabilities.sandbox.duckdb");
    } catch (error) {
      await request.body.cancel().catch(() => undefined);
      throw error;
    }
    let number = meta.imports.length + 1;
    while (this.reservedAliases.has(`import_${number}`)) number++;
    const alias = `import_${number}`;
    this.reservedAliases.add(alias);
    try {
      const ingested = unwrap(await this.analysis(meta).ingest({ alias, filename: request.filename, body: request.body, maxRows: config.maxRows }));
      const current = await this.owned(scope);
      current.imports.push({ alias, tables: ingested.tables });
      current.analysisBytes = ingested.databaseSize;
      await this.save(current);
      return { alias, tables: ingested.tables };
    } finally {
      this.reservedAliases.delete(alias);
    }
  }

  // ---- executions ------------------------------------------------------------------------

  private async record(scope: SandboxScope, executionId: string): Promise<ExecutionRecord> {
    await this.owned(scope);
    const record = await this.ctx.storage.get<ExecutionRecord>(EXEC + executionId);
    if (!record) throw new Error("sandbox execution was not found");
    return record;
  }

  private async json(record: ExecutionRecord): Promise<SandboxExecution> {
    let payload: { result?: unknown; logs?: unknown } = {};
    if (record.payloadChunks > 0) {
      payload = JSON.parse(new TextDecoder().decode(await getBlob(this.ctx.storage, OUTPUT + record.executionId, record.payloadChunks, record.payloadBytes)));
    }
    return {
      sandboxId: record.sandboxId,
      executionId: record.executionId,
      status: record.status,
      startedAt: record.startedAt,
      finishedAt: record.finishedAt,
      result: payload.result ?? null,
      error: record.error,
      logs: payload.logs ?? [],
    };
  }

  async status(scope: SandboxScope, executionId: string): Promise<Answer<SandboxExecution>> {
    return answer(() => this.statusNow(scope, executionId));
  }

  private async statusNow(scope: SandboxScope, executionId: string): Promise<SandboxExecution> {
    return this.json(await this.record(scope, executionId));
  }

  /** Signals a running execution and answers its state as it is now (RT/sandbox.rs:486-500). */
  async cancel(scope: SandboxScope, executionId: string): Promise<Answer<SandboxExecution>> {
    return answer(() => this.cancelNow(scope, executionId));
  }

  private async cancelNow(scope: SandboxScope, executionId: string): Promise<SandboxExecution> {
    const record = await this.record(scope, executionId);
    if (record.status === "running" && this.meta?.active?.executionId === executionId) this.cancelActive?.();
    return this.json(record);
  }

  /** `run` (RT/sandbox.rs:303-423): admission, one Dynamic Worker, the record. Answers when the run ends. */
  async run(scope: SandboxScope, request: { code: string; timeoutMs: number }, config: SandboxConfig): Promise<Answer<SandboxExecution>> {
    return answer(() => this.runNow(scope, request, config));
  }

  private async runNow(scope: SandboxScope, request: { code: string; timeoutMs: number }, config: SandboxConfig): Promise<SandboxExecution> {
    const meta = await this.owned(scope);
    if (meta.active) throw new Error("sandbox already has an active execution");
    if (meta.executions.length >= config.maxExecutions) throw new Error("sandbox execution limit reached");
    const executionId = `run_${crypto.randomUUID()}`;
    const record: ExecutionRecord = {
      sandboxId: meta.sandboxId,
      executionId,
      status: "running",
      startedAt: Date.now(),
      finishedAt: null,
      error: "",
      payloadBytes: 0,
      payloadChunks: 0,
    };
    meta.executions.push(executionId);
    meta.active = { executionId, instance: this.instance };
    await this.ctx.storage.put<ExecutionRecord>(EXEC + executionId, record);
    await this.save(meta);

    let cancelled = false;
    let onCancel: () => void = () => undefined;
    const cancelSignal = new Promise<"cancelled">((resolve) => {
      onCancel = () => resolve("cancelled");
    });
    this.cancelActive = () => {
      cancelled = true;
      onCancel();
    };

    let outcome: Outcome;
    let admitted = false;
    try {
      // `admission.acquire_owned()` raced against a cancel (sandbox.rs:357-363).
      const until = Date.now() + request.timeoutMs + 30_000;
      while (!cancelled && !(admitted = await this.registry().acquire({ executionId, until, concurrency: config.concurrency }))) {
        await Promise.race([sleep(ADMISSION_POLL_MS), cancelSignal]);
      }
      outcome = cancelled ? { ok: false, error: CANCELLED } : await this.execute(meta, executionId, request, config, cancelSignal);
    } catch (error) {
      outcome = { ok: false, error: error instanceof Error ? error.message : String(error) };
    } finally {
      this.cancelActive = null;
      if (admitted) await this.registry().release(executionId).catch(() => undefined);
    }

    const current = (await this.load()) ?? meta;
    if (current.active?.executionId === executionId) current.active = null;
    record.finishedAt = Date.now();
    if (outcome.ok) {
      record.status = "succeeded";
      const payload = new TextEncoder().encode(JSON.stringify({ result: outcome.result ?? null, logs: outcome.logs ?? [] }));
      record.payloadBytes = payload.byteLength;
      record.payloadChunks = await putBlob(this.ctx.storage, OUTPUT + executionId, payload);
    } else {
      record.status = outcome.error === CANCELLED ? "cancelled" : "failed";
      record.error = bounded(outcome.error, 4_000);
    }
    await this.ctx.storage.put<ExecutionRecord>(EXEC + executionId, record);
    await this.save(current);
    return this.json(record);
  }

  /** The sandbox worker's `run()` (main.rs:105-210), in a Dynamic Worker. */
  private async execute(meta: Meta, executionId: string, request: { code: string; timeoutMs: number }, config: SandboxConfig, cancelSignal: Promise<"cancelled">): Promise<Outcome> {
    if (request.timeoutMs === 0) return { ok: false, error: "worker timeout is required" };
    let program: string;
    try {
      program = compileProgram(request.code, meta.duckdb);
    } catch (error) {
      return { ok: false, error: (error as Error).message };
    }
    const deadline = Date.now() + request.timeoutMs;
    let expire: () => void = () => undefined;
    const expired = new Promise<"expired">((resolve) => {
      expire = () => resolve("expired");
    });
    const host = new ExecutionHost(this, meta, config, deadline, expire);
    const worker = this.env.LOADER.load({
      compatibilityDate: RUNNER_COMPATIBILITY_DATE,
      mainModule: "runner.js",
      modules: { "runner.js": RUNNER_MODULE, "program.js": program },
      globalOutbound: null,
      env: {},
      // CPU never exceeds wall time, so the platform stops a spinning isolate by
      // the deadline even when it never yields to the timer below.
      limits: { cpuMs: request.timeoutMs },
    });
    const runner = worker.getEntrypoint() as unknown as {
      run(host: ExecutionHost, info: { now: number; maxResultBytes: number }): Promise<{ ok: true; json: string } | { ok: false; error: string }>;
    };
    let timer: number | undefined;
    const timeout = new Promise<"expired">((resolve) => {
      timer = setTimeout(() => resolve("expired"), request.timeoutMs) as unknown as number;
    });
    try {
      const answer = await Promise.race([
        runner.run(host, { now: Date.now(), maxResultBytes: config.maxOutputBytes }).then(
          (value) => ({ value }),
          (error: unknown) => ({ error: error instanceof Error ? error.message : String(error) }),
        ),
        timeout,
        expired,
        cancelSignal,
      ]);
      if (answer === "expired") return { ok: false, error: deadlineText(request.timeoutMs) };
      if (answer === "cancelled") return { ok: false, error: CANCELLED };
      if ("error" in answer) return { ok: false, error: platformFailure(answer.error, request.timeoutMs) };
      if (!answer.value.ok) return { ok: false, error: answer.value.error };
      const envelope = JSON.parse(answer.value.json) as { result?: unknown; logs?: unknown } | null;
      const result = envelope?.result ?? null;
      const logs = envelope?.logs ?? [];
      // The worker's whole response is capped again by the manager (sandbox.rs:480-482).
      const response = JSON.stringify({ ok: true, result, error: "", logs });
      if (new TextEncoder().encode(response).byteLength > config.maxOutputBytes) return { ok: false, error: "sandbox output exceeds runtime policy" };
      return { ok: true, result, logs };
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      host.close();
    }
  }

  // ---- what a run may call (sandbox-worker `WorkerHost::dispatch`, main.rs:313-500) -------

  async workerReadText(path: string, config: SandboxConfig): Promise<string> {
    const key = workerPath(path);
    const bytes = await this.readPath(key);
    if (bytes.byteLength > config.maxFileBytes) throw new Error("sandbox file exceeds read limit");
    try {
      return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
    } catch {
      throw new Error("stream did not contain valid UTF-8");
    }
  }

  async workerWriteText(meta: Meta, path: string, content: string, config: SandboxConfig): Promise<{ path: string; size: number }> {
    const bytes = new TextEncoder().encode(content);
    if (bytes.byteLength > config.maxFileBytes) throw new Error("sandbox file exceeds write limit");
    const key = workerPath(path);
    const old = await this.writable(key);
    // The worker measures its whole root: files, imports and the analysis database.
    const current = (await this.load()) ?? meta;
    if (current.fileBytes + current.analysisBytes - old + bytes.byteLength > config.maxWorkspaceBytes) throw new Error("sandbox workspace byte limit exceeded");
    await this.writePath(current, key, bytes);
    return { path, size: bytes.byteLength };
  }

  async workerListFiles(): Promise<string[]> {
    const keys = [...(await this.ctx.storage.list({ prefix: FILE })).keys()].map((key) => key.slice(FILE.length));
    return keys.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  }

  async workerAnalysis(meta: Meta, operation: "query" | "register", payload: Record<string, unknown>, config: SandboxConfig): Promise<unknown> {
    const current = (await this.load()) ?? meta;
    const budget = config.maxWorkspaceBytes - current.fileBytes;
    const reply = unwrap(await this.analysis(current).call({ operation, payload, maxRows: config.maxRows, budgetBytes: budget }));
    current.analysisBytes = reply.databaseSize;
    await this.save(current);
    return JSON.parse(reply.json) as unknown;
  }
}

/** The worker's `file_path`: same rule, same text. */
function workerPath(path: string): string {
  try {
    return workspacePath(path);
  } catch {
    throw new Error(PATH_ESCAPES);
  }
}

/** `required_string` in the worker: a non-empty string. */
function workerString(payload: Record<string, unknown>, key: string): string {
  const value = payload[key];
  if (typeof value !== "string" || value === "") throw new Error(`${key} must be a non-empty string`);
  return value;
}

/**
 * The only thing module code can reach. One per execution; closed when the
 * execution ends, so a run that outlives its deadline (a Dynamic Worker the
 * platform has not stopped yet) can no longer read or write anything.
 */
export class ExecutionHost extends RpcTarget {
  private closed = false;

  constructor(
    private readonly workspace: SandboxWorkspace,
    private readonly meta: Meta,
    private readonly config: SandboxConfig,
    private readonly deadline: number,
    private readonly expire: () => void,
  ) {
    super();
  }

  close(): void {
    this.closed = true;
  }

  /** One host call: `{ok, value}` with the value as JSON, or `{ok:false, message}` as the V8 engine words it. */
  async call(operation: string, payloadJson: string): Promise<{ ok: true; value?: string } | { ok: false; message: string }> {
    try {
      if (this.closed) throw new Error("sandbox execution is no longer active");
      if (Date.now() >= this.deadline) {
        this.expire();
        throw new Error("sandbox execution is no longer active");
      }
      const value = await this.dispatch(operation, JSON.parse(payloadJson) as unknown);
      if (this.closed) throw new Error("sandbox execution is no longer active");
      return value === undefined ? { ok: true } : { ok: true, value: JSON.stringify(value) };
    } catch (error) {
      return { ok: false, message: `host call failed: ${error instanceof Error ? error.message : String(error)}` };
    }
  }

  private async dispatch(operation: string, raw: unknown): Promise<unknown> {
    const payload = (raw !== null && typeof raw === "object" && !Array.isArray(raw) ? raw : {}) as Record<string, unknown>;
    switch (operation) {
      case "worker.readText":
        return this.workspace.workerReadText(workerString(payload, "path"), this.config);
      case "worker.writeText":
        return this.workspace.workerWriteText(this.meta, workerString(payload, "path"), workerString(payload, "content"), this.config);
      case "worker.listFiles":
        return this.workspace.workerListFiles();
      case "worker.query":
      case "worker.register":
        if (!this.meta.duckdb) throw new Error("DuckDB was not enabled for this sandbox");
        return this.workspace.workerAnalysis(this.meta, operation === "worker.query" ? "query" : "register", payload, this.config);
      default:
        throw new Error(`unsupported sandbox worker operation ${JSON.stringify(operation)}`);
    }
  }
}
