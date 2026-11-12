// The sandbox manager inside the K20 Container: Rust's `SandboxManager`
// (rust/crates/runtime/src/sandbox.rs at 0bf3ad13) as a small HTTP server, so
// the unchanged `gonvex-sandbox-worker` (with DuckDB) runs exactly as the Rust
// runtime runs it: one process per execution, the same request JSON, the same
// workspace layout (`<root>/<sandboxId>/{files,imports}`), the same limits and
// texts. `importFile` runs `k20-ingest`, which is Rust's `data_ingest.rs`
// compiled unchanged.
//
// The Worker side (cloudflare/sandbox/src/service.ts) has already applied
// `exact_fields` and the checks Rust makes before it looks a workspace up; each
// operation here starts where Rust's does after those checks.
//
// One Container serves one tenant (cloudflare/sandbox/src/container.ts), so the
// admission semaphore is per tenant (0048 §2.3). It is keyed by tenant anyway,
// so a shared manager (local tests) keeps tenants apart.
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { constants as osConstants } from "node:os";
import { createServer } from "node:http";
import { chown, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { join, posix } from "node:path";

const ROOT = process.env.GONVEX_SANDBOX_ROOT ?? "/var/lib/gonvex/sandboxes";
const WORKER = process.env.GONVEX_SANDBOX_WORKER_BINARY ?? "/usr/local/bin/gonvex-sandbox-worker";
const INGEST = process.env.K20_INGEST_BINARY ?? "/usr/local/bin/k20-ingest";
const PORT = Number(process.env.PORT ?? 8080);
/** Rust refuses unconfined mode outside local development (main.rs:729-731). */
const ALLOW_UNCONFINED = /^(1|true|yes|on)$/i.test(process.env.GONVEX_SANDBOX_ALLOW_UNCONFINED ?? "");
const WORKER_UID = 65_534;
const WORKER_GID = 65_534;
const DUCKDB_MEMORY_BYTES = 128 << 20;

/** @type {Map<string, Workspace>} */
const workspaces = new Map();
/** Per-tenant admission (Rust: one `Semaphore` of `concurrency` permits). */
const admissions = new Map();
let started = 0;
let finished = 0;

/**
 * @typedef {{project: string, tenant: string, account: string}} Scope
 * @typedef {{status: string, startedAt: number, finishedAt: number | null, result: unknown, error: string, logs: unknown, cancel: () => void}} Execution
 * @typedef {{scope: Scope, root: string, duckdb: boolean, expiresAt: number, executions: Map<string, Execution>, active: string | null, imports: unknown[]}} Workspace
 */

class Failure extends Error {}
const fail = (message) => { throw new Failure(message); };
const sameScope = (a, b) => a.project === b.project && a.tenant === b.tenant && a.account === b.account;

/** Rust's `io::Error` Display for an errno: "No such file or directory (os error 2)". */
function ioError(error) {
  const code = error?.code;
  const errno = code ? osConstants.errno[code] : undefined;
  const texts = {
    ENOENT: "No such file or directory", EISDIR: "Is a directory", ENOTDIR: "Not a directory", EACCES: "Permission denied",
    EEXIST: "File exists", ENOSPC: "No space left on device", EFBIG: "File too large", EPERM: "Operation not permitted",
    ENAMETOOLONG: "File name too long", ENOTEMPTY: "Directory not empty",
  };
  if (errno !== undefined && texts[code]) return `${texts[code]} (os error ${errno})`;
  return String(error?.message ?? error);
}

/** `safe_file`: a relative path of normal components (Rust's `Path::components`). */
function safeFile(root, relative) {
  const escapes = "sandbox path must be relative and cannot escape the workspace";
  if (relative === "" || relative.startsWith("/")) fail(escapes);
  const parts = relative.split("/");
  if (parts[0] === ".") fail(escapes);
  const normal = parts.filter((part) => part !== "" && part !== ".");
  if (normal.length === 0 || normal.includes("..")) fail(escapes);
  return join(root, ...normal);
}

async function directorySize(root) {
  let total = 0;
  for (const entry of await readdir(root, { withFileTypes: true }).catch((error) => fail(ioError(error)))) {
    const path = join(root, entry.name);
    total += entry.isDirectory() ? await directorySize(path) : (await stat(path).catch((error) => fail(ioError(error)))).size;
  }
  return total;
}

/** `owned_workspace`. */
function owned(scope, id) {
  const workspace = workspaces.get(id);
  if (!workspace || !sameScope(workspace.scope, scope) || workspace.expiresAt <= Date.now()) fail("sandbox was not found");
  return workspace;
}

/** `prune`: expired workspaces without an active execution. */
async function prune() {
  const now = Date.now();
  const roots = [];
  for (const [id, workspace] of workspaces) {
    if (workspace.expiresAt <= now && workspace.active === null) {
      workspaces.delete(id);
      roots.push(workspace.root);
    }
  }
  await Promise.all(roots.map((root) => rm(root, { recursive: true, force: true })));
}

const executionJson = (sandboxId, executionId, execution) => ({
  sandboxId, executionId, status: execution.status, startedAt: execution.startedAt, finishedAt: execution.finishedAt,
  result: execution.result, error: execution.error, logs: execution.logs,
});

const bounded = (value, max) => [...value].slice(0, max).join("");

/** A permit of the tenant's semaphore, raced with a cancel. Null when cancelled first. */
function acquire(tenant, concurrency, cancelled) {
  let admission = admissions.get(tenant);
  if (!admission) admissions.set(tenant, (admission = { active: 0, waiting: [] }));
  if (admission.active < concurrency) {
    admission.active++;
    return Promise.resolve(() => release(tenant));
  }
  return new Promise((resolve) => {
    const waiter = { concurrency, grant: () => { admission.active++; resolve(() => release(tenant)); } };
    admission.waiting.push(waiter);
    cancelled.then(() => {
      const index = admission.waiting.indexOf(waiter);
      if (index >= 0) { admission.waiting.splice(index, 1); resolve(null); }
    });
  });
}

function release(tenant) {
  const admission = admissions.get(tenant);
  if (!admission) return;
  admission.active--;
  const next = admission.waiting[0];
  if (next && admission.active < next.concurrency) {
    admission.waiting.shift();
    next.grant();
  }
}

/** `execute_worker`: one worker process, its stdout, the outer timeout (+2 s) and cancel. */
function executeWorker(request, timeoutMs, cancelled, maxOutputBytes) {
  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawn(WORKER, [], { stdio: ["pipe", "pipe", "ignore"] });
    } catch (error) {
      reject(new Failure(`start sandbox worker: ${ioError(error)}`));
      return;
    }
    const chunks = [];
    let done = false;
    const finish = (outcome) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      if (child.exitCode === null) child.kill("SIGKILL");
      outcome();
    };
    const timer = setTimeout(() => finish(() => reject(new Failure("sandbox execution timed out"))), timeoutMs + 2_000);
    cancelled.then(() => finish(() => reject(new Failure("sandbox execution cancelled"))));
    child.on("error", (error) => finish(() => reject(new Failure(`start sandbox worker: ${ioError(error)}`))));
    child.stdout.on("data", (chunk) => chunks.push(chunk));
    child.on("close", () => finish(() => {
      const bytes = Buffer.concat(chunks);
      if (bytes.length > maxOutputBytes) return reject(new Failure("sandbox output exceeds runtime policy"));
      try {
        resolve(JSON.parse(bytes.toString("utf8")));
      } catch (error) {
        reject(new Failure(`invalid sandbox response: ${error.message}`));
      }
    }));
    child.stdin.on("error", () => undefined);
    child.stdin.end(JSON.stringify(request));
  });
}

const operations = {
  /** `create` after the policy, field and TTL checks; the registry has admitted the counts. */
  async create({ scope, sandboxId, ttlMs, duckdb }) {
    // The Worker names the sandbox, so the deployment's registry can count it first.
    const id = typeof sandboxId === "string" && /^sbx_[0-9a-f-]{36}$/.test(sandboxId) ? sandboxId : `sbx_${randomUUID()}`;
    if (workspaces.has(id)) fail("sandbox id is already in use");
    const root = join(ROOT, id);
    await mkdir(join(root, "files"), { recursive: true }).catch((error) => fail(ioError(error)));
    await mkdir(join(root, "imports"), { recursive: true }).catch((error) => fail(ioError(error)));
    // The worker chroots into `root` and drops to 65534 (main.rs:700-733). It must
    // be able to create `analysis.duckdb` and write `files/`; Rust's runtime left
    // the root owned by root, which is the "Permission denied" staging shows (note 0047 §6).
    if (typeof process.getuid === "function" && process.getuid() === 0) {
      for (const path of [root, join(root, "files")]) await chown(path, WORKER_UID, WORKER_GID);
    }
    const expiresAt = Date.now() + ttlMs;
    workspaces.set(id, { scope, root, duckdb, expiresAt, executions: new Map(), active: null, imports: [] });
    return { sandboxId: id, expiresAt, duckdb };
  },

  async destroy({ scope, sandboxId }) {
    const workspace = workspaces.get(sandboxId);
    if (!workspace || !sameScope(workspace.scope, scope)) fail("sandbox was not found");
    if (workspace.active !== null) fail("cannot destroy a sandbox with an active execution");
    workspaces.delete(sandboxId);
    await rm(workspace.root, { recursive: true, force: true }).catch((error) => fail(ioError(error)));
    return { sandboxId, destroyed: true };
  },

  /** `run` (sandbox.rs:303-423) after the field, code-size and timeout checks. */
  async run({ scope, sandboxId, code, timeoutMs, config }) {
    const workspace = owned(scope, sandboxId);
    if (workspace.active !== null) fail("sandbox already has an active execution");
    if (workspace.executions.size >= config.maxExecutions) fail("sandbox execution limit reached");
    const executionId = `run_${randomUUID()}`;
    let cancel = () => undefined;
    const cancelled = new Promise((resolve) => { cancel = resolve; });
    /** @type {Execution} */
    const execution = { status: "running", startedAt: Date.now(), finishedAt: null, result: null, error: "", logs: [], cancel };
    workspace.executions.set(executionId, execution);
    workspace.active = executionId;
    started++;
    let response;
    let releasePermit = null;
    try {
      releasePermit = await acquire(scope.tenant, config.concurrency, cancelled);
      if (releasePermit === null) throw new Failure("sandbox execution cancelled");
      const request = {
        version: 1, root: workspace.root, allowUnconfined: ALLOW_UNCONFINED, code, duckdb: workspace.duckdb, imports: workspace.imports,
        maxHeapBytes: config.maxHeapBytes, maxFileBytes: config.maxFileBytes, maxWorkspaceBytes: config.maxWorkspaceBytes,
        maxOutputBytes: config.maxOutputBytes, maxRows: config.maxRows, duckdbMemoryBytes: DUCKDB_MEMORY_BYTES, timeoutMs,
        workerUid: WORKER_UID, workerGid: WORKER_GID,
      };
      response = { ok: true, value: await executeWorker(request, timeoutMs, cancelled, config.maxOutputBytes) };
    } catch (error) {
      response = { ok: false, error: error instanceof Failure ? error.message : String(error?.message ?? error) };
    } finally {
      releasePermit?.();
      finished++;
    }
    // `owned_workspace` again: a workspace that expired meanwhile answers "not found", and
    // `ExecutionOwnership` marks the execution cancelled (sandbox.rs:61-108, 382-383).
    workspace.active = null;
    try {
      owned(scope, sandboxId);
    } catch (error) {
      Object.assign(execution, { status: "cancelled", finishedAt: Date.now(), error: "sandbox execution cancelled" });
      throw error;
    }
    execution.finishedAt = Date.now();
    if (response.ok) {
      const value = response.value ?? {};
      execution.status = "succeeded";
      execution.result = value.result ?? null;
      execution.logs = value.logs ?? [];
      if (value.ok !== true) {
        execution.status = "failed";
        execution.error = bounded(typeof value.error === "string" ? value.error : "sandbox failed", 4_000);
      }
    } else {
      execution.status = response.error === "sandbox execution cancelled" ? "cancelled" : "failed";
      execution.error = bounded(response.error, 4_000);
    }
    return executionJson(sandboxId, executionId, execution);
  },

  async cancel({ scope, sandboxId, executionId }) {
    const workspace = owned(scope, sandboxId);
    const execution = workspace.executions.get(executionId) ?? fail("sandbox execution was not found");
    if (execution.status === "running") execution.cancel();
    return executionJson(sandboxId, executionId, execution);
  },

  async status({ scope, sandboxId, executionId }) {
    const workspace = owned(scope, sandboxId);
    const execution = workspace.executions.get(executionId) ?? fail("sandbox execution was not found");
    return executionJson(sandboxId, executionId, execution);
  },

  /** `read_file`: bytes as base64; the Worker decodes text. */
  async read({ scope, sandboxId, path, config }) {
    const workspace = owned(scope, sandboxId);
    const file = safeFile(join(workspace.root, "files"), path);
    const bytes = await readFile(file).catch((error) => fail(ioError(error)));
    if (bytes.length > config.maxFileBytes) fail("sandbox file exceeds runtime policy");
    return { contentBase64: bytes.toString("base64") };
  },

  /** `write_file` after the decode and file-size checks. */
  async write({ scope, sandboxId, path, contentBase64, config }) {
    const workspace = owned(scope, sandboxId);
    const content = Buffer.from(contentBase64, "base64");
    const root = join(workspace.root, "files");
    const file = safeFile(root, path);
    const current = await directorySize(root);
    const prior = await stat(file).then((value) => value.size, () => 0);
    if (current - prior + content.length > config.maxWorkspaceBytes) fail("sandbox workspace byte limit exceeded");
    const created = await mkdir(posix.dirname(file), { recursive: true }).catch((error) => fail(ioError(error)));
    await writeFile(file, content).catch((error) => fail(ioError(error)));
    // Files the host writes stay writable by the run's uid (see `create`).
    if (typeof process.getuid === "function" && process.getuid() === 0) {
      const paths = [file];
      if (created) for (let dir = posix.dirname(file); dir.length >= created.length; dir = posix.dirname(dir)) paths.push(dir);
      for (const path of paths) await chown(path, WORKER_UID, WORKER_GID);
    }
    return { size: content.length };
  },

  /** `import_file` after the name and size checks: Rust's `data_ingest::ingest` in `k20-ingest`. */
  async import({ scope, sandboxId, filename, body }) {
    const workspace = owned(scope, sandboxId);
    if (!workspace.duckdb) fail("sandbox importFile requires capabilities.sandbox.duckdb");
    const alias = `import_${workspace.imports.length + 1}`;
    const relative = `imports/${alias}.duckdb`;
    const tables = await new Promise((resolve, reject) => {
      const child = spawn(INGEST, [join(workspace.root, relative), filename], { stdio: ["pipe", "pipe", "pipe"] });
      const out = [];
      const err = [];
      child.stdout.on("data", (chunk) => out.push(chunk));
      child.stderr.on("data", (chunk) => err.push(chunk));
      child.on("error", (error) => reject(new Failure(ioError(error))));
      child.on("close", (status) => {
        if (status === 0) {
          try { resolve(JSON.parse(Buffer.concat(out).toString("utf8"))); } catch (error) { reject(new Failure(String(error.message))); }
        } else reject(new Failure(Buffer.concat(err).toString("utf8") || `ingest exited with ${status}`));
      });
      child.stdin.on("error", () => undefined);
      child.stdin.end(body);
    });
    owned(scope, sandboxId).imports.push({ alias, path: relative, tables });
    return { alias, tables };
  },

  /** For the Container object: whether it can sleep, and the numbers PRs state. */
  async stats() {
    let live = 0;
    let active = 0;
    const running = [];
    let latestExpiry = 0;
    const now = Date.now();
    for (const [sandboxId, workspace] of workspaces) {
      if (workspace.expiresAt > now || workspace.active !== null) live++;
      if (workspace.active !== null) {
        active++;
        running.push({ sandboxId, executionId: workspace.active });
      }
      latestExpiry = Math.max(latestExpiry, workspace.expiresAt);
    }
    let cgroupBytes = null;
    for (const path of ["/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"]) {
      const text = await readFile(path, "utf8").catch(() => null);
      if (text) { cgroupBytes = Number(text.trim()); break; }
    }
    let cgroupPeak = null;
    const peak = await readFile("/sys/fs/cgroup/memory.peak", "utf8").catch(() => null);
    if (peak) cgroupPeak = Number(peak.trim());
    return { live, active, running, latestExpiry, runsStarted: started, runsFinished: finished, rssBytes: process.memoryUsage().rss, cgroupBytes, cgroupPeak, uptimeMs: Math.round(process.uptime() * 1000) };
  },
};

async function body(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return Buffer.concat(chunks);
}

const server = createServer(async (request, response) => {
  const reply = (status, value) => {
    response.writeHead(status, { "content-type": "application/json" });
    response.end(JSON.stringify(value));
  };
  try {
    const url = new URL(request.url ?? "/", "http://manager");
    if (request.method === "GET" && url.pathname === "/healthz") return reply(200, { ok: true, worker: process.env.K20_WORKER_KIND ?? "rust" });
    const operation = url.pathname.replace(/^\/op\//, "");
    if (request.method !== "POST" || !Object.hasOwn(operations, operation)) return reply(404, { ok: false, error: "not found" });
    let args;
    if (operation === "import") {
      const header = request.headers["x-sandbox-import"];
      args = { ...JSON.parse(Buffer.from(String(header ?? ""), "base64").toString("utf8")), body: await body(request) };
    } else {
      const raw = await body(request);
      args = raw.length ? JSON.parse(raw.toString("utf8")) : {};
    }
    // Rust prunes on every manager call except importFile.
    if (operation !== "import" && operation !== "stats") await prune();
    reply(200, { ok: true, value: await operations[operation](args) });
  } catch (error) {
    reply(200, { ok: false, error: error instanceof Failure ? error.message : String(error?.message ?? error) });
  }
});

await mkdir(ROOT, { recursive: true });
server.listen(PORT, "0.0.0.0", () => console.log(`k20 sandbox manager on :${PORT}`));
for (const signal of ["SIGTERM", "SIGINT"]) {
  process.on(signal, () => {
    for (const workspace of workspaces.values()) for (const execution of workspace.executions.values()) execution.cancel();
    server.close(() => rm(ROOT, { recursive: true, force: true }).finally(() => process.exit(0)));
  });
}
