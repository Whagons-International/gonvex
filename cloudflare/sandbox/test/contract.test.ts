// The sandbox contract through the whole Cloudflare path: K12's port
// (SandboxService) → ContainerBackend → the tenant's SandboxContainer object →
// the manager → the worker. These run with the Rust worker (locally, the
// image) and with the protocol stub (CI); test/rust.test.ts needs Rust.
import { env } from "cloudflare:workers";
import { beforeAll, describe, expect, it } from "vitest";
import { REGISTRY_NAME } from "../src/constants";
import { containerName } from "../src/container-backend";
import { ctxSandbox, failure, service, session, upload, workerKind } from "./support";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
let kind: "rust" | "stub" = "stub";
/** Code that waits `ms` and answers 1, in the worker at hand. */
const waiting = (ms: number) => (kind === "stub" ? `__sleep:${ms}` : `await new Promise((r) => setTimeout(r, ${ms})); return 1;`);

beforeAll(async () => {
  kind = await workerKind();
});

describe("policy and payloads", () => {
  it("is disabled by default; importFile reads storage before anything else", async () => {
    const who = session();
    const sandbox = ctxSandbox(service({ enabled: false }), who);
    await expect(sandbox.create()).rejects.toThrow(failure("sandbox is disabled by runtime policy"));
    await expect(sandbox.importFile("sbx_x", { fileId: "missing", filename: "a.csv" })).rejects.toThrow(failure("file not found"));
    const fileId = await upload(who, "a\n1\n");
    await expect(sandbox.importFile("sbx_x", { fileId, filename: "a.csv" })).rejects.toThrow(failure("sandbox was not found"));
  });

  it("checks payloads in Rust's order, with Rust's texts", async () => {
    const who = session();
    const sandboxService = service();
    const call = (operation: string, payload: unknown, duckdb = true) => sandboxService.call(operation, payload, who, duckdb);
    await expect(call("create", { ttlMs: 1, extra: 1 })).rejects.toThrow('sandbox payload contains unsupported field "extra"');
    await expect(call("create", [])).rejects.toThrow("sandbox payload must be an object");
    await expect(call("destroy", {})).rejects.toThrow("sandboxId must be a string");
    await expect(call("destroy", { sandboxId: "" })).rejects.toThrow("sandboxId is required");
    await expect(call("run", { sandboxId: "sbx_x", code: "" })).rejects.toThrow("code is required");
    await expect(call("writeFile", { sandboxId: "sbx_x", path: "a", contentBase64: "not base64!" })).rejects.toThrow("sandbox contentBase64 is invalid");
    await expect(call("frobnicate", {})).rejects.toThrow('unsupported sandbox operation "frobnicate"');
    await expect(call("worker.query", {})).rejects.toThrow('unsupported sandbox operation "worker.query"');
    await expect(call("importFile", { sandboxId: "sbx_x", fileId: "f", filename: "a.csv" }, false)).rejects.toThrow("sandbox importFile requires capabilities.sandbox.duckdb");
    // A float is not a u64: Rust falls back to the default TTL.
    const before = Date.now();
    const created = (await call("create", { ttlMs: 1.5 })) as { sandboxId: string; expiresAt: number; duckdb: boolean };
    expect(created.expiresAt - before).toBeGreaterThanOrEqual(30 * 60_000);
    expect(created.duckdb).toBe(true);
    await call("writeFile", { sandboxId: created.sandboxId, path: "bin", contentBase64: "/w==" });
    await expect(call("readText", { sandboxId: created.sandboxId, path: "bin" })).rejects.toThrow("sandbox file is not UTF-8");
    expect(await call("readFile", { sandboxId: created.sandboxId, path: "bin" })).toEqual({ contentBase64: "/w==", size: 1 });
  });
});

describe("counts, lifetime and caps", () => {
  it("allows four sandboxes per account and tenant, then refuses; destroy frees one", async () => {
    const who = session();
    const sandbox = ctxSandbox(service(), who);
    const ids: string[] = [];
    for (let i = 0; i < 4; i++) ids.push((await sandbox.create()).sandboxId);
    await expect(sandbox.create()).rejects.toThrow(failure("sandbox limit reached for this account and tenant"));
    expect((await ctxSandbox(service(), { ...who, account: { id: "account-2" } }).create()).sandboxId).toMatch(/^sbx_/);
    await sandbox.destroy(ids[0]!);
    expect((await sandbox.create()).sandboxId).toMatch(/^sbx_/);
  });

  it("refuses past the deployment's total", async () => {
    const live = (await env.SANDBOX_REGISTRY.getByName(REGISTRY_NAME).counts()).sandboxes;
    await ctxSandbox(service({ maxTotal: live + 1 }), session()).create();
    await expect(ctxSandbox(service({ maxTotal: live + 1 }), session()).create()).rejects.toThrow(failure("runtime sandbox limit reached"));
  });

  it("refuses a TTL over the maximum; at its TTL the workspace is gone", async () => {
    const who = session();
    await expect(ctxSandbox(service({ maxTtlMs: 1000 }), who).create({ ttlMs: 1001 })).rejects.toThrow(failure("sandbox ttl exceeds runtime policy"));
    const sandbox = ctxSandbox(service(), who);
    const { sandboxId, expiresAt } = await sandbox.create({ ttlMs: 2_000 });
    await sandbox.writeText(sandboxId, "a.txt", "x");
    expect(await sandbox.readText(sandboxId, "a.txt")).toBe("x");
    await sleep(Math.max(0, expiresAt - Date.now()) + 100);
    await expect(sandbox.readText(sandboxId, "a.txt")).rejects.toThrow(failure("sandbox was not found"));
  });

  it("code size, timeout and the execution count", async () => {
    const sandbox = ctxSandbox(service({ maxCodeBytes: 20, maxTimeoutMs: 5_000, defaultTimeoutMs: 5_000, maxExecutions: 2 }), session());
    const { sandboxId } = await sandbox.create();
    await expect(sandbox.run(sandboxId, { code: "return '0123456789abcdef';" })).rejects.toThrow(failure("sandbox code exceeds runtime policy"));
    await expect(sandbox.run(sandboxId, { code: "return 1;", timeoutMs: 5_001 })).rejects.toThrow(failure("sandbox timeout exceeds runtime policy"));
    expect(await sandbox.run(sandboxId, { code: "return 1;", timeoutMs: 0 })).toMatchObject({ status: "failed", error: "worker timeout is required" });
    expect(await sandbox.run(sandboxId, { code: "return 2;" })).toMatchObject({ status: "succeeded" });
    await expect(sandbox.run(sandboxId, { code: "return 3;" })).rejects.toThrow(failure("sandbox execution limit reached"));
  });

  it("file and workspace bytes", async () => {
    const sandbox = ctxSandbox(service({ maxFileBytes: 8, maxWorkspaceBytes: 12 }), session());
    const { sandboxId } = await sandbox.create();
    await expect(sandbox.writeText(sandboxId, "big", "123456789")).rejects.toThrow(failure("sandbox file exceeds runtime policy"));
    await sandbox.writeText(sandboxId, "a", "12345678");
    await sandbox.writeText(sandboxId, "a", "87654321"); // replacing a file counts its old size out
    await expect(sandbox.writeText(sandboxId, "b", "12345")).rejects.toThrow(failure("sandbox workspace byte limit exceeded"));
  });

  it("paths stay inside the workspace, with Rust's errno texts", async () => {
    const sandbox = ctxSandbox(service(), session());
    const { sandboxId } = await sandbox.create();
    for (const path of ["../x", "/etc/passwd", "./a", "a/../../b", "a/.."]) {
      await expect(sandbox.writeText(sandboxId, path, "x")).rejects.toThrow(failure("sandbox path must be relative and cannot escape the workspace"));
    }
    await sandbox.writeText(sandboxId, "dir//./nested.txt", "ok");
    expect(await sandbox.readText(sandboxId, "dir/nested.txt")).toBe("ok");
    await expect(sandbox.readText(sandboxId, "dir")).rejects.toThrow(failure("Is a directory (os error 21)"));
    await expect(sandbox.readText(sandboxId, "missing.txt")).rejects.toThrow(failure("No such file or directory (os error 2)"));
  });
});

describe("executions", () => {
  it("cancel: a running execution ends cancelled; one run at a time; no destroy while running", async () => {
    const who = session();
    const sandbox = ctxSandbox(service(), who);
    const { sandboxId } = await sandbox.create();
    const running = sandbox.run(sandboxId, { code: waiting(5_000), timeoutMs: 10_000 });
    // `run` answers when the execution ends; the manager's stats name the one in progress.
    const container = env.SANDBOX_CONTAINER.getByName(containerName({ project: who.projectId, tenant: who.tenantId }));
    let executionId: string | undefined;
    for (let i = 0; i < 100 && !executionId; i++) {
      await sleep(50);
      const stats = (await container.stats()) as { ok: boolean; value?: { manager: { running: { sandboxId: string; executionId: string }[] } } };
      executionId = stats.value?.manager.running.find((entry) => entry.sandboxId === sandboxId)?.executionId;
    }
    expect(executionId).toMatch(/^run_/);
    await expect(sandbox.run(sandboxId, { code: "return 2;" })).rejects.toThrow(failure("sandbox already has an active execution"));
    await expect(sandbox.destroy(sandboxId)).rejects.toThrow(failure("cannot destroy a sandbox with an active execution"));
    expect(await sandbox.cancel(sandboxId, executionId!)).toMatchObject({ status: "running" });
    expect(await running).toMatchObject({ status: "cancelled", error: "sandbox execution cancelled" });
    expect(await sandbox.status(sandboxId, executionId!)).toMatchObject({ status: "cancelled" });
    await expect(sandbox.status(sandboxId, "run_missing")).rejects.toThrow(failure("sandbox execution was not found"));
    expect(await sandbox.destroy(sandboxId)).toEqual({ sandboxId, destroyed: true });
  });

  it("the deadline: a run still waiting at its timeout fails", async () => {
    const sandbox = ctxSandbox(service(), session());
    const { sandboxId } = await sandbox.create();
    const run = await sandbox.run(sandboxId, { code: waiting(10_000), timeoutMs: 500 });
    expect(run.status).toBe("failed");
    // Rust's V8 watchdog answers first, naming the time left when the run started
    // (488 ms of 500); the manager's outer timeout (+2 s) is the backstop.
    expect(run.error).toMatch(kind === "stub" ? /^sandbox execution timed out$/ : /^module execution exceeded its budget: module exceeded its \d+ ms execution deadline$/);
  });

  it("run slots are per tenant (0048 §2.3): with one slot, a tenant's second run waits, another tenant's does not", async () => {
    const config = { concurrency: 1 };
    const tenantA = session();
    const first = ctxSandbox(service(config), tenantA);
    const second = ctxSandbox(service(config), { ...tenantA, account: { id: "account-2" } });
    const other = ctxSandbox(service(config), session());
    const [a, b, c] = await Promise.all([first.create(), second.create(), other.create()]);
    const started = Date.now();
    const slow = first.run(a.sandboxId, { code: waiting(1_500) });
    await sleep(300);
    const [sameTenant, otherTenant] = await Promise.all([second.run(b.sandboxId, { code: waiting(10) }), other.run(c.sandboxId, { code: waiting(10) })]);
    const slowRun = await slow;
    expect(slowRun.status).toBe("succeeded");
    expect(otherTenant.finishedAt).toBeLessThan(slowRun.finishedAt);
    expect(sameTenant.finishedAt).toBeGreaterThanOrEqual(slowRun.finishedAt);
    expect(Date.now() - started).toBeGreaterThanOrEqual(1_500);
  });
});

describe("isolation", () => {
  it("another account, tenant or project reaches no part of a sandbox", async () => {
    const owner = session();
    const sandboxService = service();
    const sandbox = ctxSandbox(sandboxService, owner);
    const { sandboxId } = await sandbox.create();
    await sandbox.writeText(sandboxId, "secret.txt", "owner only");
    const run = await sandbox.run(sandboxId, { code: "return 1;" });
    const strangers = [
      { ...owner, account: { id: "account-2" }, member: { id: "member-2" } },
      session({ accountId: owner.account.id }),
      { ...owner, projectId: "other-project" },
    ];
    for (const stranger of strangers) {
      const theirs = ctxSandbox(sandboxService, stranger);
      const fileId = await upload(stranger, "a,b\n1,2\n");
      const notFound = failure("sandbox was not found");
      await expect(theirs.readText(sandboxId, "secret.txt")).rejects.toThrow(notFound);
      await expect(theirs.readFile(sandboxId, "secret.txt")).rejects.toThrow(notFound);
      await expect(theirs.writeText(sandboxId, "secret.txt", "overwritten")).rejects.toThrow(notFound);
      await expect(theirs.run(sandboxId, { code: "return 1;" })).rejects.toThrow(notFound);
      await expect(theirs.status(sandboxId, run.executionId)).rejects.toThrow(notFound);
      await expect(theirs.cancel(sandboxId, run.executionId)).rejects.toThrow(notFound);
      await expect(theirs.importFile(sandboxId, { fileId, filename: "a.csv" })).rejects.toThrow(notFound);
      await expect(theirs.destroy(sandboxId)).rejects.toThrow(notFound);
    }
    expect(await sandbox.readText(sandboxId, "secret.txt")).toBe("owner only");
  });

  it("imports go through K13: a private file of another account is not found (IB1)", async () => {
    const owner = session();
    const colleague = { ...owner, account: { id: "account-2" }, member: { id: "member-2" } };
    const privateFile = await upload(colleague, "a\n1\n", { visibility: "private" });
    const tenantFile = await upload(colleague, "name,status\nPump,open\nWall,done\n");
    const sandbox = ctxSandbox(service(), owner);
    const { sandboxId } = await sandbox.create();
    await expect(sandbox.importFile(sandboxId, { fileId: privateFile, filename: "a.csv" })).rejects.toThrow(failure("file not found"));
    const imported = await sandbox.importFile(sandboxId, { fileId: tenantFile, filename: "tasks.csv" });
    expect(imported).toEqual({ alias: "import_1", tables: [{ tableName: "data", rowCount: 2, columns: ["name", "status"] }] });
    await expect(sandbox.importFile(sandboxId, { fileId: tenantFile, filename: "tasks.pdf" })).rejects.toThrow(
      failure("unsupported data file type .pdf (supported: CSV, TSV, XLSX, XLS, XLSB, ODS)"),
    );
    // Without capabilities.sandbox.duckdb at create, the workspace refuses imports.
    const plain = ctxSandbox(service(), owner, false);
    const plainId = (await plain.create()).sandboxId;
    await expect(sandbox.importFile(plainId, { fileId: tenantFile, filename: "tasks.csv" })).rejects.toThrow(failure("sandbox importFile requires capabilities.sandbox.duckdb"));
  });
});

describe("the worker request (protocol parity, stub only)", () => {
  it("is Rust's request: version, workspace root, limits, imports, uid/gid", async (context) => {
    if (kind !== "stub") return context.skip();
    const who = session();
    const sandbox = ctxSandbox(service({ maxRows: 7 }), who);
    const { sandboxId } = await sandbox.create();
    const fileId = await upload(who, "a\n1\n");
    await sandbox.importFile(sandboxId, { fileId, filename: "a.csv" });
    const run = await sandbox.run(sandboxId, { code: "__echo", timeoutMs: 1234 });
    const request = run.result as Record<string, unknown>;
    expect(request).toMatchObject({
      version: 1,
      allowUnconfined: true,
      duckdb: true,
      imports: [{ alias: "import_1", path: "imports/import_1.duckdb", tables: [{ tableName: "data", rowCount: 1, columns: ["a"] }] }],
      maxHeapBytes: 64 << 20,
      maxFileBytes: 64 << 20,
      maxWorkspaceBytes: 256 << 20,
      maxOutputBytes: 8 << 20,
      maxRows: 7,
      duckdbMemoryBytes: 128 << 20,
      timeoutMs: 1234,
      workerUid: 65_534,
      workerGid: 65_534,
    });
    expect(String(request["root"])).toMatch(new RegExp(`/${sandboxId}$`));
    expect(Object.keys(request).sort()).toEqual(["allowUnconfined", "duckdb", "duckdbMemoryBytes", "imports", "maxFileBytes", "maxHeapBytes", "maxOutputBytes", "maxRows", "maxWorkspaceBytes", "root", "timeoutMs", "version", "workerGid", "workerUid"]);
    expect(await sandbox.run(sandboxId, { code: "__fail:boom" })).toMatchObject({ status: "failed", error: "boom" });
    expect(await ctxSandbox(service({ maxOutputBytes: 100 }), who).run(sandboxId, { code: "__big:500" })).toMatchObject({ status: "failed", error: "sandbox output exceeds runtime policy" });
  });
});
