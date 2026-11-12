// Policy and execution caps trip as configured, with Rust's texts and order.
// CPU and memory are enforced by the platform, not by local workerd: those two
// are proven on Cloudflare by dev/ (README "Platform results").
import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { REGISTRY_NAME } from "../src/env";
import { platformFailure, workspaceName } from "../src/workspace";
import { ctxSandbox, failure, MemoryFiles, service, session } from "./support";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("policy", () => {
  it("is disabled by default, like Rust; importFile still reads storage first", async () => {
    const files = new MemoryFiles();
    const who = session();
    const sandbox = ctxSandbox(service({ enabled: false }, files), who);
    await expect(sandbox.create()).rejects.toThrow(failure("sandbox is disabled by runtime policy"));
    await expect(sandbox.importFile("sbx_1", { fileId: "missing", filename: "a.csv" })).rejects.toThrow(failure("file not found"));
    files.add("f", "a\n1\n", { tenantId: who.tenantId });
    await expect(sandbox.importFile("sbx_1", { fileId: "f", filename: "a.csv" })).rejects.toThrow(failure("sandbox was not found"));
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
    // A float, a negative number or a string is not a u64: Rust falls back to the default TTL.
    const before = Date.now();
    const created = (await call("create", { ttlMs: 1.5 })) as { expiresAt: number; duckdb: boolean };
    expect(created.expiresAt - before).toBeGreaterThanOrEqual(30 * 60_000);
    expect(created.duckdb).toBe(true);
    const text = (await call("create", {}, false)) as { sandboxId: string };
    await call("writeFile", { sandboxId: text.sandboxId, path: "bin", contentBase64: "/w==" });
    await expect(call("readText", { sandboxId: text.sandboxId, path: "bin" })).rejects.toThrow("sandbox file is not UTF-8");
    expect(await call("readFile", { sandboxId: text.sandboxId, path: "bin" })).toEqual({ contentBase64: "/w==", size: 1 });
  });

  it("without the duckdb declaration a run has no duckdb binding and an import is refused", async () => {
    const who = session();
    const files = new MemoryFiles();
    files.add("f", "a\n1\n", { tenantId: who.tenantId });
    const plain = ctxSandbox(service({}, files), who, false);
    const { sandboxId } = await plain.create();
    const run = await plain.run(sandboxId, { code: 'const kind = typeof duckdb; try { await ctx.sandbox.__worker.query("select 1"); } catch (e) { return [kind, e.message]; }' });
    expect(run.result).toEqual(["undefined", "host call failed: DuckDB was not enabled for this sandbox"]);
    const declared = ctxSandbox(service({}, files), who, true);
    await expect(declared.importFile(sandboxId, { fileId: "f", filename: "a.csv" })).rejects.toThrow(failure("sandbox importFile requires capabilities.sandbox.duckdb"));
  });
});

describe("sandbox counts and lifetime", () => {
  it("allows four sandboxes per account and tenant, then refuses; destroy frees one", async () => {
    const who = session();
    const sandbox = ctxSandbox(service(), who);
    const ids: string[] = [];
    for (let i = 0; i < 4; i++) ids.push((await sandbox.create()).sandboxId);
    await expect(sandbox.create()).rejects.toThrow(failure("sandbox limit reached for this account and tenant"));
    // Another account of the same tenant has its own four.
    expect((await ctxSandbox(service(), { ...who, account: { id: "account-2" } }).create()).sandboxId).toMatch(/^sbx_/);
    await sandbox.destroy(ids[0]!);
    expect((await sandbox.create()).sandboxId).toMatch(/^sbx_/);
  });

  it("refuses past the deployment's total", async () => {
    const registry = env.SANDBOX_REGISTRY.getByName(REGISTRY_NAME);
    const live = (await registry.counts()).sandboxes;
    const sandbox = ctxSandbox(service({ maxTotal: live + 1 }), session());
    await sandbox.create();
    await expect(ctxSandbox(service({ maxTotal: live + 1 }), session()).create()).rejects.toThrow(failure("runtime sandbox limit reached"));
  });

  it("refuses a TTL over the maximum; at its TTL a sandbox is gone and its storage deleted", async () => {
    const who = session();
    await expect(ctxSandbox(service({ maxTtlMs: 1000 }), who).create({ ttlMs: 1001 })).rejects.toThrow(failure("sandbox ttl exceeds runtime policy"));
    const sandbox = ctxSandbox(service(), who);
    // Long enough for the write under a loaded test run; then wait out the TTL.
    const { sandboxId, expiresAt } = await sandbox.create({ ttlMs: 2_000 });
    await sandbox.writeText(sandboxId, "a.txt", "x");
    await sleep(Math.max(0, expiresAt - Date.now()) + 100);
    await expect(sandbox.readText(sandboxId, "a.txt")).rejects.toThrow(failure("sandbox was not found"));
    const stub = env.SANDBOX_WORKSPACE.getByName(workspaceName({ project: who.projectId, tenant: who.tenantId, account: who.account.id }, sandboxId));
    await runDurableObjectAlarm(stub);
    const keys = await runInDurableObject(stub, async (_object, state) => [...(await state.storage.list()).keys()]);
    expect(keys).toEqual([]);
  });
});

describe("execution caps", () => {
  it("code size, timeout and the execution count", async () => {
    const sandbox = ctxSandbox(service({ maxCodeBytes: 20, maxTimeoutMs: 5_000, defaultTimeoutMs: 5_000, maxExecutions: 2 }), session());
    const { sandboxId } = await sandbox.create();
    await expect(sandbox.run(sandboxId, { code: "return '0123456789abcdef';" })).rejects.toThrow(failure("sandbox code exceeds runtime policy"));
    await expect(sandbox.run(sandboxId, { code: "return 1;", timeoutMs: 5_001 })).rejects.toThrow(failure("sandbox timeout exceeds runtime policy"));
    expect(await sandbox.run(sandboxId, { code: "return 1;", timeoutMs: 0 })).toMatchObject({ status: "failed", error: "worker timeout is required" });
    expect(await sandbox.run(sandboxId, { code: "return 2;" })).toMatchObject({ status: "succeeded", result: 2 });
    await expect(sandbox.run(sandboxId, { code: "return 3;" })).rejects.toThrow(failure("sandbox execution limit reached"));
  });

  it("the deadline: a run that is still waiting at timeoutMs fails with the V8 engine's text", async () => {
    const sandbox = ctxSandbox(service(), session());
    const { sandboxId } = await sandbox.create();
    const run = await sandbox.run(sandboxId, { code: "await new Promise((r) => setTimeout(r, 5000)); return 1;", timeoutMs: 250 });
    expect(run).toMatchObject({ status: "failed", result: null, error: "module execution exceeded its budget: module exceeded its 250 ms execution deadline" });
    expect(run.finishedAt - run.startedAt).toBeLessThan(2_000);
  });

  it("maps what the platform reports when it stops an isolate", () => {
    expect(platformFailure("Worker exceeded CPU time limit.", 1000)).toBe("module execution exceeded its budget: module exceeded its 1000 ms execution deadline");
    expect(platformFailure("Worker exceeded memory limit.", 1000)).toBe("module execution exceeded its budget: module exceeded the 134217728 byte isolate heap limit");
    expect(platformFailure("Network connection lost.", 1000)).toBe("module execution failed: Network connection lost.");
  });

  it("result size, rows per query", async () => {
    const sandbox = ctxSandbox(service({ maxOutputBytes: 1_000, maxRows: 3 }), session());
    const { sandboxId } = await sandbox.create();
    expect(await sandbox.run(sandboxId, { code: 'return "x".repeat(2000);' })).toMatchObject({
      status: "failed",
      error: "module execution exceeded its budget: run returned more than the 1000 byte result limit",
    });
    const rows = await sandbox.run(sandboxId, { code: 'return await duckdb.query("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5) SELECT i FROM n");' });
    expect(rows.result).toEqual({ columns: ["i"], rowCount: 3, rows: [{ i: 1 }, { i: 2 }, { i: 3 }], truncated: true });
  });

  it("file and workspace bytes, from the host and from a run, and table growth", async () => {
    const sandbox = ctxSandbox(service({ maxFileBytes: 8, maxWorkspaceBytes: 12 }), session());
    const { sandboxId } = await sandbox.create();
    await expect(sandbox.writeText(sandboxId, "big", "123456789")).rejects.toThrow(failure("sandbox file exceeds runtime policy"));
    await sandbox.writeText(sandboxId, "a", "12345678");
    await sandbox.writeText(sandboxId, "a", "87654321"); // replacing a file counts its old size out
    await expect(sandbox.writeText(sandboxId, "b", "12345")).rejects.toThrow(failure("sandbox workspace byte limit exceeded"));
    const inside = await sandbox.run(sandboxId, { code: 'const out = []; for (const [p, c] of [["c", "123456789"], ["c", "12345"]]) { try { await files.writeText(p, c); out.push("ok"); } catch (e) { out.push(e.message); } } return out;' });
    expect(inside.result).toEqual(["host call failed: sandbox file exceeds write limit", "host call failed: sandbox workspace byte limit exceeded"]);

    const tables = ctxSandbox(service({ maxWorkspaceBytes: 256 * 1024 }), session());
    const roomy = (await tables.create()).sandboxId;
    const grow = await tables.run(roomy, { code: 'await duckdb.register("small", [{ v: 1 }]); try { await duckdb.query("CREATE TABLE big AS WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5000) SELECT i, randomblob(200) AS b FROM n"); return "grew"; } catch (e) { return [e.message, (await duckdb.query("SELECT count(*) AS n FROM sqlite_master WHERE name = \'big\'")).rows[0].n]; }' });
    expect(grow.result).toEqual(["host call failed: sandbox workspace byte limit exceeded", 0]);
  });

  it("run slots are per tenant (0048 §2.3): with one slot, a tenant's second run waits for the first", async () => {
    const config = { concurrency: 1 };
    const whoA = session();
    const a = ctxSandbox(service(config), whoA);
    const b = ctxSandbox(service(config), { ...whoA, account: { id: "account-2" } });
    const [first, second] = await Promise.all([a.create(), b.create()]);
    const slow = a.run(first.sandboxId, { code: "await new Promise((r) => setTimeout(r, 600)); return Date.now();" });
    // Start the second run only once the first holds its tenant's slot.
    const slots = env.SANDBOX_SLOTS.getByName(JSON.stringify([whoA.projectId, whoA.tenantId]));
    for (let i = 0; i < 100; i++) {
      const held = await runInDurableObject(slots, async (_object, state) => (await state.storage.list()).size > 0);
      if (held) break;
      await sleep(50);
    }
    const quick = await b.run(second.sandboxId, { code: "return Date.now();" });
    const slowRun = await slow;
    expect(quick.result).toBeGreaterThanOrEqual(slowRun.result);
  });

  it("cancel: a running execution ends cancelled; one run at a time; no destroy while running", async () => {
    const who = session();
    const sandbox = ctxSandbox(service(), who);
    const { sandboxId } = await sandbox.create();
    const running = sandbox.run(sandboxId, { code: "await new Promise((r) => setTimeout(r, 5000)); return 1;", timeoutMs: 10_000 });
    const stub = env.SANDBOX_WORKSPACE.getByName(workspaceName({ project: who.projectId, tenant: who.tenantId, account: who.account.id }, sandboxId));
    for (let i = 0; i < 100 && !(await runInDurableObject(stub, async (_object, state) => Boolean((await state.storage.get<{ active: unknown }>("meta"))?.active))); i++) await sleep(50);
    await expect(sandbox.run(sandboxId, { code: "return 2;" })).rejects.toThrow(failure("sandbox already has an active execution"));
    await expect(sandbox.destroy(sandboxId)).rejects.toThrow(failure("cannot destroy a sandbox with an active execution"));
    const executionId = await runInDurableObject(stub, async (_object, state) => ((await state.storage.get<{ active: { executionId: string } }>("meta"))!.active.executionId));
    expect(await sandbox.cancel(sandboxId, executionId)).toMatchObject({ status: "running" });
    expect(await running).toMatchObject({ status: "cancelled", error: "sandbox execution cancelled" });
    expect(await sandbox.status(sandboxId, executionId)).toMatchObject({ status: "cancelled" });
    await expect(sandbox.status(sandboxId, "run_missing")).rejects.toThrow(failure("sandbox execution was not found"));
    expect(await sandbox.destroy(sandboxId)).toEqual({ sandboxId, destroyed: true });
  });
});
