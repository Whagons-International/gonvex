// With Rust's sandbox worker and DuckDB (K20_SANDBOX_BIN or K20_SANDBOX_URL;
// skipped with the CI stub): every Whagons call site of `ctx.sandbox`
// (whagons5-client staging a6de676951) run with the app's own helper code
// (test/whagons.ts), the run-level limits, and an Action through K12's runtime.
// These are the "seven scenarios against Rust with DuckDB" of 0048 §2.2; the
// L09 candidate runs the same code (cloudflare/spikes/l09-sandbox).
import { beforeAll, describe, expect, it } from "vitest";
import { ActionRuntime } from "../../actions/src/runtime.js";
import type { TenantActionPort } from "../../actions/src/ports.js";
import type { ActionSession } from "../../actions/src/session.js";
import { ModuleRegistry, type FunctionContract } from "../../kernel/host/src/index.js";
import * as assistant from "./fixtures/assistant.module.js";
import { WORKBOOK_XLS, WORKBOOK_XLSX } from "./fixtures/workbooks";
import { ctxSandbox, failure, service, session, upload, workerKind } from "./support";
import {
  analyzeWorkspaceTasks,
  boundedDuckDbQueryCode,
  cloudRunCode,
  createTaskSnapshotter,
  importAssistantDataFile,
  importedTableSqlName,
  qualifyImportedTables,
  resolveAnalysisFile,
  runSandboxCode,
  runSandboxQuery,
} from "./whagons";

const bytes = (base64: string) => Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
const TASKS = [
  { id: "t1", name: "Fix pump", statusId: "open", priorityId: "high", hours: 2 },
  { id: "t2", name: "Paint wall", statusId: "done", priorityId: "low", hours: 5 },
  { id: "t3", name: "Check valve", statusId: "open", priorityId: "low", hours: 1.5 },
];
const tools = (rows = TASKS) => ({
  tasksForAnalysis: async ({ offset, limit }: { offset: number; limit: number }) => rows.slice(offset, offset + limit),
});

let rust = false;
beforeAll(async () => {
  rust = (await workerKind()) === "rust";
});
const needsRust = (context: { skip: () => void }) => {
  if (!rust) context.skip();
};

describe("assistant data-analysis scenarios on Rust with DuckDB", () => {
  it("analyzeWorkspaceTasks: writeText, files.readText, duckdb.register, a GROUP BY", async (context) => {
    needsRust(context);
    const ctx = { sandbox: ctxSandbox(service(), session()), tools: tools() };
    expect(await analyzeWorkspaceTasks(ctx, { workspaceId: "w1" })).toEqual({
      columns: ["statusId", "total"],
      rowCount: 2,
      rows: [{ statusId: "open", total: 2 }, { statusId: "done", total: 1 }],
      truncated: false,
    });
  });

  it("analyzeWorkspaceTasks with an uploaded file", async (context) => {
    needsRust(context);
    const who = session();
    const fileId = await upload(who, "region,amount\nnorth,7\nsouth,4\n");
    const ctx = { sandbox: ctxSandbox(service(), who), tools: tools() };
    const result = await analyzeWorkspaceTasks(ctx, { workspaceId: "w1", fileId, filename: "sales.csv" });
    expect(result.rows).toEqual([{ statusId: "open", total: 2 }, { statusId: "done", total: 1 }]);
  });

  it("ingestAttachment: importFile reports the tables of a CSV, an .xlsx and an .xls", async (context) => {
    needsRust(context);
    const who = session();
    const sandbox = ctxSandbox(service(), who);
    const summary = async (content: Uint8Array | string, filename: string) => {
      const fileId = await upload(who, content);
      const created = await sandbox.create();
      const imported = await sandbox.importFile(created.sandboxId, { fileId, filename });
      return imported.tables.map((table: { tableName: string; columns: string[]; rowCount: number }) => ({ name: table.tableName, columns: [...table.columns], rowCount: table.rowCount }));
    };
    expect(await summary("name,status\nPump,open\nWall,done\n", "tasks.csv")).toEqual([{ name: "data", columns: ["name", "status"], rowCount: 2 }]);
    const workbook = [
      { name: "sales_2024_1", columns: ["Region", "Amount", "Closed", "Note", "Day"], rowCount: 4 },
      { name: "sheet_3_3", columns: ["k", "v"], rowCount: 2 },
    ];
    expect(await summary(bytes(WORKBOOK_XLSX), "Book.XLSX")).toEqual(workbook);
    expect(await summary(bytes(WORKBOOK_XLS), "legacy.xls")).toEqual(workbook);
  });

  it("the assistant's data-file tools on one shared sandbox: read, sample, query, profile", async (context) => {
    needsRust(context);
    const who = session();
    const first = await upload(who, bytes(WORKBOOK_XLSX));
    const second = await upload(who, "id;score\n1;10\n2;30\n3;20\n");
    const ctx = { sandbox: ctxSandbox(service(), who) };
    const { sandboxId } = await ctx.sandbox.create({ ttlMs: 10 * 60_000 });
    const sales = (await importAssistantDataFile(ctx, sandboxId, { storageId: first, filename: "sales.xlsx" })).imported;
    const scores = (await importAssistantDataFile(ctx, sandboxId, { storageId: second, filename: "scores.csv" })).imported;
    expect([sales.alias, scores.alias]).toEqual(["import_1", "import_2"]);

    const read = await runSandboxCode(ctx, sandboxId, boundedDuckDbQueryCode(`SELECT * FROM ${importedTableSqlName(sales)} LIMIT 100 OFFSET 0`, 100));
    expect(read.columns).toEqual(["Region", "Amount", "Closed", "Note", "Day"]);
    expect(read.rows[0]).toEqual({ Region: "north", Amount: "7", Closed: true, Note: "first & <one>", Day: 45292 });
    expect(read.rowCount).toBe(4);

    const sample = await runSandboxCode(ctx, sandboxId, boundedDuckDbQueryCode(`SELECT * FROM ${importedTableSqlName(sales, "sheet_3_3")} LIMIT 50`, 50));
    expect(sample.rows).toEqual([{ k: "a", v: 1 }, { k: "b", v: 0.1 }]);

    const sql = qualifyImportedTables("select id, score from data where score >= 20 order by score desc", scores);
    expect(sql).toBe('select id, score from "import_2"."data" where score >= 20 order by score desc');
    expect(await runSandboxCode(ctx, sandboxId, boundedDuckDbQueryCode(sql, 200))).toEqual({
      columns: ["id", "score"], rowCount: 2, rows: [{ id: 2, score: 30 }, { id: 3, score: 20 }], truncated: false,
    });

    const profile = await runSandboxCode(ctx, sandboxId, boundedDuckDbQueryCode(`SUMMARIZE SELECT * FROM ${importedTableSqlName(scores)}`, 200));
    expect(profile.columns).toEqual(["column_name", "column_type", "min", "max", "approx_unique", "avg", "std", "q25", "q50", "q75", "count", "null_percentage"]);
    expect(profile.rows[1]).toEqual({ column_name: "score", column_type: "BIGINT", min: "10", max: "30", approx_unique: 3, avg: "20.0", std: "10.0", q25: "12", q50: "20", q75: "28", count: 3, null_percentage: "0.00" });

    expect(await ctx.sandbox.destroy(sandboxId)).toEqual({ sandboxId, destroyed: true });
    await expect(ctx.sandbox.status(sandboxId, "run_x")).rejects.toThrow(failure("sandbox was not found"));
  });

  it("snapshotTasksData and analyzeInSandbox: TypeScript over a JSON snapshot, console.log as the answer", async (context) => {
    needsRust(context);
    const ctx = { sandbox: ctxSandbox(service(), session()), tools: tools() };
    const { sandboxId } = await ctx.sandbox.create({ ttlMs: 10 * 60_000 });
    const snapshots = new Map();
    const snapshot = createTaskSnapshotter(ctx, (operation) => operation(sandboxId), snapshots);
    const one = await snapshot({ workspaceId: "w1" });
    const two = await snapshot({ workspaceId: "w1", maxRows: 2 });
    expect([one.path, one.rowCount, one.complete, two.path, two.rowCount, two.complete]).toEqual(["tasks-snapshot-1.json", 3, true, "tasks-snapshot-2.json", 2, false]);
    expect(await resolveAnalysisFile(one.fileKey, snapshots, async () => { throw new Error("not an upload"); })).toBe(snapshots.get("tasks-snapshot-1.json"));
    const code = `
      interface Task { id: string; statusId: string; hours: number }
      const tasks: Task[] = JSON.parse(await files.readText(${JSON.stringify(one.path)}));
      const hours = tasks.reduce<Record<string, number>>((sum, task) => ({ ...sum, [task.statusId]: (sum[task.statusId] ?? 0) + task.hours }), {});
      return { files: await files.list(), hours, ids: tasks.filter((task) => task.statusId === "open").map((task) => task.id) };
    `;
    expect(await runSandboxCode(ctx, sandboxId, code, 30_000)).toEqual({ files: ["tasks-snapshot-1.json", "tasks-snapshot-2.json"], hours: { open: 3.5, done: 5 }, ids: ["t1", "t3"] });
    expect(await runSandboxCode(ctx, sandboxId, "console.log(6 * 7);")).toBe(42);
    expect(await runSandboxCode(ctx, sandboxId, 'console.info("a"); console.warn({ b: 1 });')).toEqual({ logs: [{ level: "log", message: "a" }, { level: "warn", message: '{"b":1}' }] });
    await expect(runSandboxCode(ctx, sandboxId, "const x: number = ;")).rejects.toThrow(/^TypeScript parse failed: /);
    await expect(runSandboxCode(ctx, sandboxId, 'throw new Error("boom")')).rejects.toThrow(/^module execution failed: Error: boom/);
  });

  it("the cloud agent's runCode tool: lazy create with a 5-minute TTL, destroy on close", async (context) => {
    needsRust(context);
    const tool = cloudRunCode({ sandbox: ctxSandbox(service(), session()) });
    expect(await tool.execute({ code: "const add = <T extends number>(a: T, b: T): number => a + b; return add(2, 3);" })).toBe(5);
    expect(await tool.execute({ code: "return [1, 2, 3].map((n) => n * n);" })).toEqual([1, 4, 9]);
    await tool.close();
  });

  it("parseExcelAttachment: the import works; its unqualified query fails on DuckDB (0047 §7)", async (context) => {
    needsRust(context);
    const who = session();
    const fileId = await upload(who, bytes(WORKBOOK_XLSX));
    const ctx = { sandbox: ctxSandbox(service(), who) };
    const { sandboxId } = await ctx.sandbox.create();
    const imported = await ctx.sandbox.importFile(sandboxId, { fileId, filename: "budget.xlsx" });
    const table = imported.tables[0];
    await expect(runSandboxQuery(ctx, sandboxId, `SELECT "Region" FROM "${table.tableName}" LIMIT 2000 OFFSET 0`)).rejects.toThrow(
      /^module execution failed: GonvexHostError: host call failed: Catalog Error: Table with name sales_2024_1 does not exist!/,
    );
    expect(await runSandboxQuery(ctx, sandboxId, `SELECT "Region" FROM "import_1"."${table.tableName}" LIMIT 2000 OFFSET 0`)).toEqual([]);
  });
});

describe("run-level limits and isolation on Rust", () => {
  it("heap, deadline, result size and rows trip with the V8 engine's texts", async (context) => {
    needsRust(context);
    const sandbox = ctxSandbox(service({ maxOutputBytes: 100_000, maxRows: 3 }), session());
    const { sandboxId } = await sandbox.create();
    const heap = await sandbox.run(sandboxId, { code: "const kept = []; for (;;) kept.push(new Array(1e6).fill(1));" });
    expect(heap).toMatchObject({ status: "failed", error: "module execution exceeded its budget: module exceeded the 67108864 byte isolate heap limit" });
    const spin = await sandbox.run(sandboxId, { code: "for (;;) {}", timeoutMs: 1_000 });
    expect(spin.status).toBe("failed");
    expect(spin.error).toMatch(/^module execution exceeded its budget: module exceeded its \d+ ms execution deadline$/);
    expect(spin.finishedAt - spin.startedAt).toBeLessThan(3_500);
    const big = await sandbox.run(sandboxId, { code: 'return "x".repeat(200000);' });
    expect(big).toMatchObject({ status: "failed", error: "module execution exceeded its budget: run returned more than the 100000 byte result limit" });
    const rows = await sandbox.run(sandboxId, { code: 'return await duckdb.query("SELECT * FROM range(5) t(i)");' });
    expect(rows.result).toEqual({ columns: ["i"], rowCount: 3, rows: [{ i: 0 }, { i: 1 }, { i: 2 }], truncated: true });
  });

  it("a run has no network and DuckDB has no file system access", async (context) => {
    needsRust(context);
    const sandbox = ctxSandbox(service(), session());
    const { sandboxId } = await sandbox.create();
    const probe = await sandbox.run(sandboxId, {
      code: `
        const attempt = async (f: () => unknown) => { try { await f(); return "allowed"; } catch (e) { return String((e as Error).message); } };
        return {
          fetch: typeof (globalThis as any).fetch,
          webSocket: typeof (globalThis as any).WebSocket,
          readCsv: await attempt(() => duckdb.query("SELECT * FROM read_csv_auto('/etc/passwd')")),
          httpfs: await attempt(() => duckdb.query("SELECT * FROM read_csv_auto('https://example.com/a.csv')")),
          attach: await attempt(() => duckdb.query("ATTACH '/tmp/x.duckdb' AS x")),
          escape: await attempt(() => files.readText("../../etc/passwd")),
        };
      `,
    });
    expect(probe.status).toBe("succeeded");
    expect(probe.result.fetch).toBe("undefined");
    expect(probe.result.webSocket).toBe("undefined");
    for (const key of ["readCsv", "httpfs", "attach"]) expect(probe.result[key]).not.toBe("allowed");
    expect(probe.result.escape).toBe("host call failed: sandbox path must be relative and cannot escape the workspace");
  });
});

describe("ctx.sandbox through K12's Action runtime, on the Container backend", () => {
  const PROJECT = "proj-k20";
  const any = { kind: "any" };
  const agent = (capabilities: Record<string, unknown>): Record<string, unknown> => ({ actionProfile: "agent", actionCapabilities: capabilities });
  const functions: FunctionContract[] = [
    { path: "assistant.analyze", kind: "action", export: "analyze", argsSchema: any, resultSchema: any, metadata: agent({ sandbox: { duckdb: true }, storage: true }) },
    { path: "assistant.failing", kind: "action", export: "failing", argsSchema: any, resultSchema: any, metadata: agent({ sandbox: { duckdb: true } }) },
  ];
  const registry = new ModuleRegistry(PROJECT);
  registry.install({ moduleId: PROJECT, generation: 1, artifactHash: "k20-artifact", clientContract: 1, functions, exports: assistant as unknown as Record<string, unknown> });
  const tenant = new Proxy({}, { get: () => async () => { throw new Error("no tenant in this test"); } }) as TenantActionPort;
  const actionSession = (tenantId: string): ActionSession => ({
    projectId: PROJECT, tenantId, account: { id: "acct-1" },
    member: { id: "mem-1", accountId: "acct-1", status: "active", role: "member", displayName: "One", permissions: {} },
  });

  it("an agent Action analyses tasks and an upload, and destroys its sandbox", async (context) => {
    needsRust(context);
    const tenantId = `t-${crypto.randomUUID()}`;
    const who = actionSession(tenantId);
    const fileId = await upload(who, "a,b\n1,2\n3,4\n");
    const runtime = new ActionRuntime({ registry, tenant, sandbox: service() });
    const report = await runtime.execute({ session: who, path: "assistant.analyze", args: { tasks: [{ statusId: "open" }, { statusId: "done" }, { statusId: "open" }], fileId, filename: "u.csv" } });
    expect(report.ok).toBe(true);
    expect(JSON.parse((report as { value: string }).value)).toEqual({
      status: "succeeded",
      result: { byStatus: [{ statusId: "open", total: 2 }, { statusId: "done", total: 1 }], upload: [{ n: 2 }] },
      imported: { alias: "import_1", tables: [{ tableName: "data", rowCount: 2, columns: ["a", "b"] }] },
    });
  });

  it("errors reach the module with Rust's host-call text; importFile needs the storage capability", async () => {
    const runtime = new ActionRuntime({ registry, tenant, sandbox: service() });
    const who = actionSession(`t-${crypto.randomUUID()}`);
    const call = async (operation: string, args: unknown[]) =>
      JSON.parse(((await runtime.execute({ session: who, path: "assistant.failing", args: { operation, args } })) as { value: string }).value);
    expect(await call("status", ["sbx_00000000-0000-4000-8000-000000000000", "run_1"])).toBe("GonvexHostError: host call failed: sandbox host call failed [host_call_failed]: sandbox was not found");
    expect(await call("importFile", ["sbx_x", { fileId: "f", filename: "a.csv" }])).toBe("GonvexHostError: host call failed: sandbox host call failed [host_call_failed]: sandbox importFile also requires the storage capability");
    expect(await call("create", [{ ttlMs: 999_999_999 }])).toBe("GonvexHostError: host call failed: sandbox host call failed [host_call_failed]: sandbox ttl exceeds runtime policy");
  });
});
