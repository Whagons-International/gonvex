// The assistant data-analysis scenarios: every Whagons call site of
// `ctx.sandbox` (whagons5-client staging a6de676951), run with the app's own
// helper code (test/whagons.ts) through the service as K12 calls it.
import { describe, expect, it } from "vitest";
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
import { WORKBOOK_XLS, WORKBOOK_XLSX } from "./fixtures/workbooks";
import { ctxSandbox, failure, MemoryFiles, service, session } from "./support";

const bytes = (base64: string) => Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
const TASKS = [
  { id: "t1", name: "Fix pump", statusId: "open", priorityId: "high", hours: 2 },
  { id: "t2", name: "Paint wall", statusId: "done", priorityId: "low", hours: 5 },
  { id: "t3", name: "Check valve", statusId: "open", priorityId: "low", hours: 1.5 },
];
const tools = (rows = TASKS) => ({
  tasksForAnalysis: async ({ offset, limit }: { offset: number; limit: number }) => rows.slice(offset, offset + limit),
});

describe("assistant data-analysis scenarios", () => {
  it("analyzeWorkspaceTasks: writeText, files.readText, duckdb.register, a GROUP BY", async () => {
    const who = session();
    const ctx = { sandbox: ctxSandbox(service(), who), tools: tools() };
    const result = await analyzeWorkspaceTasks(ctx, { workspaceId: "w1" });
    expect(result).toEqual({
      columns: ["statusId", "total"],
      rowCount: 2,
      rows: [{ statusId: "open", total: 2 }, { statusId: "done", total: 1 }],
      truncated: false,
    });
  });

  it("analyzeWorkspaceTasks with an uploaded file: importFile beside the task snapshot", async () => {
    const who = session();
    const files = new MemoryFiles();
    files.add("file-1", "region,amount\nnorth,7\nsouth,4\n", { tenantId: who.tenantId });
    const ctx = { sandbox: ctxSandbox(service({}, files), who), tools: tools() };
    const result = await analyzeWorkspaceTasks(ctx, { workspaceId: "w1", fileId: "file-1", filename: "sales.csv" });
    expect(result.rows).toEqual([{ statusId: "open", total: 2 }, { statusId: "done", total: 1 }]);
  });

  it("ingestAttachment: importFile reports the tables of a CSV, an .xlsx and an .xls", async () => {
    const who = session();
    const files = new MemoryFiles();
    files.add("csv", "name,status\nPump,open\nWall,done\n", { tenantId: who.tenantId, ownerId: who.account.id, visibility: "private" });
    files.add("xlsx", bytes(WORKBOOK_XLSX), { tenantId: who.tenantId });
    files.add("xls", bytes(WORKBOOK_XLS), { tenantId: who.tenantId });
    const sandbox = ctxSandbox(service({}, files), who);
    // assistantData.ts ingestAttachment: one sandbox per upload.
    const summary = async (fileId: string, filename: string) => {
      const created = await sandbox.create();
      const imported = await sandbox.importFile(created.sandboxId, { fileId, filename });
      return imported.tables.map((table: { tableName: string; columns: string[]; rowCount: number }) => ({ name: table.tableName, columns: [...table.columns], rowCount: table.rowCount }));
    };
    expect(await summary("csv", "tasks.csv")).toEqual([{ name: "data", columns: ["name", "status"], rowCount: 2 }]);
    const workbook = [
      { name: "sales_2024_1", columns: ["Region", "Amount", "Closed", "Note", "Day"], rowCount: 4 },
      { name: "sheet_3_3", columns: ["k", "v"], rowCount: 2 },
    ];
    expect(await summary("xlsx", "Book.XLSX")).toEqual(workbook);
    expect(await summary("xls", "legacy.xls")).toEqual(workbook);
  });

  it("the assistant's data-file tools on one shared sandbox: read, sample, query, profile", async () => {
    const who = session();
    const files = new MemoryFiles();
    files.add("upload-1", bytes(WORKBOOK_XLSX), { tenantId: who.tenantId });
    files.add("upload-2", "id;score\n1;10\n2;30\n3;20\n", { tenantId: who.tenantId });
    const ctx = { sandbox: ctxSandbox(service({}, files), who) };
    // completion.ts: getSharedSandbox() with a 10-minute TTL, imports cached per file key.
    const { sandboxId } = await ctx.sandbox.create({ ttlMs: 10 * 60_000 });
    const first = (await importAssistantDataFile(ctx, sandboxId, { storageId: "upload-1", filename: "sales.xlsx" })).imported;
    const second = (await importAssistantDataFile(ctx, sandboxId, { storageId: "upload-2", filename: "scores.csv" })).imported;
    expect([first.alias, second.alias]).toEqual(["import_1", "import_2"]);

    // readConversationFile
    const read = await runSandboxCode(ctx, sandboxId, boundedDuckDbQueryCode(`SELECT * FROM ${importedTableSqlName(first)} LIMIT 100 OFFSET 0`, 100));
    expect(read.columns).toEqual(["Region", "Amount", "Closed", "Note", "Day"]);
    expect(read.rows[0]).toEqual({ Region: "north", Amount: "7", Closed: 1, Note: "first & <one>", Day: 45292 });
    expect(read.rowCount).toBe(4);

    // inspectDataFile, operation "sample", on a named table
    const sample = await runSandboxCode(ctx, sandboxId, boundedDuckDbQueryCode(`SELECT * FROM ${importedTableSqlName(first, "sheet_3_3")} LIMIT 50`, 50));
    expect(sample.rows).toEqual([{ k: "a", v: 1 }, { k: "b", v: 0.1 }]);

    // queryDataFile: the model's SQL, qualified by the app
    const sql = qualifyImportedTables("select id, score from data where score >= 20 order by score desc", second);
    expect(sql).toBe('select id, score from "import_2"."data" where score >= 20 order by score desc');
    const queried = await runSandboxCode(ctx, sandboxId, boundedDuckDbQueryCode(sql, 200));
    expect(queried).toEqual({ columns: ["id", "score"], rowCount: 2, rows: [{ id: 2, score: 30 }, { id: 3, score: 20 }], truncated: false });

    // profileDataFile: DuckDB's SUMMARIZE
    const profile = await runSandboxCode(ctx, sandboxId, boundedDuckDbQueryCode(`SUMMARIZE SELECT * FROM ${importedTableSqlName(second)}`, 200));
    expect(profile.columns).toEqual(["column_name", "column_type", "min", "max", "approx_unique", "avg", "std", "q25", "q50", "q75", "count", "null_percentage"]);
    expect(profile.rows[1]).toEqual({ column_name: "score", column_type: "BIGINT", min: "10", max: "30", approx_unique: 3, avg: "20", std: "10", q25: "10", q50: "20", q75: "20", count: 3, null_percentage: "0.00" });

    // completion.ts finally: destroy the shared sandbox
    expect(await ctx.sandbox.destroy(sandboxId)).toEqual({ sandboxId, destroyed: true });
    await expect(ctx.sandbox.status(sandboxId, "run_x")).rejects.toThrow(failure("sandbox was not found"));
  });

  it("snapshotTasksData and analyzeInSandbox: TypeScript over a JSON snapshot, console.log as the answer", async () => {
    const who = session();
    const ctx = { sandbox: ctxSandbox(service(), who), tools: tools() };
    const { sandboxId } = await ctx.sandbox.create({ ttlMs: 10 * 60_000 });
    const snapshots = new Map();
    const snapshot = createTaskSnapshotter(ctx, (operation) => operation(sandboxId), snapshots);
    const one = await snapshot({ workspaceId: "w1" });
    const two = await snapshot({ workspaceId: "w1", maxRows: 2 });
    expect([one.path, one.rowCount, one.complete, two.path, two.rowCount, two.complete]).toEqual(["tasks-snapshot-1.json", 3, true, "tasks-snapshot-2.json", 2, false]);
    const imported = await resolveAnalysisFile(one.fileKey, snapshots, async () => { throw new Error("not an upload"); });
    expect(imported).toBe(snapshots.get("tasks-snapshot-1.json"));
    const code = `
      interface Task { id: string; statusId: string; hours: number }
      const tasks: Task[] = JSON.parse(await files.readText(${JSON.stringify(one.path)}));
      const hours = tasks.reduce<Record<string, number>>((sum, task) => ({ ...sum, [task.statusId]: (sum[task.statusId] ?? 0) + task.hours }), {});
      return { files: await files.list(), hours, ids: tasks.filter((task) => task.statusId === "open").map((task) => task.id) };
    `;
    expect(await runSandboxCode(ctx, sandboxId, code, 30_000)).toEqual({
      files: ["tasks-snapshot-1.json", "tasks-snapshot-2.json"],
      hours: { open: 3.5, done: 5 },
      ids: ["t1", "t3"],
    });
    expect(await runSandboxCode(ctx, sandboxId, "console.log(6 * 7);")).toBe(42);
    expect(await runSandboxCode(ctx, sandboxId, 'console.info("a"); console.warn({ b: 1 });')).toEqual({ logs: [{ level: "log", message: "a" }, { level: "warn", message: '{"b":1}' }] });
    await expect(runSandboxCode(ctx, sandboxId, "const x: number = ;")).rejects.toThrow(/^TypeScript parse failed: /);
    await expect(runSandboxCode(ctx, sandboxId, 'throw new Error("boom")')).rejects.toThrow(/^module execution failed: Error: boom/);
  });

  it("the cloud agent's runCode tool: lazy create with a 5-minute TTL, destroy on close", async () => {
    const who = session();
    const ctx = { sandbox: ctxSandbox(service(), who) };
    const tool = cloudRunCode(ctx);
    expect(await tool.execute({ code: "const add = <T extends number>(a: T, b: T): number => a + b; return add(2, 3);" })).toBe(5);
    expect(await tool.execute({ code: "return [1, 2, 3].map((n) => n * n);" })).toEqual([1, 4, 9]);
    await tool.close();
  });

  it("parseExcelAttachment: the import works; its unqualified query fails as it does on DuckDB", async () => {
    const who = session();
    const files = new MemoryFiles();
    files.add("excel", bytes(WORKBOOK_XLSX), { tenantId: who.tenantId });
    const ctx = { sandbox: ctxSandbox(service({}, files), who) };
    const { sandboxId } = await ctx.sandbox.create();
    const imported = await ctx.sandbox.importFile(sandboxId, { fileId: "excel", filename: "budget.xlsx" });
    const table = imported.tables[0];
    // `SELECT … FROM "sales_2024_1"` names no attached catalog: DuckDB 1.4.1 answers
    // `Catalog Error: Table with name sales_2024_1 does not exist!`, SQLite `no such table`.
    await expect(runSandboxQuery(ctx, sandboxId, `SELECT "Region" FROM "${table.tableName}" LIMIT 2000 OFFSET 0`)).rejects.toThrow(
      /^module execution failed: GonvexHostError: host call failed: no such table: sales_2024_1/,
    );
    // Qualified, the query succeeds, and `runSandboxQuery` keeps only an array result: [] on both runtimes.
    expect(await runSandboxQuery(ctx, sandboxId, `SELECT "Region" FROM "import_1"."${table.tableName}" LIMIT 2000 OFFSET 0`)).toEqual([]);
  });
});
