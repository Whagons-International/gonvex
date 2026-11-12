// The Whagons sandbox call sites, copied from whagons5-client `origin/staging`
// at a6de676951 (read-only reference; the app is not changed). Bodies are
// verbatim; only the context types are loosened so they run against the test
// `ctx.sandbox`. Sources:
//   gonvex/assistant/executionContext.ts  waitForSandbox, runSandboxCode, sqlIdentifier,
//     importedTableSqlName, qualifyImportedTables, boundedDuckDbQueryCode,
//     resolveAnalysisFile, createTaskSnapshotter, analyzeWorkspaceTasks, importAssistantDataFile
//   gonvex/assistantExcelActions.ts       runSandboxQuery, and parseExcelAttachment's import
//   gonvex/assistantData.ts               ingestAttachment's import
//   gonvex/agentToolRuntime.ts            the runCode tool
/* eslint-disable */
type JsonValue = any;
type Row = Record<string, any>;
type Ctx = { sandbox: any; tools?: any };

const text = (value: unknown): string => value == null ? "" : String(value);
const objectValue = (value: unknown): Row => (value !== null && typeof value === "object" && !Array.isArray(value) ? value as Row : {});

export const jsonValue = (value: unknown): JsonValue => {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value)) as JsonValue;
};

export const waitForSandbox = async (
  ctx: Ctx,
  sandboxId: string,
  executionId: string,
): Promise<JsonValue> => {
  // Poll once per second while allowing time for queueing and execution.
  for (let attempt = 0; attempt < 80; attempt += 1) {
    const state = await ctx.sandbox.status(sandboxId, executionId);
    const status = String(state.status);
    // The SDK's canonical terminal success state is `succeeded`. Accept the
    // legacy runtime value `completed` while older deployments roll forward.
    if (status === "completed" || status === "succeeded") {
      if (state.result !== undefined && state.result !== null) return state.result;

      // Models commonly use console.log for tiny ad-hoc calculations. Preserve
      // that output instead of reporting a misleading successful `null` result.
      // A single JSON-looking line is decoded so `console.log(42)` behaves like
      // `return 42`; multiple lines remain structured and retain their levels.
      const logs = Array.isArray(state.logs) ? state.logs : [];
      if (logs.length === 1) {
        const message = text(objectValue(logs[0]).message);
        if (message) {
          try {
            return jsonValue(JSON.parse(message));
          } catch {
            return message;
          }
        }
      }
      if (logs.length > 0) {
        return {
          logs: logs.map((entry: unknown) => {
            const line = objectValue(entry);
            return { level: text(line.level) || "log", message: text(line.message) };
          }),
        };
      }
      return null;
    }
    if (status !== "queued" && status !== "running") {
      throw new Error(state.error ?? `Sandbox ${status}`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 1000));
  }
  await ctx.sandbox.cancel(sandboxId, executionId);
  throw new Error("Sandbox analysis exceeded the Action wait budget");
};

export const runSandboxCode = async (
  ctx: Ctx,
  sandboxId: string,
  code: string,
  timeoutMs = 30_000,
): Promise<JsonValue> => {
  const execution = await ctx.sandbox.run(sandboxId, {
    code,
    timeoutMs: Math.min(120_000, Math.max(100, timeoutMs)),
  });
  return waitForSandbox(ctx, sandboxId, execution.executionId);
};

export const importAssistantDataFile = async (
  ctx: Ctx,
  sandboxId: string,
  file: Row,
): Promise<{ file: Row; imported: JsonValue }> => {
  // The Whagons helper first reads the row through ctx.tools.dataFileForAnalysis; the row is given here.
  const imported = await ctx.sandbox.importFile(sandboxId, {
    fileId: text(file.storageId),
    filename: text(file.filename),
  });
  return { file, imported: jsonValue(imported) };
};

export const sqlIdentifier = (value: string): string => `"${value.replace(/"/g, '""')}"`;

export const importedTableSqlName = (imported: JsonValue, requested?: string): string => {
  const metadata = objectValue(imported);
  const alias = text(metadata.alias);
  const tables = Array.isArray(metadata.tables) ? metadata.tables.map(objectValue) : [];
  const requestedLeaf = requested?.split(".").at(-1)?.replace(/^"|"$/g, "");
  const table = requestedLeaf
    ? tables.find((candidate: Row) => text(candidate.tableName) === requestedLeaf)
    : tables[0];
  const tableName = text(table?.tableName);
  if (!alias || !tableName) throw new Error("The uploaded file has no queryable table");
  return `${sqlIdentifier(alias)}.${sqlIdentifier(tableName)}`;
};

export const qualifyImportedTables = (sql: string, imported: JsonValue): string => {
  const metadata = objectValue(imported);
  const alias = text(metadata.alias);
  const tables = Array.isArray(metadata.tables) ? metadata.tables.map(objectValue) : [];
  let qualified = sql;
  for (const table of tables) {
    const tableName = text(table.tableName);
    if (!alias || !tableName) continue;
    const alreadyQualified = new RegExp(`(?:"${alias}"|${alias})\\s*\\.\\s*(?:"${tableName}"|${tableName})`, "i");
    if (alreadyQualified.test(qualified)) continue;
    const escaped = tableName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    qualified = qualified.replace(
      new RegExp(`\\b(from|join)\\s+(?:"${escaped}"|${escaped})\\b`, "gi"),
      (_match, keyword: string) => `${keyword} ${sqlIdentifier(alias)}.${sqlIdentifier(tableName)}`,
    );
  }
  return qualified;
};

export const boundedDuckDbQueryCode = (sql: string, limit: number): string => `
  const response = await duckdb.query(${JSON.stringify(sql)});
  if (Array.isArray(response)) return response.slice(0, ${limit});
  const rows = Array.isArray(response.rows) ? response.rows.slice(0, ${limit}) : [];
  return {...response, rows, rowCount: rows.length, truncated: Boolean(response.truncated) || Number(response.rowCount || 0) > rows.length};
`;

export const resolveAnalysisFile = async (
  fileKey: string | undefined,
  snapshots: ReadonlyMap<string, JsonValue>,
  importUpload: (key: string) => Promise<JsonValue>,
): Promise<JsonValue | null> => {
  if (!fileKey) return null;
  // Snapshot paths already exist in this sandbox; they are not uploaded files.
  const snapshot = snapshots.get(fileKey);
  return snapshot ?? importUpload(fileKey);
};

// Each factory belongs to one newly created sandbox. Reserve the name before
// any await so concurrent snapshots, including the same workspace, cannot alias.
export const createTaskSnapshotter = (
  ctx: Ctx,
  withSandbox: <T>(operation: (sandboxId: string) => Promise<T>) => Promise<T>,
  snapshots: Map<string, JsonValue> = new Map(),
) => {
  let sequence = 0;
  return async (input: { workspaceId?: string; maxRows?: number }) => {
    if (!input.workspaceId) throw new Error("workspaceId is required for a task snapshot");
    const workspaceId = input.workspaceId;
    const path = `tasks-snapshot-${++sequence}.json`;
    const maxRows = Math.min(50_000, Math.max(1, input.maxRows ?? 10_000));
    const rows: Row[] = [];
    for (let offset = 0; rows.length < maxRows; offset += 1000) {
      const page = await ctx.tools.tasksForAnalysis({
        workspaceId: input.workspaceId, offset, limit: Math.min(1000, maxRows - rows.length),
      });
      rows.push(...page);
      if (page.length < 1000) break;
    }
    return withSandbox(async sandboxId => {
      await ctx.sandbox.writeText(sandboxId, path, JSON.stringify(rows));
      const result = { sandboxId, path, fileKey: path, workspaceId, rowCount: rows.length, complete: rows.length < maxRows };
      snapshots.set(path, result);
      return result;
    });
  };
};

export const analyzeWorkspaceTasks = async (
  ctx: Ctx,
  args: { workspaceId: string; fileId?: string; filename?: string },
): Promise<JsonValue> => {
  if (Boolean(args.fileId) !== Boolean(args.filename)) {
    throw new Error("fileId and filename must be supplied together");
  }
  const sandbox = await ctx.sandbox.create();
  const tasks = await ctx.tools.tasksForAnalysis({
    workspaceId: args.workspaceId,
    offset: 0,
    limit: 1000,
  });
  await ctx.sandbox.writeText(sandbox.sandboxId, "tasks.json", JSON.stringify(tasks));
  if (args.fileId && args.filename) {
    await ctx.sandbox.importFile(sandbox.sandboxId, {
      fileId: args.fileId,
      filename: args.filename,
    });
  }
  const execution = await ctx.sandbox.run(sandbox.sandboxId, {
    code: `
      const tasks = JSON.parse(await files.readText("tasks.json"));
      await duckdb.register("tasks", tasks);
      return await duckdb.query(
        "SELECT statusId, count(*) AS total " +
        "FROM tasks GROUP BY statusId ORDER BY total DESC"
      );
    `,
    timeoutMs: 30_000,
  });
  return waitForSandbox(ctx, sandbox.sandboxId, execution.executionId);
};

const isJsonObject = (value: JsonValue | undefined): value is Row => (
  value !== null && typeof value === "object" && !Array.isArray(value)
);

/** assistantExcelActions.ts `runSandboxQuery`. */
export const runSandboxQuery = async (
  ctx: Ctx,
  sandboxId: string,
  sql: string,
): Promise<Row[]> => {
  const execution = await ctx.sandbox.run(sandboxId, {
    code: `return await duckdb.query(${JSON.stringify(sql)});`,
    timeoutMs: 30_000,
  });
  for (let attempt = 0; attempt < 40; attempt += 1) {
    const state = await ctx.sandbox.status(sandboxId, execution.executionId);
    if (state.status === "succeeded") {
      return Array.isArray(state.result) ? state.result.filter(isJsonObject) : [];
    }
    if (state.status !== "queued" && state.status !== "running") {
      throw new Error(state.error ?? `Sandbox ${state.status}`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 1000));
  }
  await ctx.sandbox.cancel(sandboxId, execution.executionId);
  throw new Error("Excel import exceeded the sandbox wait budget");
};

/** agentToolRuntime.ts: the `runCode` tool and `close`. */
export const cloudRunCode = (ctx: Ctx) => {
  let sandboxId: string | undefined;
  return {
    execute: async ({ code }: { code: string }) => {
      if (!sandboxId) sandboxId = (await ctx.sandbox.create({ ttlMs: 300000 })).sandboxId;
      return runSandboxCode(ctx, sandboxId!, code, 30000);
    },
    close: async () => { if (sandboxId) await ctx.sandbox.destroy(sandboxId); },
  };
};
