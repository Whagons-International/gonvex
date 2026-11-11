// Rows read and written by the analysis object per call (SqlStorageCursor
// counters, index entries included), as the PR states them. The workspace
// object keeps files and records in key-value storage and runs no SQL.
import { env } from "cloudflare:workers";
import { expect, it } from "vitest";
import { unwrap } from "../src/answer";
import { WORKBOOK_XLSX } from "./fixtures/workbooks";

const bytes = (base64: string) => Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
const stream = (data: Uint8Array | string) => new Blob([data]).stream();
const TASKS = Array.from({ length: 1000 }, (_, i) => ({ id: `t${i}`, statusId: ["open", "done", "late"][i % 3], hours: i % 7 }));

it("rows per analysis call", async () => {
  const object = env.SANDBOX_ANALYSIS.getByName(`numbers-${crypto.randomUUID()}`);
  const call = async (operation: "query" | "register", payload: Record<string, unknown>) => {
    const { stats } = unwrap(await object.call({ operation, payload, maxRows: 500, budgetBytes: 1 << 30 }));
    return { statements: stats.statements, rowsRead: stats.rowsRead, rowsWritten: stats.rowsWritten };
  };
  const numbers = {
    register1000: await call("register", { name: "tasks", rows: TASKS }),
    groupBy1000: await call("query", { statement: "SELECT statusId, count(*) AS total FROM tasks GROUP BY statusId" }),
    point: await call("query", { statement: "SELECT * FROM tasks WHERE id = $1", parameters: ["t5"] }),
    summarize3Columns: await call("query", { statement: "SUMMARIZE tasks" }),
    importCsv100: (({ statements, rowsRead, rowsWritten }) => ({ statements, rowsRead, rowsWritten }))(unwrap(await object.ingest({ alias: "import_1", filename: "a.csv", maxRows: 500, body: stream(`id,v\n${Array.from({ length: 100 }, (_, i) => `${i},${i * 2}`).join("\n")}\n`) })).stats),
    importXlsx: (({ statements, rowsRead, rowsWritten }) => ({ statements, rowsRead, rowsWritten }))(unwrap(await object.ingest({ alias: "import_2", filename: "b.xlsx", maxRows: 500, body: stream(bytes(WORKBOOK_XLSX)) })).stats),
  };
  expect(numbers).toEqual({
    register1000: { statements: 3, rowsRead: 1001, rowsWritten: 1002 }, // DROP, CREATE, one json_each INSERT
    groupBy1000: { statements: 1, rowsRead: 2000, rowsWritten: 0 },
    point: { statements: 1, rowsRead: 1000, rowsWritten: 0 }, // a registered table has no index, as in DuckDB
    summarize3Columns: { statements: 8, rowsRead: 11000, rowsWritten: 0 },
    importCsv100: { statements: 2, rowsRead: 101, rowsWritten: 102 },
    importXlsx: { statements: 4, rowsRead: 8, rowsWritten: 10 },
  });
});
