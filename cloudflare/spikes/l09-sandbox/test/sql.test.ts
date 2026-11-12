// The statement rewrite in front of SQLite (src/sql.ts).
import { describe, expect, it } from "vitest";
import { prepareStatements, specialStatement } from "../src/sql";

const imports = new Map([["import_1", new Set(["data", "sheet_2_2"])]]);

describe("prepareStatements", () => {
  it("maps catalog-qualified import tables to their stored names, in any quoting and case", () => {
    expect(prepareStatements('SELECT * FROM "import_1"."data" d JOIN Import_1 . "SHEET_2_2" s ON 1', imports)).toEqual([
      'SELECT * FROM "import_1.data" d JOIN "import_1.sheet_2_2" s ON 1',
    ]);
    expect(prepareStatements("select * from import_1.main.data", imports)).toEqual(['select * from "import_1.data"']);
  });

  it("leaves everything else alone: unknown tables, column paths, strings, comments", () => {
    const sql = "SELECT import_1.other, t.import_1, 'import_1.data;' AS s FROM x.import_1.data -- import_1.data\nWHERE a = 1";
    expect(prepareStatements(sql, imports)).toEqual([sql]);
  });

  it("splits statements as duckdb-rs runs them, and turns $N into ?N", () => {
    expect(prepareStatements("CREATE TABLE t(a); INSERT INTO t VALUES ($1);; SELECT * FROM t WHERE a = $1;", imports)).toEqual([
      "CREATE TABLE t(a)",
      " INSERT INTO t VALUES (?1)",
      " SELECT * FROM t WHERE a = ?1",
    ]);
    expect(prepareStatements("  ; -- nothing\n", imports)).toEqual([]);
  });
});

describe("specialStatement", () => {
  it("recognises DuckDB's SUMMARIZE, DESCRIBE and SHOW TABLES", () => {
    expect(specialStatement("SUMMARIZE SELECT * FROM t;")).toEqual({ kind: "summarize", source: "SELECT * FROM t" });
    expect(specialStatement("summarize import_1.data")).toEqual({ kind: "summarize", source: "SELECT * FROM import_1.data" });
    expect(specialStatement("DESCRIBE FROM t")).toEqual({ kind: "describe", source: "SELECT * FROM t" });
    expect(specialStatement("show tables")).toEqual({ kind: "showTables" });
    expect(specialStatement("SHOW databases")).toBeNull();
    expect(specialStatement("SELECT 1")).toBeNull();
  });
});
