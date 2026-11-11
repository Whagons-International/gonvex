// The file readers behind `importFile`, against what calamine and DuckDB give Rust.
import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { unwrap } from "../src/answer";
import { rustFloat, sheetTableName } from "../src/ingest/cells";
import { CsvReader, sniffDelimiter } from "../src/ingest/csv";
import { extensionOf } from "../src/ingest/index";
import { headerNames } from "../src/ingest/table";
import { readXls } from "../src/ingest/xls";
import { readXlsx } from "../src/ingest/xlsx";
import { WORKBOOK_XLS, WORKBOOK_XLSX } from "./fixtures/workbooks";

const bytes = (base64: string) => Uint8Array.from(atob(base64), (c) => c.charCodeAt(0));
const stream = (data: Uint8Array | string) => new Blob([data]).stream();

function sheetRows(sheets: ReturnType<typeof readXlsx>) {
  return sheets.map((sheet) => {
    const grid = sheet.bounds();
    const rows: (string | null)[][] = [];
    if (grid) sheet.rows(grid, (row) => rows.push(row));
    return { name: sheet.name, index: sheet.index, grid, rows };
  });
}

const SALES = [
  ["Region", "Amount", "Closed", "Note", "Day"],
  ["north", "7", "true", "first & <one>", "45292"],
  ["south", "4.5", "false", "x", "45293"],
  [null, null, null, null, null],
  ["east", "Div0", "true", "", null],
];

describe("cells", () => {
  it("formats floats as Rust's Display does", () => {
    expect([45292, 4.5, 0.1, 1e21, 1.5e-7, -2.5e22, 123456789012345680000].map(rustFloat)).toEqual([
      "45292", "4.5", "0.1", "1000000000000000000000", "0.00000015", "-25000000000000000000000", "123456789012345680000",
    ]);
  });
  it("names sheet tables as unique_table_name does", () => {
    expect(sheetTableName("Sales 2024", 0)).toBe("sales_2024_1");
    expect(sheetTableName("2nd", 2)).toBe("sheet_3_3");
    expect(sheetTableName("  ", 1)).toBe("sheet_2_2");
    expect(sheetTableName("Ünïcode", 4)).toBe("n_code_5");
  });
  it("takes the extension as Path::extension does", () => {
    expect(["a.CSV", "x.tar.xlsx", ".csv", "noext", "dir.v1/file"].map(extensionOf)).toEqual(["csv", "xlsx", "", "", ""]);
  });
  it("names header columns as DuckDB does", () => {
    expect(headerNames(["a", "", "a", "A", null])).toEqual(["a", "column1", "a_1", "A_2", "column4"]);
  });
});

describe("delimited text", () => {
  const parse = (text: string, chunk = 3) => {
    const rows: (string | null)[][] = [];
    const reader = new CsvReader(sniffDelimiter(text, true), (row) => rows.push(row));
    for (let i = 0; i < text.length; i += chunk) reader.push(text.slice(i, i + chunk));
    reader.finish();
    return rows;
  };
  it("detects the delimiter", () => {
    expect(sniffDelimiter("a;b;c\n1;2;3\n", true)).toBe(";");
    expect(sniffDelimiter("a\tb\n1\t2\n", true)).toBe("\t");
    expect(sniffDelimiter("a|b\n1|2\n", true)).toBe("|");
    expect(sniffDelimiter("a,b\n1,2\n", true)).toBe(",");
    expect(sniffDelimiter("only\n1\n", true)).toBe(",");
  });
  it("reads quotes, escaped quotes, CRLF, blank lines and empty fields across chunk boundaries", () => {
    expect(parse('name,note\r\n"a, b","say ""hi"""\r\n\r\nc,\n"multi\nline",""\n')).toEqual([
      ["name", "note"],
      ["a, b", 'say "hi"'],
      ["c", null],
      ["multi\nline", null],
    ]);
  });
});

describe("workbooks", () => {
  it("reads an .xlsx as calamine does: used range, empty rows, every cell kind", () => {
    const sheets = sheetRows(readXlsx(bytes(WORKBOOK_XLSX)));
    expect(sheets.map((s) => [s.name, s.index])).toEqual([["Sales 2024", 0], ["Empty", 1], ["2nd", 2]]);
    expect(sheets[0]!.grid).toEqual({ minRow: 1, maxRow: 5, minCol: 1, maxCol: 5 });
    expect(sheets[0]!.rows).toEqual(SALES);
    expect(sheets[1]!.grid).toBeNull();
    expect(sheets[2]!.rows).toEqual([["k", "v"], ["a", "1"], ["b", "0.1"]]);
  });

  it("reads the same workbook saved as .xls (BIFF8) by LibreOffice", () => {
    const sheets = sheetRows(readXls(bytes(WORKBOOK_XLS)));
    expect(sheets.map((s) => [s.name, s.index])).toEqual([["Sales 2024", 0], ["Empty", 1], ["2nd", 2]]);
    // LibreOffice keeps the styled empty B5 and writes the empty shared string as a
    // blank cell; the values are otherwise the .xlsx values.
    expect(sheets[0]!.rows.filter((row) => row.some((cell) => cell !== null))).toEqual(SALES.filter((row) => row.some((cell) => cell !== null)).map((row) => row.map((cell) => (cell === "" ? null : cell))));
    expect(sheets[1]!.grid).toBeNull();
    expect(sheets[2]!.rows).toEqual([["k", "v"], ["a", "1"], ["b", "0.1"]]);
  });

  it("refuses what is not a workbook", () => {
    expect(() => readXlsx(new TextEncoder().encode("not a zip"))).toThrow(/Zip error/);
    expect(() => readXls(new TextEncoder().encode("not a compound file at all".repeat(30)))).toThrow(/Cfb error/);
  });
});

describe("tables", () => {
  const analysis = () => env.SANDBOX_ANALYSIS.getByName(`ingest-${crypto.randomUUID()}`);
  const query = async (object: ReturnType<typeof analysis>, statement: string) =>
    JSON.parse(unwrap(await object.call({ operation: "query", payload: { statement }, maxRows: 500, budgetBytes: 1 << 30 })).json) as { columns: string[]; rows: Record<string, unknown>[] };

  it("imports a CSV with DuckDB's types: BIGINT, DOUBLE, BOOLEAN as 1/0, dates as text, empty as NULL", async () => {
    const object = analysis();
    const csv = "id;amount;ok;day;name\n1;2.5;true;2024-01-02;a\n2;3;FALSE;2024-01-03;\n";
    const { tables } = unwrap(await object.ingest({ alias: "import_1", filename: "data.CSV", body: stream(csv), maxRows: 500 }));
    expect(tables).toEqual([{ tableName: "data", rowCount: 2, columns: ["id", "amount", "ok", "day", "name"] }]);
    const result = await query(object, 'SELECT * FROM "import_1"."data" ORDER BY id');
    expect(result.rows).toEqual([
      { id: 1, amount: 2.5, ok: 1, day: "2024-01-02", name: "a" },
      { id: 2, amount: 3, ok: 0, day: "2024-01-03", name: null },
    ]);
    const typed = await query(object, "SELECT typeof(id) AS a, typeof(amount) AS b FROM import_1.data LIMIT 1");
    expect(typed.rows).toEqual([{ a: "integer", b: "real" }]);
  });

  it("imports every non-empty sheet of a workbook and keeps nothing from a failed import", async () => {
    const object = analysis();
    const { tables } = unwrap(await object.ingest({ alias: "import_1", filename: "book.xlsx", body: stream(bytes(WORKBOOK_XLSX)), maxRows: 500 }));
    expect(tables).toEqual([
      { tableName: "sales_2024_1", rowCount: 4, columns: ["Region", "Amount", "Closed", "Note", "Day"] },
      { tableName: "sheet_3_3", rowCount: 2, columns: ["k", "v"] },
    ]);
    const amount = await query(object, 'SELECT "Amount" FROM import_1.sales_2024_1');
    // "Div0" makes the column VARCHAR, so DuckDB answers the numbers as text too.
    expect(amount.rows.map((row) => row["Amount"])).toEqual(["7", "4.5", null, "Div0"]);
    expect(await object.ingest({ alias: "import_2", filename: "bad.csv", body: stream("a,b\n1,2\n1,2,3\n"), maxRows: 500 })).toMatchObject({ ok: false, error: expect.stringContaining(
      "parse delimited file: Invalid Input Error: CSV Error on Line: 3: Expected Number of Columns: 2 Found: 3") });
    const tablesLeft = await query(object, "SHOW TABLES");
    expect(tablesLeft.rows.map((row) => row["name"])).toEqual(["import_1.sales_2024_1", "import_1.sheet_3_3"]);
  });

  it("refuses the types Rust refuses, and the two this runtime does not read", async () => {
    const object = analysis();
    expect(await object.ingest({ alias: "import_1", filename: "notes.pdf", body: stream("x"), maxRows: 500 })).toMatchObject({ ok: false, error: expect.stringContaining(
      "unsupported data file type .pdf (supported: CSV, TSV, XLSX, XLS, XLSB, ODS)") });
    expect(await object.ingest({ alias: "import_1", filename: "book.ods", body: stream("x"), maxRows: 500 })).toMatchObject({ ok: false, error: expect.stringContaining(
      "unsupported data file type .ods on this runtime") });
    const wide = `${Array.from({ length: 101 }, (_, i) => `c${i}`).join(",")}\n`;
    expect(await object.ingest({ alias: "import_1", filename: "wide.csv", body: stream(wide), maxRows: 500 })).toMatchObject({ ok: false, error: expect.stringContaining(
      "the file has 101 columns; at most 100 are supported") });
  });
});
