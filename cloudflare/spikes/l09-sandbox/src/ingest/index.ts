// `data_ingest::ingest` (RT/data_ingest.rs:21-53): a CSV or spreadsheet upload
// becomes read-only tables under one import alias.
import type { ImportedTable } from "../../../../sandbox/src/backend";
import { CsvReader, sniffDelimiter } from "./csv";
import { MAX_TABLE_COLUMNS, TableSink } from "./table";
import { ingestSheets } from "./workbook";
import { readXls } from "./xls";
import { readXlsx } from "./xlsx";

/** Rust's `Path::extension`, lower-cased: after the last dot, unless the name only starts with one. */
export function extensionOf(filename: string): string {
  const base = filename.split("/").pop() ?? "";
  const dot = base.lastIndexOf(".");
  return dot <= 0 ? "" : base.slice(dot + 1).toLowerCase();
}

async function readAll(body: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  return new Uint8Array(await new Response(body).arrayBuffer());
}

async function ingestDelimited(sql: SqlStorage, alias: string, body: ReadableStream<Uint8Array>): Promise<ImportedTable[]> {
  const sink = new TableSink(sql, `${alias}.data`, "data", (columns) => `the file has ${columns} columns; at most ${MAX_TABLE_COLUMNS} are supported`);
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
  let reader: CsvReader | null = null;
  let head = "";
  let width = -1;
  const onRecord = (record: (string | null)[], line: number) => {
    if (width < 0) width = record.length;
    else if (record.length !== width) throw new Error(`Invalid Input Error: CSV Error on Line: ${line}: Expected Number of Columns: ${width} Found: ${record.length}`);
    sink.push(record);
  };
  const feed = (text: string, final: boolean) => {
    if (reader === null) {
      head += text;
      if (head.length < 64 * 1024 && !final) return;
      if (head.charCodeAt(0) === 0xfeff) head = head.slice(1);
      reader = new CsvReader(sniffDelimiter(head, final), onRecord);
      reader.push(head);
      head = "";
      return;
    }
    reader.push(text);
  };
  try {
    const stream = body.getReader();
    for (;;) {
      const { value, done } = await stream.read();
      if (done) break;
      feed(decoder.decode(value, { stream: true }), false);
    }
    feed(decoder.decode(), true);
    (reader as CsvReader | null)?.finish();
  } catch (error) {
    const message = error instanceof TypeError && /decode|encoded/i.test(error.message) ? "Invalid Input Error: Invalid unicode (byte sequence mismatch) detected in CSV file" : (error as Error).message;
    throw new Error(`parse delimited file: ${message}`);
  }
  const table = sink.finish();
  if (!table) throw new Error("parse delimited file: Invalid Input Error: the file is empty");
  return [table];
}

/** Creates the tables `"<alias>.<table>"`; the caller drops them if this throws. */
export async function ingestFile(sql: SqlStorage, alias: string, filename: string, body: ReadableStream<Uint8Array>): Promise<ImportedTable[]> {
  const extension = extensionOf(filename);
  switch (extension) {
    case "csv": case "tsv": case "txt":
      return ingestDelimited(sql, alias, body);
    case "xlsx": case "xlsm":
      return ingestSheets(sql, alias, readXlsx(await readAll(body)));
    case "xls":
      return ingestSheets(sql, alias, readXls(await readAll(body)));
    case "xlsb": case "ods":
      await body.cancel().catch(() => undefined);
      throw new Error(`unsupported data file type .${extension} on this runtime (supported: CSV, TSV, XLSX, XLSM, XLS)`);
    default:
      await body.cancel().catch(() => undefined);
      throw new Error(`unsupported data file type .${extension} (supported: CSV, TSV, XLSX, XLS, XLSB, ODS)`);
  }
}
