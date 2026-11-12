#!/usr/bin/env node
// A stand-in for `k20-ingest` (Rust's data_ingest.rs) in CI: a CSV's header
// and row count, Rust's text for an unsupported type.
import { readFileSync, writeFileSync } from "node:fs";

const [target, filename] = process.argv.slice(2);
const extension = (filename.match(/\.([^./]+)$/)?.[1] ?? "").toLowerCase();
if (!["csv", "tsv", "txt", "xlsx", "xlsm", "xls", "xlsb", "ods"].includes(extension)) {
  process.stderr.write(`unsupported data file type .${extension} (supported: CSV, TSV, XLSX, XLS, XLSB, ODS)`);
  process.exit(2);
}
const lines = readFileSync(0, "utf8").split(/\r?\n/).filter(Boolean);
writeFileSync(target, "");
process.stdout.write(JSON.stringify([{ tableName: "data", rowCount: Math.max(0, lines.length - 1), columns: (lines[0] ?? "").split(",") }]));
