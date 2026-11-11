// `ingest_workbook` (RT/data_ingest.rs:65-114): one table per non-empty sheet,
// with Rust's names, limits and texts.
import type { ImportedTable } from "../backend";
import { debugString, sheetTableName, type SheetGrid } from "./cells";
import { MAX_TABLE_COLUMNS, TableSink } from "./table";

export interface WorkbookSheet {
  name: string;
  /** Position among all sheets, empty ones included (it numbers the table). */
  index: number;
  /** The used range, or null for an empty sheet. */
  bounds(): SheetGrid | null;
  /** Every row of the range, empty rows included, `width` cells each. */
  rows(grid: SheetGrid, onRow: (row: (string | null)[]) => void): void;
}

const MAX_COLUMNS = 200;
const MAX_SHEET_ROWS = 2_000_000;

export function ingestSheets(sql: SqlStorage, alias: string, sheets: readonly WorkbookSheet[]): ImportedTable[] {
  const tables: ImportedTable[] = [];
  for (const sheet of sheets) {
    const grid = sheet.bounds();
    if (grid === null) continue;
    const height = grid.maxRow - grid.minRow + 1;
    const width = grid.maxCol - grid.minCol + 1;
    if (height > MAX_SHEET_ROWS + 1) throw new Error(`sheet ${debugString(sheet.name)} exceeds the row limit`);
    // Rust's limit is 200; Durable Object SQLite stores at most 100 columns (note 0040 §4).
    if (width > MAX_COLUMNS || width > MAX_TABLE_COLUMNS) throw new Error(`sheet ${debugString(sheet.name)} exceeds the column limit`);
    const table = sheetTableName(sheet.name, sheet.index);
    const sink = new TableSink(sql, `${alias}.${table}`, table, () => `sheet ${debugString(sheet.name)} exceeds the column limit`);
    try {
      sheet.rows(grid, (row) => sink.push(row));
    } catch (error) {
      throw new Error(`parse sheet ${debugString(sheet.name)}: ${(error as Error).message}`);
    }
    const meta = sink.finish();
    if (meta) tables.push(meta);
  }
  if (tables.length === 0) throw new Error("workbook has no non-empty sheets");
  return tables;
}
