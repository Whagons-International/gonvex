// Office Open XML workbooks (.xlsx, .xlsm), read as calamine 0.36 reads them
// for Rust's `ingest_workbook`. The workbook parts are small and unzipped
// whole; each worksheet is inflated as a stream, twice (bounds, then rows),
// so a large sheet never sits in memory as XML.
import { Unzip, UnzipInflate, unzipSync } from "fflate";
import { ERROR_NAMES, rustFloat, type SheetGrid } from "./cells";
import type { WorkbookSheet } from "./workbook";

const decoder = () => new TextDecoder("utf-8");

/** XML character references and the five entities. */
export function xmlText(value: string): string {
  return value.replace(/&(#x[0-9a-fA-F]+|#\d+|amp|lt|gt|quot|apos);/g, (_match, entity: string) => {
    switch (entity) {
      case "amp": return "&";
      case "lt": return "<";
      case "gt": return ">";
      case "quot": return '"';
      case "apos": return "'";
      default: return String.fromCodePoint(entity[1] === "x" ? parseInt(entity.slice(2), 16) : parseInt(entity.slice(1), 10));
    }
  });
}

/** OOXML `_xHHHH_` escapes inside string values (calamine `unescape_xml`). */
const unescapeOoxml = (value: string): string => value.replace(/_x([0-9A-Fa-f]{4})_/g, (_m, hex: string) => String.fromCharCode(parseInt(hex, 16)));

const attributes = (tag: string): Record<string, string> => {
  const out: Record<string, string> = {};
  for (const match of tag.matchAll(/([A-Za-z_:][\w:.-]*)\s*=\s*(?:"([^"]*)"|'([^']*)')/g)) out[match[1]!] = xmlText(match[2] ?? match[3] ?? "");
  return out;
};

/** The text of every `<t>` outside phonetic runs. */
function runText(xml: string): string {
  const visible = xml.replace(/<rPh\b[\s\S]*?<\/rPh>/g, "");
  let out = "";
  for (const match of visible.matchAll(/<t\b[^>]*?(?:\/>|>([\s\S]*?)<\/t>)/g)) out += xmlText(match[1] ?? "");
  return unescapeOoxml(out);
}

const columnIndex = (reference: string): number => {
  let n = 0;
  for (const character of reference) {
    const code = character.charCodeAt(0);
    if (code < 65 || code > 90) break;
    n = n * 26 + (code - 64);
  }
  return n - 1;
};

const resolveTarget = (target: string): string => (target.startsWith("/") ? target.slice(1) : `xl/${target}`).replace(/\/\.\//g, "/");

interface Cell { col: number; value: string }

/** Streams the `<row>` elements of one worksheet part. */
function scanRows(bytes: Uint8Array, part: string, shared: readonly string[], onRow: (row: number, cells: Cell[]) => void): void {
  const textDecoder = decoder();
  let buffer = "";
  let lastRow = -1;
  const drain = (final: boolean) => {
    for (;;) {
      const start = buffer.indexOf("<row");
      if (start < 0) {
        if (!final) buffer = buffer.slice(Math.max(0, buffer.length - 4));
        return;
      }
      const tagEnd = buffer.indexOf(">", start);
      if (tagEnd < 0) { buffer = buffer.slice(start); return; }
      const selfClosing = buffer[tagEnd - 1] === "/";
      const end = selfClosing ? tagEnd + 1 : buffer.indexOf("</row>", tagEnd);
      if (end < 0) { buffer = buffer.slice(start); return; }
      const open = buffer.slice(start, tagEnd + 1);
      const body = selfClosing ? "" : buffer.slice(tagEnd + 1, end);
      buffer = buffer.slice(selfClosing ? end : end + 6);
      const rowAttr = attributes(open)["r"];
      const row = rowAttr ? Number(rowAttr) - 1 : lastRow + 1;
      lastRow = row;
      const cells: Cell[] = [];
      let lastCol = -1;
      for (const match of body.matchAll(/<c\b([^>]*?)(?:\/>|>([\s\S]*?)<\/c>)/g)) {
        const attrs = attributes(match[1] ?? "");
        const col = attrs["r"] ? columnIndex(attrs["r"]) : lastCol + 1;
        lastCol = col;
        const inner = match[2] ?? "";
        const type = attrs["t"] ?? "n";
        let value: string | null = null;
        if (type === "inlineStr") {
          const is = /<is\b[^>]*>([\s\S]*?)<\/is>/.exec(inner);
          if (is) value = runText(is[1]!);
        } else {
          const v = /<v\b[^>]*>([\s\S]*?)<\/v>/.exec(inner);
          if (v) {
            const raw = xmlText(v[1]!);
            switch (type) {
              case "s": value = shared[Number(raw)] ?? ""; break;
              case "b": value = raw === "1" || raw === "true" ? "true" : "false"; break;
              case "e": value = ERROR_NAMES[raw] ?? raw; break;
              case "str": case "d": value = unescapeOoxml(raw); break;
              default: {
                if (raw === "") break;
                const n = Number(raw);
                value = Number.isNaN(n) ? raw : rustFloat(n);
              }
            }
          }
        }
        if (value !== null) cells.push({ col, value });
      }
      onRow(row, cells);
    }
  };
  let found = false;
  const unzip = new Unzip((file) => {
    if (file.name !== part) return;
    found = true;
    file.ondata = (error, chunk, final) => {
      if (error) throw error;
      buffer += textDecoder.decode(chunk, { stream: !final });
      drain(final);
    };
    file.start();
  });
  unzip.register(UnzipInflate);
  unzip.push(bytes, true);
  if (!found) throw new Error(`worksheet part ${part} is missing`);
}

/** The worksheets of an .xlsx/.xlsm, in workbook order. */
export function readXlsx(bytes: Uint8Array): WorkbookSheet[] {
  const wanted = new Set(["xl/workbook.xml", "xl/_rels/workbook.xml.rels", "xl/sharedStrings.xml"]);
  let parts: Record<string, Uint8Array>;
  try {
    parts = unzipSync(bytes, { filter: (file) => wanted.has(file.name) });
  } catch (error) {
    throw new Error(`Zip error: ${(error as Error).message}`);
  }
  const workbook = parts["xl/workbook.xml"];
  if (!workbook) throw new Error("Xlsx error: workbook.xml is missing");
  const relsXml = parts["xl/_rels/workbook.xml.rels"] ? decoder().decode(parts["xl/_rels/workbook.xml.rels"]) : "";
  const targets = new Map<string, string>();
  for (const match of relsXml.matchAll(/<Relationship\b([^>]*?)\/?>/g)) {
    const attrs = attributes(match[1]!);
    if (attrs["Id"] && attrs["Target"] && /\/worksheet$/.test(attrs["Type"] ?? "")) targets.set(attrs["Id"], resolveTarget(attrs["Target"]));
  }
  const shared: string[] = [];
  if (parts["xl/sharedStrings.xml"]) {
    const xml = decoder().decode(parts["xl/sharedStrings.xml"]);
    for (const match of xml.matchAll(/<si\b[^>]*?(?:\/>|>([\s\S]*?)<\/si>)/g)) shared.push(runText(match[1] ?? ""));
  }
  const sheets: WorkbookSheet[] = [];
  const workbookXml = decoder().decode(workbook);
  for (const match of workbookXml.matchAll(/<sheet\b([^>]*?)\/?>/g)) {
    const attrs = attributes(match[1]!);
    const id = Object.entries(attrs).find(([key]) => key === "r:id" || key.endsWith(":id"))?.[1];
    const part = id ? targets.get(id) : undefined;
    if (!part) continue;
    const name = attrs["name"] ?? "";
    const index = sheets.length;
    sheets.push({
      name,
      index,
      bounds(): SheetGrid | null {
        let grid: SheetGrid | null = null;
        scanRows(bytes, part, shared, (row, cells) => {
          for (const cell of cells) {
            if (grid === null) grid = { minRow: row, maxRow: row, minCol: cell.col, maxCol: cell.col };
            else {
              grid.minRow = Math.min(grid.minRow, row);
              grid.maxRow = Math.max(grid.maxRow, row);
              grid.minCol = Math.min(grid.minCol, cell.col);
              grid.maxCol = Math.max(grid.maxCol, cell.col);
            }
          }
        });
        return grid;
      },
      rows(grid: SheetGrid, onRow: (row: (string | null)[]) => void): void {
        const width = grid.maxCol - grid.minCol + 1;
        let next = grid.minRow;
        const emptyRow = () => new Array<string | null>(width).fill(null);
        scanRows(bytes, part, shared, (row, cells) => {
          if (row < grid.minRow || row > grid.maxRow) return;
          while (next < row) { onRow(emptyRow()); next++; }
          const values = emptyRow();
          for (const cell of cells) if (cell.col >= grid.minCol && cell.col <= grid.maxCol) values[cell.col - grid.minCol] = cell.value;
          onRow(values);
          next = row + 1;
        });
        while (next <= grid.maxRow) { onRow(emptyRow()); next++; }
      },
    });
  }
  return sheets;
}
