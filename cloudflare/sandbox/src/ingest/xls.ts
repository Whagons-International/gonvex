// Legacy Excel workbooks (.xls, BIFF8 in a compound file), the subset
// calamine reads for Rust's `ingest_workbook`: cell values (numbers, RK and
// MULRK, shared and inline strings, booleans, errors, cached formula results)
// per worksheet. Formats, styles and formulas themselves are ignored, as in
// Rust, where a date cell becomes its serial number.
import { BIFF_ERRORS, rustFloat, type SheetGrid } from "./cells";
import type { WorkbookSheet } from "./workbook";

const END_OF_CHAIN = 0xfffffffe;
const FREE = 0xffffffff;

/** The `Workbook` stream of a compound file (MS-CFB). */
export function workbookStream(bytes: Uint8Array): Uint8Array {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const signature = [0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1];
  if (bytes.byteLength < 512 || signature.some((b, i) => bytes[i] !== b)) throw new Error("Cfb error: invalid file signature");
  const sectorSize = 1 << view.getUint16(0x1e, true);
  const miniSectorSize = 1 << view.getUint16(0x20, true);
  const firstDirSector = view.getUint32(0x30, true);
  const miniCutoff = view.getUint32(0x38, true);
  const firstMiniFat = view.getUint32(0x3c, true);
  let difatSector = view.getUint32(0x44, true);
  const sector = (n: number): Uint8Array => {
    const start = (n + 1) * sectorSize;
    if (start + sectorSize > bytes.byteLength + sectorSize || start >= bytes.byteLength) throw new Error("Cfb error: sector out of range");
    return bytes.subarray(start, Math.min(bytes.byteLength, start + sectorSize));
  };
  const fatSectors: number[] = [];
  for (let i = 0; i < 109; i++) {
    const n = view.getUint32(0x4c + i * 4, true);
    if (n !== FREE) fatSectors.push(n);
  }
  for (let guard = 0; difatSector !== END_OF_CHAIN && difatSector !== FREE && guard < 100_000; guard++) {
    const s = sector(difatSector);
    const sv = new DataView(s.buffer, s.byteOffset, s.byteLength);
    const entries = sectorSize / 4 - 1;
    for (let i = 0; i < entries; i++) {
      const n = sv.getUint32(i * 4, true);
      if (n !== FREE) fatSectors.push(n);
    }
    difatSector = sv.getUint32(entries * 4, true);
  }
  const fat: number[] = [];
  for (const n of fatSectors) {
    const s = sector(n);
    const sv = new DataView(s.buffer, s.byteOffset, s.byteLength);
    for (let i = 0; i + 4 <= s.byteLength; i += 4) fat.push(sv.getUint32(i, true));
  }
  const chain = (start: number, table: readonly number[]): number[] => {
    const out: number[] = [];
    for (let n = start; n !== END_OF_CHAIN && n !== FREE; n = table[n] ?? END_OF_CHAIN) {
      if (out.length > table.length) throw new Error("Cfb error: sector chain loops");
      out.push(n);
    }
    return out;
  };
  const read = (start: number, size: number): Uint8Array => {
    const out = new Uint8Array(size);
    let offset = 0;
    for (const n of chain(start, fat)) {
      if (offset >= size) break;
      const s = sector(n).subarray(0, size - offset);
      out.set(s, offset);
      offset += s.byteLength;
    }
    if (offset < size) throw new Error("Cfb error: stream is truncated");
    return out;
  };
  const directory = (() => {
    const sectors = chain(firstDirSector, fat);
    const out = new Uint8Array(sectors.length * sectorSize);
    sectors.forEach((n, i) => out.set(sector(n), i * sectorSize));
    return out;
  })();
  const dv = new DataView(directory.buffer);
  let root: { start: number; size: number } | null = null;
  let workbook: { start: number; size: number } | null = null;
  let biff5 = false;
  for (let offset = 0; offset + 128 <= directory.byteLength; offset += 128) {
    const type = directory[offset + 0x42];
    const nameLength = dv.getUint16(offset + 0x40, true);
    let name = "";
    for (let i = 0; i + 2 < nameLength && i < 64; i += 2) name += String.fromCharCode(dv.getUint16(offset + i, true));
    const entry = { start: dv.getUint32(offset + 0x74, true), size: dv.getUint32(offset + 0x78, true) };
    if (type === 5) root = entry;
    else if (type === 2 && name.toLowerCase() === "workbook") workbook = entry;
    else if (type === 2 && name.toLowerCase() === "book") biff5 = true;
  }
  if (!workbook) throw new Error(biff5 ? "Xls error: BIFF5 workbooks are not supported; save the file as .xlsx" : "Xls error: no Workbook stream");
  if (workbook.size >= miniCutoff) return read(workbook.start, workbook.size);
  if (!root) throw new Error("Cfb error: no root entry");
  const ministream = read(root.start, root.size);
  const minifatBytes = chain(firstMiniFat, fat).map((n) => sector(n));
  const minifat: number[] = [];
  for (const s of minifatBytes) {
    const sv = new DataView(s.buffer, s.byteOffset, s.byteLength);
    for (let i = 0; i + 4 <= s.byteLength; i += 4) minifat.push(sv.getUint32(i, true));
  }
  const out = new Uint8Array(workbook.size);
  let offset = 0;
  for (const n of chain(workbook.start, minifat)) {
    if (offset >= workbook.size) break;
    const piece = ministream.subarray(n * miniSectorSize, Math.min(ministream.byteLength, (n + 1) * miniSectorSize)).subarray(0, workbook.size - offset);
    out.set(piece, offset);
    offset += piece.byteLength;
  }
  return out;
}

interface Record { type: number; data: Uint8Array; offset: number }

function* records(stream: Uint8Array, from = 0): Generator<Record> {
  const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
  let offset = from;
  while (offset + 4 <= stream.byteLength) {
    const type = view.getUint16(offset, true);
    const size = view.getUint16(offset + 2, true);
    const data = stream.subarray(offset + 4, Math.min(stream.byteLength, offset + 4 + size));
    yield { type, data, offset };
    offset += 4 + size;
  }
}

/** Reads across a record and its CONTINUE records (MS-XLS 2.1.4). */
class Segments {
  private index = 0;
  private position = 0;
  constructor(private readonly parts: Uint8Array[]) {}

  private current(): Uint8Array {
    while (this.index < this.parts.length && this.position >= this.parts[this.index]!.byteLength) {
      this.index++;
      this.position = 0;
    }
    const part = this.parts[this.index];
    if (!part) throw new Error("Xls error: record ends early");
    return part;
  }

  u8(): number { const part = this.current(); return part[this.position++]!; }
  u16(): number { return this.u8() | (this.u8() << 8); }
  u32(): number { return (this.u16() | (this.u16() << 16)) >>> 0; }
  skip(n: number): void { for (let i = 0; i < n; i++) this.u8(); }

  /** Characters that may continue in the next segment, which then starts with a new flags byte. */
  chars(count: number, high: boolean): string {
    let out = "";
    let left = count;
    let wide = high;
    while (left > 0) {
      const part = this.current();
      const size = wide ? 2 : 1;
      const available = Math.floor((part.byteLength - this.position) / size);
      const take = Math.min(left, available);
      for (let i = 0; i < take; i++) {
        out += String.fromCharCode(wide ? part[this.position]! | (part[this.position + 1]! << 8) : part[this.position]!);
        this.position += size;
      }
      left -= take;
      if (left > 0) {
        this.index++;
        this.position = 0;
        const next = this.parts[this.index];
        if (!next) throw new Error("Xls error: string ends early");
        wide = (next[0]! & 1) === 1;
        this.position = 1;
      }
    }
    return out;
  }

  /** XLUnicodeRichExtendedString. */
  richString(): string {
    const count = this.u16();
    const flags = this.u8();
    const runs = flags & 0x08 ? this.u16() : 0;
    const ext = flags & 0x04 ? this.u32() : 0;
    const text = this.chars(count, (flags & 1) === 1);
    this.skip(runs * 4 + ext);
    return text;
  }
}

/** XLUnicodeString (LABEL, STRING): `cch` u16, flags, characters. */
function unicodeString(data: Uint8Array, offset: number): string {
  return new Segments([data.subarray(offset)]).richString();
}

const rk = (value: number): number => {
  let n: number;
  if (value & 2) n = (value | 0) >> 2;
  else {
    const buffer = new DataView(new ArrayBuffer(8));
    buffer.setUint32(4, value & 0xfffffffc, true);
    n = buffer.getFloat64(0, true);
  }
  return value & 1 ? n / 100 : n;
};

interface SheetInfo { name: string; position: number }
type Cells = Map<number, Map<number, string>>;

function sheetCells(stream: Uint8Array, position: number, shared: readonly string[]): Cells {
  const cells: Cells = new Map();
  const set = (row: number, col: number, value: string) => {
    let line = cells.get(row);
    if (!line) cells.set(row, (line = new Map()));
    line.set(col, value);
  };
  let pendingString: { row: number; col: number } | null = null;
  let first = true;
  for (const { type, data } of records(stream, position)) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    if (first) {
      first = false;
      if (type !== 0x0809) throw new Error("Xls error: worksheet has no BOF record");
      continue;
    }
    if (type === 0x000a) break; // EOF
    if (data.byteLength < 6 && type !== 0x0207) continue;
    switch (type) {
      case 0x0203: set(view.getUint16(0, true), view.getUint16(2, true), rustFloat(view.getFloat64(6, true))); break; // NUMBER
      case 0x027e: set(view.getUint16(0, true), view.getUint16(2, true), rustFloat(rk(view.getUint32(6, true)))); break; // RK
      case 0x00bd: { // MULRK
        const row = view.getUint16(0, true);
        const firstCol = view.getUint16(2, true);
        const count = Math.floor((data.byteLength - 6) / 6);
        for (let i = 0; i < count; i++) set(row, firstCol + i, rustFloat(rk(view.getUint32(4 + i * 6 + 2, true))));
        break;
      }
      case 0x00fd: set(view.getUint16(0, true), view.getUint16(2, true), shared[view.getUint32(6, true)] ?? ""); break; // LABELSST
      case 0x0204: case 0x00d6: set(view.getUint16(0, true), view.getUint16(2, true), unicodeString(data, 6)); break; // LABEL, RSTRING
      case 0x0205: { // BOOLERR
        const value = data[6]!;
        set(view.getUint16(0, true), view.getUint16(2, true), data[7] ? (BIFF_ERRORS[value] ?? `Error(${value})`) : value ? "true" : "false");
        break;
      }
      case 0x0006: { // FORMULA: the cached result
        const row = view.getUint16(0, true);
        const col = view.getUint16(2, true);
        if (view.getUint16(12, true) !== 0xffff) set(row, col, rustFloat(view.getFloat64(6, true)));
        else {
          const kind = data[6];
          if (kind === 0) pendingString = { row, col };
          else if (kind === 1) set(row, col, data[8] ? "true" : "false");
          else if (kind === 2) set(row, col, BIFF_ERRORS[data[8]!] ?? `Error(${data[8]})`);
          else if (kind === 3) set(row, col, "");
        }
        break;
      }
      case 0x0207: // STRING: the string result of the FORMULA before it
        if (pendingString) set(pendingString.row, pendingString.col, unicodeString(data, 0));
        pendingString = null;
        break;
    }
  }
  return cells;
}

/** The sheets of an .xls workbook, in workbook order. */
export function readXls(bytes: Uint8Array): WorkbookSheet[] {
  const stream = workbookStream(bytes);
  const sheets: SheetInfo[] = [];
  const sstParts: Uint8Array[] = [];
  let inSst = false;
  let first = true;
  for (const { type, data } of records(stream)) {
    if (first) {
      first = false;
      if (type !== 0x0809 || data.byteLength < 2 || new DataView(data.buffer, data.byteOffset).getUint16(0, true) !== 0x0600) {
        throw new Error("Xls error: only BIFF8 workbooks (Excel 97-2003) are supported");
      }
      continue;
    }
    if (type === 0x002f) throw new Error("Xls error: workbook is password-protected");
    if (type === 0x000a) break;
    if (type === 0x003c && inSst) { sstParts.push(data); continue; }
    inSst = false;
    if (type === 0x00fc) { sstParts.push(data); inSst = true; }
    else if (type === 0x0085) {
      const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
      const position = view.getUint32(0, true);
      const count = data[6]!;
      const high = (data[7]! & 1) === 1;
      let name = "";
      for (let i = 0; i < count; i++) name += String.fromCharCode(high ? view.getUint16(8 + i * 2, true) : data[8 + i]!);
      sheets.push({ name, position });
    }
  }
  const shared: string[] = [];
  if (sstParts.length > 0) {
    const reader = new Segments(sstParts);
    reader.u32();
    const unique = reader.u32();
    for (let i = 0; i < unique; i++) shared.push(reader.richString());
  }
  return sheets.map((info, index): WorkbookSheet => {
    let cells: Cells | null = null;
    const load = () => (cells ??= sheetCells(stream, info.position, shared));
    return {
      name: info.name,
      index,
      bounds(): SheetGrid | null {
        let grid: SheetGrid | null = null;
        for (const [row, line] of load()) {
          for (const col of line.keys()) {
            if (grid === null) grid = { minRow: row, maxRow: row, minCol: col, maxCol: col };
            else {
              grid.minRow = Math.min(grid.minRow, row);
              grid.maxRow = Math.max(grid.maxRow, row);
              grid.minCol = Math.min(grid.minCol, col);
              grid.maxCol = Math.max(grid.maxCol, col);
            }
          }
        }
        return grid;
      },
      rows(grid: SheetGrid, onRow: (row: (string | null)[]) => void): void {
        const all = load();
        for (let row = grid.minRow; row <= grid.maxRow; row++) {
          const line = all.get(row);
          const values = new Array<string | null>(grid.maxCol - grid.minCol + 1).fill(null);
          if (line) for (const [col, value] of line) if (col >= grid.minCol && col <= grid.maxCol) values[col - grid.minCol] = value;
          onRow(values);
        }
        cells = null;
      },
    };
  });
}
