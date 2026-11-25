// WLP1 PackedPage adaptive encoder + direct Durable Object SqlStorageCursor.raw() encoder.
// Hot-zone DOD compliant: uses indexed loops, avoids per-row JS object allocation, and selects
// the smallest of 20 columnar encodings per column per page.

import { setBit, words } from "./bitmap";
import { fingerprint } from "./page";
import {
  E_B1, E_B2, E_B4, E_BOOL, E_CF64, E_CI32, E_D16, E_D32, E_D8, E_DICT, E_F64, E_I32,
  E_LIST8, E_NULL, E_SEQ32, E_U16, E_U8, E_UTF8,
  PAGE_DESC, PAGE_HEADER, PAGE_MAGIC, PAGE_ROWS_MAX,
  type PageCol,
} from "./wire";

const te = new TextEncoder();
const align8 = (n: number): number => (n + 7) & ~7;
const isI32 = (v: unknown): v is number =>
  typeof v === "number" && Number.isInteger(v) && v >= -2147483648 && v <= 2147483647;

interface ColumnOut {
  enc: number;
  valid: Uint8Array | null;
  data: Uint8Array | null;
  offsets: Uint8Array | null;
  strings: Uint8Array | null;
}

function badValue(col: number, kind: string, value: unknown): never {
  throw new TypeError(`page column ${col} (${kind}): cannot encode ${String(value)}`);
}

/**
 * Encodes one WLP1 PackedPage from row tuples `[rid, ver, col0, col1, ...]`.
 * `null` and `undefined` encode as null; columns 0 (`rid`) and 1 (`ver`) must be non-null i32.
 */
export function encodePage(
  tableId: number,
  cols: readonly PageCol[],
  rows: readonly (readonly unknown[])[],
): Uint8Array {
  const n = rows.length;
  const ncols = cols.length + 2;
  if (n > PAGE_ROWS_MAX) throw new RangeError(`page rows ${n} > ${PAGE_ROWS_MAX}`);

  const outs = new Array<ColumnOut>(ncols);
  const colIds = new Uint32Array(cols.length);
  let kinds = "";
  for (let i = 0; i < cols.length; i++) {
    colIds[i] = cols[i]!.id;
    kinds += cols[i]!.k;
  }

  for (let c = 0; c < ncols; c++) {
    const k = c < 2 ? "i" : cols[c - 2]!.k;
    const vals = new Array<unknown>(n);
    let presentCount = 0;
    for (let r = 0; r < n; r++) {
      const v = rows[r]![c];
      if (v === undefined || v === null) {
        vals[r] = null;
      } else {
        vals[r] = v;
        presentCount++;
      }
    }
    if (c < 2 && presentCount < n) {
      throw new TypeError(`page column ${c} (rid/ver) cannot be null`);
    }

    const o: ColumnOut = { enc: E_NULL, valid: null, data: null, offsets: null, strings: null };
    outs[c] = o;
    if (presentCount === 0) continue;

    if (presentCount < n) {
      const bitmap = new Uint32Array(words(n));
      for (let r = 0; r < n; r++) {
        if (vals[r] !== null) setBit(bitmap, r);
      }
      o.valid = new Uint8Array(bitmap.buffer, bitmap.byteOffset, bitmap.byteLength);
    }

    if (k === "r" || k === "i") {
      o.data = encodeInts(o, vals, presentCount, c, k);
    } else if (k === "f" || k === "t") {
      o.data = encodeFloats(o, vals, presentCount, c, k);
    } else if (k === "b") {
      o.enc = E_BOOL;
      const a = new Uint8Array((n + 7) >> 3);
      for (let r = 0; r < n; r++) {
        const v = vals[r];
        if (v === true || v === 1) a[r >> 3] = a[r >> 3]! | (1 << (r & 7));
        else if (v !== null && v !== false && v !== 0) badValue(c, k, v);
      }
      o.data = a;
    } else if (k === "l") {
      encodeLists(o, vals, c, k);
    } else if (k === "d" || k === "x" || k === "j") {
      encodeStrings(o, vals, c, k);
    } else {
      throw new TypeError(`page column ${c}: unknown kind ${String(k)}`);
    }
  }

  const bodyStart = align8(PAGE_HEADER + ncols * PAGE_DESC);
  const partsOff: number[] = [];
  const partsBuf: Uint8Array[] = [];
  let size = 0;

  const desc = new Int32Array(ncols * 4);
  for (let c = 0; c < ncols; c++) {
    const o = outs[c]!;
    const dBase = c * 4;
    const lanes = [o.valid, o.data, o.offsets, o.strings];
    for (let slot = 0; slot < 4; slot++) {
      const l = lanes[slot]!;
      if (l === null) {
        desc[dBase + slot] = -1;
      } else {
        const at = align8(size);
        partsOff.push(at);
        partsBuf.push(l);
        size = at + l.length;
        desc[dBase + slot] = at;
      }
    }
  }

  const out = new Uint8Array(bodyStart + size);
  const dv = new DataView(out.buffer, out.byteOffset, out.byteLength);
  dv.setUint32(0, PAGE_MAGIC, true);
  dv.setUint16(4, n, true);
  dv.setUint16(6, ncols, true);
  dv.setUint32(8, bodyStart, true);
  dv.setUint32(12, fingerprint(tableId, colIds, kinds), true);

  for (let c = 0; c < ncols; c++) {
    const d = PAGE_HEADER + c * PAGE_DESC;
    const dBase = c * 4;
    dv.setUint8(d, outs[c]!.enc);
    dv.setInt32(d + 1, desc[dBase]!, true);
    dv.setInt32(d + 5, desc[dBase + 1]!, true);
    dv.setInt32(d + 9, desc[dBase + 2]!, true);
    dv.setInt32(d + 13, desc[dBase + 3]!, true);
  }
  for (let i = 0; i < partsOff.length; i++) {
    out.set(partsBuf[i]!, bodyStart + partsOff[i]!);
  }
  return out;
}

function encodeInts(o: ColumnOut, vals: readonly unknown[], presentCount: number, col: number, kind: string): Uint8Array {
  const n = vals.length;
  const p = new Int32Array(presentCount);
  const pos = new Uint16Array(presentCount);
  let pi = 0;
  let min = 2147483647;
  let max = -2147483648;
  for (let r = 0; r < n; r++) {
    const v = vals[r];
    if (v === null) continue;
    if (!isI32(v)) badValue(col, kind, v);
    p[pi] = v;
    pos[pi] = r;
    pi++;
    if (v < min) min = v;
    if (v > max) max = v;
  }

  if (min === max) {
    o.enc = E_CI32;
    const buf = new Int32Array(1);
    buf[0] = min;
    return new Uint8Array(buf.buffer);
  }

  let enc: number;
  let cost: number;
  if (min < 0) {
    enc = E_I32;
    cost = n * 4;
  } else if (max <= 1) {
    enc = E_B1;
    cost = (n + 7) >> 3;
  } else if (max <= 3) {
    enc = E_B2;
    cost = (n + 3) >> 2;
  } else if (max <= 15) {
    enc = E_B4;
    cost = (n + 1) >> 1;
  } else if (max <= 255) {
    enc = E_U8;
    cost = n;
  } else if (max <= 65535) {
    enc = E_U16;
    cost = n * 2;
  } else {
    enc = E_I32;
    cost = n * 4;
  }

  if (cost > 8 && presentCount >= 2) {
    const i0 = pos[0]!;
    const i1 = pos[1]!;
    const d = (p[1]! - p[0]!) / (i1 - i0);
    const s = p[0]! - i0 * d;
    if (isI32(d) && isI32(s)) {
      let affine = true;
      for (let j = 0; j < presentCount; j++) {
        if (p[j]! !== s + pos[j]! * d) {
          affine = false;
          break;
        }
      }
      if (affine) {
        o.enc = E_SEQ32;
        const seq = new Int32Array(2);
        seq[0] = s;
        seq[1] = d;
        return new Uint8Array(seq.buffer);
      }
    }
  }

  o.enc = enc;
  if (enc === E_I32) {
    const a = new Int32Array(n);
    for (let r = 0; r < n; r++) a[r] = vals[r] === null ? 0 : (vals[r] as number);
    return new Uint8Array(a.buffer);
  }
  if (enc === E_U8) {
    const a = new Uint8Array(n);
    for (let r = 0; r < n; r++) a[r] = vals[r] === null ? 0 : (vals[r] as number);
    return a;
  }
  if (enc === E_U16) {
    const a = new Uint16Array(n);
    for (let r = 0; r < n; r++) a[r] = vals[r] === null ? 0 : (vals[r] as number);
    return new Uint8Array(a.buffer);
  }
  const b = enc === E_B1 ? 1 : enc === E_B2 ? 2 : 4;
  const a = new Uint8Array(cost);
  for (let r = 0; r < n; r++) {
    const v = vals[r] === null ? 0 : (vals[r] as number);
    const bitIdx = r * b;
    a[bitIdx >> 3] = a[bitIdx >> 3]! | (v << (bitIdx & 7));
  }
  return a;
}

function encodeFloats(o: ColumnOut, vals: readonly unknown[], presentCount: number, col: number, kind: string): Uint8Array {
  const n = vals.length;
  const p = new Float64Array(presentCount);
  let pi = 0;
  let min = Infinity;
  let max = -Infinity;
  let first = 0;
  let allSame = true;
  let allSafeInt = true;

  for (let r = 0; r < n; r++) {
    const v = vals[r];
    if (v === null) continue;
    if (typeof v !== "number" || !Number.isFinite(v)) badValue(col, kind, v);
    if (pi === 0) first = v;
    else if (!Object.is(v, first)) allSame = false;
    if (!Number.isSafeInteger(v) || Object.is(v, -0)) allSafeInt = false;
    p[pi++] = v;
    if (v < min) min = v;
    if (v > max) max = v;
  }

  if (allSame) {
    o.enc = E_CF64;
    const buf = new Float64Array(1);
    buf[0] = first;
    return new Uint8Array(buf.buffer);
  }

  const span = max - min;
  if (allSafeInt && span <= 0xffffffff) {
    const w = span <= 255 ? 0 : span <= 65535 ? 1 : 2;
    if (8 + (n << w) < n * 8) {
      o.enc = E_D8 + w;
      const out = new Uint8Array(8 + (n << w));
      new DataView(out.buffer).setFloat64(0, min, true);
      if (w === 0) {
        for (let r = 0; r < n; r++) out[8 + r] = vals[r] === null ? 0 : (vals[r] as number) - min;
      } else if (w === 1) {
        const d16 = new Uint16Array(out.buffer, 8, n);
        for (let r = 0; r < n; r++) d16[r] = vals[r] === null ? 0 : (vals[r] as number) - min;
      } else {
        const d32 = new Uint32Array(out.buffer, 8, n);
        for (let r = 0; r < n; r++) d32[r] = vals[r] === null ? 0 : (vals[r] as number) - min;
      }
      return out;
    }
  }

  o.enc = E_F64;
  const a = new Float64Array(n);
  for (let r = 0; r < n; r++) a[r] = vals[r] === null ? min : (vals[r] as number);
  return new Uint8Array(a.buffer);
}

function encodeLists(o: ColumnOut, vals: readonly unknown[], col: number, kind: string): void {
  const n = vals.length;
  const off = new Uint16Array(n + 1);
  const flat: number[] = [];
  let max = 0;
  for (let r = 0; r < n; r++) {
    const v = vals[r];
    if (v !== null) {
      if (!Array.isArray(v)) badValue(col, kind, v);
      for (let j = 0; j < v.length; j++) {
        const id = v[j];
        if (!isI32(id) || id < 0) badValue(col, kind, v);
        flat.push(id);
        if (id > max) max = id;
      }
    }
    if (flat.length > 65535) throw new RangeError("page list > 65535 ids");
    off[r + 1] = flat.length;
  }

  const w = max <= 255 ? 0 : max <= 65535 ? 1 : 2;
  o.enc = E_LIST8 + w;
  o.offsets = new Uint8Array(off.buffer);
  if (w === 0) {
    const a = new Uint8Array(flat.length);
    for (let i = 0; i < flat.length; i++) a[i] = flat[i]!;
    o.data = a;
  } else if (w === 1) {
    const a = new Uint16Array(flat.length);
    for (let i = 0; i < flat.length; i++) a[i] = flat[i]!;
    o.data = new Uint8Array(a.buffer);
  } else {
    const a = new Uint32Array(flat.length);
    for (let i = 0; i < flat.length; i++) a[i] = flat[i]!;
    o.data = new Uint8Array(a.buffer);
  }
}

function encodeStrings(o: ColumnOut, vals: readonly unknown[], col: number, kind: string): void {
  const n = vals.length;
  const at = new Map<string, number>();
  const uniqBytes: Uint8Array[] = [];
  const rowCodes = new Uint32Array(n);
  let uniqTotalBytes = 0;
  let directTotalBytes = 0;

  for (let r = 0; r < n; r++) {
    const v = vals[r];
    let s: string;
    if (v === null) s = "";
    else if (typeof v === "string") s = v;
    else if (kind === "j") s = JSON.stringify(v);
    else badValue(col, kind, v);

    let code = at.get(s);
    if (code === undefined) {
      code = uniqBytes.length;
      at.set(s, code);
      const b = te.encode(s);
      uniqBytes.push(b);
      uniqTotalBytes += b.length;
    }
    rowCodes[r] = code;
    directTotalBytes += uniqBytes[code]!.length;
  }

  const uniqCount = uniqBytes.length;
  const codeWidth = uniqCount > 256 ? 2 : 1;
  const dictCost = (uniqCount + 1) * 4 + uniqTotalBytes + n * codeWidth;
  const utf8Cost = (n + 1) * 4 + directTotalBytes;
  const useDict = dictCost < utf8Cost;

  if (useDict) {
    o.enc = E_DICT;
    const offs = new Uint32Array(uniqCount + 2);
    offs[0] = uniqCount;
    let cursor = 0;
    for (let i = 0; i < uniqCount; i++) {
      offs[i + 1] = cursor;
      cursor += uniqBytes[i]!.length;
    }
    offs[uniqCount + 1] = cursor;
    const sb = new Uint8Array(cursor);
    for (let i = 0; i < uniqCount; i++) sb.set(uniqBytes[i]!, offs[i + 1]!);
    o.offsets = new Uint8Array(offs.buffer);
    o.strings = sb;
    if (codeWidth === 2) {
      const codes = new Uint16Array(n);
      for (let r = 0; r < n; r++) codes[r] = rowCodes[r]!;
      o.data = new Uint8Array(codes.buffer);
    } else {
      const codes = new Uint8Array(n);
      for (let r = 0; r < n; r++) codes[r] = rowCodes[r]!;
      o.data = codes;
    }
  } else {
    o.enc = E_UTF8;
    const offs = new Uint32Array(n + 2);
    offs[0] = n;
    let cursor = 0;
    for (let r = 0; r < n; r++) {
      offs[r + 1] = cursor;
      cursor += uniqBytes[rowCodes[r]!]!.length;
    }
    offs[n + 1] = cursor;
    const sb = new Uint8Array(cursor);
    for (let r = 0; r < n; r++) sb.set(uniqBytes[rowCodes[r]!]!, offs[r + 1]!);
    o.offsets = new Uint8Array(offs.buffer);
    o.strings = sb;
  }
}

/** Minimal cursor shape satisfied by Durable Object `SqlStorageCursor`. */
export interface RawSqlCursor {
  raw<R extends SqlStorageValue[] = SqlStorageValue[]>(): IterableIterator<R>;
  readonly rowsRead: number;
}

export interface EncodedCursorResult {
  readonly pages: Uint8Array[];
  readonly totalRows: number;
  readonly rowsRead: number;
}

/**
 * Streams a Durable Object `SqlStorageCursor` via `.raw()` directly into `WLP1` PackedPages
 * of at most `pageRows` (<= 1024) rows without allocating per-row JS objects or JSON text.
 */
export function encodeSqlCursor(
  tableId: number,
  cols: readonly PageCol[],
  cursor: RawSqlCursor,
  pageRows = PAGE_ROWS_MAX,
): EncodedCursorResult {
  if (pageRows < 1 || pageRows > PAGE_ROWS_MAX) {
    throw new RangeError(`pageRows must be in 1..${PAGE_ROWS_MAX}`);
  }
  const pages: Uint8Array[] = [];
  const batch: (readonly SqlStorageValue[])[] = [];
  let totalRows = 0;
  const iter = cursor.raw();
  for (;;) {
    const next = iter.next();
    if (next.done) break;
    batch.push(next.value);
    totalRows++;
    if (batch.length === pageRows) {
      pages.push(encodePage(tableId, cols, batch));
      batch.length = 0;
    }
  }
  if (batch.length > 0) {
    pages.push(encodePage(tableId, cols, batch));
    batch.length = 0;
  }
  return { pages, totalRows, rowsRead: cursor.rowsRead };
}
