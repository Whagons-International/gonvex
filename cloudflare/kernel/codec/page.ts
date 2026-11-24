// WLP1 PackedPage reader and strict validator (wire-compatible with whagons-lite shared/codec/page.ts).
// Zero allocations per row on typed numeric lanes; strictly validates alignment, bounds, monotonicity,
// UTF-8 validity, dictionary code ranges, schema fingerprint, and exact payload length.

import { bit, words } from "./bitmap";
import {
  E_B1, E_B2, E_B4, E_BOOL, E_CF64, E_CI32, E_D16, E_D32, E_D8, E_DICT, E_F64, E_I32,
  E_LIST16, E_LIST32, E_LIST8, E_NULL, E_SEQ32, E_U16, E_U8, E_UTF8,
  PAGE_DESC, PAGE_HEADER, PAGE_MAGIC, PAGE_ROWS_MAX,
  type PageCol, type PageLane, type PageLayout, type PageReader,
} from "./wire";

const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });

export const failFrame = (code = "sync.frame"): never => {
  throw new Error(code);
};

export const fnv1a = (s: string): number => {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 0x1000193);
  return h >>> 0;
};

/** FNV-1a schema fingerprint over `<tableId>:<colId>:<kind>,` for page columns 2.. */
export function fingerprint(tableId: number, ids: ArrayLike<number>, kinds: string): number {
  let s = "";
  for (let i = 0; i < kinds.length; i++) s += `${tableId}:${ids[i]}:${kinds.charAt(i)},`;
  return fnv1a(s);
}

/** Builds a PageLayout from an explicit column list. */
export function layoutFromCols(tableId: number, cols: readonly PageCol[]): PageLayout {
  const n = cols.length;
  const ids = new Uint32Array(n);
  let kinds = "";
  for (let i = 0; i < n; i++) {
    const col = cols[i]!;
    ids[i] = col.id;
    kinds += col.k;
  }
  return { ids, kinds, fp: fingerprint(tableId, ids, kinds) };
}

type TypedCtor<A> = new (b: ArrayBufferLike, o: number, n: number) => A;

/** Reads and strictly validates a WLP1 PackedPage. */
export function readPage(table: number, flags: number, input: Uint8Array, layout: PageLayout): PageReader {
  // Typed lane views require 8-byte alignment.
  const u8 = (input.byteOffset & 7) !== 0 ? input.slice() : input;
  const buf = u8.buffer;
  const base = u8.byteOffset;
  const len = u8.length;
  if (len < PAGE_HEADER) failFrame();
  const dv = new DataView(buf, base, len);
  if (dv.getUint32(0, true) !== PAGE_MAGIC) failFrame();
  const rows = dv.getUint16(4, true);
  const ncols = dv.getUint16(6, true);
  const body = dv.getUint32(8, true);
  const fp = dv.getUint32(12, true);
  if (fp !== layout.fp) failFrame("sync.schema");

  let end = body;
  const view = <A>(C: TypedCtor<A>, at: number, n: number, w: number): A => {
    if (at < body || at + n * w > len) throw 0;
    const laneEnd = at + n * w;
    if (laneEnd > end) end = laneEnd;
    return new C(buf, base + at, n);
  };
  const checkMono = (o: ArrayLike<number>, n: number): void => {
    for (let i = 0; i < n; i++) {
      if ((o[i] ?? 0) > (o[i + 1] ?? 0)) throw 0;
    }
  };

  const lanes: PageLane[] = new Array<PageLane>(ncols);
  try {
    if (rows > PAGE_ROWS_MAX || ncols !== layout.kinds.length + 2 || body < PAGE_HEADER + ncols * PAGE_DESC) throw 0;
    for (let c = 0; c < ncols; c++) {
      const d = PAGE_HEADER + c * PAGE_DESC;
      const enc = dv.getUint8(d);
      const off0 = dv.getInt32(d + 1, true);
      const off1 = dv.getInt32(d + 5, true);
      const off2 = dv.getInt32(d + 9, true);
      const off3 = dv.getInt32(d + 13, true);
      const vo = off0 < 0 ? -1 : off0 + body;
      const data = off1 < 0 ? -1 : off1 + body;
      const offs = off2 < 0 ? -1 : off2 + body;
      const strs = off3 < 0 ? -1 : off3 + body;
      const valid = vo < 0 ? null : view(Uint32Array, vo, words(rows), 4);

      if (enc === E_NULL) {
        lanes[c] = { enc, valid, num: () => NaN };
      } else if (enc === E_I32) {
        const raw = view(Int32Array, data, rows, 4);
        lanes[c] = { enc, valid, raw, num: (r) => raw[r]! };
      } else if (enc === E_F64) {
        const raw = view(Float64Array, data, rows, 8);
        lanes[c] = { enc, valid, raw, num: (r) => raw[r]! };
      } else if (enc === E_U8) {
        const raw = view(Uint8Array, data, rows, 1);
        lanes[c] = { enc, valid, raw, num: (r) => raw[r]! };
      } else if (enc === E_U16) {
        const raw = view(Uint16Array, data, rows, 2);
        lanes[c] = { enc, valid, raw, num: (r) => raw[r]! };
      } else if (enc === E_BOOL || enc === E_B1 || enc === E_B2 || enc === E_B4) {
        const b = enc === E_B2 ? 2 : enc === E_B4 ? 4 : 1;
        const mask = (1 << b) - 1;
        const a = view(Uint8Array, data, (rows * b + 7) >> 3, 1);
        lanes[c] = { enc, valid, num: (r) => (a[(r * b) >> 3]! >> ((r * b) & 7)) & mask };
      } else if (enc === E_D8 || enc === E_D16 || enc === E_D32) {
        const b0 = view(Float64Array, data, 1, 8)[0]!;
        if (enc === E_D8) {
          const a = view(Uint8Array, data + 8, rows, 1);
          lanes[c] = { enc, valid, num: (r) => b0 + a[r]! };
        } else if (enc === E_D16) {
          const a = view(Uint16Array, data + 8, rows, 2);
          lanes[c] = { enc, valid, num: (r) => b0 + a[r]! };
        } else {
          const a = view(Uint32Array, data + 8, rows, 4);
          lanes[c] = { enc, valid, num: (r) => b0 + a[r]! };
        }
      } else if (enc === E_CI32) {
        const v = view(Int32Array, data, 1, 4)[0]!;
        lanes[c] = { enc, valid, num: () => v };
      } else if (enc === E_CF64) {
        const v = view(Float64Array, data, 1, 8)[0]!;
        lanes[c] = { enc, valid, num: () => v };
      } else if (enc === E_SEQ32) {
        const a = view(Int32Array, data, 2, 4);
        const start = a[0]!;
        const step = a[1]!;
        lanes[c] = { enc, valid, num: (r) => start + r * step };
      } else if (enc === E_UTF8 || enc === E_DICT) {
        const n = view(Uint32Array, offs, 1, 4)[0]!;
        const o = view(Uint32Array, offs + 4, n + 1, 4);
        const sb = view(Uint8Array, strs, o[n]!, 1);
        checkMono(o, n);
        const decoded = new Array<string>(n);
        for (let i = 0; i < n; i++) decoded[i] = utf8.decode(sb.subarray(o[i]!, o[i + 1]!));
        let idx: (r: number) => number = (r) => r;
        if (enc === E_UTF8) {
          if (n !== rows) throw 0;
        } else {
          const k = n > 256 ? view(Uint16Array, data, rows, 2) : view(Uint8Array, data, rows, 1);
          for (let r = 0; r < rows; r++) if (k[r]! >= n) throw 0;
          idx = (r) => k[r]!;
        }
        lanes[c] = { enc, valid, strs: decoded, idx, num: idx };
      } else if (enc === E_LIST8 || enc === E_LIST16 || enc === E_LIST32) {
        const o = view(Uint16Array, offs, rows + 1, 2);
        const m = o[rows]!;
        checkMono(o, rows);
        const ids = enc === E_LIST8
          ? view(Uint8Array, data, m, 1)
          : enc === E_LIST16
            ? view(Uint16Array, data, m, 2)
            : view(Uint32Array, data, m, 4);
        lanes[c] = { enc, valid, off: o, ids, num: (r) => o[r + 1]! - o[r]! };
      } else {
        throw 0;
      }
    }
  } catch {
    failFrame();
  }
  if (end !== len) failFrame();

  const lane = (c: number): PageLane => lanes[c] ?? failFrame();
  return {
    table,
    flags,
    rows,
    ncols,
    fingerprint: fp,
    lane,
    colId: (c) => (c < 2 ? -1 : (layout.ids[c - 2] ?? -1)),
    get(c: number, r: number): unknown {
      const l = lane(c);
      if (!(r >= 0 && r < rows)) failFrame();
      if (l.enc === E_NULL || (l.valid !== null && bit(l.valid, r) === 0)) return null;
      if (l.strs !== undefined) return l.strs[l.idx!(r)];
      if (l.ids !== undefined && l.off !== undefined) {
        const start = l.off[r]!;
        const stop = l.off[r + 1]!;
        const out = new Array<number>(stop - start);
        for (let i = 0; i < out.length; i++) out[i] = l.ids[start + i]!;
        return out;
      }
      return l.enc === E_BOOL ? l.num(r) === 1 : l.num(r);
    },
  };
}
