// WLP1 binary columnar PackedPage wire constants and types (wire-compatible with whagons-lite shared/wire.ts).
//
// PackedPage layout (little-endian):
//   Header (16 B): u32 magic ('WLP1') | u16 rows | u16 ncols | u32 bodyStart | u32 fingerprint
//   Descriptors:   ncols * 17 B: u8 enc | i32 validOff | i32 dataOff | i32 offsetsOff | i32 stringsOff (-1 = absent)
//   Body:          8-byte aligned lanes.
//
// Column 0 is `rid` (i32 family), column 1 is `ver` (i32 family), and columns 2.. are schema columns in column-id order.

export const PROTOCOL_VERSION = 1;

export const PAGE_MAGIC = 0x31504c57; // 'WLP1'
export const PAGE_HEADER = 16;
export const PAGE_DESC = 17;
export const PAGE_ALIGN = 8;
export const PAGE_ROWS_MAX = 1024;

export const FRAME_HEADER = 5;
export const F_HELLO = 1;
export const F_RESET = 2;
export const F_PAGE = 3;
export const F_DELETE = 4;
export const F_COMMIT = 5;
export const F_RESCOPE = 6;
export const F_PING = 7;
export const F_META = 8;
export const F_REJECT = 9;
export const F_WINDOW = 10;
export const F_END = 11;
export const F_ERROR = 12;
export const F_ACK = 13;
export const F_MEMBERS = 14;
export const FRAME_KINDS = 14;
export const FRAMES_MIME = "application/x-wl-frames";

export const PAGE_DETAIL = 1;
export const COMMIT_PARTIAL = 1;

/** The 20 adaptive column encodings; descriptor `enc` byte is the index here. */
export const ENCODINGS = [
  "null", "i32", "f64", "bool", "utf8", "dict", "u8", "u16", "d8", "d16", "d32",
  "ci32", "cf64", "b1", "b2", "b4", "seq32", "list8", "list16", "list32",
] as const;

export type Encoding = (typeof ENCODINGS)[number];

export const E_NULL = 0;
export const E_I32 = 1;
export const E_F64 = 2;
export const E_BOOL = 3;
export const E_UTF8 = 4;
export const E_DICT = 5;
export const E_U8 = 6;
export const E_U16 = 7;
export const E_D8 = 8;
export const E_D16 = 9;
export const E_D32 = 10;
export const E_CI32 = 11;
export const E_CF64 = 12;
export const E_B1 = 13;
export const E_B2 = 14;
export const E_B4 = 15;
export const E_SEQ32 = 16;
export const E_LIST8 = 17;
export const E_LIST16 = 18;
export const E_LIST32 = 19;

/** Column kind letters matching whagons-lite's schema registry. */
export type ColumnKind = "r" | "i" | "f" | "t" | "b" | "d" | "x" | "j" | "l";

/** One schema column descriptor (page columns 2.. after rid and ver). */
export interface PageCol {
  readonly id: number;
  readonly k: ColumnKind;
}

/** Expected layout of a table's PackedPage columns 2.. and its FNV-1a schema fingerprint. */
export interface PageLayout {
  readonly ids: ArrayLike<number>;
  readonly kinds: string;
  readonly fp: number;
}

/** One decoded columnar lane, ready for zero-boxing bulk consumption. */
export interface PageLane {
  readonly enc: number;
  /** Validity bitmap, 1 bit per row, LSB-first; null when every row is valid (or all null for E_NULL). */
  readonly valid: Uint32Array | null;
  /** Numeric value of row `r` for numeric, bool (0/1), seq, and constant encodings. */
  num(r: number): number;
  /** utf8 and dict: page-local string table (dict: dictionary; utf8: one entry per row). */
  readonly strs?: readonly string[];
  /** dict: dictionary index of row `r` (utf8: `r` itself). */
  idx?(r: number): number;
  /** list encodings: CSR offsets (rows + 1) and flat id array. */
  readonly off?: Uint16Array | Uint32Array;
  readonly ids?: Uint8Array | Uint16Array | Uint32Array;
  /** Direct typed view when encoding is plain (i32, f64, u8, u16). */
  readonly raw?: Int32Array | Float64Array | Uint8Array | Uint16Array;
}

/** Strict reader over a validated WLP1 PackedPage. */
export interface PageReader {
  readonly table: number;
  readonly flags: number;
  readonly rows: number;
  readonly ncols: number;
  readonly fingerprint: number;
  /** Schema column id of page column `c` (`c >= 2`); -1 for rid (0) and ver (1). */
  colId(c: number): number;
  lane(c: number): PageLane;
  /** Boxed read for tests and interop; returns null when invalid. */
  get(c: number, row: number): unknown;
}
