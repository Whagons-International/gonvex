// WLP1 binary frame envelope codec and streaming splitter (wire-compatible with whagons-lite shared/codec/frame*.ts).
// Frame layout (little-endian): u32 len (bytes of kind + payload) | u8 kind | payload[len - 1].
// Zero JSON parsing in hot binary lanes: PAGE, DELETE, MEMBERS, END, RESET, PING are pure binary.

import { setBit, words } from "./bitmap";
import { failFrame, readPage } from "./page";
import {
  F_COMMIT, F_DELETE, F_END, F_MEMBERS, F_PAGE, F_PING, F_RESET,
  FRAME_HEADER, FRAME_KINDS,
  type PageLayout, type PageReader,
} from "./wire";

/** Largest accepted frame length (256 MiB guard against malformed lengths). */
export const FRAME_MAX = 1 << 28;

export type BinaryFrameIn =
  | { readonly k: typeof F_RESET | typeof F_PING }
  | { readonly k: typeof F_PAGE; readonly t: number; readonly flags: number; readonly bytes: Uint8Array }
  | { readonly k: typeof F_DELETE; readonly t: number; readonly rids: Int32Array }
  | { readonly k: typeof F_COMMIT; readonly seq: number; readonly flags: number; readonly metaBytes: Uint8Array }
  | { readonly k: typeof F_END; readonly seq: number }
  | { readonly k: typeof F_MEMBERS; readonly t: number; readonly base: number; readonly n: number; readonly bits: Uint32Array };

export type BinaryFrame =
  | { readonly k: typeof F_RESET | typeof F_PING }
  | { readonly k: typeof F_PAGE; readonly t: number; readonly flags: number; readonly page: PageReader }
  | { readonly k: typeof F_DELETE; readonly t: number; readonly rids: Int32Array }
  | { readonly k: typeof F_COMMIT; readonly seq: number; readonly flags: number; readonly metaBytes: Uint8Array }
  | { readonly k: typeof F_END; readonly seq: number }
  | { readonly k: typeof F_MEMBERS; readonly t: number; readonly base: number; readonly n: number; readonly bits: Uint32Array };

function allocFrame(k: number, payloadLen: number): { u8: Uint8Array; dv: DataView } {
  const u8 = new Uint8Array(FRAME_HEADER + payloadLen);
  const dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
  dv.setUint32(0, payloadLen + 1, true);
  u8[4] = k;
  return { u8, dv };
}

/** Encodes one binary frame (`u32 len | u8 kind | payload`). */
export function encodeFrame(frame: BinaryFrameIn): Uint8Array {
  switch (frame.k) {
    case F_RESET:
    case F_PING:
      return allocFrame(frame.k, 0).u8;
    case F_PAGE: {
      const { u8, dv } = allocFrame(F_PAGE, 4 + frame.bytes.length);
      dv.setUint16(5, frame.t, true);
      dv.setUint16(7, frame.flags, true);
      u8.set(frame.bytes, 9);
      return u8;
    }
    case F_DELETE: {
      const count = frame.rids.length;
      const { u8, dv } = allocFrame(F_DELETE, 8 + count * 4);
      dv.setUint16(5, frame.t, true);
      dv.setUint16(7, 0, true);
      dv.setUint32(9, count, true);
      for (let i = 0; i < count; i++) dv.setInt32(13 + i * 4, frame.rids[i]!, true);
      return u8;
    }
    case F_COMMIT: {
      const mlen = frame.metaBytes.length;
      const { u8, dv } = allocFrame(F_COMMIT, 13 + mlen);
      dv.setFloat64(5, frame.seq, true);
      u8[13] = frame.flags;
      dv.setUint32(14, mlen, true);
      u8.set(frame.metaBytes, 18);
      return u8;
    }
    case F_END: {
      const { u8, dv } = allocFrame(F_END, 8);
      dv.setFloat64(5, frame.seq, true);
      return u8;
    }
    case F_MEMBERS: {
      const w = words(frame.n);
      if (frame.bits.length < w) throw new RangeError("MEMBERS bitmap shorter than n bits");
      const { u8, dv } = allocFrame(F_MEMBERS, 12 + w * 4);
      dv.setUint16(5, frame.t, true);
      dv.setUint16(7, 0, true);
      dv.setInt32(9, frame.base, true);
      dv.setUint32(13, frame.n, true);
      for (let i = 0; i < w; i++) dv.setUint32(17 + i * 4, frame.bits[i]!, true);
      return u8;
    }
  }
}

/** Concatenates encoded frames into one contiguous binary WebSocket or HTTP body. */
export function concatFrames(parts: readonly Uint8Array[]): Uint8Array {
  let total = 0;
  for (let i = 0; i < parts.length; i++) total += parts[i]!.length;
  const out = new Uint8Array(total);
  let off = 0;
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i]!;
    out.set(p, off);
    off += p.length;
  }
  return out;
}

/** Builds a compact `F_MEMBERS` bitmap frame covering `rids`. */
export function buildMembersFrame(tableId: number, rids: readonly number[]): Extract<BinaryFrameIn, { k: typeof F_MEMBERS }> {
  if (rids.length === 0) {
    return { k: F_MEMBERS, t: tableId, base: 0, n: 0, bits: new Uint32Array(0) };
  }
  let min = rids[0]!;
  let max = rids[0]!;
  for (let i = 1; i < rids.length; i++) {
    const r = rids[i]!;
    if (r < min) min = r;
    if (r > max) max = r;
  }
  const n = max - min + 1;
  const bits = new Uint32Array(words(n));
  for (let i = 0; i < rids.length; i++) setBit(bits, rids[i]! - min);
  return { k: F_MEMBERS, t: tableId, base: min, n, bits };
}

function scanFrames(buf: Uint8Array, out: Uint8Array[]): Uint8Array {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let i = 0;
  const len = buf.length;
  while (len - i >= FRAME_HEADER) {
    const n = dv.getUint32(i, true);
    const k = buf[i + 4]!;
    if (n < 1 || n > FRAME_MAX || k < 1 || k > FRAME_KINDS) failFrame();
    if (len - i - 4 < n) break;
    const next = i + 4 + n;
    out.push(buf.subarray(i, next));
    i = next;
  }
  return buf.subarray(i);
}

/** Splits a complete binary message into raw frame slices; rejects trailing partial frames. */
export function splitFrames(buf: Uint8Array): Uint8Array[] {
  const out: Uint8Array[] = [];
  if (scanFrames(buf, out).length !== 0) failFrame();
  return out;
}

/** Incremental streaming splitter for chunked HTTP/R2 or WS streams. */
export function createFrameSplitter(): { push(chunk: Uint8Array): Uint8Array[]; readonly bufferedBytes: number } {
  let rest = new Uint8Array(0);
  return {
    push(chunk: Uint8Array): Uint8Array[] {
      let combined: Uint8Array;
      if (rest.length > 0) {
        combined = new Uint8Array(rest.length + chunk.length);
        combined.set(rest, 0);
        combined.set(chunk, rest.length);
      } else {
        combined = chunk;
      }
      const out: Uint8Array[] = [];
      rest = scanFrames(combined, out).slice();
      return out;
    },
    get bufferedBytes(): number {
      return rest.length;
    },
  };
}

/** Decodes and strictly validates one complete binary frame. */
export function decodeFrame(frame: Uint8Array, layoutFor: (tableId: number) => PageLayout): BinaryFrame {
  const n = frame.length;
  if (n < FRAME_HEADER) failFrame();
  const dv = new DataView(frame.buffer, frame.byteOffset, n);
  if (dv.getUint32(0, true) !== n - 4) failFrame();
  const k = frame[4]!;

  if (k === F_RESET || k === F_PING) {
    if (n !== FRAME_HEADER) failFrame();
    return { k };
  }
  if (k === F_PAGE) {
    if (n < 9) failFrame();
    const t = dv.getUint16(5, true);
    const flags = dv.getUint16(7, true);
    return { k, t, flags, page: readPage(t, flags, frame.subarray(9), layoutFor(t)) };
  }
  if (k === F_DELETE) {
    if (n < 13) failFrame();
    const count = dv.getUint32(9, true);
    if (n !== 13 + count * 4) failFrame();
    const rids = new Int32Array(count);
    for (let i = 0; i < count; i++) rids[i] = dv.getInt32(13 + i * 4, true);
    return { k, t: dv.getUint16(5, true), rids };
  }
  if (k === F_COMMIT) {
    if (n < 18) failFrame();
    const mlen = dv.getUint32(14, true);
    if (n !== 18 + mlen) failFrame();
    return { k, seq: dv.getFloat64(5, true), flags: frame[13]!, metaBytes: frame.slice(18) };
  }
  if (k === F_END) {
    if (n !== 13) failFrame();
    return { k, seq: dv.getFloat64(5, true) };
  }
  if (k === F_MEMBERS) {
    if (n < 17) failFrame();
    const bn = dv.getUint32(13, true);
    const w = words(bn);
    if (n !== 17 + w * 4) failFrame();
    const bits = new Uint32Array(w);
    for (let i = 0; i < w; i++) bits[i] = dv.getUint32(17 + i * 4, true);
    return { k, t: dv.getUint16(5, true), base: dv.getInt32(9, true), n: bn, bits };
  }
  return failFrame();
}

/** Packages a visibility class's pages + deletes + commit sequence into one binary WS message. */
export function encodeClassDelivery(
  seq: number,
  tables: readonly { readonly tbl: number; readonly upsertPages: readonly Uint8Array[]; readonly deletedRids: Int32Array }[],
  flags = 0,
): Uint8Array {
  const parts: Uint8Array[] = [];
  for (let i = 0; i < tables.length; i++) {
    const entry = tables[i]!;
    for (let p = 0; p < entry.upsertPages.length; p++) {
      parts.push(encodeFrame({ k: F_PAGE, t: entry.tbl, flags: 0, bytes: entry.upsertPages[p]! }));
    }
    if (entry.deletedRids.length > 0) {
      parts.push(encodeFrame({ k: F_DELETE, t: entry.tbl, rids: entry.deletedRids }));
    }
  }
  parts.push(encodeFrame({ k: F_COMMIT, seq, flags, metaBytes: new Uint8Array(0) }));
  return concatFrames(parts);
}
