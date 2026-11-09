/** Experimental in-memory/worker projection. This is not a new wire protocol,
 * persisted cache, query engine, or replacement for normalized replica rows. */
export type ColumnKind = 'boolean' | 'u32' | 'i32' | 'f64' | 'i64' | 'string';
export type ColumnProjection = Readonly<Record<string, { readonly kind: ColumnKind; readonly nullable?: boolean }>>;
export type ColumnPageLimits = { readonly maxRows: number; readonly maxPayloadBytes: number };
type NumericLane = Uint8Array | Uint16Array | Uint32Array | Int32Array | Float64Array | BigInt64Array;
export type PackedColumn = {
  kind: ColumnKind;
  /** Missing is different from an explicitly projected NULL. */
  present: Uint8Array;
  valid: Uint8Array;
  values: NumericLane;
  dictionary?: { offsets: Uint32Array; utf8: Uint8Array };
};
export type ColumnPage = {
  version: 1;
  ids: string[];
  rowCount: number;
  rowsPresent: Uint8Array;
  columns: Record<string, PackedColumn>;
  /** All requested rows and fields were resident; not collection/query coverage. */
  completeProjection: boolean;
  /** Owned typed buffers plus UTF-8 identities/column names; excludes JS overhead. */
  payloadBytes: number;
};
export type ColumnValue = string | number | bigint | boolean | null | undefined;

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });
const bit = (array: Uint8Array, index: number) => Boolean(array[index >>> 3]! & (1 << (index & 7)));
const mark = (array: Uint8Array, index: number) => { array[index >>> 3]! |= 1 << (index & 7); };
const own = (value: object, name: string) => Object.prototype.hasOwnProperty.call(value, name);
const kinds = new Set<ColumnKind>(['boolean', 'u32', 'i32', 'f64', 'i64', 'string']);

function textBytes(value: string): number {
  let bytes = 0;
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new TypeError('Column text contains an unpaired surrogate');
      bytes += 4;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) throw new TypeError('Column text contains an unpaired surrogate');
    else bytes += unit <= 0x7f ? 1 : unit <= 0x7ff ? 2 : 3;
  }
  return bytes;
}

function utf8(value: string, remaining: number): Uint8Array {
  // Reject before allocating an unbounded encoded copy. UTF-16 code-unit length
  // is a lower bound for well-formed UTF-8; worst transient copy is bounded too.
  if (value.length > remaining) throw new RangeError('Column page payload budget exceeded');
  if (textBytes(value) > remaining) throw new RangeError('Column page payload budget exceeded');
  const bytes = encoder.encode(value);
  if (bytes.byteLength > remaining) throw new RangeError('Column page payload budget exceeded');
  return bytes;
}

export function validateColumnRequest(ids: readonly string[], projection: ColumnProjection, limits: ColumnPageLimits): void {
  if (!Number.isSafeInteger(limits.maxRows) || limits.maxRows < 0 || ids.length > limits.maxRows)
    throw new RangeError('Column page row budget exceeded');
  if (!Number.isSafeInteger(limits.maxPayloadBytes) || limits.maxPayloadBytes < 0)
    throw new RangeError('Column page requires a finite payload budget');
  if (ids.some(id => typeof id !== 'string')) throw new TypeError('Public replica identities must remain strings');
  const masks = Math.ceil(ids.length / 8);
  let minimum = masks;
  for (const id of ids) minimum += textBytes(id);
  for (const [name, specification] of Object.entries(projection)) {
    if (!name || !kinds.has(specification.kind)) throw new TypeError(`Unsupported column projection: ${name}`);
    minimum += textBytes(name) + masks * 2 + (specification.kind === 'boolean' ? masks
      : specification.kind === 'string' ? ids.length + 4 : ids.length * (['f64','i64'].includes(specification.kind) ? 8 : 4));
  }
  if (minimum > limits.maxPayloadBytes) throw new RangeError('Column page payload budget exceeded');
}

/** Read only the supplied IDs/columns. The reader must expose one synchronous,
 * atomic replica version and must not hydrate or allocate another entity cache. */
export function packColumnPage(
  ids: readonly string[],
  projection: ColumnProjection,
  read: (id: string) => Readonly<Record<string, unknown>> | undefined,
  limits: ColumnPageLimits,
): ColumnPage {
  validateColumnRequest(ids, projection, limits);
  let payloadBytes = 0;
  const reserve = (bytes: number) => {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > limits.maxPayloadBytes - payloadBytes)
      throw new RangeError('Column page payload budget exceeded');
    payloadBytes += bytes;
  };
  for (const id of ids) reserve(utf8(id, limits.maxPayloadBytes - payloadBytes).byteLength);
  const masks = Math.ceil(ids.length / 8);
  reserve(masks);
  const page: ColumnPage = { version: 1, ids: [...ids], rowCount: ids.length, rowsPresent: new Uint8Array(masks),
    columns: Object.create(null) as Record<string, PackedColumn>, completeProjection: true, payloadBytes: 0 };
  // Retain only references to the requested rows during this synchronous call.
  const rows = ids.map(id => read(id));
  rows.forEach((row, i) => { if (row) mark(page.rowsPresent, i); else page.completeProjection = false; });
  for (const [name, spec] of Object.entries(projection)) {
    reserve(utf8(name, limits.maxPayloadBytes - payloadBytes).byteLength + masks * 2);
    const present = new Uint8Array(masks), valid = new Uint8Array(masks);
    const width = spec.kind === 'boolean' ? 0 : ['f64','i64'].includes(spec.kind) ? 8 : 4;
    reserve(spec.kind === 'boolean' ? masks : spec.kind === 'string' ? 0 : ids.length * width);
    let values: NumericLane = spec.kind === 'boolean' ? new Uint8Array(masks) : spec.kind === 'f64' ? new Float64Array(ids.length)
      : spec.kind === 'i64' ? new BigInt64Array(ids.length) : spec.kind === 'i32' ? new Int32Array(ids.length)
      : spec.kind === 'string' ? new Uint8Array(0) : new Uint32Array(ids.length);
    const strings = new Map<string, number>(), encoded: Uint8Array[] = [];
    // For strings, temporary slot numbers remain bounded by maxRows.
    const slots = spec.kind === 'string' ? new Array<number>(ids.length).fill(0) : undefined;
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      if (!row || !own(row, name)) { page.completeProjection = false; continue; }
      mark(present, i);
      const value = row[name];
      if (value === null) { if (!spec.nullable) throw new TypeError(`${name} is not nullable`); continue; }
      if (value === undefined) throw new TypeError(`${name} contains undefined instead of a missing field`);
      mark(valid, i);
      if (spec.kind === 'string') {
        if (typeof value !== 'string') throw new TypeError(`${name} requires text`);
        let slot = strings.get(value);
        if (slot === undefined) {
          const bytes = utf8(value, limits.maxPayloadBytes - payloadBytes);
          reserve(bytes.byteLength); slot = strings.size; strings.set(value, slot); encoded.push(bytes);
        }
        slots![i] = slot;
      } else if (spec.kind === 'boolean') {
        if (typeof value !== 'boolean') throw new TypeError(`${name} requires boolean`);
        if (value) mark(values as Uint8Array, i);
      } else if (spec.kind === 'i64') {
        if (typeof value !== 'bigint' && !(typeof value === 'number' && Number.isSafeInteger(value)))
          throw new TypeError(`${name} requires an exact signed 64-bit integer`);
        const integer = BigInt(value as bigint | number);
        if (integer < -(1n << 63n) || integer >= (1n << 63n)) throw new RangeError(`${name} exceeds signed 64-bit range`);
        (values as BigInt64Array)[i] = integer;
      } else {
        if (typeof value !== 'number' || !Number.isFinite(value)) throw new TypeError(`${name} requires a finite number`);
        if (spec.kind === 'u32' && (!Number.isInteger(value) || value < 0 || value > 0xffffffff)) throw new RangeError(`${name} exceeds unsigned 32-bit range`);
        if (spec.kind === 'i32' && (!Number.isInteger(value) || value < -0x80000000 || value > 0x7fffffff)) throw new RangeError(`${name} exceeds signed 32-bit range`);
        (values as Uint32Array | Int32Array | Float64Array)[i] = value;
      }
    }
    const column: PackedColumn = { kind: spec.kind, present, valid, values };
    if (slots) {
      const bytesPerSlot = strings.size <= 256 ? 1 : strings.size <= 65536 ? 2 : 4;
      reserve(ids.length * bytesPerSlot + (encoded.length + 1) * 4);
      values = bytesPerSlot === 1 ? Uint8Array.from(slots) : bytesPerSlot === 2 ? Uint16Array.from(slots) : Uint32Array.from(slots);
      const offsets = new Uint32Array(encoded.length + 1), bytes = encoded.reduce((n, data) => n + data.byteLength, 0);
      // Offsets are explicitly u32, never silently truncate a large dictionary.
      if (bytes > 0xffffffff) throw new RangeError('String dictionary exceeds 32-bit offset range');
      const data = new Uint8Array(bytes);
      let offset = 0;
      encoded.forEach((value, slot) => { offsets[slot] = offset; data.set(value, offset); offset += value.byteLength; });
      offsets[encoded.length] = offset;
      column.values = values; column.dictionary = { offsets, utf8: data };
    }
    page.columns[name] = column;
  }
  page.payloadBytes = payloadBytes;
  return page;
}

/** Caller-owned lazy decoder. Only dictionary values actually read are decoded;
 * repeat cells share that decoded string. No global or normalized-row cache. */
export function columnPageReader(page: ColumnPage): { value(column: string, row: number): ColumnValue } {
  if (page.version !== 1) throw new TypeError('Unsupported column page version');
  const text = new Map<string, Map<number, string>>();
  return {
    value(name, row) {
      if (!Number.isSafeInteger(row) || row < 0 || row >= page.rowCount) throw new RangeError('Column page row is out of bounds');
      const column = own(page.columns, name) ? page.columns[name] : undefined;
      if (!column) throw new TypeError(`Column was not projected: ${name}`);
      const masks = Math.ceil(page.rowCount / 8);
      if (page.rowsPresent.byteLength < masks || column.present.byteLength < masks || column.valid.byteLength < masks)
        throw new RangeError('Detached or invalid column page masks');
      if (!bit(page.rowsPresent, row) || !bit(column.present, row)) return undefined;
      if (!bit(column.valid, row)) return null;
      if (column.values.length < (column.kind === 'boolean' ? masks : page.rowCount))
        throw new RangeError('Detached or invalid column page values');
      if (column.kind === 'boolean') return bit(column.values as Uint8Array, row);
      const value = column.values[row];
      if (column.kind !== 'string') return value;
      const slot = Number(value), dictionary = column.dictionary;
      if (!dictionary || !Number.isInteger(slot) || slot < 0 || slot + 1 >= dictionary.offsets.length) throw new RangeError('Invalid string dictionary slot');
      let cache = text.get(name); if (!cache) { cache = new Map(); text.set(name, cache); }
      let result = cache.get(slot);
      if (result === undefined) {
        const start = dictionary.offsets[slot]!, end = dictionary.offsets[slot + 1]!;
        if (start > end || end > dictionary.utf8.byteLength) throw new RangeError('Invalid string dictionary offset');
        result = decoder.decode(dictionary.utf8.subarray(start, end)); cache.set(slot, result);
      }
      return result;
    },
  };
}

/** Only buffers allocated for this projection are transferred. Replica-owned
 * data, pending Reducers and the underlying persistence are never detached. */
export function columnPageTransfers(page: ColumnPage): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  const add = (lane: ArrayBufferView) => { if (lane.buffer instanceof ArrayBuffer) buffers.add(lane.buffer); };
  add(page.rowsPresent);
  for (const column of Object.values(page.columns)) {
    add(column.present); add(column.valid); add(column.values);
    if (column.dictionary) { add(column.dictionary.offsets); add(column.dictionary.utf8); }
  }
  return [...buffers];
}
