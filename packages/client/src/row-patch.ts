import type { JsonValue, ReplicaRowPatchFrame, ServerMessage } from '@gonvex/protocol';

type Row = Record<string, JsonValue>;
type Entry = { value: Row; bytes: number };
const maxBytes = 4 * 1024 * 1024;
const maxEntries = 4096;
const encoder = new TextEncoder();
const size = (value: JsonValue) => encoder.encode(JSON.stringify(value)).byteLength;
const object = (value: unknown): value is Row => value !== null && typeof value === 'object' && !Array.isArray(value);
const version = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) > 0;
const own = (value: object, key: string) => Object.prototype.hasOwnProperty.call(value, key);

function freeze(value: JsonValue): void {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
}

/** Owns only a bounded wire dictionary, independent of durable row authority.
 * A frame is fully decoded and validated before either dictionary or logical
 * message is published. Missing bases/gaps require reconnect with full rows. */
export class RowPatchDecoder {
  private entries = new Map<number, Entry>();
  private bytes = 0;
  private sequence = 0;
  private lastVersion = 0;

  clear() {
    this.entries.clear(); this.bytes = 0; this.sequence = 0; this.lastVersion = 0;
  }

  get retainedBytes() { return this.bytes; }
  get retainedEntries() { return this.entries.size; }

  decode(frame: ReplicaRowPatchFrame): ServerMessage | undefined {
    const invalid = () => { throw new Error('invalid-replica-row-patch'); };
    if (!version(frame.sequence)) return invalid();
    if (frame.sequence <= this.sequence) return undefined;
    if (frame.sequence !== this.sequence + 1 || !object(frame.message)
      || !['replica.transaction', 'replica.delta'].includes(frame.message.type)
      || !Array.isArray(frame.rows) || !Array.isArray(frame.evicted)
      || !frame.evicted.every(version)) return invalid();
    const message = frame.message;
    const transaction = message.type === 'replica.transaction';
    if (transaction ? !Array.isArray(message.changes) : !Array.isArray(message.upserts) || message.upserts.length !== 0) return invalid();
    const evicted = new Set(frame.evicted);
    const pending = new Map<number, Entry>();
    const decoded = new Map<number, Row>();
    let newest = this.lastVersion;
    for (const row of frame.rows) {
      if (!object(row) || !Number.isSafeInteger(row.index) || row.index < 0 || decoded.has(row.index)) return invalid();
      if (transaction && (!object(message.changes[row.index]) || own(message.changes[row.index]!, 'newValue')
        || !['insert', 'update'].includes(message.changes[row.index]!.operation))) return invalid();
      let value: Row, bytes: number;
      if (own(row, 'value')) {
        if (!object(row.value) || row.base !== undefined || row.set !== undefined || row.unset !== undefined) return invalid();
        value = row.value; bytes = size(value);
        freeze(value);
      } else {
        if (!version(row.base) || !object(row.set) || (row.unset !== undefined && (!Array.isArray(row.unset)
          || !row.unset.every(key => typeof key === 'string') || new Set(row.unset).size !== row.unset.length))) return invalid();
        const base = pending.get(row.base) ?? this.entries.get(row.base);
        if (!base) throw new Error('missing-replica-row-base');
        value = { ...base.value };
        bytes = base.bytes;
        let fields = Object.keys(value).length;
        const fieldSize = (key: string, field: JsonValue) => size(key) + 1 + size(field);
        for (const key of row.unset ?? []) {
          if (own(row.set, key) || !own(value, key)) return invalid();
          bytes -= fieldSize(key, value[key]!) + (fields > 1 ? 1 : 0); fields--;
          delete value[key];
        }
        for (const [key, field] of Object.entries(row.set)) {
          if (own(value, key)) bytes -= fieldSize(key, value[key]!);
          else { bytes += fields > 0 ? 1 : 0; fields++; }
          bytes += fieldSize(key, field);
          freeze(field);
          Object.defineProperty(value, key, { value: field, enumerable: true, writable: true, configurable: true });
        }
        Object.freeze(value);
      }
      if (row.version !== undefined) {
        if (!version(row.version) || row.version <= newest || bytes > maxBytes) return invalid();
        newest = row.version;
        pending.set(row.version, { value, bytes });
      } else if (row.base !== undefined) return invalid();
      decoded.set(row.index, value);
    }
    if (transaction && message.changes.some((change, index) => !object(change)
      || (change.operation !== 'delete' && !own(change, 'newValue') && !decoded.has(index)))) return invalid();
    if (!transaction && [...decoded.keys()].some(index => index >= decoded.size)) return invalid();
    let bytes = this.bytes, count = this.entries.size;
    for (const id of evicted) {
      const previous = this.entries.get(id);
      if (previous) { bytes -= previous.bytes; count--; }
    }
    for (const [id, entry] of pending) if (!evicted.has(id)) { bytes += entry.bytes; count++; }
    if (bytes > maxBytes || count > maxEntries) return invalid();
    for (const id of evicted) this.entries.delete(id);
    for (const [id, entry] of pending) if (!evicted.has(id)) this.entries.set(id, entry);
    this.bytes = bytes; this.sequence = frame.sequence; this.lastVersion = newest;
    if (transaction) return { ...message, changes: message.changes.map((change, index) =>
      decoded.has(index) ? { ...change, newValue: decoded.get(index)! } : change) };
    return { ...message, upserts: Array.from({ length: decoded.size }, (_, index) => decoded.get(index)!) };
  }
}
