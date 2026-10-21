import type { ReplicaFile } from "./file.js";
import { bufferedFile } from "./file.js";
import {
  assert,
  crc32,
  decode,
  encode,
  safe,
  HEADER_BYTES,
  KEY_BYTES,
  NODE_BYTES,
  VALUE_BYTES,
} from "./codec.js";

type Entry = { key: string; pointer: number };
type Node = { leaf: boolean; entries: Entry[] };
export type KeyValue = { key: string; value: Uint8Array; pointer?: number };
type State = { sequence: number; root: number; end: number };
const MAGIC = encode("GVRDB001");
const view = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
const length = (node: Node) =>
  3 + node.entries.reduce((n, entry) => n + 10 + entry.key.length, 0);
const empty = (leaf: boolean): Node => ({ leaf, entries: [] });
function keyValid(key: string) {
  assert(
    key.length > 0 && key.length <= KEY_BYTES && /^[\x20-\x7e]+$/.test(key),
    "Invalid or oversized storage key",
  );
}
function pack(node: Node): Uint8Array {
  const bytes = new Uint8Array(length(node)),
    data = view(bytes);
  bytes[0] = +node.leaf;
  data.setUint16(1, node.entries.length, true);
  let at = 3;
  for (const entry of node.entries) {
    data.setUint16(at, entry.key.length, true);
    at += 2;
    bytes.set(encode(entry.key), at);
    at += entry.key.length;
    data.setBigUint64(at, BigInt(entry.pointer), true);
    at += 8;
  }
  return bytes;
}
function unpack(bytes: Uint8Array, offset: number): Node {
  assert(bytes.length >= 3 && bytes[0]! <= 1, "Corrupt node header");
  const data = view(bytes),
    count = data.getUint16(1, true);
  assert(count > 0 && count <= NODE_BYTES / 11, "Corrupt node count");
  const node = empty(bytes[0] === 1);
  let at = 3,
    previous = "";
  for (let i = 0; i < count; i++) {
    assert(at + 2 <= bytes.length, "Corrupt node key length");
    const n = data.getUint16(at, true);
    at += 2;
    assert(
      n > 0 && n <= KEY_BYTES && at + n + 8 <= bytes.length,
      "Corrupt node entry",
    );
    const key = decode(bytes.subarray(at, at + n));
    at += n;
    keyValid(key);
    const pointer = safe(Number(data.getBigUint64(at, true)));
    at += 8;
    assert(
      key > previous && pointer >= HEADER_BYTES && pointer < offset,
      "Corrupt node order or forward pointer",
    );
    previous = key;
    node.entries.push({ key, pointer });
  }
  assert(at === bytes.length, "Corrupt node trailing bytes");
  return node;
}
function lower(entries: Entry[], key: string): number {
  let lo = 0,
    hi = entries.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (entries[mid]!.key < key) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}
function header(state: State, scope: string): Uint8Array {
  const bytes = new Uint8Array(4096),
    data = view(bytes),
    name = encode(scope);
  assert(name.length > 0 && name.length <= 512, "Scope length");
  bytes.set(MAGIC);
  data.setUint32(8, 1, true);
  data.setUint16(12, name.length, true);
  bytes.set(name, 64);
  data.setBigUint64(16, BigInt(state.sequence), true);
  data.setBigUint64(24, BigInt(state.root), true);
  data.setBigUint64(32, BigInt(state.end), true);
  data.setUint32(4092, crc32(bytes.subarray(0, 4092)), true);
  return bytes;
}
function readHeader(
  bytes: Uint8Array,
  size: number,
  scope: string,
): State | undefined {
  const data = view(bytes);
  if (data.getUint32(4092, true) !== crc32(bytes.subarray(0, 4092))) return;
  assert(
    MAGIC.every((byte, i) => bytes[i] === byte) &&
      data.getUint32(8, true) === 1,
    "Unsupported replica format",
  );
  const n = data.getUint16(12, true);
  assert(
    n > 0 && n <= 512 && decode(bytes.subarray(64, 64 + n)) === scope,
    "Replica scope differs",
  );
  const state = {
    sequence: safe(Number(data.getBigUint64(16, true))),
    root: safe(Number(data.getBigUint64(24, true))),
    end: safe(Number(data.getBigUint64(32, true))),
  };
  assert(
    state.end >= HEADER_BYTES &&
      state.end <= size &&
      (!state.root || (state.root >= HEADER_BYTES && state.root < state.end)),
    "Corrupt commit bounds",
  );
  return state;
}

/** Single-owner immutable B+tree. Opening reads two headers and one root page.
 * A transaction publishes after data flush, alternate-header write, header flush.
 * Failed storage writes poison the handle. Reopen resolves an unknown outcome. */
export class ReplicaDatabase {
  private state: State;
  private poisoned = false;
  private writing = false;
  private readonly nodes = new Map<number, Node>();
  constructor(
    private readonly file: ReplicaFile,
    private readonly scope: string,
  ) {
    file = this.file = bufferedFile(file);
    assert(scope.length > 0 && encode(scope).length <= 512, "Scope length");
    const size = safe(file.size());
    if (!size) {
      this.state = { sequence: 0, root: 0, end: HEADER_BYTES };
      file.truncate(HEADER_BYTES);
      file.write(0, header(this.state, scope));
      file.flush();
      file.write(4096, header(this.state, scope));
      file.flush();
    } else {
      assert(size >= HEADER_BYTES, "Truncated replica header");
      const candidates: State[] = [];
      for (let at = 0; at < HEADER_BYTES; at += 4096) {
        const bytes = new Uint8Array(4096);
        file.read(at, bytes);
        const state = readHeader(bytes, size, scope);
        if (state) candidates.push(state);
      }
      assert(candidates.length, "No valid replica commit header");
      this.state = candidates.sort((a, b) => b.sequence - a.sequence)[0]!;
    }
    if (this.state.root) this.node(this.state.root, this.state.end);
  }
  get sequence() {
    return this.state.sequence;
  }
  get bytes() {
    return this.state.end;
  }
  close() {
    assert(!this.writing, "Transaction still open");
    this.file.close();
  }
  private healthy() {
    assert(!this.poisoned, "Unknown storage outcome; reopen required");
  }
  frame(offset: number, bound: number, kind: number): Uint8Array {
    this.healthy();
    assert(
      Number.isSafeInteger(offset) &&
        offset >= HEADER_BYTES &&
        offset + 12 <= bound,
      "Corrupt frame pointer",
    );
    const bytes = new Uint8Array(12);
    this.file.read(offset, bytes);
    const data = view(bytes),
      n = data.getUint32(4, true);
    assert(
      data.getUint32(0, true) === kind &&
        n <= (kind === 1 ? NODE_BYTES : VALUE_BYTES) &&
        offset + 12 + n <= bound,
      "Corrupt frame length/kind",
    );
    const body = new Uint8Array(n);
    this.file.read(offset + 12, body);
    assert(
      data.getUint32(8, true) === crc32(bytes.subarray(0, 8), body),
      "Corrupt frame checksum",
    );
    return body;
  }
  node(offset: number, bound: number): Node {
    const cached = this.nodes.get(offset);
    if (cached) {
      assert(
        offset + 12 + length(cached) <= bound,
        "Corrupt cached node bounds",
      );
      this.nodes.delete(offset);
      this.nodes.set(offset, cached);
      return cached;
    }
    const node = unpack(this.frame(offset, bound, 1), offset);
    for (const entry of node.entries) Object.freeze(entry);
    Object.freeze(node.entries);
    Object.freeze(node);
    if (this.nodes.size >= 32)
      this.nodes.delete(this.nodes.keys().next().value!);
    this.nodes.set(offset, node);
    return node;
  }
  get(key: string): Uint8Array | undefined {
    return this.lookup(key, this.state.root, this.state.end);
  }
  lookup(key: string, root: number, bound: number): Uint8Array | undefined {
    this.healthy();
    for (let depth = 0; depth < 32; depth++) {
      if (!root) return;
      const node = this.node(root, bound),
        entry = node.entries[lower(node.entries, key)];
      if (!entry) return;
      if (node.leaf)
        return entry.key === key
          ? this.frame(entry.pointer, root, 2)
          : undefined;
      bound = root;
      root = entry.pointer;
    }
    throw new Error("Corrupt tree depth");
  }
  *range(
    start = "",
    end?: string,
    root = this.state.root,
    bound = this.state.end,
    depth = 0,
  ): Generator<KeyValue> {
    this.healthy();
    if (!root) return;
    assert(depth < 32, "Corrupt tree depth");
    const node = this.node(root, bound);
    for (let i = lower(node.entries, start); i < node.entries.length; i++) {
      const entry = node.entries[i]!;
      if (node.leaf) {
        if (end !== undefined && entry.key >= end) break;
        yield { key: entry.key, value: this.frame(entry.pointer, root, 2) };
      } else {
        if (i > 0 && end !== undefined && node.entries[i - 1]!.key >= end)
          break;
        yield* this.range(start, end, entry.pointer, root, depth + 1);
      }
    }
  }
  verify(): number {
    this.healthy();
    const visit = (
      root: number,
      bound: number,
      depth: number,
    ): { count: number; min: string; max: string } => {
      assert(depth < 32, "Corrupt tree depth");
      const node = this.node(root, bound);
      let count = 0,
        min = "",
        max = "";
      for (const entry of node.entries) {
        if (node.leaf) {
          this.frame(entry.pointer, root, 2);
          count++;
          if (!min) min = entry.key;
        } else {
          const child = visit(entry.pointer, root, depth + 1);
          assert(
            child.max === entry.key && (!max || max < child.min),
            "Corrupt subtree bounds",
          );
          count += child.count;
          if (!min) min = child.min;
        }
        max = entry.key;
      }
      return { count, min, max };
    };
    return this.state.root
      ? visit(this.state.root, this.state.end, 0).count
      : 0;
  }
  begin(): ReplicaTransaction {
    this.healthy();
    assert(!this.writing, "Concurrent write transaction");
    this.nodes.clear();
    try {
      this.file.truncate(this.state.end);
    } catch (error) {
      this.poisoned = true;
      throw error;
    }
    this.writing = true;
    return new ReplicaTransaction(
      this,
      this.state.root,
      this.state.end,
      (next) => {
        if (!next) {
          this.writing = false;
          return;
        }
        this.healthy();
        const state = { sequence: safe(this.state.sequence + 1), ...next };
        try {
          this.file.flush();
          this.file.write(
            (state.sequence % 2) * 4096,
            header(state, this.scope),
          );
          this.file.flush();
        } catch (error) {
          this.poisoned = true;
          this.writing = false;
          throw error;
        }
        this.state = state;
        this.writing = false;
      },
    );
  }
  append(at: number, kind: number, body: Uint8Array): number {
    this.healthy();
    safe(at + 12 + body.length, "File offset budget");
    const bytes = new Uint8Array(12),
      data = view(bytes);
    data.setUint32(0, kind, true);
    data.setUint32(4, body.length, true);
    data.setUint32(8, crc32(bytes.subarray(0, 8), body), true);
    try {
      this.file.write(at, bytes);
      this.file.write(at + 12, body);
    } catch (error) {
      this.poisoned = true;
      throw error;
    }
    return at + 12 + body.length;
  }
}

export class ReplicaTransaction {
  private failed = false;
  private closed = false;
  constructor(
    private db: ReplicaDatabase,
    private root: number,
    private end: number,
    private finish: (state?: { root: number; end: number }) => void,
  ) {}
  private check() {
    assert(!this.closed && !this.failed, "Transaction closed or aborted");
  }
  abort() {
    if (!this.closed) {
      this.closed = true;
      this.finish();
    }
  }
  get(key: string) {
    this.check();
    return this.db.lookup(key, this.root, this.end);
  }
  put(key: string, value: Uint8Array) {
    this.mutate(() => {
      keyValid(key);
      assert(value.length <= VALUE_BYTES, "Value byte budget");
      this.root = this.changeRoot(key, this.value(value));
    });
  }
  delete(key: string) {
    this.mutate(() => {
      keyValid(key);
      this.root = this.changeRoot(key);
    });
  }
  private mutate(run: () => void) {
    this.check();
    try {
      run();
    } catch (error) {
      this.failed = true;
      throw error;
    }
  }
  private value(value: Uint8Array) {
    const at = this.end;
    this.end = this.db.append(at, 2, value);
    return at;
  }
  stageValue(value: Uint8Array): number {
    this.check();
    try {
      assert(value.length <= VALUE_BYTES, "Value byte budget");
      return this.value(value);
    } catch (error) {
      this.failed = true;
      throw error;
    }
  }
  private node(node: Node): Entry[] {
    if (!node.entries.length) return [];
    if (length(node) <= NODE_BYTES) {
      const at = this.end;
      this.end = this.db.append(at, 1, pack(node));
      return [{ key: node.entries.at(-1)!.key, pointer: at }];
    }
    const half = length(node) / 2;
    let n = 3,
      split = 1;
    for (let i = 0; i < node.entries.length - 1; i++) {
      n += 10 + node.entries[i]!.key.length;
      if (n >= half) {
        split = i + 1;
        break;
      }
    }
    return [
      ...this.node({ leaf: node.leaf, entries: node.entries.slice(0, split) }),
      ...this.node({ leaf: node.leaf, entries: node.entries.slice(split) }),
    ];
  }
  private changeRoot(key: string, pointer?: number): number {
    const change = (root: number, depth: number): Entry[] => {
      assert(depth < 32, "Tree depth budget");
      const source = root ? this.db.node(root, this.end) : empty(true),
        node = { leaf: source.leaf, entries: source.entries.slice() };
      let index = lower(node.entries, key);
      if (node.leaf) {
        if (node.entries[index]?.key === key)
          node.entries.splice(
            index,
            1,
            ...(pointer === undefined ? [] : [{ key, pointer }]),
          );
        else if (pointer !== undefined)
          node.entries.splice(index, 0, { key, pointer });
      } else {
        index = Math.min(index, node.entries.length - 1);
        node.entries.splice(
          index,
          1,
          ...change(node.entries[index]!.pointer, depth + 1),
        );
      }
      return this.node(node);
    };
    let entries = change(this.root, 0);
    if (entries.length > 1) entries = this.node({ leaf: false, entries });
    return entries[0]?.pointer ?? 0;
  }
  bulkReplace(rows: Iterable<KeyValue>): void {
    this.mutate(() => {
      const levels: Node[] = [];
      let previous = "";
      const push = (level: number, entry: Entry) => {
        assert(level < 32, "Bulk tree depth");
        const node = (levels[level] ??= empty(level === 0));
        if (length(node) + 10 + entry.key.length > NODE_BYTES) {
          const parent = this.node(node)[0]!;
          levels[level] = empty(level === 0);
          push(level + 1, parent);
        }
        levels[level]!.entries.push(entry);
      };
      for (const { key, value, pointer } of rows) {
        keyValid(key);
        assert(
          key > previous && value.length <= VALUE_BYTES,
          "Bulk keys must be unique and sorted; values bounded",
        );
        previous = key;
        if (pointer !== undefined)
          assert(
            Number.isSafeInteger(pointer) &&
              pointer >= HEADER_BYTES &&
              pointer < this.end,
            "Invalid staged value reference",
          );
        push(0, { key, pointer: pointer ?? this.value(value) });
      }
      if (!levels.length) {
        this.root = 0;
        return;
      }
      for (let level = 0; ; level++) {
        const node = levels[level]!;
        if (level + 1 === levels.length) {
          this.root = this.node(node)[0]!.pointer;
          return;
        }
        if (node.entries.length) push(level + 1, this.node(node)[0]!);
      }
    });
  }
  commit(): number {
    this.check();
    try {
      this.finish({ root: this.root, end: this.end });
      this.closed = true;
      return this.db.sequence;
    } catch (error) {
      this.closed = true;
      throw error;
    }
  }
}
