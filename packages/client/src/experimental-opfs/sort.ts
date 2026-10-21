import {
  assert,
  decode,
  encode,
  crc32,
  KEY_BYTES,
  VALUE_BYTES,
} from "./codec.js";
import type { KeyValue } from "./database.js";
import type { ReplicaFile } from "./file.js";

/** External merge sort. Scratch is separate from committed data and disposable.
 * The caller's budget covers buffered records and merge heads. Run descriptors
 * have an explicit cap; exhaustion refuses the snapshot without publishing it. */
export class ExternalSort {
  private rows: KeyValue[] = [];
  private bytes = 0;
  private end = 0;
  private runs: Array<{
    at: number;
    end: number;
    cache?: Uint8Array;
    cacheAt?: number;
  }> = [];
  constructor(
    private file: ReplicaFile,
    readonly budget = 1024 * 1024,
  ) {
    assert(
      budget >= VALUE_BYTES + KEY_BYTES + 96,
      "Sort budget cannot hold one maximum record",
    );
    file.truncate(0);
  }
  push(key: string, value: Uint8Array) {
    assert(
      key.length <= KEY_BYTES && value.length <= VALUE_BYTES,
      "Sort record budget",
    );
    const cost = key.length * 2 + value.length + 96;
    if (this.bytes + cost > this.budget) this.spill();
    this.bytes += cost;
    this.rows.push({ key, value });
  }
  private spill() {
    if (!this.rows.length) return;
    assert(this.runs.length < 256, "External sort run budget");
    this.rows.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    const at = this.end;
    const buffer = new Uint8Array(65536);
    let used = 0;
    const write = (bytes: Uint8Array) => {
      let start = 0;
      while (start < bytes.length) {
        const count = Math.min(bytes.length - start, buffer.length - used);
        buffer.set(bytes.subarray(start, start + count), used);
        used += count;
        start += count;
        if (used === buffer.length) {
          this.file.write(this.end, buffer);
          this.end += used;
          used = 0;
        }
      }
    };
    for (const { key, value } of this.rows) {
      const bytes = new Uint8Array(12),
        data = new DataView(bytes.buffer),
        keyBytes = encode(key);
      data.setUint32(0, key.length, true);
      data.setUint32(4, value.length, true);
      data.setUint32(8, crc32(bytes.subarray(0, 8), keyBytes, value), true);
      write(bytes);
      write(keyBytes);
      write(value);
    }
    if (used) {
      this.file.write(this.end, buffer.subarray(0, used));
      this.end += used;
    }
    this.runs.push({ at, end: this.end });
    this.rows = [];
    this.bytes = 0;
  }
  *finish(): Generator<KeyValue> {
    this.spill();
    let bytes = 0;
    const cacheBytes = Math.min(
      65536,
      Math.floor(this.budget / Math.max(1, this.runs.length) / 2),
    );
    const headBudget = this.budget - cacheBytes * this.runs.length;
    const read = (run: number, target: Uint8Array) => {
      const position = this.runs[run]!;
      let done = 0;
      while (done < target.length) {
        if (
          !position.cache ||
          position.at < position.cacheAt! ||
          position.at >= position.cacheAt! + position.cache.length
        ) {
          position.cacheAt = position.at;
          position.cache = new Uint8Array(
            Math.min(cacheBytes, position.end - position.at),
          );
          this.file.read(position.at, position.cache);
        }
        const offset = position.at - position.cacheAt!,
          count = Math.min(
            target.length - done,
            position.cache.length - offset,
          );
        assert(count > 0, "Truncated scratch run");
        target.set(position.cache.subarray(offset, offset + count), done);
        position.at += count;
        done += count;
      }
    };
    type Head = KeyValue & { run: number; cost: number };
    const heap: Head[] = [];
    const push = (entry: Head) => {
      let i = heap.length;
      heap.push(entry);
      while (i > 0) {
        const p = (i - 1) >>> 1;
        if (heap[p]!.key <= entry.key) break;
        heap[i] = heap[p]!;
        i = p;
      }
      heap[i] = entry;
    };
    const pop = () => {
      const first = heap[0]!,
        last = heap.pop()!;
      if (heap.length) {
        let i = 0;
        while (i * 2 + 1 < heap.length) {
          let child = i * 2 + 1;
          if (
            child + 1 < heap.length &&
            heap[child + 1]!.key < heap[child]!.key
          )
            child++;
          if (heap[child]!.key >= last.key) break;
          heap[i] = heap[child]!;
          i = child;
        }
        heap[i] = last;
      }
      return first;
    };
    const advance = (run: number) => {
      const position = this.runs[run]!;
      if (position.at === position.end) return;
      assert(position.at + 12 <= position.end, "Corrupt sort run header");
      const header = new Uint8Array(12);
      read(run, header);
      const data = new DataView(header.buffer),
        n = data.getUint32(0, true),
        m = data.getUint32(4, true),
        cost = n * 2 + m + 96;
      assert(
        n > 0 &&
          n <= KEY_BYTES &&
          m <= VALUE_BYTES &&
          position.at + n + m <= position.end,
        "Corrupt sort run length",
      );
      assert(bytes + cost <= headBudget, "Sort merge head budget");
      const key = new Uint8Array(n),
        value = new Uint8Array(m);
      read(run, key);
      read(run, value);
      assert(
        data.getUint32(8, true) === crc32(header.subarray(0, 8), key, value),
        "Corrupt scratch record checksum",
      );
      bytes += cost;
      push({ key: decode(key), value, run, cost });
    };
    for (let i = 0; i < this.runs.length; i++) advance(i);
    while (heap.length) {
      const head = pop();
      bytes -= head.cost;
      advance(head.run);
      yield { key: head.key, value: head.value };
    }
  }
  close() {
    this.file.close();
  }
}
