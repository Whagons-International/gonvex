/** Exact SameValueZero membership for semi-joins. String keys live in a byte
 * arena, not individually retained decoded rows/strings. Latin-1 takes one
 * byte per code unit; other strings retain their exact UTF-16 code units,
 * including lone surrogates. Hash collisions always compare the full key. */
export class CompactRelationSet {
  private arena = new Uint8Array(0);
  private view = new DataView(this.arena.buffer);
  private buckets = new Uint32Array(0);
  private end = 0;
  private count = 0;
  private other?: Set<unknown>;
  constructor(private readonly reserve: (bytes: number) => void, private readonly visit: () => void = () => {}) {}

  private hash(value: string) {
    let hash = 2166136261, wide = false;
    for (let index = 0; index < value.length; index++) {
      this.visit();
      const code = value.charCodeAt(index); wide ||= code > 255;
      hash = Math.imul(hash ^ (code & 255), 16777619);
      hash = Math.imul(hash ^ (code >>> 8), 16777619);
    }
    return { hash: hash >>> 0, wide };
  }
  private equal(at: number, value: string, hash: number) {
    if (this.view.getUint32(at, true) !== hash || this.view.getUint32(at + 4, true) !== value.length) return false;
    const wide = this.arena[at + 8] === 1;
    for (let index = 0; index < value.length; index++) {
      this.visit();
      const code = wide ? this.view.getUint16(at + 9 + index * 2, true) : this.arena[at + 9 + index];
      if (code !== value.charCodeAt(index)) return false;
    }
    return true;
  }
  private slot(value: string, hash: number) {
    let slot = hash & (this.buckets.length - 1);
    while (this.buckets[slot] && !this.equal(this.buckets[slot]! - 1, value, hash)) { this.visit(); slot = (slot + 1) & (this.buckets.length - 1); }
    return slot;
  }
  private growBuckets() {
    const size = this.buckets.length ? this.buckets.length * 2 : 16;
    if (size > 0x80000000) throw Error('Compact relation bucket range');
    this.reserve(size * 4);
    const next = new Uint32Array(size);
    for (const pointer of this.buckets) if (pointer) {
      this.visit();
      let slot = this.view.getUint32(pointer - 1, true) & (size - 1);
      while (next[slot]) { this.visit(); slot = (slot + 1) & (size - 1); }
      next[slot] = pointer;
    }
    const previousBytes = this.buckets.byteLength;
    this.buckets = next; this.reserve(-previousBytes);
  }
  private growArena(required: number) {
    if (required <= this.arena.length) return;
    if (!Number.isSafeInteger(required) || required > 0xfffffffe) throw Error('Compact relation arena range');
    let size = this.arena.length || 32;
    while (size < required) size = Math.min(size * 2, 0xfffffffe);
    // Charge both arrays during growth, before allocating/copying the new one.
    this.reserve(size);
    const next = new Uint8Array(size); next.set(this.arena);
    const previousBytes = this.arena.byteLength;
    this.arena = next; this.view = new DataView(next.buffer); this.reserve(-previousBytes);
  }
  has(value: unknown): boolean {
    if (typeof value !== 'string') return this.other?.has(value) ?? false;
    return this.buckets.length > 0 && this.buckets[this.slot(value, this.hash(value).hash)] !== 0;
  }
  get(value: unknown): true | undefined { return this.has(value) ? true : undefined; }
  add(value: unknown) {
    if (typeof value !== 'string') {
      if (!this.other?.has(value)) {
        this.reserve((JSON.stringify(value) ?? '').length * 2 + 192);
        (this.other ??= new Set()).add(value);
      }
      return;
    }
    const { hash, wide } = this.hash(value);
    if (this.buckets.length && this.buckets[this.slot(value, hash)]) return;
    // At most half of the open-addressed slots are occupied, including after
    // collisions. Rehashing preserves the arena pointers and exact key bytes.
    if (!this.buckets.length || (this.count + 1) * 2 > this.buckets.length) this.growBuckets();
    const length = 9 + value.length * (wide ? 2 : 1);
    this.growArena(this.end + length);
    this.view.setUint32(this.end, hash, true); this.view.setUint32(this.end + 4, value.length, true);
    this.arena[this.end + 8] = wide ? 1 : 0;
    for (let index = 0; index < value.length; index++) {
      if (wide) this.view.setUint16(this.end + 9 + index * 2, value.charCodeAt(index), true);
      else this.arena[this.end + 9 + index] = value.charCodeAt(index);
    }
    this.buckets[this.slot(value, hash)] = this.end + 1;
    this.end += length; this.count++;
  }
}
