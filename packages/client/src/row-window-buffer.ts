/** Fixed-capacity scratch window borrowing canonical row references.
 * Numeric positions use parallel columns; no record or array is allocated by
 * successful get/set/delete/retain calls. This is mutable scratch storage:
 * do not publish it directly as a React snapshot or use it as a second replica.
 */
export class RowWindowBuffer<T> {
  private readonly positions: Uint32Array;
  private readonly occupied: Uint8Array;
  private readonly rows: Array<T | undefined>;
  private count = 0;

  constructor(readonly capacity = 200) {
    if (!Number.isInteger(capacity) || capacity < 1 || capacity > 0xffffffff) throw new RangeError('Window capacity must be a positive 32-bit integer');
    this.positions = new Uint32Array(capacity);
    this.occupied = new Uint8Array(capacity);
    this.rows = new Array(capacity);
  }
  get size(): number { return this.count; }
  /** @allocation-hot */
  private slot(position: number): number {
    // allocation-allow hot-allocation: invalid input is a cold error path; successful access creates no container
    if (!Number.isInteger(position) || position < 0 || position > 0xffffffff) throw new RangeError('Row position must be an unsigned 32-bit integer');
    return position % this.capacity;
  }
  /** @allocation-hot */
  has(position: number): boolean {
    const slot = this.slot(position);
    return this.occupied[slot] === 1 && this.positions[slot] === position;
  }
  /** @allocation-hot */
  get(position: number): T | undefined {
    const slot = this.slot(position);
    return this.occupied[slot] === 1 && this.positions[slot] === position ? this.rows[slot] : undefined;
  }
  /** @allocation-hot */
  set(position: number, row: T): void {
    const slot = this.slot(position);
    if (!this.occupied[slot]) this.count++;
    this.positions[slot] = position;
    this.occupied[slot] = 1;
    this.rows[slot] = row;
  }
  /** @allocation-hot */
  delete(position: number): boolean {
    const slot = this.slot(position);
    if (!this.occupied[slot] || this.positions[slot] !== position) return false;
    this.rows[slot] = undefined;
    this.occupied[slot] = 0;
    this.count--;
    return true;
  }
  /** @allocation-hot */
  retainRange(start: number, length: number): void {
    this.slot(start);
    // allocation-allow hot-allocation: reject an invalid range before touching storage; the valid loop only clears references
    if (!Number.isInteger(length) || length < 0 || length > this.capacity || start + length > 0x100000000) throw new RangeError('Range must fit the window and unsigned positions');
    for (let slot = 0; slot < this.capacity; slot++) {
      if (!this.occupied[slot] || this.positions[slot]! >= start && this.positions[slot]! < start + length) continue;
      this.rows[slot] = undefined;
      this.occupied[slot] = 0;
      this.count--;
    }
  }
  /** @allocation-hot */
  clear(): void {
    this.rows.fill(undefined);
    this.occupied.fill(0);
    this.count = 0;
  }
}
