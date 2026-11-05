type SortValues = readonly unknown[] | undefined;

/** A bounded max heap over row references and parallel sort keys. Replacements
 * reuse key arrays, so scanning a large corpus allocates only capacity slots.
 * The input position preserves stable-sort ties without wrapping each row.
 */
export class QueryRowWindow<T> {
  private readonly rows: T[] = [];
  private readonly positions: number[] = [];
  private readonly sortValues: Array<unknown[] | undefined> = [];
  private seen = 0;
  private heapReady = false;

  constructor(private readonly capacity: number,
    private readonly compareRows: (left: T, right: T, leftValues: SortValues, rightValues: SortValues) => number) {}

  get size(): number { return this.rows.length; }

  /** @allocation-hot */
  add(row: T, values?: readonly unknown[]): void {
    const position = this.seen++;
    if (this.capacity === 0) return;
    if (this.rows.length < this.capacity) {
      this.rows.push(row);
      this.positions.push(position);
      // allocation-allow hot-allocation: copy scratch keys only while creating a bounded heap slot; subsequent candidates reuse those arrays
      const retained = values?.slice();
      this.sortValues.push(retained);
      return;
    }
    // A full export never displaces a candidate. Defer heap construction until
    // capacity is exceeded, and build it once in linear time.
    if (!this.heapReady) {
      for (let slot = (this.rows.length >>> 1) - 1; slot >= 0; slot--) {
        this.placeDown(slot, this.rows[slot]!, this.positions[slot]!, this.sortValues[slot]);
      }
      this.heapReady = true;
    }
    if (this.compareCandidate(row, position, values, 0) >= 0) return;
    const retained = this.sortValues[0];
    if (values && retained) for (let i = 0; i < values.length; i++) retained[i] = values[i];
    this.placeDown(0, row, position, retained);
  }

  /** @allocation-hot */
  private placeDown(slot: number, row: T, position: number, retained: unknown[] | undefined): void {
    while (slot * 2 + 1 < this.rows.length) {
      let child = slot * 2 + 1;
      if (child + 1 < this.rows.length && this.compareSlots(child + 1, child) > 0) child++;
      if (this.compareCandidate(row, position, retained, child) >= 0) break;
      this.move(child, slot);
      slot = child;
    }
    this.rows[slot] = row;
    this.positions[slot] = position;
    this.sortValues[slot] = retained;
  }

  orderedRows(offset: number): T[] {
    const order = new Uint32Array(this.rows.length);
    for (let i = 0; i < order.length; i++) order[i] = i;
    order.sort((left, right) => this.compareSlots(left, right));
    const result: T[] = [];
    for (let i = offset; i < order.length; i++) result.push(this.rows[order[i]!]!);
    return result;
  }

  /** @allocation-hot */
  private compareCandidate(row: T, position: number, values: SortValues, slot: number): number {
    return this.compareRows(row, this.rows[slot]!, values, this.sortValues[slot]) || position - this.positions[slot]!;
  }

  /** @allocation-hot */
  private compareSlots(left: number, right: number): number {
    return this.compareCandidate(this.rows[left]!, this.positions[left]!, this.sortValues[left], right);
  }

  /** @allocation-hot */
  private move(from: number, to: number): void {
    this.rows[to] = this.rows[from]!;
    this.positions[to] = this.positions[from]!;
    this.sortValues[to] = this.sortValues[from];
  }
}
