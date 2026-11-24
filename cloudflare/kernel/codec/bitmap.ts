// LSB-first Uint32 bitmaps (PackedPage validity bitmaps, MEMBERS frames, dense row bitsets).

/** Number of Uint32 words needed for `n` bits. */
export const words = (n: number): number => (n + 31) >>> 5;

/** Bit `i` of `w` as 0 or 1 (JS shift count is taken mod 32). */
export const bit = (w: ArrayLike<number>, i: number): number => ((w[i >>> 5] ?? 0) >>> i) & 1;

/** Sets bit `i` of `w`. */
export const setBit = (w: Uint32Array, i: number): void => {
  w[i >>> 5] = (w[i >>> 5] ?? 0) | (1 << i);
};
