// Deterministic download content shared by the upstream Worker and the
// scenario driver (which computes the expected SHA-256 in Node).
export const BLOCK_BYTES = 64 * 1024;

const base = new Uint8Array(BLOCK_BYTES);
for (let i = 0; i < BLOCK_BYTES; i += 1) base[i] = (i * 31 + 7) & 0xff;

/** Block `index`: the base pattern with the index stamped in its first 4 bytes. */
export function block(index: number): Uint8Array {
  const out = base.slice();
  new DataView(out.buffer).setUint32(0, index, true);
  return out;
}

export const hex = (buffer: ArrayBuffer) =>
  [...new Uint8Array(buffer)].map((b) => b.toString(16).padStart(2, "0")).join("");
