// SHA-256 whose running state can be saved and resumed (FIPS 180-4).
//
// node:crypto's Hash cannot export its state, so a table checksum that spans
// several requests (each one bounded in CPU, X10) hashes with this instead.
// The digest is the standard SHA-256: a checksum computed in pages equals the
// one node:crypto computes in one pass over the same bytes.

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);
const INITIAL = [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19];

export class Sha256 {
  constructor() {
    this.h = new Uint32Array(INITIAL);
    this.block = new Uint8Array(64);
    this.used = 0;
    /** Total bytes hashed (safe as a double up to 2^53). */
    this.bytes = 0;
    this.w = new Uint32Array(64);
  }

  update(data) {
    const bytes = data instanceof Uint8Array ? data : new Uint8Array(data.buffer ?? data, data.byteOffset ?? 0, data.byteLength);
    let i = 0;
    this.bytes += bytes.length;
    if (this.used > 0) {
      const take = Math.min(64 - this.used, bytes.length);
      this.block.set(bytes.subarray(0, take), this.used);
      this.used += take;
      i = take;
      if (this.used < 64) return this;
      this.#compress(this.block, 0);
      this.used = 0;
    }
    for (; i + 64 <= bytes.length; i += 64) this.#compress(bytes, i);
    if (i < bytes.length) {
      this.block.set(bytes.subarray(i), 0);
      this.used = bytes.length - i;
    }
    return this;
  }

  digest(encoding = 'hex') {
    if (encoding !== 'hex') throw new Error('Sha256 digests to hex only');
    const bits = this.bytes * 8;
    const tail = new Uint8Array(this.used < 56 ? 64 : 128);
    tail.set(this.block.subarray(0, this.used));
    tail[this.used] = 0x80;
    const view = new DataView(tail.buffer);
    view.setUint32(tail.length - 8, Math.floor(bits / 2 ** 32));
    view.setUint32(tail.length - 4, bits >>> 0);
    const h = new Uint32Array(this.h);
    const saved = this.h;
    this.h = h;
    for (let i = 0; i < tail.length; i += 64) this.#compress(tail, i);
    this.h = saved;
    return Array.from(h, (x) => x.toString(16).padStart(8, '0')).join('');
  }

  /** JSON-safe running state. */
  save() {
    let tail = '';
    for (let i = 0; i < this.used; i++) tail += String.fromCharCode(this.block[i]);
    return { h: Array.from(this.h), tail: btoa(tail), bytes: this.bytes };
  }

  static load(state) {
    if (!state || !Array.isArray(state.h) || state.h.length !== 8 || typeof state.tail !== 'string' || !Number.isSafeInteger(state.bytes)) {
      throw new Error('invalid hash state');
    }
    const out = new Sha256();
    out.h = new Uint32Array(state.h.map((x) => x >>> 0));
    const tail = atob(state.tail);
    if (tail.length >= 64 || tail.length !== state.bytes % 64) throw new Error('invalid hash state');
    for (let i = 0; i < tail.length; i++) out.block[i] = tail.charCodeAt(i);
    out.used = tail.length;
    out.bytes = state.bytes;
    return out;
  }

  #compress(bytes, offset) {
    const w = this.w;
    for (let t = 0; t < 16; t++) {
      const j = offset + t * 4;
      w[t] = (bytes[j] << 24) | (bytes[j + 1] << 16) | (bytes[j + 2] << 8) | bytes[j + 3];
    }
    for (let t = 16; t < 64; t++) {
      const a = w[t - 15];
      const b = w[t - 2];
      const s0 = ((a >>> 7) | (a << 25)) ^ ((a >>> 18) | (a << 14)) ^ (a >>> 3);
      const s1 = ((b >>> 17) | (b << 15)) ^ ((b >>> 19) | (b << 13)) ^ (b >>> 10);
      w[t] = (w[t - 16] + s0 + w[t - 7] + s1) | 0;
    }
    const h = this.h;
    let a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], k = h[7];
    for (let t = 0; t < 64; t++) {
      const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
      const ch = (e & f) ^ (~e & g);
      const t1 = (k + S1 + ch + K[t] + w[t]) | 0;
      const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const t2 = (S0 + maj) | 0;
      k = g;
      g = f;
      f = e;
      e = (d + t1) | 0;
      d = c;
      c = b;
      b = a;
      a = (t1 + t2) | 0;
    }
    h[0] = (h[0] + a) | 0;
    h[1] = (h[1] + b) | 0;
    h[2] = (h[2] + c) | 0;
    h[3] = (h[3] + d) | 0;
    h[4] = (h[4] + e) | 0;
    h[5] = (h[5] + f) | 0;
    h[6] = (h[6] + g) | 0;
    h[7] = (h[7] + k) | 0;
  }
}

/** `createHash`-shaped factory for RowHasher. */
export const createResumableHash = (algorithm) => {
  if (algorithm !== 'sha256') throw new Error(`unsupported hash ${algorithm}`);
  return new Sha256();
};
