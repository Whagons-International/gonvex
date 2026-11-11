/**
 * PBKDF2-HMAC-SHA256 in plain JavaScript (decision 0007).
 *
 * Workers refuse PBKDF2 above 100,000 iterations both in WebCrypto and, on
 * the deployed platform, in `node:crypto` ("Pbkdf2 failed: iteration counts
 * above 100000 are not supported"; local workerd does not enforce it). Stored
 * hashes use 210,000 rounds and must keep verifying, so the iteration loop
 * runs here.
 *
 * Each round is HMAC(P, U) with a 32-byte U: the inner and outer hash states
 * after the key pads are computed once, and every round is exactly two
 * SHA-256 compressions of a fixed-layout block, with no allocation.
 */

const K = new Int32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);
const IV = new Int32Array([0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19]);

/** One SHA-256 compression: `state` (8 words) absorbs `w[0..15]`; `w` is scratch of 64 words. */
function compress(state: Int32Array, w: Int32Array): void {
  for (let i = 16; i < 64; i++) {
    const x = w[i - 15]!;
    const y = w[i - 2]!;
    const s0 = ((x >>> 7) | (x << 25)) ^ ((x >>> 18) | (x << 14)) ^ (x >>> 3);
    const s1 = ((y >>> 17) | (y << 15)) ^ ((y >>> 19) | (y << 13)) ^ (y >>> 10);
    w[i] = (w[i - 16]! + s0 + w[i - 7]! + s1) | 0;
  }
  let a = state[0]!, b = state[1]!, c = state[2]!, d = state[3]!, e = state[4]!, f = state[5]!, g = state[6]!, h = state[7]!;
  for (let i = 0; i < 64; i++) {
    const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
    const t1 = (h + S1 + ((e & f) ^ (~e & g)) + K[i]! + w[i]!) | 0;
    const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
    const t2 = (S0 + ((a & b) ^ (a & c) ^ (b & c))) | 0;
    h = g;
    g = f;
    f = e;
    e = (d + t1) | 0;
    d = c;
    c = b;
    b = a;
    a = (t1 + t2) | 0;
  }
  state[0] = (state[0]! + a) | 0;
  state[1] = (state[1]! + b) | 0;
  state[2] = (state[2]! + c) | 0;
  state[3] = (state[3]! + d) | 0;
  state[4] = (state[4]! + e) | 0;
  state[5] = (state[5]! + f) | 0;
  state[6] = (state[6]! + g) | 0;
  state[7] = (state[7]! + h) | 0;
}

/** SHA-256 continuing from `state` after `prefixBytes` already absorbed; returns the digest words. */
function finish(state: Int32Array, data: Uint8Array, prefixBytes: number, w: Int32Array): Int32Array {
  const s = Int32Array.from(state);
  const total = prefixBytes + data.length;
  const padded = new Uint8Array(Math.ceil((data.length + 9) / 64) * 64);
  padded.set(data);
  padded[data.length] = 0x80;
  const bits = total * 8;
  const view = new DataView(padded.buffer);
  view.setUint32(padded.length - 8, Math.floor(bits / 0x100000000));
  view.setUint32(padded.length - 4, bits >>> 0);
  for (let offset = 0; offset < padded.length; offset += 64) {
    for (let i = 0; i < 16; i++) w[i] = view.getInt32(offset + i * 4);
    compress(s, w);
  }
  return s;
}

function sha256(data: Uint8Array, w: Int32Array): Uint8Array {
  const words = finish(IV, data, 0, w);
  const out = new Uint8Array(32);
  const view = new DataView(out.buffer);
  for (let i = 0; i < 8; i++) view.setInt32(i * 4, words[i]!);
  return out;
}

/** PBKDF2-HMAC-SHA256(password, salt, rounds, length). */
export function pbkdf2Sha256(password: Uint8Array, salt: Uint8Array, rounds: number, length: number): Uint8Array {
  if (!Number.isInteger(rounds) || rounds < 1) throw new RangeError("rounds must be a positive integer");
  const w = new Int32Array(64);
  // HMAC key: hashed when longer than a block, then padded with zeros.
  const key = new Uint8Array(64);
  key.set(password.length > 64 ? sha256(password, w) : password);
  const pad = (byte: number): Int32Array => {
    const state = Int32Array.from(IV);
    const view = new DataView(key.buffer);
    for (let i = 0; i < 16; i++) w[i] = view.getInt32(i * 4) ^ (byte * 0x01010101);
    compress(state, w);
    return state;
  };
  const inner = pad(0x36);
  const outer = pad(0x5c);
  const out = new Uint8Array(length);
  const outView = new DataView(out.buffer);
  const block = new Uint8Array(salt.length + 4);
  block.set(salt);
  const u = new Int32Array(8);
  const t = new Int32Array(8);
  const state = new Int32Array(8);
  for (let index = 1, offset = 0; offset < length; index++, offset += 32) {
    // U1 = HMAC(P, salt || INT(index))
    new DataView(block.buffer).setUint32(salt.length, index);
    const innerDigest = finish(inner, block, 64, w);
    const innerBytes = new Uint8Array(32);
    const innerView = new DataView(innerBytes.buffer);
    for (let i = 0; i < 8; i++) innerView.setInt32(i * 4, innerDigest[i]!);
    u.set(finish(outer, innerBytes, 64, w));
    t.set(u);
    // U_j = HMAC(P, U_{j-1}): one inner and one outer compression of a 96-byte message.
    for (let round = 1; round < rounds; round++) {
      state.set(inner);
      for (let i = 0; i < 8; i++) w[i] = u[i]!;
      w[8] = 0x80000000 | 0;
      w[9] = 0; w[10] = 0; w[11] = 0; w[12] = 0; w[13] = 0; w[14] = 0;
      w[15] = 768;
      compress(state, w);
      for (let i = 0; i < 8; i++) w[i] = state[i]!;
      state.set(outer);
      w[8] = 0x80000000 | 0;
      w[9] = 0; w[10] = 0; w[11] = 0; w[12] = 0; w[13] = 0; w[14] = 0;
      w[15] = 768;
      compress(state, w);
      for (let i = 0; i < 8; i++) {
        u[i] = state[i]!;
        t[i] = t[i]! ^ state[i]!;
      }
    }
    for (let i = 0; i < 8 && offset + i * 4 < length; i++) {
      if (offset + i * 4 + 4 <= length) outView.setInt32(offset + i * 4, t[i]!);
      else {
        const tail = new Uint8Array(4);
        new DataView(tail.buffer).setInt32(0, t[i]!);
        out.set(tail.subarray(0, length - offset - i * 4), offset + i * 4);
      }
    }
  }
  return out;
}
