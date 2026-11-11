/**
 * Password hashes (control.rs:4114-4145; contract C.3.2, decision 0007).
 *
 * `pbkdf2_sha256$<rounds>$<b64url salt>$<b64url hash>`, PBKDF2-HMAC-SHA256,
 * 210,000 rounds, 16-byte salt, 32-byte key. Stored hashes are verified at
 * their own round count and are never rehashed at a lower cost.
 *
 * Workers refuse PBKDF2 above 100,000 iterations in WebCrypto and, on the
 * deployed platform, in `node:crypto` too (local workerd does not enforce it,
 * so local tests alone would not show this). The derivation therefore runs in
 * WebAssembly built from the reference's own crates (`pbkdf2` 0.12.2 +
 * `sha2` 0.10.9, wasm/pbkdf2), with a plain-JavaScript implementation as the
 * fallback for inputs larger than the module's buffer.
 */
import { randomBytes, timingSafeEqual } from "node:crypto";
import { pbkdf2Sha256 } from "./pbkdf2";
import pbkdf2Module from "./pbkdf2.wasm";
import { base64url, fromBase64url } from "./util";

interface Pbkdf2Exports {
  memory: WebAssembly.Memory;
  buffer(): number;
  capacity(): number;
  pbkdf2(passwordLength: number, saltLength: number, rounds: number, outLength: number): number;
}

let wasm: Pbkdf2Exports | null | undefined;
function module(): Pbkdf2Exports | null {
  if (wasm === undefined) {
    try {
      wasm = new WebAssembly.Instance(pbkdf2Module, {}).exports as unknown as Pbkdf2Exports;
    } catch {
      wasm = null;
    }
  }
  return wasm;
}

/** PBKDF2-HMAC-SHA256 at any round count. */
export function derive(password: Uint8Array, salt: Uint8Array, rounds: number, length: number): Uint8Array {
  const exports = module();
  const half = exports ? exports.capacity() / 2 : 0;
  if (exports && password.length + salt.length <= half && length <= half && rounds <= 0xffffffff) {
    const memory = new Uint8Array(exports.memory.buffer, exports.buffer(), exports.capacity());
    memory.set(password, 0);
    memory.set(salt, password.length);
    if (exports.pbkdf2(password.length, salt.length, rounds, length) === 0) {
      const out = new Uint8Array(new Uint8Array(exports.memory.buffer, exports.buffer(), length));
      memory.fill(0, 0, Math.max(length, password.length + salt.length));
      return out;
    }
  }
  return pbkdf2Sha256(password, salt, rounds, length);
}

const utf8 = (value: string): Uint8Array => new TextEncoder().encode(value);

export const PASSWORD_ROUNDS = 210_000;

export function hashPassword(password: string): string {
  const salt = new Uint8Array(randomBytes(16));
  const hash = derive(utf8(password), salt, PASSWORD_ROUNDS, 32);
  return `pbkdf2_sha256$${PASSWORD_ROUNDS}$${base64url(salt)}$${base64url(hash)}`;
}

export function verifyPassword(password: string, encoded: string): boolean {
  const parts = encoded.split("$");
  if (parts.length !== 4 || parts[0] !== "pbkdf2_sha256") return false;
  // Rust `str::parse::<u32>`: decimal digits with an optional leading '+'.
  if (!/^\+?\d+$/.test(parts[1]!)) return false;
  const rounds = Number(parts[1]);
  if (!Number.isSafeInteger(rounds) || rounds > 0xffffffff) return false;
  const salt = fromBase64url(parts[2]!);
  const expected = fromBase64url(parts[3]!);
  if (!salt || !expected || rounds === 0 || expected.length === 0) return false;
  const actual = derive(utf8(password), salt, rounds, expected.length);
  return timingSafeEqual(actual, expected);
}
