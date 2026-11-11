// Canonical row JSON, row hashes and the integrity aggregates of a Replica
// Collection (replica.rs `stable_json`, `row_hash`, `ReplicaAggregate`,
// `hashes_digest`; protocol `ReplicaSetDigest`). The SDK computes the same
// bytes (`packages/client/src/replica-integrity.ts`), so every function here
// must agree with it exactly.

import { sha256Hex } from "../capture/digest.ts";
import type { JsonValue } from "../visibility/json.ts";

const encoder = new TextEncoder();

/** UTF-8 byte order of two strings (Rust `String` Ord, `keys.sort_unstable()`). */
export function byteOrder(left: string, right: string): number {
  if (left === right) return 0;
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index++) {
    const a = left.charCodeAt(index);
    const b = right.charCodeAt(index);
    if (a === b) continue;
    // A surrogate (an astral code point) sorts after every BMP code unit in UTF-8.
    const highA = a >= 0xd800 && a <= 0xdfff;
    const highB = b >= 0xd800 && b <= 0xdfff;
    if (highA !== highB) return highA ? 1 : -1;
    return a - b;
  }
  return left.length - right.length;
}

/** `write_stable_string`: serde_json's escapes, then U+2028/U+2029 escaped. */
function stableString(value: string): string {
  const text = JSON.stringify(value);
  return text.includes("\u2028") || text.includes("\u2029") ? text.replaceAll("\u2028", "\\u2028").replaceAll("\u2029", "\\u2029") : text;
}

/** `stable_json`: sorted object keys (UTF-8 order), JavaScript number formatting. */
export function stableJson(value: JsonValue | undefined): string {
  if (value === null || value === undefined) return "null";
  switch (typeof value) {
    case "boolean": return value ? "true" : "false";
    case "number": return Number.isFinite(value) ? String(value) : "null";
    case "string": return stableString(value);
  }
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  const keys = Object.keys(value).sort(byteOrder);
  let out = "{";
  for (let index = 0; index < keys.length; index++) {
    if (index > 0) out += ",";
    out += `${stableString(keys[index]!)}:${stableJson(value[keys[index]!])}`;
  }
  return `${out}}`;
}

/** UTF-8 length of a string. */
export function utf8Length(text: string): number {
  let bytes = 0;
  for (let index = 0; index < text.length; index++) {
    const code = text.charCodeAt(index);
    if (code < 0x80) bytes += 1;
    else if (code < 0x800) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff && index + 1 < text.length) {
      bytes += 4;
      index++;
    } else bytes += 3;
  }
  return bytes;
}

export interface Canonical {
  /** `stable_json` of the projection. */
  readonly text: string;
  /** SHA-256 hex of `text` (`row_hash`). */
  readonly hash: string;
  /** UTF-8 bytes of `text` (budget accounting). */
  readonly bytes: number;
}

export function canonical(value: JsonValue): Canonical {
  const text = stableJson(value);
  return { text, hash: sha256Hex(text), bytes: utf8Length(text) };
}

export const rowHash = (value: JsonValue): string => sha256Hex(stableJson(value));

function hexBytes(hash: string): Uint8Array {
  const out = new Uint8Array(32);
  for (let index = 0; index < 32; index++) out[index] = parseInt(hash.slice(index * 2, index * 2 + 2), 16) || 0;
  return out;
}

const HEX = "0123456789abcdef";
function hexOf(bytes: Uint8Array): string {
  let out = "";
  for (const byte of bytes) out += HEX[byte >> 4]! + HEX[byte & 15]!;
  return out;
}

const MASK128 = (1n << 128n) - 1n;

/**
 * `ReplicaAggregate`: `xor1` (what the SDK negotiates) or `sum128-v1`. Both
 * are reversible, so a row's old hash is removed and its new hash added
 * without the row set.
 */
export class Aggregate {
  private readonly xor = new Uint8Array(32);
  private sum = 0n;
  private rows = 0;

  constructor(readonly kind: "xor1" | "sum128-v1") {}

  clone(): Aggregate {
    const copy = new Aggregate(this.kind);
    copy.xor.set(this.xor);
    copy.sum = this.sum;
    copy.rows = this.rows;
    return copy;
  }

  add(hash: string): void { this.change(hash, false); }
  remove(hash: string): void { this.change(hash, true); }

  private change(hash: string, remove: boolean): void {
    if (this.kind === "sum128-v1") {
      // ReplicaSetDigest refuses a malformed hash or a removal below zero rows and changes nothing
      // (the digest then disagrees and the client reconciles).
      if (!/^[0-9a-f]{64}$/.test(hash) || (remove && this.rows === 0)) return;
      const value = BigInt(`0x${hash.slice(0, 32)}`);
      this.sum = (remove ? this.sum - value : this.sum + value) & MASK128;
      this.rows += remove ? -1 : 1;
    }
    const bytes = hexBytes(hash);
    for (let index = 0; index < 32; index++) this.xor[index]! ^= bytes[index]!;
  }

  /** `require_legacy_digest`: a socket without `replicaSetDigest` continues in `xor1`. */
  legacy(): Aggregate {
    if (this.kind === "xor1") return this;
    const copy = new Aggregate("xor1");
    copy.xor.set(this.xor);
    return copy;
  }

  digest(): string {
    return this.kind === "xor1" ? `xor1:${hexOf(this.xor)}` : `sum128-v1:${this.sum.toString(16).padStart(32, "0")}`;
  }
}

/** `hashes_digest`: SHA-256 of `stable_json([[key, hash], …])` in key order (the legacy whole-set digest). */
export function hashesDigest(hashes: ReadonlyMap<string, string>): string {
  const keys = [...hashes.keys()].sort(byteOrder);
  const parts = keys.map((key) => `[${stableString(key)},${stableString(hashes.get(key)!)}]`);
  return sha256Hex(`[${parts.join(",")}]`);
}

/** `json_string_byte_len` / a row's text as page budget bytes. */
export const textBytes = (text: string): number => (/^[\x00-\x7f]*$/.test(text) ? text.length : encoder.encode(text).length);
