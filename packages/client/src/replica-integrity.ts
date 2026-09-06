import type { JsonValue } from "@gonvex/protocol";

/** Hash normalized rows before advertising a Replica Collection resume. */
export async function replicaRowsHashes(
  rows: readonly JsonValue[],
  keyField: string,
): Promise<Record<string, string>> {
  const seen = new Set<string>();
  const entries = await Promise.all(rows.map(async (row) => {
    const key = replicaRowKey(row, keyField);
    if (!key) throw new Error(`replica row is missing key ${JSON.stringify(keyField)}`);
    if (seen.has(key)) throw new Error(`replica collection contains duplicate key ${JSON.stringify(key)}`);
    seen.add(key);
    return [key, await hashValue(stableStringify(row))] as const;
  }));
  return Object.fromEntries(entries);
}

export async function replicaHashesDigest(hashes: Readonly<Record<string, string>>): Promise<string> {
  const pairs = Object.keys(hashes).sort(utf8KeyCompare).map((key) => [key, hashes[key]]);
  return hashValue(stableStringify(pairs as JsonValue));
}

export function replicaRowKey(value: JsonValue, keyField: string) {
  if (!value || Array.isArray(value) || typeof value !== "object") return "";
  const key = value[keyField];
  return key === null || key === undefined ? "" : String(key);
}

async function hashValue(value: string) {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) throw new Error("web-crypto-unavailable");
  const digest = await subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (part) => part.toString(16).padStart(2, "0")).join("");
}

function stableStringify(value: JsonValue): string {
  if (typeof value === "string") {
    return JSON.stringify(value)
      .replace(/\u2028/g, "\\u2028")
      .replace(/\u2029/g, "\\u2029");
  }
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  return `{${Object.keys(value)
    .sort(utf8KeyCompare)
    .map((key) => `${stableStringify(key)}:${stableStringify(value[key]!)}`)
    .join(",")}}`;
}

function utf8KeyCompare(left: string, right: string) {
  // ASCII keys have identical UTF-8 and JavaScript lexical ordering.
  if (!/[^\x00-\x7f]/.test(left) && !/[^\x00-\x7f]/.test(right)) return left < right ? -1 : left > right ? 1 : 0;
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    const difference = leftBytes[index]! - rightBytes[index]!;
    if (difference !== 0) return difference;
  }
  return leftBytes.length - rightBytes.length;
}
