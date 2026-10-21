import { hex, assert } from "./codec.js";
export const rowPrefix = (table: string) => `r/${hex(table)}/`;
export const rowKey = (table: string, id: string) => rowPrefix(table) + hex(id);
export const dirtyPrefix = (table: string) => `d/${hex(table)}/`;
export const gramPrefix = (table: string, gram: string) =>
  `g/${hex(table)}/${hex(gram)}/`;
export const indexPrefix = (table: string, index: number) =>
  `i/${hex(table)}/${index.toString(16).padStart(4, "0")}/`;
export function pointerBytes(pointer: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(pointer), true);
  return bytes;
}
export function readPointer(bytes: Uint8Array): number {
  assert(bytes.length === 8, "Corrupt row reference");
  const pointer = Number(
    new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigUint64(
      0,
      true,
    ),
  );
  assert(Number.isSafeInteger(pointer), "Corrupt row offset");
  return pointer;
}
export function scalarKey(item: unknown): string {
  if (item === null || item === undefined) return "2";
  if (typeof item === "string") return "1" + hex(item) + "!";
  if (typeof item === "boolean") return item ? "11" : "10";
  assert(
    typeof item === "number" && Number.isFinite(item),
    "Invalid index scalar",
  );
  const bytes = new Uint8Array(8),
    data = new DataView(bytes.buffer);
  data.setFloat64(0, item === 0 ? 0 : item);
  if (bytes[0]! & 128) for (let i = 0; i < 8; i++) bytes[i] = bytes[i]! ^ 255;
  else bytes[0] = bytes[0]! ^ 128;
  return (
    "1" +
    Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")
  );
}
export function* trigrams(text: string): Generator<string> {
  let a = "",
    b = "";
  for (const char of text) {
    if (a && b) yield a + b + char;
    a = b;
    b = char;
  }
}
export function canonical(value: unknown): string {
  const stable = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(stable);
    if (value && typeof value === "object")
      return Object.fromEntries(
        Object.keys(value)
          .sort()
          .map((key) => [key, stable((value as Record<string, unknown>)[key])]),
      );
    return value;
  };
  return JSON.stringify(stable(value));
}
