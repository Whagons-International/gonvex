export const NODE_BYTES = 4096;
export const VALUE_BYTES = 262144;
export const KEY_BYTES = 1024;
export const HEADER_BYTES = 8192;
export type JsonValue =
  | null
  | string
  | number
  | boolean
  | JsonValue[]
  | { [key: string]: JsonValue };
export function validJSON(
  value: unknown,
  depth = 0,
): asserts value is JsonValue {
  assert(depth <= 64, "JSON depth budget");
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  )
    return;
  assert(
    typeof value === "object" &&
      value !== null &&
      (Array.isArray(value) ||
        Object.getPrototypeOf(value) === Object.prototype ||
        Object.getPrototypeOf(value) === null),
    "Non-JSON value",
  );
  for (const item of Object.values(value)) validJSON(item, depth + 1);
}
export const encode = (text: string) => new TextEncoder().encode(text);
const decoder = new TextDecoder("utf-8", { fatal: true });
export const decode = (bytes: Uint8Array) => decoder.decode(bytes);
const crcTable = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let j = 0; j < 8; j++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  crcTable[i] = c;
}
export function crc32(...parts: Uint8Array[]): number {
  let crc = 0xffffffff;
  for (const bytes of parts)
    for (let i = 0; i < bytes.length; i++)
      crc = crcTable[(crc ^ bytes[i]) & 255]! ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}
export function assert(ok: unknown, message: string): asserts ok {
  if (!ok) throw new Error(message);
}
export function safe(n: number, message = "Invalid integer"): number {
  assert(Number.isSafeInteger(n) && n >= 0, message);
  return n;
}
export const hex = (text: string) =>
  Array.from(encode(text), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
export const unhex = (text: string) => {
  assert(/^(?:[0-9a-f]{2})*$/.test(text), "Invalid hexadecimal key");
  return decode(
    Uint8Array.from(text.match(/../g) ?? [], (byte) => parseInt(byte, 16)),
  );
};
export const prefixEnd = (key: string) => key + "~";
