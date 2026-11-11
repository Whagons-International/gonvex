// JSON values as the Rust runtime's visibility code sees them (serde_json
// `Value`): equality, the token text of a value (`visibility_mask::scalar`),
// truthiness of a permission flag, and serde's compact serialization with
// object keys in BTreeMap order.
//
// One difference is inherent to JavaScript: a parsed JSON number does not
// remember whether it was written as an integer or a float, so `1` and `1.0`
// are equal here (serde_json says they differ) and an integral float's token
// text is `1` (serde writes `1.0`). SQLite compares them as equal too, so the
// SQL form agrees with this module. Visibility tokens are ids (TEXT); see the
// README.

export type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject;
export interface JsonObject {
  [key: string]: JsonValue;
}

export function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** `row.get(column)`: an own property, never one inherited from Object.prototype. */
export function field(row: JsonObject, column: string): JsonValue | undefined {
  return Object.hasOwn(row, column) ? row[column] : undefined;
}

/** Code point order (Rust's `str` Ord, BTreeMap/BTreeSet iteration order). */
export function compareText(left: string, right: string): number {
  if (left === right) return 0;
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index++) {
    const a = left.charCodeAt(index);
    const b = right.charCodeAt(index);
    if (a === b) continue;
    // Surrogates (U+D800–U+DFFF) encode code points above U+FFFF, which sort
    // after every BMP code point, including U+E000–U+FFFF.
    const highA = a >= 0xd800 && a <= 0xdfff;
    const highB = b >= 0xd800 && b <= 0xdfff;
    if (highA !== highB) return highA ? 1 : -1;
    return a - b;
  }
  return left.length - right.length;
}

export function sortedText(values: Iterable<string>): string[] {
  return [...values].sort(compareText);
}

/** serde_json `Value` equality (objects compare as maps). */
export function jsonEqual(left: JsonValue | undefined, right: JsonValue | undefined): boolean {
  if (left === right) return true;
  if (left === undefined || right === undefined || left === null || right === null) return false;
  if (typeof left !== "object" || typeof right !== "object") return false;
  if (Array.isArray(left)) {
    if (!Array.isArray(right) || left.length !== right.length) return false;
    for (let index = 0; index < left.length; index++) if (!jsonEqual(left[index], right[index])) return false;
    return true;
  }
  if (Array.isArray(right)) return false;
  const keys = Object.keys(left);
  if (keys.length !== Object.keys(right).length) return false;
  for (const key of keys) {
    if (!Object.hasOwn(right, key) || !jsonEqual(left[key], right[key])) return false;
  }
  return true;
}

/** serde_json::to_string: compact, object keys in BTreeMap order. */
export function serdeJson(value: JsonValue): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(serdeJson).join(",")}]`;
  return `{${sortedText(Object.keys(value))
    .map((key) => `${JSON.stringify(key)}:${serdeJson(value[key]!)}`)
    .join(",")}}`;
}

/** `visibility_mask::scalar`: the token text of a value. Missing and null are "null". */
export function scalar(value: JsonValue | undefined): string {
  if (value === undefined || value === null) return "null";
  if (typeof value === "string") return value;
  return serdeJson(value);
}

/** `visibility_mask::truthy`: a permission flag is true, "true", "1" or a non-zero number. */
export function truthy(value: JsonValue | undefined): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return value === "true" || value === "1";
  if (typeof value === "number") return value !== 0;
  return false;
}

/** `serde_json::Value::get(key)` on any value: only objects have fields. */
export function get(value: JsonValue | undefined, key: string): JsonValue | undefined {
  return isObject(value) ? field(value, key) : undefined;
}

/** Rust's `{:?}` of a string, for error texts. */
export function debugString(value: string): string {
  let out = '"';
  for (const character of value) {
    const code = character.codePointAt(0)!;
    if (character === '"') out += '\\"';
    else if (character === "\\") out += "\\\\";
    else if (character === "\n") out += "\\n";
    else if (character === "\r") out += "\\r";
    else if (character === "\t") out += "\\t";
    else if (character === "\0") out += "\\0";
    else if (code < 0x20 || code === 0x7f) out += `\\u{${code.toString(16)}}`;
    else out += character;
  }
  return `${out}"`;
}
