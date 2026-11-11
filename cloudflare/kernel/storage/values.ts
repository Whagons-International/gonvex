// Values in and out of Durable Object SQLite with the Rust runtime's rules.
//
// Out: rust/crates/store/src/rows.rs (`JsonKind::from_declared`) and
// direct.rs `write_json`, which host_calls.rs `write_query_rows` uses to
// encode module query rows. SQLite values carry a storage class, not a type;
// the declared column type decides whether an INTEGER is a boolean and
// whether TEXT holds JSON.
//
// In: rust/crates/runtime/src/sql_parameters.rs. A JSON value binds by its
// own type (booleans as 0/1, arrays and objects as JSON text); for a known
// column, a JSON column always stores serialized JSON.
//
// Durable Object SQL returns INTEGER and REAL both as JS numbers, BLOB as an
// ArrayBuffer, and binds every JS number as REAL. Integers above 2^53 are
// not exact in a JS number on either side; module code sees JS numbers in
// Rust too (V8), so the visible value is the same.

export type JsonKind = "json" | "boolean" | "integer" | "real" | "text";

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

/** rows.rs `JsonKind::from_declared`. */
export function kindFromDeclared(declared: string): JsonKind {
  const upper = declared.toUpperCase();
  if (upper.includes("JSON")) return "json";
  if (upper.includes("BOOL")) return "boolean";
  if (upper.includes("INT")) return "integer";
  if (upper.includes("REAL") || upper.includes("DOUB") || upper.includes("FLOA") || upper.includes("NUMERIC") || upper.includes("DECIMAL")) return "real";
  return "text";
}

/** A raw cell as Durable Object SQL returns it. */
export type SqlCell = null | number | string | ArrayBuffer;

const HEX = "0123456789abcdef";

/** A BLOB as lowercase hex (direct.rs `write_hex`, rows.rs `decode_value`). */
export function blobHex(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let out = "";
  for (const byte of bytes) out += HEX[byte >> 4]! + HEX[byte & 15]!;
  return out;
}

/** serde_json: a non-finite float has no JSON form and is written as null. */
const number = (value: number): number | null => (Number.isFinite(value) ? value : null);

/** direct.rs `write_json`: one cell decoded through its declared kind. */
export function decodeCell(kind: JsonKind, value: unknown): JsonValue {
  if (value === null || value === undefined) return null;
  if (value instanceof ArrayBuffer) return blobHex(value);
  if (typeof value === "number") {
    if (kind === "boolean") return value !== 0;
    return number(value);
  }
  if (typeof value === "string") {
    if (kind === "json") {
      try {
        return JSON.parse(value) as JsonValue;
      } catch {
        return value;
      }
    }
    return value;
  }
  if (typeof value === "bigint") return Number(value);
  return String(value);
}

/** host_calls.rs `json_shaped`. */
function jsonShaped(text: string): boolean {
  const trimmed = text.trim();
  const first = trimmed[0];
  return first === "{" || first === "[" || first === '"' || trimmed === "true" || trimmed === "false";
}

/**
 * host_calls.rs `write_expression_cell`: a column with no declared type.
 * TEXT that is unambiguously a JSON value (object, array, quoted string,
 * true/false) decodes as JSON, because SQL builds JSON there
 * (`json_object`, `json_group_array`); anything else by storage class.
 */
export function decodeExpressionCell(value: unknown): JsonValue {
  if (typeof value === "string" && jsonShaped(value)) {
    try {
      return JSON.parse(value) as JsonValue;
    } catch {
      return value;
    }
  }
  return decodeCell("text", value);
}

/** One SQLite parameter (sql_parameters.rs `Parameter`). */
export type Parameter =
  | { readonly kind: "null" }
  | { readonly kind: "integer"; readonly value: number }
  | { readonly kind: "real"; readonly value: number }
  | { readonly kind: "text"; readonly value: string };

export const NULL_PARAMETER: Parameter = { kind: "null" };

export class ParameterError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ParameterError";
  }
}

const I64_MAX = 2 ** 63;
const U64_MAX = 2 ** 64;

/**
 * A JS number as serde_json reads the JSON text V8 writes for it. Module
 * values cross the Rust host as JSON: `JSON.stringify` writes an integral
 * number below 1e21 without a fraction or exponent, which serde reads as an
 * i64 (or u64, which Rust refuses above i64::MAX); other numbers are f64.
 */
function numberParameter(value: number): Parameter {
  if (!Number.isFinite(value)) return NULL_PARAMETER; // JSON.stringify writes null
  if (Number.isInteger(value) && Math.abs(value) < 1e21) {
    if (value >= -I64_MAX && value < I64_MAX) return { kind: "integer", value: value === 0 ? 0 : value };
    if (value >= 0 && value < U64_MAX) throw new ParameterError("integer is outside the SQLite integer range");
  }
  return { kind: "real", value };
}

/** sql_parameters.rs `Parameter::scalar`: a JSON value bound by its own type. */
export function scalarParameter(value: unknown): Parameter {
  if (value === null || value === undefined) return NULL_PARAMETER;
  if (typeof value === "boolean") return { kind: "integer", value: value ? 1 : 0 };
  if (typeof value === "string") return { kind: "text", value };
  if (typeof value === "number") return numberParameter(value);
  if (typeof value === "bigint") return numberParameter(Number(value));
  if (typeof value === "object") return { kind: "text", value: JSON.stringify(value) };
  throw new ParameterError(`unsupported parameter type ${typeof value}`);
}

/**
 * sql_parameters.rs `Parameter::column`: a JSON column always stores
 * serialized JSON (so a string stays a JSON string), a BOOLEAN column 0/1.
 */
export function columnParameter(value: unknown, kind: JsonKind | undefined): Parameter {
  if (value === null || value === undefined) return NULL_PARAMETER;
  if (kind === "json") return { kind: "text", value: JSON.stringify(value) };
  if (kind === "boolean" && typeof value === "boolean") return { kind: "integer", value: value ? 1 : 0 };
  return scalarParameter(value);
}
