/**
 * JSON exactly as the Rust runtime writes it.
 *
 * Rust parses each row's SQLite JSON text into a `serde_json::Value` and
 * re-serializes it (`live_query.rs` `execute_in_transaction`, `query_message`).
 * The workspace enables `preserve_order` (through deno_core, `rust/Cargo.toml`),
 * so objects keep insertion order, and numbers keep serde's distinction between
 * integers and floats: SQLite's `5.0` stays `5.0`, and a float is written in
 * ryu's shortest form (`1e300`, not `1e+300`). JavaScript objects reorder
 * integer-like keys and `JSON.stringify` writes `5` for `5.0`, so the frames
 * (and the `windowRevision` hash over them) are built from this module's
 * lossless tree instead.
 */

/** A JSON number as its source lexeme. */
export class JsonNumber {
  constructor(readonly lexeme: string) {}
  /** The value as a JavaScript number (for comparisons that need one). */
  get value(): number { return Number(this.lexeme); }
}

/** An object with its members in source order (serde `preserve_order`). Duplicate keys keep the first position and the last value, as an IndexMap insert does. */
export class JsonObject {
  readonly entries: [string, JsonTree][] = [];
  private readonly index = new Map<string, number>();
  set(key: string, value: JsonTree): this {
    const at = this.index.get(key);
    if (at === undefined) {
      this.index.set(key, this.entries.length);
      this.entries.push([key, value]);
    } else this.entries[at]![1] = value;
    return this;
  }
  get(key: string): JsonTree | undefined {
    const at = this.index.get(key);
    return at === undefined ? undefined : this.entries[at]![1];
  }
  has(key: string): boolean { return this.index.has(key); }
}

export type JsonTree = null | boolean | string | JsonNumber | JsonTree[] | JsonObject;

/** Thrown by `parseJson` on malformed input (SQLite never produces it). */
export class JsonSyntaxError extends Error {}

/** Parses JSON text into a lossless tree. */
export function parseJson(text: string): JsonTree {
  let at = 0;
  const fail = (what: string): never => { throw new JsonSyntaxError(`${what} at offset ${at}`); };
  const space = () => {
    for (;;) {
      const c = text.charCodeAt(at);
      if (c === 0x20 || c === 0x0a || c === 0x0d || c === 0x09) at++;
      else return;
    }
  };
  const string = (): string => {
    at++; // opening quote
    let out = "";
    let start = at;
    for (;;) {
      const c = text.charCodeAt(at);
      if (Number.isNaN(c)) fail("unterminated string");
      if (c === 0x22) { out += text.slice(start, at); at++; return out; }
      if (c === 0x5c) {
        out += text.slice(start, at);
        const e = text[at + 1];
        at += 2;
        switch (e) {
          case '"': out += '"'; break;
          case "\\": out += "\\"; break;
          case "/": out += "/"; break;
          case "b": out += "\b"; break;
          case "f": out += "\f"; break;
          case "n": out += "\n"; break;
          case "r": out += "\r"; break;
          case "t": out += "\t"; break;
          case "u": {
            const hex = text.slice(at, at + 4);
            if (!/^[0-9a-fA-F]{4}$/.test(hex)) fail("bad unicode escape");
            out += String.fromCharCode(parseInt(hex, 16));
            at += 4;
            break;
          }
          default: fail("bad escape");
        }
        start = at;
        continue;
      }
      at++;
    }
  };
  const value = (): JsonTree => {
    space();
    const c = text[at];
    if (c === "{") {
      at++;
      const object = new JsonObject();
      space();
      if (text[at] === "}") { at++; return object; }
      for (;;) {
        space();
        if (text[at] !== '"') fail("expected a key");
        const key = string();
        space();
        if (text[at] !== ":") fail("expected ':'");
        at++;
        object.set(key, value());
        space();
        if (text[at] === ",") { at++; continue; }
        if (text[at] === "}") { at++; return object; }
        fail("expected ',' or '}'");
      }
    }
    if (c === "[") {
      at++;
      const array: JsonTree[] = [];
      space();
      if (text[at] === "]") { at++; return array; }
      for (;;) {
        array.push(value());
        space();
        if (text[at] === ",") { at++; continue; }
        if (text[at] === "]") { at++; return array; }
        fail("expected ',' or ']'");
      }
    }
    if (c === '"') return string();
    if (text.startsWith("true", at)) { at += 4; return true; }
    if (text.startsWith("false", at)) { at += 5; return false; }
    if (text.startsWith("null", at)) { at += 4; return null; }
    const match = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
    match.lastIndex = at;
    const found = match.exec(text);
    if (!found || found[0].length === 0) fail("unexpected character");
    at += found![0].length;
    return new JsonNumber(found![0]);
  };
  const result = value();
  space();
  if (at !== text.length) fail("trailing characters");
  return result;
}

/** A JavaScript value (as JSON.parse returns it) as a tree. Numbers become serde numbers by their JS value. */
export function fromJs(value: unknown): JsonTree {
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean" || typeof value === "string") return value;
  if (typeof value === "number") return new JsonNumber(Number.isFinite(value) ? String(value) : "null");
  if (value instanceof JsonNumber || value instanceof JsonObject) return value;
  if (Array.isArray(value)) return value.map(fromJs);
  if (typeof value === "object") {
    const object = new JsonObject();
    for (const [key, item] of Object.entries(value as Record<string, unknown>)) object.set(key, fromJs(item));
    return object;
  }
  return null;
}

/** The tree as a JavaScript value (key order as JavaScript keeps it). */
export function toJs(tree: JsonTree): unknown {
  if (tree instanceof JsonNumber) return tree.value;
  if (tree instanceof JsonObject) return Object.fromEntries(tree.entries.map(([key, value]) => [key, toJs(value)]));
  if (Array.isArray(tree)) return tree.map(toJs);
  return tree;
}

/** serde_json's compact serialization of a tree. */
export function stringify(tree: JsonTree): string {
  if (tree === null) return "null";
  if (tree === true) return "true";
  if (tree === false) return "false";
  if (typeof tree === "string") return quote(tree);
  if (tree instanceof JsonNumber) return serdeNumber(tree.lexeme);
  if (Array.isArray(tree)) return `[${tree.map(stringify).join(",")}]`;
  return `{${tree.entries.map(([key, value]) => `${quote(key)}:${stringify(value)}`).join(",")}}`;
}

/** serde_json `format_escaped_str`: `"`, `\`, and control characters; short forms for \b \f \n \r \t, else `\u00xx` (lowercase). */
export function quote(text: string): string {
  // Fast path: nothing to escape.
  if (!/["\\\u0000-\u001f]/.test(text)) return `"${text}"`;
  let out = '"';
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c === 0x22) out += '\\"';
    else if (c === 0x5c) out += "\\\\";
    else if (c < 0x20) {
      if (c === 0x08) out += "\\b";
      else if (c === 0x0c) out += "\\f";
      else if (c === 0x0a) out += "\\n";
      else if (c === 0x0d) out += "\\r";
      else if (c === 0x09) out += "\\t";
      else out += `\\u00${c.toString(16).padStart(2, "0")}`;
    } else out += text[i];
  }
  return out + '"';
}

const U64_MAX = 18446744073709551615n;
const I64_MIN = -9223372036854775808n;

/**
 * How serde_json (with `float_roundtrip`) re-serializes a number it parsed:
 * integers that fit u64/i64 print as integers; `-0` and integers out of range
 * become f64; every other number is an f64 written by ryu.
 */
export function serdeNumber(lexeme: string): string {
  if (lexeme === "null") return "null";
  if (/^-?\d+$/.test(lexeme)) {
    const big = BigInt(lexeme);
    const negative = lexeme.startsWith("-");
    if (negative && big === 0n) return ryu(-0); // serde: "-0" parses as F64(-0.0)
    if ((!negative && big <= U64_MAX) || (negative && big >= I64_MIN)) return big.toString();
  }
  return ryu(Number(lexeme));
}

/** ryu `format64` (as `serde_json` writes an f64); non-finite values are `null`. */
export function ryu(value: number): string {
  if (!Number.isFinite(value)) return "null";
  if (value === 0) return Object.is(value, -0) ? "-0.0" : "0.0";
  const sign = value < 0 ? "-" : "";
  // Shortest round-trip digits: ECMAScript's toExponential() picks the same digits as ryu.
  const [mantissa, exponentText] = Math.abs(value).toExponential().split("e") as [string, string];
  const digits = mantissa.replace(".", "");
  const length = digits.length;
  const kk = Number(exponentText) + 1; // 10^(kk-1) <= v < 10^kk
  const k = kk - length;
  let body: string;
  if (k >= 0 && kk <= 16) body = `${digits}${"0".repeat(k)}.0`;
  else if (kk > 0 && kk <= 16) body = `${digits.slice(0, kk)}.${digits.slice(kk)}`;
  else if (kk > -5 && kk <= 0) body = `0.${"0".repeat(-kk)}${digits}`;
  else if (length === 1) body = `${digits}e${kk - 1}`;
  else body = `${digits[0]}.${digits.slice(1)}e${kk - 1}`;
  return sign + body;
}

/**
 * Rust's `{:?}` for a `str` (`char::escape_debug` rules for the characters
 * module paths and identifiers can contain): quotes, `\\`, `\"`, `\n`, `\r`,
 * `\t`, `\0`, and `\u{…}` for other control characters.
 */
export function rustDebug(text: string): string {
  let out = '"';
  for (const ch of text) {
    const c = ch.codePointAt(0)!;
    if (ch === '"') out += '\\"';
    else if (ch === "\\") out += "\\\\";
    else if (ch === "\n") out += "\\n";
    else if (ch === "\r") out += "\\r";
    else if (ch === "\t") out += "\\t";
    else if (c === 0) out += "\\0";
    else if (c < 0x20 || c === 0x7f || (c >= 0x80 && c < 0xa0)) out += `\\u{${c.toString(16)}}`;
    else out += ch;
  }
  return out + '"';
}
