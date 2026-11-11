// serde_json's value model, as reference_integrity.rs sees it.
//
// The Rust ledger stores and compares `serde_json::Value`s: integers and
// floats are different numbers (`1 != 1.0`, `1.0` prints as "1.0"), integers
// are exact up to u64/i64, and objects keep insertion order (serde_json is
// built with `preserve_order`). JavaScript's JSON loses all three, so the port
// parses and prints JSON itself:
//   - an integer is a `bigint`, a float a `number`;
//   - an object is a `Map` (insertion order, integer-like keys included);
//   - `stringify` is serde_json's compact form (floats by zmij, the shortest
//     round-trip digits, fixed notation for exponents -5..=15).
// Parse errors carry serde_json's error texts ("expected value at line 1
// column 1"); positions follow serde_json's rules (byte column after the
// offending byte) but are not verified against every serde_json path.

export type Json = null | boolean | string | bigint | number | Json[] | JsonObject;
export type JsonObject = Map<string, Json>;

const U64_MAX = 18446744073709551615n;
const I64_MIN = -9223372036854775808n;
const RECURSION_LIMIT = 128;

/** A serde_json parse error; `message` is serde_json's Display text. */
export class JsonError extends Error {
  override readonly name = "JsonError";
}

export const isObject = (value: Json | undefined): value is JsonObject => value instanceof Map;
export const isNumber = (value: Json | undefined): value is bigint | number => typeof value === "bigint" || typeof value === "number";

/** `Value::get(key)`: a member of an object, `undefined` for anything else. */
export function get(value: Json | undefined, key: string): Json | undefined {
  return value instanceof Map ? value.get(key) : undefined;
}

/** `serde_json::from_str::<Value>`. */
export function parseJson(text: string): Json {
  return new Parser(text).document();
}

class Parser {
  private index = 0;
  private depth = RECURSION_LIMIT;
  constructor(private readonly text: string) {}

  document(): Json {
    const value = this.value();
    this.whitespace();
    if (this.index < this.text.length) this.fail("trailing characters", this.index);
    return value;
  }

  private fail(code: string, at: number | null): never {
    // serde_json: line and column of the byte after the offending one, or of the end of input.
    const end = at === null ? this.text.length : at;
    const before = this.text.slice(0, end);
    const lineStart = before.lastIndexOf("\n") + 1;
    const line = before.split("\n").length;
    let column = new TextEncoder().encode(this.text.slice(lineStart, end)).length;
    if (at !== null) column += 1;
    throw new JsonError(`${code} at line ${line} column ${column}`);
  }

  private whitespace(): void {
    for (;;) {
      const c = this.text.charCodeAt(this.index);
      if (c === 0x20 || c === 0x0a || c === 0x09 || c === 0x0d) this.index++;
      else return;
    }
  }

  private value(): Json {
    this.whitespace();
    if (this.index >= this.text.length) this.fail("EOF while parsing a value", null);
    const c = this.text[this.index]!;
    switch (c) {
      case "n": return this.ident("null", null);
      case "t": return this.ident("true", true);
      case "f": return this.ident("false", false);
      case "\"": this.index++; return this.string();
      case "[": return this.nested(() => this.array());
      case "{": return this.nested(() => this.object());
      default:
        if (c === "-" || (c >= "0" && c <= "9")) return this.number();
        return this.fail("expected value", this.index);
    }
  }

  private nested(parse: () => Json): Json {
    if (--this.depth === 0) this.fail("recursion limit exceeded", this.index);
    this.index++;
    const value = parse();
    this.depth++;
    return value;
  }

  private ident<T extends Json>(word: string, value: T): T {
    this.index++;
    for (let i = 1; i < word.length; i++, this.index++) {
      if (this.index >= this.text.length) this.fail("EOF while parsing a value", null);
      if (this.text[this.index] !== word[i]) this.fail("expected ident", this.index);
    }
    return value;
  }

  private number(): bigint | number {
    const start = this.index;
    const negative = this.text[this.index] === "-";
    if (negative) this.index++;
    const digit = (at: number) => { const c = this.text.charCodeAt(at); return c >= 0x30 && c <= 0x39; };
    if (this.index >= this.text.length) this.fail("EOF while parsing a value", null);
    if (!digit(this.index)) this.fail("invalid number", this.index);
    if (this.text[this.index] === "0") {
      this.index++;
      if (digit(this.index)) this.fail("invalid number", this.index);
    } else {
      while (digit(this.index)) this.index++;
    }
    let float = false;
    if (this.text[this.index] === ".") {
      float = true;
      this.index++;
      if (this.index >= this.text.length) this.fail("EOF while parsing a value", null);
      if (!digit(this.index)) this.fail("invalid number", this.index);
      while (digit(this.index)) this.index++;
    }
    const e = this.text[this.index];
    if (e === "e" || e === "E") {
      float = true;
      this.index++;
      const sign = this.text[this.index];
      if (sign === "+" || sign === "-") this.index++;
      if (this.index >= this.text.length) this.fail("EOF while parsing a value", null);
      if (!digit(this.index)) this.fail("invalid number", this.index);
      while (digit(this.index)) this.index++;
    }
    const text = this.text.slice(start, this.index);
    if (!float) {
      const integer = BigInt(text);
      // serde_json: u64 or a negative i64; `-0` and anything out of range is a float.
      if (!negative && integer <= U64_MAX) return integer;
      if (negative && integer < 0n && integer >= I64_MIN) return integer;
    }
    const value = Number(text);
    if (!Number.isFinite(value)) this.fail("number out of range", this.index - 1);
    return value;
  }

  private string(): string {
    let out = "";
    for (;;) {
      if (this.index >= this.text.length) this.fail("EOF while parsing a string", null);
      const c = this.text[this.index]!;
      const code = c.charCodeAt(0);
      if (c === "\"") { this.index++; return out; }
      if (code < 0x20) this.fail("control character (\\u0000-\\u001F) found while parsing a string", this.index);
      if (c !== "\\") { out += c; this.index++; continue; }
      this.index++;
      if (this.index >= this.text.length) this.fail("EOF while parsing a string", null);
      const escape = this.text[this.index]!;
      this.index++;
      switch (escape) {
        case "\"": out += "\""; break;
        case "\\": out += "\\"; break;
        case "/": out += "/"; break;
        case "b": out += "\b"; break;
        case "f": out += "\f"; break;
        case "n": out += "\n"; break;
        case "r": out += "\r"; break;
        case "t": out += "\t"; break;
        case "u": {
          const unit = this.hex();
          if (unit >= 0xdc00 && unit <= 0xdfff) this.fail("lone leading surrogate in hex escape", this.index - 1);
          if (unit >= 0xd800 && unit <= 0xdbff) {
            if (this.text[this.index] !== "\\" || this.text[this.index + 1] !== "u") this.fail("unexpected end of hex escape", this.index);
            this.index += 2;
            const low = this.hex();
            if (low < 0xdc00 || low > 0xdfff) this.fail("lone leading surrogate in hex escape", this.index - 1);
            out += String.fromCharCode(unit, low);
          } else {
            out += String.fromCharCode(unit);
          }
          break;
        }
        default: this.fail("invalid escape", this.index - 1);
      }
    }
  }

  private hex(): number {
    let value = 0;
    for (let i = 0; i < 4; i++) {
      if (this.index >= this.text.length) this.fail("EOF while parsing a string", null);
      const digit = parseInt(this.text[this.index]!, 16);
      if (Number.isNaN(digit)) this.fail("invalid escape", this.index);
      value = value * 16 + digit;
      this.index++;
    }
    return value;
  }

  private array(): Json[] {
    const out: Json[] = [];
    this.whitespace();
    if (this.text[this.index] === "]") { this.index++; return out; }
    for (;;) {
      out.push(this.value());
      this.whitespace();
      if (this.index >= this.text.length) this.fail("EOF while parsing a list", null);
      const c = this.text[this.index];
      if (c === "]") { this.index++; return out; }
      if (c !== ",") this.fail("expected `,` or `]`", this.index);
      this.index++;
      this.whitespace();
      if (this.text[this.index] === "]") this.fail("trailing comma", this.index);
    }
  }

  private object(): JsonObject {
    const out: JsonObject = new Map();
    this.whitespace();
    if (this.text[this.index] === "}") { this.index++; return out; }
    for (;;) {
      this.whitespace();
      if (this.index >= this.text.length) this.fail("EOF while parsing an object", null);
      if (this.text[this.index] !== "\"") this.fail("key must be a string", this.index);
      this.index++;
      const key = this.string();
      this.whitespace();
      if (this.index >= this.text.length) this.fail("EOF while parsing an object", null);
      if (this.text[this.index] !== ":") this.fail("expected `:`", this.index);
      this.index++;
      out.set(key, this.value());
      this.whitespace();
      if (this.index >= this.text.length) this.fail("EOF while parsing an object", null);
      const c = this.text[this.index];
      if (c === "}") { this.index++; return out; }
      if (c !== ",") this.fail("expected `,` or `}`", this.index);
      this.index++;
      this.whitespace();
      if (this.text[this.index] === "}") this.fail("trailing comma", this.index);
    }
  }
}

/** zmij `format_finite` for f64, as serde_json prints a float. */
export function formatFloat(value: number): string {
  if (value === 0) return Object.is(value, -0) ? "-0.0" : "0.0";
  const sign = value < 0 ? "-" : "";
  const [mantissa = "", exponent = "0"] = Math.abs(value).toExponential().split("e");
  const digits = mantissa.replace(".", "");
  const exp = Number(exponent);
  const length = digits.length;
  if (exp >= -5 && exp <= 15) {
    if (length - 1 <= exp) return `${sign}${digits}${"0".repeat(exp - (length - 1))}.0`;
    if (exp >= 0) return `${sign}${digits.slice(0, exp + 1)}.${digits.slice(exp + 1)}`;
    return `${sign}0.${"0".repeat(-exp - 1)}${digits}`;
  }
  const head = length > 1 ? `${digits[0]}.${digits.slice(1)}` : digits;
  return `${sign}${head}e${exp >= 0 ? "+" : "-"}${Math.abs(exp)}`;
}

/** serde_json `Number` Display. */
export function displayNumber(value: bigint | number): string {
  return typeof value === "bigint" ? value.toString() : formatFloat(value);
}

/** serde_json `Value` Display (compact JSON). */
export function stringify(value: Json): string {
  if (value === null) return "null";
  switch (typeof value) {
    case "boolean": return value ? "true" : "false";
    case "string": return JSON.stringify(value);
    case "bigint": case "number": return displayNumber(value);
  }
  if (Array.isArray(value)) return `[${value.map(stringify).join(",")}]`;
  const members: string[] = [];
  for (const [key, member] of value) members.push(`${JSON.stringify(key)}:${stringify(member)}`);
  return `{${members.join(",")}}`;
}

/** serde_json `Value` equality (object member order does not matter). */
export function equal(left: Json | undefined, right: Json | undefined): boolean {
  if (left === right) return true;
  if (left === undefined || right === undefined || left === null || right === null) return false;
  if (Array.isArray(left)) {
    return Array.isArray(right) && left.length === right.length && left.every((item, index) => equal(item, right[index]));
  }
  if (left instanceof Map) {
    if (!(right instanceof Map) || left.size !== right.size) return false;
    for (const [key, member] of left) {
      if (!right.has(key) || !equal(member, right.get(key))) return false;
    }
    return true;
  }
  return false;
}

/** The `->>` text of a value: strings as is, null as none, anything else as JSON. */
export function textOf(value: Json | undefined): string | null {
  if (value === undefined || value === null) return null;
  return typeof value === "string" ? value : stringify(value);
}

/**
 * A caller's JavaScript value as serde_json would read its `JSON.stringify`
 * text: integral numbers below 1e21 that fit u64/i64 are integers.
 */
export function fromJs(value: unknown): Json {
  if (value === null || value === undefined) return null;
  switch (typeof value) {
    case "boolean": case "string": return value;
    case "bigint": return value >= I64_MIN && value <= U64_MAX ? value : Number(value);
    case "number": {
      if (!Number.isFinite(value)) return null;
      if (Number.isInteger(value) && Math.abs(value) < 1e21) {
        const integer = BigInt(value);
        if (integer >= I64_MIN && integer <= U64_MAX) return integer;
      }
      return value;
    }
  }
  if (Array.isArray(value)) return value.map((item) => fromJs(item));
  if (value instanceof Map) return new Map([...value].map(([key, member]) => [String(key), fromJs(member)]));
  if (typeof value === "object") {
    const out: JsonObject = new Map();
    for (const [key, member] of Object.entries(value)) {
      if (member !== undefined && typeof member !== "function") out.set(key, fromJs(member));
    }
    return out;
  }
  return null;
}

/** Plain JavaScript for a value (integers as numbers when safe, else bigint). */
export function toJs(value: Json): unknown {
  if (typeof value === "bigint") return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value;
  if (Array.isArray(value)) return value.map(toJs);
  if (value instanceof Map) return Object.fromEntries([...value].map(([key, member]) => [key, toJs(member)]));
  return value;
}

/** Rust's `Ord` for `String` (UTF-8 bytes, i.e. code point order). */
export function compareText(left: string, right: string): number {
  if (left === right) return 0;
  const a = left[Symbol.iterator]();
  const b = right[Symbol.iterator]();
  for (;;) {
    const x = a.next();
    const y = b.next();
    if (x.done) return y.done ? 0 : -1;
    if (y.done) return 1;
    const cx = x.value.codePointAt(0)!;
    const cy = y.value.codePointAt(0)!;
    if (cx !== cy) return cx < cy ? -1 : 1;
  }
}

/** Lexicographic `Ord` of string tuples. */
export function compareTuples(left: readonly string[], right: readonly string[]): number {
  for (let index = 0; index < Math.min(left.length, right.length); index++) {
    const order = compareText(left[index]!, right[index]!);
    if (order !== 0) return order;
  }
  return left.length - right.length;
}

/** Rust's `str::trim` (Unicode White_Space). */
export function trimRust(text: string): string {
  return text.replace(/^\p{White_Space}+|\p{White_Space}+$/gu, "");
}

/** `str.chars().count()`. */
export const charCount = (text: string): number => [...text].length;

const NONPRINTABLE = /[\p{Cc}\p{Cf}\p{Co}\p{Cn}\p{Zl}\p{Zp}]/u;
const GRAPHEME_EXTEND = /\p{Grapheme_Extend}/u;

function escapeDebug(c: string, quote: "\"" | "'", first: boolean): string {
  switch (c) {
    case "\0": return "\\0";
    case "\t": return "\\t";
    case "\r": return "\\r";
    case "\n": return "\\n";
    case "\\": return "\\\\";
  }
  if (c === quote) return `\\${c}`;
  if (NONPRINTABLE.test(c) || (first && GRAPHEME_EXTEND.test(c))) return `\\u{${c.codePointAt(0)!.toString(16)}}`;
  return c;
}

/** Rust's `{:?}` of a `str` (approximates `char::is_printable` by Unicode category). */
export function debugStr(text: string): string {
  return `"${[...text].map((c, index) => escapeDebug(c, "\"", index === 0)).join("")}"`;
}

/** Rust's `{:?}` of a `char`. */
export function debugChar(c: string): string {
  return `'${escapeDebug(c, "'", true)}'`;
}
