// reference_integrity/jsonpath.rs: the SQL/JSON path subset reference
// declarations use, evaluated in lax mode with PostgreSQL's semantics (array
// auto-unwrapping, existential and three-valued comparisons,
// `jsonb_path_match` for predicates).
//
// Supported: `$`, `@`, `$name` variables, `.key`, `."key"`, `.*`, `[*]`,
// `[n]`, `[n to m]`, `? (filter)`, `.type()`, `.size()`, `.keyvalue()`,
// `.string()`, literals, `== != <> < <= > >=`, `&&`, `||`, `!`,
// `exists(…)`, `starts with` and an optional `lax`/`strict` prefix.
//
// Parse errors are Rust's texts, including its `{:?}` renderings of tokens.

import { compareText, debugChar, debugStr, displayNumber, isNumber, parseJson, stringify, type Json, type JsonObject } from "./json.ts";

type CompareOp = "Eq" | "Ne" | "Lt" | "Le" | "Gt" | "Ge";

type Primary =
  | { kind: "root" }
  | { kind: "current" }
  | { kind: "variable"; name: string }
  | { kind: "literal"; value: Json }
  | { kind: "nested"; expr: Expr };

type Step =
  | { kind: "member"; name: string }
  | { kind: "anyMember" }
  | { kind: "anyElement" }
  | { kind: "elements"; ranges: [bigint, bigint | null][] }
  | { kind: "filter"; predicate: Expr }
  | { kind: "type" }
  | { kind: "size" }
  | { kind: "keyvalue" }
  | { kind: "string" };

type Expr =
  | { kind: "path"; primary: Primary; steps: Step[] }
  | { kind: "compare"; left: Expr; op: CompareOp; right: Expr }
  | { kind: "startsWith"; left: Expr; right: Expr }
  | { kind: "and"; left: Expr; right: Expr }
  | { kind: "or"; left: Expr; right: Expr }
  | { kind: "not"; inner: Expr }
  | { kind: "exists"; inner: Expr };

/** Three-valued predicate result. */
type Truth = "true" | "false" | "unknown";
const truthOf = (value: boolean): Truth => (value ? "true" : "false");

export type Vars = JsonObject | undefined;

/** `Err(())` of `Context::items`: a structural error lax mode suppresses. */
const STRUCTURAL = Symbol("structural");
type Items = Json[] | typeof STRUCTURAL;

export class JsonPath {
  private constructor(private readonly expr: Expr) {}

  /** Throws the Rust error string (`Err(String)`). */
  static parse(source: string): JsonPath {
    const parser = new Parser(tokenize(source));
    const first = parser.peek();
    if (first?.kind === "word" && (first.text === "lax" || first.text === "strict")) parser.index += 1;
    const expr = parser.expr();
    if (parser.index !== parser.tokens.length) throw new PathError(`unexpected input in JSON path ${debugStr(source)}`);
    return new JsonPath(expr);
  }

  /** Whether this path equals `JsonPath::parse("$")`. */
  isRoot(): boolean {
    return this.expr.kind === "path" && this.expr.primary.kind === "root" && this.expr.steps.length === 0;
  }

  /** The first member accessor applied to `$`, if the path starts `$.key`. */
  rootMember(): string | null {
    if (this.expr.kind !== "path" || this.expr.primary.kind !== "root") return null;
    const first = this.expr.steps[0];
    return first?.kind === "member" ? first.name : null;
  }

  /** `jsonb_path_query`: every selected item; structural errors select nothing. */
  query(document: Json, vars?: Vars): Json[] {
    const context = new Context(document, vars);
    if (this.expr.kind === "path") {
      const items = context.items(this.expr, undefined);
      return items === STRUCTURAL ? [] : items;
    }
    return [truthValue(context.truth(this.expr, undefined))];
  }

  queryFirst(document: Json, vars?: Vars): Json | undefined {
    return this.query(document, vars)[0];
  }

  /** `jsonb_path_match`: null when the result is unknown or not a single boolean. */
  matches(document: Json, vars?: Vars): boolean | null {
    const context = new Context(document, vars);
    if (this.expr.kind === "path") {
      const items = context.items(this.expr, undefined);
      if (items === STRUCTURAL || items.length !== 1 || typeof items[0] !== "boolean") return null;
      return items[0];
    }
    const truth = context.truth(this.expr, undefined);
    return truth === "unknown" ? null : truth === "true";
  }
}

/** A JSON path parse error; `message` is Rust's `Err(String)`. */
export class PathError extends Error {
  override readonly name = "PathError";
}

const truthValue = (truth: Truth): Json => (truth === "true" ? true : truth === "false" ? false : null);

class Context {
  constructor(private readonly root: Json, private readonly vars: Vars) {}

  items(expr: Expr, current: Json | undefined): Items {
    if (expr.kind !== "path") return [truthValue(this.truth(expr, current))];
    let items: Json[];
    const primary = expr.primary;
    switch (primary.kind) {
      case "root": items = [this.root]; break;
      case "current":
        if (current === undefined) return STRUCTURAL;
        items = [current];
        break;
      case "variable": {
        const value = this.vars?.get(primary.name);
        if (value === undefined) return STRUCTURAL;
        items = [value];
        break;
      }
      case "literal": items = [primary.value]; break;
      case "nested": {
        const inner = this.items(primary.expr, current);
        if (inner === STRUCTURAL) return STRUCTURAL;
        items = inner;
        break;
      }
    }
    for (const step of expr.steps) {
      const next = this.step(step, items);
      if (next === STRUCTURAL) return STRUCTURAL;
      items = next;
    }
    return items;
  }

  private step(step: Step, items: Json[]): Items {
    const output: Json[] = [];
    switch (step.kind) {
      case "member":
        for (const item of unwrap(items)) {
          if (item instanceof Map && item.has(step.name)) output.push(item.get(step.name)!);
        }
        break;
      case "anyMember":
        for (const item of unwrap(items)) {
          if (item instanceof Map) output.push(...item.values());
        }
        break;
      case "anyElement":
        for (const item of items) {
          if (Array.isArray(item)) output.push(...item);
          else output.push(item);
        }
        break;
      case "elements":
        for (const item of items) {
          const elements = Array.isArray(item) ? item : [item];
          const length = BigInt(elements.length);
          for (const [start, end] of step.ranges) {
            const last = end ?? start;
            const from = start > 0n ? start : 0n;
            const to = last < length - 1n ? last : length - 1n;
            for (let index = from; index <= to; index++) output.push(elements[Number(index)]!);
          }
        }
        break;
      case "filter":
        for (const item of unwrap(items)) {
          if (this.truth(step.predicate, item) === "true") output.push(item);
        }
        break;
      case "type":
        for (const item of items) output.push(typeName(item));
        break;
      case "size":
        for (const item of items) output.push(BigInt(Array.isArray(item) ? item.length : 1));
        break;
      case "keyvalue":
        for (const item of unwrap(items)) {
          if (!(item instanceof Map)) return STRUCTURAL;
          for (const [key, value] of item) output.push(new Map<string, Json>([["key", key], ["value", value]]));
        }
        break;
      case "string":
        for (const item of unwrap(items)) {
          if (typeof item === "string") output.push(item);
          else if (isNumber(item)) output.push(displayNumber(item));
          else if (typeof item === "boolean") output.push(item ? "true" : "false");
          else return STRUCTURAL;
        }
        break;
    }
    return output;
  }

  truth(expr: Expr, current: Json | undefined): Truth {
    switch (expr.kind) {
      case "and": {
        const left = this.truth(expr.left, current);
        if (left === "false") return "false";
        const right = this.truth(expr.right, current);
        if (right === "false") return "false";
        return left === "true" && right === "true" ? "true" : "unknown";
      }
      case "or": {
        const left = this.truth(expr.left, current);
        if (left === "true") return "true";
        const right = this.truth(expr.right, current);
        if (right === "true") return "true";
        return left === "false" && right === "false" ? "false" : "unknown";
      }
      case "not": {
        const inner = this.truth(expr.inner, current);
        return inner === "true" ? "false" : inner === "false" ? "true" : "unknown";
      }
      case "exists": {
        const items = this.items(expr.inner, current);
        return items === STRUCTURAL ? "unknown" : truthOf(items.length > 0);
      }
      case "compare": {
        const left = this.items(expr.left, current);
        const right = this.items(expr.right, current);
        if (left === STRUCTURAL || right === STRUCTURAL) return "unknown";
        const rights = unwrap(right);
        let unknown = false;
        for (const l of unwrap(left)) {
          for (const r of rights) {
            const result = compare(l, expr.op, r);
            if (result === "true") return "true";
            if (result === "unknown") unknown = true;
          }
        }
        return unknown ? "unknown" : "false";
      }
      case "startsWith": {
        const left = this.items(expr.left, current);
        const right = this.items(expr.right, current);
        if (left === STRUCTURAL || right === STRUCTURAL) return "unknown";
        const prefix = right[0];
        if (right.length !== 1 || typeof prefix !== "string") return "unknown";
        let unknown = false;
        for (const item of unwrap(left)) {
          if (typeof item === "string") {
            if (item.startsWith(prefix)) return "true";
          } else {
            unknown = true;
          }
        }
        return unknown ? "unknown" : "false";
      }
      case "path": {
        const items = this.items(expr, current);
        if (items === STRUCTURAL || items.length !== 1 || typeof items[0] !== "boolean") return "unknown";
        return truthOf(items[0]);
      }
    }
  }
}

function unwrap(items: Json[]): Json[] {
  const output: Json[] = [];
  for (const item of items) {
    if (Array.isArray(item)) output.push(...item);
    else output.push(item);
  }
  return output;
}

function typeName(value: Json): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return "boolean";
  if (isNumber(value)) return "number";
  if (typeof value === "string") return "string";
  return Array.isArray(value) ? "array" : "object";
}

function compare(left: Json, op: CompareOp, right: Json): Truth {
  let ordering: number | null;
  if (left === null && right === null) ordering = 0;
  else if (left === null || right === null) return truthOf(op === "Ne");
  else if (typeof left === "boolean" && typeof right === "boolean") ordering = Number(left) - Number(right);
  else if (isNumber(left) && isNumber(right)) {
    const a = Number(left);
    const b = Number(right);
    ordering = a < b ? -1 : a > b ? 1 : a === b ? 0 : null;
  } else if (typeof left === "string" && typeof right === "string") ordering = compareText(left, right);
  else ordering = null;
  if (ordering === null) return "unknown";
  switch (op) {
    case "Eq": return truthOf(ordering === 0);
    case "Ne": return truthOf(ordering !== 0);
    case "Lt": return truthOf(ordering < 0);
    case "Le": return truthOf(ordering <= 0);
    case "Gt": return truthOf(ordering > 0);
    case "Ge": return truthOf(ordering >= 0);
  }
}

type Token =
  | { kind: "Dollar" } | { kind: "At" } | { kind: "variable"; text: string } | { kind: "Dot" } | { kind: "Star" }
  | { kind: "OpenBracket" } | { kind: "CloseBracket" } | { kind: "OpenParen" } | { kind: "CloseParen" }
  | { kind: "Question" } | { kind: "Comma" } | { kind: "Minus" } | { kind: "op"; op: CompareOp }
  | { kind: "And" } | { kind: "Or" } | { kind: "Bang" } | { kind: "str"; text: string }
  | { kind: "number"; value: Json } | { kind: "word"; text: string };

type SimpleKind = Exclude<Token["kind"], "variable" | "op" | "str" | "number" | "word">;
const simple = (kind: SimpleKind): Token => ({ kind }) as Token;

/** Rust's `{:?}` of a `Value` (only numbers reach it here). */
function debugValue(value: Json): string {
  if (isNumber(value)) return `Number(${displayNumber(value)})`;
  if (value === null) return "Null";
  if (typeof value === "boolean") return `Bool(${value})`;
  if (typeof value === "string") return `String(${debugStr(value)})`;
  return stringify(value);
}

/** Rust's `{:?}` of a `Token`. */
function debugToken(token: Token): string {
  switch (token.kind) {
    case "variable": return `Variable(${debugStr(token.text)})`;
    case "op": return `Op(${token.op})`;
    case "str": return `Str(${debugStr(token.text)})`;
    case "number": return `Number(${debugValue(token.value)})`;
    case "word": return `Word(${debugStr(token.text)})`;
    default: return token.kind;
  }
}

/** Rust's `{:?}` of an `Option<Token>`. */
const debugOption = (token: Token | undefined): string => (token === undefined ? "None" : `Some(${debugToken(token)})`);

const sameToken = (a: Token | undefined, b: Token): boolean => a !== undefined && debugToken(a) === debugToken(b);

const WHITESPACE = /^\p{White_Space}$/u;
const IDENTIFIER = /^[\p{Alphabetic}\p{N}_]$/u;
const isIdentifier = (c: string | undefined): boolean => c !== undefined && IDENTIFIER.test(c);
const isDigit = (c: string | undefined): boolean => c !== undefined && c >= "0" && c <= "9";

function tokenize(source: string): Token[] {
  const chars = [...source];
  const tokens: Token[] = [];
  let index = 0;
  while (index < chars.length) {
    const c = chars[index]!;
    const next = chars[index + 1];
    if (WHITESPACE.test(c)) { index += 1; continue; }
    switch (c) {
      case "$": {
        const start = index + 1;
        let end = start;
        while (end < chars.length && isIdentifier(chars[end])) end += 1;
        tokens.push(end === start ? simple("Dollar") : { kind: "variable", text: chars.slice(start, end).join("") });
        index = end;
        continue;
      }
      case "@": tokens.push(simple("At")); index += 1; continue;
      case ".": tokens.push(simple("Dot")); index += 1; continue;
      case "*": tokens.push(simple("Star")); index += 1; continue;
      case "[": tokens.push(simple("OpenBracket")); index += 1; continue;
      case "]": tokens.push(simple("CloseBracket")); index += 1; continue;
      case "(": tokens.push(simple("OpenParen")); index += 1; continue;
      case ")": tokens.push(simple("CloseParen")); index += 1; continue;
      case "?": tokens.push(simple("Question")); index += 1; continue;
      case ",": tokens.push(simple("Comma")); index += 1; continue;
      case "-": tokens.push(simple("Minus")); index += 1; continue;
    }
    if (c === "=" && next === "=") { tokens.push({ kind: "op", op: "Eq" }); index += 2; continue; }
    if (c === "!" && next === "=") { tokens.push({ kind: "op", op: "Ne" }); index += 2; continue; }
    if (c === "!") { tokens.push(simple("Bang")); index += 1; continue; }
    if (c === "<" && next === ">") { tokens.push({ kind: "op", op: "Ne" }); index += 2; continue; }
    if (c === "<" && next === "=") { tokens.push({ kind: "op", op: "Le" }); index += 2; continue; }
    if (c === "<") { tokens.push({ kind: "op", op: "Lt" }); index += 1; continue; }
    if (c === ">" && next === "=") { tokens.push({ kind: "op", op: "Ge" }); index += 2; continue; }
    if (c === ">") { tokens.push({ kind: "op", op: "Gt" }); index += 1; continue; }
    if (c === "&" && next === "&") { tokens.push(simple("And")); index += 2; continue; }
    if (c === "|" && next === "|") { tokens.push(simple("Or")); index += 2; continue; }
    if (c === "\"") {
      let text = "";
      index += 1;
      for (;;) {
        const d = chars[index];
        if (d === undefined) throw new PathError(`unterminated string in JSON path ${debugStr(source)}`);
        index += 1;
        if (d === "\"") break;
        if (d !== "\\") { text += d; continue; }
        const escaped = chars[index];
        if (escaped === undefined) throw new PathError(`unterminated escape in JSON path ${debugStr(source)}`);
        index += 1;
        switch (escaped) {
          case "n": text += "\n"; break;
          case "t": text += "\t"; break;
          case "r": text += "\r"; break;
          case "b": text += "\u{8}"; break;
          case "f": text += "\u{c}"; break;
          case "u": {
            const hex = index + 4 <= chars.length ? chars.slice(index, index + 4).join("") : "";
            index += 4;
            // u32::from_str_radix(hex, 16) then char::from_u32 (no surrogates).
            const code = /^\+?[0-9a-fA-F]+$/.test(hex) ? parseInt(hex, 16) : Number.NaN;
            if (Number.isNaN(code) || (code >= 0xd800 && code <= 0xdfff)) throw new PathError(`invalid escape in JSON path ${debugStr(source)}`);
            text += String.fromCodePoint(code);
            break;
          }
          default: text += escaped;
        }
      }
      tokens.push({ kind: "str", text });
      continue;
    }
    if (isDigit(c)) {
      const start = index;
      while (
        index < chars.length &&
        (isDigit(chars[index]) || (chars[index] === "." && isDigit(chars[index + 1])) || chars[index] === "e" || chars[index] === "E")
      ) {
        if ((chars[index] === "e" || chars[index] === "E") && (chars[index + 1] === "+" || chars[index + 1] === "-")) index += 1;
        index += 1;
      }
      const text = chars.slice(start, index).join("");
      let value: Json;
      try {
        value = parseJson(text);
      } catch {
        throw new PathError(`invalid number ${debugStr(text)} in JSON path`);
      }
      tokens.push({ kind: "number", value });
      continue;
    }
    if (isIdentifier(c)) {
      const start = index;
      while (index < chars.length && isIdentifier(chars[index])) index += 1;
      tokens.push({ kind: "word", text: chars.slice(start, index).join("") });
      continue;
    }
    throw new PathError(`unexpected ${debugChar(c)} in JSON path ${debugStr(source)}`);
  }
  return tokens;
}

class Parser {
  index = 0;
  constructor(readonly tokens: Token[]) {}

  peek(): Token | undefined {
    return this.tokens[this.index];
  }

  private next(): Token | undefined {
    const token = this.tokens[this.index];
    if (token !== undefined) this.index += 1;
    return token;
  }

  private isWord(token: Token | undefined, word: string): boolean {
    return token?.kind === "word" && token.text === word;
  }

  private expect(expected: Token): void {
    const token = this.next();
    if (!sameToken(token, expected)) throw new PathError(`expected ${debugToken(expected)} in JSON path, found ${debugOption(token)}`);
  }

  expr(): Expr {
    let left = this.and();
    while (this.peek()?.kind === "Or") {
      this.index += 1;
      left = { kind: "or", left, right: this.and() };
    }
    return left;
  }

  private and(): Expr {
    let left = this.unary();
    while (this.peek()?.kind === "And") {
      this.index += 1;
      left = { kind: "and", left, right: this.unary() };
    }
    return left;
  }

  private unary(): Expr {
    if (this.peek()?.kind === "Bang") {
      this.index += 1;
      return { kind: "not", inner: this.unary() };
    }
    if (this.isWord(this.peek(), "exists") && this.tokens[this.index + 1]?.kind === "OpenParen") {
      this.index += 2;
      const inner = this.expr();
      this.expect(simple("CloseParen"));
      return { kind: "exists", inner };
    }
    const left = this.value();
    const token = this.peek();
    if (token?.kind === "op") {
      this.index += 1;
      return { kind: "compare", left, op: token.op, right: this.value() };
    }
    if (this.isWord(token, "starts")) {
      this.index += 1;
      const word = this.next();
      if (!this.isWord(word, "with")) throw new PathError(`expected \`with\`, found ${debugOption(word)}`);
      return { kind: "startsWith", left, right: this.value() };
    }
    return left;
  }

  private value(): Expr {
    const token = this.next();
    let primary: Primary;
    switch (token?.kind) {
      case "Dollar": primary = { kind: "root" }; break;
      case "At": primary = { kind: "current" }; break;
      case "variable": primary = { kind: "variable", name: token.text }; break;
      case "str": primary = { kind: "literal", value: token.text }; break;
      case "number": primary = { kind: "literal", value: token.value }; break;
      case "Minus": {
        const number = this.next();
        if (number?.kind !== "number" || !isNumber(number.value)) throw new PathError(`expected a number after \`-\`, found ${debugOption(number)}`);
        primary = { kind: "literal", value: parseJson(`-${displayNumber(number.value)}`) };
        break;
      }
      case "word":
        switch (token.text) {
          case "true": primary = { kind: "literal", value: true }; break;
          case "false": primary = { kind: "literal", value: false }; break;
          case "null": primary = { kind: "literal", value: null }; break;
          default: throw new PathError(`unexpected ${debugStr(token.text)} in JSON path`);
        }
        break;
      case "OpenParen": {
        const inner = this.expr();
        this.expect(simple("CloseParen"));
        primary = { kind: "nested", expr: inner };
        break;
      }
      default: throw new PathError(`unexpected ${debugOption(token)} in JSON path`);
    }
    const steps: Step[] = [];
    for (;;) {
      const peeked = this.peek();
      if (peeked?.kind === "Dot") {
        this.index += 1;
        const member = this.next();
        if (member?.kind === "Star") steps.push({ kind: "anyMember" });
        else if (member?.kind === "str") steps.push({ kind: "member", name: member.text });
        else if (member?.kind === "word") {
          if (this.peek()?.kind === "OpenParen") {
            this.index += 1;
            this.expect(simple("CloseParen"));
            switch (member.text) {
              case "type": steps.push({ kind: "type" }); break;
              case "size": steps.push({ kind: "size" }); break;
              case "keyvalue": steps.push({ kind: "keyvalue" }); break;
              case "string": steps.push({ kind: "string" }); break;
              default: throw new PathError(`unsupported JSON path method ${member.text}()`);
            }
          } else {
            steps.push({ kind: "member", name: member.text });
          }
        } else {
          throw new PathError(`unexpected ${debugOption(member)} after \`.\``);
        }
      } else if (peeked?.kind === "OpenBracket") {
        this.index += 1;
        if (this.peek()?.kind === "Star") {
          this.index += 1;
          this.expect(simple("CloseBracket"));
          steps.push({ kind: "anyElement" });
          continue;
        }
        const ranges: [bigint, bigint | null][] = [];
        for (;;) {
          const start = this.indexValue();
          let end: bigint | null = null;
          if (this.isWord(this.peek(), "to")) {
            this.index += 1;
            end = this.indexValue();
          }
          ranges.push([start, end]);
          const separator = this.next();
          if (separator?.kind === "Comma") continue;
          if (separator?.kind === "CloseBracket") break;
          throw new PathError(`unexpected ${debugOption(separator)} in subscript`);
        }
        steps.push({ kind: "elements", ranges });
      } else if (peeked?.kind === "Question") {
        this.index += 1;
        this.expect(simple("OpenParen"));
        const predicate = this.expr();
        this.expect(simple("CloseParen"));
        steps.push({ kind: "filter", predicate });
      } else {
        break;
      }
    }
    return { kind: "path", primary, steps };
  }

  private indexValue(): bigint {
    const token = this.next();
    if (token?.kind === "number" && isNumber(token.value)) {
      // Number::as_i64: integers within i64 only.
      const value = token.value;
      if (typeof value === "bigint" && value <= 9223372036854775807n) return value;
      throw new PathError(`invalid subscript ${displayNumber(value)}`);
    }
    throw new PathError(`expected a subscript, found ${debugOption(token)}`);
  }
}
