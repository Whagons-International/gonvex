// SQL tokenizer for module statements. It follows SQLite's own tokenizer
// (tokenize.c) for everything that can hide a `;` or a parameter, with the
// rules of rust/crates/runtime/src/sql_admission.rs: string literals, quoted
// identifiers ("…", `…`, […]), blob literals, TCL-style variables
// ($name(…), $a::b), `--` comments ended only by \n, and non-nesting /* */
// comments. Comments and whitespace produce no tokens; everything else does,
// with its byte range in the source so callers can rewrite the text.

export type TokenType = "word" | "id" | "string" | "blob" | "param" | "number" | "symbol";

export interface Token {
  readonly type: TokenType;
  /** The exact source text of the token. */
  readonly text: string;
  /** Unquoted value for `id` and `string`, the text otherwise. */
  readonly value: string;
  /** ASCII-lowercased text for `word`, empty otherwise. */
  readonly lower: string;
  readonly start: number;
  readonly end: number;
}

export type LexErrorKind = "nul" | "unterminated";

export class SqlLexError extends Error {
  constructor(readonly kind: LexErrorKind) {
    super(kind === "nul"
      ? "a database statement may not contain NUL"
      : "a database statement contains an unterminated literal or comment");
    this.name = "SqlLexError";
  }
}

const SPACE = new Set([" ", "\t", "\n", "\f", "\r"]);

/** sql_admission.rs `id_char`: ASCII alphanumerics, `_`, `$` and any non-ASCII character. */
export function isIdChar(char: string | undefined): boolean {
  if (char === undefined) return false;
  const code = char.charCodeAt(0);
  return (code >= 48 && code <= 57) || (code >= 65 && code <= 90) || (code >= 97 && code <= 122)
    || code === 95 || code === 36 || code >= 0x80;
}

const isDigit = (char: string | undefined) => char !== undefined && char >= "0" && char <= "9";

export function asciiLower(text: string): string {
  return text.replace(/[A-Z]+/g, (upper) => upper.toLowerCase());
}

function token(type: TokenType, source: string, start: number, end: number, value?: string): Token {
  const text = source.slice(start, end);
  return { type, text, value: value ?? text, lower: type === "word" ? asciiLower(text) : "", start, end };
}

/** Index just past a quoted run that ends at `close`; a doubled `close` is an escape when `doubling`. */
function quoted(source: string, start: number, close: string, doubling: boolean): number {
  let index = start;
  while (index < source.length) {
    if (source[index] === close) {
      if (doubling && source[index + 1] === close) { index += 2; continue; }
      return index + 1;
    }
    index++;
  }
  throw new SqlLexError("unterminated");
}

function unquote(text: string, close: string, doubling: boolean): string {
  const inner = text.slice(1, -1);
  return doubling ? inner.replaceAll(close + close, close) : inner;
}

export function tokenize(source: string): Token[] {
  if (source.includes("\0")) throw new SqlLexError("nul");
  const tokens: Token[] = [];
  let index = 0;
  const length = source.length;
  while (index < length) {
    const char = source[index]!;
    if (SPACE.has(char)) { index++; continue; }
    if (char === "-" && source[index + 1] === "-") {
      const newline = source.indexOf("\n", index);
      index = newline < 0 ? length : newline + 1;
      continue;
    }
    if (char === "/" && source[index + 1] === "*") {
      const end = source.indexOf("*/", index + 2);
      if (end < 0) throw new SqlLexError("unterminated");
      index = end + 2;
      continue;
    }
    if (char === "'") {
      const end = quoted(source, index + 1, "'", true);
      tokens.push(token("string", source, index, end, unquote(source.slice(index, end), "'", true)));
      index = end;
      continue;
    }
    if (char === '"' || char === "`") {
      const end = quoted(source, index + 1, char, true);
      tokens.push(token("id", source, index, end, unquote(source.slice(index, end), char, true)));
      index = end;
      continue;
    }
    if (char === "[") {
      const end = quoted(source, index + 1, "]", false);
      tokens.push(token("id", source, index, end, source.slice(index + 1, end - 1)));
      index = end;
      continue;
    }
    if ((char === "x" || char === "X") && source[index + 1] === "'") {
      const end = quoted(source, index + 2, "'", false);
      tokens.push(token("blob", source, index, end));
      index = end;
      continue;
    }
    if (char === "?") {
      let end = index + 1;
      while (isDigit(source[end])) end++;
      tokens.push(token("param", source, index, end));
      index = end;
      continue;
    }
    if (char === "$" || char === "@" || char === ":" || char === "#") {
      // SQLite variables, including TCL-style `$a::b(any)` names.
      let end = index + 1;
      let named = 0;
      while (end < length) {
        const next = source[end]!;
        if (isIdChar(next)) { named++; end++; }
        else if (next === "(" && named > 0) {
          end++;
          while (end < length && !SPACE.has(source[end]!) && source[end] !== ")") end++;
          if (source[end] === ")") end++;
          break;
        } else if (next === ":" && source[end + 1] === ":") end += 2;
        else break;
      }
      if (end === index + 1) tokens.push(token("symbol", source, index, end));
      else tokens.push(token("param", source, index, end));
      index = end;
      continue;
    }
    if (isDigit(char) || (char === "." && isDigit(source[index + 1]))) {
      let end = index + 1;
      while (end < length) {
        const next = source[end]!;
        if ((next === "+" || next === "-") && (source[end - 1] === "e" || source[end - 1] === "E") && isDigit(source[end + 1]) && !/^0[xX]/.test(source.slice(index, end))) { end++; continue; }
        if (isIdChar(next) || next === ".") { end++; continue; }
        break;
      }
      tokens.push(token("number", source, index, end));
      index = end;
      continue;
    }
    if (isIdChar(char)) {
      let end = index + 1;
      while (end < length && isIdChar(source[end])) end++;
      tokens.push(token("word", source, index, end));
      index = end;
      continue;
    }
    // One UTF-16 code unit is enough here: everything >= 0x80 is an id char.
    tokens.push(token("symbol", source, index, index + 1));
    index++;
  }
  return tokens;
}

/** sql_admission.rs `word`: a lowercased word, "(" for an open parenthesis, "" otherwise. */
export function headWord(tokens: readonly Token[], index: number): string {
  const found = tokens[index];
  if (!found) return "";
  if (found.type === "word") return found.lower;
  if (found.type === "symbol" && found.text === "(") return "(";
  return "";
}

export const isSymbol = (found: Token | undefined, text: string): boolean =>
  found !== undefined && found.type === "symbol" && found.text === text;

export const isWord = (found: Token | undefined, lower: string): boolean =>
  found !== undefined && found.type === "word" && found.lower === lower;

/** Index of the `)` closing the `(` at `open`, or -1. */
export function closeParen(tokens: readonly Token[], open: number): number {
  if (!isSymbol(tokens[open], "(")) return -1;
  let depth = 0;
  for (let index = open; index < tokens.length; index++) {
    const found = tokens[index]!;
    if (found.type !== "symbol") continue;
    if (found.text === "(") depth++;
    else if (found.text === ")" && --depth === 0) return index;
  }
  return -1;
}

/** Splits `tokens` at top-level commas. */
export function splitTopLevel(tokens: readonly Token[], separator = ","): Token[][] {
  const parts: Token[][] = [];
  let current: Token[] = [];
  let depth = 0;
  for (const found of tokens) {
    if (found.type === "symbol") {
      if (found.text === "(") depth++;
      else if (found.text === ")") depth--;
      else if (depth === 0 && found.text === separator) { parts.push(current); current = []; continue; }
    }
    current.push(found);
  }
  parts.push(current);
  return parts;
}
