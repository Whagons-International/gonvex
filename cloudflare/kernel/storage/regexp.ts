// REGEXP without a custom function.
//
// The Rust runtime registers sqlx's `regexp(pattern, value)` on every tenant
// connection (store/src/registry.rs `with_regexp`), and the SDK's `matches`
// data predicate compiles to `column REGEXP $n` (module-sdk data.ts). Durable
// Object SQLite has no custom functions, so the storage layer rewrites the
// operator into plain SQL with the same result:
//
//   - sqlx returns NULL when the value or the pattern is not TEXT (NULL,
//     INTEGER, REAL, BLOB); otherwise 1 or 0 from `Regex::is_match`;
//   - the pattern is compiled here, from the bound value, into GLOB, length()
//     and substr() predicates, which work on characters as Rust's regex does
//     on Unicode scalar values.
//
// It covers the portable subset the SDK admits (anchors, literals, `.`,
// bracket classes, quantifiers, plain groups) where the language is a fixed
// sequence with at most one repeated character class between fully anchored
// ends. That includes the one pattern Whagons uses,
// `^PR-[0-9]+$` (prefix PR, PO or GR; purchasing/common.ts:164). Any other
// pattern fails with a clear error instead of matching differently.

import { databaseMessage } from "./errors";
import { isSymbol, isWord, type Token } from "./lexer";
import type { Rewrite, StatementWriter } from "./parameters";

export class RegexpUnsupportedError extends Error {
  constructor(pattern: string, reason: string) {
    super(`REGEXP pattern ${JSON.stringify(pattern)} cannot run on Durable Object SQLite, which has no custom SQL functions: ${reason}. ` +
      "The storage layer translates anchored literals, character classes and at most one repeated class (for example ^PR-[0-9]+$).");
    this.name = "RegexpUnsupportedError";
  }
}

/** One character matcher: a literal, a class, or `.` (any character but \n). */
type Matcher =
  | { readonly kind: "literal"; readonly char: string }
  | { readonly kind: "class"; readonly negated: boolean; readonly items: readonly (readonly [string, string])[] }
  | { readonly kind: "any" };

interface Atom {
  readonly matcher: Matcher;
  readonly min: number;
  /** Infinity for an open repeat. */
  readonly max: number;
}

interface Parsed {
  readonly startAnchored: boolean;
  readonly endAnchored: boolean;
  readonly atoms: Atom[];
}

const ESCAPES: Readonly<Record<string, string>> = { n: "\n", t: "\t", r: "\r", f: "\f", v: "\v" };

function parse(pattern: string): Parsed {
  const chars = Array.from(pattern);
  let index = 0;
  const fail = (reason: string): never => { throw new RegexpUnsupportedError(pattern, reason); };
  if (pattern.includes("\0")) fail("it contains NUL");
  const escaped = (char: string | undefined): string => {
    if (char === undefined) return fail("it ends with a backslash");
    if (ESCAPES[char] !== undefined) return ESCAPES[char]!;
    if (/[A-Za-z0-9]/.test(char)) return fail(`the escape \\${char} is not supported`);
    return char;
  };
  let startAnchored = false;
  if (chars[0] === "^") { startAnchored = true; index = 1; }
  let endAnchored = false;
  const atoms: Atom[] = [];
  let groups = 0;
  while (index < chars.length) {
    const char = chars[index]!;
    let matcher: Matcher | undefined;
    if (char === "$") {
      if (index !== chars.length - 1) fail("`$` is only supported at the end");
      endAnchored = true;
      index++;
      continue;
    }
    if (char === "^") fail("`^` is only supported at the start");
    if (char === "|") fail("alternation `|` is not supported");
    if (char === "(") {
      if (chars[index + 1] === "?") fail("group flags are not supported");
      groups++;
      index++;
      continue;
    }
    if (char === ")") {
      if (groups === 0) fail("unbalanced `)`");
      groups--;
      index++;
      if (["?", "*", "+", "{"].includes(chars[index] ?? "")) fail("a quantified group is not supported");
      continue;
    }
    if (char === ".") { matcher = { kind: "any" }; index++; }
    else if (char === "\\") { matcher = { kind: "literal", char: escaped(chars[index + 1]) }; index += 2; }
    else if (char === "[") {
      index++;
      let negated = false;
      if (chars[index] === "^") { negated = true; index++; }
      if (chars[index] === "[" && chars[index + 1] === ":") fail("POSIX classes are not supported");
      const items: [string, string][] = [];
      let first = true;
      for (;;) {
        let item = chars[index];
        if (item === undefined) fail("unterminated `[`");
        if (item === "]" && !first) { index++; break; }
        if (item === "[") fail("nested classes are not supported");
        if (item === "\\") { item = escaped(chars[index + 1]); index += 2; } else index++;
        first = false;
        let high = item!;
        if (chars[index] === "-" && chars[index + 1] !== undefined && chars[index + 1] !== "]") {
          let next = chars[index + 1]!;
          index += 2;
          if (next === "\\") { next = escaped(chars[index]); index++; }
          high = next;
          if (high.codePointAt(0)! < item!.codePointAt(0)!) fail("a class range is reversed");
        }
        items.push([item!, high]);
      }
      matcher = { kind: "class", negated, items };
    } else if (char === "?" || char === "*" || char === "+" || char === "{") fail(`\`${char}\` has nothing to repeat`);
    else { matcher = { kind: "literal", char }; index++; } // `]` and `}` alone are literals in both engines
    // Quantifier.
    let min = 1;
    let max = 1;
    const quantifier = chars[index];
    if (quantifier === "?") { min = 0; max = 1; index++; }
    else if (quantifier === "*") { min = 0; max = Infinity; index++; }
    else if (quantifier === "+") { min = 1; max = Infinity; index++; }
    else if (quantifier === "{") {
      const close = chars.indexOf("}", index);
      const body = close < 0 ? "" : chars.slice(index + 1, close).join("");
      const range = /^(\d+)(,(\d*))?$/.exec(body);
      if (!range) fail("this `{` repetition is not supported");
      min = Number(range![1]);
      max = range![2] === undefined ? min : range![3] === "" ? Infinity : Number(range![3]);
      if (max < min) fail("a repetition range is reversed");
      index = close + 1;
    }
    if (chars[index] === "?" && quantifier !== undefined && "?*+{".includes(quantifier)) index++; // lazy: same match/no-match
    if (chars[index] === "?" || chars[index] === "*" || chars[index] === "+") fail("stacked quantifiers are not supported");
    atoms.push({ matcher: matcher!, min, max });
  }
  if (groups !== 0) fail("unbalanced `(`");
  return { startAnchored, endAnchored, atoms };
}

const NEWLINE = "\n";

/** A GLOB fragment matching exactly one character the matcher accepts. */
function globOne(matcher: Matcher): string {
  if (matcher.kind === "any") return `[^${NEWLINE}]`;
  if (matcher.kind === "literal") return "*?[".includes(matcher.char) ? `[${matcher.char}]` : matcher.char;
  return globClass(matcher.items, matcher.negated);
}

/** A GLOB bracket class: `]` first, `^` not first, `-` last. */
function globClass(items: readonly (readonly [string, string])[], negated: boolean): string {
  let close = false, caret = false, dash = false;
  let body = "";
  for (const [low, high] of items) {
    if (low === high) {
      if (low === "]") close = true;
      else if (low === "^") caret = true;
      else if (low === "-") dash = true;
      else body += low;
    } else {
      // A range endpoint that is special inside a GLOB class.
      if (low === "]" || low === "^" || high === "]") {
        for (let code = low.codePointAt(0)!; code <= high.codePointAt(0)!; code++) {
          const char = String.fromCodePoint(code);
          if (char === "]") close = true; else if (char === "^") caret = true; else if (char === "-") dash = true; else body += char;
        }
        continue;
      }
      body += `${low}-${high}`;
    }
  }
  return `[${negated ? "^" : ""}${close ? "]" : ""}${body}${caret ? "^" : ""}${dash ? "-" : ""}]`;
}

/** A GLOB class matching one character the matcher rejects. */
function globComplement(matcher: Matcher): string {
  if (matcher.kind === "any") return NEWLINE;
  if (matcher.kind === "literal") return globClass([[matcher.char, matcher.char]], true);
  return globClass(matcher.items, !matcher.negated);
}

const sqlString = (text: string) => `'${text.replaceAll("'", "''")}'`;

/** Expands fixed repeats and drops what unanchored ends make redundant. */
function normalize(parsed: Parsed, pattern: string): { prefix: Matcher[]; repeat?: Atom; suffix: Matcher[] } {
  const atoms = [...parsed.atoms];
  // At an unanchored end, a repeat can give back all but its minimum.
  if (!parsed.startAnchored) while (atoms.length && atoms[0]!.max !== atoms[0]!.min) atoms[0] = { ...atoms[0]!, max: atoms[0]!.min };
  if (!parsed.endAnchored) while (atoms.length && atoms.at(-1)!.max !== atoms.at(-1)!.min) atoms[atoms.length - 1] = { ...atoms.at(-1)!, max: atoms.at(-1)!.min };
  const prefix: Matcher[] = [];
  const suffix: Matcher[] = [];
  let repeat: Atom | undefined;
  for (const atom of atoms) {
    if (atom.min > 256) throw new RegexpUnsupportedError(pattern, "a repetition over 256 is not supported");
    if (atom.min === atom.max) {
      for (let count = 0; count < atom.min; count++) (repeat ? suffix : prefix).push(atom.matcher);
      continue;
    }
    if (repeat) throw new RegexpUnsupportedError(pattern, "more than one variable-length part is not supported");
    // A variable part needs both ends anchored to be checked without a scan.
    if (!parsed.startAnchored || !parsed.endAnchored) throw new RegexpUnsupportedError(pattern, "a variable-length part needs both ^ and $");
    repeat = atom;
  }
  return repeat ? { prefix, repeat, suffix } : { prefix, suffix };
}

/**
 * A SQL boolean expression over the TEXT `value` that is 1 exactly when
 * Rust's `Regex::new(pattern).is_match(value)` is true.
 */
export function regexpPredicate(pattern: string, value: string): string {
  const parsed = parse(pattern);
  const { prefix, repeat, suffix } = normalize(parsed, pattern);
  const head = prefix.map(globOne).join("");
  const tail = suffix.map(globOne).join("");
  if (!repeat) {
    const glob = `${parsed.startAnchored ? "" : "*"}${head}${tail}${parsed.endAnchored ? "" : "*"}`;
    return glob === "*" || glob === "**" ? "1" : `${value} GLOB ${sqlString(glob)}`;
  }
  const fixed = prefix.length + suffix.length;
  const parts: string[] = [];
  parts.push(`length(${value}) >= ${fixed + repeat.min}`);
  if (repeat.max !== Infinity) parts.push(`length(${value}) <= ${fixed + repeat.max}`);
  if (prefix.length || suffix.length) parts.push(`${value} GLOB ${sqlString(`${head}*${tail}`)}`);
  const middle = suffix.length
    ? `substr(${value}, ${prefix.length + 1}, length(${value}) - ${fixed})`
    : prefix.length ? `substr(${value}, ${prefix.length + 1})` : value;
  parts.push(`${middle} NOT GLOB ${sqlString(`*${globComplement(repeat.matcher)}*`)}`);
  return parts.join(" AND ");
}

/** Tokens that may precede a REGEXP left operand without binding tighter than REGEXP. */
const OPERAND_BEFORE = new Set(["and", "or", "not", "where", "on", "when", "then", "else", "case", "select", "having", "returning"]);
/** Words that may follow a REGEXP right operand. */
const OPERAND_AFTER = new Set(["and", "or", "then", "else", "end", "when", "order", "group", "limit", "having", "union", "intersect", "except", "window", "returning", "offset", "from", "where", "join", "inner", "left", "cross", "natural", "on", "using", "as"]);

export class RegexpRewriteError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RegexpRewriteError";
  }
}

/** Start index of the operand that ends at token `end`, or -1 when it is not a simple operand. */
function operandStart(tokens: readonly Token[], end: number): number {
  const last = tokens[end];
  if (!last) return -1;
  let start: number;
  if (isSymbol(last, ")")) {
    let depth = 0;
    start = -1;
    for (let index = end; index >= 0; index--) {
      const found = tokens[index]!;
      if (isSymbol(found, ")")) depth++;
      else if (isSymbol(found, "(") && --depth === 0) { start = index; break; }
    }
    if (start < 0) return -1;
    if (tokens[start - 1]?.type === "word" && !OPERAND_BEFORE.has(tokens[start - 1]!.lower)) start--; // function name
  } else if (last.type === "word" || last.type === "id") {
    start = end;
    if (isSymbol(tokens[end - 1], ".") && (tokens[end - 2]?.type === "word" || tokens[end - 2]?.type === "id")) start = end - 2;
  } else if (last.type === "string" || last.type === "param" || last.type === "number") start = end;
  else return -1;
  const before = tokens[start - 1];
  if (before === undefined || isSymbol(before, "(") || isSymbol(before, ",")) return start;
  if (before.type === "word" && OPERAND_BEFORE.has(before.lower)) return start;
  return -1;
}

/**
 * Rewrites every `x [NOT] REGEXP p` in the statement. `p` must be a bound
 * parameter or a string literal, so the pattern is known before the statement
 * runs. Returns the token rewrites, in order.
 */
export function regexpRewrites(tokens: readonly Token[], values: Pick<StatementWriter, "valueAt">): Rewrite[] {
  const rewrites: Rewrite[] = [];
  for (let index = 0; index < tokens.length; index++) {
    const found = tokens[index]!;
    if (found.type === "word" && found.lower === "regexp" && isSymbol(tokens[index + 1], "(")) {
      throw new RegexpRewriteError(databaseMessage(1, "no such function: regexp") + " (Durable Object SQLite has no custom functions; use `value REGEXP pattern`)");
    }
    if (!isWord(found, "regexp")) continue;
    const negated = isWord(tokens[index - 1], "not");
    const operandEnd = negated ? index - 2 : index - 1;
    const start = operandStart(tokens, operandEnd);
    const right = tokens[index + 1];
    const after = tokens[index + 2];
    const rightIsSimple = right !== undefined && (right.type === "param" || right.type === "string");
    const afterIsBoundary = after === undefined || isSymbol(after, ")") || isSymbol(after, ",") || isSymbol(after, ";") || (after.type === "word" && OPERAND_AFTER.has(after.lower));
    if (start < 0 || !rightIsSimple || !afterIsBoundary) {
      throw new RegexpRewriteError("REGEXP on Durable Object SQLite needs the form `operand [NOT] REGEXP $n` (or a string literal pattern) between AND/OR/WHERE-level boundaries");
    }
    if (rewrites.length && rewrites.at(-1)!.to >= start) throw new RegexpRewriteError("nested REGEXP operands are not supported on Durable Object SQLite");
    let pattern: string | undefined;
    if (right!.type === "string") pattern = right!.value;
    else {
      const value = values.valueAt(index + 1);
      pattern = value.kind === "text" ? value.value : undefined;
    }
    // Compile now so an unsupported pattern fails before anything runs.
    const compiled = pattern === undefined ? undefined : regexpPredicate(pattern, "\u0000");
    rewrites.push({
      from: start,
      to: index + 1,
      render(writer) {
        const operand = `(${writer.emit(start, operandEnd)})`;
        // sqlx: NULL unless both the pattern and the value are TEXT.
        const predicate = compiled === undefined ? "NULL" : `CASE WHEN typeof${operand} = 'text' THEN (${compiled.replaceAll("\u0000", operand)}) END`;
        // `x NOT REGEXP p`: the NOT sits between the operand and REGEXP, inside the replaced range.
        return negated ? `(NOT (${predicate}))` : `(${predicate})`;
      },
    });
  }
  return rewrites;
}
