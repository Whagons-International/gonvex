// SQLite lexer for the schema transform: tokens with offsets, statement
// splitting (a CREATE TRIGGER ends at `; END ;`, as in sqlite3_complete),
// identifier quoting, and small parsers for the statement shapes the
// transform rewrites. Pure functions; no SQLite needed.

export type TokenType = "space" | "string" | "ident" | "word" | "(" | ")" | "," | ";" | "op";

export interface Token {
  type: TokenType;
  start: number;
  end: number;
  /** Upper-cased text, for `word` tokens only. */
  upper?: string;
}

const WORD = /[A-Za-z_\u0080-￿][A-Za-z0-9_$\u0080-￿]*/y;
const SPACE = /\s/;
const OP_STOP = /[\s'"`[(),;A-Za-z_\-/\u0080-￿]/;

export function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  const n = sql.length;
  let i = 0;
  while (i < n) {
    const c = sql[i]!;
    const start = i;
    if (SPACE.test(c)) {
      while (i < n && SPACE.test(sql[i]!)) i += 1;
      tokens.push({ type: "space", start, end: i });
    } else if (c === "-" && sql[i + 1] === "-") {
      while (i < n && sql[i] !== "\n") i += 1;
      tokens.push({ type: "space", start, end: i });
    } else if (c === "/" && sql[i + 1] === "*") {
      const close = sql.indexOf("*/", i + 2);
      i = close < 0 ? n : close + 2;
      tokens.push({ type: "space", start, end: i });
    } else if (c === "'" || c === '"' || c === "`") {
      i += 1;
      while (i < n) {
        if (sql[i] === c) {
          if (sql[i + 1] === c) i += 2;
          else {
            i += 1;
            break;
          }
        } else i += 1;
      }
      tokens.push({ type: c === "'" ? "string" : "ident", start, end: i });
    } else if (c === "[") {
      const close = sql.indexOf("]", i);
      i = close < 0 ? n : close + 1;
      tokens.push({ type: "ident", start, end: i });
    } else if (c === "(" || c === ")" || c === "," || c === ";") {
      i += 1;
      tokens.push({ type: c, start, end: i });
    } else {
      WORD.lastIndex = i;
      const m = WORD.exec(sql);
      if (m) {
        i += m[0].length;
        tokens.push({ type: "word", start, end: i, upper: m[0].toUpperCase() });
      } else {
        i += 1;
        while (i < n && !OP_STOP.test(sql[i]!)) i += 1;
        tokens.push({ type: "op", start, end: i });
      }
    }
  }
  return tokens;
}

/** Tokens without whitespace and comments. */
export const significant = (sql: string): Token[] => tokenize(sql).filter((t) => t.type !== "space");

export const text = (sql: string, t: Token): string => sql.slice(t.start, t.end);

export function unquote(name: string): string {
  if (name.startsWith('"') || name.startsWith("`")) return name.slice(1, -1).replace(name[0] === '"' ? /""/g : /``/g, name[0]!);
  if (name.startsWith("[")) return name.slice(1, -1);
  return name;
}

export const quoteIdent = (name: string): string => `"${name.replace(/"/g, '""')}"`;
export const quoteString = (value: string): string => `'${value.replace(/'/g, "''")}'`;

/** SQLite compares identifiers case-insensitively for ASCII letters. */
export const sameName = (a: string, b: string): boolean => a.toLowerCase() === b.toLowerCase();

export const isNameToken = (t: Token | undefined): t is Token => !!t && (t.type === "word" || t.type === "ident");

/** Splits a script into statements (without the trailing `;`), skipping blanks. */
export function splitStatements(sql: string): string[] {
  const out: string[] = [];
  let start = 0;
  let words: string[] = [];
  let last: string | null = null;
  let beforeLast: string | null = null;
  const isTrigger = () =>
    words[0] === "CREATE" && (words[1] === "TRIGGER" || (words[2] === "TRIGGER" && (words[1] === "TEMP" || words[1] === "TEMPORARY")));
  for (const token of tokenize(sql)) {
    if (token.type === "space") continue;
    if (token.type === ";") {
      if (!isTrigger() || (last === "END" && beforeLast === ";")) {
        const statement = sql.slice(start, token.start).trim();
        if (statement && significant(statement).length) out.push(statement);
        start = token.end;
        words = [];
        last = null;
        beforeLast = null;
        continue;
      }
      beforeLast = last;
      last = ";";
      continue;
    }
    const word = token.type === "word" ? token.upper! : token.type;
    if (words.length < 6) words.push(word);
    beforeLast = last;
    last = word;
  }
  const tail = sql.slice(start).trim();
  if (tail && significant(tail).length) out.push(tail);
  return out;
}

/** Removes leading comments and whitespace. */
export function stripLeadingComments(sql: string): string {
  const first = tokenize(sql).find((t) => t.type !== "space");
  return first ? sql.slice(first.start) : "";
}

export type StatementKind =
  | { kind: "create-table"; name: string; temp: boolean; virtual: boolean; ifNotExists: boolean }
  | { kind: "create-view"; name: string; temp: boolean; ifNotExists: boolean }
  | { kind: "create-index"; name: string; table: string; unique: boolean; ifNotExists: boolean }
  | { kind: "create-trigger"; name: string; table: string; temp: boolean; ifNotExists: boolean }
  | { kind: "drop"; object: "table" | "view" | "index" | "trigger"; name: string; ifExists: boolean }
  | { kind: "alter-rename"; table: string; to: string }
  | { kind: "alter-rename-column"; table: string; from: string; to: string }
  | { kind: "alter-add-column"; table: string }
  | { kind: "alter-drop-column"; table: string; column: string }
  | { kind: "other"; verb: string };

/** Reads a possibly schema-qualified name at tokens[i]; returns name and next index. */
function readName(sql: string, toks: Token[], i: number): { name: string; schema: string | null; next: number } {
  const first = toks[i];
  if (!isNameToken(first)) throw new Error(`expected a name at offset ${toks[i]?.start ?? sql.length}`);
  const dot = toks[i + 1];
  if (dot && dot.type === "op" && text(sql, dot) === "." && isNameToken(toks[i + 2])) {
    return { schema: unquote(text(sql, first)), name: unquote(text(sql, toks[i + 2]!)), next: i + 3 };
  }
  return { schema: null, name: unquote(text(sql, first)), next: i + 1 };
}

/** Classifies one statement by its head. Unknown statements are `other`. */
export function classify(statement: string): StatementKind {
  const sql = statement;
  const toks = significant(sql);
  const w = (k: number) => toks[k]?.upper;
  let i = 0;
  if (w(0) === "CREATE") {
    i = 1;
    let temp = false;
    if (w(i) === "TEMP" || w(i) === "TEMPORARY") {
      temp = true;
      i += 1;
    }
    let unique = false;
    if (w(i) === "UNIQUE") {
      unique = true;
      i += 1;
    }
    let virtual = false;
    if (w(i) === "VIRTUAL") {
      virtual = true;
      i += 1;
    }
    const object = w(i);
    i += 1;
    let ifNotExists = false;
    if (w(i) === "IF" && w(i + 1) === "NOT" && w(i + 2) === "EXISTS") {
      ifNotExists = true;
      i += 3;
    }
    const read = readName(sql, toks, i);
    if (read.schema && /^temp/i.test(read.schema)) temp = true;
    if (object === "TABLE") return { kind: "create-table", name: read.name, temp, virtual, ifNotExists };
    if (object === "VIEW") return { kind: "create-view", name: read.name, temp, ifNotExists };
    if (object === "INDEX") {
      let k = read.next;
      while (toks[k] && toks[k]!.upper !== "ON") k += 1;
      return { kind: "create-index", name: read.name, table: readName(sql, toks, k + 1).name, unique, ifNotExists };
    }
    if (object === "TRIGGER") {
      let k = read.next;
      let depth = 0;
      while (toks[k] && !(depth === 0 && toks[k]!.upper === "ON")) {
        if (toks[k]!.type === "(") depth += 1;
        else if (toks[k]!.type === ")") depth -= 1;
        k += 1;
      }
      return { kind: "create-trigger", name: read.name, table: readName(sql, toks, k + 1).name, temp, ifNotExists };
    }
    return { kind: "other", verb: `CREATE ${object ?? ""}`.trim() };
  }
  if (w(0) === "DROP") {
    const object = w(1);
    i = 2;
    let ifExists = false;
    if (w(i) === "IF" && w(i + 1) === "EXISTS") {
      ifExists = true;
      i += 2;
    }
    if (object === "TABLE" || object === "VIEW" || object === "INDEX" || object === "TRIGGER") {
      return { kind: "drop", object: object.toLowerCase() as "table", name: readName(sql, toks, i).name, ifExists };
    }
    return { kind: "other", verb: `DROP ${object ?? ""}`.trim() };
  }
  if (w(0) === "ALTER" && w(1) === "TABLE") {
    const read = readName(sql, toks, 2);
    i = read.next;
    if (w(i) === "RENAME" && w(i + 1) === "TO") return { kind: "alter-rename", table: read.name, to: readName(sql, toks, i + 2).name };
    if (w(i) === "RENAME") {
      let k = i + 1;
      if (w(k) === "COLUMN") k += 1;
      const from = readName(sql, toks, k);
      return { kind: "alter-rename-column", table: read.name, from: from.name, to: readName(sql, toks, from.next + 1).name };
    }
    if (w(i) === "ADD") return { kind: "alter-add-column", table: read.name };
    if (w(i) === "DROP") {
      let k = i + 1;
      if (w(k) === "COLUMN") k += 1;
      return { kind: "alter-drop-column", table: read.name, column: readName(sql, toks, k).name };
    }
    return { kind: "other", verb: "ALTER TABLE" };
  }
  return { kind: "other", verb: w(0) ?? (toks[0] ? text(sql, toks[0]) : "") };
}

/** A CREATE TRIGGER statement cut into its head, WHEN clause and body statements. */
export interface TriggerParts {
  /** Text up to (not including) WHEN or BEGIN. */
  head: string;
  /** WHEN expression text, or null. */
  when: string | null;
  /** Body statements, without trailing `;`. */
  body: string[];
}

export function parseTrigger(sql: string): TriggerParts {
  const toks = significant(sql);
  let depth = 0;
  let whenAt = -1;
  let beginAt = -1;
  for (let k = 0; k < toks.length; k += 1) {
    const t = toks[k]!;
    if (t.type === "(") depth += 1;
    else if (t.type === ")") depth -= 1;
    else if (depth === 0 && t.upper === "WHEN" && whenAt < 0 && beginAt < 0) {
      // CASE ... WHEN cannot appear in a trigger head before the WHEN clause.
      whenAt = k;
    } else if (depth === 0 && t.upper === "BEGIN") {
      beginAt = k;
      break;
    }
  }
  const last = toks[toks.length - 1];
  if (beginAt < 0 || !last || last.upper !== "END") throw new Error("not a CREATE TRIGGER ... BEGIN ... END statement");
  const headEnd = whenAt >= 0 ? toks[whenAt]!.start : toks[beginAt]!.start;
  const head = sql.slice(0, headEnd).trimEnd();
  const when = whenAt >= 0 ? sql.slice(toks[whenAt]!.end, toks[beginAt]!.start).trim() : null;
  const bodyText = sql.slice(toks[beginAt]!.end, last.start);
  const body: string[] = [];
  let from = 0;
  depth = 0;
  for (const t of tokenize(bodyText)) {
    if (t.type === "(") depth += 1;
    else if (t.type === ")") depth -= 1;
    else if (t.type === ";" && depth === 0) {
      const s = bodyText.slice(from, t.start).trim();
      if (s) body.push(s);
      from = t.end;
    }
  }
  const tail = bodyText.slice(from).trim();
  if (tail && significant(tail).length) body.push(tail);
  return { head, when, body };
}

export function buildTrigger(parts: TriggerParts): string {
  const when = parts.when === null ? "" : ` WHEN ${parts.when}`;
  return `${parts.head}${when} BEGIN\n${parts.body.map((s) => `  ${s};`).join("\n")}\nEND`;
}

/**
 * Relation names a statement mentions: every name token that is not a column
 * qualifier's member (after ".") and not a function name (before "("). It
 * over-approximates (column names come back too); callers intersect it with a
 * catalog of known relations.
 */
export function mentionedNames(sql: string): Set<string> {
  const toks = significant(sql);
  const out = new Set<string>();
  for (let k = 0; k < toks.length; k += 1) {
    const t = toks[k]!;
    if (!isNameToken(t)) continue;
    const prev = toks[k - 1];
    if (prev && prev.type === "op" && text(sql, prev) === ".") continue;
    out.add(unquote(text(sql, t)).toLowerCase());
  }
  return out;
}

/** Replaces relation names (name tokens not after ".") using `map` (keys lower-case). */
export function renameRelations(sql: string, map: Map<string, string>): string {
  if (!map.size) return sql;
  let out = "";
  let last = 0;
  let prev: Token | null = null;
  for (const t of tokenize(sql)) {
    if (isNameToken(t) && !(prev && prev.type === "op" && text(sql, prev).endsWith("."))) {
      const target = map.get(unquote(text(sql, t)).toLowerCase());
      if (target !== undefined) {
        out += sql.slice(last, t.start) + quoteIdent(target);
        last = t.end;
      }
    }
    if (t.type !== "space") prev = t;
  }
  return out + sql.slice(last);
}

/** Index of the token closing the parenthesis opened at toks[open]. */
export function closingParen(toks: Token[], open: number): number {
  let depth = 0;
  for (let k = open; k < toks.length; k += 1) {
    if (toks[k]!.type === "(") depth += 1;
    else if (toks[k]!.type === ")") {
      depth -= 1;
      if (depth === 0) return k;
    }
  }
  throw new Error("unbalanced parentheses");
}

export const utf8Bytes = (s: string): number => new TextEncoder().encode(s).length;
