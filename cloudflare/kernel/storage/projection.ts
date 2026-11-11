// Result typing for module SELECTs: a port of
// rust/crates/runtime/src/sql_projection.rs. It propagates declared column
// kinds through aliases, joins, subqueries and CTEs so result columns decode
// by their declared type. This is typing, not admission: SQLite still parses
// and runs the statement, and an unknown expression keeps expression-cell
// decoding.
//
// The Rust host falls back to `sqlite3_column_decltype` for columns this
// inference leaves open. Durable Object SQL has no decltype, so this port
// adds the cases where decltype would still name a column type: a column
// found in exactly one source by name after a USING/NATURAL join, and the
// known column lists of json_each/json_tree (whose columns have no declared
// type, as decltype reports).

import type { JsonKind } from "./values";

type Token =
  | { readonly t: "id"; readonly s: string }
  | { readonly t: "word"; readonly s: string }
  | { readonly t: "symbol"; readonly s: string }
  | { readonly t: "literal" };

function tokens(sql: string): Token[] | undefined {
  const out: Token[] = [];
  let index = 0;
  const length = sql.length;
  const isAlpha = (char: string | undefined) => char !== undefined && /[A-Za-z_]/.test(char);
  const isWordChar = (char: string | undefined) => char !== undefined && /[A-Za-z0-9_$]/.test(char);
  while (index < length) {
    const char = sql[index]!;
    if (/[ \t\n\r\f]/.test(char)) { index++; continue; }
    if (char === "-" && sql[index + 1] === "-") {
      const newline = sql.indexOf("\n", index);
      index = newline < 0 ? length : newline + 1;
      continue;
    }
    if (char === "/" && sql[index + 1] === "*") {
      const end = sql.indexOf("*/", index + 2);
      if (end < 0) return undefined;
      index = end + 2;
      continue;
    }
    if (char === "'" || char === '"' || char === "`" || char === "[") {
      const close = char === "[" ? "]" : char;
      let value = "";
      index++;
      for (;;) {
        const next = sql[index];
        if (next === undefined) return undefined;
        index++;
        if (next === close) {
          if (close !== "]" && sql[index] === close) { value += close; index++; } else break;
        } else value += next;
      }
      out.push(char === "'" ? { t: "literal" } : { t: "id", s: value });
      continue;
    }
    if (isAlpha(char)) {
      const start = index++;
      while (isWordChar(sql[index])) index++;
      out.push({ t: "word", s: sql.slice(start, index) });
      continue;
    }
    out.push({ t: "symbol", s: char });
    index++;
  }
  return out;
}

const nameOf = (token: Token | undefined): string | undefined =>
  token && (token.t === "id" || token.t === "word") ? token.s : undefined;
const word = (token: Token | undefined, text: string): boolean =>
  token !== undefined && token.t === "word" && token.s.toLowerCase() === text;
const symbol = (token: Token | undefined, char: string): boolean =>
  token !== undefined && token.t === "symbol" && token.s === char;
const same = (a: string, b: string) => a.toLowerCase() === b.toLowerCase();

function close(list: readonly Token[], start: number): number | undefined {
  if (!symbol(list[start], "(")) return undefined;
  let depth = 1;
  for (let index = start + 1; index < list.length; index++) {
    const token = list[index]!;
    if (symbol(token, "(")) depth++;
    else if (symbol(token, ")") && --depth === 0) return index;
  }
  return undefined;
}

function split(list: readonly Token[]): Token[][] {
  const out: Token[][] = [];
  let start = 0;
  let depth = 0;
  list.forEach((token, index) => {
    if (symbol(token, "(")) depth++;
    else if (symbol(token, ")")) depth--;
    else if (symbol(token, ",") && depth === 0) { out.push(list.slice(start, index)); start = index + 1; }
  });
  out.push(list.slice(start));
  return out;
}

export type Field = readonly [name: string, kind: JsonKind | undefined];
type Fields = Field[];

interface Source {
  readonly alias: string;
  readonly table: string;
  readonly fields: Fields;
  readonly known: boolean;
}

const fieldKind = (fields: Fields, column: string): JsonKind | undefined =>
  fields.find(([name]) => same(name, column))?.[1];
const hasField = (fields: Fields, column: string): boolean => fields.some(([name]) => same(name, column));

const CLAUSES = ["where", "group", "order", "having", "limit", "offset", "union", "intersect", "except", "window", "returning"];
const clause = (token: Token) => CLAUSES.some((text) => word(token, text));
const RESERVED = ["join", "left", "right", "full", "inner", "outer", "cross", "natural", "on", "using", "as", "indexed", "not"];
const reserved = (token: Token) => clause(token) || RESERVED.some((text) => word(token, text)) || token.t === "symbol";

/** Table-valued functions with a fixed set of visible columns, none with a declared type. */
const FUNCTION_COLUMNS: Readonly<Record<string, readonly string[]>> = {
  json_each: ["key", "value", "type", "atom", "id", "parent", "fullkey", "path"],
  json_tree: ["key", "value", "type", "atom", "id", "parent", "fullkey", "path"],
};

function expression(list: readonly Token[], sources: readonly Source[]): JsonKind | undefined {
  if (list.length === 1 && nameOf(list[0]) !== undefined) {
    const column = nameOf(list[0])!;
    const matches = sources.filter((source) => fieldKind(source.fields, column) !== undefined).map((source) => fieldKind(source.fields, column)!);
    if (matches.length === 1) return matches[0];
    // decltype: USING/NATURAL joins merge the shared column, so all owners agree.
    if (matches.length > 1 && matches.every((kind) => kind === matches[0])) {
      const owners = sources.filter((source) => hasField(source.fields, column));
      if (owners.length === matches.length) return matches[0];
    }
  } else if (list.length === 3 && symbol(list[1], ".")) {
    const alias = nameOf(list[0]);
    const column = nameOf(list[2]);
    if (alias === undefined) return undefined;
    const source = sources.find((candidate) => same(candidate.alias, alias) || same(candidate.table, alias));
    if (!source || column === undefined) return undefined;
    return fieldKind(source.fields, column);
  }
  const first = list[0];
  if (first && ["json", "json_array", "json_object", "json_group_array", "json_group_object"].some((text) => word(first, text))
    && symbol(list[1], "(") && close(list, 1) === list.length - 1) return "json";
  if (first && (word(first, "coalesce") || word(first, "ifnull")) && symbol(list[1], "(") && close(list, 1) === list.length - 1) {
    const kinds = split(list.slice(2, -1))
      .filter((part) => !(part.length === 1 && word(part[0], "null")))
      .map((part) => expression(part, sources));
    const firstKind = kinds[0];
    if (firstKind !== undefined && kinds.every((kind) => kind === firstKind)) return firstKind;
  }
  return undefined;
}

export type Resolve = (table: string) => Field[];

function infer(list: readonly Token[], inherited: ReadonlyMap<string, Fields>, resolve: Resolve): Fields | undefined {
  const ctes = new Map(inherited);
  let start = 0;
  if (word(list[0], "with")) {
    start = 1;
    if (word(list[start], "recursive")) start++;
    for (;;) {
      const cte = nameOf(list[start])?.toLowerCase();
      if (cte === undefined) return undefined;
      start++;
      let aliases: (string | undefined)[] | undefined | null = null;
      if (symbol(list[start], "(")) {
        const end = close(list, start);
        if (end === undefined) return undefined;
        const parts = split(list.slice(start + 1, end)).map((part) => (part.length === 1 ? nameOf(part[0]) : undefined));
        aliases = parts.some((part) => part === undefined) ? undefined : parts;
        start = end + 1;
      }
      if (!word(list[start], "as")) return undefined;
      start++;
      if (word(list[start], "not")) start++;
      if (word(list[start], "materialized")) start++;
      const end = close(list, start);
      if (end === undefined) return undefined;
      const fields = infer(list.slice(start + 1, end), ctes, resolve) ?? [];
      if (aliases) {
        if (aliases.length !== fields.length) return undefined;
        fields.forEach((field, index) => { fields[index] = [aliases[index]!, field[1]]; });
      }
      ctes.set(cte, fields);
      start = end + 1;
      if (symbol(list[start], ",")) start++;
      else break;
    }
  }
  if (!word(list[start], "select")) return undefined;
  start++;
  if (word(list[start], "distinct") || word(list[start], "all")) start++;
  let depth = 0;
  let from: number | undefined;
  let projectionEnd = list.length;
  for (let index = start; index < list.length; index++) {
    const token = list[index]!;
    if (symbol(token, "(")) depth++;
    else if (symbol(token, ")")) depth--;
    else if (depth === 0 && word(token, "from")) { from = index; projectionEnd = index; break; }
    else if (depth === 0 && clause(token)) { projectionEnd = index; break; }
    else if (depth === 0 && symbol(token, ";")) { projectionEnd = index; break; }
  }
  const sources: Source[] = [];
  if (from !== undefined) {
    let index = from + 1;
    for (;;) {
      let fields: Fields;
      let table: string;
      let known: boolean;
      if (symbol(list[index], "(")) {
        const end = close(list, index);
        if (end === undefined) return undefined;
        const nested = infer(list.slice(index + 1, end), ctes, resolve);
        known = nested !== undefined;
        fields = nested ?? [];
        table = "";
        index = end + 1;
      } else {
        const first = nameOf(list[index]);
        if (first === undefined) return undefined;
        if (symbol(list[index + 1], ".")) {
          const second = nameOf(list[index + 2]);
          if (second === undefined) return undefined;
          index += 3;
          table = `${first}.${second}`;
        } else {
          index += 1;
          table = first;
        }
        if (symbol(list[index], "(")) {
          const end = close(list, index);
          if (end === undefined) return undefined;
          index = end + 1;
          const columns = FUNCTION_COLUMNS[table.toLowerCase()];
          fields = columns ? columns.map((name) => [name, undefined] as const) : [];
          known = columns !== undefined;
        } else {
          const cte = ctes.get(table.toLowerCase());
          if (cte) { fields = [...cte]; known = true; }
          else { fields = resolve(table); known = fields.length > 0; }
        }
      }
      if (word(list[index], "as")) index++;
      let alias = table;
      const candidate = list[index];
      if (candidate && !reserved(candidate)) {
        const name = nameOf(candidate);
        if (name !== undefined) { index++; alias = name; }
      }
      sources.push({ alias, table, fields, known });
      let nested = 0;
      let next: number | undefined;
      while (list[index] !== undefined) {
        const token = list[index]!;
        if (symbol(token, "(")) nested++;
        else if (symbol(token, ")")) nested--;
        else if (nested === 0 && clause(token)) break;
        else if (nested === 0 && (word(token, "join") || symbol(token, ","))) { next = index + 1; break; }
        index++;
      }
      if (next === undefined) break;
      index = next;
    }
  }
  const fields: Fields = [];
  for (let part of split(list.slice(start, projectionEnd))) {
    if (part.length === 1 && symbol(part[0], "*")) {
      if (sources.some((source) => !source.known)) return undefined;
      for (const source of sources) fields.push(...source.fields);
      continue;
    }
    if (part.length === 3 && symbol(part[1], ".") && symbol(part[2], "*")) {
      const alias = nameOf(part[0]);
      const source = alias === undefined ? undefined : sources.find((candidate) => same(candidate.alias, alias) || same(candidate.table, alias));
      if (!source || !source.known) return undefined;
      fields.push(...source.fields);
      continue;
    }
    let alias: string | undefined;
    let level = 0;
    let asAt: number | undefined;
    part.forEach((token, index) => {
      if (symbol(token, "(")) level++;
      else if (symbol(token, ")")) level--;
      else if (level === 0 && word(token, "as")) asAt = index;
    });
    if (asAt !== undefined) {
      alias = nameOf(part[asAt + 1]);
      part = part.slice(0, asAt);
    } else if (part.length > 1 && nameOf(part.at(-1)) !== undefined && !symbol(part[part.length - 2], ".")) {
      alias = nameOf(part.at(-1));
      part = part.slice(0, -1);
    }
    const output = alias ?? (part.length === 1 ? nameOf(part[0]) : part.length === 3 && symbol(part[1], ".") ? nameOf(part[2]) : undefined) ?? "";
    fields.push([output, expression(part, sources)]);
  }
  return fields;
}

/**
 * Declared kinds of a statement's result columns, `undefined` for an
 * expression column; `undefined` overall when the statement's shape is not
 * understood. `resolve` returns a table's columns in declaration order and
 * throws to fail the statement (Rust propagates catalog errors).
 */
export function projectionKinds(sql: string, resolve: Resolve): (JsonKind | undefined)[] | undefined {
  const list = tokens(sql);
  if (!list) return undefined;
  return infer(list, new Map(), resolve)?.map(([, kind]) => kind);
}
