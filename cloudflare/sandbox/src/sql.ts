// What the analysis object does to module SQL before SQLite sees it.
//
// The `duckdb` binding runs on Durable Object SQLite (note 0040). Three
// differences from DuckDB are bridged here, because Whagons' own code emits
// them (gonvex/assistant/executionContext.ts, completion.ts):
// - imported files are DuckDB catalogs: `"import_1"."data"` names a table in
//   an attached database. Here that table is stored as `"import_1.data"`, and
//   `alias.table` (and `alias.main.table`) references are rewritten to it;
// - `$1` placeholders (DuckDB, Postgres) become SQLite's `?1`;
// - several statements run in order and the last one answers, as duckdb-rs `prepare` does.
// `SUMMARIZE`, `DESCRIBE` and `SHOW TABLES` are answered by analysis.ts.

type TokenKind = "word" | "quoted" | "string" | "space" | "comment" | "param" | "punct";
interface Token { kind: TokenKind; text: string; /** identifier name, for words and quoted identifiers */ name?: string }

export function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const c = sql[i]!;
    const start = i;
    if (/\s/.test(c)) {
      while (i < n && /\s/.test(sql[i]!)) i++;
      tokens.push({ kind: "space", text: sql.slice(start, i) });
    } else if (c === "-" && sql[i + 1] === "-") {
      while (i < n && sql[i] !== "\n") i++;
      tokens.push({ kind: "comment", text: sql.slice(start, i) });
    } else if (c === "/" && sql[i + 1] === "*") {
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? n : end + 2;
      tokens.push({ kind: "comment", text: sql.slice(start, i) });
    } else if (c === "'" ) {
      i++;
      while (i < n) {
        if (sql[i] === "'" && sql[i + 1] === "'") i += 2;
        else if (sql[i] === "'") { i++; break; }
        else i++;
      }
      tokens.push({ kind: "string", text: sql.slice(start, i) });
    } else if (c === '"' || c === "`" || c === "[") {
      const close = c === "[" ? "]" : c;
      i++;
      let name = "";
      while (i < n) {
        if (sql[i] === close && close !== "]" && sql[i + 1] === close) { name += close; i += 2; }
        else if (sql[i] === close) { i++; break; }
        else name += sql[i++];
      }
      tokens.push({ kind: "quoted", text: sql.slice(start, i), name });
    } else if (c === "$" && /[0-9]/.test(sql[i + 1] ?? "")) {
      i++;
      while (i < n && /[0-9]/.test(sql[i]!)) i++;
      tokens.push({ kind: "param", text: sql.slice(start, i) });
    } else if (/[A-Za-z_\u0080-￿]/.test(c)) {
      while (i < n && /[A-Za-z0-9_$\u0080-￿]/.test(sql[i]!)) i++;
      const text = sql.slice(start, i);
      tokens.push({ kind: "word", text, name: text });
    } else {
      i++;
      tokens.push({ kind: "punct", text: c });
    }
  }
  return tokens;
}

const significant = (token: Token) => token.kind !== "space" && token.kind !== "comment";

export const quoteIdentifier = (name: string): string => `"${name.replace(/"/g, '""')}"`;

/** The stored name of an imported table. */
export const importedTableName = (alias: string, table: string): string => `${alias}.${table}`;

/**
 * Module SQL split into statements (DuckDB runs them all and answers the last),
 * each with `$N` as `?N` and `alias.table` / `alias.main.table` as the stored
 * import table. `imports` maps a lower-case alias to its lower-case tables.
 */
export function prepareStatements(sql: string, imports: ReadonlyMap<string, ReadonlySet<string>>): string[] {
  const out: string[] = [];
  let current: Token[] = [];
  for (const token of tokenize(sql)) {
    if (token.kind === "punct" && token.text === ";") {
      if (current.some(significant)) out.push(rewrite(current, imports));
      current = [];
    } else current.push(token);
  }
  if (current.some(significant)) out.push(rewrite(current, imports));
  return out;
}

/** One statement (no `;`), rewritten. */
export function prepareStatement(sql: string, imports: ReadonlyMap<string, ReadonlySet<string>>): string {
  const statements = prepareStatements(sql, imports);
  if (statements.length !== 1) throw new Error("expected exactly one statement");
  return statements[0]!;
}

function rewrite(tokens: Token[], imports: ReadonlyMap<string, ReadonlySet<string>>): string {
  const out: string[] = [];
  const next = (from: number): number => {
    let j = from;
    while (j < tokens.length && !significant(tokens[j]!)) j++;
    return j;
  };
  const isDot = (j: number) => tokens[j]?.kind === "punct" && tokens[j]?.text === ".";
  const nameAt = (j: number): string | undefined => {
    const token = tokens[j];
    return token && (token.kind === "word" || token.kind === "quoted") ? token.name : undefined;
  };
  let previous = -1;
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]!;
    if (token.kind === "param") {
      previous = i;
      out.push(`?${token.text.slice(1)}`);
      continue;
    }
    const alias = nameAt(i)?.toLowerCase();
    // `x.import_1.data` is a column path, not a catalog reference.
    const tables = alias !== undefined && !isDot(previous) ? imports.get(alias) : undefined;
    if (significant(token)) previous = i;
    if (tables) {
      const dot1 = next(i + 1);
      const part1 = next(dot1 + 1);
      const first = isDot(dot1) ? nameAt(part1)?.toLowerCase() : undefined;
      if (first !== undefined && tables.has(first)) {
        out.push(quoteIdentifier(importedTableName(alias!, first)));
        i = previous = part1;
        continue;
      }
      if (first === "main") {
        const dot2 = next(part1 + 1);
        const part2 = next(dot2 + 1);
        const second = isDot(dot2) ? nameAt(part2)?.toLowerCase() : undefined;
        if (second !== undefined && tables.has(second)) {
          out.push(quoteIdentifier(importedTableName(alias!, second)));
          i = previous = part2;
          continue;
        }
      }
    }
    out.push(token.text);
  }
  return out.join("");
}

export type Special =
  | { kind: "summarize"; source: string }
  | { kind: "describe"; source: string }
  | { kind: "showTables" };

/** DuckDB statements SQLite has no word for. `source` is a SELECT to read from. */
export function specialStatement(sql: string): Special | null {
  const trimmed = sql.trim().replace(/;\s*$/, "");
  const match = /^(summarize|describe|show)\b\s*([\s\S]*)$/i.exec(trimmed);
  if (!match) return null;
  const verb = match[1]!.toLowerCase();
  const rest = match[2]!.trim();
  if (verb === "show") return /^tables$/i.test(rest) ? { kind: "showTables" } : null;
  if (!rest) return null;
  const source = /^(select|with|values|from|\()/i.test(rest) ? (/^from\b/i.test(rest) ? `SELECT * ${rest}` : rest) : `SELECT * FROM ${rest}`;
  return { kind: verb === "summarize" ? "summarize" : "describe", source };
}
