// Durable Object SQLite limits the transform's output must stay inside, and a
// static checker that measures one statement against them. Measured values
// come from S1 (docs/cloudflare/spikes/S1-schema.md, "Platform limits").

import { classify, closingParen, isNameToken, parseTrigger, significant, text, unquote, utf8Bytes, type Token } from "./lexer.ts";

export interface Limits {
  /** SQLITE_LIMIT_SQL_LENGTH: bytes per statement. */
  statementBytes: number;
  /** CREATE text is re-parsed inside `INSERT INTO sqlite_schema ... '%q'`, which doubles every quote. */
  ddlBytes: number;
  /** SQLITE_LIMIT_COMPOUND_SELECT: terms per compound chain. */
  compoundTerms: number;
  /** SQLITE_LIMIT_EXPR_DEPTH. */
  expressionDepth: number;
  /** Longest chain of CTEs one query may read (platform only, S1 cte-depth). */
  cteChain: number;
  /** Strict hosts refuse a statement that reads this many union branches or more (S1 "Blocker"). */
  unionBranchesRead: number;
  /** Bound parameters per query (runtime only; recorded for callers). */
  boundParameters: number;
  /** Columns per table or result set. */
  columns: number;
}

export const LIMITS: Readonly<Limits> = Object.freeze({
  statementBytes: 100_000,
  ddlBytes: 100_000,
  compoundTerms: 5,
  expressionDepth: 100,
  cteChain: 99,
  unionBranchesRead: 10,
  boundParameters: 100,
  columns: 100,
});

export type LimitName =
  | "statement-bytes"
  | "ddl-bytes"
  | "compound-terms"
  | "expression-depth"
  | "cte-chain"
  | "union-branches-read"
  | "temp-schema"
  | "custom-function"
  | "transaction-control"
  | "attach"
  | "columns";

export interface Violation {
  limit: LimitName;
  /** Measured value (bytes, terms, depth, branches) or the offending name. */
  value: number | string;
  max?: number;
  detail: string;
}

/**
 * Functions a Durable Object provides: SQLite core, date/time, math, JSON,
 * window and aggregate functions, FTS5 auxiliary functions. Anything else is a
 * custom function, which an object cannot register.
 */
export const BUILTIN_FUNCTIONS: ReadonlySet<string> = new Set(
  `abs changes char coalesce concat concat_ws format glob hex if ifnull iif instr last_insert_rowid length like
likelihood likely load_extension lower ltrim max min nullif octet_length printf quote random randomblob replace round
rtrim sign soundex sqlite_compileoption_get sqlite_compileoption_used sqlite_offset sqlite_source_id sqlite_version
substr substring total_changes trim typeof unhex unicode unlikely upper zeroblob
avg count group_concat string_agg sum total median percentile percentile_cont percentile_disc
date time datetime julianday unixepoch strftime timediff
acos acosh asin asinh atan atan2 atanh ceil ceiling cos cosh degrees exp floor ln log log10 log2 mod pi pow power
radians sin sinh sqrt tan tanh trunc
json jsonb json_array jsonb_array json_array_length json_error_position json_extract jsonb_extract json_insert
jsonb_insert json_object jsonb_object json_patch jsonb_patch json_pretty json_remove jsonb_remove json_replace
jsonb_replace json_set jsonb_set json_type json_valid json_quote json_group_array jsonb_group_array json_group_object
jsonb_group_object json_each json_tree
row_number rank dense_rank percent_rank cume_dist ntile lag lead first_value last_value nth_value
bm25 highlight snippet fts5 fts5_get_locale fts5_locale fts5_insttoken
cast raise`
    .split(/\s+/)
    .filter(Boolean),
);

/** Words followed by "(" that are not function calls. */
const NOT_FUNCTIONS = new Set(
  `AS MATERIALIZED IN VALUES EXISTS KEY CHECK UNIQUE CONFLICT USING ON OVER FILTER WHERE AND OR NOT SELECT FROM JOIN WHEN
THEN ELSE SET DEFAULT BY INTO TABLE INDEX REFERENCES IF WINDOW PARTITION RETURNING CASE END IS LIKE GLOB BETWEEN ALL
DISTINCT UNION EXCEPT INTERSECT WITH RECURSIVE VIEW TRIGGER BEGIN ROW LIMIT OFFSET HAVING GROUP ORDER UPDATE INSERT
DELETE OF GENERATED ALWAYS STORED VIRTUAL COLLATE ESCAPE MATCH REGEXP ABORT FAIL IGNORE ROLLBACK REPLACE_INTO
PRIMARY FOREIGN CONSTRAINT`
    .split(/\s+/)
    .filter(Boolean),
);

export interface FunctionCall {
  name: string;
  at: number;
}

/**
 * Function calls in a statement. A name before "(" is a call unless it is a
 * keyword, a table/CTE/view name with a column list, or a column list head.
 */
export function functionCalls(sql: string): FunctionCall[] {
  const toks = significant(sql);
  const calls: FunctionCall[] = [];
  for (let i = 1; i < toks.length; i += 1) {
    const t = toks[i]!;
    if (t.type !== "(") continue;
    const prev = toks[i - 1]!;
    if (prev.type !== "word") continue;
    if (NOT_FUNCTIONS.has(prev.upper!)) continue;
    const before = toks[i - 2];
    // `INSERT INTO t(...)`, `CREATE TABLE t(...)`, `ON t(...)`, `REFERENCES t(...)`, `name(cols) AS (...)`.
    if (before && before.type === "word" && ["INTO", "TABLE", "ON", "REFERENCES", "VIEW", "EXISTS", "WITH", "RECURSIVE", "USING", "INDEX", "TRIGGER", "OF"].includes(before.upper!)) continue;
    if (before && before.type === "," && isCteHead(toks, i)) continue;
    if (before && before.type === "op" && text(sql, before) === ".") continue;
    const close = closingParen(toks, i);
    const next = toks[close + 1];
    const after = toks[close + 2];
    if (next && next.upper === "AS" && after && (after.type === "(" || after.upper === "MATERIALIZED" || after.upper === "NOT")) continue;
    calls.push({ name: prev.upper!.toLowerCase(), at: prev.start });
  }
  return calls;
}

function isCteHead(toks: Token[], open: number): boolean {
  const close = closingParen(toks, open);
  const next = toks[close + 1];
  return !!next && next.upper === "AS";
}

/** Every compound-select chain's term count (operators + 1). */
export function compoundChains(sql: string): number[] {
  const toks = significant(sql);
  const stack = [0];
  const chains: number[] = [];
  const close = () => {
    const ops = stack.pop() ?? 0;
    if (ops > 0) chains.push(ops + 1);
  };
  for (const t of toks) {
    if (t.type === "(") stack.push(0);
    else if (t.type === ")") close();
    else if (t.type === "word" && (t.upper === "UNION" || t.upper === "INTERSECT" || t.upper === "EXCEPT")) stack[stack.length - 1]! += 1;
  }
  while (stack.length) close();
  return chains;
}

const RUN_OPERATORS = new Set(["AND", "OR"]);

/**
 * Estimated expression-tree height: each function call adds one level, and a
 * run of AND / OR / `||` / arithmetic operators at one level adds one per
 * operator (SQLite builds them left-deep). Parentheses alone add nothing.
 * Subqueries start a new count (SQLite checks each Select's expressions).
 */
export function expressionDepth(sql: string): number {
  const toks = significant(sql);
  let best = 0;
  // Returns the height of the expression run in toks[from, to).
  const height = (from: number, to: number): number => {
    let max = 0;
    let ops = 0;
    for (let k = from; k < to; k += 1) {
      const t = toks[k]!;
      if (t.type === "(") {
        const close = closingParen(toks, k);
        const inner = toks[k + 1];
        const isSubquery = !!inner && (inner.upper === "SELECT" || inner.upper === "WITH" || inner.upper === "VALUES");
        const prev = toks[k - 1];
        const isCall = !!prev && prev.type === "word" && !NOT_FUNCTIONS.has(prev.upper!);
        if (isSubquery) {
          best = Math.max(best, height(k + 1, close));
        } else {
          // Split arguments at top-level commas; each argument is its own run.
          let argFrom = k + 1;
          let depth = 0;
          let inside = 0;
          for (let j = k + 1; j <= close; j += 1) {
            const u = toks[j]!;
            if (u.type === "(") depth += 1;
            else if (u.type === ")") {
              if (depth === 0) {
                inside = Math.max(inside, height(argFrom, j));
                break;
              }
              depth -= 1;
            } else if (u.type === "," && depth === 0) {
              inside = Math.max(inside, height(argFrom, j));
              argFrom = j + 1;
            }
          }
          max = Math.max(max, inside + (isCall ? 1 : 0));
        }
        k = close;
      } else if (t.type === "word" && RUN_OPERATORS.has(t.upper!)) ops += 1;
      else if (t.type === "op") {
        const o = text(sql, t);
        if (o === "||" || o === "+" || o === "*" || o === "/" || o === "%") ops += 1;
      } else if (t.upper === "SELECT" || t.upper === "FROM" || t.upper === "WHERE" || t.upper === "ON" || t.upper === "SET" || t.type === "," || t.type === ";") {
        // A new clause or a new list item starts a new expression.
        best = Math.max(best, Math.max(max, 1) + ops);
        max = 0;
        ops = 0;
      }
    }
    const h = Math.max(max, 1) + ops;
    best = Math.max(best, h);
    return h;
  };
  height(0, toks.length);
  return best;
}

/**
 * Longest dependency chain among the CTEs of every WITH clause in a
 * statement (a CTE reading another adds one).
 */
export function cteChainDepth(sql: string): number {
  const toks = significant(sql);
  let best = 0;
  for (let i = 0; i < toks.length; i += 1) {
    if (toks[i]!.upper !== "WITH") continue;
    const ctes = parseCteList(sql, toks, i);
    if (!ctes) continue;
    const index = new Map(ctes.list.map((c, k) => [c.name.toLowerCase(), k]));
    const deps = ctes.list.map((c) => {
      const s = new Set<number>();
      for (const t of significant(c.body)) {
        if (!isNameToken(t)) continue;
        const k = index.get(unquote(text(c.body, t)).toLowerCase());
        if (k !== undefined) s.add(k);
      }
      return s;
    });
    const memo = new Map<number, number>();
    const depth = (k: number): number => {
      const known = memo.get(k);
      if (known !== undefined) return known;
      memo.set(k, 1);
      let m = 0;
      for (const d of deps[k]!) if (d !== k) m = Math.max(m, depth(d));
      memo.set(k, m + 1);
      return m + 1;
    };
    for (let k = 0; k < ctes.list.length; k += 1) best = Math.max(best, depth(k));
  }
  return best;
}

export interface Cte {
  name: string;
  /** Full `name [(cols)] AS [NOT] [MATERIALIZED] (body)` text. */
  text: string;
  body: string;
}

/** Parses `WITH [RECURSIVE] cte, ...` starting at toks[withAt]; returns the CTEs and the index after them. */
export function parseCteList(sql: string, toks: Token[], withAt: number): { recursive: boolean; list: Cte[]; next: number } | null {
  let i = withAt + 1;
  const recursive = toks[i]?.upper === "RECURSIVE";
  if (recursive) i += 1;
  const list: Cte[] = [];
  for (;;) {
    const nameTok = toks[i];
    if (!isNameToken(nameTok)) return null;
    const start = nameTok.start;
    const name = unquote(text(sql, nameTok));
    i += 1;
    if (toks[i]?.type === "(") i = closingParen(toks, i) + 1;
    if (toks[i]?.upper !== "AS") return null;
    i += 1;
    while (toks[i] && toks[i]!.type === "word" && (toks[i]!.upper === "MATERIALIZED" || toks[i]!.upper === "NOT")) i += 1;
    if (toks[i]?.type !== "(") return null;
    const close = closingParen(toks, i);
    list.push({ name, text: sql.slice(start, toks[close]!.end), body: sql.slice(toks[i]!.end, toks[close]!.start) });
    i = close + 1;
    if (toks[i]?.type === ",") {
      i += 1;
      continue;
    }
    return { recursive, list, next: i };
  }
}

/** Statement bytes as SQLite stores a CREATE in sqlite_schema (quotes doubled, plus the INSERT wrapper). */
export function effectiveDdlBytes(statement: string, name = ""): number {
  const quotes = (statement.match(/'/g) ?? []).length;
  return utf8Bytes(statement) + quotes + 2 * utf8Bytes(name) + 100;
}

export interface CheckContext {
  /** Union branches a statement reads when it selects from this relation (lower-case name). */
  branchesRead?: (relation: string) => number;
  limits?: Readonly<Limits>;
}

/** Union branches one statement reads: its own compound terms plus those of every relation it names. */
export function unionBranchesRead(statement: string, branchesRead: (relation: string) => number): number {
  let total = 0;
  for (const terms of compoundChains(statement)) total += terms;
  const toks = significant(statement);
  for (let k = 0; k < toks.length; k += 1) {
    const t = toks[k]!;
    if (!isNameToken(t)) continue;
    const prev = toks[k - 1];
    if (prev && prev.type === "op" && text(statement, prev) === ".") continue;
    total += branchesRead(unquote(text(statement, t)).toLowerCase());
  }
  return total;
}

/** Measures one statement against every static limit. Trigger bodies are checked statement by statement. */
export function checkStatement(statement: string, context: CheckContext = {}): Violation[] {
  const limits = context.limits ?? LIMITS;
  const out: Violation[] = [];
  const bytes = utf8Bytes(statement);
  if (bytes > limits.statementBytes) out.push({ limit: "statement-bytes", value: bytes, max: limits.statementBytes, detail: `${bytes} bytes` });
  const kind = classify(statement);
  if (kind.kind !== "other" && kind.kind !== "drop" && !kind.kind.startsWith("alter")) {
    const name = "name" in kind ? kind.name : "";
    const effective = effectiveDdlBytes(statement, name);
    if (effective > limits.ddlBytes) out.push({ limit: "ddl-bytes", value: effective, max: limits.ddlBytes, detail: `${effective} bytes after quote doubling` });
  }
  if ((kind.kind === "create-table" || kind.kind === "create-view" || kind.kind === "create-trigger") && kind.temp) {
    out.push({ limit: "temp-schema", value: "TEMP", detail: "the temp schema is not authorized" });
  }
  const toks = significant(statement);
  for (let k = 0; k < toks.length; k += 1) {
    const t = toks[k]!;
    if (t.upper === "TEMP" || t.upper === "TEMPORARY") {
      const next = toks[k + 1];
      if (next && next.type === "op" && text(statement, next) === ".") out.push({ limit: "temp-schema", value: "temp.", detail: "reads or writes the temp schema" });
    }
    if (t.upper === "REGEXP") out.push({ limit: "custom-function", value: "regexp", detail: "REGEXP needs a custom function" });
    if (t.upper === "ATTACH" || t.upper === "DETACH") out.push({ limit: "attach", value: t.upper, detail: "ATTACH is not authorized" });
  }
  const first = toks[0]?.upper;
  if (first === "BEGIN" || first === "COMMIT" || first === "SAVEPOINT" || first === "RELEASE" || (first === "ROLLBACK" && kind.kind === "other") || first === "END") {
    out.push({ limit: "transaction-control", value: first, detail: "transactions go through the storage API" });
  }
  for (const call of functionCalls(statement)) {
    if (!BUILTIN_FUNCTIONS.has(call.name)) out.push({ limit: "custom-function", value: call.name, detail: `${call.name}() is not a built-in function` });
  }
  const bodies = kind.kind === "create-trigger" ? triggerStatements(statement) : [statement];
  for (const body of bodies) {
    const chains = compoundChains(body);
    const worst = chains.length ? Math.max(...chains) : 0;
    if (worst > limits.compoundTerms) out.push({ limit: "compound-terms", value: worst, max: limits.compoundTerms, detail: `compound SELECT of ${worst} terms` });
    const depth = expressionDepth(body);
    if (depth > limits.expressionDepth) out.push({ limit: "expression-depth", value: depth, max: limits.expressionDepth, detail: `expression depth about ${depth}` });
    const chain = cteChainDepth(body);
    if (chain > limits.cteChain) out.push({ limit: "cte-chain", value: chain, max: limits.cteChain, detail: `CTE chain of ${chain}` });
    if (context.branchesRead && kind.kind !== "create-view") {
      const read = unionBranchesRead(body, context.branchesRead);
      if (read >= limits.unionBranchesRead) out.push({ limit: "union-branches-read", value: read, max: limits.unionBranchesRead - 1, detail: `one statement reads ${read} union branches` });
    }
  }
  return dedupe(out);
}

/** A trigger's WHEN clause and each body statement compile as separate units. */
function triggerStatements(statement: string): string[] {
  const parts = parseTrigger(statement);
  return [...(parts.when ? [`SELECT ${parts.when}`] : []), ...parts.body];
}

function dedupe(list: Violation[]): Violation[] {
  const seen = new Set<string>();
  return list.filter((v) => {
    const key = `${v.limit}:${v.value}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
