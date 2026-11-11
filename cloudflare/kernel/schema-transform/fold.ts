// T3: expression depth over SQLITE_LIMIT_EXPR_DEPTH (100).
//
// The Whagons schema folds Unicode case with about 200 nested replace()
// calls. A chain is found by its run of `replace (` tokens.
//  - In a view or trigger statement it becomes a recursive CTE that applies
//    the same (search, replacement) pairs in the same order.
//  - In an index (no subqueries allowed) it is cut into VIRTUAL generated
//    columns (no declared type, so no affinity conversion) of at most `maxDepth` levels, each reading the previous one; the
//    index keeps its name, CASE and WHERE, so its uniqueness error text is
//    unchanged.

import { quoteIdent, significant, type Token } from "./lexer.ts";

interface Call {
  start: number; // index of the `replace` token
  open: number;
  close: number;
  commas: [number, number];
}

interface Chain {
  calls: Call[]; // calls[0] is the outermost
}

/** replace() chains nested at least `minLength` deep. */
export function replaceChains(sql: string, minLength: number): { toks: Token[]; chains: Chain[] } {
  const toks = significant(sql);
  const chains: Chain[] = [];
  for (let i = 0; i < toks.length; i += 1) {
    let n = 0;
    while (toks[i + 2 * n]?.upper === "REPLACE" && toks[i + 2 * n + 1]?.type === "(") n += 1;
    if (n < minLength) {
      if (n) i += 2 * n - 1;
      continue;
    }
    const calls: Call[] = [];
    for (let k = 0; k < n; k += 1) {
      const open = i + 2 * k + 1;
      let depth = 0;
      const commas: number[] = [];
      let close = -1;
      for (let j = open; j < toks.length; j += 1) {
        const t = toks[j]!;
        if (t.type === "(") depth += 1;
        else if (t.type === ")") {
          depth -= 1;
          if (depth === 0) {
            close = j;
            break;
          }
        } else if (t.type === "," && depth === 1) commas.push(j);
      }
      if (close < 0 || commas.length !== 2) throw new Error("replace() without exactly three arguments");
      calls.push({ start: i + 2 * k, open, close, commas: [commas[0]!, commas[1]!] });
    }
    chains.push({ calls });
    i = calls[0]!.close;
  }
  return { toks, chains };
}

const LITERAL = /^'(?:[^']|'')*'$/;
const literalValue = (s: string): string => s.slice(1, -1).replace(/''/g, "'");

function pairsOf(sql: string, toks: Token[], chain: Chain): Array<[string, string]> {
  const slice = (a: number, b: number) => sql.slice(toks[a]!.start, toks[b]!.end).trim();
  // Innermost call applies first.
  return chain.calls
    .slice()
    .reverse()
    .map((c) => [slice(c.commas[0] + 1, c.commas[1] - 1), slice(c.commas[1] + 1, c.close - 1)]);
}

function innerOf(sql: string, toks: Token[], chain: Chain): string {
  const innermost = chain.calls[chain.calls.length - 1]!;
  return sql.slice(toks[innermost.open + 1]!.start, toks[innermost.commas[0] - 1]!.end).trim();
}

/**
 * Rewrites every deep chain in a view or trigger statement into a recursive
 * CTE scalar subquery. Returns the statement unchanged when there is none.
 */
export function foldChainsInQuery(sql: string, { minLength = 95 } = {}): string {
  const { toks, chains } = replaceChains(sql, minLength);
  if (!chains.length) return sql;
  let out = sql;
  for (const chain of chains.slice().reverse()) {
    const pairs = pairsOf(sql, toks, chain);
    if (!pairs.every(([a, b]) => LITERAL.test(a) && LITERAL.test(b))) throw new Error("a deep replace() chain has non-literal arguments");
    const json = JSON.stringify(pairs.map(([a, b]) => [literalValue(a), literalValue(b)])).replace(/'/g, "''");
    const inner = innerOf(sql, toks, chain);
    const n = pairs.length;
    const fold =
      `(WITH RECURSIVE "_gonvex_st_fold"(i, s) AS (SELECT 0, ${inner} UNION ALL ` +
      `SELECT i + 1, replace(s, json_extract('${json}', '$[' || i || '][0]'), json_extract('${json}', '$[' || i || '][1]')) ` +
      `FROM "_gonvex_st_fold" WHERE i < ${n}) SELECT s FROM "_gonvex_st_fold" WHERE i = ${n})`;
    const start = toks[chain.calls[0]!.start]!.start;
    const end = toks[chain.calls[0]!.close]!.end;
    out = out.slice(0, start) + fold + out.slice(end);
  }
  return out;
}

export interface IndexFold {
  /** `ALTER TABLE ... ADD COLUMN ... VIRTUAL` statements, in order. */
  columns: Array<{ name: string; sql: string }>;
  /** The CREATE INDEX with each chain replaced by the last generated column. */
  index: string;
}

/** Cuts every deep chain of a CREATE INDEX into chained VIRTUAL generated columns. */
export function foldChainsInIndex(sql: string, indexName: string, table: string, { maxDepth = 90, minLength = 95 } = {}): IndexFold | null {
  const { toks, chains } = replaceChains(sql, minLength);
  if (!chains.length) return null;
  let out = sql;
  const columns: Array<{ name: string; sql: string }> = [];
  const perChain: Array<Array<{ name: string; sql: string }>> = [];
  const reversed = chains.slice().reverse();
  for (const [c, chain] of reversed.entries()) {
    const chainIndex = chains.length - 1 - c;
    const pairs = pairsOf(sql, toks, chain);
    let expression = innerOf(sql, toks, chain);
    const added: Array<{ name: string; sql: string }> = [];
    for (let k = 0; k < pairs.length; k += maxDepth) {
      let e = expression;
      for (const [a, b] of pairs.slice(k, k + maxDepth)) e = `replace(${e}, ${a}, ${b})`;
      if (k + maxDepth >= pairs.length) {
        expression = e;
        break;
      }
      const name = `_gonvex_st_fold_${indexName}_${chainIndex}_${added.length}`;
      added.push({ name, sql: `ALTER TABLE ${quoteIdent(table)} ADD COLUMN ${quoteIdent(name)} GENERATED ALWAYS AS (${e}) VIRTUAL` });
      expression = quoteIdent(name);
    }
    perChain.unshift(added);
    const start = toks[chain.calls[0]!.start]!.start;
    const end = toks[chain.calls[0]!.close]!.end;
    out = out.slice(0, start) + expression + out.slice(end);
  }
  for (const added of perChain) columns.push(...added);
  return { columns, index: out };
}

/** Whether a statement still holds a replace() chain at least `minLength` deep. */
export const hasDeepChain = (sql: string, minLength = 95): boolean => replaceChains(sql, minLength).chains.length > 0;

