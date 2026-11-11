// T1: compound SELECTs over the 5-term limit become left-nested chains.
//
// A chain t1 op1 t2 ... tn is evaluated left to right, so
//   SELECT * FROM (t1 op1 ... t5) op5 t6 ... tn
// is the same query with one term fewer per extra level; repeat until every
// chain fits. A trailing ORDER BY / LIMIT stays on the outermost chain. S1
// verified this on every refused view (cloudflare/spikes/s1/results/verify-workarounds.json).

import { significant, type Token } from "./lexer.ts";

const COMPOUND = new Set(["UNION", "INTERSECT", "EXCEPT"]);

export function rewriteCompounds(sql: string, limit = 5): string {
  for (let pass = 0; pass < 100_000; pass += 1) {
    const next = rewriteOne(sql, limit);
    if (next === null) return sql;
    sql = next;
  }
  throw new Error("compound rewrite did not converge");
}

interface Frame {
  operators: number[];
  open: number;
}

function rewriteOne(sql: string, limit: number): string | null {
  const toks = significant(sql);
  const stack: Frame[] = [{ operators: [], open: -1 }];
  for (let i = 0; i < toks.length; i += 1) {
    const t = toks[i]!;
    if (t.type === "(") stack.push({ operators: [], open: i });
    else if (t.type === ")" || t.type === ";") {
      const frame = stack.pop()!;
      if (frame.operators.length >= limit) return rewriteScope(sql, toks, frame, limit);
      if (t.type === ";" || stack.length === 0) stack.push({ operators: [], open: i });
    } else if (t.type === "word" && COMPOUND.has(t.upper!)) {
      // `UNION ALL` counts once: ALL is the next token, not an operator.
      stack[stack.length - 1]!.operators.push(i);
    }
  }
  const frame = stack.pop();
  if (frame && frame.operators.length >= limit) return rewriteScope(sql, toks, frame, limit);
  return null;
}

function rewriteScope(sql: string, toks: Token[], frame: Frame, limit: number): string {
  const firstOperator = frame.operators[0]!;
  // The chain starts at the last SELECT/VALUES at this depth before the first
  // operator (earlier ones belong to a WITH clause, a CREATE ... AS head or a
  // trigger statement head).
  let depth = 0;
  let chainStart = -1;
  for (let i = frame.open + 1; i < firstOperator; i += 1) {
    const t = toks[i]!;
    if (t.type === "(") depth += 1;
    else if (t.type === ")") depth -= 1;
    else if (depth === 0 && t.type === "word" && (t.upper === "SELECT" || t.upper === "VALUES")) chainStart = i;
  }
  if (chainStart < 0) throw new Error("cannot find the start of a compound SELECT");
  const cut = frame.operators[limit - 1]!;
  const head = sql.slice(toks[chainStart]!.start, toks[cut]!.start).trimEnd();
  return `${sql.slice(0, toks[chainStart]!.start)}SELECT * FROM (${head}) ${sql.slice(toks[cut]!.start)}`;
}

/** Top-level UNION ALL terms of a SELECT, or null if it is not a pure UNION ALL chain. */
export function unionAllTerms(sql: string): string[] | null {
  const toks = significant(sql);
  const cuts: Array<[number, number]> = [];
  let depth = 0;
  for (let i = 0; i < toks.length; i += 1) {
    const t = toks[i]!;
    if (t.type === "(") depth += 1;
    else if (t.type === ")") depth -= 1;
    else if (depth === 0 && t.type === "word" && COMPOUND.has(t.upper!)) {
      const next = toks[i + 1];
      if (t.upper !== "UNION" || !next || next.upper !== "ALL") return null;
      cuts.push([t.start, next.end]);
    } else if (depth === 0 && t.type === "word" && (t.upper === "ORDER" || t.upper === "LIMIT") && cuts.length) return null;
  }
  if (!cuts.length) return null;
  const terms: string[] = [];
  let from = 0;
  for (const [start, end] of cuts) {
    terms.push(sql.slice(from, start).trim());
    from = end;
  }
  terms.push(sql.slice(from).trim());
  return terms;
}
