// Views of the shape the reference-integrity generator emits:
//
//   CREATE VIEW v AS WITH [RECURSIVE] r0 AS MATERIALIZED (...), ..., rN AS MATERIALIZED (t1 UNION ALL ... tk)
//   SELECT <aggregate> FROM rN GROUP BY ...
//
// T2 partitions such a view by union branch when it is over the size limit;
// T6 splits it into one view per branch so no statement reads the union. Each
// piece keeps, as CTEs, only what its branches read, so MATERIALIZED still holds.

import { unionAllTerms } from "./compound.ts";
import { isNameToken, quoteIdent, significant, text, tokenize, unquote, type Token } from "./lexer.ts";
import { parseCteList, type Cte } from "./limits.ts";

export interface UnionView {
  name: string;
  /** `CREATE VIEW name AS` text (no IF NOT EXISTS handling beyond what the source has). */
  head: string;
  recursive: boolean;
  ctes: Cte[];
  /** Index of the CTE the final SELECT reads. */
  union: number;
  /** Its UNION ALL terms (one term when it is not a compound). */
  terms: string[];
  /** The final SELECT, reading only ctes[union]. */
  finalSelect: string;
  /** CTE dependencies by index. */
  deps: Array<Set<number>>;
}

/** Parses a CREATE VIEW of the shape above, or returns null. */
export function parseUnionView(sql: string): UnionView | null {
  const toks = significant(sql);
  if (toks[0]?.upper !== "CREATE") return null;
  let i = 1;
  if (toks[i]?.upper === "TEMP" || toks[i]?.upper === "TEMPORARY") i += 1;
  if (toks[i]?.upper !== "VIEW") return null;
  const asIndex = toks.findIndex((t, k) => k > i && t.upper === "AS");
  if (asIndex < 0) return null;
  const nameTok = toks[asIndex - 1];
  if (!isNameToken(nameTok)) return null; // a column list `v(a,b) AS` is not this shape
  const withAt = asIndex + 1;
  if (toks[withAt]?.upper !== "WITH") return null;
  const list = parseCteList(sql, toks, withAt);
  if (!list || !list.list.length) return null;
  const finalStart = toks[list.next];
  if (!finalStart || finalStart.upper !== "SELECT") return null;
  const finalSelect = sql.slice(finalStart.start);
  const index = new Map(list.list.map((c, k) => [c.name.toLowerCase(), k]));
  const refsOf = (body: string): Set<number> => {
    const out = new Set<number>();
    for (const t of significant(body)) {
      if (!isNameToken(t)) continue;
      const k = index.get(unquote(text(body, t)).toLowerCase());
      if (k !== undefined) out.add(k);
    }
    return out;
  };
  const deps = list.list.map((c) => refsOf(c.body));
  const finalRefs = [...refsOf(finalSelect)];
  if (finalRefs.length !== 1) return null;
  const union = finalRefs[0]!;
  const body = list.list[union]!.body.trim();
  const terms = unionAllTerms(body) ?? [body];
  return {
    name: unquote(text(sql, nameTok)),
    head: sql.slice(0, toks[asIndex]!.end),
    recursive: list.recursive,
    ctes: list.list,
    union,
    terms,
    finalSelect,
    deps,
  };
}

/** CTE indices (sorted, in source order) that a set of branch terms reads, transitively. */
export function closure(view: UnionView, terms: string[]): number[] {
  const index = new Map(view.ctes.map((c, k) => [c.name.toLowerCase(), k]));
  const roots: number[] = [];
  for (const term of terms) {
    for (const t of significant(term)) {
      if (!isNameToken(t)) continue;
      const k = index.get(unquote(text(term, t)).toLowerCase());
      if (k !== undefined) roots.push(k);
    }
  }
  const seen = new Set<number>();
  const stack = [...roots];
  while (stack.length) {
    const k = stack.pop()!;
    if (seen.has(k)) continue;
    seen.add(k);
    for (const d of view.deps[k]!) stack.push(d);
  }
  if (seen.has(view.union)) throw new Error(`${view.name}: a branch reads its own union CTE`);
  return [...seen].sort((a, b) => a - b);
}

/** `CREATE VIEW name AS WITH <closure> t1 UNION ALL ...` for a group of branch terms. */
export function pieceSql(view: UnionView, name: string, terms: string[]): string {
  const needed = closure(view, terms);
  const body = terms.join("\nUNION ALL\n");
  if (!needed.length) return `CREATE VIEW ${quoteIdent(name)} AS ${body}`;
  const prefix = view.recursive ? "WITH RECURSIVE " : "WITH ";
  return `CREATE VIEW ${quoteIdent(name)} AS ${prefix}${needed.map((k) => view.ctes[k]!.text).join(",\n")}\n${body}`;
}

/**
 * The final SELECT with its reference to the union CTE replaced by `source`
 * (a parenthesized subquery or a quoted table name). The CTE name is kept as
 * the alias when the SELECT gives none, so qualified references still resolve.
 */
export function finalOver(view: UnionView, source: string): string {
  const sql = view.finalSelect;
  const target = view.ctes[view.union]!.name.toLowerCase();
  const toks = tokenize(sql);
  const sig = toks.filter((t) => t.type !== "space");
  let out = "";
  let last = 0;
  let replaced = 0;
  for (let k = 0; k < sig.length; k += 1) {
    const t = sig[k]!;
    if (!isNameToken(t) || unquote(text(sql, t)).toLowerCase() !== target) continue;
    const prev = sig[k - 1];
    if (prev && prev.type === "op" && text(sql, prev) === ".") continue;
    const next = sig[k + 1];
    if (next && next.type === "op" && text(sql, next) === ".") continue; // qualifier, keep
    const aliased = !!next && (next.upper === "AS" || (isNameToken(next) && !RESERVED_AFTER_TABLE.has(next.upper ?? "")));
    out += sql.slice(last, t.start) + (aliased ? source : `${source} AS ${quoteIdent(view.ctes[view.union]!.name)}`);
    last = t.end;
    replaced += 1;
  }
  if (replaced !== 1) throw new Error(`${view.name}: the final SELECT names its union CTE ${replaced} times`);
  return out + sql.slice(last);
}

const RESERVED_AFTER_TABLE = new Set([
  "WHERE", "GROUP", "ORDER", "LIMIT", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "CROSS", "NATURAL", "ON", "USING",
  "UNION", "EXCEPT", "INTERSECT", "WINDOW", "HAVING", "INDEXED", "NOT",
]);

/** Result column names of one SELECT term, in order. Throws on `*`. */
export function resultColumns(term: string): string[] {
  const toks = significant(term);
  let i = 0;
  if (toks[i]?.upper !== "SELECT") throw new Error("branch is not a SELECT");
  i += 1;
  if (toks[i]?.upper === "DISTINCT" || toks[i]?.upper === "ALL") i += 1;
  const items: Token[][] = [];
  let current: Token[] = [];
  let depth = 0;
  for (; i < toks.length; i += 1) {
    const t = toks[i]!;
    if (t.type === "(") depth += 1;
    else if (t.type === ")") depth -= 1;
    if (depth === 0 && t.upper === "FROM") break;
    if (depth === 0 && t.type === ",") {
      items.push(current);
      current = [];
      continue;
    }
    current.push(t);
  }
  items.push(current);
  return items.map((item) => {
    const last = item[item.length - 1];
    if (!last) throw new Error("empty result column");
    if (last.type === "op" && text(term, last) === "*") throw new Error("branch selects `*`; its columns are unknown without a catalog");
    if (!isNameToken(last)) throw new Error(`result column without a name: ${item.map((t) => text(term, t)).join(" ")}`);
    return unquote(text(term, last));
  });
}
