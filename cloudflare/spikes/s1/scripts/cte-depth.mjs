// Longest CTE dependency path of every view with a WITH clause (the platform
// refuses a query that reads a chain of more than 99 CTEs).
// Usage: node scripts/cte-depth.mjs <tenant.db>
import { DatabaseSync } from "node:sqlite";
import { tokenize, unquote } from "./sql.mjs";

const db = new DatabaseSync(process.argv[2], { readOnly: true });
const out = [];
for (const { name, sql } of db.prepare("SELECT name, sql FROM sqlite_schema WHERE type='view' AND sql LIKE '%WITH%'").all()) {
  const toks = tokenize(sql).filter((t) => t.type !== "space");
  let i = toks.findIndex((t) => t.upper === "WITH");
  if (i < 0) continue;
  i += 1;
  if (toks[i].upper === "RECURSIVE") i += 1;
  const ctes = [];
  for (;;) {
    const cteName = unquote(sql.slice(toks[i].start, toks[i].end));
    while (toks[i].upper !== "AS") i += 1;
    i += 1;
    while (toks[i].type === "word") i += 1;
    const open = i;
    let depth = 0;
    for (; ; i += 1) { if (toks[i].type === "(") depth += 1; else if (toks[i].type === ")" && --depth === 0) break; }
    ctes.push({ name: cteName, body: sql.slice(toks[open].end, toks[i].start) });
    i += 1;
    if (toks[i] && toks[i].type === ",") { i += 1; continue; }
    break;
  }
  const index = new Map(ctes.map((c, k) => [c.name, k]));
  const deps = ctes.map((c) => [...new Set(tokenize(c.body).filter((t) => t.type === "word" && index.has(c.body.slice(t.start, t.end))).map((t) => index.get(c.body.slice(t.start, t.end))))]);
  const longest = [];
  for (let k = 0; k < ctes.length; k += 1) longest[k] = 1 + Math.max(0, ...deps[k].map((d) => longest[d]));
  out.push({ view: name, ctes: ctes.length, longestChain: Math.max(...longest) });
}
out.sort((a, b) => b.longestChain - a.longestChain);
console.log(JSON.stringify({ views: out.length, over99: out.filter((v) => v.longestChain > 99), top: out.slice(0, 12) }, null, 1));
