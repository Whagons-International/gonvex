// Longest chain of CTEs (each reading the previous) and of views (each
// selecting from the previous) that a query may read.
// Usage: node scripts/nesting.mjs <base>
const base = process.argv[2];
const object = `nesting-${Date.now()}`;
const call = async (route, body) => (await fetch(`${base}${route}?o=${object}`, { method: "POST", headers: { "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify(body) })).json();
const ok = (r) => !r.error && r.ok !== false;
async function largest(lo, hi, test) {
  if (!(await test(lo))) return lo - 1;
  while (lo < hi) { const mid = Math.ceil((lo + hi) / 2); if (await test(mid)) lo = mid; else hi = mid - 1; }
  return lo;
}
const materialized = await largest(2, 1000, async (n) => {
  const ctes = Array.from({ length: n }, (_, i) => (i === 0 ? "r0 AS MATERIALIZED (SELECT 1 AS x)" : `r${i} AS MATERIALIZED (SELECT x + 1 AS x FROM r${i - 1})`));
  return ok(await call("/query", { sql: `WITH ${ctes.join(", ")} SELECT x FROM r${n - 1}` }));
});
const plain = await largest(2, 1000, async (n) => {
  const ctes = Array.from({ length: n }, (_, i) => (i === 0 ? "r0 AS (SELECT 1 AS x)" : `r${i} AS (SELECT x + 1 AS x FROM r${i - 1})`));
  return ok(await call("/query", { sql: `WITH ${ctes.join(", ")} SELECT x FROM r${n - 1}` }));
});
await call("/exec", { statements: Array.from({ length: 400 }, (_, i) => (i === 0 ? "CREATE VIEW v0 AS SELECT 1 AS x" : `CREATE VIEW v${i} AS SELECT x + 1 AS x FROM v${i - 1}`)) });
const views = await largest(1, 399, async (n) => ok(await call("/query", { sql: `SELECT x FROM v${n}` })));
await call("/reset", {});
console.log(JSON.stringify({ materializedCteChain: materialized, plainCteChain: plain, viewChain: views + 1 }));
