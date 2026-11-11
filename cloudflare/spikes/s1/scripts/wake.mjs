// Cold and warm wake of a loaded tenant object, timed from the client.
// /abort restarts the object, so the next request runs a fresh constructor,
// reopens SQLite and parses the whole schema on its first statement.
// Usage: node scripts/wake.mjs <base> <object> [rounds] [out.json] [--pid <workerd pid>]
import { readFileSync, writeFileSync } from "node:fs";

const [base, object, roundsArg, outPath] = process.argv.slice(2);
const rounds = Number(roundsArg || 5);
const pidIndex = process.argv.indexOf("--pid");
const pids = pidIndex > 0 ? process.argv[pidIndex + 1].split(",") : [];
const rss = () => pids.reduce((sum, pid) => {
  try { return sum + Number(/VmRSS:\s+(\d+)/.exec(readFileSync(`/proc/${pid}/status`, "utf8"))[1]) * 1024; } catch { return sum; }
}, 0);

// A grid page and a record-view page: the first reads a client makes.
const QUERY = "SELECT g.rowid, g.name, t.\"_id\", t.\"statusId\" FROM taskGridRows g JOIN tasks t ON t.\"_id\" = g.\"taskId\" ORDER BY g.\"updatedAt\" DESC LIMIT 50";

async function call(route, body = {}, target = object) {
  const started = performance.now();
  const response = await fetch(`${base}${route}?o=${encodeURIComponent(target)}`, { method: "POST",
    headers: { "content-type": "application/json", "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify(body) });
  const value = await response.json().catch(() => ({}));
  return { ...value, ms: performance.now() - started };
}

const ping = [];
for (let i = 0; i < 5; i += 1) ping.push((await call("/wake", {}, "s1-empty")).ms);
const results = [];
for (let r = 0; r < rounds; r += 1) {
  await call("/abort");
  await new Promise((resolve) => setTimeout(resolve, 1500));
  const rssBefore = rss();
  const cold = await call("/wake", { sql: QUERY });
  const rssAfterCold = rss();
  const warm = await call("/wake", { sql: QUERY });
  const entry = { round: r, coldMs: Math.round(cold.ms), fresh: cold.fresh, rows: cold.rows, warmMs: Math.round(warm.ms), error: cold.error,
    rssBeforeMB: pids.length ? +(rssBefore / 2 ** 20).toFixed(1) : undefined, rssAfterColdMB: pids.length ? +(rssAfterCold / 2 ** 20).toFixed(1) : undefined };
  results.push(entry);
  console.log(JSON.stringify(entry));
}
const median = (xs) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const summary = {
  object, rounds, emptyObjectRoundTripMs: Math.round(median(ping)),
  coldMedianMs: median(results.map((r) => r.coldMs)), coldMaxMs: Math.max(...results.map((r) => r.coldMs)),
  warmMedianMs: median(results.map((r) => r.warmMs)), allFresh: results.every((r) => r.fresh), results,
};
console.log(JSON.stringify({ ...summary, results: undefined }));
if (outPath && !outPath.startsWith("--")) writeFileSync(outPath, JSON.stringify(summary, null, 2) + "\n");
