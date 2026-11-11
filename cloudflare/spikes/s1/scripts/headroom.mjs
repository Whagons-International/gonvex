// JS memory a freshly woken object can hold before the platform restarts its
// isolate. The platform does not fail the request: the next request lands on a
// new instance, so a restart shows as the held total dropping. Compare the
// loaded tenant object with an empty one; the difference is what the tenant's
// SQLite state takes from the isolate's budget.
// Usage: node scripts/headroom.mjs <base> <object> [stepMiB] [rounds]
const [base, object, stepArg, roundsArg] = process.argv.slice(2);
const step = Number(stepArg || 8);
const call = async (route, body = {}) => {
  const r = await fetch(`${base}${route}?o=${encodeURIComponent(object)}`, { method: "POST", headers: { "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify(body) });
  return r.json().catch(() => ({ error: `HTTP ${r.status}` }));
};
const results = [];
for (let round = 0; round < Number(roundsArg || 2); round += 1) {
  await call("/abort");
  await new Promise((r) => setTimeout(r, 2000));
  const wake = await call("/wake", { sql: "SELECT count(*) FROM sqlite_schema" });
  let held = 0;
  for (;;) {
    const r = await call("/alloc", { mb: step });
    if (r.error || r.heldMiB === undefined || r.heldMiB <= held) break;
    held = r.heldMiB;
    if (held >= 1024) break;
  }
  results.push(held);
}
console.log(JSON.stringify({ object, stepMiB: step, maxHeldMiBBeforeRestart: results }));
