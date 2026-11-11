// Aggregate Workers Logs for this Worker: count, CPU and wall time grouped by fields. Run under bin/cf-env.sh.
//   node load/logs-calc.mjs <fromISO> <toISO> [groupBy,...] [filterKey=value]
const [from, to, groups = "$workers.eventType,$workers.executionModel", filt] = process.argv.slice(2);
const filters = [{ key: "$metadata.service", operation: "eq", type: "string", value: "gonvex-cf-dev-s3" }];
if (filt) {
  const [k, v] = filt.split("=");
  filters.push({ key: k, operation: "eq", type: "string", value: v });
}
const res = await fetch(`https://api.cloudflare.com/client/v4/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/workers/observability/telemetry/query`, {
  method: "POST",
  headers: { authorization: `Bearer ${process.env.CLOUDFLARE_API_TOKEN}`, "content-type": "application/json" },
  body: JSON.stringify({
    queryId: "s3-spike-calc",
    timeframe: { from: Date.parse(from), to: Date.parse(to) },
    view: "calculations",
    parameters: {
      filters,
      groupBys: groups.split(",").map((value) => ({ type: "string", value })),
      calculations: [
        { operator: "count", alias: "n" },
        { operator: "sum", key: "$workers.cpuTimeMs", keyType: "number", alias: "cpuSum" },
        { operator: "median", key: "$workers.cpuTimeMs", keyType: "number", alias: "cpuP50" },
        { operator: "p99", key: "$workers.cpuTimeMs", keyType: "number", alias: "cpuP99" },
        { operator: "max", key: "$workers.cpuTimeMs", keyType: "number", alias: "cpuMax" },
        { operator: "median", key: "$workers.wallTimeMs", keyType: "number", alias: "wallP50" },
        { operator: "p99", key: "$workers.wallTimeMs", keyType: "number", alias: "wallP99" },
      ],
    },
  }),
});
const body = await res.json();
if (!body.success) {
  console.log(JSON.stringify(body.errors ?? body).slice(0, 2000));
  process.exit(1);
}
const calcs = body.result.calculations ?? [];
const rows = {};
for (const c of calcs) {
  for (const a of c.aggregates ?? []) {
    const key = (a.groups ?? []).map((g) => g.value).join(" | ") || "(all)";
    rows[key] ??= {};
    rows[key][c.alias ?? c.calculation] = Math.round(a.value * 100) / 100;
  }
}
if (process.argv.includes("--json")) console.log(JSON.stringify(rows));
else console.table(rows);
