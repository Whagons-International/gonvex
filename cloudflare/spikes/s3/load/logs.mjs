// Query Workers Logs (observability) for this Worker. Run under bin/cf-env.sh.
//   node load/logs.mjs <fromISO> <toISO> [limit] [--raw]
const [from, to, limit = "5"] = process.argv.slice(2);
const res = await fetch(`https://api.cloudflare.com/client/v4/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/workers/observability/telemetry/query`, {
  method: "POST",
  headers: { authorization: `Bearer ${process.env.CLOUDFLARE_API_TOKEN}`, "content-type": "application/json" },
  body: JSON.stringify({
    queryId: "s3-spike",
    timeframe: { from: Date.parse(from), to: Date.parse(to) },
    view: "events",
    limit: Number(limit),
    parameters: { filters: [{ key: "$metadata.service", operation: "eq", type: "string", value: "gonvex-cf-dev-s3" }] },
  }),
});
const body = await res.json();
console.log(JSON.stringify(body, null, 1).slice(0, Number(process.env.MAXCHARS ?? 1e9)));
