// Workers Logs (observability) events for the S5 Worker with an error outcome or level. Run under bin/cf-env.sh.
//   node test/logs.mjs <fromISO> <toISO> [limit]
const [from, to, limit = "50"] = process.argv.slice(2);
const res = await fetch(`https://api.cloudflare.com/client/v4/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/workers/observability/telemetry/query`, {
  method: "POST",
  headers: { authorization: `Bearer ${process.env.CLOUDFLARE_API_TOKEN}`, "content-type": "application/json" },
  body: JSON.stringify({
    queryId: "s5-spike", timeframe: { from: Date.parse(from), to: Date.parse(to) }, view: "events", limit: Number(limit),
    parameters: { filters: [{ key: "$metadata.service", operation: "eq", type: "string", value: process.env.S5_SERVICE ?? "gonvex-cf-dev-s5" }] },
  }),
});
const body = await res.json();
const events = body.result?.events?.events ?? [];
const bad = events.filter((e) => (e.$workers?.outcome && e.$workers.outcome !== "ok") || e.$metadata?.level === "error" || e.$metadata?.error);
console.log(JSON.stringify({
  total: events.length, errors: body.errors,
  outcomes: events.reduce((m, e) => ((m[e.$workers?.outcome ?? "?"] = (m[e.$workers?.outcome ?? "?"] ?? 0) + 1), m), {}),
  bad: bad.map((e) => ({ t: new Date(e.timestamp).toISOString(), outcome: e.$workers?.outcome, path: e.$workers?.event?.request?.path ?? e.$workers?.event?.rpcMethod, entry: e.$workers?.entrypoint, error: e.$metadata?.error, message: String(e.$metadata?.message ?? "").slice(0, 300) })),
}, null, 1));
