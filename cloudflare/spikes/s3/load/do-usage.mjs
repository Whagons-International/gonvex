// Per-minute usage of this Worker's Durable Objects from the GraphQL Analytics API.
// Run under bin/cf-env.sh:  node load/do-usage.mjs <sinceISO> <untilISO> [nameFilter]
const [since, until, nameFilter] = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const nsRes = await fetch(`https://api.cloudflare.com/client/v4/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/workers/durable_objects/namespaces?per_page=1000`, {
  headers: { authorization: `Bearer ${process.env.CLOUDFLARE_API_TOKEN}` },
});
const ns = ((await nsRes.json()).result ?? []).filter((n) => n.script === "gonvex-cf-dev-s3");
const query = `query($accountTag: string!, $since: Time!, $until: Time!, $ns: [string!]) { viewer { accounts(filter:{accountTag:$accountTag}) {
  durableObjectsPeriodicGroups(limit: 2000, filter:{datetime_geq:$since, datetime_leq:$until, namespaceId_in:$ns}, orderBy:[datetimeMinute_ASC]) {
    dimensions { datetimeMinute name objectId actorMemoryMb coloCode namespaceId }
    sum { activeTime cpuTime duration inboundWebsocketMsgCount outboundWebsocketMsgCount rowsWritten rowsRead subrequests exceededCpuErrors exceededMemoryErrors fatalInternalErrors }
  }
  durableObjectsInvocationsAdaptiveGroups(limit: 2000, filter:{datetime_geq:$since, datetime_leq:$until, namespaceId_in:$ns}, orderBy:[datetimeMinute_ASC]) {
    dimensions { datetimeMinute objectId status }
    sum { requests errors responseBodySize wallTime }
    quantiles { wallTimeP50 wallTimeP99 }
  }
} } }`;
const res = await fetch("https://api.cloudflare.com/client/v4/graphql", {
  method: "POST",
  headers: { authorization: `Bearer ${process.env.CLOUDFLARE_API_TOKEN}`, "content-type": "application/json" },
  body: JSON.stringify({ query, variables: { accountTag: process.env.CLOUDFLARE_ACCOUNT_ID, since, until, ns: ns.map((n) => n.id) } }),
});
const body = await res.json();
if (body.errors) {
  console.log(JSON.stringify(body.errors));
  process.exit(1);
}
const acc = body.data.viewer.accounts[0];
const cls = Object.fromEntries(ns.map((n) => [n.id, n.class]));
const rows = acc.durableObjectsPeriodicGroups
  .filter((g) => !nameFilter || (g.dimensions.name ?? "").includes(nameFilter))
  .map((g) => ({
    minute: g.dimensions.datetimeMinute.slice(11, 16),
    class: cls[g.dimensions.namespaceId],
    name: g.dimensions.name,
    colo: g.dimensions.coloCode,
    memMb: g.dimensions.actorMemoryMb,
    cpuMs: Math.round(g.sum.cpuTime / 1000),
    activeMs: Math.round(g.sum.activeTime / 1000),
    gbs: +g.sum.duration.toFixed(3),
    wsIn: g.sum.inboundWebsocketMsgCount,
    wsOut: g.sum.outboundWebsocketMsgCount,
    rowsW: g.sum.rowsWritten,
    sub: g.sum.subrequests,
    cpuErr: g.sum.exceededCpuErrors,
    memErr: g.sum.exceededMemoryErrors,
    fatal: g.sum.fatalInternalErrors,
  }));
if (process.argv.includes("--json")) console.log(JSON.stringify({ periodic: rows, invocations: acc.durableObjectsInvocationsAdaptiveGroups }));
else {
  console.table(rows);
  const inv = acc.durableObjectsInvocationsAdaptiveGroups.map((g) => ({ minute: g.dimensions.datetimeMinute.slice(11, 16), obj: g.dimensions.objectId.slice(0, 8), status: g.dimensions.status, req: g.sum.requests, err: g.sum.errors, wallP50: g.quantiles.wallTimeP50, wallP99: g.quantiles.wallTimeP99 }));
  console.table(inv);
}
