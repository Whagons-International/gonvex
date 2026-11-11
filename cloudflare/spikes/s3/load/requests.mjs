// Billed requests per object (durableObjectsInvocationsAdaptiveGroups) joined to object names. Run under bin/cf-env.sh.
const [since, until] = process.argv.slice(2);
const auth = { authorization: `Bearer ${process.env.CLOUDFLARE_API_TOKEN}`, "content-type": "application/json" };
const ns = ((await (await fetch(`https://api.cloudflare.com/client/v4/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/workers/durable_objects/namespaces?per_page=1000`, { headers: auth })).json()).result ?? []).filter((n) => n.script === "gonvex-cf-dev-s3");
const q = `query($a: string!, $s: Time!, $u: Time!, $ns: [string!]) { viewer { accounts(filter:{accountTag:$a}) {
  p: durableObjectsPeriodicGroups(limit: 5000, filter:{datetime_geq:$s, datetime_leq:$u, namespaceId_in:$ns}) { dimensions { name objectId } }
  i: durableObjectsInvocationsAdaptiveGroups(limit: 5000, filter:{datetime_geq:$s, datetime_leq:$u, namespaceId_in:$ns}) { dimensions { objectId } sum { requests } }
} } }`;
const body = await (await fetch("https://api.cloudflare.com/client/v4/graphql", { method: "POST", headers: auth, body: JSON.stringify({ query: q, variables: { a: process.env.CLOUDFLARE_ACCOUNT_ID, s: since, u: until, ns: ns.map((n) => n.id) } }) })).json();
if (body.errors) throw new Error(JSON.stringify(body.errors));
const a = body.data.viewer.accounts[0];
const names = Object.fromEntries(a.p.map((g) => [g.dimensions.objectId, g.dimensions.name]));
const req = {};
for (const g of a.i) req[names[g.dimensions.objectId] ?? g.dimensions.objectId] = (req[names[g.dimensions.objectId] ?? g.dimensions.objectId] ?? 0) + g.sum.requests;
console.log(JSON.stringify(req, null, 1));
