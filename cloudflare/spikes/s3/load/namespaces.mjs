// List this Worker's Durable Object namespaces. Run under bin/cf-env.sh.
const res = await fetch(`https://api.cloudflare.com/client/v4/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/workers/durable_objects/namespaces?per_page=1000`, {
  headers: { authorization: `Bearer ${process.env.CLOUDFLARE_API_TOKEN}` },
});
const body = await res.json();
console.log(JSON.stringify((body.result ?? []).filter((n) => n.script === "gonvex-cf-dev-s3").map((n) => ({ id: n.id, class: n.class, name: n.name }))));
