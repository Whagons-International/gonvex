// Query the Cloudflare GraphQL Analytics API. Run under bin/cf-env.sh. Prints no credentials.
//   node load/gql.mjs '<query>' '<variables json>'
const q = process.argv[2];
const vars = JSON.parse(process.argv[3] ?? "{}");
vars.accountTag ??= process.env.CLOUDFLARE_ACCOUNT_ID;
const res = await fetch("https://api.cloudflare.com/client/v4/graphql", {
  method: "POST",
  headers: { authorization: `Bearer ${process.env.CLOUDFLARE_API_TOKEN}`, "content-type": "application/json" },
  body: JSON.stringify({ query: q, variables: vars }),
});
console.log(JSON.stringify(await res.json(), null, 1));
