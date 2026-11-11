// Deploys or deletes the K02 verification Worker `gonvex-cf-dev-k02` on the development account.
//
//   whagons-dev credentials exec full-cloudflare-access -- node kernel/migrate/verify/deploy.mjs deploy <token-file>
//   whagons-dev credentials exec full-cloudflare-access -- node kernel/migrate/verify/deploy.mjs delete
//
// `deploy` writes a random request token to <token-file> (keep it outside the repo) and sets it as the
// Worker's TOKEN secret; provision.mjs sends it from $K02_TOKEN. Never prints credentials.
import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const NAME = "gonvex-cf-dev-k02";
const here = dirname(fileURLToPath(import.meta.url));
const config = join(here, "worker/wrangler.jsonc");
const [action, tokenFile] = process.argv.slice(2);

function env() {
  const file = process.env.WHAGONS_CREDENTIAL_FILE;
  if (!file) throw new Error("run under `whagons-dev credentials exec full-cloudflare-access --`");
  const { accountId, apiToken } = JSON.parse(readFileSync(file, "utf8"));
  if (!accountId || !apiToken) throw new Error("credential is missing accountId or apiToken");
  return { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId, CLOUDFLARE_API_TOKEN: apiToken };
}

const wrangler = (args, input) =>
  execFileSync("npx", ["wrangler", ...args, "--config", config], { env: env(), input, stdio: [input === undefined ? "inherit" : "pipe", "pipe", "pipe"], cwd: join(here, "../../..") }).toString();

if (action === "deploy") {
  if (!tokenFile) throw new Error("usage: deploy <token-file>");
  const out = wrangler(["deploy", "--name", NAME]);
  const url = /https:\/\/\S+\.workers\.dev/.exec(out)?.[0];
  const token = randomBytes(24).toString("hex");
  wrangler(["secret", "put", "TOKEN", "--name", NAME], token);
  writeFileSync(tokenFile, token, { mode: 0o600 });
  console.log(JSON.stringify({ deployed: NAME, url, upload: /Total Upload: [^\n]+/.exec(out)?.[0] }));
} else if (action === "delete") {
  wrangler(["delete", "--name", NAME, "--force"]);
  console.log(JSON.stringify({ deleted: NAME }));
} else {
  console.error("usage: deploy.mjs deploy <token-file> | delete");
  process.exit(2);
}
