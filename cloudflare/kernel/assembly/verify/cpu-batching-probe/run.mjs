// Deploys or deletes ONLY gonvex-cf-dev-cpuprobe-20261120 (the throwaway probe in this folder). Credentials come from
// WHAGONS_CREDENTIAL_FILE and are passed only to the wrangler child process; never printed.
//   whagons-dev credentials exec full-cloudflare-access -- node run.mjs deploy|delete
import { readFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
const here = dirname(fileURLToPath(import.meta.url));
const c = JSON.parse(await readFile(process.env.WHAGONS_CREDENTIAL_FILE));
const env = { ...process.env, CLOUDFLARE_ACCOUNT_ID: c.accountId, CLOUDFLARE_API_TOKEN: c.apiToken };
const args = process.argv[2] === "delete" ? ["delete", "--name", "gonvex-cf-dev-cpuprobe-20261120", "--force"] : ["deploy", "-c", join(here, "wrangler.jsonc")];
const r = spawnSync(join(here, "../../../../node_modules/.bin/wrangler"), args, { env, encoding: "utf8" });
console.log((r.stdout + r.stderr).split("\n").filter((l) => /Uploaded|Deployed|https:\/\/|Successfully|error/i.test(l)).join("\n"));
