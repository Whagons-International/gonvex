#!/usr/bin/env node
// Deploys or deletes the preview Worker gonvex-cf-dev-preview-<branch-slug>.
//
//   node scripts/deploy-preview.mjs deploy [--branch NAME]
//   node scripts/deploy-preview.mjs delete [--branch NAME]
//   node scripts/deploy-preview.mjs name   [--branch NAME]
//
// Credentials, never printed:
//   - arch3: whagons-dev credentials exec full-cloudflare-access -- node scripts/deploy-preview.mjs deploy
//     (reads accountId / apiToken from $WHAGONS_CREDENTIAL_FILE)
//   - CI: CLOUDFLARE_ACCOUNT_ID / CLOUDFLARE_API_TOKEN in the environment.
// They are passed only to the wrangler child process.
import { spawn, execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const PACKAGE_DIR = join(dirname(fileURLToPath(import.meta.url)), "..");
const PREFIX = "gonvex-cf-dev-preview-";
const MAX_NAME = 63; // Worker names become a DNS label on workers.dev.
const NAME_PATTERN = /^gonvex-cf-dev-preview-[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;

export function previewName(branch) {
  let slug = branch
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (!slug) throw new Error("branch name has no usable characters");
  const room = MAX_NAME - PREFIX.length;
  if (slug.length > room) {
    const hash = createHash("sha256").update(branch).digest("hex").slice(0, 6);
    slug = `${slug.slice(0, room - hash.length - 1).replace(/-+$/, "")}-${hash}`;
  }
  const name = PREFIX + slug;
  if (!NAME_PATTERN.test(name) || name.length > MAX_NAME) {
    throw new Error(`refusing unsafe preview name ${JSON.stringify(name)}`);
  }
  return name;
}

function currentBranch() {
  return (
    process.env.GITHUB_HEAD_REF ||
    execFileSync("git", ["rev-parse", "--abbrev-ref", "HEAD"], { cwd: PACKAGE_DIR, encoding: "utf8" }).trim()
  );
}

function credentials() {
  const file = process.env.WHAGONS_CREDENTIAL_FILE;
  if (file) {
    let parsed;
    try {
      parsed = JSON.parse(readFileSync(file, "utf8"));
    } catch {
      throw new Error("WHAGONS_CREDENTIAL_FILE is not valid JSON (contents not shown)");
    }
    const accountId = parsed?.accountId;
    const apiToken = parsed?.apiToken;
    if (typeof accountId !== "string" || !accountId || typeof apiToken !== "string" || !apiToken) {
      throw new Error("credential is missing accountId or apiToken");
    }
    return { accountId, apiToken };
  }
  const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
  const apiToken = process.env.CLOUDFLARE_API_TOKEN;
  if (!accountId || !apiToken) {
    throw new Error(
      "no credentials: run under `whagons-dev credentials exec full-cloudflare-access --` or set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN",
    );
  }
  return { accountId, apiToken };
}

function wrangler(args, creds) {
  const env = { ...process.env, WRANGLER_SEND_METRICS: "false" };
  delete env.WHAGONS_CREDENTIAL_FILE;
  env.CLOUDFLARE_ACCOUNT_ID = creds.accountId;
  env.CLOUDFLARE_API_TOKEN = creds.apiToken;
  return new Promise((resolve, reject) => {
    const child = spawn("pnpm", ["exec", "wrangler", ...args], {
      cwd: PACKAGE_DIR,
      env,
      stdio: ["ignore", "pipe", "inherit"],
    });
    let stdout = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      process.stdout.write(chunk);
    });
    child.on("error", reject);
    child.on("close", (code) =>
      code === 0 ? resolve(stdout) : reject(new Error(`wrangler ${args[0]} exited with ${code}`)),
    );
  });
}

async function smoke(url) {
  // A fresh workers.dev route can take a few seconds to answer.
  const deadline = Date.now() + 90_000;
  let last = "no response";
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      const body = await response.text();
      if (response.ok && body.includes('"ok":true')) return body;
      last = `HTTP ${response.status}`;
    } catch (error) {
      last = error instanceof Error ? error.message : String(error);
    }
    await new Promise((resolve) => setTimeout(resolve, 3_000));
  }
  throw new Error(`preview did not become healthy at ${url}: ${last}`);
}

async function main() {
  const [command, ...rest] = process.argv.slice(2);
  const branchFlag = rest.indexOf("--branch");
  const branch = branchFlag >= 0 ? rest[branchFlag + 1] : currentBranch();
  if (!branch) throw new Error("--branch needs a value");
  const name = previewName(branch);

  switch (command) {
    case "name":
      console.log(name);
      return;
    case "deploy": {
      const output = await wrangler(["deploy", "--name", name], credentials());
      const url = output.match(/https:\/\/[a-z0-9.-]+\.workers\.dev/)?.[0];
      if (!url) throw new Error("deployed, but wrangler printed no workers.dev URL");
      const body = await smoke(url);
      console.log(`preview ${name} healthy at ${url}: ${body}`);
      if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `name=${name}\nurl=${url}\n`);
      return;
    }
    case "delete":
      await wrangler(["delete", "--name", name, "--force"], credentials());
      console.log(`deleted preview ${name}`);
      return;
    default:
      throw new Error("usage: deploy-preview.mjs deploy|delete|name [--branch NAME]");
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
