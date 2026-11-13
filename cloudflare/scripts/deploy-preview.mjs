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
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { deployAssembly, deleteAssembly } from "../kernel/assembly/deploy.mjs";
import { randomBytes } from "node:crypto";

const PACKAGE_DIR = join(dirname(fileURLToPath(import.meta.url)), "..");
const PREFIX = "gonvex-cf-dev-preview-";
const MAX_NAME = 54; // Leave room for the assembled companion Workers and buckets.
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
      const imagesFile = process.env.GONVEX_ASSEMBLY_IMAGES_FILE;
      if (!imagesFile) throw new Error("GONVEX_ASSEMBLY_IMAGES_FILE with exact-source immutable Container digests is required");
      const built = JSON.parse(readFileSync(imagesFile, "utf8"));
      const source = execFileSync("git", ["rev-parse", "HEAD"], { cwd: PACKAGE_DIR, encoding: "utf8" }).trim();
      if (built.source !== source) throw new Error("Container build source differs from the serving Worker checkout");
      const directory = process.env.GONVEX_ASSEMBLY_DEPLOY_DIR;
      if (!directory) throw new Error("GONVEX_ASSEMBLY_DEPLOY_DIR is required");
      const deployed = await deployAssembly({ name, directory, images: built.images, secrets: {
        GONVEX_ADMIN_KEY: randomBytes(32).toString("hex"), GONVEX_DASHBOARD_SESSION_SECRET: randomBytes(32).toString("hex"),
      } });
      const url = deployed.url;
      const body = await smoke(url);
      console.log(`preview ${name} healthy at ${url}: ${body}`);
      if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `name=${name}\nurl=${url}\n`);
      return;
    }
    case "delete":
      if (!process.env.GONVEX_ASSEMBLY_DEPLOY_DIR) throw new Error("GONVEX_ASSEMBLY_DEPLOY_DIR is required");
      await deleteAssembly({ name, directory: process.env.GONVEX_ASSEMBLY_DEPLOY_DIR });
      console.log(`deleted assembled preview ${name}`);
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
