#!/usr/bin/env node
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { RELEASE_PACKAGES } from "./release-packages.mjs";
import { compareVersions, validateVersion } from "./release-version.mjs";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const REPO = "Whagons-International/gonvex";

export function assertStagingCandidate({ version, sha, head, staging, clean, versions, checks }) {
  validateVersion(version);
  if (!/^\d+\.\d+\.\d+-staging\.\d+$/.test(version)) throw new Error("A staging prerelease version is required");
  if (!/^[0-9a-f]{40}$/.test(sha) || head !== sha || staging !== sha) {
    throw new Error("Release SHA must be the exact checked-out origin/staging commit");
  }
  if (!clean) throw new Error("Release working tree must be clean");
  if (versions.length !== RELEASE_PACKAGES.length || versions.some(value => value !== version)) {
    throw new Error("All public packages must have the requested staging version committed");
  }
  const ci = checks.filter(check => check.name === "CI").sort((a, b) => b.id - a.id)[0];
  if (!ci || ci.status !== "completed" || ci.conclusion !== "success") {
    throw new Error("Successful CI on the exact staging SHA is required");
  }
}

function release(args) {
  const version = args[args.indexOf("--version") + 1];
  validateVersion(version ?? "");
  if (!/^\d+\.\d+\.\d+-staging\.\d+$/.test(version)) throw new Error("A staging prerelease version is required");
  const manifests = RELEASE_PACKAGES.map(directory => {
    const path = join(ROOT, "packages", directory, "package.json");
    return { directory, path, manifest: JSON.parse(readFileSync(path, "utf8")) };
  });
  if (args.includes("--prepare") && !args.includes("--publish")) {
    const branch = output("git", ["branch", "--show-current"]);
    if (!branch || ["main", "production", "staging"].includes(branch)) throw new Error("Prepare the version on a feature branch for a staging PR");
    for (const { manifest } of manifests) {
      if (compareVersions(version, manifest.version) < 0) throw new Error("Cannot decrease a package version");
    }
    // Preparation changes only manifests. Commit release notes and the lockfile
    // alongside these in the PR; publication never commits or pushes a branch.
    for (const { path, manifest } of manifests) {
      writeFileSync(path, `${JSON.stringify({ ...manifest, version }, null, 2)}\n`);
    }
    console.log(`Prepared ${manifests.length} packages at ${version} for a staging PR`);
    return;
  }
  if (!args.includes("--publish") || args.includes("--prepare") || !args.includes("--sha")) {
    throw new Error("usage: --prepare --version X.Y.Z-staging.N | --publish --version X.Y.Z-staging.N --sha FULL_SHA [--dry-run]");
  }
  const sha = args[args.indexOf("--sha") + 1];
  run("git", ["fetch", "origin", "staging"]);
  const pages = JSON.parse(output("gh", ["api", `repos/${REPO}/commits/${sha}/check-runs`, "--paginate", "--slurp"]));
  assertStagingCandidate({
    version, sha, head: output("git", ["rev-parse", "HEAD"]), staging: output("git", ["rev-parse", "FETCH_HEAD"]),
    clean: !output("git", ["status", "--porcelain"]), versions: manifests.map(({ manifest }) => manifest.version),
    checks: pages.flatMap(page => page.check_runs),
  });
  const tag = `v${version}`;
  const notes = join(ROOT, "releases", `${tag}.md`);
  if (!readFileSync(notes, "utf8").trim()) throw new Error("Committed release notes are required");
  const remoteTag = output("git", ["ls-remote", "origin", `refs/tags/${tag}`, `refs/tags/${tag}^{}`]);
  if (remoteTag && !remoteTag.split("\n").some(line => line === `${sha}\trefs/tags/${tag}^{}` || line === `${sha}\trefs/tags/${tag}`)) {
    throw new Error("Existing release tag identifies another commit");
  }
  const destination = mkdtempSync(join(tmpdir(), "gonvex-staging-release-"));
  try {
    const tarballs = manifests.map(({ directory, manifest }) => {
      const staging = npmJSON(["view", manifest.name, "dist-tags.staging", "--json"]);
      if (staging && compareVersions(staging, version) > 0) throw new Error(`${manifest.name} already has a newer staging release`);
      run("pnpm", ["--dir", `packages/${directory}`, "pack", "--pack-destination", destination]);
      const tarball = join(destination, `${manifest.name.replace(/^@/, "").replace("/", "-")}-${version}.tgz`);
      const integrity = `sha512-${createHash("sha512").update(readFileSync(tarball)).digest("base64")}`;
      const published = npmJSON(["view", `${manifest.name}@${version}`, "dist.integrity", "--json"]);
      if (published && published !== integrity) throw new Error(`${manifest.name}@${version} already exists with different contents`);
      return { manifest, tarball, integrity, published };
    });
    if (readdirSync(destination).length !== manifests.length) throw new Error("Incomplete staging package set");
    if (args.includes("--dry-run")) {
      console.log(`Verified staging release ${version} at ${sha}; nothing published or tagged`);
      return;
    }
    output("npm", ["whoami"]);
    for (const { manifest, tarball, integrity, published } of tarballs) {
      if (!published) run("npm", ["publish", tarball, "--access", "public", "--tag", "staging"]);
      else run("npm", ["dist-tag", "add", `${manifest.name}@${version}`, "staging"]);
      if (npmJSON(["view", `${manifest.name}@${version}`, "dist.integrity", "--json"]) !== integrity) {
        throw new Error(`Published integrity mismatch for ${manifest.name}`);
      }
    }
    if (!remoteTag) {
      const localTag = spawnSync("git", ["rev-parse", "--verify", `refs/tags/${tag}^{}`], { cwd: ROOT, encoding: "utf8" });
      if (localTag.status === 0 && localTag.stdout.trim() !== sha) throw new Error("Local release tag identifies another commit");
      if (localTag.status !== 0) run("git", ["tag", "-a", tag, sha, "-F", notes]);
      run("git", ["push", "origin", `refs/tags/${tag}`]);
    }
    const existing = spawnSync("gh", ["release", "view", tag, "--repo", REPO], { cwd: ROOT, stdio: "ignore" });
    if (existing.status !== 0) run("gh", ["release", "create", tag, "--repo", REPO, "--verify-tag", "--prerelease", "--latest=false", "--title", tag, "--notes-file", notes]);
    console.log(`Released ${version} on npm's staging tag and GitHub at ${sha}`);
  } finally {
    rmSync(destination, { recursive: true, force: true });
  }
}

function run(command, args) { execFileSync(command, args, { cwd: ROOT, stdio: "inherit" }); }
function output(command, args) { return execFileSync(command, args, { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim(); }
function npmJSON(args) {
  const result = spawnSync("npm", args, { cwd: ROOT, encoding: "utf8" });
  if (result.status !== 0) {
    if (result.stderr.includes("E404")) return undefined;
    throw new Error(`npm metadata lookup failed: ${result.stderr}`);
  }
  return result.stdout.trim() ? JSON.parse(result.stdout) : undefined;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { release(process.argv.slice(2)); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
