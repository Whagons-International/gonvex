#!/usr/bin/env node
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { compareVersions, selectReleaseVersion } from "./release-version.mjs";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
const DEFAULT_MODEL = "moonshotai/kimi-k2.5";
const OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1";
const NOTE_CONTEXT_LIMIT = 75_000;
const RELEASE_PACKAGES = ["protocol", "client", "expo-sqlite", "react", "module-sdk", "gonvex", "create-gonvex"];

const args = new Set(process.argv.slice(2));
const dryRun = args.has("--dry-run") || process.env.DRY_RUN === "1";
const notesPreview = args.has("--notes-preview") || process.env.NOTES_PREVIEW === "1";
const versionInfo = args.has("--version-info");

loadDotEnv(join(ROOT, ".env"));
loadDotEnv(join(ROOT, ".env.local"));

if (versionInfo) {
  printVersionInfo();
} else if (notesPreview) {
  await generatePreviewNotes();
} else {
  await releaseCLI();
}

async function releaseCLI() {
  requireCommand("git");
  requireCommand("gh");
  requireCommand("pnpm");
  requireCommand("npm");

  if (!dryRun) ensureCleanWorkingTree();
  if (!dryRun) ensureNPMLogin();

  const latestTag = latestReleaseTag();
  const { version } = currentReleaseSelection(latestTag);
  const tagName = `v${version}`;

  if (tagExists(tagName)) throw new Error(`Tag ${tagName} already exists.`);

  console.log("=== Step 1: Determine version ===");
  console.log(`  previous tag: ${latestTag || "none"}`);
  console.log(`  version: ${version}${dryRun ? " (dry run)" : ""}`);

  const restore = dryRun ? snapshotFiles(RELEASE_PACKAGES.map(packagePath)) : undefined;
  try {
    console.log("=== Step 2: Update package versions ===");
    for (const packageName of RELEASE_PACKAGES) updatePackageVersion(packageName, version);

    console.log("=== Step 3: Validate workspace ===");
    // Scope validation to the packages being published. A whole-workspace
    // `-r` run also pulls in apps/ (docs, dashboard), whose unrelated build
    // state must not gate an npm package release.
    const releaseFilters = RELEASE_PACKAGES
      .map((packageName) => readJSON(packagePath(packageName)).name)
      .flatMap((name) => ["--filter", name]);
    // A clean release checkout has no generated dist/ declarations. Build the
    // workspace packages in dependency order before TypeScript resolves their
    // workspace exports during consumer typechecks.
    run("pnpm", [...releaseFilters, "build"]);
    run("pnpm", [...releaseFilters, "typecheck"]);
    run("cargo", [
      "test",
      "--locked",
      "--manifest-path",
      "rust/Cargo.toml",
      "--workspace",
      "--all-targets",
    ]);

    console.log("=== Step 4: Generate release notes ===");
    const notesFile = dryRun ? `/tmp/gonvex-release-notes-${version}.md` : join(ROOT, "releases", `${tagName}.md`);
    const notes = await generateReleaseNotes({ version, tagName, previousTag: latestTag, outputFile: notesFile });

    console.log("=== Step 5: Pack check ===");
    for (const packageName of RELEASE_PACKAGES) run("pnpm", ["--dir", `packages/${packageName}`, "pack", "--dry-run"]);

    console.log(`=== Step 6: Publish npm packages${dryRun ? " (dry run)" : ""} ===`);
    for (const packageName of RELEASE_PACKAGES) publishPackage(`packages/${packageName}`, dryRun);

    if (dryRun) {
      console.log("");
      console.log(`Dry run complete for ${tagName}. No files were committed, tagged, pushed, or published.`);
      console.log(`Release notes preview: ${notesFile}`);
      return;
    }

    console.log("=== Step 7: Commit, tag, push, and create GitHub release ===");
    run("git", ["add", "package.json", ...RELEASE_PACKAGES.map((packageName) => `packages/${packageName}/package.json`), "releases"]);
    run("git", ["commit", "-m", `Release ${tagName}`]);
    run("git", ["tag", "-a", tagName, "-F", notesFile]);
    run("git", ["push", "origin", "main"]);
    run("git", ["push", "origin", tagName]);
    run("gh", ["release", "create", tagName, "--title", `Release ${version}`, "--notes", notes]);

    console.log("");
    console.log(`Done: ${tagName} published to npm and GitHub.`);
  } finally {
    restore?.();
  }
}

async function generatePreviewNotes() {
  const latestTag = latestReleaseTag();
  const { version } = currentReleaseSelection(latestTag);
  const tagName = `v${version}`;
  const outputFile = process.env.RELEASE_NOTES_FILE || `/tmp/gonvex-release-notes-${version}.md`;

  console.log("=== Release notes preview ===");
  console.log(`  previous tag: ${latestTag || "none"}`);
  console.log(`  version: ${version}`);
  console.log(`  output: ${outputFile}`);

  await generateReleaseNotes({ version, tagName, previousTag: latestTag, outputFile });
}

async function generateReleaseNotes({ version, tagName, previousTag, outputFile }) {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) throw new Error("OPENROUTER_API_KEY is required in .env to generate release notes.");

  const releaseName = `Release ${version}`;
  const context = releaseContext(previousTag);
  const model = process.env.OPENROUTER_MODEL || DEFAULT_MODEL;
  const body = JSON.stringify({
    model,
    messages: [
      {
        role: "system",
        content:
          "You write concise, accurate GitHub release notes for an npm CLI project. Do not invent changes. Prefer user-facing impact first, then technical notes.",
      },
      {
        role: "user",
        content: `Create release notes for Gonvex ${tagName}.

Required Markdown format:
## Summary
- 2 to 4 bullets

## Changes
- Group notable changes in plain language

## Technical Notes
- Include npm CLI, package, schema, runtime, or release-process notes only if supported by the provided context

Release context:
${context}`,
      },
    ],
    temperature: 0.2,
  });

  const response = await fetch(`${OPENROUTER_BASE_URL}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey}`,
      "HTTP-Referer": "https://github.com/Whagons-International/gonvex",
      "X-Title": "Gonvex Release Notes",
    },
    body,
  });

  if (!response.ok) {
    throw new Error(`OpenRouter returned ${response.status} ${response.statusText}: ${await response.text()}`);
  }

  const result = await response.json();
  const notes = stripCodeFence(result?.choices?.[0]?.message?.content || "");
  if (!notes) throw new Error("OpenRouter returned empty release notes.");

  mkdirSync(dirname(outputFile), { recursive: true });
  writeFileSync(outputFile, `${notes}\n`, "utf8");
  console.log(`  wrote ${outputFile}`);
  return notes;
}

function releaseContext(previousTag) {
  const base = previousTag || EMPTY_TREE;
  const logRange = previousTag ? `${previousTag}..HEAD` : "HEAD";
  const commits = gitAllowFailure([
    "log",
    "--date=short",
    "--pretty=format:commit %h%n date: %ad%n subject: %s%n body:%n%b%n---END COMMIT---",
    logRange,
  ]) || "No committed changes found.";
  const changedFiles = gitAllowFailure(["diff", "--name-status", "--find-renames", "--find-copies", base]) || "No changed files found.";
  const diffStat = gitAllowFailure(["diff", "--stat", "--find-renames", "--find-copies", base]) || "No diff stat found.";
  const fullDiff = gitAllowFailure(["diff", "--find-renames", "--find-copies", "--minimal", base]);
  const truncatedDiff = fullDiff.length > NOTE_CONTEXT_LIMIT ? `${fullDiff.slice(0, NOTE_CONTEXT_LIMIT)}\n\n[diff truncated]` : fullDiff;

  return [
    `Previous tag: ${previousTag || "none"}`,
    "",
    "Commits:",
    commits,
    "",
    "Changed files:",
    changedFiles,
    "",
    "Diff stat:",
    diffStat,
    "",
    "Line diff:",
    truncatedDiff || "No line diff found.",
  ].join("\n");
}

function updatePackageVersion(packageName, version) {
  const path = packagePath(packageName);
  const packageJSON = readJSON(path);
  packageJSON.version = version;
  writeJSON(path, packageJSON);
  console.log(`  ${packageName}: ${version}`);
}

function snapshotFiles(paths) {
  const snapshots = paths.map((path) => [path, readFileSync(path, "utf8")]);
  return () => {
    for (const [path, content] of snapshots) {
      writeFileSync(path, content, "utf8");
    }
  };
}

function publishPackage(path, dryRunPublish) {
  const command = ["--dir", path, "publish", "--access", "public", "--no-git-checks"];
  if (dryRunPublish) command.push("--dry-run");
  run("pnpm", command);
}

function printVersionInfo() {
  const latestTag = latestReleaseTag();
  const { baselineVersion, highestPackageVersion, version } = currentReleaseSelection(latestTag);
  console.log("=== Version Info ===");
  console.log(`latest tag:      ${latestTag || "none"}`);
  console.log(`highest package: ${highestPackageVersion}`);
  console.log(`release baseline: ${baselineVersion}`);
  console.log(`next version:     ${version}`);
  console.log(`git hash:       ${git(["rev-parse", "--short", "HEAD"])}`);
}

function currentReleaseSelection(latestTag) {
  return selectReleaseVersion({
    latestTag,
    packageVersions: RELEASE_PACKAGES.map((packageName) => readJSON(packagePath(packageName)).version),
    requestedVersion: requestedVersion(),
  });
}

function latestReleaseTag() {
  const fromGH = gitAllowFailure(["-c", "pager.branch=false", "ls-remote", "--tags", "origin", "v*"])
    .split("\n")
    .map((line) => line.match(/refs\/tags\/(v\d+\.\d+\.\d+)$/)?.[1])
    .filter(Boolean)
    .sort(compareVersions)
    .at(-1);
  if (fromGH) return fromGH;
  return gitAllowFailure(["tag", "-l", "v*", "--sort=-v:refname"]).split("\n").filter(Boolean)[0] || "";
}

function tagExists(tagName) {
  return gitAllowFailure(["tag", "-l", tagName]).trim() === tagName;
}

function requestedVersion() {
  return process.env.VERSION || process.env.RELEASE_VERSION || "";
}

function ensureCleanWorkingTree() {
  const status = git(["status", "--porcelain"]);
  if (status) throw new Error(`Working tree must be clean before release. Commit or stash changes first.\n${status}`);
}

function ensureNPMLogin() {
  try {
    const user = execFileSync("npm", ["whoami"], { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
    console.log(`  npm user: ${user}`);
  } catch {
    throw new Error("npm login is required before publishing. Run: npm login");
  }
}

function requireCommand(command) {
  const result = spawnSync(command, ["--version"], { cwd: ROOT, stdio: "ignore" });
  if (result.error) throw new Error(`${command} is required for release.`);
}

function run(command, commandArgs, options = {}) {
  const result = spawnSync(command, commandArgs, { cwd: ROOT, stdio: "inherit", ...options });
  if (result.status !== 0) throw new Error(`${command} ${commandArgs.join(" ")} failed with exit code ${result.status}`);
}

function git(commandArgs) {
  return execFileSync("git", commandArgs, { cwd: ROOT, encoding: "utf8", maxBuffer: 128 * 1024 * 1024 }).trimEnd();
}

function gitAllowFailure(commandArgs) {
  try {
    return git(commandArgs);
  } catch (error) {
    const stdout = error?.stdout;
    if (typeof stdout === "string") return stdout.trimEnd();
    if (stdout) return stdout.toString("utf8").trimEnd();
    return "";
  }
}

function readJSON(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function writeJSON(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function packagePath(packageName) {
  return join(ROOT, "packages", packageName, "package.json");
}

function stripCodeFence(text) {
  return text.trim().replace(/^```(?:markdown)?\s*/i, "").replace(/```$/i, "").trim();
}

function loadDotEnv(path) {
  if (!existsSync(path)) return;
  for (const rawLine of readFileSync(path, "utf8").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const index = line.indexOf("=");
    if (index === -1) continue;
    const key = line.slice(0, index).trim();
    if (!key || process.env[key] !== undefined) continue;
    process.env[key] = line.slice(index + 1).trim().replace(/^['"]|['"]$/g, "");
  }
}
