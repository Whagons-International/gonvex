import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import test from "node:test";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { compareVersions, selectReleaseVersion } from "./release-version.mjs";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");

test("release builds workspace declarations before typechecking consumers", () => {
  const source = readFileSync(resolve(ROOT, "scripts/release-cli.mjs"), "utf8");
  const build = source.indexOf('run("pnpm", [...releaseFilters, "build"]);');
  const typecheck = source.indexOf('run("pnpm", [...releaseFilters, "typecheck"]);');

  assert.ok(build >= 0, "release build step is missing");
  assert.ok(typecheck > build, "consumer typechecks must run after dependency declarations are built");
});

test("release validates the Rust workspace", () => {
  const source = readFileSync(resolve(ROOT, "scripts/release-cli.mjs"), "utf8");
  assert.match(source, /"rust\/Cargo\.toml"/);
  assert.match(source, /"--workspace"/);
  assert.doesNotMatch(source, /run\("go"/);
});

test("release includes every public Gonvex SDK needed by TypeScript modules and mobile replicas", () => {
  const source = readFileSync(resolve(ROOT, "scripts/release-cli.mjs"), "utf8");
  assert.match(source, /"module-sdk"/);
  assert.match(source, /"expo-sqlite"/);
});

test("selects a version above every package when the release tag is stale", () => {
  const selection = selectReleaseVersion({
    latestTag: "v0.1.0",
    packageVersions: ["0.1.2", "0.1.2", "0.1.2", "0.1.7", "0.1.1"],
  });

  assert.deepEqual(selection, {
    baselineVersion: "0.1.7",
    highestPackageVersion: "0.1.7",
    version: "0.1.8",
  });
});

test("selects a version above the tag when the tag is newer than every package", () => {
  const selection = selectReleaseVersion({
    latestTag: "v0.2.4",
    packageVersions: ["0.1.2", "0.1.7"],
  });

  assert.equal(selection.baselineVersion, "0.2.4");
  assert.equal(selection.version, "0.2.5");
});

test("rejects an explicit release version that would not advance the release", () => {
  assert.throws(
    () =>
      selectReleaseVersion({
        latestTag: "v0.1.0",
        packageVersions: ["0.1.2", "0.1.7"],
        requestedVersion: "0.1.7",
      }),
    /must be greater than the current release baseline 0\.1\.7/,
  );
});

test("version info reports a next version greater than the checked-in packages and tag", () => {
  const result = spawnSync(process.execPath, ["scripts/release-cli.mjs", "--version-info"], {
    cwd: ROOT,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, result.stderr);
  const latestTag = result.stdout.match(/latest tag:\s+v?(\d+\.\d+\.\d+)|latest tag:\s+(none)/)?.[1];
  const highestPackage = result.stdout.match(/highest package:\s+(\d+\.\d+\.\d+)/)?.[1];
  const nextVersion = result.stdout.match(/next version:\s+(\d+\.\d+\.\d+)/)?.[1];

  assert.ok(highestPackage, result.stdout);
  assert.ok(nextVersion, result.stdout);
  assert.ok(compareVersions(nextVersion, highestPackage) > 0, result.stdout);
  if (latestTag) assert.ok(compareVersions(nextVersion, latestTag) > 0, result.stdout);
});
