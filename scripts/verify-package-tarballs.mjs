#!/usr/bin/env node

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { RELEASE_PACKAGES } from "./release-packages.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packageDirectories = RELEASE_PACKAGES;
const destination = mkdtempSync(join(tmpdir(), "gonvex-package-tarballs-"));

try {
  const sourcePackages = packageDirectories.map((directory) => ({
    directory,
    manifest: JSON.parse(
      readFileSync(join(root, "packages", directory, "package.json"), "utf8"),
    ),
  }));
  const versions = new Set(sourcePackages.map(({ manifest }) => manifest.version));
  assert.equal(versions.size, 1, "publishable Gonvex packages must use one version");
  const [version] = versions;

  for (const { directory, manifest: source } of sourcePackages) {
    const before = new Set(readdirSync(destination));
    execFileSync(
      "pnpm",
      ["--dir", join("packages", directory), "pack", "--pack-destination", destination],
      { cwd: root, stdio: "pipe" },
    );
    const created = readdirSync(destination).filter(
      (name) => name.endsWith(".tgz") && !before.has(name),
    );
    assert.equal(created.length, 1, `${source.name} must create exactly one tarball`);
    const tarball = join(destination, created[0]);
    const entries = execFileSync("tar", ["-tzf", tarball], { encoding: "utf8" })
      .trim()
      .split("\n");
    assert(entries.includes("package/package.json"), `${source.name} tarball lacks package.json`);
    assert(
      entries.some((entry) => entry.startsWith("package/dist/")),
      `${source.name} tarball lacks built dist files`,
    );
    if (source.name === "@gonvex/module-sdk") {
      for (const file of ["globals.js", "globals.d.ts", "vectors.json", "runner.js"]) {
        assert(entries.includes(`package/runtime-contract/${file}`), `SDK tarball lacks runtime-contract/${file}`);
      }
    }

    const packed = JSON.parse(
      execFileSync("tar", ["-xOzf", tarball, "package/package.json"], {
        encoding: "utf8",
      }),
    );
    assert.equal(packed.name, source.name);
    assert.equal(packed.version, version);
    const serialized = JSON.stringify(packed);
    assert(!serialized.includes("workspace:"), `${source.name} tarball contains workspace:*`);
    for (const section of ["dependencies", "peerDependencies", "optionalDependencies"]) {
      for (const [name, range] of Object.entries(packed[section] ?? {})) {
        if (name.startsWith("@gonvex/")) {
          assert.equal(
            range,
            version,
            `${source.name} ${section}.${name} must use the exact release version`,
          );
        }
      }
    }
  }

  process.stdout.write(
    `Verified ${packageDirectories.length} Gonvex npm tarballs at ${version}.\n`,
  );
} finally {
  rmSync(destination, { recursive: true, force: true });
}
