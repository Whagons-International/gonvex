import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ignoredDirectories = new Set([".git", "node_modules", "target"]);
const retiredNames = new Set(["go.mod", "go.sum"]);

function findRetiredSources(directory) {
  const matches = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (entry.isDirectory() && ignoredDirectories.has(entry.name)) continue;
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      matches.push(...findRetiredSources(absolute));
      continue;
    }
    if (entry.name.endsWith(".go") || retiredNames.has(entry.name)) {
      matches.push(path.relative(root, absolute));
    }
  }
  return matches;
}

test("the repository has one Rust server implementation", () => {
  assert.deepEqual(findRetiredSources(root), []);
});
