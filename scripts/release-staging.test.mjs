import assert from "node:assert/strict";
import test from "node:test";
import { assertStagingCandidate } from "./release-staging.mjs";
import { RELEASE_PACKAGES } from "./release-packages.mjs";

const sha = "abc123".padEnd(40, "0");
const version = "0.5.2-staging.30";
const candidate = {
  version, sha, head: sha, staging: sha, clean: true,
  versions: RELEASE_PACKAGES.map(() => version),
  checks: [{ id: 1, name: "CI", status: "completed", conclusion: "success" }],
};
test("only a clean, green, exact staging commit can be published", () => {
  assert.doesNotThrow(() => assertStagingCandidate(candidate));
  for (const change of [
    { version: "0.5.2" }, { version: "0.5.2-production.1" }, { sha: "abc123" },
    { head: "d".repeat(40) }, { staging: "d".repeat(40) }, { clean: false },
    { versions: [version] }, { versions: RELEASE_PACKAGES.map(() => "0.5.2-staging.29") },
    { checks: [] }, { checks: [{ id: 1, name: "CI", status: "in_progress", conclusion: null }] },
    { checks: [...candidate.checks, { id: 2, name: "CI", status: "completed", conclusion: "failure" }] },
  ]) assert.throws(() => assertStagingCandidate({ ...candidate, ...change }));
});
