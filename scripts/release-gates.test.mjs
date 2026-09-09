import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  assertDashboardHealth,
  assertPromotionCandidate,
  assertRuntimeHealth,
} from "./release-gates.mjs";

const sha = "a".repeat(40);
const productionSha = "b".repeat(40);

function candidate(overrides = {}) {
  return {
    sha,
    productionSha,
    mainComparison: { status: "ahead", behind_by: 0 },
    productionComparison: { status: "ahead", behind_by: 0 },
    checkRuns: [
      { name: "CI", status: "completed", conclusion: "success" },
      { name: "Whagons compatibility", status: "completed", conclusion: "success" },
    ],
    devApplications: {
      runtime: { status: "running:healthy" },
      dashboard: { status: "running:healthy" },
    },
    devRuntimeHealth: { ok: true, version: sha },
    ...overrides,
  };
}

test("accepts only a tested fast-forward already healthy in dev", () => {
  assert.doesNotThrow(() => assertPromotionCandidate(candidate()));
  assert.throws(
    () => assertPromotionCandidate(candidate({ productionComparison: { status: "diverged", behind_by: 1 } })),
    /fast-forward/,
  );
  assert.throws(
    () => assertPromotionCandidate(candidate({ checkRuns: [{ name: "CI", status: "completed", conclusion: "failure" }] })),
    /successful CI/,
  );
  assert.throws(
    () => assertPromotionCandidate(candidate({
      devRuntimeHealth: { ok: true, version: productionSha },
    })),
    /version/,
  );
});

test("requires public health and the exact runtime version after deployment", () => {
  assert.doesNotThrow(() => assertRuntimeHealth({ ok: true, version: sha }, sha));
  assert.throws(() => assertRuntimeHealth({ ok: true, version: productionSha }, sha), /version/);
  assert.throws(() => assertRuntimeHealth({ ok: false, version: sha }, sha), /not healthy/);
  assert.doesNotThrow(() => assertDashboardHealth("ok\n"));
  assert.throws(() => assertDashboardHealth("starting\n"), /not healthy/);
});

test("main CI leaves development deployment to Coolify auto-deploy", async () => {
  const workflow = await readFile(
    new URL("../.github/workflows/runtime-regression.yml", import.meta.url),
    "utf8",
  );
  assert.match(workflow, /push:\s*\n\s*branches: \[main\]/);
  assert.doesNotMatch(workflow, /deploy-dev-runtime/);
  assert.doesNotMatch(workflow, /scripts\/deploy-coolify\.mjs/);
});

test("production promotion waits for approval and records the release only after smoke tests", async () => {
  const workflow = await readFile(
    new URL("../.github/workflows/promote-production.yml", import.meta.url),
    "utf8",
  );
  assert.match(workflow, /name: gonvex-production/);
  assert.match(workflow, /require_auth:/);
  assert.match(workflow, /GONVEX_EXPECT_REQUIRE_AUTH: \$\{\{ inputs\.require_auth \}\}/);
  assert.match(workflow, /GONVEX_COOLIFY_AUTO_DEPLOY: "false"/);
  const deploy = workflow.indexOf("Roll runtime, then dashboard");
  const smoke = workflow.indexOf("Verify public production endpoints");
  const finalize = workflow.indexOf("Atomically record the release branch and immutable tag");
  const rollback = workflow.indexOf("Roll back deployment if release metadata cannot be finalized");
  assert.ok(deploy >= 0 && deploy < smoke);
  assert.ok(smoke < finalize);
  assert.ok(finalize < rollback);
  assert.match(workflow, /git push --atomic origin/);
  assert.match(workflow, /GONVEX_DEPLOY_SHA: \$\{\{ needs\.validate\.outputs\.production_sha \}\}/);
  assert.match(workflow, /failure\(\) && steps\.finalize\.outcome != 'success'/);
});


test("promotion rejects missing, skipped, or failed app compatibility", () => {
  for (const conclusion of [undefined, "skipped", "failure", "cancelled"]) {
    const checkRuns = [{ name: "CI", status: "completed", conclusion: "success" }];
    if (conclusion) checkRuns.push({ name: "Whagons compatibility", status: "completed", conclusion });
    assert.throws(() => assertPromotionCandidate(candidate({ checkRuns })), /successful Whagons compatibility/);
  }
});
