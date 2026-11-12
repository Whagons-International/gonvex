import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  assertDashboardHealth,
  assertPromotionCandidate,
  assertRuntimeHealth,
  captureRuntimeRollbackSHA,
} from "./release-gates.mjs";

const sha = "a".repeat(40);
const productionSha = "b".repeat(40);

function candidate(overrides = {}) {
  return {
    sha,
    productionSha,
    mainComparison: { status: "ahead", behind_by: 0 },
    productionComparison: { status: "ahead", behind_by: 0 },
    checkRuns: [{ name: "CI", status: "completed", conclusion: "success" }],
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

test("rollback captures the healthy live SHA even when branch metadata differs", async () => {
  const live = "c".repeat(40);
  const result = await captureRuntimeRollbackSHA("https://runtime.example", async (url, options) => {
    assert.equal(url.pathname, "/healthz");
    assert.equal(options.headers["Cache-Control"], "no-cache");
    return { ok: true, json: async () => ({ ok: true, version: live }) };
  });
  assert.equal(result, live);
  assert.notEqual(result, productionSha);
});

test("rollback capture rejects unhealthy, unavailable and ambiguous runtime identity", async () => {
  for (const payload of [{ ok: false, version: sha }, { ok: true, version: sha.slice(0, 7) }, { ok: true }, { ok: true, version: "HEAD" }]) {
    await assert.rejects(captureRuntimeRollbackSHA("https://runtime.example", async () => ({ ok: true, json: async () => payload })));
  }
  await assert.rejects(captureRuntimeRollbackSHA("https://runtime.example", async () => ({ ok: false, status: 503 })), /503/);
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
  const baseline = workflow.indexOf("Capture the currently serving runtime for rollback");
  const rollback = workflow.indexOf("Roll back to the captured live runtime after a failed promotion");
  assert.ok(baseline >= 0 && baseline < deploy);
  assert.ok(deploy >= 0 && deploy < smoke);
  assert.ok(smoke < finalize);
  assert.ok(finalize < rollback);
  assert.match(workflow, /git push --atomic origin/);
  assert.match(workflow, /GONVEX_DEPLOY_SHA: \$\{\{ steps\.baseline\.outputs\.sha \}\}/);
  assert.doesNotMatch(workflow, /GONVEX_DEPLOY_SHA: \$\{\{ needs\.validate\.outputs\.production_sha \}\}/);
  assert.match(workflow, /failure\(\) && steps\.baseline\.outcome == 'success' && steps\.finalize\.outcome != 'success'/);
});
