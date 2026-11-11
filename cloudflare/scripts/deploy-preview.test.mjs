import assert from "node:assert/strict";
import { test } from "node:test";
import { previewName } from "./deploy-preview.mjs";

test("slugs branch names under the gonvex-cf-dev-preview- prefix", () => {
  assert.equal(previewName("cf/P01-scaffold"), "gonvex-cf-dev-preview-cf-p01-scaffold");
  assert.equal(previewName("Feature__X//y"), "gonvex-cf-dev-preview-feature-x-y");
});

test("keeps long names within the 63-character DNS label and unique", () => {
  const a = previewName(`cf/K07-${"a".repeat(80)}`);
  const b = previewName(`cf/K07-${"a".repeat(81)}`);
  assert.ok(a.length <= 63 && b.length <= 63);
  assert.ok(a.startsWith("gonvex-cf-dev-preview-cf-k07-"));
  assert.notEqual(a, b);
  assert.match(a, /[a-z0-9]$/);
});

test("refuses branch names with no usable characters", () => {
  assert.throws(() => previewName("///"));
});
