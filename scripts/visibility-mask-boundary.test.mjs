import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = readFileSync(new URL("../rust/crates/runtime/src/visibility_mask.rs", import.meta.url), "utf8");

test("row visibility evaluator cannot acquire I/O capabilities", () => {
  // Permit only JSON values and the two data-only principal/expression types.
  const imports = [...source.matchAll(/^use ([^;]+);/gm)].map((match) => match[1]);
  assert.deepEqual(imports.sort(), [
    "crate::visibility::{ResolvedVisibility, VisibilityExpression}",
    "serde_json::Value",
  ].sort());
  assert.doesNotMatch(source, /\b(sqlx|tokio|async|await|unsafe|extern|include|mod|std|Database|TenantTransaction|TenantSession|Pool)\b/);
  assert.doesNotMatch(source, /\b(crate|super|self)::(?!visibility::)/);
});
