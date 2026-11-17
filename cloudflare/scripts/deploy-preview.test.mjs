import assert from "node:assert/strict";
import { test } from "node:test";
import { previewName } from "./deploy-preview.mjs";
import { servingConfigs, developmentName } from "../kernel/assembly/deploy.mjs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

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

test("composes every development service, bucket, workflow and queue under one isolated name", async () => {
  const directory = await mkdtemp(join(tmpdir(), "k22-config-"));
  try {
    const name = previewName(`cf/K22-${"x".repeat(90)}`);
    assert.equal(developmentName(name), name);
    const configs = await servingConfigs(name, directory, { kernelUrl: `https://${name}.example.invalid`, images: { sandbox: 'registry.invalid/gonvex-cf-dev-sandbox@sha256:abc', builder: 'registry.invalid/gonvex-cf-dev-builder@sha256:def' } });
    for (const kind of ['kernel', 'actions', 'sandbox', 'builder']) {
      const config = configs[kind].config;
      assert.ok(config.name.length <= 63);
      for (const binding of config.durable_objects?.bindings ?? []) if (binding.script_name) assert.equal(binding.script_name, name);
      for (const bucket of config.r2_buckets ?? []) assert.equal(bucket.bucket_name, `${name}-${bucket.binding.toLowerCase()}`);
    }
    assert.equal(configs.kernel.config.limits.cpu_ms, 5000);
    assert.equal(configs.actions.config.limits.cpu_ms, 300000);
    assert.deepEqual(configs.kernel.config.services.map((s) => s.service), [`${name}-actions`, `${name}-builder`]);
    assert.deepEqual(configs.bootstrap.config.services, []);
    assert.deepEqual(configs.bootstrap.config.workflows, []);
    assert.equal(configs.builder.config.vars.GONVEX_KERNEL_SCRIPT, name);
    assert.throws(() => developmentName('production'));
  } finally { await rm(directory, { recursive: true, force: true }); }
});
