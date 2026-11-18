import assert from "node:assert/strict";
import { test } from "node:test";
import { previewName } from "./deploy-preview.mjs";
import { servingConfigs, developmentName, TENANT_CPU_LIMIT_MS, assertTenantCpuLimit } from "../kernel/assembly/deploy.mjs";
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
    for (const kind of ['kernel', 'runtime', 'actions', 'sandbox', 'builder']) {
      const config = configs[kind].config;
      assert.ok(config.name.length <= 63);
      for (const binding of config.durable_objects?.bindings ?? []) if (binding.script_name) assert.equal(binding.script_name, binding.class_name==='AssemblyTenant'?`${name}-runtime`:name);
      for (const bucket of config.r2_buckets ?? []) assert.equal(bucket.bucket_name, `${name}-${bucket.binding.toLowerCase()}`);
    }
    assert.equal(configs.kernel.config.limits.cpu_ms, 30000);
    // 0022 §5b / 0089: tenant objects live in the runtime script; its CPU limit is what stops `errors.spin`.
    assert.equal(configs.runtime.config.limits.cpu_ms, TENANT_CPU_LIMIT_MS);
    assert.equal(TENANT_CPU_LIMIT_MS, 5000);
    // Publication must reach the newly serving Worker through public ingress,
    // including after the builder deploys from this same source configuration.
    for (const kind of ['kernel', 'bootstrap']) {
      assert.ok(configs[kind].config.compatibility_flags.includes('global_fetch_strictly_public'), `${kind} publication ingress must use public routing`);
      assert.ok(!configs[kind].config.compatibility_flags.includes('global_fetch_private_origin'));
    }
    assert.equal(configs.actions.config.limits.cpu_ms, 300000);
    assert.deepEqual(configs.kernel.config.services.map((s) => s.service), [`${name}-actions`, `${name}-builder`, `${name}-runtime`]);
    assert.deepEqual(configs.bootstrap.config.services, []);
    assert.deepEqual(configs.bootstrap.config.workflows, []);
    assert.equal(configs.builder.config.vars.GONVEX_KERNEL_SCRIPT, name);
    assert.throws(() => developmentName('production'));
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("refuses a tenant runtime configuration above the ruled CPU limit (0022 §5b, 0089)", () => {
  for (const cpu_ms of [30000, 10000, undefined]) {
    assert.throws(() => assertTenantCpuLimit({ limits: cpu_ms === undefined ? undefined : { cpu_ms } }), /limits\.cpu_ms must be 5000/);
  }
  assert.doesNotThrow(() => assertTenantCpuLimit({ limits: { cpu_ms: 5000 } }));
});
