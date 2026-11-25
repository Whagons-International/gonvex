import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { servingConfigs } from "../deploy.mjs";

describe("deployAssembly servingConfigs bootstrap", () => {
  it("omits ActionHost external DO binding from runtimeBootstrap so fresh deploys do not hit CF 10061", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gonvex-deploy-bootstrap-"));
    try {
      const configs = await servingConfigs("gonvex-cf-dev-fresh-test", dir, {
        kernelUrl: "https://gonvex-cf-dev-fresh-test.example.workers.dev",
      });
      const fullBindings = configs.runtime.config.durable_objects.bindings;
      assert.ok(
        fullBindings.some((b) => b.class_name === "ActionHost" && b.script_name === "gonvex-cf-dev-fresh-test-actions"),
        "full runtime config must keep ActionHost binding to -actions",
      );

      const boot = configs.runtimeBootstrap.config;
      assert.deepEqual(boot.services, []);
      assert.deepEqual(boot.workflows, []);
      assert.ok(
        !boot.durable_objects.bindings.some((b) => b.class_name === "ActionHost"),
        "runtimeBootstrap must omit ActionHost before -actions script exists",
      );
      assert.ok(
        boot.durable_objects.bindings.some((b) => b.name === "TENANT" && b.class_name === "AssemblyTenant"),
        "runtimeBootstrap must preserve local AssemblyTenant binding",
      );
      assert.ok(
        boot.durable_objects.bindings.some((b) => b.name === "CONTROL" && b.script_name === "gonvex-cf-dev-fresh-test"),
        "runtimeBootstrap must preserve kernel DO bindings already deployed by bootstrap",
      );
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
