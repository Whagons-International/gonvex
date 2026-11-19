// X15: the control object parses the ~2 MB deployed manifest once per row version instead of on every
// sign-in `auth` (projectModule, moduleEpoch, moduleInfo). Any write to the row drops the parsed copy.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { ControlObject } from "../src/control-object";
import { projectModule } from "../src/directory";
import { deployedManifest } from "../src/manifest-cache";
import { saveManifest } from "../src/dev";
import type { Ctx } from "../src/context";
import { unique } from "./helpers";

const control = () => env.CONTROL.getByName("control") as DurableObjectStub<ControlObject>;
const ctxOf = (instance: ControlObject) => ({ sql: instance.sql }) as unknown as Ctx;

describe("X15 deployed manifest cache", () => {
  it("serves one parsed copy until the row is written, even by a same-hash write in the same millisecond", async () => {
    const project = unique("manifest");
    await runInDurableObject(control(), (instance: ControlObject) => {
      const ctx = ctxOf(instance);
      saveManifest(ctx, project, { module: { hash: "h1" }, functions: [{ path: "a.one", kind: "query" }] });
      const first = deployedManifest(instance.sql, project)!;
      expect(deployedManifest(instance.sql, project)).toBe(first);
      expect(projectModule(ctx, project)).toBe(projectModule(ctx, project));
      expect(Object.keys(projectModule(ctx, project)!.functions)).toEqual(["a.one"]);

      // Same hash, different content, written immediately: the write itself drops the cached copy.
      saveManifest(ctx, project, { module: { hash: "h1" }, functions: [{ path: "b.two", kind: "mutation" }] });
      expect(deployedManifest(instance.sql, project)).not.toBe(first);
      expect(Object.keys(projectModule(ctx, project)!.functions)).toEqual(["b.two"]);
      expect(projectModule(ctx, project)!.functions["b.two"]!.kind).toBe("mutation");

      instance.sql.run("DELETE FROM gonvex_runtime_manifests WHERE project_id=$1", project);
      expect(deployedManifest(instance.sql, project)).toBeNull();
      expect(projectModule(ctx, project)).toBeNull();
    });
  });

  it("a row changed outside this Sql (another writer) is detected by its hash and updated_at", async () => {
    const project = unique("manifest-ext");
    await runInDurableObject(control(), (instance: ControlObject, state) => {
      const ctx = ctxOf(instance);
      saveManifest(ctx, project, { module: { hash: "h1" }, functions: [{ path: "a.one", kind: "query" }] });
      expect(projectModule(ctx, project)!.artifactHash).toBe("h1");
      state.storage.sql.exec("UPDATE gonvex_runtime_manifests SET manifest=?,module_hash='h2',updated_at=updated_at+1 WHERE project_id=?",
        JSON.stringify({ module: { hash: "h2" }, functions: [{ path: "c.three", kind: "query" }] }), project);
      expect(projectModule(ctx, project)!.artifactHash).toBe("h2");
      expect(Object.keys(projectModule(ctx, project)!.functions)).toEqual(["c.three"]);
    });
  });

  it("an empty module hash is no deployed module, as before", async () => {
    const project = unique("manifest-empty");
    await runInDurableObject(control(), (instance: ControlObject) => {
      const ctx = ctxOf(instance);
      saveManifest(ctx, project, { module: {}, functions: [] });
      expect(deployedManifest(instance.sql, project)).not.toBeNull();
      expect(projectModule(ctx, project)).toBeNull();
    });
  });
});
