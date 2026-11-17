// BUG-1: every Whagons Action failed on the Actions Worker with "module export
// for <fn> is not a callable action handler". The Action bundle is the
// complete bundle, whose entry re-exports each file as a namespace
// (`export * as apiKeys from "./apiKeys"`), so `apiKeys.create` is a property
// of the `apiKeys` export. Rust finds it by walking the dotted path
// (V8/isolate.rs `resolve_handler`); the kernel looked only for a top-level
// export named after the path or the manifest's handler.
import { describe, expect, it } from "vitest";
// Built by vitest.config.ts from test/fixtures/namespaced-app.
// @ts-expect-error virtual module
import * as bundle from "gonvex-test:namespaced-app";
import { moduleArtifact, projectModuleFromManifest } from "../../reducer/module";
import { findExport, ModuleEngine, ModuleRegistry, resolveHandler, type FunctionContract } from "../src/index.js";

const any = { kind: "any" };
// The manifest entries the Gonvex CLI writes for these exports (whagons5-client
// 678bdc80 gonvex/_generated/manifest.json): `handler` is the source export
// name, and there is no `export`.
const manifest = {
  functions: {
    "apiKeys.create": { kind: "action", handler: "create", file: "gonvex/apiKeys.ts", actionProfile: "agent", args: any, result: any },
    "taskResources.generateUploadUrl": { kind: "action", handler: "generateUploadUrl", file: "gonvex/taskResources/commands.ts", actionProfile: "standard", args: any, result: any },
  },
  module: { hash: "namespaced-app", generation: 1 },
};

function registry(): ModuleRegistry {
  // The Actions Worker's path (cloudflare/kernel/assembly/actions.ts).
  const module = projectModuleFromManifest("namespaced-app", manifest);
  const registry = new ModuleRegistry("namespaced-app");
  registry.install(moduleArtifact(module, module.generation, bundle as Record<string, unknown>));
  return registry;
}

const context = { nowUnixMs: 0, identity: {}, invocation: {}, capabilities: {} };

describe("BUG-1: Actions exported through namespace re-exports", () => {
  it("resolves every Action in the built bundle", () => {
    const engine = registry().activeGeneration!.engine;
    expect(engine.unresolved()).toEqual([]);
  });

  it("runs a direct agent-profile export and a sub-file re-export", async () => {
    const engine = registry().activeGeneration!.engine;
    const created = await engine.invoke({ call: () => null }, { function: "apiKeys.create", kind: "action", args: { name: "ci" }, context });
    expect(JSON.parse(created.value)).toEqual({ name: "ci", kind: "action", function: "apiKeys.create" });
    const upload = await engine.invoke({ call: () => null }, { function: "taskResources.generateUploadUrl", kind: "action", args: {}, context });
    expect(JSON.parse(upload.value)).toEqual({ uploadUrl: "https://uploads.invalid/taskResources.generateUploadUrl" });
  });
});

describe("export lookup never resolves to another function", () => {
  const contract = (path: string, kind: FunctionContract["kind"], metadata?: Record<string, unknown>, exportName?: string): FunctionContract =>
    ({ path, kind, argsSchema: any, resultSchema: any, ...(metadata ? { metadata } : {}), ...(exportName ? { export: exportName } : {}) });
  const definition = (kind: string, tag: string) => ({ kind, handler: async () => tag });
  const run = async (engine: ModuleEngine, path: string, kind: FunctionContract["kind"]) =>
    JSON.parse((await engine.invoke({ call: () => null }, { function: path, kind, args: {}, context })).value);

  it("runs tasks.list, not a top-level `list` reducer, when both exist", async () => {
    const contracts = [contract("tasks.list", "query", { handler: "list", export: "list" }), contract("notes.list", "reducer", { handler: "list", export: "list" })];
    const exports = { list: definition("reducer", "notes.list (top-level list)"), tasks: { list: definition("query", "tasks.list") }, notes: { list: definition("reducer", "notes.list") } };
    const engine = new ModuleEngine(contracts, exports);
    expect(engine.unresolved()).toEqual([]);
    expect(await run(engine, "tasks.list", "query")).toBe("tasks.list");
    expect(await run(engine, "notes.list", "reducer")).toBe("notes.list");
  });

  it("refuses a shared bare name instead of guessing", () => {
    // tasks.list has no namespace export; `list` belongs to notes.list too, so it is ambiguous.
    const contracts = [
      contract("tasks.list", "query", { handler: "list", file: "gonvex/tasks.ts" }),
      contract("projects.list", "query", { handler: "list", file: "gonvex/projects.ts" }),
    ];
    const engine = new ModuleEngine(contracts, { list: definition("query", "wrong") });
    expect(engine.unresolved()).toEqual(["tasks.list", "projects.list"]);
  });

  it("refuses a bare-name match of the wrong kind", () => {
    const engine = new ModuleEngine([contract("tasks.list", "query", { handler: "list" })], { list: definition("reducer", "wrong") });
    expect(engine.unresolved()).toEqual(["tasks.list"]);
  });

  it("runs the right `create` when two modules export one, also with a flat `create`", async () => {
    const contracts = [contract("apiKeys.create", "action", { handler: "create" }), contract("tags.create", "reducer", { handler: "create" })];
    const exports = {
      create: definition("reducer", "flat create"),
      apiKeys: { create: definition("action", "apiKeys.create") },
      tags: { create: definition("reducer", "tags.create") },
    };
    const engine = new ModuleEngine(contracts, exports);
    expect(await run(engine, "apiKeys.create", "action")).toBe("apiKeys.create");
    expect(await run(engine, "tags.create", "reducer")).toBe("tags.create");
  });

  it("resolves the split Query/Reducer bundle by its full-path exports, even beside bare names", async () => {
    const contracts = [contract("tasks.list", "query", { handler: "list", export: "list" }), contract("notes.list", "reducer", { handler: "list", export: "list" })];
    const exports = { "tasks.list": definition("query", "tasks.list"), "notes.list": definition("reducer", "notes.list"), list: definition("reducer", "bare list") };
    const engine = new ModuleEngine(contracts, exports);
    expect(await run(engine, "tasks.list", "query")).toBe("tasks.list");
    expect(await run(engine, "notes.list", "reducer")).toBe("notes.list");
  });

  it("shares one declared export between paths of the same file (dispatcher exports)", () => {
    const dispatch = () => "dispatch";
    const contracts = [contract("a", "action", { export: "contract" }), contract("b", "action", { export: "contract" })];
    expect(new ModuleEngine(contracts, { contract: dispatch }).unresolved()).toEqual([]);
  });

  it("uses a unique bare export name in flat single-module bundles", () => {
    const named = () => "named";
    const contracts = [contract("ns.fn", "action", { export: "run" }), contract("ns.other", "action", { export: "other" })];
    expect(findExport({ run: named }, contracts[0]!, contracts)).toBe(named);
    expect(findExport({}, contracts[0]!, contracts)).toBeUndefined();
  });

  it("uses `default` only for its own createModule() registration or a single-function module", () => {
    const handler = () => "registered";
    const contracts = [contract("ns.fn", "action"), contract("ns.other", "action")];
    const registered = { default: { runtimeRegistrations: () => [{ path: "ns.fn", handler }] } };
    expect(resolveHandler(findExport(registered, contracts[0]!, contracts), "ns.fn")).toBe(handler);
    expect(findExport(registered, contracts[1]!, contracts)).toBeUndefined();
    const plain = { default: () => "only" };
    expect(findExport(plain, contracts[0]!, contracts)).toBeUndefined();
    expect(findExport(plain, contracts[0]!)).toBe(plain.default);
  });

  it("skips a lookup that throws", () => {
    const fn = () => "ok";
    const exports = Object.defineProperty({ fn }, "ns", { get() { throw new ReferenceError("Cannot access 'ns' before initialization"); } });
    expect(findExport(exports, contract("ns.fn", "action"))).toBe(fn);
  });
});
