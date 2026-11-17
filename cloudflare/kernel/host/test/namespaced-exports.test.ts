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
import { findExport, handlerCandidates, ModuleEngine, ModuleRegistry, resolveHandler, type FunctionContract } from "../src/index.js";

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

describe("export lookup matches Rust's handler_candidates", () => {
  const contract = (path: string, metadata?: Record<string, unknown>, exportName?: string): FunctionContract =>
    ({ path, kind: "action", argsSchema: any, resultSchema: any, ...(metadata ? { metadata } : {}), ...(exportName ? { export: exportName } : {}) });

  it("orders export, handler, path, leaf, default", () => {
    expect(handlerCandidates(contract("a.b.c", { export: "x", handler: "y" }))).toEqual(["x", "y", "a.b.c", "c", "default"]);
    expect(handlerCandidates(contract("a.run", { handler: "run" }))).toEqual(["run", "a.run", "run", "default"]);
    expect(handlerCandidates(contract("solo"))).toEqual(["solo", "default"]);
  });

  it("prefers the named export, then the full path, then nested namespaces, then the leaf", () => {
    const named = () => "named";
    const flat = () => "flat";
    const nested = () => "nested";
    const leaf = () => "leaf";
    expect(findExport({ run: named, "ns.fn": flat }, contract("ns.fn", { export: "run" }))).toBe(named);
    expect(findExport({ "ns.fn": flat, ns: { fn: nested } }, contract("ns.fn"))).toBe(flat);
    expect(findExport({ ns: { fn: nested }, fn: leaf }, contract("ns.fn"))).toBe(nested);
    expect(findExport({ fn: leaf }, contract("ns.fn"))).toBe(leaf);
    expect(findExport({}, contract("ns.fn"))).toBeUndefined();
  });

  it("falls back to createModule()'s default registrations", () => {
    const handler = () => "registered";
    const exports = { default: { runtimeRegistrations: () => [{ path: "ns.fn", handler }] } };
    expect(resolveHandler(findExport(exports, contract("ns.fn")), "ns.fn")).toBe(handler);
  });

  it("skips a candidate whose lookup throws", () => {
    const fn = () => "ok";
    const exports = Object.defineProperty({ fn }, "ns", { get() { throw new ReferenceError("Cannot access 'ns' before initialization"); } });
    expect(findExport(exports, contract("ns.fn"))).toBe(fn);
  });

  it("keeps resolving the split Query/Reducer bundle by path", () => {
    const fn = () => "ok";
    expect(new ModuleEngine([contract("tasks.list", { handler: "list" })], { "tasks.list": fn }).unresolved()).toEqual([]);
  });
});
