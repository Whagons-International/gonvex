// X13b: the Action Worker installs each app with a loader, so reading a
// project's contracts never evaluates its bundle (~350-450 ms CPU for Whagons on
// a cold isolate); the engine evaluates it once, on the first handler lookup or
// an explicit `load()`.
import { describe, expect, it } from "vitest";
import { ModuleEngine, ModuleRegistry } from "../src/index.js";

const any = { kind: "any" };
const contracts = [{ path: "ns.run", kind: "action" as const, argsSchema: any, resultSchema: any }];
const context = { nowUnixMs: 0, identity: {}, invocation: {}, capabilities: {} };

describe("lazily evaluated exports", () => {
  it("loads on the first handler lookup, once", async () => {
    let loads = 0;
    const engine = new ModuleEngine(contracts, () => { loads++; return { ns: { run: () => "ran" } }; });
    expect(engine.functions.get("ns.run")?.kind).toBe("action");
    expect(loads).toBe(0);
    for (let n = 0; n < 3; n++) expect(JSON.parse((await engine.invoke({ call: () => null }, { function: "ns.run", kind: "action", args: {}, context })).value)).toBe("ran");
    expect(loads).toBe(1);
  });

  it("installs and leases a generation without loading", () => {
    let loads = 0;
    const registry = new ModuleRegistry("lazy");
    registry.install({ moduleId: "lazy", generation: 1, artifactHash: "h", functions: contracts, exports: () => { loads++; return {}; } });
    registry.lease().release();
    expect(loads).toBe(0);
  });

  it("load() evaluates once and reports it", () => {
    let loads = 0;
    const engine = new ModuleEngine(contracts, () => { loads++; return { ns: { run: () => "ran" } }; });
    expect(engine.loaded).toBe(false);
    engine.load(); engine.load();
    expect(engine.loaded).toBe(true);
    expect(loads).toBe(1);
  });

  it("surfaces an evaluation error to the call instead of reporting a missing handler", async () => {
    const engine = new ModuleEngine(contracts, () => { throw new TypeError("bundle failed to evaluate"); });
    await expect(engine.invoke({ call: () => null }, { function: "ns.run", kind: "action", args: {}, context })).rejects.toThrow("bundle failed to evaluate");
  });
});
