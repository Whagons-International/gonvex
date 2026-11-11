// Bundle generations and system.reload (B.6, 0005/0016 G0-2), and invocation
// state isolation through AsyncLocalStorage.
import { describe, expect, it } from "vitest";
import * as fixture from "./fixtures/host.module.js";
import { ModuleEngine, ModuleRegistry, isWireError, type ModuleHost, type ModuleReloaded } from "../src/index.js";
import { fixtureArtifact, fixtureFunctions } from "./worker.js";

const wire = (run: () => unknown) => { try { run(); } catch (error) { if (isWireError(error)) return error; throw error; } throw new Error("expected a WireError"); };

describe("module generations", () => {
  it("activates monotonically and emits system.reload", () => {
    const registry = new ModuleRegistry("fixture");
    const events: ModuleReloaded[] = [];
    registry.onReload((event) => events.push(event));
    expect(wire(() => registry.lease())).toMatchObject({ code: "module_not_loaded", retryable: true, message: "module fixture has no active generation" });
    registry.install(fixtureArtifact(1, "hash-1"));
    registry.install(fixtureArtifact(3, "hash-3"));
    expect(events.map((event) => event.frame)).toEqual([
      { type: "system.reload", reason: "module generation changed", artifactHash: "hash-1" },
      { type: "system.reload", reason: "module generation changed", artifactHash: "hash-3" },
    ]);
    expect(events[1]).toMatchObject({ generation: 3, retired: 1, clientContract: 1 });
    expect(wire(() => registry.load(fixtureArtifact(2)))).toMatchObject({ code: "generation_conflict", retryable: false, message: "module fixture generation 2 is not newer than the active one" });
    expect(wire(() => registry.activate(9))).toMatchObject({ code: "unknown_generation", message: "module fixture generation 9 was never loaded" });
  });

  it("serves a pinned older generation from the active one and refuses a newer pin", () => {
    const registry = new ModuleRegistry("fixture");
    registry.install(fixtureArtifact(2));
    const lease = registry.lease(1);
    expect(lease.generation.generation).toBe(2);
    lease.release();
    expect(wire(() => registry.lease(5))).toMatchObject({ code: "unknown_generation", retryable: true, message: "module fixture generation 5 is not loaded; the host is on 2" });
  });

  it("lets running calls finish on the generation they started on", async () => {
    const registry = new ModuleRegistry("fixture", {}, 1_000);
    registry.install(fixtureArtifact(1));
    const held = registry.lease();
    registry.install(fixtureArtifact(2));
    expect(held.generation.generation).toBe(1);
    expect(registry.lease().generation.generation).toBe(2);
    expect(registry.draining()).toEqual([1]);
    held.release();
    await Promise.resolve();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(registry.draining()).toEqual([]);
  });

  it("refuses an invalid schema at load and an unresolved export at dispatch", async () => {
    const registry = new ModuleRegistry("fixture");
    const { exports: _unused, ...rest } = fixtureArtifact(1);
    const missing = registry.load({ ...rest, exports: {} });
    expect(missing.engine.unresolved()).toContain("notes.list");
    const error = await missing.engine.invoke({ call: () => [] }, { function: "notes.list", kind: "query", args: {}, context: { nowUnixMs: 1, identity: {}, invocation: {}, capabilities: { dbRead: true } } }).then(() => undefined, (cause: unknown) => cause);
    expect((error as Error).message).toBe("module artifact is invalid: module export for notes.list is not a callable query handler");
    const badSchema = { ...fixtureArtifact(1), functions: [{ ...fixtureFunctions()[0]!, argsSchema: { kind: "nope" } }] };
    expect(wire(() => new ModuleRegistry("fixture").load(badSchema)).message).toContain('has an invalid arguments schema: $.kind: unsupported schema kind "nope"');
  });
});

describe("invocation state", () => {
  it("keeps each interleaved invocation's clock and random sequence", async () => {
    const engine = new ModuleEngine(fixtureFunctions(), fixture as Record<string, unknown>);
    // A host whose answer arrives later, so the two invocations interleave.
    const host: ModuleHost = { call: () => new Promise((resolve) => setTimeout(() => resolve([{ one: 1 }]), 5)) };
    const run = (now: number) => engine.invoke(host, {
      function: "notes.clock", kind: "query", args: {},
      context: { nowUnixMs: now, identity: {}, invocation: {}, capabilities: { dbRead: true } },
    });
    const [a, b] = await Promise.all([run(1_000), run(2_000)]);
    expect(JSON.parse(a.value)).toMatchObject({ now: 1_000, pinnedAcrossAwait: true });
    expect(JSON.parse(b.value)).toMatchObject({ now: 2_000, pinnedAcrossAwait: true });
  });

  it("leaves the kernel's own Date and crypto native", () => {
    expect(Date.now()).toBeGreaterThan(1_700_000_000_000);
    expect(typeof crypto.randomUUID()).toBe("string");
    expect(typeof fetch).toBe("function");
  });
});
