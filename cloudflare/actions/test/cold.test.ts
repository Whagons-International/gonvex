// X13b: a cold Action isolate evaluates the app bundle (Whagons: ~350-450 ms CPU) while the tenant writes
// the inline run (startCall: 55-230 ms on dev, mostly storage durability), instead of one after the other.
// `startInline` is the code `ActionWorker.call` runs; the bundle and the tenant are stand-ins with the same
// shape: a loader that burns CPU once, and a startCall answered after a delay.
import { describe, expect, it } from "vitest";
import { ModuleEngine } from "../../kernel/host/src/index.js";
import { runInline, startInline } from "../src/worker.js";

const any = { kind: "any" };
const contracts = [{ path: "ns.run", kind: "action" as const, argsSchema: any, resultSchema: any }];
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
/** Busy CPU for about `ms` (workerd's clock moves only across I/O, so the loop is calibrated once). */
let perMs = 0;
async function calibrate() { if (perMs) return; const start = Date.now(); let x = 0; for (let i = 0; i < 2e7; i++) x = (x * 31 + i) | 0; await tick(); perMs = 2e7 / Math.max(1, Date.now() - start); return x; }
function burn(ms: number) { let x = 0; for (let i = 0, n = perMs * ms; i < n; i++) x = (x * 31 + i) | 0; return x; }
const EVAL_MS = 200, START_MS = 150;
const tenant = { startCall: () => new Promise<string>((resolve) => setTimeout(() => resolve("claim"), START_MS)) };
const request = {} as Parameters<typeof startInline>[1];

async function timed(run: () => Promise<unknown>) { await tick(); const start = Date.now(); await run(); await tick(); return Date.now() - start; }

describe("X13b cold inline start", () => {
  it("overlaps the bundle evaluation with the tenant's run start; a warm isolate only waits for the start", async () => {
    await calibrate();
    const engine = () => new ModuleEngine(contracts, () => { burn(EVAL_MS); return { ns: { run: () => 1 } }; });
    // Before X13b: the bundle evaluated when the contracts were read, then the run started.
    const before = await timed(async () => { const e = engine(); e.load(); await tenant.startCall(); });
    const coldEngine = engine();
    const cold = await timed(() => startInline(tenant, request, coldEngine));
    expect(coldEngine.loaded).toBe(true);
    const warm = await timed(() => startInline(tenant, request, coldEngine));
    console.log(`X13b bench ${JSON.stringify({ evalMs: EVAL_MS, startCallMs: START_MS, coldBeforeMs: before, coldAfterMs: cold, warmMs: warm })}`);
    expect(cold).toBeLessThan(before - START_MS / 2);
    expect(warm).toBeLessThan(START_MS + 60);
  });

  it("a bundle that fails to evaluate still starts the run and fails later, in execute", async () => {
    const broken = new ModuleEngine(contracts, () => { throw new TypeError("bundle failed to evaluate"); });
    expect(await startInline(tenant, request, broken)).toBe("claim");
    expect(broken.loaded).toBe(false);
  });
});

describe("X13b reply before completion", () => {
  it("runInline returns the answer while completeCall is still pending, then records it", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const completed: unknown[] = [];
    const tenant = {
      call: async () => ({ ok: true as const, value: "null" }),
      claimCall: async () => { throw new Error("not used"); },
      renewCall: async () => ({ v: 1 as const, ok: true, persisted: false }),
      releaseCall: async () => true,
      completeCall: async (request: unknown) => { await gate; completed.push(request); return { v: 1 as const, applied: true, appliedAttempt: 1 }; },
    };
    const runtime = { execute: async () => ({ ok: true as const, value: '"FIXED"', stats: {} as never }) } as unknown as import("../src/runtime.js").ActionRuntime;
    const claim = { v: 1 as const, ok: true as const, id: "call-1", attempt: 1, path: "agent.rewrite", args: {}, provenance: null, session: {} as never, leaseMs: 300_000, renewMs: 30_000 };
    const { report, completion } = await runInline({ runtime, tenant: tenant as never }, claim);
    expect(report).toMatchObject({ ok: true, value: '"FIXED"' });
    expect(completed).toEqual([]);
    release();
    await completion;
    expect(completed).toEqual([{ v: 1, id: "call-1", attempt: 1, value: '"FIXED"', error: undefined, committedRevision: undefined, inlineReply: true }]);
  });
});
