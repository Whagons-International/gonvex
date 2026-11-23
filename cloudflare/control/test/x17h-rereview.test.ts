// control/test/x17h-rereview.test.ts (run: npx vitest run --root control test/x17h-rereview.test.ts)
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { FANOUT, type Ctx } from "../src/context";
import type { ControlObject } from "../src/control-object";
import { seed } from "./helpers";
const control = () => env.CONTROL.getByName("control") as DurableObjectStub<ControlObject>;
type Internals = { context(): Ctx };
function counting(ctx: Ctx, delayMs: number) {
  const stats = { inFlight: 0, max: 0 };
  const wrapped: Ctx = { ...ctx, tenant: (route) => { const port = ctx.tenant(route); return { admit: async (a: string) => {
    stats.inFlight++; stats.max = Math.max(stats.max, stats.inFlight);
    try { await new Promise((r) => setTimeout(r, delayMs)); return await port.admit(a); } finally { stats.inFlight--; } } } as unknown as ReturnType<Ctx["tenant"]>; } };
  return { ctx: wrapped, stats };
}
describe("#537 re-review: prefetch bound", () => {
  const N = FANOUT + 2;
  const tenants = Object.fromEntries(Array.from({ length: N }, (_, i) => [`t${String(i).padStart(2, "0")}`, ["sid"]]));
  it("prefetches at most FANOUT, the requested tenant included even when it sorts last", async () => {
    const sessions = await import("../src/sessions"); sessions.clearAdmissionCache();
    const seeded = await seed(tenants); const sid = seeded.members.sid!.accountId, last = seeded.tenants[`t${N - 1}`]!;
    await runInDurableObject(control(), async (instance: ControlObject) => {
      const ctx = (instance as unknown as Internals).context();
      const p = sessions.prefetchAdmissions(ctx, seeded.project, sid, last);
      expect(p.admissions.size).toBe(FANOUT); expect(p.admissions.has(last)).toBe(true); expect(ctx.tenantCalls).toBe(FANOUT);
      await Promise.all([...p.admissions.values()].map((e) => e.member));
    });
  }, 120_000);
  it("prefetched plus post-commit admissions never exceed FANOUT in flight (0012 R15)", async () => {
    const sessions = await import("../src/sessions"); sessions.clearAdmissionCache();
    const seeded = await seed(tenants); const sid = seeded.members.sid!.accountId, last = seeded.tenants[`t${N - 1}`]!;
    await runInDurableObject(control(), async (instance: ControlObject) => {
      const { ctx, stats } = counting((instance as unknown as Internals).context(), 200);
      const p = sessions.prefetchAdmissions(ctx, seeded.project, sid, last);
      const result = await sessions.sessionResultFromDirectory(ctx, seeded.project, { account: { id: sid }, activeTenantId: last }, p);
      expect((result.tenants as unknown[]).length).toBe(N); expect(result.activeTenantId).toBe(last);
      expect(stats.max).toBeLessThanOrEqual(FANOUT); // FAILS on c18b4049: 33
    });
  }, 120_000);
});
describe("X17r: the sign-in prefetch prewarms only the tenant the client opens", () => {
  it("passes { prewarm: true } with the preferred tenant's admit and nothing with the others", async () => {
    const sessions = await import("../src/sessions"); sessions.clearAdmissionCache();
    const seeded = await seed({ ta: ["sid"], tb: ["sid"], tc: ["sid"] });
    const sid = seeded.members.sid!.accountId, wanted = seeded.tenants.tc!;
    await runInDurableObject(control(), async (instance: ControlObject) => {
      const base = (instance as unknown as Internals).context();
      const seen: Array<[string, unknown]> = [];
      const ctx: Ctx = { ...base, tenant: (route) => { const port = base.tenant(route); return { admit: async (a: string, o?: unknown) => {
        seen.push([route.tenantId, o]); return o === undefined ? port.admit(a) : (port.admit as (a: string, o: unknown) => ReturnType<typeof port.admit>)(a, o); } } as unknown as ReturnType<Ctx["tenant"]>; } };
      const p = sessions.prefetchAdmissions(ctx, seeded.project, sid, wanted);
      await Promise.all([...p.admissions.values()].map((e) => e.member));
      expect(seen.length).toBe(3);
      expect(seen.filter(([, o]) => o !== undefined)).toEqual([[wanted, { prewarm: true }]]);
    });
  }, 120_000);
});
