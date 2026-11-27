import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { assertResidentObject, resident, residentEnv, residentHandler, stackJurisdiction } from "../residency";
import { admitRegion, stackRegion, tenantRegion } from "../../../control/operator/regions";
import type { ProbeEnv } from "./worker";

/** A namespace as the platform shapes it: `jurisdiction(j)` returns a namespace whose ids carry `j`. */
function fakeNamespace(jurisdiction: string | null = null, calls: string[] = []) {
  const id = (name: string) => ({ name, jurisdiction: jurisdiction ?? undefined, toString: () => `${jurisdiction ?? "none"}:${name}`, equals: () => false });
  const ns = {
    idFromName: id, newUniqueId: () => id("unique"), idFromString: (s: string) => id(s),
    get: (i: { name: string }) => ({ id: i, name: i.name }), getByName: (n: string) => ({ id: id(n), name: n }),
    jurisdiction: (j: string) => { calls.push(j); return fakeNamespace(j, calls); },
  };
  return ns;
}

describe("stack jurisdiction", () => {
  it("is null when unset and refuses unknown values (fail closed)", () => {
    expect(stackJurisdiction({})).toBeNull();
    expect(stackJurisdiction({ GONVEX_JURISDICTION: "" })).toBeNull();
    expect(stackJurisdiction({ GONVEX_JURISDICTION: "eu" })).toBe("eu");
    expect(() => stackJurisdiction({ GONVEX_JURISDICTION: "EU" })).toThrow(/not one of/);
    expect(() => stackJurisdiction({ GONVEX_JURISDICTION: "us" })).toThrow(/not one of/);
  });

  it("restricts every Durable Object namespace once and leaves other bindings alone", () => {
    const calls: string[] = [];
    const service = { fetch: () => new Response(""), idFromName: () => "rpc answers anything", get: () => null, jurisdiction: () => { throw new Error("not a namespace"); } };
    const bucket = { put: () => null };
    const raw = { GONVEX_JURISDICTION: "eu", TENANT: fakeNamespace(null, calls), CONTROL: fakeNamespace(null, calls), ACTIONS: service, FILES: bucket, NAME: "x" };
    const out = residentEnv(raw);
    expect(calls).toEqual(["eu", "eu"]);
    expect(out.TENANT.idFromName("t1").jurisdiction).toBe("eu");
    expect(out.CONTROL.getByName("control").id.jurisdiction).toBe("eu");
    expect(out.ACTIONS).toBe(service);
    expect(out.FILES).toBe(bucket);
    expect(out.NAME).toBe("x");
    expect(residentEnv(raw)).toBe(out);
    expect(residentEnv(out)).toBe(out);
    expect(calls).toHaveLength(2);
    expect(raw.TENANT.idFromName("t1").jurisdiction).toBeUndefined();
  });

  it("is the identity without a jurisdiction", () => {
    expect(residentEnv(env)).toBe(env);
    const plain = { TENANT: fakeNamespace() };
    expect(residentEnv(plain)).toBe(plain);
  });

  it("refuses an object reached outside the stack jurisdiction", () => {
    const stack = { GONVEX_JURISDICTION: "eu" };
    expect(() => assertResidentObject({ id: { jurisdiction: undefined, toString: () => "0123456789abcdef" } }, stack)).toThrow(/outside the stack jurisdiction eu/);
    expect(() => assertResidentObject({ id: { jurisdiction: "fedramp", toString: () => "0123456789abcdef" } }, stack)).toThrow(/outside/);
    expect(() => assertResidentObject({ id: { jurisdiction: "eu", toString: () => "x" } }, stack)).not.toThrow();
    expect(() => assertResidentObject({}, stack)).not.toThrow(); // WorkerEntrypoint: no id
    expect(() => assertResidentObject({ id: { jurisdiction: undefined } }, {})).not.toThrow(); // no stack jurisdiction
  });

  it("wraps classes: the constructor sees the resident env and the class keeps its name", () => {
    // The platform's base classes need a real DurableObjectState; the mixin itself only needs (ctx, env).
    class Base { constructor(readonly ctx: DurableObjectState, readonly env: Record<string, unknown>) {} seen() { return this.env; } }
    const Wrapped = resident(Base);
    expect(Wrapped.name).toBe("Base");
    const calls: string[] = [];
    const ctx = { id: { jurisdiction: "eu", toString: () => "eu-id" } } as unknown as DurableObjectState;
    const object = new Wrapped(ctx, { GONVEX_JURISDICTION: "eu", TENANT: fakeNamespace(null, calls) });
    expect(calls).toEqual(["eu"]);
    expect(((object as Base).seen().TENANT as ReturnType<typeof fakeNamespace>).idFromName("a").jurisdiction).toBe("eu");
    expect(() => new Wrapped({ id: { jurisdiction: undefined, toString: () => "plain-object-id" } } as unknown as DurableObjectState, { GONVEX_JURISDICTION: "eu", TENANT: fakeNamespace() })).toThrow(/outside/);
  });

  it("wraps default handlers", async () => {
    const calls: string[] = [];
    const handler = residentHandler({ fetch: (_r: Request, e: { TENANT: ReturnType<typeof fakeNamespace> }) => Response.json(e.TENANT.idFromName("a").jurisdiction ?? null) });
    const response = await handler.fetch(new Request("http://x/"), { GONVEX_JURISDICTION: "eu", TENANT: fakeNamespace(null, calls) } as never);
    expect(await response.json()).toBe("eu");
  });

  it("an unrestricted stack serves its probes as before", async () => {
    const probe = (env as unknown as ProbeEnv).PROBE.getByName("plain");
    const info = await (probe as unknown as { info(): Promise<{ jurisdiction: string | null; name: string }> }).info();
    expect(info.name).toBe("plain");
    expect(info.jurisdiction).toBeNull();
    const residency = await (probe as unknown as { gonvexResidency(): Promise<{ stackJurisdiction: string | null }> }).gonvexResidency();
    expect(residency.stackJurisdiction).toBeNull();
  });
});

describe("tenant regions in the control catalog", () => {
  it("records the stack's region and refuses one the stack cannot place", () => {
    expect(stackRegion({})).toBeNull();
    expect(stackRegion({ GONVEX_JURISDICTION: "eu" })).toBe("eu");
    expect(() => stackRegion({ GONVEX_JURISDICTION: "moon" })).toThrow();
    expect(admitRegion("eu", undefined)).toEqual({ region: "eu" });
    expect(admitRegion("eu", "eu")).toEqual({ region: "eu" });
    expect(admitRegion(null, undefined)).toEqual({ region: null });
    expect(admitRegion(null, "eu")).toMatchObject({ error: expect.stringMatching(/GONVEX_JURISDICTION=eu/) });
    expect(admitRegion("eu", "fedramp")).toMatchObject({ error: expect.stringMatching(/keeps data in eu/) });
    expect(admitRegion("eu", "mars")).toMatchObject({ error: expect.stringMatching(/region must be one of/) });
    const recorded = new Map([["p\u0000a", "eu"], ["p\u0000b", null]]);
    expect(tenantRegion(recorded, "eu", "p", "a")).toBe("eu");
    expect(tenantRegion(recorded, "eu", "p", "b")).toBeNull();
    expect(tenantRegion(recorded, "eu", "p", "legacy")).toBe("eu");
  });
});
