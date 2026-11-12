import { describe, expect, it } from "vitest";
import type { ServiceGrant } from "../../../control/src/types.ts";
import type { ServiceTenantCall, ServiceTenantOutcome } from "../../../kernel/principals/frames.ts";
import { parseServicePrincipals } from "../../../control/src/auth.ts";
import { ControlDirectory, IngressMisconfigured, LookupBudgetExceeded, type ControlAuthenticator } from "../directory.ts";
import type { ServiceTenantPort } from "../invoke.ts";
import { Limiter, parseRate } from "../limits.ts";
import { DeliveryDesk, MAX_LEDGER, type DeliveryRequest } from "../object.ts";
import { classifyFailure, PROVIDERS, telnyxFresh, telnyxSeconds } from "../providers.ts";
import { metaDescription, ogTitle, truncate } from "../share.ts";
import { control } from "./helpers.ts";

const grant: ServiceGrant = { principalId: "ingress", route: { projectId: "p", tenantId: "t", databaseUrl: "do://tenant/t" }, functions: ["calling.ingestWebhook"], maxDelegationSeconds: 900, channel: "system" };
const request = (key?: string): DeliveryRequest => ({
  v: 1,
  grant,
  path: "calling.ingestWebhook",
  args: { rawBody: "{}" },
  bucket: "webhook",
  ...(key ? { replay: { key, untilMs: 10_000 } } : {}),
});

type Seen = { grant: ServiceGrant; call: ServiceTenantCall };
function port(answer: (seen: Seen) => Promise<ServiceTenantOutcome>) {
  const seen: Seen[] = [];
  const value: ServiceTenantPort = { serviceCall: async (g, call) => { seen.push({ grant: g, call }); return answer({ grant: g, call }); } };
  return { value, seen };
}
const ok = (value: unknown): ServiceTenantOutcome => ({ ok: true, result: JSON.stringify(value) });

describe("rate limits", () => {
  it("parses the gateway's <per second>/<burst> form", () => {
    expect(parseRate("50/100", { perSecond: 1, burst: 1 })).toEqual({ perSecond: 50, burst: 100 });
    expect(parseRate("5", { perSecond: 1, burst: 1 })).toEqual({ perSecond: 5, burst: 5 });
    expect(parseRate("0", { perSecond: 1, burst: 1 })).toEqual({ perSecond: 0, burst: 1 });
    expect(parseRate("x/2", { perSecond: 1, burst: 1 })).toEqual({ perSecond: 1, burst: 1 });
    expect(parseRate(undefined, { perSecond: 9, burst: 9 })).toEqual({ perSecond: 9, burst: 9 });
  });

  it("keeps a bucket per key, at most maxBuckets of them, dropping the least recently used", () => {
    const limiter = new Limiter({ webhook: { perSecond: 1, burst: 1 } }, 3);
    expect(limiter.take("webhook", 0, "a")).toBe(true);
    expect(limiter.take("webhook", 0, "a")).toBe(false);
    expect(limiter.take("webhook", 0, "b")).toBe(true);
    for (const key of ["c", "d"]) limiter.take("webhook", 0, key);
    expect(limiter.size).toBe(3);
    // "a" was the least recently used: dropped, so it starts full again.
    expect(limiter.take("webhook", 0, "a")).toBe(true);
  });

  it("allows the burst, then the rate", () => {
    const limiter = new Limiter({ webhook: { perSecond: 2, burst: 3 } });
    expect([0, 0, 0, 0].map(() => limiter.take("webhook", 1_000))).toEqual([true, true, true, false]);
    expect(limiter.take("webhook", 1_400)).toBe(false);
    expect(limiter.take("webhook", 1_500)).toBe(true);
    expect(limiter.take("other", 1_500)).toBe(true);
  });

  it("answers limited over the tenant's rate, without running the Action", async () => {
    const actions = port(async () => ok(1));
    const desk = new DeliveryDesk(() => actions.value, new Limiter({ webhook: { perSecond: 0.001, burst: 2 } }), () => 0);
    const outcomes = await Promise.all([1, 2, 3].map((n) => desk.deliver(request(`k${n}`))));
    expect(outcomes.map((o) => o.kind)).toEqual(["done", "done", "limited"]);
    expect(actions.seen.length).toBe(2);
  });
});

describe("the delivery desk", () => {
  it("hands K17's tenant path the grant as resolved and an Action call with a fresh command id", async () => {
    const actions = port(async () => ok(null));
    const desk = new DeliveryDesk(() => actions.value, new Limiter({}), () => 0);
    await desk.deliver(request());
    await desk.deliver(request());
    expect(actions.seen[0]!.grant).toEqual(grant);
    expect(actions.seen[0]!.call).toMatchObject({ kind: "action", path: "calling.ingestWebhook", args: { rawBody: "{}" } });
    expect(actions.seen[0]!.call).not.toHaveProperty("channel");
    expect(actions.seen[0]!.call.id).toMatch(/^ingress_[0-9a-f]{32}$/);
    expect(actions.seen[1]!.call.id).not.toBe(actions.seen[0]!.call.id);
  });

  it("parses the result text once, and treats a result that is not JSON as transient", async () => {
    const desk = new DeliveryDesk(() => ({ serviceCall: async () => ({ ok: true, result: '{"a":1}' }) }), new Limiter({}), () => 0);
    expect(await desk.deliver(request())).toEqual({ kind: "done", report: { ok: true, value: { a: 1 } } });
    const broken = new DeliveryDesk(() => ({ serviceCall: async () => ({ ok: true, result: "{" }) }), new Limiter({}), () => 0);
    expect(await broken.deliver(request())).toMatchObject({ kind: "done", report: { ok: false, transient: true } });
  });

  it("refuses a function outside the principal's allowlist and a missing Action runtime", async () => {
    const actions = port(async () => ok(null));
    const desk = new DeliveryDesk(() => actions.value, new Limiter({}), () => 0);
    expect(await desk.deliver({ ...request(), path: "github.ingestWebhook" })).toEqual({ kind: "unavailable", reason: 'function "github.ingestWebhook" is not allowed for the ingress service principal' });
    expect(actions.seen).toEqual([]);
    const unbound = new DeliveryDesk(() => null, new Limiter({}), () => 0);
    expect(await unbound.deliver(request())).toEqual({ kind: "unavailable", reason: "no tenant object namespace is bound (TENANT)" });
  });

  it("remembers only accepted deliveries, until their window ends", async () => {
    let fail = true;
    let now = 0;
    const actions = port(async () => (fail ? { ok: false, error: "database is busy" } : ok(1)));
    const desk = new DeliveryDesk(() => actions.value, new Limiter({}), () => now);
    expect((await desk.deliver(request("a"))).kind).toBe("done");
    expect(desk.ledger.has("a")).toBe(false);
    fail = false;
    await desk.deliver(request("a"));
    expect(await desk.deliver(request("a"))).toEqual({ kind: "done", report: { ok: true, value: null }, duplicate: true });
    expect(actions.seen.length).toBe(2);
    now = 10_001;
    await desk.deliver(request("a"));
    expect(actions.seen.length).toBe(3);
  });

  it("turns a thrown port error into a transient failure", async () => {
    const desk = new DeliveryDesk(() => ({ serviceCall: async () => { throw new Error("Network connection lost."); } }), new Limiter({}), () => 0);
    expect(await desk.deliver(request("x"))).toEqual({ kind: "done", report: { ok: false, error: "Network connection lost.", transient: true } });
    expect(desk.ledger.size).toBe(0);
  });

  it("keeps at most MAX_LEDGER entries", async () => {
    const desk = new DeliveryDesk(() => ({ serviceCall: async () => ok(1) }), new Limiter({}), () => 0);
    for (let n = 0; n < MAX_LEDGER + 5; n++) await desk.deliver(request(`key-${n}`));
    expect(desk.ledger.size).toBe(MAX_LEDGER);
    expect(desk.ledger.has("key-0")).toBe(false);
    expect(desk.ledger.has(`key-${MAX_LEDGER + 4}`)).toBe(true);
  });
});

describe("providers", () => {
  it("accepts a Telnyx timestamp within 300 s either way", () => {
    expect(telnyxSeconds("1790000000")).toBe(1790000000);
    for (const bad of ["", "1e9", "-5", "12.5", " 1", "9".repeat(16)]) expect(telnyxSeconds(bad), bad).toBeNull();
    expect(telnyxFresh(1000, 1_300_000)).toBe(true);
    expect(telnyxFresh(1000, 1_300_001)).toBe(false);
    expect(telnyxFresh(1300, 1_000_000)).toBe(true);
    expect(telnyxFresh(1301, 1_000_000)).toBe(false);
  });

  it("maps the Actions' error texts", () => {
    const [calling, relay, github] = PROVIDERS;
    expect(classifyFailure(calling!, "TypeScript module host returned execution_failed: Invalid Telnyx webhook signature")).toBe("signature");
    expect(classifyFailure(relay!, "Telnyx webhook timestamp is outside the replay window")).toBe("signature");
    expect(classifyFailure(relay!, "Telnyx webhook verification is not configured")).toBe("other");
    expect(classifyFailure(github!, "GitHub webhook payload is too large")).toBe("tooLarge");
    expect(classifyFailure(github!, "tenant is not available")).toBe("unknownTenant");
  });
});

describe("tenant resolution", () => {
  it("treats a refused ingress credential as a deploy error, never as an unknown tenant", async () => {
    const directory = new ControlDirectory(() => control() as unknown as ControlAuthenticator, "gvx_svc_not_the_ingress");
    await expect(directory.resolve("proj-fixture", "tenant_alpha")).rejects.toBeInstanceOf(IngressMisconfigured);
  });

  const unknownTenant: ControlAuthenticator = {
    authenticate: async () => ({ message: { type: "auth.error", id: "ingress", code: "transient", error: "tenant is not available" }, connection: {} as never }),
  };

  it("caches answers for 30 s, keyed by the exact selector", async () => {
    let now = 0;
    const directory = new ControlDirectory(() => unknownTenant, "gvx_svc_x", { clock: () => now });
    expect(await directory.resolve("p", "nobody")).toBeNull();
    expect(await directory.resolve("p", "nobody")).toBeNull();
    expect(directory.lookups).toBe(1);
    // A different spelling is a different selector (K06 matches tenant ids case-sensitively).
    await directory.resolve("p", "NOBODY");
    expect(directory.lookups).toBe(2);
    now = 30_001;
    await directory.resolve("p", "nobody");
    expect(directory.lookups).toBe(3);
  });

  it("caps the negative cache, dropping the oldest entries", async () => {
    const directory = new ControlDirectory(() => unknownTenant, "gvx_svc_x", { clock: () => 0, cap: 3, lookupRate: { perSecond: 0, burst: 1 } });
    for (const selector of ["a", "b", "c", "d"]) await directory.resolve("p", selector);
    expect([...directory.unknown.entries.keys()]).toEqual(["p\u0000b", "p\u0000c", "p\u0000d"]);
  });

  it("spends a per-project lookup budget on misses only, then refuses without asking the control object", async () => {
    let now = 0;
    const directory = new ControlDirectory(() => unknownTenant, "gvx_svc_x", { clock: () => now, lookupRate: { perSecond: 1, burst: 2 } });
    await directory.resolve("p", "a");
    await directory.resolve("p", "b");
    await expect(directory.resolve("p", "c")).rejects.toBeInstanceOf(LookupBudgetExceeded);
    // Cached answers cost nothing; another project has its own budget.
    expect(await directory.resolve("p", "a")).toBeNull();
    expect(await directory.resolve("q", "c")).toBeNull();
    expect(directory.lookups).toBe(3);
    now = 1_000;
    expect(await directory.resolve("p", "c")).toBeNull();
    expect(directory.lookups).toBe(4);
  });
});

describe("share text", () => {
  it("truncates by characters, not UTF-16 units", () => {
    expect(truncate("📞📞📞📞", 3)).toBe("📞📞…");
    expect(truncate("  short  ", 10)).toBe("short");
    expect(truncate(null, 3)).toBe("");
  });

  it("builds the title and description from the preview", () => {
    expect(ogTitle({ title: "", workspaceName: "W" })).toBe("Shared task - W");
    expect(metaDescription({ description: "D", spotName: "S", assigneeCount: 1, dueDate: Date.UTC(2026, 0, 2) })).toBe("D • Spot S • Due Jan 2, 2026 • 1 assignee");
    expect(metaDescription({})).toBe("");
  });
});

describe("principal configuration (K06, 0045 §3)", () => {
  const entry = (extra: Record<string, unknown>) => JSON.stringify([{ id: "ingress", tokenSha256: "a".repeat(64), projects: ["p"], functions: ["calling.ingestWebhook"], ...extra }]);

  it("defaults the channel to api and accepts system", () => {
    expect(parseServicePrincipals(entry({}))[0]!.channel).toBe("api");
    expect(parseServicePrincipals(entry({ channel: "system" }))[0]!.channel).toBe("system");
  });

  it("refuses any other channel", () => {
    expect(() => parseServicePrincipals(entry({ channel: "ui" }))).toThrow('principal "ingress" channel must be "api" or "system"');
  });
});
