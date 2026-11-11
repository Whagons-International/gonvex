// The Action runtime beyond the parity scenario: deadlines per profile, the
// ports (secrets, storage K13, scheduler K11, sandbox K20), capability
// declarations Rust refuses, the 16-request limit and the network edge cases.
import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { ActionRuntime, type ActionRuntimeOptions } from "../src/runtime.js";
import { completeUtf8Length, NetworkHost } from "../src/fetch.js";
import type { TenantActionPort } from "../src/ports.js";
import { delegatedProvenance, directProvenance, durableHop, parseProvenance, persisted, publicInfo } from "../src/provenance.js";
import { PROJECT } from "./fixture.js";
import { FakeScheduler, FakeStorage, member, registry, testControl, testFetcher } from "./worker.js";

async function setup(options: Partial<ActionRuntimeOptions> = {}) {
  const tenantId = `runtime-${crypto.randomUUID()}`;
  const tenant = env.TENANT.getByName(tenantId);
  await tenant.configure(tenantId);
  const runtime = new ActionRuntime({ registry: registry(), tenant: tenant as unknown as TenantActionPort, control: testControl, fetcher: testFetcher, ...options });
  return { runtime, tenantId, session: member(tenantId) };
}

const anonymous = (tenantId: string) => ({ projectId: PROJECT, tenantId, account: { id: "" }, member: { id: "" } });

describe("deadlines (B.6)", () => {
  it("a standard Action stops at its deadline with Rust's budget error", async () => {
    const { runtime, session } = await setup({ actionTimeoutMs: 200 });
    const report = await runtime.execute({ session, path: "long.wait", args: { ms: 2_000 } });
    expect(report.ok).toBe(false);
    expect(!report.ok && report.error).toMatch(/^TypeScript module host returned budget_exceeded: module proj-k12 function long\.wait: module exceeded its \d+ ms execution deadline$/);
    expect(!report.ok && report.class).toBe("transient");
  });

  it("an agent Action has no deadline", async () => {
    const { runtime, session } = await setup({ actionTimeoutMs: 200 });
    const report = await runtime.execute({ session, path: "long.agentWait", args: { ms: 600 } });
    expect(report).toMatchObject({ ok: true });
    expect(JSON.parse((report as { value: string }).value)).toMatchObject({ waited: 600, channel: "ui" });
  });

  it("a public Action called anonymously stops at the public deadline", async () => {
    const { runtime, tenantId } = await setup({ publicActionTimeoutMs: 300 });
    const report = await runtime.execute({ session: anonymous(tenantId), path: "long.publicWait", args: { ms: 3_000 } });
    expect(report).toMatchObject({ ok: false, error: "module database capability failed: public Action execution deadline exceeded" });
    const quick = await runtime.execute({ session: anonymous(tenantId), path: "long.publicWait", args: { ms: 10 } });
    expect(quick).toMatchObject({ ok: true });
  });

  it("an anonymous caller may not run a non-public Action", async () => {
    const { runtime, tenantId } = await setup();
    const report = await runtime.execute({ session: anonymous(tenantId), path: "long.wait", args: { ms: 1 } });
    expect(report).toMatchObject({ ok: false, error: "module database capability failed: authenticate with an active tenant before calling a non-public action" });
  });

  it("fetch takes the rest of the deadline as its budget, and cannot start after it", async () => {
    const { runtime, session } = await setup({ actionTimeoutMs: 400 });
    const report = await runtime.execute({ session, path: "long.slowFetch", args: { events: 50, delayMs: 50 } });
    expect(report.ok).toBe(false);
    // The engine's deadline and the request budget end together; either text is Rust's.
    expect(!report.ok && report.error).toMatch(/fetch response body timed out within the \d+ ms Action request budget|module exceeded its \d+ ms execution deadline/);
    let now = 1_000;
    const host = new NetworkHost({ origins: ["http://127.0.0.1:47812"], deadlineUnixMs: 900, fetcher: testFetcher, clock: () => now });
    await expect(host.fetch({ url: "http://127.0.0.1:47812/text" })).rejects.toThrow("fetch cannot start after the Action execution deadline");
    now = 0;
    await expect(host.fetch({ url: "http://127.0.0.1:47812/text" })).resolves.toMatchObject({ status: 200, body: "hello" });
  });

  it("an agent Action streams for longer than a standard Action's budget", async () => {
    const { runtime, session } = await setup({ actionTimeoutMs: 200 });
    const report = await runtime.execute({ session, path: "long.agentSlowFetch", args: { events: 8, delayMs: 60 } });
    expect(report.ok).toBe(true);
    expect(JSON.parse((report as { value: string }).value).text.split("\n\n").filter(Boolean)).toHaveLength(8);
  });
});

describe("ports", () => {
  it("secrets: declared names only, from the control object", async () => {
    const { runtime, session } = await setup();
    const report = await runtime.execute({ session, path: "long.secret", args: {} });
    expect(JSON.parse((report as { value: string }).value)).toEqual({ keys: ["K12_SECRET"], value: "s3cr3t" });
    const { runtime: noControl, session: s2 } = await setup({ control: undefined });
    expect(await noControl.execute({ session: s2, path: "long.secret", args: {} })).toMatchObject({
      ok: false, error: 'Action "proj-k12" requires a capability the Rust runtime has not configured: project secrets require the Control Plane',
    });
  });

  it("storage (K13) and scheduler (K11) receive the session, the payload and the provenance", async () => {
    const storage = new FakeStorage();
    const scheduler = new FakeScheduler();
    const { runtime, session } = await setup({ storage, scheduler });
    const before = Date.now();
    const report = await runtime.execute({ session, path: "long.ports", args: {} });
    const after = Date.now();
    expect(report.ok).toBe(true);
    const value = JSON.parse((report as { value: string }).value);
    expect(value.metadata).toEqual({ id: "file-1", ownerId: "acct-alice", visibility: "tenant", status: "uploaded" });
    expect(value.store).toEqual({ id: "stored-1", sizeBytes: 5, contentType: "text/plain" });
    expect(value.denied).toBe("GonvexHostError: host call failed: storage host call failed [host_call_failed]: file not found");
    expect(value.job).toMatch(/^job_[0-9a-f-]{36}$/);
    expect(value.wake1).toBe(value.wake2);
    expect(storage.calls.map((call) => [call.operation, call.account])).toEqual([["getMetadata", "acct-alice"], ["store", "acct-alice"], ["getMetadata", "acct-alice"]]);
    // runAfter uses the server clock at call time (B.4), not ctx.now.
    expect(scheduler.jobs[0]).toMatchObject({ function: "long.wait", coalesce: false });
    expect(scheduler.jobs[0]!.runAtUnixMs).toBeGreaterThanOrEqual(before + 1_000);
    expect(scheduler.jobs[0]!.runAtUnixMs).toBeLessThanOrEqual(after + 1_000);
    expect(scheduler.jobs[1]).toMatchObject({ coalesce: true, runAtUnixMs: 4102444700000 });
    expect(scheduler.jobs[0]!.provenance).toMatchObject({ channel: "ui", actorAccountId: "acct-alice", actionStack: ["long.ports"] });
    expect(scheduler.jobs[0]!.provenance).not.toHaveProperty("deadlineUnixMs");
  });

  it("an anonymous Action has no scheduler, as in Rust", async () => {
    const { tenantId } = await setup();
    const contract = registry().activeGeneration!.engine.functions.get("long.ports")!;
    const { declaredCapabilities } = await import("../src/runtime.js");
    expect(declaredCapabilities(contract, false).scheduler).toBe(true);
    expect(declaredCapabilities(contract, true).scheduler).toBe(false);
    expect(tenantId).toBeTruthy();
  });

  it("sandbox without a sandbox runtime (K20) fails with Rust's policy text", async () => {
    const { runtime, session } = await setup();
    const report = await runtime.execute({ session, path: "long.sandbox", args: {} });
    expect(JSON.parse((report as { value: string }).value)).toBe("GonvexHostError: host call failed: sandbox host call failed [host_call_failed]: sandbox is disabled by runtime policy");
  });

  it("functions on a standard profile fail every invocation (B.1)", async () => {
    const { runtime, session } = await setup();
    expect(await runtime.execute({ session, path: "long.standardFunctions", args: { ms: 1 } })).toMatchObject({
      ok: false, error: "module database capability failed: interactive function invocation requires an agent Action profile",
    });
  });

  it("the 17th concurrent request is refused at the isolate, without the capability prefix", async () => {
    const { runtime, session } = await setup();
    const report = await runtime.execute({ session, path: "long.concurrent", args: {} });
    expect(JSON.parse((report as { value: string }).value)).toEqual(["GonvexHostError: host call failed: Action concurrent network request limit exceeded"]);
  });

  it("an Action with no module answers Rust's ModuleMissing", async () => {
    const { ModuleRegistry } = await import("../../kernel/host/src/index.js");
    const { runtime, session } = await setup({ registry: new ModuleRegistry(PROJECT) });
    expect(await runtime.execute({ session, path: "long.wait", args: {} })).toMatchObject({ ok: false, error: `project "${PROJECT}" has no active TypeScript module`, class: "transient" });
  });
});

describe("network edge cases", () => {
  const origins = ["http://127.0.0.1:47812", "http://other.test"];
  it("follows 303 as GET without the body, and drops credentials on a host change", async () => {
    const seen: Array<{ url: string; method: string; auth: string | null; body: string }> = [];
    const fetcher = async (request: Request) => {
      seen.push({ url: request.url, method: request.method, auth: request.headers.get("authorization"), body: await request.text() });
      if (request.url.endsWith("/start")) return new Response(null, { status: 303, headers: { location: "http://other.test/next" } });
      return new Response("done", { headers: { "content-type": "text/plain" } });
    };
    const host = new NetworkHost({ origins, deadlineUnixMs: null, fetcher });
    const response = await host.fetch({ url: "http://127.0.0.1:47812/start", method: "POST", headers: { authorization: "Bearer x" }, body: "payload" });
    expect(response).toMatchObject({ status: 200, url: "http://other.test/next", body: "done" });
    expect(seen).toEqual([
      { url: "http://127.0.0.1:47812/start", method: "POST", auth: "Bearer x", body: "payload" },
      { url: "http://other.test/next", method: "GET", auth: null, body: "" },
    ]);
  });

  it("keeps the method and body on 307", async () => {
    const methods: string[] = [];
    const fetcher = async (request: Request) => {
      methods.push(`${request.method}:${await request.text()}`);
      return request.url.endsWith("/a") ? new Response(null, { status: 307, headers: { location: "/b" } }) : new Response("ok");
    };
    const host = new NetworkHost({ origins, deadlineUnixMs: null, fetcher });
    await host.fetch({ url: "http://127.0.0.1:47812/a", method: "PUT", body: "x" });
    expect(methods).toEqual(["PUT:x", "PUT:x"]);
  });

  it("checks the origin again on the host, and refuses non-http URLs", async () => {
    const host = new NetworkHost({ origins, deadlineUnixMs: null, fetcher: testFetcher });
    await expect(host.fetch({ url: "http://127.0.0.1:47813/x" })).rejects.toThrow('fetch origin "http://127.0.0.1:47813" is not declared for this Action');
    await expect(host.fetch({ url: "ftp://127.0.0.1:47812/x" })).rejects.toThrow("fetch only supports absolute http and https URLs");
    await expect(host.fetch({ url: "/relative" })).rejects.toThrow("fetch only supports absolute http and https URLs");
    await expect(host.fetch({})).rejects.toThrow("invalid fetch request: missing field `url`");
  });

  it("maps an unreachable host to reqwest's text", async () => {
    const host = new NetworkHost({ origins: ["http://unreachable.test"], deadlineUnixMs: null, fetcher: testFetcher });
    await expect(host.fetch({ url: "http://unreachable.test/" })).rejects.toThrow("fetch request failed: error sending request");
  });

  it("accepts exactly 8 MiB and refuses one byte more", async () => {
    const host = new NetworkHost({ origins, deadlineUnixMs: null, fetcher: testFetcher });
    const exact = (await host.fetch({ url: "http://127.0.0.1:47812/bytes?n=8388608&chunked=1" })) as { body: string };
    expect(exact.body.length).toBe(8388608);
    await expect(host.fetch({ url: "http://127.0.0.1:47812/bytes?n=8388609&chunked=1" })).rejects.toThrow("fetch response exceeds the 8388608 byte limit");
  });

  it("holds back a split UTF-8 character in a stream", async () => {
    expect(completeUtf8Length(new Uint8Array([0x61, 0xc3]))).toBe(1);
    expect(completeUtf8Length(new Uint8Array([0x61, 0xc3, 0xa9]))).toBe(3);
    expect(completeUtf8Length(new Uint8Array([0xf0, 0x9f, 0x98]))).toBe(0);
    const host = new NetworkHost({ origins, deadlineUnixMs: null, fetcher: testFetcher });
    const head = (await host.fetch({ url: "http://127.0.0.1:47812/sse?events=2&delayMs=5" })) as { stream: number };
    const chunks: string[] = [];
    for (;;) {
      const next = (await host.read(head.stream)) as { done: boolean; chunk?: string };
      if (next.done) break;
      chunks.push(next.chunk!);
    }
    expect(chunks.length).toBeGreaterThan(1);
    expect(chunks.join("")).toBe('data: {"n":1,"text":"café"}\n\ndata: {"n":2,"text":"café"}\n\n');
    expect(chunks.some((chunk) => chunk.includes("�"))).toBe(false);
    expect(await host.read(head.stream)).toEqual({ done: true });
    expect(host.cancel(99)).toBeNull();
  });
});

describe("provenance", () => {
  const session = member("t1");
  it("matches Rust's shapes: direct, delegated, persisted, hops", () => {
    const direct = directProvenance(session, "ui", "action-1", "hash");
    expect(publicInfo(direct)).toEqual({
      channel: "ui", rootChannel: "ui", actorAccountId: "acct-alice", actorMemberId: "mem-alice", onBehalfOfMemberId: null,
      rootCommandId: "action-1", commandId: "action-1", parentCommandId: null, agentExecutionId: null, threadId: null, turnId: null,
      toolCallId: null, artifactHash: "hash", delegation: null,
    });
    const delegated = directProvenance({ ...session, delegation: { principal: "gateway", actor: null } }, "ui", "c", "h");
    expect(delegated.channel).toBe("api");
    const child = delegatedProvenance(session, { ...direct, actionStack: ["a"] }, "agent-2", "hash", () => "0123-4567");
    expect(child).toMatchObject({ channel: "agent", rootChannel: "ui", parentCommandId: "action-1", depth: 1, agentExecutionId: "agent_01234567", actionStack: ["a"] });
    const stored = persisted({ ...direct, deadlineUnixMs: 5 });
    expect(stored).not.toHaveProperty("deadlineUnixMs");
    expect(stored).not.toHaveProperty("durableHops");
    const parsed = parseProvenance(JSON.stringify({ ...stored, durableHops: 31 }))!;
    expect(durableHop(parsed)).toBe(true);
    expect(parsed).toMatchObject({ durableHops: 32, depth: 0 });
    expect(durableHop(parsed)).toBe(false);
    expect(parseProvenance("{}")).toBeUndefined();
  });
});
