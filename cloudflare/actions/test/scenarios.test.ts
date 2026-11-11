// S4's twelve scenarios (docs/cloudflare/spikes/S4-long-actions.md, "Conformance
// scenarios for K12"), ported to the kernel: a real tenant object (outbox,
// leases, alarm sweep) and a real Workflow (`ActionWorkflow`) in local workerd.
//
// Time is scaled: the tenant runs with a short lease and renewal interval
// (`configure`), so "the lease expires after 5 min" becomes one short lease.
// Scenarios that need to stand in for a runner the platform kills drive the
// Workflow step's body (`runClaimed`) by hand, with the owner the engine gives
// a retried step (the instance id). The 30-minute agent Action runs outside
// the suite (dev/README.md); scenario 1 here runs the same loop for 6 s.
import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { ActionRuntime } from "../src/runtime.js";
import { runClaimed } from "../src/runner.js";
import type { ActionJob } from "../src/outbox.js";
import type { TenantActionPort } from "../src/ports.js";
import { TransientRpcError, tenantRpc } from "../src/rpc.js";
import { instanceId } from "../src/tenant.js";
import { PROJECT } from "./fixture.js";
import { member, registry, testControl, testFetcher } from "./worker.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function tenantFor(name: string, config: { leaseMs?: number; renewMs?: number; maxAttempts?: number; dispatch?: "workflow" | "record" | "fail" } = {}) {
  const tenantId = `${name}-${crypto.randomUUID().slice(0, 8)}`;
  const tenant = env.TENANT.getByName(tenantId);
  await tenant.configure(tenantId, config);
  const runtime = new ActionRuntime({ registry: registry(), tenant: tenant as unknown as TenantActionPort, control: testControl, fetcher: testFetcher });
  const job = (id: string, n = 1): ActionJob => ({ v: 1, projectId: PROJECT, tenantId, id, n });
  return { tenant, tenantId, runtime, job, port: tenant as unknown as TenantActionPort };
}

async function until<T>(check: () => Promise<T | undefined | false>, label: string, ms = 30_000): Promise<T> {
  const deadline = Date.now() + ms;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out: ${label}`);
    await sleep(50);
  }
}

async function workflowOutput(id: string, n = 1): Promise<Record<string, unknown>> {
  return until(async () => {
    try {
      const status = await (await env.ACTION_WORKFLOW.get(instanceId({ id, n }))).status();
      if (status.status === "errored" || status.status === "terminated") throw new Error(`workflow ${status.status}: ${JSON.stringify(status.error)}`);
      return status.status === "complete" ? (status.output as Record<string, unknown>) : undefined;
    } catch (error) {
      if (String(error).includes("workflow ")) throw error;
      return undefined;
    }
  }, `workflow ${id}-d${n}`);
}

describe("S4 scenarios on the kernel", () => {
  it("1-2. an agent Action runs for 6 s in a Workflow step; renewals keep their cadence; the stream arrives whole and in order; applied once", async () => {
    const { tenant } = await tenantFor("s1", { leaseMs: 3_000, renewMs: 300 });
    const id = await tenant.enqueue("long.agent", { seconds: 6, events: 5, delayMs: 20, pauseMs: 200 });
    const output = await workflowOutput(id);
    expect(output).toMatchObject({ kind: "done", origin: "outbox", attempt: 1, ok: true, completion: { applied: true, appliedAttempt: 1 } });
    // 6 s at 300 ms: about 20 renewals; at least 15 survive scheduling jitter in local workerd.
    expect(output.renewals as number).toBeGreaterThanOrEqual(15);
    // Writes to `locked_at` only below half a lease: far fewer than renewals.
    expect(output.persisted as number).toBeLessThanOrEqual(Math.ceil((output.renewals as number) / 4) + 1);
    expect(await tenant.outboxRows()).toEqual([]);
    expect(await tenant.runRows()).toEqual([{ id, origin: "outbox", state: "done", attempts: 1, dispatch_count: 1, last_error: null }]);
  });

  it("2. the stream's events arrive whole, in order (one long run, checked inside the Action)", async () => {
    const { runtime, tenantId } = await tenantFor("s2");
    const report = await runtime.execute({ session: member(tenantId), path: "long.agent", args: { seconds: 1, events: 30, delayMs: 10, pauseMs: 10 } });
    const value = JSON.parse((report as { value: string }).value);
    expect(value.inOrder).toBe(true);
    expect(value.events).toBe(value.turns * 30);
  });

  it("4. one request waits for its response longer than a standard Action's budget would allow", async () => {
    const { tenant } = await tenantFor("s4", { leaseMs: 2_000, renewMs: 200 });
    const id = await tenant.enqueue("long.slowRequest", { ms: 3_000 });
    const output = await workflowOutput(id);
    expect(output).toMatchObject({ kind: "done", ok: true, completion: { applied: true } });
    expect(output.renewals as number).toBeGreaterThanOrEqual(10);
  });

  it("5. the runner is killed mid-run; the same owner re-claims as attempt 2; one completion", async () => {
    const { tenant, runtime, job, port } = await tenantFor("s5", { dispatch: "record" });
    const id = await tenant.enqueue("long.effect", { ms: 400, tag: "x" });
    const owner = `workflow:${instanceId(job(id))}`;
    // Attempt 1 dies with its isolate: it never renews, completes or fails.
    const claim1 = await port.claim({ v: 1, id, owner });
    expect(claim1).toMatchObject({ ok: true, attempt: 1 });
    // The engine retries the step in the same instance: same owner, immediate takeover.
    const outcome = await runClaimed({ runtime, tenant: port, control: testControl }, job(id), owner);
    expect(outcome).toMatchObject({ kind: "done", attempt: 2, ok: true });
    expect(await port.complete({ v: 1, id, attempt: 2 })).toEqual({ v: 1, applied: true, appliedAttempt: 2 });
    // A late completion from the dead attempt is rejected; a replay is a duplicate.
    expect(await port.complete({ v: 1, id, attempt: 1 })).toMatchObject({ applied: false, duplicate: true });
    expect(await port.complete({ v: 1, id, attempt: 2 })).toMatchObject({ applied: false, duplicate: true, appliedAttempt: 2 });
    expect(await tenant.runRows()).toMatchObject([{ id, state: "done", attempts: 2 }]);
  });

  it("6. a zombie stops renewing; its lease expires; the sweep hands off again; a new owner wins; the zombie's completion is rejected", async () => {
    const { tenant, runtime, job, port } = await tenantFor("s6", { leaseMs: 1_200, renewMs: 150, dispatch: "record" });
    const id = await tenant.enqueue("long.effect", { ms: 3_000, tag: "zombie" });
    expect(await tenant.recordedJobs()).toHaveLength(1);
    const zombie = runClaimed({ runtime, tenant: port, control: testControl, stopRenewing: (_attempt, elapsed) => elapsed > 300 }, job(id), "workflow:zombie");
    // The lease runs out about 1.2 s after the last renewal; the alarm sweep hands the row off again.
    const second = await until(async () => (await tenant.recordedJobs()).find((recorded) => recorded.n === 2), "second hand-off", 10_000);
    const fresh = await runClaimed({ runtime, tenant: port, control: testControl }, second, `workflow:${instanceId(second)}`);
    expect(fresh).toMatchObject({ kind: "done", attempt: 2 });
    expect(await port.complete({ v: 1, id, attempt: 2 })).toMatchObject({ applied: true });
    const late = await zombie;
    // The zombie either noticed (its renewal was refused) or finished; its completion never applies.
    if (late.kind === "done") expect(await port.complete({ v: 1, id, attempt: late.attempt })).toMatchObject({ applied: false, duplicate: true });
    else expect(late.kind).toBe("lost");
    expect(await tenant.runRows()).toMatchObject([{ id, state: "done", attempts: 2, dispatch_count: 2 }]);
  });

  it("6b. the same with the tenant object reset while the zombie holds the lease: the persisted lease still expires", async () => {
    const { tenant, runtime, job, port, tenantId } = await tenantFor("s6b", { leaseMs: 1_500, renewMs: 150, dispatch: "record" });
    const id = await tenant.enqueue("long.effect", { ms: 2_500, tag: "zombie" });
    const before = await tenant.instanceId();
    const zombie = runClaimed({ runtime, tenant: port, control: testControl, stopRenewing: (_attempt, elapsed) => elapsed > 200 }, job(id), "workflow:zombie");
    await sleep(400);
    await tenant.crash().catch(() => undefined);
    const after = env.TENANT.getByName(tenantId);
    expect(await after.instanceId()).not.toBe(before);
    // A fresh instance: no lease in memory. Recovery rests on `locked_at` and the alarm.
    const second = await until(async () => (await after.recordedJobs()).find((recorded) => recorded.n >= 2), "hand-off after reset", 10_000);
    const fresh = await runClaimed({ runtime, tenant: after as unknown as TenantActionPort, control: testControl }, second, `workflow:${instanceId(second)}`);
    expect(fresh).toMatchObject({ kind: "done", attempt: 2 });
    expect(await (after as unknown as TenantActionPort).complete({ v: 1, id, attempt: 2 })).toMatchObject({ applied: true });
    const late = await zombie;
    if (late.kind === "done") expect(await (after as unknown as TenantActionPort).complete({ v: 1, id, attempt: 1 })).toMatchObject({ applied: false });
    expect(await after.runRows()).toMatchObject([{ id, state: "done", attempts: 2 }]);
  });

  it("7. the hand-off after commit is lost; the sweep hands it off after one lease and it completes", async () => {
    const { tenant, tenantId } = await tenantFor("s7", { leaseMs: 1_000, renewMs: 200 });
    const id = await tenant.enqueue("long.wait", { ms: 10 }, { skipHandoff: true });
    expect(await tenant.runRows()).toEqual([]);
    // Only the alarm (the sweep) can find it now.
    await tenant.sweep();
    const output = await workflowOutput(id);
    expect(output).toMatchObject({ kind: "done", completion: { applied: true } });
    expect(await env.TENANT.getByName(tenantId).runRows()).toMatchObject([{ id, state: "done", dispatch_count: 1 }]);
  });

  it("7b. a failing hand-off is retried by the sweep", async () => {
    const { tenant, tenantId } = await tenantFor("s7b", { leaseMs: 800, renewMs: 200, dispatch: "fail" });
    const id = await tenant.enqueue("long.wait", { ms: 10 });
    expect(await tenant.runRows()).toEqual([]);
    await tenant.configure(tenantId, { leaseMs: 800, renewMs: 200, dispatch: "workflow" });
    await tenant.sweep();
    expect(await workflowOutput(id)).toMatchObject({ kind: "done", completion: { applied: true } });
  });

  it("8. the hand-off is duplicated: the second runner is refused while leased, then as done; one completion", async () => {
    const { tenant, tenantId } = await tenantFor("s8", { leaseMs: 5_000, renewMs: 200 });
    const id = await tenant.enqueue("long.wait", { ms: 800 });
    // A second Workflow instance for the same row, as a duplicated create() would make.
    await env.ACTION_WORKFLOW.create({ id: instanceId({ id, n: 2 }), params: { v: 1, projectId: PROJECT, tenantId, id, n: 2 } });
    const [first, second] = await Promise.all([workflowOutput(id, 1), workflowOutput(id, 2)]);
    const outcomes = [first, second].map((output) => output.kind).sort();
    expect(outcomes).toEqual(["done", "skipped"]);
    expect([first, second].find((output) => output.kind === "skipped")).toMatchObject({ reason: "done" });
    expect(await tenant.runRows()).toMatchObject([{ id, state: "done", attempts: 1 }]);
  });

  it("9. an applied result replayed, and a stale attempt's completion: both no-ops", async () => {
    const { tenant, runtime, job, port } = await tenantFor("s9", { dispatch: "record" });
    const id = await tenant.enqueue("long.wait", { ms: 1 });
    const outcome = await runClaimed({ runtime, tenant: port, control: testControl }, job(id), "workflow:a");
    expect(outcome).toMatchObject({ kind: "done", attempt: 1 });
    expect(await port.complete({ v: 1, id, attempt: 7 })).toMatchObject({ applied: false, rejected: true });
    expect(await port.complete({ v: 1, id, attempt: 1 })).toMatchObject({ applied: true });
    expect(await port.complete({ v: 1, id, attempt: 1 })).toMatchObject({ applied: false, duplicate: true });
    expect(await port.renew({ v: 1, id, attempt: 1 })).toMatchObject({ ok: false });
    expect(await port.claim({ v: 1, id, owner: "workflow:b" })).toMatchObject({ ok: false, reason: "done" });
  });

  it("10. an Action cancelled mid-run: its next renewal is refused, the attempt stops, nothing is applied", async () => {
    const { tenant, runtime, job, port } = await tenantFor("s10", { leaseMs: 3_000, renewMs: 150, dispatch: "record" });
    const id = await tenant.enqueue("long.effect", { ms: 5_000, tag: "cancel" });
    const running = runClaimed({ runtime, tenant: port, control: testControl }, job(id), "workflow:c");
    await sleep(300);
    expect(await tenant.cancel(id)).toBe(true);
    const started = Date.now();
    expect(await running).toMatchObject({ kind: "lost", attempt: 1 });
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(await port.complete({ v: 1, id, attempt: 1 })).toMatchObject({ applied: false, rejected: true });
    expect(await tenant.listDead()).toMatchObject({ items: [{ id, function: "long.effect", attempts: 1, lastError: "cancelled" }] });
  });

  it("11. an Action that fails every attempt ends dead after MAX_ATTEMPTS, with backoff between attempts; an operator requeues it", async () => {
    const { tenant, tenantId } = await tenantFor("s11", { leaseMs: 2_000, renewMs: 200, maxAttempts: 3 });
    const id = await tenant.enqueue("long.fail", {});
    // Attempt 1 now, 2 after 1 s, 3 after 2 s more, each handed off by the alarm.
    type DeadPage = { items: Array<{ id: string; function: string; attempts: number; lastError: string | null }> };
    const dead = await until<DeadPage>(async () => {
      const page = (await env.TENANT.getByName(tenantId).listDead()) as unknown as DeadPage;
      return page.items.length ? page : undefined;
    }, "dead", 20_000);
    expect(dead.items).toMatchObject([{ id, function: "long.fail", attempts: 3 }]);
    expect(dead.items[0]!.lastError).toMatch(/^TypeScript module host returned execution_failed: module proj-k12 function long\.fail: Error: long\.fail always fails/);
    expect(await tenant.outboxRows()).toEqual([]);
    expect(await tenant.runRows()).toMatchObject([{ id, state: "dead", attempts: 3, dispatch_count: 3 }]);
    // Requeued: back in Rust's table with its attempts reset, and handed off at once.
    expect(await tenant.requeue(id)).toBe(true);
    const rows = await tenant.outboxRows();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ id, action_path: "long.fail" });
    expect(await tenant.requeue("nope")).toBe(false);
  }, 30_000);

  it("12. deploy skew: a tenant that does not know a method yet is a transient failure the step retries", async () => {
    await expect(tenantRpc("claim", async () => { throw new Error("RPC receiver does not implement the method \"claim\""); })).rejects.toBeInstanceOf(TransientRpcError);
    await expect(tenantRpc("claim", async () => { throw new Error("Durable Object reset because its code was updated."); })).rejects.toBeInstanceOf(TransientRpcError);
    await expect(tenantRpc("claim", async () => { throw new Error("some module bug"); })).rejects.not.toBeInstanceOf(TransientRpcError);
    // Every tenant method takes one object argument with `v: 1`: an older peer ignores fields it does not know.
    const { port, tenant } = await tenantFor("s12", { dispatch: "record" });
    const id = await tenant.enqueue("long.wait", { ms: 1 });
    expect(await port.claim({ v: 1, id, owner: "w", future: "field" } as never)).toMatchObject({ ok: true, attempt: 1 });
  });

  it("direct agent call: a run of origin `call`, through a Workflow, delivered once", async () => {
    const { tenant, tenantId } = await tenantFor("direct", { leaseMs: 2_000, renewMs: 200 });
    const id = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.agentWait", args: { ms: 300, tag: "direct" } });
    const output = await workflowOutput(id);
    expect(output).toMatchObject({ kind: "done", origin: "call", ok: true, completion: { applied: true } });
    const finished = await tenant.finishedCalls();
    expect(finished).toHaveLength(1);
    expect(finished[0]).toMatchObject({ id, ok: true });
    expect(JSON.parse(finished[0]!.value!)).toMatchObject({ waited: 300, tag: "direct", channel: "ui" });
    // A direct call that fails is answered once, not retried (Rust answers the socket once).
    const failing = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.fail", args: {} });
    expect(await workflowOutput(failing)).toMatchObject({ kind: "done", ok: false, completion: { applied: true } });
    const calls = await tenant.finishedCalls();
    expect(calls[1]).toMatchObject({ id: failing, ok: false });
    expect(calls[1]!.error).toMatch(/long\.fail always fails/);
  });

  it("an outbox Action runs as the system channel with one durable hop, as Rust delivers it", async () => {
    const { tenant, runtime, job, port } = await tenantFor("prov", { dispatch: "record" });
    const id = await tenant.enqueue("long.wait", { ms: 1, tag: "p" });
    const outcome = await runClaimed({ runtime, tenant: port, control: testControl }, job(id), "workflow:p");
    expect(outcome.kind).toBe("done");
    const value = JSON.parse((outcome as { value: string }).value);
    expect(value).toMatchObject({ channel: "system", commandId: `outbox-${id}` });
    expect(value.parent).toMatch(/^enqueue-/);
  });

  it("an outbox row past 32 durable hops ends dead, not retried", async () => {
    const { tenant, runtime, job, port } = await tenantFor("hops", { dispatch: "record" });
    const id = await tenant.enqueue("long.wait", { ms: 1 });
    // The chain already made 32 hops.
    await runInTenant(tenant, id, 32);
    const outcome = await runClaimed({ runtime, tenant: port, control: testControl }, job(id), "workflow:h");
    expect(outcome).toMatchObject({ kind: "failed", state: "dead", error: "durable work chain exceeded the maximum of 32 scheduled or outbox hops" });
  });
});

async function runInTenant(tenant: DurableObjectStub<import("./worker.js").TestTenant>, id: string, hops: number) {
  const { runInDurableObject } = await import("cloudflare:test");
  await runInDurableObject(tenant, async (_instance, state) => {
    const row = state.storage.sql.exec<{ provenance: string }>("SELECT provenance FROM _gonvex_action_outbox WHERE id = ?", id).one();
    const provenance = { ...JSON.parse(row.provenance), durableHops: hops };
    state.storage.sql.exec("UPDATE _gonvex_action_outbox SET provenance = ? WHERE id = ?", JSON.stringify(provenance), id);
  });
}
