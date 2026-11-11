// S4's twelve scenarios (docs/cloudflare/spikes/S4-long-actions.md, "Conformance
// scenarios for K12"), on a real tenant object and a real Workflow in local
// workerd, along the two paths an Action takes since 0037:
//
// - outbox Actions: K11's real `Scheduler` claims the row on the tenant alarm
//   and hands it to K12's `WorkflowHandoff`; the Workflow renews K11's claim
//   (`Scheduler.renew`) and reports once (`Scheduler.complete`);
// - direct agent calls: K12's `ActionCalls` run, its own lease and sweep.
//
// Time is scaled: short leases, so "the lease expires after 5 min" becomes
// one short lease. Scenarios that stand in for a runner the platform kills
// record the hand-off and drive the step body by hand, with the claim (or the
// owner) the engine gives a retried step. The 30-minute agent Action runs
// outside the suite (dev/README in ../README.md); scenario 1 here runs the same
// loop for 6 s.
import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { runCall } from "../src/runner.js";
import { TransientRpcError, tenantRpc } from "../src/rpc.js";
import { instanceId } from "../src/tenant.js";
import type { DurableRun } from "../src/handoff.js";
import { deadItems, handedOff, outboxInstance, sleep, tenantFor, until, workflowOutput } from "./helpers.js";
import { member } from "./worker.js";

type Events = Array<{ type: string; outcome: string; attempt?: number; id: string }>;

describe("S4 scenarios: outbox Actions through K11's scheduler and WorkflowHandoff", () => {
  it("1-2. an agent Action runs 6 s in a Workflow step; K11's claim is renewed on cadence; the stream arrives whole; completed once", async () => {
    const { tenant } = await tenantFor("o1", { outboxLeaseMs: 1_500 });
    const id = await tenant.enqueue("long.agent", { seconds: 6, events: 5, delayMs: 20, pauseMs: 200 });
    const output = await workflowOutput(outboxInstance(id));
    expect(output).toMatchObject({ kind: "done", outcome: { ok: true }, reported: true });
    // A renewal every third of K11's 1.5 s lease: about 12 in 6 s.
    const renewals = output.renewals as { renewed: number; gapsMs: { min: number; max: number } };
    expect(renewals.renewed).toBeGreaterThanOrEqual(9);
    expect(renewals.gapsMs.max).toBeLessThan(750); // always well inside K11's half-lease rule
    expect(await tenant.outboxRows()).toEqual([]);
    const events = (await tenant.schedulerEvents()) as Events;
    expect(events.filter((event) => event.type === "action").map((event) => event.outcome)).toEqual(["accepted", "completed"]);
  });

  it("2. one long event-stream run arrives in order (checked inside the Action)", async () => {
    const { runtime, tenantId } = await tenantFor("o2");
    const report = await runtime.execute({ session: member(tenantId), path: "long.agent", args: { seconds: 1, events: 30, delayMs: 10, pauseMs: 10 } });
    const value = JSON.parse((report as { value: string }).value);
    expect(value.inOrder).toBe(true);
    expect(value.events).toBe(value.turns * 30);
  });

  it("4. one request waits for its response while the claim is renewed", async () => {
    const { tenant } = await tenantFor("o4", { outboxLeaseMs: 900 });
    const id = await tenant.enqueue("long.slowRequest", { ms: 3_000 });
    const output = await workflowOutput(outboxInstance(id));
    expect(output).toMatchObject({ kind: "done", outcome: { ok: true }, reported: true });
    expect((output.renewals as { renewed: number }).renewed).toBeGreaterThanOrEqual(7);
  });

  it("5. the step dies mid-run; the engine's retry runs again under the same claim; completed once, the dead run's report refused", async () => {
    const { tenant, runRecorded } = await tenantFor("o5", { handoff: "record" });
    const id = await tenant.enqueue("long.effect", { ms: 300, tag: "x" });
    const [run] = await until(async () => { const runs = await handedOff(tenant); return runs.length ? runs : undefined; }, "hand-off");
    expect(run!.claim).toMatchObject({ source: "outbox", id, attempt: 1 });
    // The first step dies with its isolate (never reports). The engine retries the step with the same payload.
    void runRecorded(run!, { stopRenewing: () => true });
    const retried = await runRecorded(run!);
    expect(retried).toMatchObject({ kind: "done", outcome: { ok: true } });
    expect(await tenant.completeRun({ v: 1, claim: run!.claim, outcome: { ok: true } })).toBe(true);
    expect(await tenant.completeRun({ v: 1, claim: run!.claim, outcome: { ok: true } })).toBe(false);
    expect(await tenant.outboxRows()).toEqual([]);
  });

  it("6. a zombie stops renewing; K11's lease expires; K11 re-claims as attempt 2; the zombie's renewal and report are refused", async () => {
    const { tenant, runRecorded } = await tenantFor("o6", { handoff: "record", outboxLeaseMs: 1_200 });
    const id = await tenant.enqueue("long.effect", { ms: 3_500, tag: "zombie" });
    const [first] = await until(async () => { const runs = await handedOff(tenant); return runs.length ? runs : undefined; }, "hand-off");
    const zombie = runRecorded(first!, { stopRenewing: (elapsed) => elapsed > 300 });
    // K11's alarm finds the expired lease and hands the row over again.
    const second = await until(async () => (await handedOff(tenant)).find((run) => run.claim.source === "outbox" && run.claim.attempt === 2), "re-claim", 10_000);
    expect(await runRecorded(second)).toMatchObject({ kind: "done", outcome: { ok: true } });
    expect(await tenant.completeRun({ v: 1, claim: second.claim, outcome: { ok: true } })).toBe(true);
    const late = await zombie;
    if (late.kind === "done") expect(await tenant.completeRun({ v: 1, claim: first!.claim, outcome: late.outcome })).toBe(false);
    expect(await tenant.renewRun({ v: 1, claim: first!.claim })).toBe(false);
    expect(await tenant.outboxRows()).toEqual([]);
  });

  it("6b. the same with the tenant object reset while the zombie holds the claim", async () => {
    const { tenant, tenantId, runRecorded } = await tenantFor("o6b", { handoff: "record", outboxLeaseMs: 1_500 });
    const id = await tenant.enqueue("long.effect", { ms: 2_500, tag: "zombie" });
    const [first] = await until(async () => { const runs = await handedOff(tenant); return runs.length ? runs : undefined; }, "hand-off");
    const before = await tenant.instanceId();
    const zombie = runRecorded(first!, { stopRenewing: (elapsed) => elapsed > 200 });
    await sleep(400);
    await tenant.crash().catch(() => undefined);
    const after = env.TENANT.getByName(tenantId);
    expect(await after.instanceId()).not.toBe(before);
    // A fresh instance: recovery rests on K11's persisted `locked_at` and the alarm.
    const second = await until(async () => (await handedOff(after)).find((run) => run.claim.source === "outbox" && run.claim.attempt === 2), "re-claim after reset", 10_000);
    expect(await runRecorded(second, { on: after as never })).toMatchObject({ kind: "done", outcome: { ok: true } });
    expect(await after.completeRun({ v: 1, claim: second.claim, outcome: { ok: true } })).toBe(true);
    await zombie;
    expect(await after.completeRun({ v: 1, claim: first!.claim, outcome: { ok: true } })).toBe(false);
    expect(await after.outboxRows()).toEqual([]);
    expect(id).toBeTruthy();
  });

  it("7. the hand-off fails; K11 retries the row with backoff; the next hand-off completes it", async () => {
    const { tenant, tenantId } = await tenantFor("o7", { handoff: "fail" });
    const id = await tenant.enqueue("long.wait", { ms: 10 });
    await until(async () => ((await tenant.schedulerEvents()) as Events).some((event) => event.type === "action" && event.outcome === "retry"), "retry");
    await tenant.configure(tenantId, { handoff: "workflow" });
    expect(await workflowOutput(outboxInstance(id, 2))).toMatchObject({ kind: "done", outcome: { ok: true }, reported: true });
    expect(await tenant.outboxRows()).toEqual([]);
  });

  it("8. a duplicated hand-off: two Workflow instances for one claim; one report applies", async () => {
    const { tenant, tenantId, handoffJob } = await tenantFor("o8", { handoff: "record", outboxLeaseMs: 5_000 });
    const id = await tenant.enqueue("long.wait", { ms: 600 });
    const [run] = await until(async () => { const runs = await handedOff(tenant); return runs.length ? runs : undefined; }, "hand-off");
    await env.ACTION_WORKFLOW.create({ id: `dup-a-${id}`, params: handoffJob(run!) });
    await env.ACTION_WORKFLOW.create({ id: `dup-b-${id}`, params: handoffJob(run!) });
    const outputs = await Promise.all([workflowOutput(`dup-a-${id}`), workflowOutput(`dup-b-${id}`)]);
    expect(outputs.map((output) => output.reported).sort()).toEqual([false, true]);
    expect(await env.TENANT.getByName(tenantId).outboxRows()).toEqual([]);
  });

  it("9. a replayed report and a stale attempt's report are refused", async () => {
    const { tenant, runRecorded } = await tenantFor("o9", { handoff: "record" });
    await tenant.enqueue("long.wait", { ms: 1 });
    const [run] = await until(async () => { const runs = await handedOff(tenant); return runs.length ? runs : undefined; }, "hand-off");
    expect(await runRecorded(run!)).toMatchObject({ kind: "done" });
    const stale = { ...run!.claim, attempt: 7 } as DurableRun["claim"];
    expect(await tenant.completeRun({ v: 1, claim: stale, outcome: { ok: true } })).toBe(false);
    expect(await tenant.completeRun({ v: 1, claim: run!.claim, outcome: { ok: true } })).toBe(true);
    expect(await tenant.completeRun({ v: 1, claim: run!.claim, outcome: { ok: true } })).toBe(false);
    expect(await tenant.renewRun({ v: 1, claim: run!.claim })).toBe(false);
  });

  it("11. an Action that fails every attempt is retried with backoff by K11 and ends in `_gonvex_action_dead`; an operator requeues it", async () => {
    const { tenant } = await tenantFor("o11", { outboxMaxAttempts: 3 });
    const id = await tenant.enqueue("long.fail", {});
    const dead = await until(async () => { const rows = await tenant.deadRows(); return rows.length ? rows : undefined; }, "dead", 20_000);
    expect(dead).toMatchObject([{ id, action_path: "long.fail", attempts: 3 }]);
    expect(String(dead[0]!.last_error)).toMatch(/^TypeScript module host returned execution_failed: module proj-k12 function long\.fail: Error: long\.fail always fails/);
    expect(await tenant.outboxRows()).toEqual([]);
    expect((await deadItems(tenant))).toMatchObject([{ id, source: "outbox", function: "long.fail", attempts: 3 }]);
    expect(await tenant.requeueDead(id)).toBe(true);
    expect(await tenant.outboxRows()).toMatchObject([{ id, action_path: "long.fail" }]);
  }, 30_000);

  it("12. deploy skew: a tenant that does not know a method yet is a transient failure the step retries", async () => {
    await expect(tenantRpc("completeRun", async () => { throw new Error('RPC receiver does not implement the method "completeRun"'); })).rejects.toBeInstanceOf(TransientRpcError);
    await expect(tenantRpc("claimCall", async () => { throw new Error("Durable Object reset because its code was updated."); })).rejects.toBeInstanceOf(TransientRpcError);
    await expect(tenantRpc("claimCall", async () => { throw new Error("some module bug"); })).rejects.not.toBeInstanceOf(TransientRpcError);
    // Every tenant method takes one object argument with `v: 1`: an older peer ignores fields it does not know.
    const { tenant, tenantId } = await tenantFor("o12", { dispatch: "record" });
    const id = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.agentWait", args: { ms: 1 } });
    expect(await tenant.claimCall({ v: 1, id, owner: "w", future: "field" } as never)).toMatchObject({ ok: true, attempt: 1 });
  });

  it("an outbox Action runs as Rust delivers it: system channel, command `outbox-<id>`, one durable hop", async () => {
    const { tenant } = await tenantFor("prov", { handoff: "record" });
    const id = await tenant.enqueue("long.wait", { ms: 1, tag: "p" });
    const [run] = await until(async () => { const runs = await handedOff(tenant); return runs.length ? runs : undefined; }, "hand-off");
    expect(run).toMatchObject({ id, function: "long.wait", actor: { kind: "account", accountId: "acct-alice" } });
    expect(run!.provenance).toMatchObject({ channel: "system", commandId: `outbox-${id}`, durableHops: 1 });
    // K04 still stores a partial provenance in outbox rows (no `artifactHash`), which Rust's serde refuses, so K11
    // starts a fresh one as Rust would: the parent is the row id, and the runner fills the Member in (`direct`).
    // The 0032 follow-up (full InvocationProvenance in outbox rows) turns the parent into the queuing command.
    expect(run).toMatchObject({ direct: true, provenance: { parentCommandId: id } });
  });
});

describe("S4 scenarios: direct agent calls through ActionCalls", () => {
  it("1. a direct agent call runs in a Workflow step and is delivered once", async () => {
    const { tenant, tenantId } = await tenantFor("d1", { leaseMs: 2_000, renewMs: 200 });
    const id = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.agentWait", args: { ms: 1_000, tag: "direct" } });
    const output = await workflowOutput(instanceId({ id, n: 1 }));
    expect(output).toMatchObject({ kind: "done", ok: true, completion: { applied: true } });
    expect(output.renewals as number).toBeGreaterThanOrEqual(3);
    const finished = await tenant.finishedCalls();
    expect(finished).toHaveLength(1);
    expect(JSON.parse(finished[0]!.value!)).toMatchObject({ waited: 1_000, tag: "direct", channel: "ui" });
    // A direct call that fails is answered once, not retried (Rust answers the socket once).
    const failing = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.fail", args: {} });
    expect(await workflowOutput(instanceId({ id: failing, n: 1 }))).toMatchObject({ kind: "done", ok: false, completion: { applied: true } });
    expect((await tenant.finishedCalls())[1]).toMatchObject({ id: failing, ok: false });
  });

  it("5. the runner is killed mid-run; the same owner re-claims as attempt 2; one completion", async () => {
    const { tenant, tenantId, runtime, port, callJob } = await tenantFor("d5", { dispatch: "record" });
    const id = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.effect", args: { ms: 300 } });
    const owner = `workflow:${instanceId(callJob(id))}`;
    expect(await port.claimCall({ v: 1, id, owner })).toMatchObject({ ok: true, attempt: 1 });
    expect(await runCall({ runtime, tenant: port }, callJob(id), owner)).toMatchObject({ kind: "done", attempt: 2, ok: true });
    expect(await port.completeCall({ v: 1, id, attempt: 2, value: "{}" })).toEqual({ v: 1, applied: true, appliedAttempt: 2 });
    expect(await port.completeCall({ v: 1, id, attempt: 1, value: "{}" })).toMatchObject({ applied: false, duplicate: true });
    expect(await tenant.finishedCalls()).toHaveLength(1);
  });

  it("6. a zombie stops renewing; its lease expires; the sweep hands it off again; the zombie's completion never applies (and after a reset)", async () => {
    const { tenant, tenantId, runtime, port, callJob } = await tenantFor("d6", { leaseMs: 1_200, renewMs: 150, dispatch: "record" });
    const id = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.effect", args: { ms: 3_000 } });
    const zombie = runCall({ runtime, tenant: port, stopRenewing: (_attempt, elapsed) => elapsed > 300 }, callJob(id), "workflow:zombie");
    await sleep(500);
    await tenant.crash().catch(() => undefined);
    const after = env.TENANT.getByName(tenantId);
    const second = await until(async () => (await after.recordedCalls()).find((job) => job.n === 2), "second hand-off", 10_000);
    const fresh = await runCall({ runtime, tenant: after as never }, second, `workflow:${instanceId(second)}`);
    expect(fresh).toMatchObject({ kind: "done", attempt: 2 });
    expect(await after.completeCall({ v: 1, id, attempt: 2, value: "{}" })).toMatchObject({ applied: true });
    const late = await zombie;
    if (late.kind === "done") expect(await after.completeCall({ v: 1, id, attempt: 1, value: "{}" })).toMatchObject({ applied: false });
    expect(await after.runRows()).toMatchObject([{ id, state: "done", attempts: 2, dispatch_count: 2 }]);
  });

  it("7. the hand-off fails; the sweep hands it off again and it completes", async () => {
    const { tenant, tenantId } = await tenantFor("d7", { leaseMs: 1_000, renewMs: 200, dispatch: "fail" });
    const id = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.agentWait", args: { ms: 10 } });
    expect(await tenant.runRows()).toMatchObject([{ id, dispatch_count: 0 }]);
    await tenant.configure(tenantId, { leaseMs: 1_000, renewMs: 200, dispatch: "workflow" });
    // The alarm's sweep retries the hand-off one renewal interval after it failed.
    expect(await workflowOutput(instanceId({ id, n: 1 }))).toMatchObject({ kind: "done", completion: { applied: true } });
  });

  it("8. a duplicated hand-off: the second runner is refused while leased, then as done; one completion", async () => {
    const { tenant, tenantId, callJob } = await tenantFor("d8", { leaseMs: 5_000, renewMs: 200 });
    const id = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.agentWait", args: { ms: 800 } });
    await env.ACTION_WORKFLOW.create({ id: instanceId({ id, n: 2 }), params: callJob(id, 2) });
    const outputs = await Promise.all([workflowOutput(instanceId({ id, n: 1 })), workflowOutput(instanceId({ id, n: 2 }))]);
    expect(outputs.map((output) => output.kind).sort()).toEqual(["done", "skipped"]);
    expect(outputs.find((output) => output.kind === "skipped")).toMatchObject({ reason: "done" });
    expect(await tenant.finishedCalls()).toHaveLength(1);
  });

  it("9. an applied completion replayed, and a stale attempt's: both no-ops", async () => {
    const { tenant, tenantId, runtime, port, callJob } = await tenantFor("d9", { dispatch: "record" });
    const id = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.agentWait", args: { ms: 1 } });
    expect(await runCall({ runtime, tenant: port }, callJob(id), "workflow:a")).toMatchObject({ kind: "done", attempt: 1 });
    expect(await port.completeCall({ v: 1, id, attempt: 7, value: "{}" })).toMatchObject({ applied: false, rejected: true });
    expect(await port.completeCall({ v: 1, id, attempt: 1, value: "{}" })).toMatchObject({ applied: true });
    expect(await port.completeCall({ v: 1, id, attempt: 1, value: "{}" })).toMatchObject({ applied: false, duplicate: true });
    expect(await port.renewCall({ v: 1, id, attempt: 1 })).toMatchObject({ ok: false });
    expect(await port.claimCall({ v: 1, id, owner: "workflow:b" })).toMatchObject({ ok: false, reason: "done" });
  });

  it("10. a call cancelled mid-run: its next renewal is refused, the attempt stops, the call is answered once as cancelled", async () => {
    const { tenant, tenantId, runtime, port, callJob } = await tenantFor("d10", { leaseMs: 3_000, renewMs: 150, dispatch: "record" });
    const id = await tenant.beginCall({ v: 1, session: member(tenantId), path: "long.effect", args: { ms: 5_000 } });
    const running = runCall({ runtime, tenant: port }, callJob(id), "workflow:c");
    await sleep(300);
    expect(await tenant.cancelCall(id)).toBe(true);
    const started = Date.now();
    expect(await running).toMatchObject({ kind: "lost", attempt: 1 });
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(await port.completeCall({ v: 1, id, attempt: 1, value: "{}" })).toMatchObject({ applied: false, rejected: true });
    expect(await tenant.finishedCalls()).toEqual([{ id, ok: false, error: "cancelled" }]);
  });
});
