// K11's hand-off (note 0034): K11's scheduler claims the row and hands the run
// over; K12 runs it in a Workflow step, renews the claim, and reports once.
// The tenant object here stands in for K11's `Scheduler.renew` / `complete`.
import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { handoffInstanceId, WorkflowHandoff, type DurableRun, type HandoffJob } from "../src/handoff.js";
import { PROJECT } from "./fixture.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function setup(name: string) {
  const tenantId = `${name}-${crypto.randomUUID().slice(0, 8)}`;
  const tenant = env.TENANT.getByName(tenantId);
  await tenant.configure(tenantId);
  const handoff = new WorkflowHandoff(env.ACTION_WORKFLOW as unknown as Workflow<HandoffJob>, PROJECT, tenantId);
  return { tenant, tenantId, handoff };
}

function run(id: string, fn: string, args: unknown, extra: Partial<DurableRun> = {}): DurableRun {
  return {
    claim: { source: "outbox", id, attempt: 1, leaseMs: 900 },
    function: fn, args, actor: { kind: "account", accountId: "acct-alice", email: "alice@example.com" },
    provenance: { channel: "system", rootChannel: "ui", actorAccountId: null, actorMemberId: null, onBehalfOfMemberId: null, rootCommandId: "root", commandId: `outbox-${id}`, parentCommandId: "queuing-command", agentExecutionId: null, threadId: null, turnId: null, toolCallId: null, artifactHash: "a", depth: 0, durableHops: 1, actionStack: [] },
    direct: false, cronName: null, scheduledFor: null, id, ...extra,
  };
}

async function finished(instance: string) {
  const deadline = Date.now() + 20_000;
  for (;;) {
    try {
      const status = await (await env.ACTION_WORKFLOW.get(instance)).status();
      if (status.status === "complete") return status.output as Record<string, unknown>;
      if (status.status === "errored") throw new Error(JSON.stringify(status.error));
    } catch (error) { if (String(error).includes("{")) throw error; }
    if (Date.now() > deadline) throw new Error(`timed out: ${instance}`);
    await sleep(50);
  }
}

describe("K11 hand-off (ActionHandoff on a Workflow)", () => {
  it("accepts every Action, runs it as the re-resolved Member with K11's provenance, renews the claim and reports once", async () => {
    const { tenant, handoff } = await setup("handoff");
    const r = run("h1", "long.wait", { ms: 1_200, tag: "k11" });
    await tenant.grantClaim(r.claim);
    expect(await handoff.startAction(r)).toEqual({ accepted: true });
    // The same claim handed over twice is the same instance.
    expect(await handoff.startAction(r)).toEqual({ accepted: true });
    const output = await finished(handoffInstanceId(r));
    expect(output).toMatchObject({ kind: "done", outcome: { ok: true }, reported: true });
    expect(output.renewals as number).toBeGreaterThanOrEqual(2);
    expect(await tenant.reportedRuns()).toEqual([{ claim: r.claim, outcome: { ok: true } }]);
  });

  it("reports a failing Action's error for K11 to retry or kill", async () => {
    const { tenant, handoff } = await setup("handoff-fail");
    const r = run("h2", "long.fail", {});
    await tenant.grantClaim(r.claim);
    await handoff.startAction(r);
    expect(await finished(handoffInstanceId(r))).toMatchObject({ kind: "done", outcome: { ok: false }, reported: true });
    const [report] = await tenant.reportedRuns();
    expect(report!.outcome).toMatchObject({ ok: false });
    expect((report!.outcome as { error: string }).error).toMatch(/^TypeScript module host returned execution_failed: module proj-k12 function long\.fail: Error: long\.fail always fails/);
  });

  it("stops without reporting when K11 gives the row to a newer delivery", async () => {
    const { tenant, handoff } = await setup("handoff-lost");
    const r = run("h3", "long.wait", { ms: 3_000 });
    await tenant.grantClaim(r.claim);
    await handoff.startAction(r);
    await sleep(400);
    await tenant.revokeClaim(r.claim);
    expect(await finished(handoffInstanceId(r))).toMatchObject({ kind: "lost" });
    expect(await tenant.reportedRuns()).toEqual([]);
  });

  it("a scheduled job as the system actor, and a direct provenance filled from the session", async () => {
    const { tenant, handoff } = await setup("handoff-job");
    const job = run("job_1", "long.wait", { ms: 1 }, { claim: { source: "job", id: "job_1", token: "owner:1", leaseMs: 120_000 }, actor: { kind: "system" } });
    await tenant.grantClaim(job.claim);
    await handoff.startAction(job);
    expect(await finished(handoffInstanceId(job))).toMatchObject({ kind: "done", outcome: { ok: true } });
    const unknown = run("h4", "long.wait", { ms: 1 }, { actor: { kind: "account", accountId: "acct-nobody", email: "" } });
    await tenant.grantClaim(unknown.claim);
    await handoff.startAction(unknown);
    // A Member that is no longer admitted fails the delivery, as in Rust.
    expect(await finished(handoffInstanceId(unknown))).toMatchObject({ kind: "done", outcome: { ok: false, error: "invalid session" } });
  });
});
