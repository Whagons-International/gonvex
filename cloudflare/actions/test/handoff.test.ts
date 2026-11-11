// K11's hand-off, end to end with the real K11 `Scheduler` (0034, 0037): a
// Reducer queues an Action, K11 claims it on the tenant alarm and calls
// `WorkflowHandoff.startAction`, the Workflow runs it with K11's claim renewed
// through `Scheduler.renew`, and reports once through `Scheduler.complete`.
// Scheduled Actions (`ctx.scheduler`) take the same path with a job claim.
import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { handoffInstanceId, WorkflowHandoff, type DurableRun, type HandoffJob } from "../src/handoff.js";
import { handedOff, outboxInstance, tenantFor, until, workflowOutput } from "./helpers.js";
import { PROJECT } from "./fixture.js";
import { member } from "./worker.js";

type Events = Array<{ type: string; outcome: string; id: string; error?: string }>;

describe("K11 hand-off (WorkflowHandoff on K11's real Scheduler)", () => {
  it("an outbox Action: claimed by K11, accepted, run in the Workflow as the re-resolved Member, completed through Scheduler.complete", async () => {
    const { tenant } = await tenantFor("h1", { outboxLeaseMs: 1_500 });
    const id = await tenant.enqueue("long.wait", { ms: 1_200, tag: "k11" });
    const output = await workflowOutput(outboxInstance(id));
    expect(output).toMatchObject({ kind: "done", outcome: { ok: true }, reported: true });
    expect((output.renewals as { renewed: number }).renewed).toBeGreaterThanOrEqual(2);
    expect(await tenant.outboxRows()).toEqual([]);
    const events = ((await tenant.schedulerEvents()) as Events).filter((event) => event.type === "action");
    expect(events.map((event) => [event.id, event.outcome])).toEqual([[id, "accepted"], [id, "completed"]]);
  });

  it("a failing Action is reported with Rust's error text; K11 retries it", async () => {
    const { tenant } = await tenantFor("h2", { outboxMaxAttempts: 2 });
    const id = await tenant.enqueue("long.fail", {});
    const events = await until(async () => {
      const list = ((await tenant.schedulerEvents()) as Events).filter((event) => event.type === "action" && event.outcome === "retry");
      return list.length ? list : undefined;
    }, "retry");
    expect(events[0]).toMatchObject({ id });
    expect(events[0]!.error).toMatch(/^TypeScript module host returned execution_failed: module proj-k12 function long\.fail: Error: long\.fail always fails/);
  });

  it("a renewal or report on a claim K11 no longer holds is refused; the same claim handed over twice is one instance", async () => {
    const { tenant, tenantId } = await tenantFor("h3", { handoff: "record" });
    const id = await tenant.enqueue("long.wait", { ms: 1 });
    const [run] = await until(async () => { const runs = await handedOff(tenant); return runs.length ? runs : undefined; }, "hand-off");
    expect(await tenant.renewRun({ v: 1, claim: run!.claim })).toBe(true);
    const handoff = new WorkflowHandoff(env.ACTION_WORKFLOW as unknown as Workflow<HandoffJob>, PROJECT, tenantId);
    expect(await handoff.startAction(run as DurableRun)).toEqual({ accepted: true });
    expect(await handoff.startAction(run as DurableRun)).toEqual({ accepted: true });
    expect(await workflowOutput(handoffInstanceId(run as DurableRun))).toMatchObject({ kind: "done", reported: true });
    expect(await tenant.renewRun({ v: 1, claim: run!.claim })).toBe(false);
    expect(await tenant.completeRun({ v: 1, claim: run!.claim, outcome: { ok: true } })).toBe(false);
    expect(id).toBeTruthy();
  });

  it("a scheduled Action (ctx.scheduler through the tenant's schedule RPC, K11's enqueue) runs with a job claim and completes the job", async () => {
    const { tenant, tenantId } = await tenantFor("h4");
    const id = await tenant.schedule({ v: 1, id: "job_k12-scheduled", function: "long.wait", args: { ms: 5, tag: "job" }, runAtUnixMs: Date.now(), coalesce: false, session: member(tenantId), provenance: {} });
    expect(id).toBe("job_k12-scheduled");
    const job = await until(async () => {
      const rows = (await tenant.jobRows()) as Array<{ id: string; status: string }>;
      return rows.find((row) => row.id === id && row.status !== "pending");
    }, "job ran", 20_000);
    expect(job).toMatchObject({ id, function_path: "long.wait", status: "completed" });
    const events = ((await tenant.schedulerEvents()) as Events).filter((event) => event.type === "job");
    expect(events.map((event) => event.outcome)).toEqual(["accepted", "completed"]);
  });

  it("a Member who is no longer admitted fails the delivery with the control object's text, as in Rust", async () => {
    const { tenant, runRecorded } = await tenantFor("h5", { handoff: "record" });
    await tenant.enqueue("long.wait", { ms: 1 });
    const [run] = await until(async () => { const runs = await handedOff(tenant); return runs.length ? runs : undefined; }, "hand-off");
    const revoked = { ...run!, actor: { kind: "account" as const, accountId: "acct-revoked", email: "" } };
    expect(await runRecorded(revoked)).toMatchObject({ kind: "done", outcome: { ok: false, error: "invalid session" } });
  });
});
