// What the tenant object composes to serve Actions, exposed by RPC with these
// method names and one object argument each:
//
// - `call`: a tool or `ctx.functions` Query/Reducer, run by K04's
//   `TenantModuleHost` (each Reducer its own transaction);
// - `claimCall` / `renewCall` / `completeCall` / `beginCall`: direct agent
//   calls (`ActionCalls`, the only table K12 claims);
// - `renewRun` / `completeRun`: a run K11's scheduler handed over
//   (`Scheduler.renew` / `Scheduler.complete`);
// - `schedule`: `ctx.scheduler` from an Action (`Scheduler.enqueue`).
//
// K11's scheduler gets `WorkflowHandoff` as its `actions` port, and the
// object's `TenantAlarm` also runs `ActionCalls.alarmSource()`.
import type { TenantModuleHost } from "../../kernel/host/src/index.js";
import type { Scheduler } from "../../kernel/scheduler/index.ts";
import type { ActionCalls, CallJob, Dispatcher } from "./calls.js";
import type { HandoffClaim, HandoffRunOutcome, HandoffTenantPort } from "./handoff.js";
import type { Claim, ClaimRequest, CompleteRequest, Completion, RenewRequest, Renewal, ScheduleRequest, TenantActionPort, TenantCallReport, TenantCallRequest } from "./ports.js";
import { publicInfo } from "./provenance.js";
import { identityOf, isAnonymous, type ActionSession } from "./session.js";

export class TenantActions implements TenantActionPort, HandoffTenantPort {
  constructor(readonly calls: ActionCalls, private readonly host: TenantModuleHost, private readonly scheduler: Scheduler) {}

  /** A tool or `ctx.functions` call: one Query, or one Reducer in its own transaction. */
  async call(request: TenantCallRequest): Promise<TenantCallReport> {
    const report = await this.host.run({
      function: request.function,
      kind: request.kind,
      args: request.args,
      identity: identityOf(request.session),
      invocation: publicInfo(request.provenance),
      anonymous: isAnonymous(request.session),
      ...(request.provenance.deadlineUnixMs === null ? {} : { deadlineUnixMs: request.provenance.deadlineUnixMs }),
    });
    if (report.ok) return { ok: true, value: report.value ?? "null" };
    return { ok: false, error: report.error?.message ?? "call failed", transient: report.error?.class === "transient" };
  }

  claimCall(request: ClaimRequest): Promise<Claim> { return this.calls.claim(request); }
  renewCall(request: RenewRequest): Promise<Renewal> { return this.calls.renew(request); }
  completeCall(request: CompleteRequest): Promise<Completion> { return this.calls.complete(request); }
  beginCall(request: { v: 1; session: ActionSession; path: string; args: unknown; provenance?: unknown }): Promise<string> { return this.calls.begin(request); }

  async renewRun(request: { v: 1; claim: HandoffClaim }): Promise<boolean> { return this.scheduler.renew(request.claim); }
  completeRun(request: { v: 1; claim: HandoffClaim; outcome: HandoffRunOutcome }): Promise<boolean> { return this.scheduler.complete(request.claim, request.outcome); }

  /** `ctx.scheduler` from an Action: K11's direct insert (E.b.1). */
  schedule(request: ScheduleRequest): Promise<string> {
    return this.scheduler.enqueue({ function: request.function, args: request.args, runAtUnixMs: request.runAtUnixMs, provenance: request.provenance, coalesce: request.coalesce, jobId: request.id });
  }
}

/** The tenant's hand-off of a direct call: one Workflow instance per (run, hand-off number). */
export class WorkflowDispatcher implements Dispatcher {
  constructor(private readonly workflow: Workflow<CallJob>) {}

  async dispatch(job: CallJob): Promise<void> {
    try {
      await this.workflow.create({ id: instanceId(job), params: job });
    } catch (error) {
      // The same hand-off created twice (a retried RPC): the instance exists and runs once.
      if (/already exists|duplicate/i.test(String((error as Error)?.message ?? error))) return;
      throw error;
    }
  }
}

/** Workflow instance ids allow `[a-zA-Z0-9_-]`, at most 100 characters. */
export function instanceId(job: Pick<CallJob, "id" | "n">): string {
  return `${job.id.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 90)}-d${job.n}`;
}
