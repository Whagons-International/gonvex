// What the tenant object composes to serve Actions: the lease protocol
// (`ActionOutbox`) and the Queries and Reducers Actions call (tools,
// `ctx.functions`), run by K04's `TenantModuleHost` in the object.
//
// The tenant object (K07) exposes these as RPC methods with the same names and
// one object argument each (ports.ts), and calls `outbox.afterCommit()` after
// a Reducer commit that wrote the outbox, and `outbox.sweep()` from its alarm
// (K11 multiplexes the alarm).
import type { TenantModuleHost } from "../../kernel/host/src/index.js";
import type { ActionJob, ActionOutbox, Dispatcher } from "./outbox.js";
import type { Claim, ClaimRequest, CompleteRequest, Completion, FailRequest, RenewRequest, Renewal, TenantActionPort, TenantCallReport, TenantCallRequest } from "./ports.js";
import { publicInfo } from "./provenance.js";
import { identityOf, isAnonymous, type ActionSession } from "./session.js";

export class TenantActions implements TenantActionPort {
  constructor(readonly outbox: ActionOutbox, private readonly host: TenantModuleHost) {}

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

  claim(request: ClaimRequest): Promise<Claim> { return this.outbox.claim(request); }
  renew(request: RenewRequest): Promise<Renewal> { return this.outbox.renew(request); }
  fail(request: FailRequest) { return this.outbox.fail(request); }
  complete(request: CompleteRequest): Promise<Completion> { return this.outbox.complete(request); }
  beginCall(request: { v: 1; session: ActionSession; path: string; args: unknown; provenance?: unknown }): Promise<string> { return this.outbox.beginCall(request); }
}

/** The tenant's hand-off: one Workflow instance per (row, dispatch number). */
export class WorkflowDispatcher implements Dispatcher {
  constructor(private readonly workflow: Workflow<ActionJob>) {}

  async dispatch(job: ActionJob): Promise<void> {
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
export function instanceId(job: Pick<ActionJob, "id" | "n">): string {
  return `${job.id.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 90)}-d${job.n}`;
}
