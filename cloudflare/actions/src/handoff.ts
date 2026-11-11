// K11's hand-off (note 0034, approved in 0032): K11's scheduler claims the
// outbox row or the scheduled job in the tenant object, applies the durable
// hop, and hands the run to K12 through its `ActionHandoff` port. K12 runs it
// in a Workflow step, renews the claim through the tenant object, and reports
// the outcome once. K11 owns the row, its attempts, retries and dead state.
//
// The types mirror cloudflare/kernel/scheduler/ports.ts (K11, PR #357)
// structurally, so this file needs no import from a package that is not merged.
import type { Provenance as RunnerProvenance } from "./provenance.js";
import { parseProvenance } from "./provenance.js";
import type { ControlPort } from "./ports.js";
import type { ActionRuntime } from "./runtime.js";
import { systemSession, type ActionSession } from "./session.js";

/** K11 `Claim`. */
export type HandoffClaim =
  | { readonly source: "job"; readonly id: string; readonly token: string; readonly leaseMs: number }
  | { readonly source: "outbox"; readonly id: string; readonly attempt: number; readonly leaseMs: number };

/** K11 `DurableRun` (the fields K12 reads). */
export interface DurableRun {
  readonly claim: HandoffClaim;
  readonly function: string;
  readonly args: unknown;
  readonly actor: { readonly kind: "system" } | { readonly kind: "account"; readonly accountId: string; readonly email: string };
  /** Hop applied; channel `scheduler` (jobs) or `system` (outbox). Persisted shape: no deadline. */
  readonly provenance: unknown;
  /** The stored provenance had no root: fill the resolved session's actor in (K11 `completeDirect`). */
  readonly direct: boolean;
  readonly cronName: string | null;
  readonly scheduledFor: number | null;
  readonly id: string;
}

export type HandoffRunOutcome = { readonly ok: true } | { readonly ok: false; readonly error: string };
export type HandoffOutcome = HandoffRunOutcome | { readonly accepted: true };

/** What the tenant object exposes for handed-off runs: K11's `Scheduler.renew` and `Scheduler.complete`. */
export interface HandoffTenantPort {
  renewRun(request: { v: 1; claim: HandoffClaim }): Promise<boolean>;
  completeRun(request: { v: 1; claim: HandoffClaim; outcome: HandoffRunOutcome }): Promise<boolean>;
}

/** The Workflow payload of a handed-off run. */
export interface HandoffJob { v: 1; kind: "handoff"; projectId: string; tenantId: string; run: DurableRun }

/** One Workflow instance per claim: a retried hand-off of the same claim is the same instance. */
export function handoffInstanceId(run: Pick<DurableRun, "id" | "claim">): string {
  const suffix = run.claim.source === "outbox" ? `a${run.claim.attempt}` : `j${run.claim.token}`;
  return `h-${run.id}-${suffix}`.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 100);
}

/** K11's `ActionHandoff`, implemented with a Workflow (0016 G0-8): every Action is accepted and runs in a step. */
export class WorkflowHandoff {
  constructor(private readonly workflow: Workflow<HandoffJob>, private readonly projectId: string, private readonly tenantId: string) {}

  async startAction(run: DurableRun): Promise<HandoffOutcome> {
    try {
      await this.workflow.create({ id: handoffInstanceId(run), params: { v: 1, kind: "handoff", projectId: this.projectId, tenantId: this.tenantId, run } });
    } catch (error) {
      if (!/already exists|duplicate/i.test(String((error as Error)?.message ?? error))) throw error;
    }
    return { accepted: true };
  }
}

export interface HandoffDeps {
  runtime: ActionRuntime;
  tenant: HandoffTenantPort;
  control?: ControlPort | undefined;
  /** Renewal interval; K11 asks for at least every `leaseMs / 2`. Defaults to min(30 s, leaseMs / 3). */
  renewMs?: number | undefined;
}

export type HandoffResult =
  | { kind: "done"; outcome: HandoffRunOutcome; renewals: number }
  | { kind: "lost"; renewals: number };

/**
 * Runs one handed-off Action with its claim renewed; the caller reports a
 * `done` result with `completeRun`. A refused renewal ends the run: K11 gave
 * the row to a newer delivery, and this run's outcome must not be reported.
 */
export async function runHandoff(deps: HandoffDeps, job: HandoffJob): Promise<HandoffResult> {
  const { run } = job;
  let session: ActionSession;
  try {
    session = run.actor.kind === "system" || run.actor.accountId === "" || run.actor.accountId.startsWith("_gonvex_")
      ? systemSession(job.projectId, job.tenantId)
      : await requireControl(deps).tenantSessionForAccount(job.projectId, job.tenantId, run.actor.accountId);
  } catch (error) {
    return { kind: "done", outcome: { ok: false, error: String((error as Error)?.message ?? error) }, renewals: 0 };
  }
  const provenance = parseProvenance(run.provenance);
  if (provenance && run.direct) {
    provenance.actorAccountId = session.account.id.trim() || null;
    provenance.actorMemberId = session.member.id.trim() || null;
    if (session.delegation !== undefined && session.delegation !== null) provenance.delegation = session.delegation;
  }
  let renewals = 0;
  let lostLease: () => void = () => undefined;
  const lost = new Promise<"lost">((resolve) => { lostLease = () => resolve("lost"); });
  let stopped = false;
  const timer = setInterval(() => {
    if (stopped) return;
    deps.tenant.renewRun({ v: 1, claim: run.claim }).then((ok) => {
      if (stopped) return;
      if (ok) renewals++;
      else lostLease();
    }, () => undefined);
  }, deps.renewMs ?? Math.min(30_000, Math.floor(run.claim.leaseMs / 3)));
  try {
    const report = await Promise.race([
      deps.runtime.execute({ session, path: run.function, args: run.args, provenance: provenance as RunnerProvenance | undefined }),
      lost,
    ]);
    if (report === "lost") return { kind: "lost", renewals };
    return { kind: "done", outcome: report.ok ? { ok: true } : { ok: false, error: report.error }, renewals };
  } finally {
    stopped = true;
    clearInterval(timer);
  }
}

function requireControl(deps: HandoffDeps): ControlPort {
  if (!deps.control) throw new Error("Control Plane is unavailable");
  return deps.control;
}
