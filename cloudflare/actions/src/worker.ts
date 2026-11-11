// The Action Worker (CONTRACT.md "Action Worker", 0016 G0-8):
//
// - `ActionWorkflow` runs every handed-off Action as a Workflow step: no
//   wall-clock limit, and the engine's own retries for crashes. Two payloads:
//   a run K11's scheduler claimed (outbox and scheduled Actions; K11 owns
//   `_gonvex_action_outbox`, 0037), reported back with `completeRun`; and a
//   direct agent call (`ActionCalls`), applied with the fenced `completeCall`.
// - `ActionWorker.call` serves a direct `action.call` (K05/K07): a standard
//   or public Action runs inline, bounded by its 15 min or 60 s deadline; an
//   agent Action becomes a run in `_gonvex_st_action_runs` and a Workflow
//   instance, and its result reaches the socket through the tenant's
//   `onFinished`.
//
// The module bundle ships in the Worker version (G0-2): the build that makes
// the Worker (K19) calls `configureActions` with the project's registry. Queues
// are never used for Actions (S4).
import { WorkflowEntrypoint, WorkerEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import type { ModuleRegistry } from "../../kernel/host/src/index.js";
import type { Fetcher } from "./fetch.js";
import type { CallJob } from "./calls.js";
import { runHandoff, type HandoffJob, type HandoffTenantPort } from "./handoff.js";
import type { Completion, ControlPort, SandboxPort, ScheduleRequest, SchedulerPort, StoragePort, TenantActionPort } from "./ports.js";
import type { Provenance } from "./provenance.js";
import { tenantRpc } from "./rpc.js";
import { runCall, type RunOutcome } from "./runner.js";
import { ActionRuntime, profileOf, type ActionReport } from "./runtime.js";
import type { ActionSession } from "./session.js";

export interface ActionEnv {
  /** Tenant objects, `getByName(tenantId)`; they expose `TenantActions`' methods by RPC (tenant.ts). */
  TENANT: DurableObjectNamespace;
  /** Workflow step timeout for a run (default "24 hours"; agent Actions have no other deadline). */
  ACTION_STEP_TIMEOUT?: string;
  /** First retry delay of the run step (default "5 seconds", doubling), as S4 measured it. */
  ACTION_STEP_RETRY_DELAY?: string;
}

/** What the Worker build provides: the project's generations and the other packages' ports. */
export interface ActionWorkerConfig<Env extends ActionEnv = ActionEnv> {
  registry(env: Env): ModuleRegistry;
  control?(env: Env): ControlPort | undefined;
  /** `ctx.scheduler` from an Action. Default: the tenant's `schedule` RPC (K11's `Scheduler.enqueue`). */
  scheduler?(env: Env, tenantId: string): SchedulerPort | undefined;
  storage?(env: Env, tenantId: string): StoragePort | undefined;
  sandbox?(env: Env, tenantId: string): SandboxPort | undefined;
  fetcher?(env: Env): Fetcher | undefined;
  /** Renewal interval override (tests). */
  renewMs?: number;
}

let config: ActionWorkerConfig<never> | undefined;

export function configureActions<Env extends ActionEnv>(value: ActionWorkerConfig<Env>): void {
  config = value as unknown as ActionWorkerConfig<never>;
}

type TenantStub = TenantActionPort & HandoffTenantPort & {
  beginCall(request: { v: 1; session: ActionSession; path: string; args: unknown; provenance?: unknown }): Promise<string>;
  schedule(request: ScheduleRequest): Promise<string>;
};

function tenantStub(env: ActionEnv, tenantId: string): TenantStub {
  return (env.TENANT as unknown as { getByName(name: string): TenantStub }).getByName(tenantId);
}

function runtimeFor(env: ActionEnv, tenantId: string): { runtime: ActionRuntime; tenant: TenantStub; control: ControlPort | undefined } {
  if (!config) throw new Error("the Action Worker has no module: configureActions was not called");
  const c = config as unknown as ActionWorkerConfig<ActionEnv>;
  const tenant = tenantStub(env, tenantId);
  const control = c.control?.(env);
  const runtime = new ActionRuntime({
    registry: c.registry(env),
    tenant,
    control,
    scheduler: c.scheduler ? c.scheduler(env, tenantId) : { enqueue: (request) => tenantRpc("schedule", () => tenant.schedule(request)) },
    storage: c.storage?.(env, tenantId),
    sandbox: c.sandbox?.(env, tenantId),
    fetcher: c.fetcher?.(env),
  });
  return { runtime, tenant, control };
}

/** A result above this size is applied from inside the run step (a step result holds 1 MiB). */
export const STEP_RESULT_INLINE_BYTES = 512 * 1024;

type StepOutcome =
  | (Omit<Extract<RunOutcome, { kind: "done" }>, "stats" | "renewals"> & { renewals: number; persisted: number; gapsMs: { min: number; max: number } | null; hostCalls: number; fetches: number; completion?: Completion })
  | { kind: "skipped"; reason: string }
  | { kind: "lost"; attempt: number; renewals: number };

export class ActionWorkflow extends WorkflowEntrypoint<ActionEnv, CallJob | HandoffJob> {
  override async run(event: Readonly<WorkflowEvent<CallJob | HandoffJob>>, step: WorkflowStep) {
    const payload = event.payload;
    if (payload.kind === "handoff") return this.handoff(payload, step);
    return this.call(payload, event.instanceId, step);
  }

  /** A direct agent call: claim the run, run it, apply its answer once with `completeCall`. */
  private async call(job: CallJob, instance: string, step: WorkflowStep) {
    // The engine retries a step within the same instance, so the same owner takes its own lease over at once.
    const owner = `workflow:${instance}`;
    const timeout = (this.env.ACTION_STEP_TIMEOUT ?? "24 hours") as "24 hours";
    const delay = (this.env.ACTION_STEP_RETRY_DELAY ?? "5 seconds") as "5 seconds";
    const outcome = (await step.do("run action", { retries: { limit: 10, delay, backoff: "exponential" }, timeout }, async (): Promise<StepOutcome> => {
      const { runtime, tenant } = runtimeFor(this.env, job.tenantId);
      const c = config as unknown as ActionWorkerConfig<ActionEnv>;
      const outcome = await runCall({ runtime, tenant, renewMs: c.renewMs }, job, owner);
      switch (outcome.kind) {
        case "skipped":
          // Another owner holds the lease: let the engine retry later. Anything else is final for this hand-off.
          if (outcome.reason === "leased") throw new Error(`Action call ${job.id} is leased by another runner for ${outcome.retryAfterMs ?? 0} ms`);
          return { kind: "skipped", reason: outcome.reason };
        case "lost": return { kind: "lost", attempt: outcome.attempt, renewals: outcome.renewals.renewed };
        case "done": {
          const { stats, renewals, ...done } = outcome;
          // The first gap runs from the claim; the rest are renewal to renewal (S4's cadence measure).
          const gaps = renewals.gapsMs.slice(1);
          const small = {
            ...done, renewals: renewals.renewed, persisted: renewals.persisted,
            gapsMs: gaps.length ? { min: Math.min(...gaps), max: Math.max(...gaps) } : null,
            hostCalls: stats.hostCalls, fetches: stats.fetches,
          };
          if (done.bytes <= STEP_RESULT_INLINE_BYTES) return small;
          // A step result holds 1 MiB: a large answer is applied from inside the step.
          const completion = await tenantRpc("completeCall", () => tenant.completeCall({ v: 1, id: job.id, attempt: done.attempt, value: done.value, error: done.error }));
          return { ...small, value: undefined, completion };
        }
      }
    })) as StepOutcome;
    if (outcome.kind !== "done" || outcome.completion) return outcome;
    const completion = await step.do("apply result", { retries: { limit: 20, delay: "2 seconds", backoff: "exponential" }, timeout: "5 minutes" }, async () =>
      tenantRpc("completeCall", () => tenantStub(this.env, job.tenantId).completeCall({ v: 1, id: job.id, attempt: outcome.attempt, value: outcome.value, error: outcome.error })));
    return { ...outcome, value: undefined, completion };
  }

  /**
   * A run K11's scheduler claimed and handed over (0034, 0037): run it with
   * its claim renewed, then report the outcome once. K11 decides retry, dead
   * or done. A crash retries the step; K11's claim is still ours until its
   * lease runs out, after which a renewal is refused and the run stops.
   */
  private async handoff(job: HandoffJob, step: WorkflowStep) {
    const timeout = (this.env.ACTION_STEP_TIMEOUT ?? "24 hours") as "24 hours";
    const delay = (this.env.ACTION_STEP_RETRY_DELAY ?? "5 seconds") as "5 seconds";
    const result = await step.do("run action", { retries: { limit: 10, delay, backoff: "exponential" }, timeout }, async () => {
      const { runtime, tenant, control } = runtimeFor(this.env, job.tenantId);
      const c = config as unknown as ActionWorkerConfig<ActionEnv>;
      const outcome = await runHandoff({ runtime, tenant, control, renewMs: c.renewMs }, job);
      return outcome.kind === "done" && !outcome.outcome.ok ? { ...outcome, outcome: { ok: false as const, error: outcome.outcome.error.slice(0, 8_000) } } : outcome;
    });
    if (result.kind === "lost") return result;
    const reported = await step.do("report outcome", { retries: { limit: 20, delay: "2 seconds", backoff: "exponential" }, timeout: "5 minutes" }, async () =>
      tenantRpc("completeRun", () => tenantStub(this.env, job.tenantId).completeRun({ v: 1, claim: job.run.claim, outcome: result.outcome })));
    return { ...result, reported };
  }
}

/** A direct `action.call` from a socket owner. */
export interface DirectCall {
  session: ActionSession;
  path: string;
  args: unknown;
  provenance?: Provenance;
}

export class ActionWorker extends WorkerEntrypoint<ActionEnv> {
  /** Inline for standard and public Actions; a Workflow run for agent Actions (`{ pending: id }`). */
  async call(input: DirectCall): Promise<ActionReport | { pending: true; id: string }> {
    const { runtime, tenant } = runtimeFor(this.env, input.session.tenantId);
    const active = runtime.options.registry.activeGeneration;
    const contract = active?.engine.functions.get(input.path);
    if (contract?.kind === "action" && profileOf(contract) === "agent" && input.session.account.id !== "") {
      const id = await tenantRpc("beginCall", () => tenant.beginCall({ v: 1, session: input.session, path: input.path, args: input.args, provenance: input.provenance }));
      return { pending: true, id };
    }
    return runtime.execute({ session: input.session, path: input.path, args: input.args, provenance: input.provenance });
  }
}
