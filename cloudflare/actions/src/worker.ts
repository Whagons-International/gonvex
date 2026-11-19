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
import type { Claim, Completion, ControlPort, SandboxPort, ScheduleRequest, SchedulerPort, StoragePort, TenantActionPort } from "./ports.js";
import type { Provenance } from "./provenance.js";
import { isTransientRpcError, tenantRpc } from "./rpc.js";
import { runCall, runClaimed, type RunnerDeps, type RunOutcome } from "./runner.js";
import { ActionRuntime, profileOf, type ActionReport } from "./runtime.js";
import type { ActionSession } from "./session.js";
import { publicLimits, type PublicLimitsEnv } from "../../kernel/public/config";

export interface ActionEnv extends PublicLimitsEnv {
  /** Tenant objects, `getByName(tenantId)`; they expose `TenantActions`' methods by RPC (tenant.ts). */
  TENANT: DurableObjectNamespace;
  /** Workflow step timeout for a run (default "24 hours"; agent Actions have no other deadline). */
  ACTION_STEP_TIMEOUT?: string;
  /** First retry delay of the run step (default "5 seconds", doubling), as S4 measured it. */
  ACTION_STEP_RETRY_DELAY?: string;
}

/** What the Worker build provides: the project's generations and the other packages' ports. */
export interface ActionWorkerConfig<Env extends ActionEnv = ActionEnv> {
  registry(env: Env, projectId: string): ModuleRegistry;
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
  startCall(request: { v: 1; session: ActionSession; path: string; args: unknown; provenance?: unknown; owner: string }): Promise<Extract<Claim, { ok: true }>>;
  releaseCall(request: { v: 1; id: string; attempt: number }): Promise<boolean>;
  schedule(request: ScheduleRequest): Promise<string>;
};

function tenantStub(env: ActionEnv, tenantId: string): TenantStub {
  return (env.TENANT as unknown as { getByName(name: string): TenantStub }).getByName(tenantId);
}

function runtimeFor(env: ActionEnv, tenantId: string, projectId: string): { runtime: ActionRuntime; tenant: TenantStub; control: ControlPort | undefined } {
  if (!config) throw new Error("the Action Worker has no module: configureActions was not called");
  const c = config as unknown as ActionWorkerConfig<ActionEnv>;
  const tenant = tenantStub(env, tenantId);
  const control = c.control?.(env);
  const runtime = new ActionRuntime({
    registry: c.registry(env, projectId),
    tenant,
    control,
    scheduler: c.scheduler ? c.scheduler(env, tenantId) : { enqueue: (request) => tenantRpc("schedule", () => tenant.schedule(request)) },
    storage: c.storage?.(env, tenantId),
    sandbox: c.sandbox?.(env, tenantId),
    fetcher: c.fetcher?.(env),
    // K16 (0042): GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS is read in one place; the 60 s rule itself is this runtime's.
    publicActionTimeoutMs: publicLimits(env).actionTimeoutMs,
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
      const { runtime, tenant } = runtimeFor(this.env, job.tenantId, job.projectId);
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
          const completion = await tenantRpc("completeCall", () => tenant.completeCall({ v: 1, id: job.id, attempt: done.attempt, value: done.value, error: done.error, committedRevision: done.committedRevision }));
          return { ...small, value: undefined, completion };
        }
      }
    })) as StepOutcome;
    if (outcome.kind !== "done" || outcome.completion) return outcome;
    const completion = await step.do("apply result", { retries: { limit: 20, delay: "2 seconds", backoff: "exponential" }, timeout: "5 minutes" }, async () =>
      tenantRpc("completeCall", () => tenantStub(this.env, job.tenantId).completeCall({ v: 1, id: job.id, attempt: outcome.attempt, value: outcome.value, error: outcome.error, committedRevision: outcome.committedRevision })));
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
      const { runtime, tenant, control } = runtimeFor(this.env, job.tenantId, job.projectId);
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
  /** Server service-grant access only; decoded client frames cannot set this. */
  allowInternal?: boolean;
  /** K18 provider/share requests await the actual result, including agent Actions. */
  inline?: boolean;
}

export class ActionWorker extends WorkerEntrypoint<ActionEnv> {
  /** Inline for standard and public Actions; a Workflow run for agent Actions (`{ pending: id }`). */
  async call(input: DirectCall): Promise<ActionReport | { pending: true; id: string }> {
    return serveCall(this.env, (work) => this.ctx.waitUntil(work), input);
  }
}

/**
 * `ActionWorker.call`'s body, shared with the per-tenant `ActionHost` (X13g). `waitUntil` keeps the
 * completion that is recorded after the answer (X13b) alive.
 */
export async function serveCall(env: ActionEnv, waitUntil: (work: Promise<unknown>) => void, input: DirectCall): Promise<ActionReport | { pending: true; id: string }> {
  const { runtime, tenant } = runtimeFor(env, input.session.tenantId, input.session.projectId);
  const active = runtime.options.registry.activeGeneration;
  const contract = active?.engine.functions.get(input.path);
  if (contract?.kind === "action" && profileOf(contract) === "agent" && input.session.account.id !== "" && input.inline !== true) {
    const { executionAccess: _oldAccess, ...identity } = input.session;
    const session: ActionSession = input.allowInternal === true ? { ...identity, executionAccess: { allowInternal: true } } : identity;
    const request = { v: 1 as const, session, path: input.path, args: input.args, provenance: input.provenance };
    // X13: run it here, as Rust runs it inline on the socket loop. A Workflow create and its first
    // step cost seconds on the platform, and the socket's next frame (a `tasks.create` after the
    // AI rewrite, say) waits for this Action's answer, in Rust too. The run is still durable: the
    // tenant claims it for this runner, and if this runner dies its lease expires and the sweep
    // ends it (X13b).
    let claim: Awaited<ReturnType<TenantStub["startCall"]>>;
    try {
      claim = await startInline(tenant, { ...request, owner: `inline:${crypto.randomUUID()}` }, active?.engine);
    } catch (error) {
      // Deploy skew: an older tenant object has no `startCall`. Hand the call to a Workflow as before.
      if (!/does not implement the method/i.test(String((error as Error)?.message ?? error))) throw error;
      const id = await tenantRpc("beginCall", () => tenant.beginCall(request));
      return { pending: true, id };
    }
    // The answer goes back in this call, so the socket sends it from the dispatch it is waiting on,
    // as for a standard Action. Through the tenant's reply RPC instead, the gateway would defer the
    // socket's next tenant call to its next fresh event (X02's depth rule): one alarm per Action.
    const { report, completion } = await runInline({ runtime, tenant, renewMs: (config as unknown as ActionWorkerConfig<ActionEnv> | undefined)?.renewMs }, claim);
    // X13b: the completion is persisted after the answer leaves, never on the reply path.
    waitUntil(completion);
    return report ?? { pending: true, id: claim.id };
  }
  return runtime.execute({ session: input.session, path: input.path, args: input.args, provenance: input.provenance, allowInternal: input.allowInternal === true });
}

/** X13g: evaluates the project's active module in this isolate now (the ActionHost's prewarm). */
export function prewarm(env: ActionEnv, tenantId: string, projectId: string): boolean {
  const { runtime } = runtimeFor(env, tenantId, projectId);
  const engine = runtime.options.registry.activeGeneration?.engine;
  if (!engine) return false;
  engine.load();
  return engine.loaded;
}

/**
 * X13b: starts an inline run in the tenant. On a cold isolate the app bundle has not been evaluated yet
 * (Whagons: ~350-450 ms CPU); it is evaluated while the tenant writes the run instead of after it. One
 * turn of the event loop lets the request leave before the CPU-bound evaluation. A bundle that fails to
 * evaluate fails the call in `execute`, as before.
 */
export async function startInline<T>(tenant: { startCall(request: Parameters<TenantStub["startCall"]>[0]): Promise<T> }, request: Parameters<TenantStub["startCall"]>[0], engine: { readonly loaded: boolean; load(): void } | undefined): Promise<T> {
  const started = tenant.startCall(request);
  if (engine && !engine.loaded) {
    void started.catch(() => undefined);
    await new Promise((resolve) => setTimeout(resolve, 0));
    try { engine.load(); } catch { /* reported by execute */ }
  }
  return started;
}

/** Attempts and first delay for applying an inline run's answer (a Workflow's "apply result" step retries too). */
const INLINE_COMPLETE_ATTEMPTS = 6;
const INLINE_COMPLETE_DELAY_MS = 250;

/**
 * X13: one direct agent call run inside `ActionWorker.call`. X13b (lead decision): the answer goes back
 * first and the completion is persisted after it (`completion`, which the caller hands to
 * `ctx.waitUntil`), so the durable completion write is off the reply path, as Rust persists nothing for
 * a socket's Action. The completion is idempotent (`completeCall` answers a replay as a duplicate). If
 * this runner dies between the answer and the completion, the sweep never runs an inline run again: it
 * ends it as dead (`ActionCalls`), so the Action's effects never happen twice. `report` is null when
 * this runner has no answer to send: the run was cancelled or another attempt owns it. A run that throws
 * instead of answering sent nothing, so it is released to a Workflow at once, as before.
 */
export async function runInline(deps: RunnerDeps & { tenant: Pick<TenantStub, "completeCall" | "releaseCall" | "renewCall" | "claimCall" | "call"> }, claim: Extract<Claim, { ok: true }>): Promise<{ report: ActionReport | null; completion: Promise<void> }> {
  let outcome: Awaited<ReturnType<typeof runClaimed>>;
  try {
    outcome = await runClaimed(deps, claim);
  } catch (error) {
    console.error("actions: inline run failed; handing it to a Workflow", { path: claim.path, error: String((error as Error)?.message ?? error).slice(0, 300) });
    await tenantRpc("releaseCall", () => deps.tenant.releaseCall({ v: 1, id: claim.id, attempt: claim.attempt })).catch(() => undefined);
    return { report: null, completion: Promise.resolve() };
  }
  if (outcome.kind !== "done") return { report: null, completion: Promise.resolve() };
  const done = outcome;
  const request = { v: 1 as const, id: claim.id, attempt: done.attempt, value: done.value, error: done.error, committedRevision: done.committedRevision, inlineReply: true };
  const completion = (async () => {
    for (let attempt = 1; ; attempt++) {
      try {
        await tenantRpc("completeCall", () => deps.tenant.completeCall(request));
        return;
      } catch (error) {
        if (attempt >= INLINE_COMPLETE_ATTEMPTS || !isTransientRpcError(error)) {
          console.error("actions: inline completion not recorded; the sweep ends the run when its lease ends", { path: claim.path, error: String((error as Error)?.message ?? error).slice(0, 300) });
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, INLINE_COMPLETE_DELAY_MS * 2 ** (attempt - 1)));
      }
    }
  })();
  const report: ActionReport = done.ok
    ? { ok: true, value: done.value ?? "null", ...(done.committedRevision === undefined ? {} : { committedRevision: done.committedRevision }), stats: done.stats }
    : { ok: false, error: done.error ?? "action failed", class: done.errorClass ?? "rejected", stats: done.stats };
  return { report, completion };
}
