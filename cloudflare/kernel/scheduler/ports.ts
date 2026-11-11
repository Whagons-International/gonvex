// What the scheduler needs from the rest of the tenant object, as interfaces:
// the active generation (K04/K19), the Reducer path (K07), the Action runtime
// (K12) and the `dueWhen` reader (K01). Nothing here is Cloudflare-specific,
// so a test or a simulated clock can stand in for any of them.
import type { Provenance } from "./provenance.ts";
import type { CronSpec } from "./specs.ts";

export type FunctionKind = "query" | "reducer" | "action";

/** The active module generation, as far as scheduling needs it. */
export interface ActiveModule {
  /** Kind of a registered function, or undefined (RT `module.functions.get`). */
  kindOf(path: string): FunctionKind | undefined;
  /** The generation's `module.crons` (parsed with `parseCronSpecs`). */
  crons(): readonly CronSpec[];
  /** The generation's artifact hash (`provenance.artifactHash` of direct runs). */
  artifactHash(): string;
}

/** Who a durable run acts as. `system` is Rust's system session (`_gonvex_system`). */
export type Actor =
  | { readonly kind: "system" }
  /** Re-resolve this account's Member before running (`tenant_session_for_account`); a failure fails the run. */
  | { readonly kind: "account"; readonly accountId: string; readonly email: string };

/**
 * The claim a run holds. A handed-off Action renews it at least every
 * `leaseMs / 2` (S4: persisted leases) and completes with it; a zombie with an
 * old claim cannot renew or complete a newer one.
 */
export type Claim =
  | { readonly source: "job"; readonly id: string; readonly token: string; readonly leaseMs: number }
  | { readonly source: "outbox"; readonly id: string; readonly attempt: number; readonly leaseMs: number };

/** One scheduled job or outbox Action to run now. */
export interface DurableRun {
  readonly claim: Claim;
  readonly function: string;
  readonly args: unknown;
  readonly actor: Actor;
  /** Provenance with the durable hop applied (channel `scheduler` for jobs, `system` for outbox Actions). */
  readonly provenance: Provenance;
  /**
   * The stored provenance had no root, so Rust builds `direct_provenance` from
   * the resolved session: call `completeDirect({ provenance, direct }, session)`.
   * Always false for the system actor (the scheduler completes it).
   */
  readonly direct: boolean;
  /** Cron runs: the cron and its due time. */
  readonly cronName: string | null;
  readonly scheduledFor: number | null;
  /** The job's id (`ctx.invocation.commandId` of a scheduled run) or the outbox row's id. */
  readonly id: string;
}

export type RunOutcome = { readonly ok: true } | { readonly ok: false; readonly error: string };

/** An Action handed to a Workflow step: it reports back with `Scheduler.complete`. */
export type HandoffOutcome = RunOutcome | { readonly accepted: true };

/** K07: runs one scheduled Reducer in the tenant object (lane "scheduled", the run's command id and provenance). */
export interface ReducerRunner {
  runReducer(run: DurableRun): Promise<RunOutcome>;
}

/**
 * K12: starts one Action. A short Action may finish before this resolves
 * (`{ok}`); a long one is accepted, continues in a Workflow step, renews its
 * claim and completes through the tenant object (`Scheduler.renew`,
 * `Scheduler.complete`).
 */
export interface ActionHandoff {
  startAction(run: DurableRun): Promise<HandoffOutcome>;
}

/** A cron's `dueWhen`: true when the first column of the first row is truthy. Throwing fails open (the run happens). */
export interface DueWhenEvaluator {
  evaluate(statement: string, nowMs: number): Promise<boolean>;
}

export type SchedulerEvent =
  | { type: "job"; id: string; function: string; cronName: string; outcome: "completed" | "dead" | "skipped" | "accepted" | "lease-lost"; error?: string }
  | { type: "action"; id: string; function: string; attempt: number; outcome: "completed" | "retry" | "dead" | "accepted" | "lease-lost"; error?: string }
  | { type: "promotion"; id: string; outcome: "promoted" | "retry" | "dead"; error?: string }
  | { type: "dueWhen"; cronName: string; error: string }
  | { type: "cron"; cronName: string; error: string };
