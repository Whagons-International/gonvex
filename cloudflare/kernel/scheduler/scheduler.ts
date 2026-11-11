// The tenant object's scheduler (package K11): crons, `runAfter`/`runAt`,
// coalescing `wakeAt`, the action outbox drain, leases, backoff, hop limits,
// a maximum attempt count and the dead state, all on the object's one alarm.
// Reference: RT/scheduler.rs, RT/scheduled_lane.rs, RT/execution.rs (outbox
// drain), ST/lib.rs (outbox SQL); CONTRACT E.b, E.c.1, E.timers; IB2 (0001).
import type { AlarmBudget, AlarmSource, TenantAlarm } from "./alarm.ts";
import {
  claimDueJob,
  deletePendingCronRuns,
  endJob,
  insertCronRun,
  insertOneShotJob,
  insertOneShotJobs,
  oldestCompleted,
  pruneCompletedJobs,
  renewJobLease,
  type ClaimedJob,
  type OneShotJob,
} from "./jobs.ts";
import {
  claimAction,
  completeAction,
  deleteOutboxRows,
  dueScheduleRows,
  killActions,
  renewAction,
  retryAction,
  retryOutboxRows,
  SCHEDULE_OUTBOX_PATH,
  type OutboxRow,
} from "./outbox.ts";
import type { ActionHandoff, ActiveModule, Actor, Claim, DueWhenEvaluator, DurableRun, HandoffOutcome, ReducerRunner, RunOutcome, SchedulerEvent } from "./ports.ts";
import {
  directProvenance,
  durableHop,
  MAX_DURABLE_HOPS,
  parseProvenance,
  SYSTEM_ACCOUNT_ID,
  SYSTEM_SESSION,
  type Provenance,
} from "./provenance.ts";
import type { CronSpec } from "./specs.ts";
import { installScheduler, SchedulerSql, type SchedulerStorage } from "./sql.ts";
import { cronJobId, CronSpecError, DEFAULT_STAGGER, nextCronRun, nextRun, retryDelayMs, rustDebugString, staggerKey, wakeKey, type Stagger } from "./timing.ts";

/** Lease of a claimed job (RT/scheduler.rs CLAIM_DUE_SQL: 120 s). */
export const JOB_LEASE_MS = 120_000;
/** Lease of a claimed outbox row (ST/lib.rs: 300 s). */
export const OUTBOX_LEASE_MS = 300_000;
/** IB2 default: an outbox Action dies after this many attempts (0001). */
export const MAX_ATTEMPTS = 8;
/** Completed jobs are kept this long (RT/scheduler.rs COMPLETED_JOB_RETENTION). */
export const COMPLETED_RETENTION_MS = 7 * 86_400_000;
/** Retention runs at most this often (PRUNE_INTERVAL), in batches (PRUNE_BATCH, PRUNE_MAX_BATCHES). */
export const PRUNE_INTERVAL_MS = 600_000;
export const PRUNE_BATCH = 5_000;
export const PRUNE_MAX_BATCHES = 20;

export interface SchedulerOptions {
  storage: SchedulerStorage;
  /** Project and tenant ids: the stagger key and cron job ids hash them, as in Rust. */
  project: string;
  tenant: string;
  module: ActiveModule;
  reducers: ReducerRunner;
  actions: ActionHandoff;
  dueWhen?: DueWhenEvaluator;
  clock?: () => number;
  stagger?: Stagger;
  maxDurableHops?: number;
  /** IB2: outbox attempts before `dead`; also how many lost leases a scheduled job survives. */
  maxAttempts?: number;
  jobLeaseMs?: number;
  outboxLeaseMs?: number;
  /** Claim token prefix (`{owner}:{claim_sequence}`). */
  owner?: string;
  completedRetentionMs?: number;
  log?: (event: SchedulerEvent) => void;
}

/** A `runAfter`/`runAt`/`wakeAt` request. */
export interface ScheduleRequest {
  function: string;
  args: unknown;
  runAtUnixMs: number;
  /** Stored provenance of the caller (InvocationProvenance JSON). */
  provenance: unknown;
  /** `wakeAt` */
  coalesce?: boolean;
  /** Reducers pass their deterministic `job_<intent uuid>`; otherwise a random id is made. */
  jobId?: string;
}

export interface DeadItem {
  id: string;
  /** `job` (gonvex_scheduled_jobs) or `outbox` (_gonvex_action_dead). */
  source: "job" | "outbox";
  function: string;
  args: unknown;
  attempts: number;
  lastError: string | null;
  deadAt: number;
}

export interface SchedulerStats {
  jobRuns: number;
  jobsCompleted: number;
  jobsDead: number;
  jobsSkipped: number;
  actionsDelivered: number;
  actionsRetried: number;
  actionsDead: number;
  promoted: number;
}

/** The text a run fails with when the active generation no longer has its function (RT/scheduler.rs execute_job). */
export const NO_LONGER_REGISTERED = "scheduled function is no longer registered";

export class Scheduler {
  readonly sql: SchedulerSql;
  readonly stats: SchedulerStats = { jobRuns: 0, jobsCompleted: 0, jobsDead: 0, jobsSkipped: 0, actionsDelivered: 0, actionsRetried: 0, actionsDead: 0, promoted: 0 };
  private readonly clock: () => number;
  private readonly stagger: Stagger;
  private readonly maxHops: number;
  private readonly maxAttempts: number;
  private readonly jobLeaseMs: number;
  private readonly outboxLeaseMs: number;
  private readonly owner: string;
  private readonly retentionMs: number;
  private readonly log: (event: SchedulerEvent) => void;
  private alarm: TenantAlarm | undefined;
  private installed = false;
  private lastPrune = Number.NEGATIVE_INFINITY;

  constructor(private readonly options: SchedulerOptions) {
    this.sql = new SchedulerSql(options.storage);
    this.clock = options.clock ?? (() => Date.now());
    this.stagger = options.stagger ?? DEFAULT_STAGGER;
    this.maxHops = options.maxDurableHops ?? MAX_DURABLE_HOPS;
    this.maxAttempts = options.maxAttempts ?? MAX_ATTEMPTS;
    this.jobLeaseMs = options.jobLeaseMs ?? JOB_LEASE_MS;
    this.outboxLeaseMs = options.outboxLeaseMs ?? OUTBOX_LEASE_MS;
    this.owner = options.owner ?? "tenant-object";
    this.retentionMs = options.completedRetentionMs ?? COMPLETED_RETENTION_MS;
    this.log = options.log ?? (() => {});
  }

  /** Creates the tables once per object instance (idempotent). */
  install(): void {
    if (this.installed) return;
    installScheduler(this.options.storage);
    this.installed = true;
  }

  /** Registers the scheduler's sources on the object's alarm. */
  attach(alarm: TenantAlarm): void {
    this.install();
    this.alarm = alarm;
    for (const source of this.sources()) alarm.register(source);
  }

  /** Re-arms the alarm after work was queued (a commit that wrote the outbox, an enqueue, a sync, a completion). */
  async notify(): Promise<void> {
    await this.alarm?.arm();
  }

  sources(): AlarmSource[] {
    return [
      { name: "scheduler.outbox", nextAt: (now) => this.outboxNextAt(now), run: (now, budget) => this.drainOutbox(now, budget) },
      { name: "scheduler.jobs", nextAt: (now) => this.jobsNextAt(now), run: (now, budget) => this.runDueJobs(now, budget) },
      { name: "scheduler.retention", nextAt: () => this.retentionNextAt(), run: (now) => this.pruneCompleted(now) },
    ];
  }

  // ---- entry points ----------------------------------------------------------------

  /**
   * `ctx.scheduler.runAfter/runAt/wakeAt` from an Action (E.b.1: Action Worker
   * → tenant object RPC): a direct insert, not transactional with anything.
   * Returns the id of the job that will run. Throws Rust's text when the
   * function is not a registered Reducer or Action.
   */
  async enqueue(request: ScheduleRequest): Promise<string> {
    this.install();
    const fn = request.function.trim();
    this.requireSchedulable(fn);
    const now = this.clock();
    const id = this.sql.transaction(() => insertOneShotJob(this.sql, this.oneShot(request, fn), now));
    await this.notify();
    return id;
  }

  /**
   * The same insert as a statement for the caller's own transaction (E.b.1:
   * "promotion becomes a direct insert in the Reducer's transaction"). K07
   * runs it with K01's `session.run(sql, params)` inside the Reducer's
   * storage transaction, then calls `notify()` after the commit. The table
   * must exist (`install`).
   */
  scheduleStatement(request: ScheduleRequest & { jobId: string }): { sql: string; params: unknown[] } {
    const fn = request.function.trim();
    this.requireSchedulable(fn);
    return oneShotStatement(this.oneShot(request, fn), this.clock());
  }

  /**
   * sync_project for this tenant (on deploy, on provisioning, after an import):
   * every pending cron run is replaced by the next run of each cron in
   * `specs`. Project-scope crons need the project to have exactly one tenant
   * route; `tenantRoutes` is the project's route count (K19 knows it).
   * Throws Rust's text for an invalid expression or a project-scope cron on a
   * project with another number of routes; nothing changes then.
   */
  async syncCrons(specs: readonly CronSpec[] = this.options.module.crons(), options: { tenantRoutes?: number } = {}): Promise<number> {
    this.install();
    const routes = options.tenantRoutes ?? 1;
    if (specs.some((spec) => spec.scope === "project") && routes !== 1) {
      throw new CronSpecError(`project-scoped crons require exactly one project database route; project ${rustDebugString(this.options.project)} has ${routes} tenant routes`);
    }
    const now = this.clock();
    const runs = specs.flatMap((spec) => {
      if (spec.scope !== "tenant" && spec.scope !== "project") return [];
      const next = nextRun(spec, now, staggerKey(this.options.project, this.options.tenant, spec.name), this.stagger);
      if (next === null) return [];
      return [{ id: cronJobId(this.options.project, this.options.tenant, spec.name, next), function: spec.function, args: spec.args ?? null, cronName: spec.name, runAt: next, provenance: null }];
    });
    this.sql.transaction(() => {
      deletePendingCronRuns(this.sql);
      for (const run of runs) insertCronRun(this.sql, run, now);
    });
    await this.notify();
    return runs.length;
  }

  // ---- handed-off Actions report back ---------------------------------------------

  /** Extends a handed-off run's claim (call at least every `leaseMs / 2`). False: the claim is gone, stop the run's side effects. */
  renew(claim: Claim): boolean {
    const now = this.clock();
    return claim.source === "job"
      ? this.sql.transaction(() => renewJobLease(this.sql, claim.id, claim.token, now, claim.leaseMs))
      : this.sql.transaction(() => renewAction(this.sql, claim.id, claim.attempt, now));
  }

  /** Ends a handed-off run. False when its claim was lost (a newer delivery owns the row). */
  async complete(claim: Claim, outcome: RunOutcome): Promise<boolean> {
    const ended = claim.source === "job" ? this.endClaimedJob(claim, outcome) : this.endAction(claim, outcome);
    await this.notify();
    return ended;
  }

  // ---- operator: dead work (CONTRACT D.1 "dead Actions") ---------------------------

  /** Dead jobs and Actions, newest first; `cursor` continues a listing. */
  listDead(options: { limit?: number; cursor?: string | null } = {}): { items: DeadItem[]; cursor: string | null } {
    this.install();
    const limit = Math.min(Math.max(Math.trunc(options.limit ?? 50), 1), 500);
    const [beforeAt, beforeId] = decodeCursor(options.cursor);
    const jobs = this.sql.all(
      `SELECT id,function_path,args,claim_sequence,last_error,completed_at FROM gonvex_scheduled_jobs
       WHERE status='dead' AND (completed_at<?1 OR (completed_at=?1 AND id<?2)) ORDER BY completed_at DESC,id DESC LIMIT CAST(?3 AS INTEGER)`,
      beforeAt, beforeId, limit + 1,
    ).map((row): DeadItem => ({
      id: String(row["id"]), source: "job", function: String(row["function_path"]), args: parseJsonText(row["args"]),
      attempts: Number(row["claim_sequence"]), lastError: row["last_error"] === null ? null : String(row["last_error"]), deadAt: Number(row["completed_at"]),
    }));
    const actions = this.sql.all(
      `SELECT id,action_path,args,attempts,last_error,dead_at FROM _gonvex_action_dead
       WHERE dead_at<?1 OR (dead_at=?1 AND id<?2) ORDER BY dead_at DESC,id DESC LIMIT CAST(?3 AS INTEGER)`,
      beforeAt, beforeId, limit + 1,
    ).map((row): DeadItem => ({
      id: String(row["id"]), source: "outbox", function: String(row["action_path"]), args: parseJsonText(row["args"]),
      attempts: Number(row["attempts"]), lastError: row["last_error"] === null ? null : String(row["last_error"]), deadAt: Number(row["dead_at"]),
    }));
    const merged = [...jobs, ...actions].sort((a, b) => b.deadAt - a.deadAt || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0));
    const items = merged.slice(0, limit);
    const last = items[items.length - 1];
    return { items, cursor: merged.length > limit && last ? `${last.deadAt}:${last.id}` : null };
  }

  /**
   * Makes a dead job or Action pending again with its attempts reset. A dead
   * cron run comes back as a one-shot run of the same function (the cron
   * already has its next run). False when there is no dead row with that id.
   */
  async requeueDead(id: string): Promise<boolean> {
    this.install();
    const now = this.clock();
    const requeued = this.sql.transaction(() => {
      const job = this.sql.one(
        `UPDATE gonvex_scheduled_jobs SET status='pending',cron_name='',claim_token='',claim_sequence=0,lease_until=NULL,
           completed_at=NULL,last_error=NULL,run_at=CAST(?2 AS INTEGER),updated_at=CAST(?2 AS INTEGER)
         WHERE id=?1 AND status='dead' RETURNING id`,
        id, now,
      );
      if (job) return true;
      const action = this.sql.one(
        `INSERT INTO _gonvex_action_outbox (id,action_path,args,actor_user_id,actor_email,provenance,status,attempts,available_at,created_at)
         SELECT id,action_path,args,actor_user_id,actor_email,provenance,'pending',0,CAST(?2 AS INTEGER),created_at FROM _gonvex_action_dead WHERE id=?1
         RETURNING id`,
        id, now,
      );
      if (!action) return false;
      this.sql.run(`DELETE FROM _gonvex_action_dead WHERE id=?1`, id);
      return true;
    });
    if (requeued) await this.notify();
    return requeued;
  }

  // ---- jobs ------------------------------------------------------------------------

  private jobsNextAt(now: number): number | null {
    this.install();
    // NEXT_DUE_SQL's two partial-index probes, plus the earliest lease.
    const row = this.sql.one<{ oneshot: number | null; cron: number | null; lease: number | null }>(
      `SELECT (SELECT run_at FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name='' AND claim_token='' ORDER BY run_at,id LIMIT 1) AS oneshot,
              (SELECT run_at FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name<>'' AND claim_token='' ORDER BY run_at,id LIMIT 1) AS cron,
              (SELECT lease_until FROM gonvex_scheduled_jobs WHERE status='pending' AND claim_token<>'' ORDER BY lease_until LIMIT 1) AS lease`,
    );
    return nextWithClaim(minOf(row?.oneshot ?? null, row?.cron ?? null), row?.lease ?? null, now);
  }

  /** Claims and runs due jobs one at a time (one scheduled run per tenant at a time). */
  private async runDueJobs(_now: number, budget: AlarmBudget): Promise<number> {
    let units = 0;
    while (units < budget.maxUnits) {
      const at = this.clock();
      // A handed-off run still holds its lease: the next job waits for it.
      if (this.sql.one(`SELECT 1 AS x FROM gonvex_scheduled_jobs WHERE status='pending' AND claim_token<>'' AND lease_until>?1 LIMIT 1`, at)) break;
      const job = this.sql.transaction(() => claimDueJob(this.sql, this.owner, at, this.jobLeaseMs));
      if (!job) break;
      units++;
      this.stats.jobRuns++;
      const outcome = await this.executeJob(job);
      if ("accepted" in outcome) {
        this.log({ type: "job", id: job.id, function: job.function, cronName: job.cronName, outcome: "accepted" });
        break;
      }
      this.endClaimedJob({ source: "job", id: job.id, token: job.claimToken, leaseMs: this.jobLeaseMs }, outcome, job);
    }
    return units;
  }

  private async executeJob(job: ClaimedJob): Promise<HandoffOutcome | { ok: true; skipped: true }> {
    // IB2: a job whose claim keeps getting lost (the object reset under it) dies instead of looping.
    if (job.claimSequence > this.maxAttempts) {
      return { ok: false, error: `scheduled run lost its lease ${job.claimSequence - 1} times without finishing` };
    }
    if (job.cronName) {
      const due = await this.dueCheck(job.cronName);
      if (due === false) return { ok: true, skipped: true };
    }
    try {
      const stored = parseProvenance(job.provenance);
      const actor: Actor = stored?.actorAccountId && stored.actorAccountId !== SYSTEM_ACCOUNT_ID
        ? { kind: "account", accountId: stored.actorAccountId, email: "" }
        : { kind: "system" };
      const kind = this.options.module.kindOf(job.function);
      if (kind === undefined) return { ok: false, error: NO_LONGER_REGISTERED };
      let provenance: Provenance;
      let direct = false;
      if (!stored || !stored.rootCommandId.trim()) {
        provenance = directProvenance(actor.kind === "system" ? SYSTEM_SESSION : { accountId: actor.accountId, memberId: "" }, job.id, this.options.module.artifactHash());
        direct = actor.kind !== "system";
      } else {
        provenance = stored;
      }
      provenance.parentCommandId = provenance.commandId;
      provenance.commandId = job.id;
      provenance.channel = "scheduler";
      const hop = durableHop(provenance, this.maxHops);
      if (hop) return { ok: false, error: hop };
      const run: DurableRun = {
        claim: { source: "job", id: job.id, token: job.claimToken, leaseMs: this.jobLeaseMs },
        id: job.id, function: job.function, args: job.args, actor, provenance, direct,
        cronName: job.cronName || null, scheduledFor: job.scheduledFor,
      };
      if (kind === "reducer") return await this.options.reducers.runReducer(run);
      if (kind === "action") return await this.options.actions.startAction(run);
      return { ok: false, error: "scheduled function must be a Reducer or Action" };
    } catch (error) {
      return { ok: false, error: error instanceof Error ? error.message : String(error) };
    }
  }

  /** RT/scheduler.rs due_check: `undefined` when the cron has no `dueWhen` or it failed (the run happens). */
  private async dueCheck(cronName: string): Promise<boolean | undefined> {
    const spec = this.options.module.crons().find((cron) => cron.name === cronName);
    const statement = spec?.dueWhen?.trim();
    if (!statement || !this.options.dueWhen) return undefined;
    try {
      return await this.options.dueWhen.evaluate(statement, this.clock());
    } catch (error) {
      this.log({ type: "dueWhen", cronName, error: error instanceof Error ? error.message : String(error) });
      return undefined;
    }
  }

  /** `finish`: the run ends `completed` (or `dead` on failure, IB2) and a cron gets its next run in the same transaction. */
  private endClaimedJob(claim: Extract<Claim, { source: "job" }>, outcome: RunOutcome & { skipped?: true }, job?: ClaimedJob): boolean {
    const now = this.clock();
    const status = outcome.ok ? "completed" : "dead";
    const error = outcome.ok ? null : outcome.error;
    let event: SchedulerEvent | undefined;
    let row = job;
    const ended = this.sql.transaction(() => {
      const current = (row ??= this.claimedRow(claim));
      if (!current) return false;
      if (!endJob(this.sql, claim.id, claim.token, status, error, now)) return false;
      if (current.cronName) {
        const spec = this.options.module.crons().find((cron) => cron.name === current.cronName);
        if (spec) {
          let next: number | null = null;
          try {
            next = nextCronRun(spec, current.scheduledFor, now, staggerKey(this.options.project, this.options.tenant, spec.name), this.stagger);
          } catch (cronError) {
            event = { type: "cron", cronName: current.cronName, error: cronError instanceof Error ? cronError.message : String(cronError) };
          }
          if (next !== null) {
            insertCronRun(this.sql, {
              id: cronJobId(this.options.project, this.options.tenant, current.cronName, next),
              function: current.function, args: current.args, cronName: current.cronName, runAt: next, provenance: current.provenance,
            }, now);
          }
        }
      }
      return true;
    });
    if (event) this.log(event);
    const name = row?.function ?? "";
    const cronName = row?.cronName ?? "";
    if (!ended) {
      this.log({ type: "job", id: claim.id, function: name, cronName, outcome: "lease-lost" });
      return false;
    }
    if (outcome.ok && outcome.skipped) this.stats.jobsSkipped++;
    else if (outcome.ok) this.stats.jobsCompleted++;
    else this.stats.jobsDead++;
    this.log(outcome.ok
      ? { type: "job", id: claim.id, function: name, cronName, outcome: outcome.skipped ? "skipped" : "completed" }
      : { type: "job", id: claim.id, function: name, cronName, outcome: "dead", error: outcome.error });
    return true;
  }

  private claimedRow(claim: Extract<Claim, { source: "job" }>): ClaimedJob | undefined {
    const row = this.sql.one(
      `SELECT id,function_path,args,run_at,scheduled_for,cron_name,claim_token,claim_sequence,lease_until,provenance FROM gonvex_scheduled_jobs
       WHERE id=?1 AND status='pending' AND claim_token=?2`,
      claim.id, claim.token,
    );
    if (!row) return undefined;
    return {
      id: String(row["id"]), function: String(row["function_path"]), args: parseJsonText(row["args"]), runAt: Number(row["run_at"]),
      scheduledFor: Number(row["scheduled_for"]), cronName: String(row["cron_name"]), claimToken: String(row["claim_token"]),
      claimSequence: Number(row["claim_sequence"]), leaseUntil: Number(row["lease_until"]), provenance: parseJsonText(row["provenance"]),
    };
  }

  // ---- the action outbox -------------------------------------------------------------

  private outboxNextAt(now: number): number | null {
    this.install();
    const row = this.sql.one<{ schedules: number | null; actions: number | null; lease: number | null }>(
      `SELECT (SELECT available_at FROM _gonvex_action_outbox WHERE status='pending' AND action_path=?1 ORDER BY action_path,available_at LIMIT 1) AS schedules,
              (SELECT available_at FROM _gonvex_action_outbox WHERE status='pending' AND action_path<>?1 ORDER BY available_at,created_at LIMIT 1) AS actions,
              (SELECT locked_at+CAST(?2 AS INTEGER)+1 FROM _gonvex_action_outbox WHERE status='processing' ORDER BY locked_at LIMIT 1) AS lease`,
      SCHEDULE_OUTBOX_PATH, this.outboxLeaseMs,
    );
    // A processing row becomes claimable once `locked_at < now - lease` (ST/lib.rs), one ms after `locked_at + lease`.
    const actions = nextWithClaim(row?.actions ?? null, row?.lease ?? null, now);
    return minOf(row?.schedules ?? null, actions);
  }

  private async drainOutbox(now: number, budget: AlarmBudget): Promise<number> {
    let units = this.promoteSchedules(now) > 0 ? 1 : 0;
    while (units < budget.maxUnits) {
      const at = this.clock();
      const inFlight = this.sql.one(
        `SELECT 1 AS x FROM _gonvex_action_outbox WHERE status='processing' AND locked_at>=?1-CAST(?2 AS INTEGER) AND action_path<>?3 LIMIT 1`,
        at, this.outboxLeaseMs, SCHEDULE_OUTBOX_PATH,
      );
      if (inFlight) break;
      const row = this.sql.transaction(() => claimAction(this.sql, at, this.outboxLeaseMs));
      if (!row) break;
      units++;
      const claim: Extract<Claim, { source: "outbox" }> = { source: "outbox", id: row.id, attempt: row.attempts, leaseMs: this.outboxLeaseMs };
      const prepared = this.prepareAction(row, claim);
      if ("error" in prepared) {
        this.kill(row, claim.attempt, prepared.error);
        continue;
      }
      let outcome: HandoffOutcome;
      try {
        outcome = await this.options.actions.startAction(prepared.run);
      } catch (error) {
        outcome = { ok: false, error: error instanceof Error ? error.message : String(error) };
      }
      if ("accepted" in outcome) {
        this.log({ type: "action", id: row.id, function: row.path, attempt: row.attempts, outcome: "accepted" });
        break;
      }
      this.endAction(claim, outcome, row.path);
    }
    return units;
  }

  /** One drain turn's promotions (RT/execution.rs promote_due_schedules), in one transaction. Returns how many were due. */
  private promoteSchedules(now: number): number {
    return this.sql.transaction(() => {
      const rows = dueScheduleRows(this.sql, now, this.outboxLeaseMs);
      if (rows.length === 0) return 0;
      const jobs: OneShotJob[] = [];
      const promoted: string[] = [];
      const failed: { row: OutboxRow; error: string }[] = [];
      for (const row of rows) {
        const result = this.promotion(row);
        if ("error" in result) failed.push({ row, error: result.error });
        else {
          jobs.push(result.job);
          promoted.push(row.id);
        }
      }
      insertOneShotJobs(this.sql, jobs, now);
      deleteOutboxRows(this.sql, promoted);
      this.stats.promoted += promoted.length;
      const retry: { id: string; attempts: number; delayMs: number; error: string }[] = [];
      const dead: { id: string; attempt: number | null; error: string; attempts: number }[] = [];
      for (const { row, error } of failed) {
        const attempts = row.attempts + 1;
        if (attempts >= this.maxAttempts) dead.push({ id: row.id, attempt: null, error, attempts });
        else retry.push({ id: row.id, attempts, delayMs: retryDelayMs(attempts), error });
        this.log({ type: "promotion", id: row.id, outcome: attempts >= this.maxAttempts ? "dead" : "retry", error });
      }
      retryOutboxRows(this.sql, retry, now);
      this.stats.actionsDead += killActions(this.sql, dead, now);
      return rows.length;
    });
  }

  /** RT/execution.rs promote_scheduled_outbox + Scheduler::enqueue checks. */
  private promotion(row: OutboxRow): { job: OneShotJob } | { error: string } {
    const payload = row.args;
    if (typeof payload !== "object" || payload === null || Array.isArray(payload)) return { error: "scheduled outbox payload must be an object" };
    const p = payload as Record<string, unknown>;
    const jobId = p["jobId"];
    if (typeof jobId !== "string" || !jobId.startsWith("job_") || jobId.length > 128) return { error: "scheduled outbox payload has an invalid jobId" };
    const fn = p["function"];
    if (typeof fn !== "string" || !fn.trim()) return { error: "scheduled outbox payload has no function" };
    const runAt = p["runAtUnixMs"];
    if (typeof runAt !== "number" || !Number.isSafeInteger(runAt) || Math.abs(runAt) > 8.64e15) return { error: "scheduled outbox payload has an invalid runAtUnixMs" };
    const kind = this.options.module.kindOf(fn.trim());
    if (kind === undefined) return { error: `scheduled function ${rustDebugString(fn)} is not registered` };
    if (kind !== "reducer" && kind !== "action") return { error: "only Reducers and Actions can be scheduled" };
    const provenance = parseProvenance(row.provenance) ?? directProvenance(SYSTEM_SESSION, row.id, "");
    return { job: { id: jobId, function: fn.trim(), args: p["args"] ?? null, runAt, provenance, coalesce: p["coalesce"] === true } };
  }

  /** The run of a claimed outbox Action (RT/execution.rs deliver_outbox_action), or why it dies without running. */
  private prepareAction(row: OutboxRow, claim: Extract<Claim, { source: "outbox" }>): { run: DurableRun } | { error: string } {
    // IB2: the claim after the last allowed attempt means that attempt lost its lease.
    if (row.attempts > this.maxAttempts) return { error: `Action delivery lost its lease after ${this.maxAttempts} attempts` };
    const stored = parseProvenance(row.provenance);
    if (stored) {
      const hop = durableHop(stored, this.maxHops);
      if (hop) return { error: hop };
    }
    const actor: Actor = row.actorAccountId.startsWith("_gonvex_") ? { kind: "system" } : { kind: "account", accountId: row.actorAccountId, email: row.actorEmail };
    let provenance: Provenance;
    let direct = false;
    if (stored) provenance = stored;
    else {
      provenance = directProvenance(actor.kind === "system" ? SYSTEM_SESSION : { accountId: actor.accountId, memberId: "" }, row.id, "");
      provenance.durableHops = 1;
      direct = actor.kind !== "system";
    }
    provenance.parentCommandId = provenance.commandId;
    provenance.commandId = `outbox-${row.id}`;
    provenance.channel = "system";
    return { run: { claim, id: row.id, function: row.path, args: row.args, actor, provenance, direct, cronName: null, scheduledFor: null } };
  }

  /** Success deletes the row; failure retries with backoff until the attempt cap, then the row dies (IB2). */
  private endAction(claim: Extract<Claim, { source: "outbox" }>, outcome: RunOutcome, path = ""): boolean {
    const now = this.clock();
    if (outcome.ok) {
      const done = this.sql.transaction(() => completeAction(this.sql, claim.id, claim.attempt));
      if (done) this.stats.actionsDelivered++;
      this.log({ type: "action", id: claim.id, function: path, attempt: claim.attempt, outcome: done ? "completed" : "lease-lost" });
      return done;
    }
    if (claim.attempt >= this.maxAttempts) {
      const moved = this.sql.transaction(() => killActions(this.sql, [{ id: claim.id, attempt: claim.attempt, error: outcome.error }], now)) === 1;
      if (moved) this.stats.actionsDead++;
      this.log({ type: "action", id: claim.id, function: path, attempt: claim.attempt, outcome: moved ? "dead" : "lease-lost", error: outcome.error });
      return moved;
    }
    const retried = this.sql.transaction(() => retryAction(this.sql, claim.id, claim.attempt, retryDelayMs(claim.attempt), outcome.error, now));
    if (retried) this.stats.actionsRetried++;
    this.log({ type: "action", id: claim.id, function: path, attempt: claim.attempt, outcome: retried ? "retry" : "lease-lost", error: outcome.error });
    return retried;
  }

  private kill(row: OutboxRow, attempt: number, error: string): void {
    const moved = this.sql.transaction(() => killActions(this.sql, [{ id: row.id, attempt, error }], this.clock()));
    this.stats.actionsDead += moved;
    this.log({ type: "action", id: row.id, function: row.path, attempt, outcome: "dead", error });
  }

  // ---- retention -------------------------------------------------------------------

  private retentionNextAt(): number | null {
    this.install();
    const oldest = oldestCompleted(this.sql);
    if (oldest === null) return null;
    return Math.max(oldest + this.retentionMs, this.lastPrune + PRUNE_INTERVAL_MS);
  }

  /** prune_completed: batches of 5,000, at most 20 per pass, one transaction each. */
  private pruneCompleted(now: number): number {
    this.lastPrune = now;
    const before = now - this.retentionMs;
    let batches = 0;
    while (batches < PRUNE_MAX_BATCHES) {
      const deleted = this.sql.transaction(() => pruneCompletedJobs(this.sql, before, PRUNE_BATCH));
      batches++;
      if (deleted < PRUNE_BATCH) break;
    }
    return batches;
  }

  // ---- helpers -----------------------------------------------------------------------

  private requireSchedulable(fn: string): void {
    const kind = this.options.module.kindOf(fn);
    if (kind === undefined) throw new Error(`scheduled function ${rustDebugString(fn)} is not registered`);
    if (kind !== "reducer" && kind !== "action") throw new Error("only Reducers and Actions can be scheduled");
  }

  private oneShot(request: ScheduleRequest, fn: string): OneShotJob {
    return {
      id: request.jobId ?? `job_${crypto.randomUUID()}`,
      function: fn,
      args: request.args ?? null,
      runAt: request.runAtUnixMs,
      provenance: request.provenance ?? null,
      coalesce: request.coalesce === true,
    };
  }
}

/** The insert of `insertOneShotJob` as one statement and its parameters, for a caller's own transaction. */
export function oneShotStatement(job: OneShotJob, now: number): { sql: string; params: unknown[] } {
  return {
    sql: `INSERT INTO gonvex_scheduled_jobs (id,function_path,args,run_at,scheduled_for,provenance,wake_key,created_at,updated_at)
      SELECT ?1,?2,?3,?4,?4,?5,?6,?7,?7
      WHERE NOT EXISTS (SELECT 1 FROM gonvex_scheduled_jobs WHERE id=?1)
      ON CONFLICT (wake_key) WHERE status='pending' AND claim_token='' AND wake_key<>''
      DO UPDATE SET run_at=min(gonvex_scheduled_jobs.run_at,excluded.run_at),
                    scheduled_for=min(gonvex_scheduled_jobs.scheduled_for,excluded.scheduled_for),
                    updated_at=excluded.updated_at
      RETURNING id`,
    params: [job.id, job.function, JSON.stringify(job.args ?? null), job.runAt, JSON.stringify(job.provenance ?? null), job.coalesce ? wakeKeyOf(job) : "", now],
  };
}

const wakeKeyOf = (job: OneShotJob) => wakeKey(job.function, job.args);

/**
 * The next time a claim-guarded queue needs the alarm: when a claim still
 * holds its lease, nothing else can start before it ends (its completion
 * re-arms the alarm) or its lease runs out; otherwise the earliest of the
 * unclaimed work and expired leases.
 */
function nextWithClaim(unclaimed: number | null, lease: number | null, now: number): number | null {
  if (lease !== null && lease > now) return lease;
  return minOf(unclaimed, lease);
}

function minOf(a: number | null, b: number | null): number | null {
  if (a === null) return b;
  if (b === null) return a;
  return Math.min(a, b);
}

function parseJsonText(value: SqlStorageValue | undefined): unknown {
  if (value === null || value === undefined) return null;
  try {
    return JSON.parse(String(value));
  } catch {
    return null;
  }
}

function decodeCursor(cursor: string | null | undefined): [number, string] {
  if (!cursor) return [Number.MAX_SAFE_INTEGER, "\u{10ffff}"];
  const colon = cursor.indexOf(":");
  const at = Number(cursor.slice(0, colon));
  if (colon < 0 || !Number.isSafeInteger(at)) return [Number.MAX_SAFE_INTEGER, "\u{10ffff}"];
  return [at, cursor.slice(colon + 1)];
}
