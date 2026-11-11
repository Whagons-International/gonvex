// SQL over `gonvex_scheduled_jobs` (RT/scheduler.rs insert_one_shot_job,
// insert_cron_job, CLAIM_DUE_SQL, NEXT_DUE_SQL, finish, renew_lease,
// prune_completed_jobs, sync_project; CONTRACT E.b). Every function is
// synchronous; the caller wraps mutations in `SchedulerSql.transaction`.
import type { SchedulerSql } from "./sql.ts";
import { wakeKey } from "./timing.ts";

/** One `runAfter` / `runAt` / `wakeAt` job to insert. */
export interface OneShotJob {
  id: string;
  function: string;
  args: unknown;
  runAt: number;
  /** Stored provenance (InvocationProvenance JSON). */
  provenance: unknown;
  /** `wakeAt`: merge with the unclaimed wake of the same function and args (earlier time wins). */
  coalesce: boolean;
}

export interface ClaimedJob {
  id: string;
  function: string;
  args: unknown;
  runAt: number;
  scheduledFor: number;
  cronName: string;
  claimToken: string;
  claimSequence: number;
  leaseUntil: number;
  provenance: unknown;
}

const parseJson = (text: SqlStorageValue | undefined): unknown => {
  if (text === null || text === undefined) return null;
  try {
    return JSON.parse(String(text));
  } catch {
    return null;
  }
};

const json = (value: unknown) => JSON.stringify(value ?? null);

/**
 * Inserts one-shot jobs, or merges a `wakeAt` into the pending unclaimed wake
 * of the same function and args (earlier `run_at` and `scheduled_for` win).
 * A job whose id exists already (in any state) is not inserted again, so
 * replaying a promotion is a no-op. One statement for the whole batch.
 */
export function insertOneShotJobs(db: SchedulerSql, jobs: readonly OneShotJob[], now: number): void {
  if (jobs.length === 0) return;
  const seen = new Set<string>();
  const rows = jobs.filter((job) => !seen.has(job.id) && seen.add(job.id)).map((job) => ({
    id: job.id, f: job.function, a: json(job.args), r: job.runAt, p: json(job.provenance),
    w: job.coalesce ? wakeKey(job.function, job.args) : "",
  }));
  db.run(
    `INSERT INTO gonvex_scheduled_jobs (id,function_path,args,run_at,scheduled_for,provenance,wake_key,created_at,updated_at)
     SELECT json_extract(j.value,'$.id'),json_extract(j.value,'$.f'),json_extract(j.value,'$.a'),
            CAST(json_extract(j.value,'$.r') AS INTEGER),CAST(json_extract(j.value,'$.r') AS INTEGER),
            json_extract(j.value,'$.p'),json_extract(j.value,'$.w'),CAST(?2 AS INTEGER),CAST(?2 AS INTEGER)
     FROM json_each(?1) AS j
     WHERE NOT EXISTS (SELECT 1 FROM gonvex_scheduled_jobs g WHERE g.id=json_extract(j.value,'$.id'))
     ORDER BY j.key
     ON CONFLICT (wake_key) WHERE status='pending' AND claim_token='' AND wake_key<>''
     DO UPDATE SET run_at=min(gonvex_scheduled_jobs.run_at,excluded.run_at),
                   scheduled_for=min(gonvex_scheduled_jobs.scheduled_for,excluded.scheduled_for),
                   updated_at=excluded.updated_at`,
    JSON.stringify(rows), now,
  );
}

/** `insert_one_shot_job` for one job: returns the id of the job that will run (the surviving wake when merged). */
export function insertOneShotJob(db: SchedulerSql, job: OneShotJob, now: number): string {
  const key = job.coalesce ? wakeKey(job.function, job.args) : "";
  const row = db.one<{ id: string }>(
    `INSERT INTO gonvex_scheduled_jobs (id,function_path,args,run_at,scheduled_for,provenance,wake_key,created_at,updated_at)
     SELECT ?1,?2,?3,CAST(?4 AS INTEGER),CAST(?4 AS INTEGER),?5,?6,CAST(?7 AS INTEGER),CAST(?7 AS INTEGER)
     WHERE NOT EXISTS (SELECT 1 FROM gonvex_scheduled_jobs WHERE id=?1)
     ON CONFLICT (wake_key) WHERE status='pending' AND claim_token='' AND wake_key<>''
     DO UPDATE SET run_at=min(gonvex_scheduled_jobs.run_at,excluded.run_at),
                   scheduled_for=min(gonvex_scheduled_jobs.scheduled_for,excluded.scheduled_for),
                   updated_at=excluded.updated_at
     RETURNING id`,
    job.id, job.function, json(job.args), job.runAt, json(job.provenance), key, now,
  );
  return row?.id ?? job.id;
}

export interface CronRun {
  id: string;
  function: string;
  args: unknown;
  cronName: string;
  runAt: number;
  provenance: unknown;
}

/** `insert_cron_job`: a pending run already waiting for the same cron wins (unique pending-cron index). */
export function insertCronRun(db: SchedulerSql, run: CronRun, now: number): void {
  db.run(
    `INSERT INTO gonvex_scheduled_jobs (id,function_path,args,run_at,scheduled_for,cron_name,provenance,created_at,updated_at)
     VALUES (?1,?2,?3,CAST(?4 AS INTEGER),CAST(?4 AS INTEGER),?5,?6,CAST(?7 AS INTEGER),CAST(?7 AS INTEGER)) ON CONFLICT DO NOTHING`,
    run.id, run.function, json(run.args), run.runAt, run.cronName, json(run.provenance), now,
  );
}

/**
 * CLAIM_DUE_SQL for one job: the next due pending job (one-shot before cron,
 * then `run_at`, `id`) that is unclaimed or whose lease ran out.
 */
export function claimDueJob(db: SchedulerSql, owner: string, now: number, leaseMs: number): ClaimedJob | undefined {
  const row = db.one(
    `UPDATE gonvex_scheduled_jobs SET claim_sequence=claim_sequence+1,
       claim_token=?1 || ':' || CAST(claim_sequence+1 AS TEXT),
       lease_until=CAST(?2 AS INTEGER)+CAST(?3 AS INTEGER), updated_at=CAST(?2 AS INTEGER)
     WHERE id IN (
       SELECT id FROM gonvex_scheduled_jobs
       WHERE status='pending' AND run_at<=?2
         AND (claim_token='' OR lease_until IS NULL OR lease_until<=?2)
       ORDER BY (cron_name<>'') ASC,run_at,id LIMIT 1)
     RETURNING id,function_path,args,run_at,scheduled_for,cron_name,claim_token,claim_sequence,lease_until,provenance`,
    owner, now, leaseMs,
  );
  if (!row) return undefined;
  return {
    id: String(row["id"]), function: String(row["function_path"]), args: parseJson(row["args"]),
    runAt: Number(row["run_at"]), scheduledFor: Number(row["scheduled_for"]), cronName: String(row["cron_name"]),
    claimToken: String(row["claim_token"]), claimSequence: Number(row["claim_sequence"]),
    leaseUntil: Number(row["lease_until"]), provenance: parseJson(row["provenance"]),
  };
}

/** A claimed job whose lease is still running: one scheduled run per tenant at a time. */
export function jobInFlight(db: SchedulerSql, now: number): boolean {
  return db.one(
    `SELECT 1 AS x FROM gonvex_scheduled_jobs WHERE status='pending' AND claim_token<>'' AND lease_until>?1 LIMIT 1`,
    now,
  ) !== undefined;
}

/** Renews a claim (`renew_lease`); false when the claim is no longer this one's. */
export function renewJobLease(db: SchedulerSql, id: string, claimToken: string, now: number, leaseMs: number): boolean {
  return db.one(
    `UPDATE gonvex_scheduled_jobs SET lease_until=CAST(?3 AS INTEGER)+CAST(?4 AS INTEGER),updated_at=CAST(?3 AS INTEGER)
     WHERE id=?1 AND status='pending' AND claim_token=?2 RETURNING id`,
    id, claimToken, now, leaseMs,
  ) !== undefined;
}

/**
 * Ends a claimed run (`finish`): `completed`, or `dead` with its error
 * (IB2). False when the lease was lost (the row changed hands or a sync
 * removed it); nothing is written then.
 */
export function endJob(db: SchedulerSql, id: string, claimToken: string, status: "completed" | "dead", error: string | null, now: number): boolean {
  return db.one(
    `UPDATE gonvex_scheduled_jobs SET status=?3, completed_at=CAST(?4 AS INTEGER), claim_token='', lease_until=NULL,
       updated_at=CAST(?4 AS INTEGER), last_error=?5
     WHERE id=?1 AND status='pending' AND claim_token=?2 RETURNING id`,
    id, claimToken, status, now, error,
  ) !== undefined;
}

/**
 * When the jobs table next needs the alarm: the earliest unclaimed pending
 * `run_at` (NEXT_DUE_SQL, one probe per partial index) or the earliest lease
 * expiry of a claimed job.
 */
export function jobsNextDue(db: SchedulerSql): number | null {
  const row = db.one<{ next: number | null }>(
    `SELECT min(t) AS next FROM (
       SELECT (SELECT run_at FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name='' AND claim_token='' ORDER BY run_at,id LIMIT 1) AS t
       UNION ALL
       SELECT (SELECT run_at FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name<>'' AND claim_token='' ORDER BY run_at,id LIMIT 1)
       UNION ALL
       SELECT (SELECT lease_until FROM gonvex_scheduled_jobs WHERE status='pending' AND claim_token<>'' ORDER BY lease_until LIMIT 1))`,
  );
  return row?.next ?? null;
}

/** Oldest completed job (for the retention alarm), or null. */
export function oldestCompleted(db: SchedulerSql): number | null {
  const row = db.one<{ at: number }>(
    `SELECT completed_at AS at FROM gonvex_scheduled_jobs WHERE status='completed' ORDER BY completed_at,id LIMIT 1`,
  );
  return row ? Number(row.at) : null;
}

/** `prune_completed_jobs`: deletes up to `limit` completed jobs older than `before`; returns how many. */
export function pruneCompletedJobs(db: SchedulerSql, before: number, limit: number): number {
  return db.all(
    `DELETE FROM gonvex_scheduled_jobs WHERE id IN (
       SELECT id FROM gonvex_scheduled_jobs WHERE status='completed' AND completed_at<?1 ORDER BY completed_at,id LIMIT CAST(?2 AS INTEGER))
     RETURNING id`,
    before, limit,
  ).length;
}

/** sync_project's first statement: every pending cron run of this tenant goes. */
export function deletePendingCronRuns(db: SchedulerSql): number {
  return db.all(`DELETE FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name<>'' RETURNING id`).length;
}
