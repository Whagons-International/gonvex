// The tenant object's side of Actions: the outbox lease protocol S4 proved
// (docs/cloudflare/spikes/S4-long-actions.md, cloudflare/spikes/s4/src/tenant.ts),
// on Rust's own `_gonvex_action_outbox` table.
//
// Kept from Rust (rust/crates/store/src/lib.rs claim_outbox, renew_action,
// complete_action, retry_action; runtime/src/execution.rs
// deliver_outbox_action): a claim sets `status='processing'`,
// `locked_at=now`, `attempts+1`, and `attempts` fences every later renewal
// and completion; a row whose `locked_at` is more than one lease (5 min) old
// may be claimed again; a failure resets the row to `pending` with
// exponential backoff from 1 s, capped at 15 minutes; success deletes the row.
//
// Different on purpose (S4, 0001 IB2, 0016 G0-8):
// - completion is fenced on the attempt and applied in one transaction, so a
//   zombie attempt or a replayed completion changes nothing (exactly once);
// - a row that fails its 8th attempt, or exceeds the 32 durable hops, ends
//   `dead` (kept for operators, requeued only by an operator);
// - a renewal extends the lease in memory and writes `locked_at` only when
//   less than half of the persisted lease is left (about 24 writes per running
//   hour instead of 120). The persisted `locked_at` alone bounds recovery: an
//   alarm that wakes a fresh instance still sees the lease expire on time;
// - the object hands an Action off to a Workflow right after the commit that
//   queued it, and its sweep hands off again whatever was not claimed within
//   one lease (a lost `create()`), and whatever lease expired (a zombie);
// - a direct agent `action.call` runs through the same protocol as a run of
//   origin `call`; its result is delivered once through `onFinished`.
//
// Kernel-only state lives in `_gonvex_st_action_runs` (the `_gonvex_st_`
// prefix marks kernel objects that P03 export and M02 leave out). Scheduler
// rows (`_gonvex.scheduler.enqueue`) belong to K11 and are never touched here.
import type { ActionSession } from "./session.js";
import type { Claim, ClaimRequest, CompleteRequest, Completion, FailRequest, RenewRequest, Renewal } from "./ports.js";

export const LEASE_MS = 300_000;
export const RENEW_MS = 30_000;
export const MAX_ATTEMPTS = 8;
/** Receipts of applied completions outlive every engine retry of the step that applied them. */
export const RECEIPT_RETENTION_MS = 7 * 24 * 3600_000;
/** A direct call's result is kept for a duplicate completion only up to this size (a DO row holds 2 MB). */
export const MAX_STORED_RESULT_BYTES = 1 << 20;
export const SCHEDULE_OUTBOX_PATH = "_gonvex.scheduler.enqueue";
const PAGE = 100;

export const OUTBOX = "_gonvex_action_outbox";
export const RUNS = "_gonvex_st_action_runs";

/** Rust's DDL (rust/crates/store/src/feed.rs), for objects provisioned before K03's infrastructure. */
export const OUTBOX_SQL = `CREATE TABLE IF NOT EXISTS _gonvex_action_outbox (
  id TEXT PRIMARY KEY,
  action_path TEXT NOT NULL,
  args JSON NOT NULL,
  actor_user_id TEXT,
  actor_email TEXT,
  provenance JSON NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing')),
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  locked_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);
CREATE INDEX IF NOT EXISTS _gonvex_action_outbox_pending
  ON _gonvex_action_outbox (available_at, created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS _gonvex_action_outbox_pending_path
  ON _gonvex_action_outbox (action_path, available_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS _gonvex_action_outbox_processing
  ON _gonvex_action_outbox (locked_at) WHERE status = 'processing';`;

/**
 * Lease and receipt state the Rust table has no column for. For a run of
 * origin `call` it also holds the job columns, under the outbox's names.
 */
export const RUNS_SQL = `CREATE TABLE IF NOT EXISTS _gonvex_st_action_runs (
  id TEXT PRIMARY KEY,
  origin TEXT NOT NULL DEFAULT 'outbox' CHECK (origin IN ('outbox', 'call')),
  state TEXT NOT NULL DEFAULT 'live' CHECK (state IN ('live', 'done', 'dead')),
  owner TEXT,
  dispatched_at INTEGER,
  dispatch_count INTEGER NOT NULL DEFAULT 0,
  finished_at INTEGER,
  action_path TEXT,
  args TEXT,
  actor_user_id TEXT,
  actor_email TEXT,
  provenance TEXT,
  status TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at INTEGER,
  locked_at INTEGER,
  last_error TEXT,
  session TEXT,
  result TEXT,
  result_bytes INTEGER
);
CREATE INDEX IF NOT EXISTS _gonvex_st_action_runs_live ON _gonvex_st_action_runs (origin) WHERE state = 'live';
CREATE INDEX IF NOT EXISTS _gonvex_st_action_runs_finished ON _gonvex_st_action_runs (state, finished_at) WHERE state <> 'live';`;

/** `retry_delay_ms`: 1 s after the first failure, doubling, capped at 15 minutes. */
export function retryDelayMs(attempts: number): number {
  const exponent = Math.min(Math.max(attempts, 1), 30) - 1;
  return Math.min(1000 * 2 ** exponent, 15 * 60_000);
}

/** What the tenant hands to a runner: one Workflow instance per hand-off. */
export interface ActionJob { v: 1; projectId: string; tenantId: string; id: string; n: number }

export interface Dispatcher {
  /** Starts a runner for `job`. Idempotent per (id, n): a second create of the same instance is not an error. */
  dispatch(job: ActionJob): Promise<void>;
}

/** A direct call's outcome, delivered once (K05/K07 turn it into `action.result` / `action.error`). */
export interface CallFinished { id: string; ok: boolean; value?: string | undefined; error?: string | undefined }

export interface ActionOutboxOptions {
  storage: DurableObjectStorage;
  projectId: string;
  tenantId: string;
  dispatcher: Dispatcher;
  /** Asks the object's single alarm (K11) to fire by `at`; the alarm calls `sweep()`. */
  wake: (at: number) => void | Promise<void>;
  onFinished?: ((finished: CallFinished) => void | Promise<void>) | undefined;
  clock?: (() => number) | undefined;
  randomUUID?: (() => string) | undefined;
  leaseMs?: number | undefined;
  renewMs?: number | undefined;
  maxAttempts?: number | undefined;
}

interface Job {
  id: string;
  origin: "outbox" | "call";
  /** `live` while it may run, `done` or `dead` when finished (`null`: no run row yet). */
  state: "live" | "done" | "dead";
  path: string;
  args: string;
  actor_user_id: string | null;
  actor_email: string | null;
  provenance: string | null;
  status: "pending" | "processing";
  attempts: number;
  available_at: number;
  locked_at: number | null;
  last_error: string | null;
  owner: string | null;
  dispatched_at: number | null;
  dispatch_count: number;
  session: string | null;
  result: string | null;
}

const JOB_FROM_OUTBOX = `SELECT o.id, 'outbox' AS origin, COALESCE(r.state, 'live') AS state, o.action_path AS path, o.args,
  o.actor_user_id, o.actor_email, o.provenance, o.status, o.attempts, o.available_at, o.locked_at, o.last_error,
  r.owner, r.dispatched_at, COALESCE(r.dispatch_count, 0) AS dispatch_count, NULL AS session, NULL AS result
  FROM ${OUTBOX} o LEFT JOIN ${RUNS} r ON r.id = o.id`;
const JOB_FROM_RUNS = `SELECT id, origin, state, COALESCE(action_path, '') AS path, COALESCE(args, 'null') AS args,
  actor_user_id, actor_email, provenance, COALESCE(status, 'pending') AS status, attempts, COALESCE(available_at, 0) AS available_at,
  locked_at, last_error, owner, dispatched_at, dispatch_count, session, result FROM ${RUNS}`;

/** Counters for the PR's numbers and the tests (rows as SQLite counts them). */
export interface OutboxStats { rowsRead: number; rowsWritten: number; dispatches: number }

export class ActionOutbox {
  private readonly sql: SqlStorage;
  private readonly clock: () => number;
  private readonly randomUUID: () => string;
  readonly leaseMs: number;
  readonly renewMs: number;
  readonly maxAttempts: number;
  /** Leases extended by renewals since `locked_at` was last written. Never needed for recovery. */
  private readonly leases = new Map<string, number>();
  readonly stats: OutboxStats = { rowsRead: 0, rowsWritten: 0, dispatches: 0 };

  constructor(private readonly options: ActionOutboxOptions) {
    this.sql = options.storage.sql;
    this.clock = options.clock ?? (() => Date.now());
    this.randomUUID = options.randomUUID ?? (() => crypto.randomUUID());
    this.leaseMs = options.leaseMs ?? LEASE_MS;
    this.renewMs = options.renewMs ?? RENEW_MS;
    this.maxAttempts = options.maxAttempts ?? MAX_ATTEMPTS;
    this.sql.exec(OUTBOX_SQL);
    this.sql.exec(RUNS_SQL);
  }

  private exec<T extends Record<string, SqlStorageValue>>(statement: string, ...bindings: unknown[]): T[] {
    const cursor = this.sql.exec<T>(statement, ...bindings);
    const rows = cursor.toArray();
    this.stats.rowsRead += cursor.rowsRead;
    this.stats.rowsWritten += cursor.rowsWritten;
    return rows;
  }

  private load(id: string): Job | undefined {
    return (this.exec(`${JOB_FROM_OUTBOX} WHERE o.id = ?`, id)[0] ?? this.exec(`${JOB_FROM_RUNS} WHERE id = ?`, id)[0]) as Job | undefined;
  }

  /** The table holding a job's columns. */
  private table(job: Job): string { return job.origin === "outbox" ? OUTBOX : RUNS; }

  /** The later of the persisted lease (`locked_at` + one lease) and one extended in memory since. */
  leaseExpiry(job: Pick<Job, "id" | "locked_at">): number {
    return Math.max(this.leases.get(job.id) ?? 0, (job.locked_at ?? 0) + this.leaseMs);
  }

  /** One statement: creates the run row or updates the named columns. */
  private upsertRun(id: string, origin: "outbox" | "call", fields: Record<string, SqlStorageValue>): void {
    const names = Object.keys(fields);
    this.exec(
      `INSERT INTO ${RUNS} (id, origin${names.map((name) => `, ${name}`).join("")}) VALUES (?, ?${names.map(() => ", ?").join("")})
       ON CONFLICT (id) DO UPDATE SET ${names.map((name) => `${name} = excluded.${name}`).join(", ")}`,
      id, origin, ...names.map((name) => fields[name]),
    );
  }

  // ---- hand-off --------------------------------------------------------------

  private async dispatch(job: Pick<Job, "id" | "origin" | "dispatch_count">): Promise<boolean> {
    const n = job.dispatch_count + 1;
    try {
      await this.options.dispatcher.dispatch({ v: 1, projectId: this.options.projectId, tenantId: this.options.tenantId, id: job.id, n });
    } catch {
      // The sweep tries again; the row stays as it was.
      return false;
    }
    this.stats.dispatches++;
    this.upsertRun(job.id, job.origin, { dispatched_at: this.clock(), dispatch_count: n });
    return true;
  }

  /**
   * After the commit of a Reducer that queued Actions: hands every pending,
   * due, never-dispatched Action to a runner, then arms the sweep.
   */
  async afterCommit(): Promise<void> {
    const now = this.clock();
    let after = "";
    for (;;) {
      const page = this.exec(`${JOB_FROM_OUTBOX} WHERE o.status = 'pending' AND o.action_path <> ? AND o.available_at <= ? AND r.dispatched_at IS NULL AND o.id > ? ORDER BY o.id LIMIT ${PAGE}`, SCHEDULE_OUTBOX_PATH, now, after) as unknown as Job[];
      for (const job of page) await this.dispatch(job);
      if (page.length < PAGE) break;
      after = page[page.length - 1]!.id;
    }
    await this.options.wake(now + this.leaseMs);
  }

  /** A direct `action.call` of an agent Action, run by a Workflow like an outbox Action. Returns the run id. */
  async beginCall(input: { session: ActionSession; path: string; args: unknown; provenance?: unknown }): Promise<string> {
    const id = `call-${this.randomUUID()}`;
    const now = this.clock();
    this.exec(
      `INSERT INTO ${RUNS} (id, origin, state, action_path, args, actor_user_id, actor_email, provenance, status, attempts, available_at, session)
       VALUES (?, 'call', 'live', ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, 'pending', 0, ?, ?)`,
      id, input.path, JSON.stringify(input.args ?? null), input.session.account.id, input.session.account.email ?? "",
      input.provenance === undefined ? null : JSON.stringify(input.provenance), now, JSON.stringify(input.session),
    );
    await this.dispatch({ id, origin: "call", dispatch_count: 0 });
    await this.options.wake(now + this.leaseMs);
    return id;
  }

  // ---- the lease protocol (tenant RPCs, one object argument each) -------------

  async claim(request: ClaimRequest): Promise<Claim> {
    const now = this.clock();
    const job = this.load(request.id);
    if (!job || job.path === SCHEDULE_OUTBOX_PATH) return { v: 1, ok: false, reason: "missing" };
    if (job.state === "done" || job.state === "dead") return { v: 1, ok: false, reason: job.state };
    if (job.status === "processing" && job.owner !== request.owner) {
      const expiry = this.leaseExpiry(job);
      if (expiry > now) return { v: 1, ok: false, reason: "leased", retryAfterMs: expiry - now };
    }
    if (job.status === "pending" && job.available_at > now) return { v: 1, ok: false, reason: "notDue", retryAfterMs: job.available_at - now };
    if (job.attempts >= this.maxAttempts) {
      await this.kill(job, job.last_error ?? `Action failed ${job.attempts} times`);
      return { v: 1, ok: false, reason: "dead" };
    }
    const attempt = job.attempts + 1;
    this.options.storage.transactionSync(() => {
      this.exec(`UPDATE ${this.table(job)} SET status = 'processing', locked_at = ?, attempts = ? WHERE id = ?`, now, attempt, job.id);
      this.upsertRun(job.id, job.origin, { owner: request.owner, state: "live" });
    });
    this.leases.set(job.id, now + this.leaseMs);
    await this.options.wake(now + this.leaseMs);
    let args: unknown = null;
    try { args = JSON.parse(job.args); } catch { args = null; }
    let session: ActionSession | null = null;
    if (job.session) { try { session = JSON.parse(job.session) as ActionSession; } catch { session = null; } }
    return {
      v: 1, ok: true, id: job.id, origin: job.origin, attempt, path: job.path, args,
      actorAccountId: job.actor_user_id ?? "", provenance: job.provenance, session, leaseMs: this.leaseMs, renewMs: this.renewMs,
    };
  }

  /** Only the current attempt may extend its lease; `locked_at` is written only below half a lease. */
  async renew(request: RenewRequest): Promise<Renewal> {
    const job = this.load(request.id);
    const ok = job !== undefined && job.state === "live" && job.status === "processing" && job.attempts === request.attempt;
    if (!ok) return { v: 1, ok: false, persisted: false };
    const now = this.clock();
    this.leases.set(job.id, now + this.leaseMs);
    let persisted = false;
    if ((job.locked_at ?? 0) + this.leaseMs < now + this.leaseMs / 2) {
      this.exec(`UPDATE ${this.table(job)} SET locked_at = ? WHERE id = ? AND attempts = ?`, now, job.id, request.attempt);
      persisted = true;
      await this.options.wake(now + this.leaseMs);
    }
    return { v: 1, ok: true, persisted };
  }

  /** `retry_action`, fenced on the attempt; the 8th failure (or a terminal one) ends `dead`. */
  async fail(request: FailRequest): Promise<{ v: 1; state: "retry" | "dead" | "stale" }> {
    const job = this.load(request.id);
    if (!job || job.state !== "live" || job.status !== "processing" || job.attempts !== request.attempt) return { v: 1, state: "stale" };
    const error = request.error.slice(0, 8_000);
    this.leases.delete(job.id);
    if (request.terminal === true || job.attempts >= this.maxAttempts) {
      await this.kill(job, error);
      return { v: 1, state: "dead" };
    }
    const due = this.clock() + retryDelayMs(job.attempts);
    this.options.storage.transactionSync(() => {
      this.exec(`UPDATE ${this.table(job)} SET status = 'pending', locked_at = NULL, available_at = ?, last_error = ? WHERE id = ?`, due, error, job.id);
      this.upsertRun(job.id, job.origin, { owner: null, dispatched_at: null });
    });
    await this.options.wake(due);
    return { v: 1, state: "retry" };
  }

  /**
   * The Reducer-like completion: fenced on the attempt and applied in one
   * transaction. A replay of an applied completion is a no-op (`duplicate`); a
   * completion from any other attempt is `rejected`.
   */
  async complete(request: CompleteRequest): Promise<Completion> {
    const now = this.clock();
    let finished: CallFinished | undefined;
    const outcome = this.options.storage.transactionSync((): Completion => {
      const job = this.load(request.id);
      if (job?.state === "done") return { v: 1, applied: false, duplicate: true, appliedAttempt: job.attempts };
      if (!job || job.state !== "live" || job.status !== "processing" || job.attempts !== request.attempt) return { v: 1, applied: false, rejected: true };
      const bytes = request.value === undefined ? null : new TextEncoder().encode(request.value).byteLength;
      if (job.origin === "outbox") {
        this.exec(`DELETE FROM ${OUTBOX} WHERE id = ?`, job.id);
        this.upsertRun(job.id, "outbox", { state: "done", attempts: request.attempt, finished_at: now, result_bytes: bytes, owner: null });
      } else {
        const stored = request.value !== undefined && (bytes ?? 0) <= MAX_STORED_RESULT_BYTES ? request.value : null;
        this.exec(`UPDATE ${RUNS} SET state = 'done', finished_at = ?, result = ?, result_bytes = ?, last_error = ?, owner = NULL, session = NULL WHERE id = ?`,
          now, stored, bytes, request.error ?? null, job.id);
        finished = request.error !== undefined ? { id: job.id, ok: false, error: request.error } : { id: job.id, ok: true, value: request.value ?? "null" };
      }
      return { v: 1, applied: true, appliedAttempt: request.attempt };
    });
    if (outcome.applied) {
      this.leases.delete(request.id);
      if (finished) await this.options.onFinished?.(finished);
    }
    return outcome;
  }

  /** `cancel`: the row ends `dead`; the running attempt finds out at its next renewal. */
  async cancel(id: string): Promise<boolean> {
    const job = this.load(id);
    if (!job || job.state !== "live" || job.path === SCHEDULE_OUTBOX_PATH) return false;
    this.leases.delete(id);
    await this.kill(job, "cancelled");
    return true;
  }

  /** Moves a job to `dead`. Outbox rows leave Rust's table (its CHECK has no `dead`) and keep everything in the run row. */
  private async kill(job: Job, error: string): Promise<void> {
    const now = this.clock();
    this.options.storage.transactionSync(() => {
      if (job.origin === "outbox") {
        this.upsertRun(job.id, "outbox", {
          state: "dead", finished_at: now, action_path: job.path, args: job.args, actor_user_id: job.actor_user_id,
          actor_email: job.actor_email, provenance: job.provenance, attempts: job.attempts, last_error: error, owner: null,
        });
        this.exec(`DELETE FROM ${OUTBOX} WHERE id = ?`, job.id);
      } else {
        this.exec(`UPDATE ${RUNS} SET state = 'dead', finished_at = ?, last_error = ?, owner = NULL, session = NULL WHERE id = ?`, now, error, job.id);
      }
    });
    this.leases.delete(job.id);
    if (job.origin === "call") await this.options.onFinished?.({ id: job.id, ok: false, error });
  }

  // ---- recovery ----------------------------------------------------------------

  /**
   * The alarm's work (K11 multiplexes it): hands off again every Action whose
   * lease expired or whose hand-off was not claimed within one lease, starts
   * Actions whose retry is due, and prunes old receipts. Pages through the
   * open rows. Returns when it wants to run next (or `null`).
   */
  async sweep(): Promise<number | null> {
    const now = this.clock();
    let next = Infinity;
    const visit = async (job: Job) => {
      if (job.path === SCHEDULE_OUTBOX_PATH) return;
      const handedOffRecently = job.dispatched_at !== null && job.dispatched_at + this.leaseMs > now;
      let due: number;
      if (job.status === "processing") {
        const expiry = this.leaseExpiry(job);
        due = expiry > now ? expiry : handedOffRecently ? job.dispatched_at! + this.leaseMs : now;
      } else if (job.available_at > now) {
        due = job.available_at;
      } else {
        due = handedOffRecently ? job.dispatched_at! + this.leaseMs : now;
      }
      if (due > now) { next = Math.min(next, due); return; }
      next = Math.min(next, (await this.dispatch(job)) ? now + this.leaseMs : now + this.renewMs);
    };
    for (const source of [`${JOB_FROM_OUTBOX} WHERE o.status IN ('pending', 'processing') AND o.id > ? ORDER BY o.id LIMIT ${PAGE}`,
      `${JOB_FROM_RUNS} WHERE origin = 'call' AND state = 'live' AND id > ? ORDER BY id LIMIT ${PAGE}`]) {
      let after = "";
      for (;;) {
        const page = this.exec(source, after) as unknown as Job[];
        for (const job of page) await visit(job);
        if (page.length < PAGE) break;
        after = page[page.length - 1]!.id;
      }
    }
    // Receipts: pruned in one bounded statement per sweep.
    const pruned = this.exec(`DELETE FROM ${RUNS} WHERE id IN (SELECT id FROM ${RUNS} WHERE state = 'done' AND finished_at < ? LIMIT 500) RETURNING id`, now - RECEIPT_RETENTION_MS);
    const oldest = this.exec<{ at: number | null }>(`SELECT MIN(finished_at) AS at FROM ${RUNS} WHERE state = 'done'`)[0]?.at ?? null;
    if (pruned.length === 500) next = Math.min(next, now + 1_000);
    else if (oldest !== null) next = Math.min(next, oldest + RECEIPT_RETENTION_MS);
    const at = Number.isFinite(next) ? next : null;
    if (at !== null) await this.options.wake(at);
    return at;
  }

  // ---- operators (CONTRACT.md D.1 "dead Actions") -----------------------------------

  listDead(limit = 50, cursor = ""): { items: Array<{ id: string; function: string; args: unknown; attempts: number; lastError: string | null; deadAt: number }>; cursor: string | null } {
    const rows = this.exec<{ id: string; action_path: string | null; args: string | null; attempts: number; last_error: string | null; finished_at: number }>(
      `SELECT id, action_path, args, attempts, last_error, finished_at FROM ${RUNS} WHERE state = 'dead' AND origin = 'outbox' AND id > ? ORDER BY id LIMIT ?`,
      cursor, Math.min(Math.max(limit, 1), 500) + 1);
    const page = rows.slice(0, Math.min(Math.max(limit, 1), 500));
    return {
      items: page.map((row) => {
        let args: unknown = null;
        try { args = JSON.parse(row.args ?? "null"); } catch { args = row.args; }
        return { id: row.id, function: row.action_path ?? "", args, attempts: row.attempts, lastError: row.last_error, deadAt: row.finished_at };
      }),
      cursor: rows.length > page.length ? page[page.length - 1]!.id : null,
    };
  }

  /** Puts a dead outbox Action back in Rust's table with its attempts reset, and hands it off. */
  async requeue(id: string): Promise<boolean> {
    const row = this.exec<{ action_path: string; args: string; actor_user_id: string | null; actor_email: string | null; provenance: string | null }>(
      `SELECT action_path, args, actor_user_id, actor_email, provenance FROM ${RUNS} WHERE id = ? AND state = 'dead' AND origin = 'outbox'`, id)[0];
    if (!row) return false;
    const now = this.clock();
    this.options.storage.transactionSync(() => {
      this.exec(`INSERT INTO ${OUTBOX} (id, action_path, args, actor_user_id, actor_email, provenance, status, attempts, available_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?)`,
        id, row.action_path, row.args, row.actor_user_id, row.actor_email, row.provenance ?? "{}", now);
      this.exec(`UPDATE ${RUNS} SET state = 'live', finished_at = NULL, action_path = NULL, args = NULL, actor_user_id = NULL, actor_email = NULL, provenance = NULL, attempts = 0, last_error = NULL, dispatched_at = NULL WHERE id = ?`, id);
    });
    const job = this.load(id);
    if (job) await this.dispatch(job);
    await this.options.wake(now + this.leaseMs);
    return true;
  }

  /** One row's state, for tests and the operator view. */
  inspect(id: string): Record<string, unknown> | null {
    const job = this.load(id);
    return job ? { ...job, leaseExpiry: job.state === "live" && job.status === "processing" ? this.leaseExpiry(job) : null } : null;
  }

  /** Forgets in-memory leases, as an object reset does (tests). */
  forgetMemory(): void { this.leases.clear(); }
}
