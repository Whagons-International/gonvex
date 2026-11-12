// Direct agent `action.call`s in the tenant object: each becomes a run in
// `_gonvex_st_action_runs`, run by a Workflow (ActionWorkflow) with the lease
// protocol S4 proved, and its result is delivered once through `onFinished`
// for K05/K07 to answer the socket.
//
// One claimer per table (0037, as 0022 §1): K11's scheduler owns
// `_gonvex_action_outbox`; outbox and scheduled Actions reach K12 only through
// K11's hand-off (handoff.ts). This file never reads or writes that table.
//
// The protocol (S4, cloudflare/spikes/s4/src/tenant.ts):
// - a claim sets `status='processing'`, `locked_at=now`, `attempts+1`; the
//   attempt fences every later renewal and completion; the owner that already
//   holds the run (a retried Workflow step) takes it over at once, any other
//   owner waits until the lease has expired;
// - a renewal extends the lease in memory and writes `locked_at` only when
//   less than half of the persisted lease is left. The persisted `locked_at`
//   alone bounds recovery, so an alarm that wakes a fresh instance still sees
//   the lease expire on time;
// - completion is fenced on the attempt and applied in one transaction; a
//   replay is `duplicate`, another attempt's completion is `rejected`;
// - a direct call is answered once and not retried, as Rust answers the
//   socket once; only a runner that died (its lease expired, or the step
//   crashed) is run again, at most `maxAttempts` times, then the call fails;
// - the sweep (an alarm source on K11's `TenantAlarm`) hands a run off again
//   when its hand-off was not claimed within one lease or its lease expired,
//   and prunes old receipts.
import type { ActionSession } from "./session.js";
import type { Claim, ClaimRequest, CompleteRequest, Completion, Renewal, RenewRequest } from "./ports.js";

export const LEASE_MS = 300_000;
export const RENEW_MS = 30_000;
/** IB2's cap (0001), applied to runners that died: a call whose runner died this often fails. */
export const MAX_ATTEMPTS = 8;
/** Receipts of applied completions outlive every engine retry of the step that applied them. */
export const RECEIPT_RETENTION_MS = 7 * 24 * 3600_000;
/** A result is kept for a duplicate completion only up to this size (a DO row holds 2 MB). */
export const MAX_STORED_RESULT_BYTES = 1 << 20;
const PAGE = 100;
const DISPATCH_CONCURRENCY = 16;

export const RUNS = "_gonvex_st_action_runs";

export const RUNS_SQL = `CREATE TABLE IF NOT EXISTS _gonvex_st_action_runs (
  id TEXT PRIMARY KEY,
  state TEXT NOT NULL DEFAULT 'live' CHECK (state IN ('live', 'done', 'dead')),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing')),
  attempts INTEGER NOT NULL DEFAULT 0,
  owner TEXT,
  locked_at INTEGER,
  dispatched_at INTEGER,
  dispatch_count INTEGER NOT NULL DEFAULT 0,
  action_path TEXT NOT NULL,
  args TEXT NOT NULL,
  session TEXT,
  provenance TEXT,
  created_at INTEGER NOT NULL,
  finished_at INTEGER,
  last_error TEXT,
  result TEXT,
  result_bytes INTEGER
);
CREATE INDEX IF NOT EXISTS _gonvex_st_action_runs_live ON _gonvex_st_action_runs (id) WHERE state = 'live';
CREATE INDEX IF NOT EXISTS _gonvex_st_action_runs_finished ON _gonvex_st_action_runs (finished_at) WHERE state <> 'live';`;

/**
 * Kernel objects K12 adds to a tenant, for A01's `addedObjects()` (0037): P03
 * export and M02 leave them out, as for K11's `SCHEDULER_OBJECTS`.
 */
export const ACTION_OBJECTS: readonly (readonly ["table" | "index", string])[] = [
  ["table", RUNS],
  ["index", "_gonvex_st_action_runs_live"],
  ["index", "_gonvex_st_action_runs_finished"],
];

/** What the tenant hands to a runner: one Workflow instance per (run, hand-off number). */
export interface CallJob { v: 1; kind: "call"; projectId: string; tenantId: string; id: string; n: number }

export interface Dispatcher {
  /** Starts a runner for `job`. Idempotent per (id, n): a second create of the same instance is not an error. */
  dispatch(job: CallJob): Promise<void>;
}

/** A direct call's outcome, delivered once (K05/K07 turn it into `action.result` / `action.error`). */
export interface CallFinished { reply?: { host: string; connectionId: string; epoch: number; id: string; path: string; tenantId: string; traceJson?: string }; committedRevision?: number | undefined; id: string; ok: boolean; value?: string | undefined; error?: string | undefined }

export interface ActionCallsOptions {
  storage: DurableObjectStorage;
  projectId: string;
  tenantId: string;
  dispatcher: Dispatcher;
  /** Re-arms the object's alarm after work was queued (K11's `TenantAlarm.arm`); the alarm runs `alarmSource()`. */
  wake: () => void | Promise<void>;
  onFinished?: ((finished: CallFinished) => void | Promise<void>) | undefined;
  clock?: (() => number) | undefined;
  randomUUID?: (() => string) | undefined;
  leaseMs?: number | undefined;
  renewMs?: number | undefined;
  maxAttempts?: number | undefined;
}

interface Run {
  id: string;
  state: "live" | "done" | "dead";
  status: "pending" | "processing";
  attempts: number;
  owner: string | null;
  locked_at: number | null;
  dispatched_at: number | null;
  dispatch_count: number;
  action_path: string;
  args: string;
  session: string | null;
  provenance: string | null;
  created_at: number;
  last_error: string | null;
}

/** Counters for the PR's numbers and the tests (rows as SQLite counts them, index entries included). */
export interface CallStats { rowsRead: number; rowsWritten: number; dispatches: number }

/** K11's `AlarmSource` shape (kernel/scheduler/alarm.ts), structurally. */
export interface CallsAlarmSource {
  readonly name: string;
  nextAt(now: number): number | null;
  run(now: number, budget: { readonly startedAt: number; readonly maxUnits: number }): Promise<number>;
}

export class ActionCalls {
  private readonly sql: SqlStorage;
  private readonly clock: () => number;
  private readonly randomUUID: () => string;
  readonly leaseMs: number;
  readonly renewMs: number;
  readonly maxAttempts: number;
  /** Leases extended by renewals since `locked_at` was last written. Never needed for recovery. */
  private readonly leases = new Map<string, number>();
  /** A hand-off that failed is tried again after one renewal interval, not in a tight alarm loop. */
  private readonly retryAt = new Map<string, number>();
  readonly stats: CallStats = { rowsRead: 0, rowsWritten: 0, dispatches: 0 };

  constructor(private readonly options: ActionCallsOptions) {
    this.sql = options.storage.sql;
    this.clock = options.clock ?? (() => Date.now());
    this.randomUUID = options.randomUUID ?? (() => crypto.randomUUID());
    this.leaseMs = options.leaseMs ?? LEASE_MS;
    this.renewMs = options.renewMs ?? RENEW_MS;
    this.maxAttempts = options.maxAttempts ?? MAX_ATTEMPTS;
    this.sql.exec(RUNS_SQL);
  }

  private exec<T extends Record<string, SqlStorageValue>>(statement: string, ...bindings: unknown[]): T[] {
    const cursor = this.sql.exec<T>(statement, ...bindings);
    const rows = cursor.toArray();
    this.stats.rowsRead += cursor.rowsRead;
    this.stats.rowsWritten += cursor.rowsWritten;
    return rows;
  }

  private load(id: string): Run | undefined {
    return this.exec(`SELECT * FROM ${RUNS} WHERE id = ?`, id)[0] as unknown as Run | undefined;
  }

  /** The later of the persisted lease (`locked_at` + one lease) and one extended in memory since. */
  leaseExpiry(run: Pick<Run, "id" | "locked_at">): number {
    return Math.max(this.leases.get(run.id) ?? 0, (run.locked_at ?? 0) + this.leaseMs);
  }

  /**
   * Hands runs to runners: creates run concurrently (16 at a time; S4
   * measured 740 ms p50 per Workflow `create()`), then one statement records
   * every hand-off that succeeded. A failed create is left for the sweep.
   */
  private async dispatchMany(runs: ReadonlyArray<Pick<Run, "id" | "dispatch_count">>): Promise<Set<string>> {
    const done: Array<{ id: string; n: number }> = [];
    for (let start = 0; start < runs.length; start += DISPATCH_CONCURRENCY) {
      await Promise.all(runs.slice(start, start + DISPATCH_CONCURRENCY).map(async (run) => {
        const n = run.dispatch_count + 1;
        try {
          await this.options.dispatcher.dispatch({ v: 1, kind: "call", projectId: this.options.projectId, tenantId: this.options.tenantId, id: run.id, n });
          done.push({ id: run.id, n });
          this.retryAt.delete(run.id);
        } catch {
          // The sweep tries again after one renewal interval.
          this.retryAt.set(run.id, this.clock() + this.renewMs);
        }
      }));
    }
    if (done.length) {
      this.stats.dispatches += done.length;
      this.exec(
        `UPDATE ${RUNS} SET dispatched_at = ?, dispatch_count = json_extract(j.value, '$.n')
         FROM json_each(?) AS j WHERE ${RUNS}.id = json_extract(j.value, '$.id')`,
        this.clock(), JSON.stringify(done),
      );
    }
    return new Set(done.map((run) => run.id));
  }

  /** Starts a direct agent call: one row, one hand-off. Returns the run id. */
  async begin(input: { session: ActionSession; path: string; args: unknown; provenance?: unknown }): Promise<string> {
    const id = `call-${this.randomUUID()}`;
    this.exec(
      `INSERT INTO ${RUNS} (id, action_path, args, session, provenance, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
      id, input.path, JSON.stringify(input.args ?? null), JSON.stringify(input.session),
      input.provenance === undefined ? null : JSON.stringify(input.provenance), this.clock(),
    );
    await this.dispatchMany([{ id, dispatch_count: 0 }]);
    await this.options.wake();
    return id;
  }

  // ---- the run protocol (tenant RPCs, one object argument each) -----------------

  async claim(request: ClaimRequest): Promise<Claim> {
    const now = this.clock();
    const run = this.load(request.id);
    if (!run) return { v: 1, ok: false, reason: "missing" };
    if (run.state !== "live") return { v: 1, ok: false, reason: run.state };
    if (run.status === "processing" && run.owner !== request.owner) {
      const expiry = this.leaseExpiry(run);
      if (expiry > now) return { v: 1, ok: false, reason: "leased", retryAfterMs: expiry - now };
    }
    if (run.attempts >= this.maxAttempts) {
      await this.kill(run, run.last_error ?? `Action runner stopped ${run.attempts} times without finishing`);
      return { v: 1, ok: false, reason: "dead" };
    }
    const attempt = run.attempts + 1;
    this.exec(`UPDATE ${RUNS} SET status = 'processing', locked_at = ?, attempts = ?, owner = ? WHERE id = ?`, now, attempt, request.owner, run.id);
    this.leases.set(run.id, now + this.leaseMs);
    await this.options.wake();
    let args: unknown = null;
    try { args = JSON.parse(run.args); } catch { args = null; }
    return {
      v: 1, ok: true, id: run.id, attempt, path: run.action_path, args, provenance: run.provenance,
      session: JSON.parse(run.session ?? "null") as ActionSession, leaseMs: this.leaseMs, renewMs: this.renewMs,
    };
  }

  /** Only the current attempt may extend its lease; `locked_at` is written only below half a lease. */
  async renew(request: RenewRequest): Promise<Renewal> {
    const run = this.load(request.id);
    if (!run || run.state !== "live" || run.status !== "processing" || run.attempts !== request.attempt) return { v: 1, ok: false, persisted: false };
    const now = this.clock();
    this.leases.set(run.id, now + this.leaseMs);
    if ((run.locked_at ?? 0) + this.leaseMs >= now + this.leaseMs / 2) return { v: 1, ok: true, persisted: false };
    this.exec(`UPDATE ${RUNS} SET locked_at = ? WHERE id = ? AND attempts = ?`, now, run.id, request.attempt);
    await this.options.wake();
    return { v: 1, ok: true, persisted: true };
  }

  private replyContext(run: Run): Pick<CallFinished, "reply"> {
    const session = JSON.parse(run.session ?? "null") as ActionSession | null;
    if (!session?.reply) return {};
    const { host, connectionId, epoch, id, path } = session.reply;
    return { reply: { host, connectionId, epoch, id, path, tenantId: session.tenantId, ...(session.reply.traceJson ? { traceJson: session.reply.traceJson } : {}) } };
  }

  /** Fenced on the attempt, one statement; the outcome is delivered once. */
  async complete(request: CompleteRequest): Promise<Completion> {
    const now = this.clock();
    const run = this.load(request.id);
    if (run?.state === "done") return { v: 1, applied: false, duplicate: true, appliedAttempt: run.attempts };
    if (!run || run.state !== "live" || run.status !== "processing" || run.attempts !== request.attempt) return { v: 1, applied: false, rejected: true };
    const bytes = request.value === undefined ? null : new TextEncoder().encode(request.value).byteLength;
    const stored = request.value !== undefined && (bytes ?? 0) <= MAX_STORED_RESULT_BYTES ? request.value : null;
    const applied = this.exec(
      `UPDATE ${RUNS} SET state = 'done', finished_at = ?, result = ?, result_bytes = ?, last_error = ?, owner = NULL, session = NULL
       WHERE id = ? AND state = 'live' AND status = 'processing' AND attempts = ? RETURNING id`,
      now, stored, bytes, request.error ?? null, run.id, request.attempt,
    ).length === 1;
    if (!applied) return { v: 1, applied: false, rejected: true };
    this.leases.delete(run.id);
    await this.options.onFinished?.(request.error !== undefined ? { id: run.id, ok: false, error: request.error, ...this.replyContext(run) } : { id: run.id, ok: true, value: request.value ?? "null", ...(request.committedRevision === undefined ? {} : { committedRevision: request.committedRevision }), ...this.replyContext(run) });
    return { v: 1, applied: true, appliedAttempt: request.attempt };
  }

  /** Cancels a live call (the client went away): the run ends `dead`, its next renewal is refused. */
  async cancel(id: string): Promise<boolean> {
    const run = this.load(id);
    if (!run || run.state !== "live") return false;
    await this.kill(run, "cancelled");
    return true;
  }

  private async kill(run: Run, error: string): Promise<void> {
    this.exec(`UPDATE ${RUNS} SET state = 'dead', finished_at = ?, last_error = ?, owner = NULL, session = NULL WHERE id = ?`, this.clock(), error, run.id);
    this.leases.delete(run.id);
    await this.options.onFinished?.({ id: run.id, ok: false, error, ...this.replyContext(run) });
  }

  // ---- recovery: an alarm source on K11's TenantAlarm --------------------------------

  /** When a live run next needs the sweep; `now` means hand it off now. */
  private dueAt(run: Pick<Run, "id" | "status" | "locked_at" | "dispatched_at">, now: number): number {
    const handedOff = run.dispatched_at !== null && run.dispatched_at + this.leaseMs > now
      ? run.dispatched_at + this.leaseMs
      : Math.max(now, this.retryAt.get(run.id) ?? 0);
    if (run.status === "processing") {
      const expiry = this.leaseExpiry(run);
      return expiry > now ? expiry : handedOff;
    }
    return handedOff;
  }

  /** Earliest time the sweep has work: live runs are the open direct calls of one tenant, a handful. */
  nextAt(now: number): number | null {
    let next = Infinity;
    for (const run of this.exec<{ id: string; status: string; locked_at: number | null; dispatched_at: number | null }>(
      `SELECT id, status, locked_at, dispatched_at FROM ${RUNS} WHERE state = 'live'`)) {
      next = Math.min(next, this.dueAt(run as Pick<Run, "id" | "status" | "locked_at" | "dispatched_at">, now));
    }
    const oldest = this.exec<{ at: number | null }>(`SELECT MIN(finished_at) AS at FROM ${RUNS} WHERE state = 'done'`)[0]?.at ?? null;
    if (oldest !== null) next = Math.min(next, oldest + RECEIPT_RETENTION_MS);
    return Number.isFinite(next) ? next : null;
  }

  /** Hands off every live run that needs it, in pages, and prunes receipts in one bounded statement. */
  async sweep(now = this.clock()): Promise<number> {
    let units = 0;
    let after = "";
    for (;;) {
      const page = this.exec(`SELECT id, status, locked_at, dispatched_at, dispatch_count FROM ${RUNS} WHERE state = 'live' AND id > ? ORDER BY id LIMIT ${PAGE}`, after) as unknown as Run[];
      const due = page.filter((run) => this.dueAt(run, now) <= now);
      units += (await this.dispatchMany(due)).size;
      if (page.length < PAGE) break;
      after = page[page.length - 1]!.id;
    }
    units += this.exec(`DELETE FROM ${RUNS} WHERE id IN (SELECT id FROM ${RUNS} WHERE state = 'done' AND finished_at < ? LIMIT 500) RETURNING id`, now - RECEIPT_RETENTION_MS).length;
    return units;
  }

  /** Registers with K11's `TenantAlarm` (`alarm.register(calls.alarmSource())`). */
  alarmSource(): CallsAlarmSource {
    return { name: "actions.calls", nextAt: (now) => this.nextAt(now), run: (now) => this.sweep(now) };
  }

  /** One run's state, for tests and an operator view. */
  inspect(id: string): Record<string, unknown> | null {
    const run = this.load(id);
    return run ? { ...run, leaseExpiry: run.state === "live" && run.status === "processing" ? this.leaseExpiry(run) : null } : null;
  }

  /** Forgets in-memory leases, as an object reset does (tests). */
  forgetMemory(): void { this.leases.clear(); }
}
