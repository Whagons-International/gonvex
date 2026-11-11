// The tenant Durable Object side of a long Action: the outbox row, its lease,
// the sweep that recovers lost work, and the Reducer-like result application.
//
// Mirrors the Rust reference (`rust/crates/store/src/lib.rs` claim_outbox /
// renew_action / complete_action, `runtime/src/execution.rs`
// deliver_outbox_action): claim bumps `attempts` and that number fences every
// later renew and completion. Differences are deliberate and listed in
// docs/cloudflare/spikes/S4-long-actions.md:
//   * a renewal extends the lease in memory and writes `lease_until` only when
//     less than half of the persisted lease is left (about 24 row writes per
//     running hour instead of 120). A first version kept the lease only in
//     memory and gave every claimed row a fresh lease on wake; the platform
//     stall test showed that a sleeping tenant then never expires a dead
//     runner's lease, because each sweep alarm wakes a new instance;
//   * completion is fenced on the attempt and leaves a receipt, so it applies
//     exactly once;
//   * a maximum attempt count ends in a `dead` state.
import { DurableObject } from "cloudflare:workers";
import type { Env, Runner } from "./env";

export interface ClaimOk {
  ok: true;
  attempt: number;
  path: string;
  args: string;
  leaseMs: number;
  renewMs: number;
}
export interface ClaimRefused {
  ok: false;
  reason: "missing" | "done" | "dead" | "leased";
  retryAfterMs?: number;
}
export type Claim = ClaimOk | ClaimRefused;

export interface Completion {
  applied: boolean;
  duplicate?: boolean;
  rejected?: boolean;
  appliedAttempt?: number;
}

type Row = {
  id: string;
  path: string;
  args: string;
  runner: Runner;
  status: "pending" | "processing" | "done" | "dead";
  attempts: number;
  owner: string | null;
  lease_until: number | null;
  dispatched_at: number | null;
  dispatch_count: number;
  created_at: number;
  completed_at: number | null;
  last_error: string | null;
  result: string | null;
};

export class TenantObject extends DurableObject<Env> {
  private readonly sql: SqlStorage;
  /** Lease deadlines extended by renewals since the last persisted write. */
  private readonly leases = new Map<string, number>();
  private readonly redispatched = new Map<string, number>();
  private readonly wokeAt = Date.now();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
      CREATE TABLE IF NOT EXISTS action_outbox(
        id TEXT PRIMARY KEY, path TEXT NOT NULL, args TEXT NOT NULL,
        runner TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        owner TEXT, dispatched_at INTEGER, dispatch_count INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL, completed_at INTEGER, last_error TEXT, result TEXT);
      CREATE INDEX IF NOT EXISTS action_outbox_open ON action_outbox(status) WHERE status IN ('pending','processing');
      CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY, created_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS effects(action_id TEXT PRIMARY KEY, attempt INTEGER NOT NULL, applied_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS counter(k TEXT PRIMARY KEY, n INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT, action_id TEXT NOT NULL,
        at INTEGER NOT NULL, kind TEXT NOT NULL, attempt INTEGER, detail TEXT);
    `);
    try {
      this.sql.exec("ALTER TABLE action_outbox ADD COLUMN lease_until INTEGER");
    } catch {
      // Column already present.
    }
  }

  private get leaseMs() {
    return Number(this.env.LEASE_MS ?? 300_000);
  }
  private get renewMs() {
    return Number(this.env.RENEW_MS ?? 30_000);
  }
  private get maxAttempts() {
    return Number(this.env.MAX_ATTEMPTS ?? 8);
  }

  /** Spike instrumentation only. Production keeps none of these rows. */
  private event(id: string, kind: string, attempt: number | null, detail?: unknown) {
    this.sql.exec(
      "INSERT INTO events(action_id,at,kind,attempt,detail) VALUES (?,?,?,?,?)",
      id, Date.now(), kind, attempt, detail === undefined ? null : JSON.stringify(detail),
    );
  }

  private row(id: string): Row | undefined {
    return this.sql.exec<Row>("SELECT * FROM action_outbox WHERE id=?", id).toArray()[0];
  }

  /** The later of the persisted deadline and one extended in memory since. */
  private leaseUntil(row: Row): number {
    return Math.max(this.leases.get(row.id) ?? 0, row.lease_until ?? 0);
  }

  /** Expires a lease now, in memory and on disk, so another owner may claim. */
  private releaseLease(id: string) {
    this.leases.set(id, 0);
    this.sql.exec("UPDATE action_outbox SET lease_until=0 WHERE id=?", id);
  }

  private async scheduleSweep(at: number) {
    const current = await this.ctx.storage.getAlarm();
    if (current === null || current > at) await this.ctx.storage.setAlarm(at);
  }

  /**
   * The Reducer that queues the Action: one transaction writes the caller's
   * row and the outbox row. The hand-off happens after commit.
   */
  async enqueue(tenant: string, runner: Runner, path: string, args: unknown, skipHandoff = false) {
    const id = crypto.randomUUID();
    const now = Date.now();
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("INSERT OR REPLACE INTO meta(k,v) VALUES ('tenant',?)", tenant);
      this.sql.exec("INSERT INTO requests(id,created_at) VALUES (?,?)", id, now);
      this.sql.exec(
        "INSERT INTO action_outbox(id,path,args,runner,status,created_at) VALUES (?,?,?,?, 'pending', ?)",
        id, path, JSON.stringify(args), runner, now,
      );
    });
    this.event(id, "enqueued", null, { runner });
    if (skipHandoff) {
      // Simulates a crash between commit and hand-off: only the sweep finds it.
      this.event(id, "handoff-skipped", null);
      await this.scheduleSweep(now + this.leaseMs);
    } else {
      await this.dispatch(this.row(id)!);
    }
    return { id, enqueuedAt: now };
  }

  private async dispatch(row: Row, cause = "commit") {
    const tenant = this.sql.exec<{ v: string }>("SELECT v FROM meta WHERE k='tenant'").one().v;
    const n = row.dispatch_count + 1;
    const started = Date.now();
    let handle: string;
    try {
      if (row.runner === "queue") {
        await this.env.ACTIONS.send({ tenant, id: row.id });
        handle = "queue";
      } else if (row.runner === "workflow") {
        const instance = await this.env.ACTION_WF.create({ id: `${row.id}-d${n}`, params: { tenant, id: row.id } });
        handle = instance.id;
      } else {
        const runner = this.env.RUNNER.get(this.env.RUNNER.idFromName(`${tenant}/${row.id}`));
        await runner.start(tenant, row.id, row.runner === "alarm-await" ? "await" : "heartbeat");
        handle = `runner:${tenant}/${row.id}`;
      }
    } catch (error) {
      this.event(row.id, "dispatch-failed", null, { cause, error: String(error) });
      await this.scheduleSweep(Date.now() + this.leaseMs);
      return;
    }
    this.sql.exec(
      "UPDATE action_outbox SET dispatched_at=?, dispatch_count=? WHERE id=?",
      Date.now(), n, row.id,
    );
    this.event(row.id, "dispatched", null, { cause, n, handle, handoffMs: Date.now() - started });
    await this.scheduleSweep(Date.now() + this.leaseMs);
  }

  async claim(id: string, owner: string): Promise<Claim> {
    const now = Date.now();
    const row = this.row(id);
    if (!row) return { ok: false, reason: "missing" };
    if (row.status === "done" || row.status === "dead") {
      this.event(id, "claim-refused", null, { owner, reason: row.status });
      return { ok: false, reason: row.status };
    }
    if (row.status === "processing" && row.owner !== owner) {
      const lease = this.leaseUntil(row);
      if (lease > now) {
        this.event(id, "claim-refused", null, { owner, reason: "leased", retryAfterMs: lease - now });
        return { ok: false, reason: "leased", retryAfterMs: lease - now };
      }
    }
    if (row.attempts >= this.maxAttempts) {
      this.sql.exec("UPDATE action_outbox SET status='dead' WHERE id=?", id);
      this.leases.delete(id);
      this.event(id, "dead", row.attempts, { owner });
      return { ok: false, reason: "dead" };
    }
    const attempt = row.attempts + 1;
    this.sql.exec(
      "UPDATE action_outbox SET status='processing', attempts=?, owner=?, lease_until=? WHERE id=?",
      attempt, owner, now + this.leaseMs, id,
    );
    this.leases.set(id, now + this.leaseMs);
    this.redispatched.delete(id);
    this.event(id, "claimed", attempt, { owner, takeover: row.status === "processing" });
    await this.scheduleSweep(now + this.leaseMs);
    return { ok: true, attempt, path: row.path, args: row.args, leaseMs: this.leaseMs, renewMs: this.renewMs };
  }

  /**
   * Only the current attempt may extend its lease. Writes `lease_until` only
   * when less than half of the persisted lease is left, so a tenant restart
   * never shortens a live lease below half its length.
   */
  renew(id: string, attempt: number): boolean {
    const row = this.row(id);
    const ok = row?.status === "processing" && row.attempts === attempt;
    let persisted = false;
    if (ok) {
      const now = Date.now();
      this.leases.set(id, now + this.leaseMs);
      if ((row.lease_until ?? 0) < now + this.leaseMs / 2) {
        this.sql.exec("UPDATE action_outbox SET lease_until=? WHERE id=?", now + this.leaseMs, id);
        persisted = true;
      }
    }
    this.event(id, ok ? "renewed" : "renew-refused", attempt, persisted ? { persisted } : undefined);
    return ok;
  }

  /** A failed attempt releases its lease so the platform retry can claim at once. */
  fail(id: string, attempt: number, error: string) {
    const row = this.row(id);
    if (row?.status !== "processing" || row.attempts !== attempt) return false;
    this.sql.exec("UPDATE action_outbox SET last_error=? WHERE id=?", error.slice(0, 2000), id);
    this.releaseLease(id);
    this.event(id, "failed", attempt, { error: error.slice(0, 300) });
    return true;
  }

  /**
   * The Reducer-like result application. One transaction: fence on the
   * attempt, write the effect, mark the row done. A replay of an applied
   * result is a no-op; a stale attempt is rejected.
   */
  complete(id: string, attempt: number, result: unknown): Completion {
    const outcome = this.ctx.storage.transactionSync((): Completion => {
      const row = this.row(id);
      if (row?.status === "done") return { applied: false, duplicate: true, appliedAttempt: row.attempts };
      if (row?.status !== "processing" || row.attempts !== attempt) return { applied: false, rejected: true };
      const now = Date.now();
      this.sql.exec("INSERT INTO effects(action_id,attempt,applied_at) VALUES (?,?,?)", id, attempt, now);
      this.sql.exec("INSERT INTO counter(k,n) VALUES ('applied',1) ON CONFLICT(k) DO UPDATE SET n=n+1");
      this.sql.exec(
        "UPDATE action_outbox SET status='done', completed_at=?, result=? WHERE id=?",
        now, JSON.stringify(result).slice(0, 500_000), id,
      );
      return { applied: true, appliedAttempt: attempt };
    });
    if (outcome.applied) this.leases.delete(id);
    this.event(
      id,
      outcome.applied ? "complete-applied" : outcome.duplicate ? "complete-duplicate" : "complete-rejected",
      attempt,
    );
    return outcome;
  }

  /** Recovers lost work: expired leases and hand-offs that never arrived. */
  override async alarm() {
    const now = Date.now();
    let next = Infinity;
    const open = this.sql
      .exec<Row>("SELECT * FROM action_outbox WHERE status IN ('pending','processing')")
      .toArray();
    for (const row of open) {
      const since = this.redispatched.get(row.id);
      if (since !== undefined && since + this.leaseMs > now) {
        next = Math.min(next, since + this.leaseMs);
        continue;
      }
      const due =
        row.status === "processing"
          ? this.leaseUntil(row)
          : (row.dispatched_at ?? row.created_at) + this.leaseMs;
      if (due > now) {
        next = Math.min(next, due);
        continue;
      }
      // A lease released by `fail` has deadline 0: report it as released.
      this.event(row.id, "sweep-redispatch", row.attempts, { status: row.status, overdueMs: due > 0 ? now - due : "released" });
      if (row.status === "processing") this.releaseLease(row.id);
      this.redispatched.set(row.id, now);
      await this.dispatch(row, "sweep");
      next = Math.min(next, now + this.leaseMs);
    }
    if (Number.isFinite(next)) await this.ctx.storage.setAlarm(next);
  }

  /**
   * Cancels an Action: the row becomes `dead`, so further claims are refused
   * and the running attempt's next renewal fails, which aborts it.
   */
  cancel(id: string) {
    const row = this.row(id);
    if (!row || row.status === "done" || row.status === "dead") return false;
    this.sql.exec("UPDATE action_outbox SET status='dead', last_error='cancelled', lease_until=0 WHERE id=?", id);
    this.leases.delete(id);
    this.event(id, "cancelled", row.attempts);
    return true;
  }

  /** Forces a second hand-off of the same row (duplicate-delivery test). */
  async redispatch(id: string) {
    const row = this.row(id);
    if (row) await this.dispatch(row, "manual");
    return Boolean(row);
  }

  inspect(id: string) {
    const row = this.row(id);
    const events = this.sql
      .exec("SELECT seq,at,kind,attempt,detail FROM events WHERE action_id=? ORDER BY seq", id)
      .toArray();
    const effects = this.sql.exec("SELECT * FROM effects WHERE action_id=?", id).toArray();
    return {
      row: row && { ...row, result: row.result ? JSON.parse(row.result) : null },
      leaseUntil: row ? this.leaseUntil(row) : null,
      effects,
      events,
      appliedTotal: this.sql.exec<{ n: number }>("SELECT COALESCE((SELECT n FROM counter WHERE k='applied'),0) AS n").one().n,
      wokeAt: this.wokeAt,
    };
  }

  /** Test hook: reset this object (a tenant crash or eviction). In-memory leases vanish. */
  crash() {
    this.ctx.abort("s4 tenant crash test");
  }
}
