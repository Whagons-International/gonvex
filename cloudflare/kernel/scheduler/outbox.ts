// SQL over `_gonvex_action_outbox` and `_gonvex_action_dead` (ST/lib.rs
// outbox_due, claim_outbox, renew_action, complete_action, retry_action;
// RT/execution.rs promote_due_schedules; CONTRACT E.c.1, E.b.5). Synchronous;
// the caller wraps mutations in `SchedulerSql.transaction`.
import type { SchedulerSql } from "./sql.ts";

/** Reducer-scheduled work waits in the outbox under this path until it is promoted to a job (RT/host_calls.rs). */
export const SCHEDULE_OUTBOX_PATH = "_gonvex.scheduler.enqueue";

/** Promotions per turn (RT/execution.rs promote_due_schedules). */
export const PROMOTE_BATCH = 100;

export interface OutboxRow {
  id: string;
  path: string;
  args: unknown;
  /** `COALESCE(actor_user_id, '')`. */
  actorAccountId: string;
  actorEmail: string;
  /** Attempt number after this claim (`attempts AS next_attempt`). */
  attempts: number;
  provenance: unknown;
  availableAt: number;
  createdAt: number;
}

const parseJson = (text: SqlStorageValue | undefined): unknown => {
  if (text === null || text === undefined) return null;
  try {
    return JSON.parse(String(text));
  } catch {
    return undefined;
  }
};

function outboxRow(row: Record<string, SqlStorageValue>): OutboxRow {
  return {
    id: String(row["id"]), path: String(row["action_path"]), args: parseJson(row["args"]),
    actorAccountId: String(row["actor_account_id"] ?? ""), actorEmail: String(row["actor_email"] ?? ""),
    attempts: Number(row["attempts"]), provenance: parseJson(row["provenance"]),
    availableAt: Number(row["available_at"]), createdAt: Number(row["created_at"]),
  };
}

const COLUMNS = `id,action_path,args,COALESCE(actor_user_id,'') AS actor_account_id,COALESCE(actor_email,'') AS actor_email,attempts,provenance,available_at,created_at`;

/**
 * When the outbox next needs the alarm: the earliest pending `available_at`
 * or the earliest lease expiry of a row in `processing`.
 */
export function outboxNextDue(db: SchedulerSql, leaseMs: number): number | null {
  const row = db.one<{ next: number | null }>(
    `SELECT min(t) AS next FROM (
       SELECT (SELECT available_at FROM _gonvex_action_outbox WHERE status='pending' ORDER BY available_at,created_at LIMIT 1) AS t
       UNION ALL
       SELECT (SELECT locked_at+CAST(?1 AS INTEGER)+1 FROM _gonvex_action_outbox WHERE status='processing' ORDER BY locked_at LIMIT 1))`,
    leaseMs,
  );
  return row?.next ?? null;
}

/**
 * Due schedule rows (pending and due, or abandoned in `processing`), oldest
 * first. Promotion reads them and resolves each in the same transaction, so
 * the kernel never leaves one in `processing`; abandoned ones come from a
 * Rust export.
 */
export function dueScheduleRows(db: SchedulerSql, now: number, leaseMs: number, limit = PROMOTE_BATCH): OutboxRow[] {
  return db.all(
    `SELECT ${COLUMNS} FROM _gonvex_action_outbox
     WHERE action_path=?1 AND ((status='pending' AND available_at<=?2) OR (status='processing' AND locked_at<?2-CAST(?3 AS INTEGER)))
     ORDER BY available_at,created_at LIMIT CAST(?4 AS INTEGER)`,
    SCHEDULE_OUTBOX_PATH, now, leaseMs, limit,
  ).map(outboxRow);
}

/** Deletes promoted rows in one statement. */
export function deleteOutboxRows(db: SchedulerSql, ids: readonly string[]): void {
  if (ids.length === 0) return;
  db.run(`DELETE FROM _gonvex_action_outbox WHERE id IN (SELECT value FROM json_each(?1))`, JSON.stringify(ids));
}

/** A failed promotion: back to pending after the backoff, attempts counted (`retry_action`). One statement. */
export function retryOutboxRows(db: SchedulerSql, rows: readonly { id: string; attempts: number; delayMs: number; error: string }[], now: number): void {
  if (rows.length === 0) return;
  db.run(
    `UPDATE _gonvex_action_outbox SET status='pending',locked_at=NULL,
       attempts=CAST(json_extract(r.value,'$.n') AS INTEGER),
       available_at=CAST(?2 AS INTEGER)+CAST(json_extract(r.value,'$.d') AS INTEGER),
       last_error=json_extract(r.value,'$.e')
     FROM json_each(?1) AS r WHERE _gonvex_action_outbox.id=json_extract(r.value,'$.id')`,
    JSON.stringify(rows.map((row) => ({ id: row.id, n: row.attempts, d: row.delayMs, e: row.error }))), now,
  );
}

/** An Action (not a schedule row) is in `processing` with a live lease: one Action in flight per tenant. */
export function actionInFlight(db: SchedulerSql, now: number, leaseMs: number): boolean {
  return db.one(
    `SELECT 1 AS x FROM _gonvex_action_outbox WHERE status='processing' AND locked_at>=?1-CAST(?2 AS INTEGER) AND action_path<>?3 LIMIT 1`,
    now, leaseMs, SCHEDULE_OUTBOX_PATH,
  ) !== undefined;
}

/**
 * `claim_outbox(NotPath(schedule))`: the oldest due pending Action, or one
 * whose lease ran out, becomes `processing` with `attempts + 1`.
 */
export function claimAction(db: SchedulerSql, now: number, leaseMs: number): OutboxRow | undefined {
  const row = db.one(
    `UPDATE _gonvex_action_outbox SET status='processing',locked_at=CAST(?1 AS INTEGER),attempts=attempts+1
     WHERE id=(SELECT id FROM (
       SELECT * FROM (SELECT id,available_at,created_at FROM _gonvex_action_outbox
                      WHERE status='pending' AND available_at<=?1 AND action_path<>?3 ORDER BY available_at,created_at LIMIT 1)
       UNION ALL
       SELECT * FROM (SELECT id,available_at,created_at FROM _gonvex_action_outbox
                      WHERE status='processing' AND locked_at<?1-CAST(?2 AS INTEGER) AND action_path<>?3 ORDER BY available_at,created_at LIMIT 1))
       ORDER BY available_at,created_at LIMIT 1)
     RETURNING ${COLUMNS}`,
    now, leaseMs, SCHEDULE_OUTBOX_PATH,
  );
  return row ? outboxRow(row) : undefined;
}

/** `renew_action`: only the current delivery attempt may extend its claim. */
export function renewAction(db: SchedulerSql, id: string, attempt: number, now: number): boolean {
  return db.one(
    `UPDATE _gonvex_action_outbox SET locked_at=CAST(?3 AS INTEGER) WHERE id=?1 AND status='processing' AND attempts=?2 RETURNING id`,
    id, attempt, now,
  ) !== undefined;
}

/** `complete_action`, guarded by the attempt so a zombie delivery cannot complete a newer claim. */
export function completeAction(db: SchedulerSql, id: string, attempt: number): boolean {
  return db.one(`DELETE FROM _gonvex_action_outbox WHERE id=?1 AND status='processing' AND attempts=?2 RETURNING id`, id, attempt) !== undefined;
}

/** `retry_action` for the current attempt: pending again after the backoff. */
export function retryAction(db: SchedulerSql, id: string, attempt: number, delayMs: number, error: string, now: number): boolean {
  return db.one(
    `UPDATE _gonvex_action_outbox SET status='pending',locked_at=NULL,available_at=CAST(?3 AS INTEGER)+CAST(?4 AS INTEGER),last_error=?5
     WHERE id=?1 AND status='processing' AND attempts=?2 RETURNING id`,
    id, attempt, now, delayMs, error,
  ) !== undefined;
}

/**
 * Moves outbox rows to `_gonvex_action_dead` (IB2): kept for operators,
 * never run again unless requeued. `attempt` guards a claimed row; null
 * moves the row in whatever state it is. `attempts` overrides the recorded
 * count (a failed promotion counts its attempt without a claim).
 */
export function killActions(db: SchedulerSql, rows: readonly { id: string; attempt: number | null; error: string; attempts?: number }[], now: number): number {
  if (rows.length === 0) return 0;
  const payload = JSON.stringify(rows.map((row) => ({ id: row.id, n: row.attempt, e: row.error, a: row.attempts ?? null })));
  const moved = db.all<{ id: string }>(
    `INSERT INTO _gonvex_action_dead (id,action_path,args,actor_user_id,actor_email,provenance,attempts,available_at,last_error,created_at,dead_at)
     SELECT o.id,o.action_path,o.args,o.actor_user_id,o.actor_email,o.provenance,COALESCE(json_extract(r.value,'$.a'),o.attempts),o.available_at,json_extract(r.value,'$.e'),o.created_at,CAST(?2 AS INTEGER)
     FROM json_each(?1) AS r JOIN _gonvex_action_outbox o ON o.id=json_extract(r.value,'$.id')
     WHERE json_extract(r.value,'$.n') IS NULL OR (o.status='processing' AND o.attempts=json_extract(r.value,'$.n'))
     ON CONFLICT (id) DO UPDATE SET attempts=excluded.attempts,last_error=excluded.last_error,dead_at=excluded.dead_at
     RETURNING id`,
    payload, now,
  ).map((row) => row.id);
  if (moved.length > 0) deleteOutboxRows(db, moved);
  return moved.length;
}
