// The scheduler's tables in the tenant object and the SQL surface it runs on.
//
// `gonvex_scheduled_jobs` is the Control Plane table moved into the tenant
// object (0012 R6): the same columns and indexes without `project_id` and
// `tenant_id` (CONTROL_SCHEMA_SQL at 0bf3ad13, ST/control_schema.sql:261-301),
// `status` gains `dead` and the row gains `last_error` (IB2), and one index
// serves lease expiry, because a Durable Object alarm must know when an
// abandoned claim becomes claimable again (Rust polls every 30 s instead).
//
// `_gonvex_action_outbox` stays byte for byte Rust's (K03 installs it and its
// receipt digest covers the DDL). Its CHECK allows only `pending` and
// `processing`, so an outbox Action that dies moves to `_gonvex_action_dead`
// in the same transaction (IB2; note 0034).
//
// Every statement binds the scheduler's clock instead of `unixepoch()`, so a
// simulated clock drives the same SQL (test/whagons-day.test.ts).

export const SCHEDULED_JOBS_SQL = `
CREATE TABLE IF NOT EXISTS gonvex_scheduled_jobs (
  id TEXT PRIMARY KEY,
  function_path TEXT NOT NULL, args JSON, run_at INTEGER NOT NULL,
  scheduled_for INTEGER NOT NULL, cron_name TEXT NOT NULL DEFAULT '',
  provenance JSON NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','completed','dead')),
  claim_sequence INTEGER NOT NULL DEFAULT 0, claim_token TEXT NOT NULL DEFAULT '',
  lease_until INTEGER,
  created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
  completed_at INTEGER, wake_key TEXT NOT NULL DEFAULT '',
  last_error TEXT
);
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_due_oneshot
  ON gonvex_scheduled_jobs(run_at,id) WHERE status='pending' AND cron_name='';
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_due_cron
  ON gonvex_scheduled_jobs(run_at,id) WHERE status='pending' AND cron_name<>'';
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_completed
  ON gonvex_scheduled_jobs(completed_at,id) WHERE status='completed';
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_claim_order
  ON gonvex_scheduled_jobs((cron_name<>''),run_at,id) WHERE status='pending';
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_scheduled_jobs_one_pending_cron
  ON gonvex_scheduled_jobs(cron_name) WHERE status='pending' AND cron_name<>'';
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_scheduled_jobs_one_pending_wake
  ON gonvex_scheduled_jobs(wake_key)
  WHERE status='pending' AND claim_token='' AND wake_key<>'';
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_leases
  ON gonvex_scheduled_jobs(lease_until) WHERE status='pending' AND claim_token<>'';
CREATE INDEX IF NOT EXISTS gonvex_scheduled_jobs_dead
  ON gonvex_scheduled_jobs(completed_at,id) WHERE status='dead';
`;

export const DEAD_ACTIONS_SQL = `
CREATE TABLE IF NOT EXISTS _gonvex_action_dead (
  id TEXT PRIMARY KEY,
  action_path TEXT NOT NULL,
  args JSON NOT NULL,
  actor_user_id TEXT,
  actor_email TEXT,
  provenance JSON NOT NULL DEFAULT '{}',
  attempts INTEGER NOT NULL,
  available_at INTEGER NOT NULL,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  dead_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS _gonvex_action_dead_by_time ON _gonvex_action_dead(dead_at,id);
`;

/** Objects this package adds to a tenant database (for A01's `addedObjects()` and the export tools). */
export const SCHEDULER_OBJECTS: readonly (readonly ["table" | "index", string])[] = [
  ["table", "gonvex_scheduled_jobs"],
  ["index", "gonvex_scheduled_jobs_due_oneshot"],
  ["index", "gonvex_scheduled_jobs_due_cron"],
  ["index", "gonvex_scheduled_jobs_completed"],
  ["index", "gonvex_scheduled_jobs_claim_order"],
  ["index", "gonvex_scheduled_jobs_one_pending_cron"],
  ["index", "gonvex_scheduled_jobs_one_pending_wake"],
  ["index", "gonvex_scheduled_jobs_leases"],
  ["index", "gonvex_scheduled_jobs_dead"],
  ["table", "_gonvex_action_dead"],
  ["index", "_gonvex_action_dead_by_time"],
];

/** What the scheduler needs from `ctx.storage`. */
export interface SchedulerStorage {
  readonly sql: SqlStorage;
  transactionSync<T>(closure: () => T): T;
}

export interface SqlCounters {
  statements: number;
  rowsRead: number;
  rowsWritten: number;
}

export const emptyCounters = (): SqlCounters => ({ statements: 0, rowsRead: 0, rowsWritten: 0 });

type Row = Record<string, SqlStorageValue>;

/** Kernel SQL with row counters. Never spans an await, so it cannot interleave with an invocation. */
export class SchedulerSql {
  readonly counters: SqlCounters = emptyCounters();

  constructor(readonly storage: SchedulerStorage) {}

  all<T extends Row = Row>(sql: string, ...binds: unknown[]): T[] {
    const cursor = this.storage.sql.exec<T>(sql, ...(binds as SqlStorageValue[]));
    const rows = cursor.toArray();
    this.count(cursor);
    return rows;
  }

  one<T extends Row = Row>(sql: string, ...binds: unknown[]): T | undefined {
    return this.all<T>(sql, ...binds)[0];
  }

  run(sql: string, ...binds: unknown[]): number {
    const cursor = this.storage.sql.exec(sql, ...(binds as SqlStorageValue[]));
    cursor.toArray();
    this.count(cursor);
    return cursor.rowsWritten;
  }

  transaction<T>(closure: () => T): T {
    return this.storage.transactionSync(closure);
  }

  private count(cursor: SqlStorageCursor<Row>): void {
    this.counters.statements++;
    this.counters.rowsRead += cursor.rowsRead;
    this.counters.rowsWritten += cursor.rowsWritten;
  }
}

/** Creates the scheduler's tables and indexes (idempotent). K02 runs it at provisioning; the scheduler once per wake. */
export function installScheduler(storage: SchedulerStorage): void {
  storage.transactionSync(() => {
    for (const script of [SCHEDULED_JOBS_SQL, DEAD_ACTIONS_SQL]) {
      for (const statement of script.split(";").map((part) => part.trim()).filter(Boolean)) storage.sql.exec(statement);
    }
  });
}
