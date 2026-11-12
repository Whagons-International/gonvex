/**
 * The telemetry tables of the control object: error events and groups,
 * performance events, error support sessions and the Reducer log table, with
 * the retention the reference lacks (contract-changes 0041).
 *
 * Writes keep the reference's statements and semantics
 * (`telemetry.rs` `persist_event`, `capture_performance_event`,
 * `register_error_session`) but run once per envelope or batch instead of once
 * per event: one `INSERT … SELECT FROM json_each` for the events, one read of
 * the groups they touch, one upsert of those groups, all in one storage
 * transaction. No statement runs per row and no statement binds more than
 * three parameters.
 *
 * The tables and their columns are the Control Plane schema's
 * (`rust/crates/store/src/control_schema.sql`), unchanged; this module adds
 * two indexes for retention (`gonvex_cf_*`).
 */
import type { Json } from "../session/decode";
import type { PerformanceRow, PreparedErrorEvent } from "./envelope";

/** What the store needs of the control object's SQL layer (`control/src/sql.ts`, `$N` placeholders). */
export interface TelemetrySql {
  all<T extends Record<string, SqlStorageValue> = Record<string, SqlStorageValue>>(query: string, ...args: unknown[]): T[];
  run(query: string, ...args: unknown[]): void;
  tx<T>(fn: () => T): T;
  script(text: string): void;
}

const DAY = 86_400_000;

/**
 * Retention and size caps (0041 §2). Rust bounds only performance events
 * (14 days, on every insert); every other table here grows without limit.
 */
export const RETENTION = {
  performance: { maxAgeMs: 14 * DAY, maxRowsPerProject: 100_000 },
  errorEvents: { maxAgeMs: 30 * DAY, maxRowsPerProject: 20_000 },
  errorGroups: { maxAgeMs: 90 * DAY, maxRowsPerProject: 5_000 },
  reducerLogs: { maxAgeMs: 14 * DAY, maxRowsPerProject: 10_000 },
  supportSessions: { maxAgeMs: 30 * DAY },
  /** Keys per counter map of a group (`tenants`, `releases`, `environments`, `accounts`, `devices`). */
  counterKeys: 1_000,
  /** Rows deleted per statement, and statements per table per prune. */
  page: 500,
  pagesPerRun: 40,
  /** Inserted rows per project between two cap checks on the write path. */
  capCheckEvery: { performance: 1_000, errorEvents: 200 },
} as const;

export const TELEMETRY_SCHEMA = `
CREATE INDEX IF NOT EXISTS gonvex_cf_error_events_age ON gonvex_error_events(project_id,occurred_at);
CREATE INDEX IF NOT EXISTS gonvex_cf_error_groups_age ON gonvex_error_groups(project_id,last_seen);
`;

const NOW = "CAST(unixepoch('subsec')*1000 AS INTEGER)";

// ---- counter maps in stored key order ------------------------------------------------------------

type Counts = Array<[key: string, raw: string]>;

/** Scans one JSON value starting at `i`; returns the index after it. */
function skipValue(text: string, i: number): number {
  let depth = 0;
  for (; i < text.length; i++) {
    const c = text[i];
    if (c === '"') {
      for (i++; i < text.length && text[i] !== '"'; i++) if (text[i] === "\\") i++;
      if (depth === 0) return i + 1;
    } else if (c === "{" || c === "[") depth++;
    else if (c === "}" || c === "]") {
      if (depth === 0) return i;
      depth--;
      if (depth === 0) return i + 1;
    } else if (c === "," && depth === 0) return i;
  }
  return i;
}

const isWs = (c: string | undefined) => c === " " || c === "\n" || c === "\r" || c === "\t";

/** A stored counter map as `[key, raw value text]` in its stored order (serde `preserve_order`). */
export function parseCounts(text: string | null | undefined): Counts {
  const out: Counts = [];
  if (!text) return out;
  let i = 0;
  const ws = () => { while (isWs(text[i])) i++; };
  ws();
  if (text[i] !== "{") return out;
  i++;
  ws();
  if (text[i] === "}") return out;
  while (i < text.length) {
    ws();
    const keyEnd = skipValue(text, i);
    const key = JSON.parse(text.slice(i, keyEnd)) as string;
    i = keyEnd;
    ws();
    if (text[i] !== ":") break;
    i++;
    ws();
    const valueEnd = skipValue(text, i);
    const raw = text.slice(i, valueEnd).trim();
    const existing = out.findIndex(([name]) => name === key);
    if (existing >= 0) out[existing] = [key, raw];
    else out.push([key, raw]);
    i = valueEnd;
    ws();
    if (text[i] !== ",") break;
    i++;
  }
  return out;
}

/** `increment`: `as_u64` (a non-negative integer literal, else 0) plus one; new keys go last, up to the cap. */
function increment(counts: Counts, index: Map<string, number>, key: string): void {
  if (key === "") return;
  const at = index.get(key);
  if (at !== undefined) {
    const raw = counts[at]![1];
    const current = /^\d+$/.test(raw) ? Number(raw) : 0;
    counts[at] = [key, String(current + 1)];
    return;
  }
  if (counts.length >= RETENTION.counterKeys) return;
  index.set(key, counts.length);
  counts.push([key, "1"]);
}

const serializeCounts = (counts: Counts) => `{${counts.map(([key, raw]) => `${JSON.stringify(key)}:${raw}`).join(",")}}`;

const COUNTER_COLUMNS = ["tenants", "releases", "environments", "accounts", "devices"] as const;
type CounterColumn = (typeof COUNTER_COLUMNS)[number];
const COUNTER_FIELD: Record<CounterColumn, keyof PreparedErrorEvent> = {
  tenants: "tenant", releases: "release", environments: "environment", accounts: "account", devices: "device",
};

// ---- the store ---------------------------------------------------------------------------------

export interface ErrorSessionInput {
  /** `gonvex_support_sessions.id` = the connection id (`register_error_session`). */
  connectionId: string;
  projectId: string;
  tenantId: string;
  accountId: string;
  /** Already bounded (200 / 100 chars); "" keeps the stored value. */
  release: string;
  environment: string;
  /**
   * Live until the socket closes (`support_register`). Only while presence v2
   * is on: Rust's hub registers nothing when it is off, and the gateway then
   * reports no close either. Absent: not live.
   */
  live?: boolean;
}

export interface PruneReport {
  performance: number;
  errorEvents: number;
  errorGroups: number;
  reducerLogs: number;
  supportSessions: number;
}

export class TelemetryStore {
  private readonly sinceCheck = { performance: new Map<string, number>(), errorEvents: new Map<string, number>() };

  constructor(private readonly sql: TelemetrySql, private readonly now: () => number = Date.now) {
    sql.script(TELEMETRY_SCHEMA);
  }

  /** `register_error_session`'s upsert of `gonvex_support_sessions` (the caller decides whether one is needed). */
  registerErrorSession(input: ErrorSessionInput): void {
    this.sql.tx(() =>
      this.sql.run(
        `INSERT INTO gonvex_support_sessions
           (id,project_id,tenant_id,account_id,connection_id,release,environment)
           VALUES($1,$2,$3,$4,$1,$5,$6)
           ON CONFLICT(id) DO UPDATE SET
             project_id=EXCLUDED.project_id,
             tenant_id=EXCLUDED.tenant_id,
             account_id=EXCLUDED.account_id,
             connection_id=EXCLUDED.connection_id,
             release=CASE WHEN EXCLUDED.release<>'' THEN EXCLUDED.release ELSE gonvex_support_sessions.release END,
             environment=CASE WHEN EXCLUDED.environment<>'' THEN EXCLUDED.environment ELSE gonvex_support_sessions.environment END,
             last_seen_at=${NOW}`,
        input.connectionId, input.projectId, input.tenantId, input.accountId, input.release, input.environment,
      ),
    );
  }

  /**
   * `persist_event` for each event of one envelope, in order, in one
   * transaction. Returns, per event, whether it was new (an event id the
   * project already has, or one repeated earlier in the envelope, is not).
   */
  recordErrors(projectId: string, events: readonly PreparedErrorEvent[]): boolean[] {
    if (events.length === 0) return [];
    const inserted = this.sql.tx(() => {
      const rows = events.map((e) => ({
        eventId: e.eventId, fingerprint: e.fingerprint, occurredAt: e.occurredAt, tenant: e.tenant, release: e.release,
        level: e.level, account: e.account, device: e.device, payload: e.payload,
      }));
      const created = new Set(
        this.sql
          .all<{ event_id: string }>(
            `INSERT INTO gonvex_error_events
               (project_id,event_id,fingerprint,occurred_at,tenant_id,release,level,account_id,device_id,payload)
             SELECT $1,json_extract(value,'$.eventId'),json_extract(value,'$.fingerprint'),json_extract(value,'$.occurredAt'),
                    json_extract(value,'$.tenant'),json_extract(value,'$.release'),json_extract(value,'$.level'),
                    json_extract(value,'$.account'),json_extract(value,'$.device'),json_extract(value,'$.payload')
             FROM json_each($2) WHERE true
             ON CONFLICT(project_id,event_id) DO NOTHING
             RETURNING event_id`,
            projectId, JSON.stringify(rows),
          )
          .map((row) => row.event_id),
      );
      const seen = new Set<string>();
      const fresh = events.map((e) => {
        const first = !seen.has(e.eventId);
        seen.add(e.eventId);
        return first && created.has(e.eventId);
      });
      const accepted = events.filter((_, i) => fresh[i]);
      if (accepted.length === 0) return fresh;

      const fingerprints = [...new Set(accepted.map((e) => e.fingerprint))];
      const existing = new Map(
        this.sql
          .all<Record<string, string>>(
            `SELECT fingerprint,tenants,releases,environments,accounts,devices FROM gonvex_error_groups
             WHERE project_id=$1 AND fingerprint IN (SELECT value FROM json_each($2))`,
            projectId, JSON.stringify(fingerprints),
          )
          .map((row) => [row.fingerprint!, row]),
      );
      interface Group {
        fingerprint: string; firstSeen: number; lastSeen: number; count: number; title: string; culprit: string; level: string; latest: string;
        counts: Record<CounterColumn, { counts: Counts; index: Map<string, number> }>;
      }
      const groups = new Map<string, Group>();
      for (const e of accepted) {
        let group = groups.get(e.fingerprint);
        if (!group) {
          const row = existing.get(e.fingerprint);
          const counts = {} as Group["counts"];
          for (const column of COUNTER_COLUMNS) {
            const parsed = parseCounts(row?.[column]);
            counts[column] = { counts: parsed, index: new Map(parsed.map(([key], i) => [key, i])) };
          }
          group = { fingerprint: e.fingerprint, firstSeen: e.occurredAt, lastSeen: e.occurredAt, count: 0, title: "", culprit: "", level: "", latest: "", counts };
          groups.set(e.fingerprint, group);
        }
        for (const column of COUNTER_COLUMNS) increment(group.counts[column].counts, group.counts[column].index, e[COUNTER_FIELD[column]] as string);
        group.count++;
        group.lastSeen = e.occurredAt;
        group.title = e.title;
        group.culprit = e.culprit;
        group.level = e.level;
        group.latest = e.payload;
      }
      const upserts = [...groups.values()].map((g) => ({
        fingerprint: g.fingerprint, title: g.title, culprit: g.culprit, level: g.level, firstSeen: g.firstSeen, lastSeen: g.lastSeen, count: g.count,
        tenants: serializeCounts(g.counts.tenants.counts), releases: serializeCounts(g.counts.releases.counts),
        environments: serializeCounts(g.counts.environments.counts), accounts: serializeCounts(g.counts.accounts.counts),
        devices: serializeCounts(g.counts.devices.counts), latest: g.latest,
      }));
      this.sql.run(
        `INSERT INTO gonvex_error_groups
           (project_id,fingerprint,title,culprit,level,first_seen,last_seen,event_count,
            tenants,releases,environments,accounts,devices,latest_event)
         SELECT $1,json_extract(value,'$.fingerprint'),json_extract(value,'$.title'),json_extract(value,'$.culprit'),
                json_extract(value,'$.level'),json_extract(value,'$.firstSeen'),json_extract(value,'$.lastSeen'),
                json_extract(value,'$.count'),json_extract(value,'$.tenants'),json_extract(value,'$.releases'),
                json_extract(value,'$.environments'),json_extract(value,'$.accounts'),json_extract(value,'$.devices'),
                json_extract(value,'$.latest')
         FROM json_each($2) WHERE true
         ON CONFLICT(project_id,fingerprint) DO UPDATE SET
           title=EXCLUDED.title,culprit=EXCLUDED.culprit,level=EXCLUDED.level,
           last_seen=EXCLUDED.last_seen,event_count=gonvex_error_groups.event_count+EXCLUDED.event_count,
           tenants=EXCLUDED.tenants,releases=EXCLUDED.releases,
           environments=EXCLUDED.environments,accounts=EXCLUDED.accounts,
           devices=EXCLUDED.devices,latest_event=EXCLUDED.latest_event,
           regression=(gonvex_error_groups.status='resolved')`,
        projectId, JSON.stringify(upserts),
      );
      return fresh;
    });
    const added = inserted.filter(Boolean).length;
    if (added > 0 && this.due("errorEvents", projectId, added)) this.capProject("errorEvents", projectId, RETENTION.pagesPerRun);
    return inserted;
  }

  /**
   * `capture_performance_event` for a batch: insert, ignoring event ids the
   * project already has, then Rust's 14-day delete for the project, bounded to
   * one page per write (the hourly prune takes the rest).
   */
  recordPerformance(projectId: string, rows: readonly PerformanceRow[]): number {
    if (rows.length === 0) return 0;
    const added = this.sql.tx(() => {
      const created = this.sql.all(
        `INSERT INTO gonvex_performance_events
           (project_id,event_id,tenant_id,account_id,kind,path,reason,outcome,error,
            client_sent_at_ms,client_received_at_ms,client_duration_ms,trace,device)
         SELECT $1,json_extract(value,'$.eventId'),json_extract(value,'$.tenantId'),json_extract(value,'$.accountId'),
                json_extract(value,'$.kind'),json_extract(value,'$.path'),json_extract(value,'$.reason'),
                json_extract(value,'$.outcome'),json_extract(value,'$.error'),json_extract(value,'$.clientSentAtMs'),
                json_extract(value,'$.clientReceivedAtMs'),json_extract(value,'$.clientDurationMs'),
                json_extract(value,'$.trace'),json_extract(value,'$.device')
         FROM json_each($2) WHERE true
         ON CONFLICT(project_id,event_id) DO NOTHING
         RETURNING 1`,
        projectId, JSON.stringify(rows),
      ).length;
      this.sql.run(
        `DELETE FROM gonvex_performance_events WHERE rowid IN (
           SELECT rowid FROM gonvex_performance_events WHERE project_id=$1 AND created_at<$2 LIMIT ${RETENTION.page})`,
        projectId, this.now() - RETENTION.performance.maxAgeMs,
      );
      return created;
    });
    if (added > 0 && this.due("performance", projectId, added)) this.capProject("performance", projectId, RETENTION.pagesPerRun);
    return added;
  }

  private due(store: "performance" | "errorEvents", projectId: string, added: number): boolean {
    const counters = this.sinceCheck[store];
    // A fresh object checks on its first write per project, then every `capCheckEvery` rows.
    const next = (counters.get(projectId) ?? RETENTION.capCheckEvery[store]) + added;
    if (next < RETENTION.capCheckEvery[store]) {
      counters.set(projectId, next);
      return false;
    }
    counters.set(projectId, 0);
    return true;
  }

  // ---- retention -------------------------------------------------------------------------------

  /** Deletes a project's oldest rows beyond its cap, at most `pages` statements. Returns rows deleted. */
  private capProject(store: "performance" | "errorEvents" | "errorGroups" | "reducerLogs", projectId: string, pages: number): number {
    const table = { performance: "gonvex_performance_events", errorEvents: "gonvex_error_events", errorGroups: "gonvex_error_groups", reducerLogs: "gonvex_runtime_reducer_logs" }[store];
    const order = { performance: "created_at", errorEvents: "occurred_at", errorGroups: "last_seen", reducerLogs: "id" }[store];
    const cap = RETENTION[store].maxRowsPerProject;
    // One index count, then the oldest rows beyond the cap, a page at a time.
    let excess = Number(this.sql.all<{ n: number }>(`SELECT count(*) AS n FROM ${table} WHERE project_id=$1`, projectId)[0]?.n ?? 0) - cap;
    let deleted = 0;
    for (let page = 0; page < pages && excess > 0; page++) {
      const limit = Math.min(RETENTION.page, excess);
      const n = this.sql.tx(() => this.sql.all(
        `DELETE FROM ${table} WHERE rowid IN (
           SELECT rowid FROM ${table} WHERE project_id=$1 ORDER BY ${order} ASC LIMIT ${limit})
         RETURNING 1`,
        projectId,
      ).length);
      deleted += n;
      excess -= n;
      if (n === 0) break;
    }
    return deleted;
  }

  /** Deletes rows older than `cutoff` in pages; returns rows deleted. */
  private async expire(table: string, column: string, cutoff: number, extra = "", args: unknown[] = []): Promise<number> {
    let deleted = 0;
    for (let page = 0; page < RETENTION.pagesPerRun; page++) {
      const n = this.sql.tx(() => this.sql.all(
        `DELETE FROM ${table} WHERE rowid IN (SELECT rowid FROM ${table} WHERE ${column}<$1 ${extra} LIMIT ${RETENTION.page}) RETURNING 1`,
        cutoff, ...args,
      ).length);
      deleted += n;
      if (n < RETENTION.page) break;
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    return deleted;
  }

  /**
   * The hourly prune (the control object's alarm): age limits, then per-project
   * caps, in pages of 500 with a yield between pages. `liveSessions` are the
   * support session ids of open sockets, which are never pruned.
   */
  async prune(liveSessions: ReadonlySet<string> = new Set()): Promise<PruneReport> {
    const now = this.now();
    const report: PruneReport = { performance: 0, errorEvents: 0, errorGroups: 0, reducerLogs: 0, supportSessions: 0 };
    report.performance += await this.expire("gonvex_performance_events", "created_at", now - RETENTION.performance.maxAgeMs);
    report.errorEvents += await this.expire("gonvex_error_events", "occurred_at", now - RETENTION.errorEvents.maxAgeMs);
    report.errorGroups += await this.expire("gonvex_error_groups", "last_seen", now - RETENTION.errorGroups.maxAgeMs);
    report.reducerLogs += await this.expire("gonvex_runtime_reducer_logs", "created_at", now - RETENTION.reducerLogs.maxAgeMs);
    report.supportSessions += await this.expire(
      "gonvex_support_sessions", "last_seen_at", now - RETENTION.supportSessions.maxAgeMs,
      "AND id NOT IN (SELECT value FROM json_each($2))", [JSON.stringify([...liveSessions])],
    );
    const caps = [
      ["performance", "gonvex_performance_events"],
      ["errorEvents", "gonvex_error_events"],
      ["errorGroups", "gonvex_error_groups"],
      ["reducerLogs", "gonvex_runtime_reducer_logs"],
    ] as const;
    for (const [store, table] of caps) {
      const over = this.sql.all<{ project_id: string }>(
        `SELECT project_id FROM ${table} GROUP BY project_id HAVING count(*)>${RETENTION[store].maxRowsPerProject}`,
      );
      for (const { project_id } of over) {
        report[store] += this.capProject(store, project_id, RETENTION.pagesPerRun);
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    }
    return report;
  }
}

export type { Json };
