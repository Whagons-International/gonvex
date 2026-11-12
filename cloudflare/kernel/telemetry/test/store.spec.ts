/**
 * `TelemetryStore` inside the real control object (K06 schema, DO SQLite):
 * envelope batches behave like Rust's event-by-event writes, counter maps
 * keep their order and cap, performance rows are de-duplicated, and every
 * table has its retention and per-project cap (0041 §2), on the write path
 * and on the hourly prune. Rows read and written are measured for the PR.
 */
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { OperatorControlObject } from "../../../control/operator/object.ts";
import { prepareEvents, type PerformanceRow } from "../envelope";
import { RETENTION, parseCounts } from "../store";
import type { TelemetryTestEnv } from "./worker";

const testEnv = env as unknown as TelemetryTestEnv;
const DAY = 86_400_000;
const member = (projectId: string, tenantId = "t1", accountId = "a1") => ({ projectId, tenantId, connectionId: "c1", accountId });

function inControl<T>(fn: (control: OperatorControlObject) => T | Promise<T>): Promise<T> {
  return runInDurableObject(testEnv.CONTROL.getByName("control"), (instance: OperatorControlObject) => fn(instance));
}

const perfRow = (eventId: string, extra: Partial<PerformanceRow> = {}): PerformanceRow => ({
  eventId, tenantId: "t1", accountId: "a1", kind: "query", path: "q", reason: "", outcome: "ok", error: "",
  clientSentAtMs: null, clientReceivedAtMs: 1, clientDurationMs: 5, trace: null, device: null, ...extra,
});

/** Inserts `n` rows with a recursive CTE (no statement per row). */
function bulk(control: OperatorControlObject, sql: string, n: number, ...args: unknown[]) {
  control.sql.run(`WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x<${n}) ${sql}`, ...args);
}
const count = (control: OperatorControlObject, table: string, projectId: string) =>
  Number(control.sql.all(`SELECT count(*) AS n FROM ${table} WHERE project_id=$1`, projectId)[0]!.n);

describe("error envelopes in one transaction", () => {
  it("equals Rust's event-by-event writes: counts, first/last seen, latest, duplicates and regression", async () => {
    const events = await prepareEvents(member("p-batch"), [
      { message: "m", name: "N", eventId: "e1", timestamp: "2025-01-01T00:00:00Z", release: "1", deviceId: "d1" },
      { message: "m", name: "N", eventId: "e1", timestamp: "2025-01-03T00:00:00Z", release: "x" },
      { message: "m", name: "N", eventId: "e2", timestamp: "2025-01-02T00:00:00Z", release: "2", deviceId: "d1", level: "warning" },
      { message: "other", eventId: "e3", timestamp: "2025-01-04T00:00:00Z" },
    ]);
    await inControl((control) => {
      const before = control.sql.mark();
      expect(control.telemetry.recordErrors("p-batch", events)).toEqual([true, false, true, true]);
      const after = control.sql.mark();
      console.log(`recordErrors, 4 events / 2 groups: rows read ${after.read - before.read}, written ${after.written - before.written}`);
      const group = control.sql.all("SELECT * FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2", "p-batch", events[0]!.fingerprint)[0]!;
      expect(group).toMatchObject({
        event_count: 2, first_seen: Date.UTC(2025, 0, 1), last_seen: Date.UTC(2025, 0, 2), level: "warning", culprit: "",
        releases: '{"1":1,"2":1}', devices: '{"d1":2}', tenants: '{"t1":2}', accounts: '{"a1":2}', environments: "{}", regression: 0,
      });
      expect(JSON.parse(String(group.latest_event)).eventId).toBe("e2");
      // A repeat of a stored id is not new; a resolved group regresses on its next event.
      expect(control.telemetry.recordErrors("p-batch", events.slice(0, 1))).toEqual([false]);
      control.sql.run("UPDATE gonvex_error_groups SET status='resolved' WHERE project_id=$1", "p-batch");
      expect(control.telemetry.recordErrors("p-batch", [{ ...events[0]!, eventId: "e9" }])).toEqual([true]);
      const regressed = control.sql.all("SELECT event_count,regression,status FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint=$2", "p-batch", events[0]!.fingerprint)[0]!;
      expect(regressed).toEqual({ event_count: 3, regression: 1, status: "resolved" });
    });
  });

  it("costs a bounded number of rows per envelope (the PR's numbers)", async () => {
    const one = await prepareEvents(member("p-cost"), [{ message: "one", eventId: "c1" }]);
    const twenty = await prepareEvents(member("p-cost"), Array.from({ length: 20 }, (_, i) => ({ message: `m${i % 4}`, eventId: `c20-${i}`, deviceId: `d${i}` })));
    await inControl((control) => {
      const measure = (label: string, fn: () => unknown) => {
        const before = control.sql.mark();
        fn();
        const after = control.sql.mark();
        console.log(`${label}: rows read ${after.read - before.read}, written ${after.written - before.written}`);
        return { read: after.read - before.read, written: after.written - before.written };
      };
      const first = measure("recordErrors, 1 event, new group", () => control.telemetry.recordErrors("p-cost", one));
      const again = measure("recordErrors, 1 known event id", () => control.telemetry.recordErrors("p-cost", one));
      const many = measure("recordErrors, 20 events, 4 groups", () => control.telemetry.recordErrors("p-cost", twenty));
      const perf = measure("recordPerformance, 50 rows", () => control.telemetry.recordPerformance("p-cost", Array.from({ length: 50 }, (_, i) => perfRow(`cost-${i}`))));
      // Flat in the batch: one insert, one group read, one upsert (plus index writes).
      expect(first.written).toBeLessThan(20);
      expect(again.written).toBe(0);
      expect(many.written).toBeLessThan(20 * 6);
      expect(perf.written).toBeLessThan(50 * 4);
    });
  });

  it("keeps counter maps in stored order and caps them at 1,000 keys", async () => {
    const events = await prepareEvents(member("p-counts"), Array.from({ length: 20 }, (_, i) => ({ message: "m", eventId: `e${i}`, deviceId: `d${i}` })));
    await inControl((control) => {
      control.sql.run(
        `INSERT INTO gonvex_error_groups(project_id,fingerprint,title,first_seen,last_seen,event_count,devices,latest_event)
         VALUES($1,$2,'m',0,0,1,(SELECT json_group_object('k'||x,1) FROM (WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x<995) SELECT x FROM s)),'{}')`,
        "p-counts", events[0]!.fingerprint,
      );
      control.telemetry.recordErrors("p-counts", events);
      const devices = parseCounts(String(control.sql.all("SELECT devices FROM gonvex_error_groups WHERE project_id=$1", "p-counts")[0]!.devices));
      expect(devices).toHaveLength(RETENTION.counterKeys);
      expect(devices.slice(0, 2).map(([key]) => key)).toEqual(["k1", "k2"]);
      expect(devices.slice(-5).map(([key]) => key)).toEqual(["d0", "d1", "d2", "d3", "d4"]);
    });
    expect(parseCounts('{"b":1, "a":"x","b":3,"c":{"n":[1,2]},"d":2.0}')).toEqual([["b", "3"], ["a", '"x"'], ["c", '{"n":[1,2]}'], ["d", "2.0"]]);
  });

  it("enforces the per-project event cap on the write path", async () => {
    await inControl(async (control) => {
      bulk(control, `INSERT INTO gonvex_error_events(project_id,event_id,fingerprint,occurred_at,payload) SELECT $1,'old-'||x,'f',$2+x,'{}' FROM seq`, RETENTION.errorEvents.maxRowsPerProject + 300, "p-cap", Date.now() - 2 * DAY);
      const events = await prepareEvents(member("p-cap"), [{ message: "new", eventId: "new-1" }]);
      control.telemetry.recordErrors("p-cap", events);
      expect(count(control, "gonvex_error_events", "p-cap")).toBe(RETENTION.errorEvents.maxRowsPerProject);
      // 20,301 rows: the 301 oldest went.
      expect(control.sql.all("SELECT event_id FROM gonvex_error_events WHERE project_id=$1 AND event_id IN ('old-1','old-301','old-302','new-1') ORDER BY event_id", "p-cap")).toEqual([{ event_id: "new-1" }, { event_id: "old-302" }]);
    });
  });
});

describe("performance events", () => {
  it("inserts a batch in one statement, ignores known ids, and measures rows", async () => {
    await inControl((control) => {
      const rows = Array.from({ length: 50 }, (_, i) => perfRow(`r${i}`, i === 0 ? { trace: '{"a":1}', device: '{"b":2}', clientSentAtMs: 0.5 } : {}));
      const before = control.sql.mark();
      expect(control.telemetry.recordPerformance("p-perf", rows)).toBe(50);
      const after = control.sql.mark();
      console.log(`recordPerformance, 50 rows: rows read ${after.read - before.read}, written ${after.written - before.written}`);
      expect(control.telemetry.recordPerformance("p-perf", [perfRow("r0"), perfRow("r50")])).toBe(1);
      const first = control.sql.all("SELECT * FROM gonvex_performance_events WHERE project_id=$1 AND event_id='r0'", "p-perf")[0]!;
      expect(first).toMatchObject({ trace: '{"a":1}', device: '{"b":2}', client_sent_at_ms: 0.5, client_duration_ms: 5, reason: "" });
    });
  });

  it("drops rows older than 14 days as it writes, and caps a project at 100,000 rows", async () => {
    await inControl((control) => {
      bulk(control, `INSERT INTO gonvex_performance_events(project_id,event_id,kind,path,outcome,client_received_at_ms,created_at) SELECT $1,'x'||x,'query','q','ok',1,$2+x FROM seq`, RETENTION.performance.maxRowsPerProject + 50, "p-perf-cap", Date.now() - DAY);
      bulk(control, `INSERT INTO gonvex_performance_events(project_id,event_id,kind,path,outcome,client_received_at_ms,created_at) SELECT $1,'old'||x,'query','q','ok',1,$2 FROM seq`, 10, "p-perf-cap", Date.now() - 15 * DAY);
      control.telemetry.recordPerformance("p-perf-cap", [perfRow("fresh")]);
      expect(count(control, "gonvex_performance_events", "p-perf-cap")).toBe(RETENTION.performance.maxRowsPerProject);
      expect(control.sql.all("SELECT count(*) AS n FROM gonvex_performance_events WHERE project_id=$1 AND (event_id LIKE 'old%' OR event_id='x1')", "p-perf-cap")[0]!.n).toBe(0);
      expect(control.sql.all("SELECT count(*) AS n FROM gonvex_performance_events WHERE project_id=$1 AND event_id='fresh'", "p-perf-cap")[0]!.n).toBe(1);
    });
  });
});

describe("the hourly prune", () => {
  it("applies every age limit and keeps live support sessions", async () => {
    await inControl(async (control) => {
      const now = Date.now();
      const p = "p-prune";
      control.sql.run(`INSERT INTO gonvex_performance_events(project_id,event_id,kind,path,outcome,client_received_at_ms,created_at) VALUES($1,'old','query','q','ok',1,$2),($1,'new','query','q','ok',1,$3)`, p, now - 15 * DAY, now - 13 * DAY);
      control.sql.run(`INSERT INTO gonvex_error_events(project_id,event_id,fingerprint,occurred_at,payload) VALUES($1,'old','f',$2,'{}'),($1,'new','f',$3,'{}')`, p, now - 31 * DAY, now - 29 * DAY);
      control.sql.run(`INSERT INTO gonvex_error_groups(project_id,fingerprint,title,first_seen,last_seen,latest_event) VALUES($1,'old','t',0,$2,'{}'),($1,'new','t',0,$3,'{}')`, p, now - 91 * DAY, now - 89 * DAY);
      control.sql.run(`INSERT INTO gonvex_runtime_reducer_logs(project_id,kind,entry,created_at) VALUES($1,'reducer','{}',$2),($1,'reducer','{"new":1}',$3)`, p, now - 15 * DAY, now - 13 * DAY);
      control.sql.run(`INSERT INTO gonvex_support_sessions(id,project_id,last_seen_at) VALUES('s-old',$1,$2),('s-live',$1,$2),('s-new',$1,$3)`, p, now - 31 * DAY, now - DAY);
      const report = await control.telemetry.prune(new Set(["s-live"]));
      // Other tests share this object's storage, so the report counts at least this project's rows.
      for (const key of ["performance", "errorEvents", "errorGroups", "reducerLogs", "supportSessions"] as const) expect(report[key], key).toBeGreaterThanOrEqual(1);
      expect(control.sql.all("SELECT event_id FROM gonvex_performance_events WHERE project_id=$1", p)).toEqual([{ event_id: "new" }]);
      expect(control.sql.all("SELECT event_id FROM gonvex_error_events WHERE project_id=$1", p)).toEqual([{ event_id: "new" }]);
      expect(control.sql.all("SELECT fingerprint FROM gonvex_error_groups WHERE project_id=$1", p)).toEqual([{ fingerprint: "new" }]);
      expect(control.sql.all("SELECT entry FROM gonvex_runtime_reducer_logs WHERE project_id=$1", p)).toEqual([{ entry: '{"new":1}' }]);
      expect(control.sql.all("SELECT id FROM gonvex_support_sessions WHERE project_id=$1 ORDER BY id", p)).toEqual([{ id: "s-live" }, { id: "s-new" }]);
    });
  });

  it("caps groups and Reducer log rows per project, oldest first", async () => {
    await inControl(async (control) => {
      const now = Date.now();
      bulk(control, `INSERT INTO gonvex_error_groups(project_id,fingerprint,title,first_seen,last_seen,latest_event) SELECT $1,'g'||x,'t',0,$2+x,'{}' FROM seq`, RETENTION.errorGroups.maxRowsPerProject + 7, "p-groups", now - DAY);
      bulk(control, `INSERT INTO gonvex_runtime_reducer_logs(project_id,kind,entry,created_at) SELECT $1,'reducer','{}',$2 FROM seq`, RETENTION.reducerLogs.maxRowsPerProject + 3, "p-logs", now);
      const report = await control.telemetry.prune();
      expect(report.errorGroups).toBeGreaterThanOrEqual(7);
      expect(report.reducerLogs).toBeGreaterThanOrEqual(3);
      expect(count(control, "gonvex_error_groups", "p-groups")).toBe(RETENTION.errorGroups.maxRowsPerProject);
      expect(control.sql.all("SELECT count(*) AS n FROM gonvex_error_groups WHERE project_id=$1 AND fingerprint IN ('g1','g7','g8')", "p-groups")[0]!.n).toBe(1);
      expect(count(control, "gonvex_runtime_reducer_logs", "p-logs")).toBe(RETENTION.reducerLogs.maxRowsPerProject);
    });
  });

  it("runs on the control object's alarm", async () => {
    await inControl(async (control) => {
      control.sql.run(`INSERT INTO gonvex_performance_events(project_id,event_id,kind,path,outcome,client_received_at_ms,created_at) VALUES('p-alarm','old','query','q','ok',1,$1)`, Date.now() - 20 * DAY);
      await control.alarm();
      expect(count(control, "gonvex_performance_events", "p-alarm")).toBe(0);
    });
  });
});

describe("error support sessions in the control object", () => {
  it("upserts, keeps a stored release when the new one is empty, and is live until the socket closes", async () => {
    await inControl(async (control) => {
      const input = { connectionId: "conn-x", projectId: "p-sessions", tenantId: "t", accountId: "a", release: "1.0", environment: "prod", live: true };
      expect(await control.telemetryRegisterSession(input)).toBe(true);
      expect(await control.telemetryRegisterSession({ ...input, release: "", environment: "" })).toBe(true);
      expect(control.sql.all("SELECT id,connection_id,release,environment FROM gonvex_support_sessions WHERE project_id='p-sessions'")).toEqual([
        { id: "conn-x", connection_id: "conn-x", release: "1.0", environment: "prod" },
      ]);
      expect([...control.supportLiveIds("p-sessions")]).toEqual(["conn-x"]);
      // A presence report for the same connection keeps the error session; the close forgets both.
      await control.connectionState("conn-x", { projectId: "p-sessions", tenantId: "t", authenticated: true, supportSessionId: "support_conn-x" });
      expect([...control.supportLiveIds("p-sessions")].sort()).toEqual(["conn-x", "support_conn-x"]);
      await control.connectionState("conn-x", null);
      expect(control.supportLiveIds("p-sessions").size).toBe(0);
    });
  });

  it("does not mark a session live while presence v2 is off, so no session leaks and the prune can take it", async () => {
    await inControl(async (control) => {
      // The gateway reports no close for it (presence off keeps no record), so it must never become live.
      const input = { connectionId: "conn-off", projectId: "p-sessions-off", tenantId: "t", accountId: "a", release: "1", environment: "prod", live: false };
      expect(await control.telemetryRegisterSession(input)).toBe(true);
      expect(await control.telemetryRegisterSession({ ...input, live: undefined as unknown as boolean })).toBe(true);
      expect(control.supportLiveIds("p-sessions-off").size).toBe(0);
      expect(control.sql.all("SELECT id FROM gonvex_support_sessions WHERE project_id='p-sessions-off'")).toEqual([{ id: "conn-off" }]);
      // 31 days later the hourly alarm prunes it (a live id would be kept).
      control.sql.run("UPDATE gonvex_support_sessions SET last_seen_at=$1 WHERE id='conn-off'", Date.now() - 31 * DAY);
      // K19's alarm runs the hourly prune only when it is due; an earlier test already ran it.
      (control as unknown as { ctx: DurableObjectState }).ctx.storage.kv.delete("k19/prune-due");
      await control.alarm();
      expect(control.sql.all("SELECT id FROM gonvex_support_sessions WHERE project_id='p-sessions-off'")).toEqual([]);
    });
  });
});
