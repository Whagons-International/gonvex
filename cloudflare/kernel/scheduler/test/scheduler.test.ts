// Scheduler behavior in a Durable Object (CONTRACT E.b, E.c.1, E.timers; IB2):
// promotion of Reducer-scheduled work, coalescing wakes, provenance and hops,
// the outbox drain with leases, backoff, the attempt cap and the dead state,
// dueWhen, cron sync, retention, the alarm.
import { describe, expect, it } from "vitest";
import { TenantStorage } from "../../storage/index.ts";
import { storageAlarm, TenantAlarm } from "../alarm.ts";
import { storageDueWhen } from "../due-when.ts";
import { SCHEDULE_OUTBOX_PATH } from "../outbox.ts";
import { durableHopsExceeded } from "../provenance.ts";
import { NO_LONGER_REGISTERED, Scheduler } from "../scheduler.ts";
import { parseCronSpecs, type CronSpec } from "../specs.ts";
import { cronJobId, retryDelayMs, wakeKey } from "../timing.ts";
import { harness, installTenantRuntime, Module, rows, SimulatedAlarm, withObject, type Harness } from "./helpers.ts";

const T0 = Date.UTC(2026, 10, 11, 9, 0, 0);
const kinds = { "jobs.reduce": "reducer", "jobs.act": "action", "jobs.query": "query" } as const;

/** A Reducer's provenance as Rust stores it with queued work (full InvocationProvenance JSON). */
function stored(overrides: Record<string, unknown> = {}) {
  return {
    channel: "ui", rootChannel: "ui", actorAccountId: "acct-1", actorMemberId: "member-1", onBehalfOfMemberId: null,
    rootCommandId: "cmd-root", commandId: "cmd-1", parentCommandId: null, agentExecutionId: null, threadId: null,
    turnId: null, toolCallId: null, artifactHash: "artifact-0", depth: 2, actionStack: [], ...overrides,
  };
}

/** What a Reducer's `ctx.scheduler.runAt/wakeAt` writes in Rust and in K04: an outbox row under the schedule path. */
function scheduleRow(h: Harness, id: string, payload: Record<string, unknown>, provenance: unknown = stored(), createdAt = h.clock.now) {
  h.state.storage.sql.exec(
    `INSERT INTO _gonvex_action_outbox (id, action_path, args, actor_user_id, actor_email, provenance, available_at, created_at) VALUES (?, ?, ?, 'acct-1', 'a@example.com', ?, ?, ?)`,
    id, SCHEDULE_OUTBOX_PATH, JSON.stringify(payload), JSON.stringify(provenance), createdAt, createdAt,
  );
}

/** What `ctx.actions.enqueue` writes. */
function actionRow(h: Harness, id: string, path: string, args: unknown, options: { actor?: string | null; provenance?: unknown; at?: number } = {}) {
  const at = options.at ?? h.clock.now;
  h.state.storage.sql.exec(
    `INSERT INTO _gonvex_action_outbox (id, action_path, args, actor_user_id, actor_email, provenance, available_at, created_at) VALUES (?, ?, ?, ?, NULL, ?, ?, ?)`,
    id, path, JSON.stringify(args), options.actor === undefined ? "acct-1" : options.actor, JSON.stringify(options.provenance ?? stored()), at, at,
  );
}

const jobs = (h: Harness) => rows<{ id: string; function_path: string; status: string; run_at: number; cron_name: string; claim_token: string; last_error: string | null; wake_key: string }>(
  h.state, "SELECT id, function_path, status, run_at, cron_name, claim_token, last_error, wake_key FROM gonvex_scheduled_jobs ORDER BY run_at, id");
const outbox = (h: Harness) => rows<{ id: string; status: string; attempts: number; available_at: number; last_error: string | null }>(
  h.state, "SELECT id, status, attempts, available_at, last_error FROM _gonvex_action_outbox ORDER BY created_at, id");
const dead = (h: Harness) => rows<{ id: string; attempts: number; last_error: string }>(h.state, "SELECT id, attempts, last_error FROM _gonvex_action_dead ORDER BY id");

describe("Reducer-scheduled work (runAfter, runAt, wakeAt)", () => {
  it("promotes schedule rows to jobs in one transaction and runs them at their time, with the Reducer's provenance one hop on", async () => {
    await withObject(async (state) => {
      const h = harness(state, { start: T0, module: new Module(kinds) });
      scheduleRow(h, "o1", { jobId: "job_a", function: "jobs.reduce", args: { n: 1 }, runAtUnixMs: T0 + 5_000, coalesce: false });
      scheduleRow(h, "o2", { jobId: "job_b", function: " jobs.act ", args: null, runAtUnixMs: T0 + 1_000 });
      await h.scheduler.notify();
      expect(h.port.at).toBe(T0);
      await h.runUntil(T0);
      expect(outbox(h)).toEqual([]);
      expect(jobs(h).map((j) => [j.id, j.function_path, j.status, j.run_at])).toEqual([
        ["job_b", "jobs.act", "pending", T0 + 1_000], ["job_a", "jobs.reduce", "pending", T0 + 5_000]]);
      expect(h.port.at).toBe(T0 + 1_000);
      await h.runUntil(T0 + 10_000);
      expect(h.runners.runs.map((r) => [r.run.id, r.kind, r.at])).toEqual([["job_b", "action", T0 + 1_000], ["job_a", "reducer", T0 + 5_000]]);
      const run = h.runners.runs[1]!.run;
      expect(run.actor).toEqual({ kind: "account", accountId: "acct-1", email: "" });
      expect(run.args).toEqual({ n: 1 });
      expect(run.provenance).toMatchObject({
        channel: "scheduler", rootChannel: "ui", rootCommandId: "cmd-root", parentCommandId: "cmd-1", commandId: "job_a",
        actorAccountId: "acct-1", actorMemberId: "member-1", depth: 0, durableHops: 1, artifactHash: "artifact-0",
      });
      expect(jobs(h).map((j) => j.status)).toEqual(["completed", "completed"]);
      // Only the retention of the completed history keeps an alarm.
      expect(h.port.at).toBe(T0 + 1_000 + 7 * 86_400_000);
    });
  });

  it("wakeAt coalesces to one pending run at the earliest time; a claimed wake leaves room for exactly one more", async () => {
    await withObject(async (state) => {
      const h = harness(state, { start: T0, module: new Module(kinds) });
      const wake = (id: string, at: number, args: unknown = { ids: [1, 2], tenant: "t" }) =>
        scheduleRow(h, `o-${id}`, { jobId: id, function: "jobs.act", args, runAtUnixMs: at, coalesce: true });
      wake("job_1", T0 + 60_000);
      wake("job_2", T0 + 90_000);
      wake("job_3", T0 + 20_000, { tenant: "t", ids: [1, 2] });
      wake("job_4", T0 + 70_000);
      scheduleRow(h, "o-plain", { jobId: "job_plain", function: "jobs.act", args: { ids: [1, 2], tenant: "t" }, runAtUnixMs: T0 + 60_000 });
      await h.scheduler.notify();
      await h.runUntil(T0);
      expect(jobs(h).map((j) => [j.id, j.run_at, j.wake_key])).toEqual([
        ["job_1", T0 + 20_000, wakeKey("jobs.act", { ids: [1, 2], tenant: "t" })], ["job_plain", T0 + 60_000, ""]]);
      // The wake runs as a long Action (accepted): while it holds its claim, new wakes queue one more run.
      h.runners.reply = () => ({ accepted: true });
      await h.runUntil(T0 + 20_000);
      expect(h.runners.runs.map((r) => r.run.id)).toEqual(["job_1"]);
      wake("job_5", T0 + 25_000);
      wake("job_6", T0 + 26_000);
      await h.scheduler.notify();
      await h.runUntil(T0 + 20_000);
      expect(jobs(h).filter((j) => j.status === "pending" && j.claim_token === "").map((j) => j.id)).toEqual(["job_5", "job_plain"]);
      // One scheduled run at a time: job_5 waits for job_1 to complete.
      await h.runUntil(T0 + 30_000);
      expect(h.runners.runs.map((r) => r.run.id)).toEqual(["job_1"]);
      h.runners.reply = () => ({ ok: true });
      const claim = h.runners.runs[0]!.run.claim;
      expect(h.scheduler.renew(claim)).toBe(true);
      expect(await h.scheduler.complete(claim, { ok: true })).toBe(true);
      expect(await h.scheduler.complete(claim, { ok: true })).toBe(false);
      await h.runUntil(T0 + 60_000);
      expect(h.runners.runs.map((r) => r.run.id)).toEqual(["job_1", "job_5", "job_plain"]);
      // Replaying the promotion of a job that already ran is a no-op.
      scheduleRow(h, "o-replay", { jobId: "job_1", function: "jobs.act", args: {}, runAtUnixMs: T0, coalesce: true });
      await h.scheduler.notify();
      await h.runUntil(T0 + 60_000);
      expect(jobs(h).map((j) => [j.id, j.status])).toEqual([["job_1", "completed"], ["job_5", "completed"], ["job_plain", "completed"]]);
    });
  });

  it("invalid schedule rows retry with backoff and die at the attempt cap; K04's provenance shape falls back to the system actor as in Rust", async () => {
    await withObject(async (state) => {
      const h = harness(state, { start: T0, module: new Module(kinds), maxAttempts: 3 });
      scheduleRow(h, "o-bad-id", { jobId: "nope", function: "jobs.reduce", runAtUnixMs: T0 });
      scheduleRow(h, "o-unknown", { jobId: "job_u", function: "jobs.gone", runAtUnixMs: T0 });
      scheduleRow(h, "o-query", { jobId: "job_q", function: "jobs.query", runAtUnixMs: T0 });
      scheduleRow(h, "o-k04", { jobId: "job_k", function: "jobs.reduce", args: {}, runAtUnixMs: T0, coalesce: false },
        { commandId: "cmd-1", rootCommandId: "cmd-1", rootChannel: "ui", channel: "ui", actorAccountId: "acct-1", actorMemberId: "m", onBehalfOfMemberId: null, agentExecutionId: null });
      await h.scheduler.notify();
      await h.runUntil(T0);
      expect(outbox(h).map((o) => [o.id, o.attempts, o.available_at - T0, o.last_error])).toEqual([
        ["o-bad-id", 1, 1_000, "scheduled outbox payload has an invalid jobId"],
        ["o-query", 1, 1_000, "only Reducers and Actions can be scheduled"],
        ["o-unknown", 1, 1_000, 'scheduled function "jobs.gone" is not registered'],
      ]);
      // The K04 row lacks artifactHash, so serde would refuse it: Rust stores direct system provenance and runs as the system.
      expect(h.runners.runs.map((r) => [r.run.id, r.run.actor])).toEqual([["job_k", { kind: "system" }]]);
      await h.runUntil(T0 + 1_000);
      expect(outbox(h).map((o) => [o.attempts, o.available_at - T0])).toEqual([[2, 3_000], [2, 3_000], [2, 3_000]]);
      await h.runUntil(T0 + 60_000);
      expect(outbox(h)).toEqual([]);
      expect(dead(h).map((d) => [d.id, d.attempts])).toEqual([["o-bad-id", 3], ["o-query", 3], ["o-unknown", 3]]);
    });
  });
});

describe("scheduled runs: dead state, hops, leases (IB2)", () => {
  it("a failed run ends dead with its error; a no-longer-registered function and a query die too; a cron still gets its next run", async () => {
    await withObject(async (state) => {
      const crons = parseCronSpecs([{ name: "tick", function: "jobs.reduce", scope: "tenant", args: { x: 1 }, intervalMs: 60_000 }]);
      const module = new Module({ ...kinds }, crons);
      const h = harness(state, { start: T0, module, project: "p", tenant: "t" });
      await h.scheduler.syncCrons();
      await h.scheduler.enqueue({ function: "jobs.reduce", args: {}, runAtUnixMs: T0 + 1, provenance: stored(), jobId: "job_fail" });
      await h.scheduler.enqueue({ function: "jobs.act", args: {}, runAtUnixMs: T0 + 2, provenance: null, jobId: "job_gone" });
      await h.scheduler.enqueue({ function: "jobs.reduce", args: {}, runAtUnixMs: T0 + 3, provenance: stored({ durableHops: 32 }), jobId: "job_hops" });
      h.runners.reply = (run) => (run.id === "job_fail" || run.cronName ? { ok: false, error: "boom" } : { ok: true });
      delete (module.kinds as Record<string, string>)["jobs.act"];
      await h.runUntil(T0 + 70_000);
      const byId = new Map(jobs(h).map((j) => [j.id, j]));
      expect(byId.get("job_fail")).toMatchObject({ status: "dead", last_error: "boom" });
      expect(byId.get("job_gone")).toMatchObject({ status: "dead", last_error: NO_LONGER_REGISTERED });
      expect(byId.get("job_hops")).toMatchObject({ status: "dead", last_error: durableHopsExceeded(32) });
      const cronRuns = jobs(h).filter((j) => j.cron_name === "tick");
      expect(cronRuns.map((j) => j.status)).toEqual(["dead", "pending"]);
      expect(cronRuns[1]!.id).toBe(cronJobId("p", "t", "tick", cronRuns[1]!.run_at));
      expect(h.runners.runs.map((r) => r.run.id)).toEqual(["job_fail", cronRuns[0]!.id]);
      // Operators see dead work and can requeue it; a dead cron run comes back once, as a one-shot.
      const listed = h.scheduler.listDead({ limit: 2 });
      expect(listed.items).toHaveLength(2);
      const rest = h.scheduler.listDead({ limit: 10, cursor: listed.cursor });
      expect([...listed.items, ...rest.items].map((i) => i.id).sort()).toEqual([cronRuns[0]!.id, "job_fail", "job_gone", "job_hops"].sort());
      expect(rest.cursor).toBeNull();
      h.runners.reply = () => ({ ok: true });
      expect(await h.scheduler.requeueDead(cronRuns[0]!.id)).toBe(true);
      expect(await h.scheduler.requeueDead("job_nope")).toBe(false);
      await h.runUntil(h.clock.now);
      expect(jobs(h).find((j) => j.id === cronRuns[0]!.id)).toMatchObject({ status: "completed", cron_name: "" });
      expect(jobs(h).filter((j) => j.cron_name === "tick" && j.status === "pending")).toHaveLength(1);
    });
  });

  it("an abandoned claim is re-run after its lease (at least once), and dies once its lease was lost maxAttempts times", async () => {
    await withObject(async (state) => {
      const h = harness(state, { start: T0, module: new Module(kinds), maxAttempts: 3 });
      await h.scheduler.enqueue({ function: "jobs.act", args: {}, runAtUnixMs: T0, provenance: null, jobId: "job_x" });
      h.runners.reply = () => ({ accepted: true }); // handed off, never reports back
      await h.runUntil(T0);
      expect(h.port.at).toBe(T0 + 120_000);
      await h.runUntil(T0 + 400_000);
      expect(h.runners.runs.map((r) => [r.at - T0, (r.run.claim as { token: string }).token])).toEqual([
        [0, "tenant-object:1"], [120_000, "tenant-object:2"], [240_000, "tenant-object:3"]]);
      expect(jobs(h)[0]).toMatchObject({ status: "dead", last_error: "scheduled run lost its lease 3 times without finishing" });
      // A zombie's late completion cannot touch the row.
      expect(await h.scheduler.complete(h.runners.runs[0]!.run.claim, { ok: true })).toBe(false);
    });
  });

  it("dueWhen: false skips the run (completed, next run queued), true runs it, an error fails open; $1 is the current time", async () => {
    await withObject(async (state) => {
      state.storage.sql.exec("CREATE TABLE due_flags (name TEXT PRIMARY KEY, due INTEGER, at INTEGER)");
      state.storage.sql.exec("INSERT INTO due_flags VALUES ('a', 0, NULL), ('b', 1, NULL)");
      const crons = parseCronSpecs([
        { name: "a", function: "jobs.reduce", scope: "tenant", intervalMs: 60_000, dueWhen: "SELECT due FROM due_flags WHERE name = 'a'" },
        { name: "b", function: "jobs.reduce", scope: "tenant", intervalMs: 60_000, dueWhen: "SELECT due FROM due_flags WHERE name = 'b' AND $1 > 0" },
        { name: "c", function: "jobs.reduce", scope: "tenant", intervalMs: 60_000, dueWhen: "SELECT missing FROM due_flags" },
        { name: "d", function: "jobs.reduce", scope: "tenant", intervalMs: 60_000, dueWhen: "SELECT $1 >= 0 AND typeof($1) = 'integer'" },
      ]);
      const h = harness(state, { start: T0, module: new Module(kinds, crons), dueWhen: storageDueWhen(new TenantStorage(state.storage)) });
      await h.scheduler.syncCrons();
      await h.runUntil(T0 + 60_000);
      expect(h.runners.runs.map((r) => r.run.cronName).sort()).toEqual(["b", "c", "d"]);
      expect(h.scheduler.stats.jobsSkipped).toBe(1);
      expect(h.events.filter((e) => e.type === "dueWhen").map((e) => (e as { cronName: string }).cronName)).toEqual(["c"]);
      expect(jobs(h).filter((j) => j.cron_name === "a").map((j) => j.status)).toEqual(["completed", "pending"]);
    });
  });
});

describe("the action outbox", () => {
  it("delivers in order, one at a time; failures back off 1 s, 2 s, 4 s…; the attempt cap moves the row to the dead table; requeue resets it", async () => {
    await withObject(async (state) => {
      const h = harness(state, { start: T0, module: new Module(kinds) });
      actionRow(h, "a1", "jobs.act", { n: 1 }, { at: T0 });
      actionRow(h, "a2", "jobs.act", { n: 2 }, { at: T0 + 1 });
      actionRow(h, "a3", "jobs.act", { n: 3 }, { actor: "_gonvex_system", provenance: "not provenance", at: T0 + 2 });
      h.runners.reply = (run) => (run.id === "a2" ? { ok: false, error: `nope ${run.claim.source === "outbox" ? run.claim.attempt : 0}` } : { ok: true });
      await h.scheduler.notify();
      await h.runUntil(T0 + 2);
      expect(h.runners.runs.map((r) => r.run.id)).toEqual(["a1", "a2", "a3"]);
      const [a1, , a3] = h.runners.runs.map((r) => r.run);
      expect(a1!.actor).toEqual({ kind: "account", accountId: "acct-1", email: "" });
      expect(a1!.provenance).toMatchObject({ channel: "system", commandId: "outbox-a1", parentCommandId: "cmd-1", durableHops: 1, depth: 0 });
      // Unparseable provenance: direct system provenance with one hop (RT/execution.rs deliver_outbox_action).
      expect(a3!.actor).toEqual({ kind: "system" });
      expect(a3!.provenance).toMatchObject({ channel: "system", rootChannel: "system", rootCommandId: "a3", parentCommandId: "a3", commandId: "outbox-a3", actorAccountId: "_gonvex_system", durableHops: 1, artifactHash: "" });
      expect(outbox(h).map((o) => [o.id, o.status, o.attempts, o.available_at - h.clock.now, o.last_error])).toEqual([["a2", "pending", 1, 1_000 - 1, "nope 1"]]);
      await h.runUntil(T0 + 3_600_000);
      const attempts = h.runners.runs.filter((r) => r.run.id === "a2").map((r) => r.at);
      expect(attempts).toHaveLength(8);
      expect(attempts.slice(1).map((at, i) => at - attempts[i]!)).toEqual([1, 2, 3, 4, 5, 6, 7].map(retryDelayMs));
      expect(outbox(h)).toEqual([]);
      expect(dead(h)).toEqual([{ id: "a2", attempts: 8, last_error: "nope 8" }]);
      expect(h.scheduler.listDead().items).toEqual([{ id: "a2", source: "outbox", function: "jobs.act", args: { n: 2 }, attempts: 8, lastError: "nope 8", deadAt: attempts[7] }]);
      h.runners.reply = () => ({ ok: true });
      expect(await h.scheduler.requeueDead("a2")).toBe(true);
      expect(outbox(h)).toMatchObject([{ id: "a2", status: "pending", attempts: 0 }]);
      await h.runUntil(h.clock.now);
      expect(outbox(h)).toEqual([]);
      expect(dead(h)).toEqual([]);
    });
  });

  it("an accepted Action holds the outbox until it completes or its lease runs out; renewals are attempt-guarded; the hop guard kills instead of dropping", async () => {
    await withObject(async (state) => {
      const h = harness(state, { start: T0, module: new Module(kinds) });
      actionRow(h, "long", "jobs.act", {}, { at: T0 });
      actionRow(h, "next", "jobs.act", {}, { at: T0 + 1 });
      actionRow(h, "loop", "jobs.act", {}, { at: T0 + 2, provenance: stored({ durableHops: 32 }) });
      h.runners.reply = (run) => (run.id === "long" ? { accepted: true } : { ok: true });
      await h.scheduler.notify();
      await h.runUntil(T0 + 10);
      expect(h.runners.runs.map((r) => r.run.id)).toEqual(["long"]);
      expect(h.port.at).toBe(T0 + 300_001);
      const first = h.runners.runs[0]!.run.claim;
      h.clock.now = T0 + 150_000;
      expect(h.scheduler.renew(first)).toBe(true);
      // The lease ran out (renewal stopped): the row is claimed again with attempt 2, and the zombie cannot finish it.
      await h.runUntil(T0 + 450_001);
      expect(h.runners.runs.map((r) => [r.run.id, r.at - T0])).toEqual([["long", 0], ["long", 450_001]]);
      expect(h.scheduler.renew(first)).toBe(false);
      expect(await h.scheduler.complete(first, { ok: true })).toBe(false);
      expect(await h.scheduler.complete(h.runners.runs[1]!.run.claim, { ok: true })).toBe(true);
      await h.runUntil(T0 + 450_001);
      expect(h.runners.runs.map((r) => r.run.id)).toEqual(["long", "long", "next"]);
      expect(dead(h)).toEqual([{ id: "loop", attempts: 1, last_error: durableHopsExceeded(32) }]);
    });
  });
});

describe("cron sync and retention", () => {
  it("sync replaces every pending cron run, keeps one pending run per cron, and refuses what Rust refuses", async () => {
    await withObject(async (state) => {
      const specs: CronSpec[] = parseCronSpecs([
        { name: "m", function: "jobs.reduce", scope: "tenant", expression: "* * * * *" },
        { name: "h", function: "jobs.reduce", scope: "tenant", expression: "0 * * * *" },
      ]);
      const h = harness(state, { start: T0, module: new Module(kinds, specs), project: "p", tenant: "t" });
      expect(await h.scheduler.syncCrons()).toBe(2);
      expect(await h.scheduler.syncCrons()).toBe(2);
      expect(jobs(h).map((j) => j.cron_name).sort()).toEqual(["h", "m"]);
      await expect(h.scheduler.syncCrons([{ ...specs[0]!, expression: "* * * * 0" }])).rejects.toThrow(
        `cron "m" has invalid expression: 0 * * * * 0\n          ^\nDays of Week must be greater than or equal to 1. ('0' specified.)`);
      await expect(h.scheduler.syncCrons([{ ...specs[0]!, scope: "project" }], { tenantRoutes: 2 })).rejects.toThrow(
        'project-scoped crons require exactly one project database route; project "p" has 2 tenant routes');
      expect(jobs(h).map((j) => j.cron_name).sort()).toEqual(["h", "m"]);
      expect(await h.scheduler.syncCrons([specs[1]!])).toBe(1);
      expect(jobs(h).map((j) => j.cron_name)).toEqual(["h"]);
      // Missed runs are skipped, never replayed: after three days asleep, one run, then the next one on the lattice.
      h.clock.now = T0 + 3 * 86_400_000;
      await h.runUntil(h.clock.now);
      expect(h.runners.runs).toHaveLength(1);
      const pending = jobs(h).filter((j) => j.status === "pending");
      expect(pending).toHaveLength(1);
      expect(pending[0]!.run_at).toBeGreaterThan(h.clock.now);
      expect(pending[0]!.run_at).toBeLessThanOrEqual(h.clock.now + 3_600_000);
    });
  });

  it("completed jobs are pruned after 7 days in batches; dead jobs are kept", async () => {
    await withObject(async (state) => {
      const h = harness(state, { start: T0, module: new Module(kinds) });
      h.runners.reply = (run) => (run.id === "job_dead" ? { ok: false, error: "x" } : { ok: true });
      for (const id of ["job_1", "job_2", "job_dead"]) await h.scheduler.enqueue({ function: "jobs.reduce", args: {}, runAtUnixMs: T0, provenance: null, jobId: id });
      await h.runUntil(T0);
      expect(h.port.at).toBe(T0 + 7 * 86_400_000);
      await h.runUntil(T0 + 8 * 86_400_000);
      expect(jobs(h).map((j) => [j.id, j.status])).toEqual([["job_dead", "dead"]]);
      expect(h.port.at).toBeNull();
    });
  });
});

describe("the tenant alarm", () => {
  it("sets the object's alarm to the earliest due work, only moves it earlier on arm, and clears it when nothing is pending", async () => {
    await withObject(async (state) => {
      installTenantRuntime(state);
      // Real object alarms: keep them in the future so the platform never fires them during the test.
      const T0 = Date.now() + 3_600_000;
      let now = T0;
      const alarm = new TenantAlarm(storageAlarm(state.storage), { clock: () => now });
      const runs: string[] = [];
      const scheduler = new Scheduler({
        storage: state.storage, project: "p", tenant: "t", module: new Module(kinds), clock: () => now,
        reducers: { runReducer: async (run) => { runs.push(run.id); return { ok: true }; } },
        actions: { startAction: async () => ({ ok: true }) },
      });
      scheduler.attach(alarm);
      expect(await state.storage.getAlarm()).toBeNull();
      await scheduler.enqueue({ function: "jobs.reduce", args: {}, runAtUnixMs: T0 + 50_000, provenance: null, jobId: "job_late" });
      expect(await state.storage.getAlarm()).toBe(T0 + 50_000);
      await scheduler.enqueue({ function: "jobs.reduce", args: {}, runAtUnixMs: T0 + 10_000, provenance: null, jobId: "job_soon" });
      expect(await state.storage.getAlarm()).toBe(T0 + 10_000);
      now = T0 + 10_000;
      const report = await alarm.fire();
      expect(report.sources.map((s) => [s.name, s.units])).toEqual([["scheduler.jobs", 1]]);
      expect(await state.storage.getAlarm()).toBe(T0 + 50_000);
      now = T0 + 50_000;
      await alarm.fire();
      // Completed history keeps the retention alarm (7 days); nothing else is pending.
      expect(await state.storage.getAlarm()).toBe(T0 + 10_000 + 7 * 86_400_000);
      expect(runs).toEqual(["job_soon", "job_late"]);
      await state.storage.deleteAlarm();
    });
  });

  it("a source that throws is retried with backoff and does not stop the others", async () => {
    await withObject(async (state) => {
      void state;
      let now = T0;
      const port = new SimulatedAlarm();
      const alarm = new TenantAlarm(port, { clock: () => now, onError: () => {} });
      let fineDone = false;
      alarm.register({ name: "broken", nextAt: () => T0, run: () => { throw new Error("down"); } });
      alarm.register({ name: "fine", nextAt: () => (fineDone ? null : T0), run: () => { fineDone = true; return 1; } });
      const report = await alarm.fire();
      expect(report.sources).toEqual([{ name: "broken", units: 0, error: "down" }, { name: "fine", units: 1 }]);
      expect(report.next).toBe(T0 + 1_000);
      now = T0 + 1_000;
      expect((await alarm.fire()).next).toBe(T0 + 3_000);
      expect(port.at).toBe(T0 + 3_000);
    });
  });
});

describe("plans", () => {
  it("claims and next-due probes use indexes, not a scan of the job history", async () => {
    await withObject(async (state) => {
      const h = harness(state, { start: T0, module: new Module(kinds) });
      state.storage.sql.exec(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
        INSERT INTO gonvex_scheduled_jobs (id, function_path, run_at, scheduled_for, status, completed_at, cron_name)
        SELECT 'done_' || i, 'jobs.reduce', ?1 - i * 1000, ?1 - i * 1000, 'completed', ?1 - i * 1000, CASE WHEN i % 2 THEN 'c' || i ELSE '' END FROM n`, T0);
      state.storage.sql.exec("ANALYZE gonvex_scheduled_jobs");
      const plan = (sql: string, ...binds: SqlStorageValue[]) => rows<{ detail: string }>(state, `EXPLAIN QUERY PLAN ${sql}`, ...binds).map((r) => r.detail);
      const scans = (details: string[]) => details.filter((d) => /^SCAN gonvex_scheduled_jobs(?! USING)/.test(d));
      expect(scans(plan(`SELECT id FROM gonvex_scheduled_jobs WHERE status='pending' AND run_at<=?1 AND (claim_token='' OR lease_until IS NULL OR lease_until<=?1)
        ORDER BY (cron_name<>'') ASC,run_at,id LIMIT 1`, T0))).toEqual([]);
      expect(scans(plan(`SELECT (SELECT run_at FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name='' AND claim_token='' ORDER BY run_at,id LIMIT 1),
        (SELECT run_at FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name<>'' AND claim_token='' ORDER BY run_at,id LIMIT 1),
        (SELECT lease_until FROM gonvex_scheduled_jobs WHERE status='pending' AND claim_token<>'' ORDER BY lease_until LIMIT 1)`))).toEqual([]);
      const before = h.scheduler.sql.counters.rowsRead;
      await h.scheduler.enqueue({ function: "jobs.reduce", args: {}, runAtUnixMs: T0, provenance: null, jobId: "job_now" });
      await h.runUntil(T0);
      expect(h.runners.runs).toHaveLength(1);
      expect(h.scheduler.sql.counters.rowsRead - before).toBeLessThan(100);
    });
  });
});
