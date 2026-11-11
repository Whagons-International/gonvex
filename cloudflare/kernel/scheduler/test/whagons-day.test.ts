// K11 done_when: "All 14 Whagons crons fire on schedule in a 24-hour
// simulated clock run."
//
// The 14 tenant crons of whagons5-client 2457bb65f (fixtures/whagons-crons.json)
// run in a real Durable Object's SQLite with the scheduler's own tables, on
// the one tenant alarm, with a simulated clock that jumps to each alarm. The
// expected schedule is the Rust runtime's: golden/rust-vectors.json
// `whagonsDays` replays RT/scheduler.rs sync_project and finish() for the
// same project, tenant and start, with the `cron` 0.15 crate. Every run must
// happen at exactly the time Rust schedules it, with Rust's job id, and
// nothing else may run.
import { describe, expect, it } from "vitest";
import { parseCronSpecs } from "../specs.ts";
import type { FunctionKind } from "../ports.ts";
import whagons from "./fixtures/whagons-crons.json";
import vectors from "./golden/rust-vectors.json";
import { harness, Module, rows, withObject } from "./helpers.ts";

const specs = parseCronSpecs(whagons.crons);
const kinds = whagons.kinds as Record<string, FunctionKind>;

describe("the 14 Whagons crons over a simulated day", () => {
  it("the fixture is Whagons' 14 tenant crons: 12 Reducers and 2 Actions", () => {
    expect(specs).toHaveLength(14);
    expect(specs.every((spec) => spec.scope === "tenant")).toBe(true);
    expect(Object.values(kinds).filter((kind) => kind === "action")).toHaveLength(2);
  });

  for (const day of vectors.whagonsDays) {
    it(`${day.tenant} from ${new Date(day.start).toISOString()}: every run on Rust's schedule, with Rust's job ids`, { timeout: 120_000 }, async () => {
      await withObject(async (state) => {
        const h = harness(state, { start: day.start, project: day.project, tenant: day.tenant, module: new Module(kinds, specs) });
        await h.scheduler.syncCrons();
        const startedAt = performance.now();
        const before = { ...h.scheduler.sql.counters };
        const events = await h.runUntil(day.end - 1);
        const elapsed = performance.now() - startedAt;

        // Each cron: exactly Rust's runs, at Rust's times, with Rust's ids, through the right runner.
        const byCron = new Map<string, { at: number; scheduledFor: number | null; id: string; kind: string }[]>();
        for (const { run, at, kind } of h.runners.runs) {
          const list = byCron.get(run.cronName ?? "") ?? [];
          list.push({ at, scheduledFor: run.scheduledFor, id: run.id, kind });
          byCron.set(run.cronName ?? "", list);
        }
        expect([...byCron.keys()].sort()).toEqual(specs.map((spec) => spec.name).sort());
        let total = 0;
        for (const expected of day.crons) {
          const spec = specs.find((s) => s.name === expected.name)!;
          const actual = byCron.get(expected.name) ?? [];
          expect({ cron: expected.name, runs: actual.map((r) => [r.scheduledFor, r.id]) }).toEqual({ cron: expected.name, runs: expected.runs });
          // Fired on schedule: the clock at the run is the run's due time (the alarm fires at the earliest due job).
          expect(actual.filter((r) => r.at !== r.scheduledFor)).toEqual([]);
          expect(new Set(actual.map((r) => r.kind))).toEqual(new Set([kinds[spec.function] === "action" ? "action" : "reducer"]));
          total += actual.length;
        }
        expect(h.runners.runs).toHaveLength(total);
        expect(total).toBe(6_153);

        // Each cron's next run is pending at Rust's time.
        const pending = rows<{ cron_name: string; run_at: number; status: string }>(state,
          "SELECT cron_name, run_at, status FROM gonvex_scheduled_jobs WHERE status='pending' ORDER BY cron_name");
        expect(pending.map((r) => [r.cron_name, r.run_at])).toEqual(
          [...day.crons].sort((a, b) => (a.name < b.name ? -1 : 1)).map((c) => [c.name, c.pendingAfter]));
        // Completed history is kept (7-day retention); nothing died.
        expect(rows(state, "SELECT count(*) AS n FROM gonvex_scheduled_jobs WHERE status='completed'")[0]).toEqual({ n: total });
        expect(h.scheduler.stats).toMatchObject({ jobRuns: total, jobsCompleted: total, jobsDead: 0 });

        // Provenance of a cron run: system actor, scheduler channel, the job id as command, one durable hop.
        const sample = h.runners.runs.find((r) => r.run.cronName === "tools.check-overdue-items")!.run;
        expect(sample.actor).toEqual({ kind: "system" });
        expect(sample.direct).toBe(false);
        expect(sample.provenance).toMatchObject({
          channel: "scheduler", rootChannel: "system", actorAccountId: "_gonvex_system", actorMemberId: "_gonvex_system",
          rootCommandId: sample.id, parentCommandId: sample.id, commandId: sample.id, artifactHash: "artifact-1", durableHops: 1, depth: 0,
        });
        expect(sample.args).toEqual({});

        const used = {
          statements: h.scheduler.sql.counters.statements - before.statements,
          rowsRead: h.scheduler.sql.counters.rowsRead - before.rowsRead,
          rowsWritten: h.scheduler.sql.counters.rowsWritten - before.rowsWritten,
        };
        console.log(`[K11 numbers] ${day.tenant}: ${total} runs, ${events} alarm events, ${h.port.sets} alarm sets; `
          + `${used.statements} statements, ${used.rowsRead} rows read, ${used.rowsWritten} rows written `
          + `(${(used.rowsRead / total).toFixed(1)} read, ${(used.rowsWritten / total).toFixed(1)} written per run); ${elapsed.toFixed(0)} ms`);
      });
    });
  }
});
