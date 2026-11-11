import { reducer, schema } from "@gonvex/module-sdk";

// Scheduler (CONTRACT.md E.b, package K11): Reducer-scheduled work through the
// outbox and the scheduler, on the checksummed `board` table.

const JOB_ID = /^job_[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

/** Inserts a board row, then schedules: runAfter(0) tick, runAt(+300 ms) tick, and three identical wakeAt (+500 ms) that coalesce. */
export const plan = reducer({
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  args: schema.object({ id: schema.string() }),
  result: schema.any(),
  run: async (ctx, args) => {
    const c = ctx as unknown as Record<string, any>;
    await c.db.query("INSERT INTO board (id, title, position, bumps) VALUES ($1, 'planned', 100, 0)", [args.id]);
    const ids = [
      await c.scheduler.runAfter(0, "scheduler.tick", { id: args.id }),
      await c.scheduler.runAt(c.now + 300, "scheduler.tick", { id: args.id }),
      await c.scheduler.wakeAt(c.now + 500, "scheduler.wake", { id: args.id }),
      await c.scheduler.wakeAt(c.now + 400, "scheduler.wake", { id: args.id }),
      await c.scheduler.wakeAt(c.now + 600, "scheduler.wake", { id: args.id }),
    ];
    return { jobIds: ids.map((id: unknown) => typeof id === "string" && JOB_ID.test(id)), distinct: new Set(ids).size };
  },
});

export const tick = reducer({
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  args: schema.object({ id: schema.string() }),
  result: schema.any(),
  run: async (ctx, args) => {
    const c = ctx as unknown as Record<string, any>;
    await c.db.query("UPDATE board SET bumps = bumps + 1 WHERE id = $1", [args.id]);
    return null;
  },
});

/** A coalesced wake: appends once per run, so three wakeAt calls leave one "+wake". */
export const wake = reducer({
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  args: schema.object({ id: schema.string() }),
  result: schema.any(),
  run: async (ctx, args) => {
    const c = ctx as unknown as Record<string, any>;
    await c.db.query("UPDATE board SET title = title || '+wake' WHERE id = $1", [args.id]);
    return null;
  },
});

/** Reschedules itself forever: the durable hop limit (32) ends the chain. `position` records the last step that ran. */
export const chain = reducer({
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  args: schema.object({ id: schema.string(), n: schema.integer() }),
  result: schema.any(),
  run: async (ctx, args) => {
    const c = ctx as unknown as Record<string, any>;
    if (args.n === 0) await c.db.query("INSERT INTO board (id, title, position, bumps) VALUES ($1, 'chain', 0, 0)", [args.id]);
    else await c.db.query("UPDATE board SET position = $2, bumps = bumps + 1 WHERE id = $1", [args.id, args.n]);
    await c.scheduler.runAfter(0, "scheduler.chain", { id: args.id, n: args.n + 1 });
    return null;
  },
});

/** The two rows this scenario writes (read by a Reducer that writes nothing). */
export const rows = reducer({
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  args: schema.object({}),
  result: schema.any(),
  run: async (ctx) => {
    const c = ctx as unknown as Record<string, any>;
    return c.db.query("SELECT id, title, position, bumps FROM board WHERE id IN ('sched', 'chain') ORDER BY id");
  },
});
