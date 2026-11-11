import { query, reducer, schema } from "@gonvex/module-sdk";

// Module-host behavior (CONTRACT.md B, package K04): the ctx surface per kind,
// the pinned clock and seeded random, secure-entropy denial, Rust's host-call
// error texts, the aborted-transaction rule and module savepoints.

const errorText = async (run: () => unknown): Promise<string> => {
  try { await run(); return "no error"; } catch (error) { return error instanceof Error ? `${error.name}: ${error.message}` : String(error); }
};

const surface = (ctx: Record<string, unknown>) => ({
  keys: Object.keys(ctx).sort(),
  db: ctx.db ? Object.keys(ctx.db as object).sort() : null,
  typeofFetch: typeof (globalThis as { fetch?: unknown }).fetch,
  typeofPerformance: typeof (globalThis as { performance?: unknown }).performance,
});

/** A public one-shot Query with a handler: the Query dispatch path through the module host. */
export const querySurface = query({
  public: true,
  args: schema.object({}),
  result: schema.any(),
  run: async (ctx) => {
    const c = ctx as unknown as Record<string, any>;
    const before = Date.now();
    await c.db.query("SELECT count(*) AS n FROM board");
    return {
      ...surface(c),
      clockPinned: before === c.now && Date.now() === c.now && +new Date() === c.now,
      random: await errorText(() => Math.random()),
      entropy: await errorText(() => crypto.getRandomValues(new Uint8Array(4))),
      write: await errorText(() => c.db.query("DELETE FROM board")),
      permissions: await errorText(() => c.permissions.has("anything")),
      afterWrite: await errorText(() => c.db.query("SELECT 1 AS one")),
    };
  },
});

/** The Reducer surface, the clock and the seeded random sequence. */
export const reducerSurface = reducer({
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  args: schema.object({}),
  result: schema.any(),
  run: async (ctx) => {
    const c = ctx as unknown as Record<string, any>;
    let random: unknown;
    try { random = [Math.random(), Math.random(), Math.random()]; } catch (error) { random = (error as Error).message; }
    return {
      ...surface(c),
      clockPinned: Date.now() === c.now,
      random,
      entropy: await errorText(() => crypto.randomUUID()),
      storage: typeof c.storage,
      unknownSchedule: await errorText(() => c.scheduler.runAfter(1, "host.missing", {})),
      jobId: /^job_[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(await c.scheduler.runAt(c.now + 365 * 86_400_000, "board.add", { id: "never", title: "never", position: 0 })),
      composite: await errorText(() => c.db.update("board", 1.5, {})),
      identifier: await errorText(() => c.db.insert("bad name", {})),
    };
  },
});

/** A caught SQL error without ROLLBACK TO: the commit is refused (B.7). */
export const caughtSqlError = reducer({
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  args: schema.object({ id: schema.string() }),
  result: schema.any(),
  run: async ({ db }, args) => {
    await db.insert("board", { id: args.id, title: "first", position: 1, bumps: 0 });
    let caught = "";
    try { await db.insert("board", { id: args.id, title: "duplicate", position: 2, bumps: 0 }); } catch (error) { caught = (error as Error).message; }
    let next = "";
    try { await db.query("SELECT 1 AS one"); } catch (error) { next = (error as Error).message; }
    return { caught, next };
  },
});

/** Module savepoints: ROLLBACK TO recovers, RELEASE keeps the rest (0006). */
export const savepoints = reducer({
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  args: schema.object({ id: schema.string() }),
  result: schema.any(),
  run: async ({ db }, args) => {
    await db.query("SAVEPOINT attempt");
    await db.insert("board", { id: `${args.id}-inside`, title: "rolled back", position: 10, bumps: 0 });
    try { await db.insert("board", { id: `${args.id}-inside`, title: "duplicate", position: 11, bumps: 0 }); } catch { /* recovered below */ }
    await db.query("ROLLBACK TO attempt");
    await db.query("RELEASE attempt");
    await db.insert("board", { id: `${args.id}-after`, title: "kept", position: 12, bumps: 0 });
    return "recovered";
  },
});
