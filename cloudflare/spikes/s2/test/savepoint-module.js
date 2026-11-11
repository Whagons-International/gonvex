// Synthetic Reducers on the real @gonvex/module-sdk applyDataWrites, for the
// savepoint cases no Whagons Reducer triggers on purpose: a batch that fails
// mid-way and is caught, one that is not caught, nested savepoints with
// ROLLBACK TO and continue, and a caught SQL error without ROLLBACK TO
// (which the Rust store refuses to commit). Table: s2SavepointRows.
import { reducer, schema, applyDataWrites } from "@gonvex/module-sdk";

const table = "s2SavepointRows";
const insertBatch = (ids) => ({ kind: "insert", table, rows: ids.map((_id) => ({ _id, label: `batch:${_id}` })) });

export const caughtBatchFailure = reducer({
  args: schema.object({ prefix: schema.string() }), result: schema.any(), interactive: false,
  run: async (ctx, { prefix }) => {
    await ctx.db.insert(table, { _id: `${prefix}-before`, label: "before" });
    let caught = null;
    try {
      // The second insert collides with the row written above: UNIQUE fails
      // mid-batch, applyDataWrites rolls back to its savepoint and rethrows.
      await applyDataWrites(ctx, [insertBatch([`${prefix}-batch`]), insertBatch([`${prefix}-before`])]);
    } catch (error) { caught = String(error.message); }
    await ctx.db.insert(table, { _id: `${prefix}-after`, label: "after" });
    return { caught };
  },
});

export const uncaughtBatchFailure = reducer({
  args: schema.object({ prefix: schema.string() }), result: schema.any(), interactive: false,
  run: async (ctx, { prefix }) => {
    await ctx.db.insert(table, { _id: `${prefix}-before`, label: "before" });
    await applyDataWrites(ctx, [insertBatch([`${prefix}-batch`]), insertBatch([`${prefix}-before`])]);
    return "unreachable";
  },
});

export const nestedSavepoints = reducer({
  args: schema.object({ prefix: schema.string() }), result: schema.any(), interactive: false,
  run: async (ctx, { prefix }) => {
    await ctx.db.query("SAVEPOINT s2_outer");
    await ctx.db.insert(table, { _id: `${prefix}-discarded`, label: "inside outer" });
    await applyDataWrites(ctx, [insertBatch([`${prefix}-discarded-batch`])]); // nested SAVEPOINT + RELEASE
    await ctx.db.query("ROLLBACK TO s2_outer");
    await ctx.db.insert(table, { _id: `${prefix}-kept`, label: "after rollback to" });
    await ctx.db.query("RELEASE s2_outer");
    // Each savepoint is a nested storage transaction in the Durable Object,
    // which yields to the event loop. The pinned clock and the seeded
    // Math.random must still belong to this invocation afterwards.
    return {
      rows: (await ctx.db.query(`SELECT "_id" FROM "${table}" WHERE "_id" LIKE $1 ORDER BY "_id"`, [`${prefix}-%`])).map((row) => row._id),
      clockPinned: Date.now() === ctx.now && +new Date() === ctx.now,
      random: [Math.random(), Math.random()],
    };
  },
});

export const caughtSqlErrorWithoutRecovery = reducer({
  args: schema.object({ prefix: schema.string() }), result: schema.any(), interactive: false,
  run: async (ctx, { prefix }) => {
    await ctx.db.insert(table, { _id: `${prefix}-a`, label: "a" });
    try { await ctx.db.insert(table, { _id: `${prefix}-a`, label: "duplicate" }); } catch { /* swallowed */ }
    return "returned normally";
  },
});

export const s2Table = `CREATE TABLE IF NOT EXISTS "${table}" ("_id" TEXT PRIMARY KEY, "label" TEXT NOT NULL)`;
