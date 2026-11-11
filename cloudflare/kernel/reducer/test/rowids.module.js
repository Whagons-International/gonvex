// A local (offline-allowed, no optimistic handler) Reducer: the SDK wraps it in
// `reducerExecutionContext`, so inserts without an id get `reducerRowId` ids.
import { reducer, schema } from "@gonvex/module-sdk";

export const addLocal = reducer({
  args: schema.object({ title: schema.string() }),
  result: schema.any(),
  run: async ({ db }, args) => {
    const first = await db.insert("board", { title: args.title, position: 7, bumps: 0 });
    const second = await db.insert("board", { title: `${args.title} 2`, position: 8, bumps: 0 });
    return { ids: [first.id, second.id] };
  },
});

/** Grants and checks a permission: ctx.permissions answers from committed bits only. */
export const grantAndCheck = reducer({
  args: schema.object({ id: schema.string(), permission: schema.string() }),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "k07 test" },
  nonOptimisticReason: "k07 test",
  run: async ({ db, permissions, member }, args) => {
    const before = await permissions.has(args.permission);
    await db.insert("k07_grants", { _id: args.id, memberId: member.id, permission: args.permission, scope: "" });
    const after = await permissions.has(args.permission);
    return { before, after };
  },
});

export const check = reducer({
  args: schema.object({ permission: schema.string(), scope: schema.optional(schema.string()) }),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "k07 test" },
  nonOptimisticReason: "k07 test",
  run: async ({ permissions }, args) => permissions.has(args.permission, args.scope),
});

export const revoke = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "k07 test" },
  nonOptimisticReason: "k07 test",
  run: async ({ db }, args) => { await db.delete("k07_grants", args.id); return null; },
});

/** Enqueues an Action and schedules a Reducer: outbox rows commit with the Reducer. */
export const enqueue = reducer({
  args: schema.object({}),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "k07 test" },
  nonOptimisticReason: "k07 test",
  run: async ({ actions, scheduler }) => ({ action: await actions.enqueue("k07.notify", { n: 1 }), job: await scheduler.runAfter(60000, "board.add", { id: "later", title: "later", position: 9 }) }),
});

/** Writes a child row whose parent does not exist: the deferred FK fails the commit. */
export const orphan = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "k07 test" },
  nonOptimisticReason: "k07 test",
  run: async ({ db }, args) => { await db.query("INSERT INTO storage_children (id, parent_id) VALUES ($1, 'missing')", [args.id]); return null; },
});

/** Changes the caller's own Member row (role): the session must re-authenticate. */
export const promoteSelf = reducer({
  args: schema.object({ role: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "k07 test" },
  nonOptimisticReason: "k07 test",
  run: async ({ db, member }, args) => { await db.query("UPDATE members SET role = $1, membership_revision = membership_revision + 1 WHERE id = $2", [args.role, member.id]); return null; },
});

/** Returns `size` characters: a receipt larger than a Durable Object row (2 MB) cannot be stored. */
export const big = reducer({
  args: schema.object({ size: schema.integer() }),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "k07 test" },
  nonOptimisticReason: "k07 test",
  run: async ({ db }, args) => { await db.insert("board", { id: `big-${args.size}`, title: "big", position: 1, bumps: 0 }); return "x".repeat(args.size); },
});
