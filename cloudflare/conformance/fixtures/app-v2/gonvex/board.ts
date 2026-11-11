import { liveQuery, reducer, replicaCollection, schema, visibility } from "@gonvex/module-sdk";

/** Every tenant Member sees the board. */
export const boardVisibility = visibility({ table: "board", key: "id", sets: {}, where: { operator: "public" } });

// v2: rows gain `label` (migration 0002); the collection definition changes.

/** Replica Collection: the whole board. */
export const all = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({ id: schema.id("board"), title: schema.string(), position: schema.integer(), bumps: schema.integer(), label: schema.string() })),
  replica: { table: "board", key: "id", columns: ["id", "title", "position", "bumps", "label"], orderBy: "position", orderDirection: "asc", mode: "eager", maxRows: 1000 },
});

/** Live Query window over the board, ordered by position. */
export const page = liveQuery({
  args: schema.object({ offset: schema.integer(), limit: schema.integer() }),
  result: schema.array(schema.object({ id: schema.id("board"), title: schema.string(), position: schema.integer(), bumps: schema.integer(), label: schema.string() })),
  liveQueryPlan: {
    table: "board",
    key: "id",
    columns: ["id", "title", "position", "bumps", "label"],
    sort: { columnArgument: "sortColumn", directionArgument: "sortDirection", defaultColumn: "position", defaultDirection: "asc", allowedColumns: ["position"] },
    window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 3, maxLimit: 50 },
  },
  run: async () => [],
});

export const add = reducer({
  args: schema.object({ id: schema.string(), title: schema.string(), position: schema.integer() }),
  result: schema.object({ id: schema.id("board"), title: schema.string(), position: schema.integer(), bumps: schema.integer(), label: schema.string() }),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => db.insert("board", { id: args.id, title: args.title, position: args.position, bumps: 0, label: "v2" }),
});

export const rename = reducer({
  args: schema.object({ id: schema.string(), title: schema.string() }),
  result: schema.object({ id: schema.id("board"), title: schema.string(), position: schema.integer(), bumps: schema.integer(), label: schema.string() }),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => db.update("board", args.id, { title: args.title }),
});

/** Increments a counter: replays of one call must apply once. */
export const bump = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.object({ bumps: schema.integer() }),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    const [current] = await db.query<{ bumps: number }>("SELECT bumps FROM board WHERE id = ?", [args.id]);
    if (!current) throw new Error("no such board row");
    await db.update("board", args.id, { bumps: current.bumps + 1 });
    return { bumps: current.bumps + 1 };
  },
});

export const remove = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => { await db.delete("board", args.id); return null; },
});

/** Reports which bundle generation answered (the upgrade scenario swaps it). */
export const generation = reducer({
  args: schema.object({}),
  result: schema.object({ generation: schema.integer() }),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async () => ({ generation: 2 }),
});
