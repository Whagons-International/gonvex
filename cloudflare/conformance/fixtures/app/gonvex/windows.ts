import { reducer, replicaCollection, schema, visibility } from "@gonvex/module-sdk";

// Replica windows (K10): every budget and filter a Replica Collection
// definition has, over one public table.

export const winItemsVisibility = visibility({ table: "win_items", key: "id", sets: {}, where: { operator: "public" } });

/** The whole table: large enough for several snapshot pages. */
export const winAll = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({ id: schema.string(), grp: schema.string(), rank: schema.integer(), label: schema.string() })),
  replica: { table: "win_items", key: "id", columns: ["id", "grp", "rank", "label"], orderBy: "rank", orderDirection: "asc", mode: "eager", maxRows: 5000 },
});

/** The three lowest ranks: a truncated window whose boundary moves. */
export const winTop = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({ id: schema.string(), rank: schema.integer(), label: schema.string() })),
  replica: { table: "win_items", key: "id", columns: ["id", "rank", "label"], orderBy: "rank", orderDirection: "asc", mode: "progressive", maxRows: 3 },
});

/** The highest ranks within 300 canonical bytes: truncated by bytes. */
export const winBytes = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({ id: schema.string(), rank: schema.integer(), label: schema.string() })),
  replica: { table: "win_items", key: "id", columns: ["id", "rank", "label"], orderBy: "rank", orderDirection: "desc", mode: "progressive", maxBytes: 300 },
});

/** One group's live items: an argument filter and an exclusion. */
export const winGroup = replicaCollection({
  args: schema.object({ grp: schema.string() }),
  result: schema.array(schema.object({ id: schema.string(), grp: schema.string(), rank: schema.integer(), label: schema.string() })),
  replica: { table: "win_items", key: "id", columns: ["id", "grp", "rank", "label"], orderBy: "rank", orderDirection: "asc", mode: "eager", maxRows: 1000, equalFilters: { grp: "grp" }, excludeWhenSet: ["archived_at"] },
});

/** Inserts `count` items `<prefix>-0000…` of one group, ranks from `from`, in one transaction. */
export const winFill = reducer({
  args: schema.object({ prefix: schema.string(), grp: schema.string(), from: schema.integer(), count: schema.integer() }),
  result: schema.object({ count: schema.integer() }),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    for (let i = 0; i < args.count; i++) {
      const n = String(i).padStart(4, "0");
      await db.insert("win_items", { id: `${args.prefix}-${n}`, grp: args.grp, rank: args.from + i, label: `item ${args.prefix} ${n} of a filled window` });
    }
    return { count: args.count };
  },
});

/** Like `winFill`, with labels padded to `pad` characters (snapshot size benchmarks). */
export const winFillLarge = reducer({
  args: schema.object({ prefix: schema.string(), grp: schema.string(), from: schema.integer(), count: schema.integer(), pad: schema.integer() }),
  result: schema.object({ count: schema.integer() }),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    const padding = "x".repeat(args.pad);
    for (let i = 0; i < args.count; i++) {
      const n = String(args.from + i).padStart(6, "0");
      await db.insert("win_items", { id: `${args.prefix}-${n}`, grp: args.grp, rank: args.from + i, label: `${n} ${padding}` });
    }
    return { count: args.count };
  },
});

/** Creates or rewrites one item. */
export const winPut = reducer({
  args: schema.object({ id: schema.string(), grp: schema.string(), rank: schema.integer(), label: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    const [current] = await db.query<{ id: string }>("SELECT id FROM win_items WHERE id = ?", [args.id]);
    if (current) await db.update("win_items", args.id, { grp: args.grp, rank: args.rank, label: args.label });
    else await db.insert("win_items", { id: args.id, grp: args.grp, rank: args.rank, label: args.label });
    return null;
  },
});

export const winArchive = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => { await db.update("win_items", args.id, { archived_at: 1 }); return null; },
});

export const winDrop = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => { await db.delete("win_items", args.id); return null; },
});
