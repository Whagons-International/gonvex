import { applyDataWrites, existsDataRows, reducer, schema, selectRows, type ReducerContext } from "@gonvex/module-sdk";

// Module SQL against the tenant database: what the storage layer (K01) must
// reproduce. Each Reducer returns what the module saw, including the text of
// the errors it caught.

const caught = async (run: () => Promise<unknown>): Promise<string | null> => {
  try {
    await run();
    return null;
  } catch (error) {
    return String((error as Error)?.message ?? error);
  }
};

/** Placeholders by number, parameter types, decoding by declared type, refused statements. */
export const statements = reducer({
  args: schema.object({}),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }) => {
    const placeholders = await db.query("SELECT $2 AS b, $1 AS a, $2 || $1 AS ba, $1 AS again", ["one", "two"]);
    await db.query("INSERT INTO storage_values (n, id, label, meta, flag, data) VALUES ($5, $1, $2, $3, $4, x'00ff10')", ["v1", 3, JSON.stringify({ x: [1, 2] }), true, 7]);
    const stored = await db.query(
      "SELECT label, typeof(label) AS label_type, meta, flag, n, data, json_object('n', $1) AS built, $1 / 2 AS half, json_group_array(id) OVER () AS ids FROM storage_values WHERE id = $2",
      [3, "v1"],
    );
    const returning = await db.query("INSERT INTO storage_values (id, label, meta, flag) VALUES ($1, $2, $3, $4) RETURNING id, label, meta, flag", ["v2", "[1]", '["x"]', false]);
    const refused = {
      batch: await caught(() => db.query("SELECT 1; DELETE FROM storage_values")),
      attach: await caught(() => db.query("ATTACH DATABASE ':memory:' AS other")),
      pragma: await caught(() => db.query("PRAGMA foreign_keys = OFF")),
      vacuum: await caught(() => db.query("VACUUM")),
      begin: await caught(() => db.query("BEGIN")),
    };
    const remaining = await db.query("SELECT count(*) AS n FROM storage_values");
    return { placeholders, stored, returning, refused, remaining };
  },
});

/** Module savepoints: ROLLBACK TO keeps the savepoint; a caught batch failure keeps the rest. */
export const sqlSavepoints = reducer({
  args: schema.object({}),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async (ctx: ReducerContext) => {
    const { db } = ctx;
    await db.query("SAVEPOINT outer_sp");
    await db.query("INSERT INTO storage_parents (id) VALUES ('rolled-1')");
    await db.query("SAVEPOINT inner_sp");
    await db.query("INSERT INTO storage_parents (id) VALUES ('rolled-2')");
    await db.query("ROLLBACK TO outer_sp");
    await db.query("INSERT INTO storage_parents (id) VALUES ('kept')");
    await db.query("RELEASE outer_sp");
    const batch = await caught(() => applyDataWrites(ctx, [
      { kind: "insert", table: "storage_parents", key: "id", rows: [{ id: "in-batch" }] },
      { kind: "insert", table: "storage_parents", key: "id", rows: [{ id: "kept" }] },
    ]));
    await db.query("INSERT INTO storage_parents (id) VALUES ('after')");
    return { batch, parents: await db.query("SELECT id FROM storage_parents ORDER BY id") };
  },
});

/** Catches a SQL error and returns without ROLLBACK TO: the Reducer must not commit. */
export const caughtFailure = reducer({
  args: schema.object({}),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }) => {
    await db.query("INSERT INTO storage_parents (id) VALUES ('caught-1')");
    const first = await caught(() => db.query("INSERT INTO storage_parents (id) VALUES ('caught-1')"));
    const next = await caught(() => db.query("SELECT 1"));
    return { first, next };
  },
});

/** Writes a child whose deferred foreign key has no parent: fails at commit. */
export const orphan = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    await db.query("INSERT INTO storage_children (id, parent_id) VALUES ($1, 'missing')", [args.id]);
    return { wrote: args.id };
  },
});

/** A child written before its parent, in one transaction: commits. */
export const adopt = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    await db.query("INSERT INTO storage_children (id, parent_id) VALUES ($1, $2)", [args.id, `parent-${args.id}`]);
    await db.query("INSERT INTO storage_parents (id) VALUES ($1)", [`parent-${args.id}`]);
    return db.query("SELECT id, parent_id FROM storage_children WHERE id = $1", [args.id]);
  },
});

/** 300 existence probes in batches of 256 (over 100 bound parameters each). */
export const exists = reducer({
  args: schema.object({}),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }) => {
    await db.query("INSERT INTO storage_documents (id, number) VALUES ('d2', 'name-2'), ('d267', 'name-267')");
    const found = await existsDataRows(db, Array.from({ length: 300 }, (_, index) => ({ table: "storage_documents", where: { column: "number", op: "eq", value: `name-${index}` } })));
    return found.flatMap((value, index) => (value ? [index] : []));
  },
});

/** The `matches` predicate (REGEXP), as Whagons allocates document numbers. */
export const matching = reducer({
  args: schema.object({}),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }) => {
    await db.query("INSERT INTO storage_documents (id, number) VALUES ('p1', 'PR-009'), ('p2', 'PR-0010'), ('p3', 'PR-12x'), ('p4', 'imported 7'), ('p5', 'PR-')");
    return selectRows(db, { table: "storage_documents", columns: ["number"], where: { column: "number", op: "matches", pattern: "^PR-[0-9]+$" }, orderBy: [{ column: "number" }] });
  },
});
