import { reducer, schema } from "@gonvex/module-sdk";

/** Writes, then throws: the write must roll back and the class is `rejected`. */
export const reject = reducer({
  args: schema.object({ id: schema.string() }),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    await db.insert("board", { id: args.id, title: "must roll back", position: 999, bumps: 0 });
    throw new Error("fixture rejected the intent");
  },
});

/** Never returns: the invocation deadline makes it `transient`. */
export const spin = reducer({
  args: schema.object({}),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async () => { for (;;) { /* burn the deadline */ } },
});

/** Returns a value that violates its declared result schema. */
export const badResult = reducer({
  args: schema.object({}),
  result: schema.object({ n: schema.integer() }),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async () => ({ n: "not a number" }) as unknown as { n: number },
});
