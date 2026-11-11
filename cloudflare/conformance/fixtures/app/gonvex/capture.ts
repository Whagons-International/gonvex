import { reducer, schema } from "@gonvex/module-sdk";

// Change capture (K03): runs the given statements as one Reducer, so each call
// is one transaction and one revision. A step with `catch` records its error
// instead of failing; `fail` throws after the statements (nothing commits).

interface Step {
  sql: string;
  params?: unknown[];
  catch?: boolean;
}

export const apply = reducer({
  args: schema.object({ steps: schema.any(), fail: schema.any() }),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async ({ db }, args) => {
    const errors: Array<string | null> = [];
    for (const step of args.steps as Step[]) {
      try {
        await db.query(step.sql, step.params ?? []);
        errors.push(null);
      } catch (error) {
        if (!step.catch) throw error;
        errors.push(String((error as Error)?.message ?? error));
      }
    }
    if (args.fail) throw new Error("capture fixture: rolled back on purpose");
    return { errors };
  },
});
