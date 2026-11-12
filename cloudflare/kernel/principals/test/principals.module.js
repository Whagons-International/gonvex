// K17 test module: Reducers that leave durable work, so a delegated call's
// outbox provenance can be read back.
import { reducer, schema } from "@gonvex/module-sdk";

export const enqueue = reducer({
  args: schema.object({}),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "k17 test" },
  nonOptimisticReason: "k17 test",
  run: async ({ actions }) => ({ action: await actions.enqueue("k17.notify", { n: 1 }) }),
});
