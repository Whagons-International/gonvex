import { cron, internalReducer, schema } from "@gonvex/module-sdk";

export const heartbeat = internalReducer({
  args: schema.object({}),
  result: schema.object({ ok: schema.boolean() }),
  run: async () => ({ ok: true }),
});

export const heartbeatSchedule = cron({
  name: "heartbeat",
  function: "system.heartbeat",
  args: {},
  intervalMs: 15_000,
});
