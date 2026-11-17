import { action } from "@gonvex/module-sdk";

// Direct export with the agent profile (whagons5-client gonvex/apiKeys.ts).
export const create = action({
  profile: "agent",
  run: async (ctx, args: { name: string }) => ({ name: args.name, kind: ctx.kind, function: ctx.function }),
});
