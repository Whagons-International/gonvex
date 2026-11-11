import { internalReducer, schema } from "@gonvex/module-sdk";

/**
 * Stand-in for Whagons' `legacyBridge.createBoardMessage`
 * (`cloudflare/bridge-support/patches/`): the same path, arguments and result,
 * and the same caller check. It records the identity and channel the runtime
 * gave the call, so `verify/service-principal.mjs` can assert them. Whagons'
 * own authorization logic is unit-tested in the app (`gonvex/module/legacyBridge.test.ts`).
 */
export const createBoardMessage = internalReducer({
  args: schema.object({
    entity: schema.object({
      _id: schema.string(),
      boardId: schema.string(),
      createdBy: schema.string(),
      title: schema.optional(schema.string()),
      content: schema.optional(schema.string()),
      isPinned: schema.optional(schema.boolean()),
      startsAt: schema.optional(schema.number()),
      endsAt: schema.optional(schema.number()),
    }),
  }),
  result: schema.string(),
  offline: { mode: "onlineOnly", reason: "service principal only" },
  run: async (ctx, args) => {
    if (ctx.invocation?.channel !== "api" || ctx.member?.id !== "_gonvex_service:whagons-legacy-bridge") {
      throw new Error("Only the legacy mobile bridge may call this function");
    }
    const entity = args.entity;
    await ctx.db.insert("boardMessages", {
      _id: entity._id,
      _creationTime: ctx.now,
      boardId: entity.boardId,
      createdBy: entity.createdBy,
      title: entity.title ?? null,
      content: entity.content ?? null,
      isPinned: entity.isPinned === true,
      startsAt: entity.startsAt ?? null,
      endsAt: entity.endsAt ?? null,
      caller: ctx.member.id,
      channel: ctx.invocation.channel,
    });
    return entity._id;
  },
});

/** Not on the principal's allowlist: the runtime must refuse it. */
export const notAllowed = internalReducer({
  args: schema.object({}),
  result: schema.null(),
  offline: { mode: "onlineOnly", reason: "service principal only" },
  run: async () => null,
});
