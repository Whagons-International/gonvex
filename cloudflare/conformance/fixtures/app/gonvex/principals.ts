import { internalReducer, reducer, schema } from "@gonvex/module-sdk";

// Service principals (K17). A service socket calls `principals.verifyKey`
// (internal, as the API gateway's `apiKeys.authenticate` is) and the probes
// below; a delegated member socket calls the probes as that member. Results
// report what the module saw: the caller and `ctx.invocation`.

type Seen = {
  account: string | null;
  member: string | null;
  role: string | null;
  channel: string;
  rootChannel: string;
  actorAccountId: string | null;
  actorMemberId: string | null;
  commandIsRoot: boolean;
  delegation: unknown;
};

function seen(ctx: {
  auth: { account: { id: string } | null };
  member: { id: string; role?: string } | null;
  invocation: { channel: string; rootChannel: string; actorAccountId: string | null; actorMemberId: string | null; commandId: string; rootCommandId: string; delegation?: unknown };
}): Seen {
  const i = ctx.invocation;
  return {
    account: ctx.auth.account?.id ?? null,
    member: ctx.member?.id ?? null,
    role: ctx.member?.role ?? null,
    channel: i.channel,
    rootChannel: i.rootChannel,
    actorAccountId: i.actorAccountId,
    actorMemberId: i.actorMemberId,
    commandIsRoot: i.commandId === i.rootCommandId,
    delegation: i.delegation ?? null,
  };
}

/** The gateway's key check: internal, so only a service principal that allowlists it reaches it. */
export const verifyKey = internalReducer({
  args: schema.object({ keyHash: schema.string() }),
  result: schema.any(),
  run: async (ctx, args) => ({ ok: args.keyHash === "k17-valid-key-digest", seen: seen(ctx) }),
});

/** A read-only probe: no write, so no committed revision. */
export const whoami = reducer({
  args: schema.object({}),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async (ctx) => seen(ctx),
});

/** A write as the caller (a board row), reporting the provenance it ran with. */
export const record = reducer({
  args: schema.object({ id: schema.string(), title: schema.string() }),
  result: schema.any(),
  offline: { mode: "onlineOnly", reason: "conformance fixture" },
  nonOptimisticReason: "conformance fixture checks server results",
  run: async (ctx, args) => {
    await ctx.db.insert("board", { id: args.id, title: args.title, position: 900, bumps: 0 });
    return seen(ctx);
  },
});
